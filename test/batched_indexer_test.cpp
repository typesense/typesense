#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define private public
#include "batched_indexer.h"
#undef private

namespace {

std::shared_ptr<http_req> make_req(const uint64_t start_ts, const int64_t log_index,
                                   const std::string& collection) {
    auto req = std::make_shared<http_req>();
    req->start_ts = start_ts;
    req->log_index = log_index;
    req->params["collection"] = collection;
    return req;
}

std::shared_ptr<http_res> make_res() {
    return std::make_shared<http_res>(nullptr);
}

}  // namespace

TEST(BatchedIndexerTest, UsesLatestChunkLogIndexForReferenceDependencies) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    indexer.coll_to_references["product_vehicle_fitments_se"] = {"vehicles_se"};

    auto current_req = make_req(200, 50, "product_vehicle_fitments_se");
    auto req_res = BatchedIndexer::req_res_t(current_req->start_ts, "", current_req, make_res(), 0, 3, 0, true, 50);
    req_res.is_complete = true;
    indexer.req_res_map.emplace(current_req->start_ts, req_res);

    auto earlier_req = make_req(100, 10, "vehicles_se");
    req_res = BatchedIndexer::req_res_t(earlier_req->start_ts, "", earlier_req, make_res(), 0, 4, 0, false, 20);
    req_res.is_complete = true;
    indexer.req_res_map.emplace(earlier_req->start_ts, req_res);

    auto later_req = make_req(300, 40, "vehicles_se");
    req_res = BatchedIndexer::req_res_t(later_req->start_ts, "", later_req, make_res(), 0, 4, 0, false, 60);
    req_res.is_complete = true;
    indexer.req_res_map.emplace(later_req->start_ts, req_res);

    const auto wait_on = indexer.get_requests_to_wait_on(current_req->start_ts,
                                                          "product_vehicle_fitments_se", true);

    EXPECT_EQ(1, wait_on.size());
    EXPECT_EQ(1, wait_on.count(earlier_req->start_ts));
    EXPECT_EQ(0, wait_on.count(later_req->start_ts));
}

TEST(BatchedIndexerTest, KeepsRelatedRequestDependenciesBoundedByCollectionTails) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    indexer.coll_to_references["link_a"] = {"products"};
    indexer.coll_to_references["link_b"] = {"products"};
    indexer.coll_to_references["link_c"] = {"products"};

    const auto add_request = [&indexer](const uint64_t start_ts, const int64_t log_index,
                                        const std::string& collection) {
        auto req = make_req(start_ts, log_index, collection);
        indexer.req_res_map.emplace(start_ts,
                                    BatchedIndexer::req_res_t(start_ts, "", req, make_res(), 0, 1, 0, true,
                                                              log_index));
    };

    constexpr uint64_t backlog_size = 64;
    constexpr uint64_t link_a_offset = 1000;
    constexpr uint64_t link_b_offset = 2000;
    constexpr uint64_t link_c_offset = 3000;
    for (uint64_t i = 1; i <= backlog_size; i++) {
        add_request(i, i, "products");
        add_request(link_a_offset + i, link_a_offset + i, "link_a");
        add_request(link_b_offset + i, link_b_offset + i, "link_b");
        add_request(link_c_offset + i, link_c_offset + i, "link_c");
    }
    indexer.collection_request_tails["products"] = backlog_size;
    indexer.collection_request_tails["link_a"] = link_a_offset + backlog_size;
    indexer.collection_request_tails["link_b"] = link_b_offset + backlog_size;
    indexer.collection_request_tails["link_c"] = link_c_offset + backlog_size;

    constexpr uint64_t current_request_id = 10000;
    add_request(current_request_id, current_request_id, "products");

    const auto wait_on = indexer.get_requests_to_wait_on(current_request_id, "products");

    // The backlog length must not affect dependency-set size. The tail of each relevant collection already carries
    // the ordering chain for all of its predecessors.
    EXPECT_EQ(4, wait_on.size());
    EXPECT_EQ(1, wait_on.count(backlog_size));
    EXPECT_EQ(1, wait_on.count(link_a_offset + backlog_size));
    EXPECT_EQ(1, wait_on.count(link_b_offset + backlog_size));
    EXPECT_EQ(1, wait_on.count(link_c_offset + backlog_size));
}

TEST(BatchedIndexerTest, RevisitsOnlyReferenceWaitersAffectedByCompletion) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    constexpr uint64_t backlog_size = 64;
    {
        std::unique_lock lk(indexer.mutex);
        for (uint64_t request_id = 1; request_id <= backlog_size; request_id++) {
            auto req = make_req(request_id, request_id, "orders");
            indexer.req_res_map.emplace(request_id,
                                        BatchedIndexer::req_res_t(request_id, "", req, make_res(), request_id,
                                                                  1, 0, true, request_id));
            if (request_id == 1) {
                continue;
            }

            BatchedIndexer::refq_entry ref(0, request_id);
            ref.waiting_on_requests.insert(request_id - 1);
            indexer.add_reference_request(std::move(ref));
        }
        indexer.req_res_map.erase(1);
    }

    // Completing request 1 affects only request 2. An indexed waiter lookup should not revisit requests 3 through 64.
    const auto entries_examined = indexer.process_reference_queue_with_lock(1);

    EXPECT_EQ(1, entries_examined);
    ASSERT_EQ(1, indexer.queues[0].size());
    EXPECT_EQ(2, indexer.queues[0].front());
    EXPECT_EQ(backlog_size - 2, indexer.reference_q.size());
}

TEST(BatchedIndexerTest, ReleasesIndexedReferenceWaiterAfterEveryDependencyCompletes) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    {
        std::unique_lock lk(indexer.mutex);
        for (const auto request_id : {uint64_t{10}, uint64_t{20}, uint64_t{30}}) {
            auto req = make_req(request_id, request_id, "orders");
            indexer.req_res_map.emplace(request_id,
                                        BatchedIndexer::req_res_t(request_id, "", req, make_res(), request_id,
                                                                  1, 0, true, request_id));
        }

        BatchedIndexer::refq_entry ref(0, 30);
        ref.waiting_on_requests.insert(10);
        ref.waiting_on_requests.insert(20);
        indexer.add_reference_request(std::move(ref));
        indexer.req_res_map.erase(10);
    }

    EXPECT_EQ(1, indexer.process_reference_queue_with_lock(10));
    EXPECT_TRUE(indexer.queues[0].empty());
    ASSERT_EQ(1, indexer.reference_q.size());
    EXPECT_EQ(1, indexer.reference_q.front().waiting_on_requests.count(20));

    {
        std::unique_lock lk(indexer.mutex);
        indexer.req_res_map.erase(20);
    }
    EXPECT_EQ(1, indexer.process_reference_queue_with_lock(20));
    EXPECT_TRUE(indexer.reference_q.empty());
    EXPECT_TRUE(indexer.reference_q_by_request.empty());
    EXPECT_TRUE(indexer.reference_waiters.empty());
    ASSERT_EQ(1, indexer.queues[0].size());
    EXPECT_EQ(30, indexer.queues[0].front());
}

TEST(BatchedIndexerTest, ReleasesAffectedReferenceWaitersInQueueOrder) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    {
        std::unique_lock lk(indexer.mutex);
        for (const auto request_id : {uint64_t{10}, uint64_t{20}, uint64_t{30}}) {
            auto req = make_req(request_id, request_id, "orders");
            indexer.req_res_map.emplace(request_id,
                                        BatchedIndexer::req_res_t(request_id, "", req, make_res(), request_id,
                                                                  1, 0, true, request_id));
        }

        for (const auto request_id : {uint64_t{20}, uint64_t{30}}) {
            BatchedIndexer::refq_entry ref(0, request_id);
            ref.waiting_on_requests.insert(10);
            indexer.add_reference_request(std::move(ref));
        }
        indexer.req_res_map.erase(10);
    }

    EXPECT_EQ(2, indexer.process_reference_queue_with_lock(10));
    ASSERT_EQ(2, indexer.queues[0].size());
    EXPECT_EQ(20, indexer.queues[0][0]);
    EXPECT_EQ(30, indexer.queues[0][1]);
}

TEST(BatchedIndexerTest, WaitsOnReorderedQueueTailAfterReferenceRequestIsReleased) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    const auto add_request = [&indexer](const uint64_t start_ts, const std::string& collection) {
        auto req = make_req(start_ts, start_ts, collection);
        indexer.req_res_map.emplace(start_ts,
                                    BatchedIndexer::req_res_t(start_ts, "", req, make_res(), start_ts,
                                                              1, 0, true, start_ts));
    };

    // Request 20 entered the reference queue while orders referenced inventory and was blocked on request 10. Request
    // 30 bypassed it while the reference was absent, modeling state produced before that gap was prevented.
    add_request(10, "inventory");
    add_request(20, "orders");
    BatchedIndexer::refq_entry blocked_request(0, 20);
    blocked_request.waiting_on_requests.insert(10);
    indexer.reference_q.emplace_back(std::move(blocked_request));

    add_request(30, "orders");
    indexer.queues[0].emplace_back(30);

    // Once request 10 completes, request 20 leaves reference_q and is appended behind request 30. Its logical request
    // order is older, but it is now the physical tail that a related request must wait for.
    indexer.req_res_map.erase(10);
    indexer.queues[0].emplace_back(20);
    indexer.reference_q.clear();
    indexer.collection_request_tails["orders"] = 20;
    ASSERT_EQ(2, indexer.queues[0].size());
    EXPECT_EQ(30, indexer.queues[0][0]);
    EXPECT_EQ(20, indexer.queues[0][1]);

    indexer.coll_to_references["orders"] = {"products"};
    add_request(40, "products");

    const auto wait_on = indexer.get_requests_to_wait_on(40, "products");

    EXPECT_EQ(1, wait_on.size());
    EXPECT_EQ(1, wait_on.count(20));
    EXPECT_EQ(0, wait_on.count(30));
}

TEST(BatchedIndexerTest, ChainsSameCollectionRequestsThroughReferenceQueueTail) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    const auto add_request = [&indexer](const uint64_t request_id) {
        auto req = make_req(request_id, request_id, "orders");
        indexer.req_res_map.emplace(request_id,
                                    BatchedIndexer::req_res_t(request_id, "", req, make_res(), request_id,
                                                              1, 0, true, request_id));
    };

    add_request(10);
    add_request(20);
    {
        std::unique_lock lk(indexer.mutex);
        BatchedIndexer::refq_entry ref(0, 20);
        ref.waiting_on_requests.insert(10);
        indexer.add_reference_request(std::move(ref));
    }
    indexer.collection_request_tails["orders"] = 20;

    // Even if a schema update temporarily removes every reference, request 30 must extend request 20's explicit
    // chain instead of entering the collection's main queue ahead of it.
    add_request(30);
    const auto wait_on = indexer.get_requests_to_wait_on(30, "orders");

    ASSERT_EQ(1, wait_on.size());
    EXPECT_EQ(1, wait_on.count(20));
}

TEST(BatchedIndexerTest, OrdersMixedLogIndexRequestsTransitively) {
    // Pair-dependent ordering previously compared A and B by raft log index but pairs involving legacy request C
    // by last_updated, producing A < B < C < A. The total request order must not contain that cycle (or its
    // reverse), since it is used by std::sort and to select the latest predecessor for a collection.
    constexpr uint64_t a_log_index = 10;
    constexpr uint64_t a_last_updated = 30;
    constexpr uint64_t a_req_id = 1;
    constexpr uint64_t b_log_index = 20;
    constexpr uint64_t b_last_updated = 10;
    constexpr uint64_t b_req_id = 2;
    constexpr uint64_t c_log_index = 0;
    constexpr uint64_t c_last_updated = 20;
    constexpr uint64_t c_req_id = 3;

    const bool a_before_b = BatchedIndexer::is_request_earlier(a_log_index, a_last_updated, a_req_id,
                                                                b_log_index, b_last_updated, b_req_id);
    const bool b_before_a = BatchedIndexer::is_request_earlier(b_log_index, b_last_updated, b_req_id,
                                                                a_log_index, a_last_updated, a_req_id);
    const bool b_before_c = BatchedIndexer::is_request_earlier(b_log_index, b_last_updated, b_req_id,
                                                                c_log_index, c_last_updated, c_req_id);
    const bool c_before_b = BatchedIndexer::is_request_earlier(c_log_index, c_last_updated, c_req_id,
                                                                b_log_index, b_last_updated, b_req_id);
    const bool c_before_a = BatchedIndexer::is_request_earlier(c_log_index, c_last_updated, c_req_id,
                                                                a_log_index, a_last_updated, a_req_id);
    const bool a_before_c = BatchedIndexer::is_request_earlier(a_log_index, a_last_updated, a_req_id,
                                                                c_log_index, c_last_updated, c_req_id);

    EXPECT_FALSE((a_before_b && b_before_c && c_before_a) ||
                 (b_before_a && c_before_b && a_before_c));
}

TEST(BatchedIndexerTest, RestoresMixedLogIndexRequestsInReferenceDependencyOrder) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    const auto add_request = [&source_indexer](const uint64_t start_ts, const int64_t log_index,
                                                const uint64_t last_updated, const std::string& collection) {
        auto req = make_req(start_ts, log_index, collection);
        source_indexer.req_res_map.emplace(start_ts,
                                           BatchedIndexer::req_res_t(start_ts, "", req, make_res(), last_updated,
                                                                     1, 0, true, log_index));
    };

    // The legacy request has no raft log index but was updated later, so it is the latest dependency. A restart
    // must not replay it before the older, raft-indexed request merely because zero sorts before a real log index.
    add_request(10, 0, 30, "products");
    add_request(20, 80, 10, "products");
    add_request(30, 0, 40, "link_a");

    nlohmann::json state;
    source_indexer.serialize_state(state);

    BatchedIndexer restored_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    restored_indexer.load_state(state);
    restored_indexer.coll_to_references["link_a"] = {"products"};

    const auto wait_on = restored_indexer.get_requests_to_wait_on(30, "link_a");
    EXPECT_EQ(1, wait_on.size());
    EXPECT_EQ(1, wait_on.count(10));
    EXPECT_EQ(0, wait_on.count(20));

    const auto& restored_queue = restored_indexer.queues[0];
    ASSERT_EQ(3, restored_queue.size());
    EXPECT_EQ(20, restored_queue[0]);
    EXPECT_EQ(10, restored_queue[1]);
    EXPECT_EQ(30, restored_queue[2]);
}

TEST(BatchedIndexerTest, KeepsPersistedCollectionTailLastAfterMixedOrderRestore) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    const auto add_request = [&source_indexer](const uint64_t request_id, const int64_t log_index,
                                                const uint64_t last_updated) {
        auto req = make_req(request_id, log_index, "products");
        source_indexer.req_res_map.emplace(request_id,
                                           BatchedIndexer::req_res_t(request_id, "", req, make_res(), last_updated,
                                                                     1, 0, true, log_index));
    };

    add_request(10, 0, 30);
    add_request(20, 80, 10);
    source_indexer.collection_request_tails["products"] = 20;

    nlohmann::json state;
    source_indexer.serialize_state(state);

    BatchedIndexer restored_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    restored_indexer.load_state(state);

    const auto& restored_queue = restored_indexer.queues[0];
    ASSERT_EQ(2, restored_queue.size());
    EXPECT_EQ(10, restored_queue[0]);
    EXPECT_EQ(20, restored_queue[1]);
    EXPECT_EQ(20, restored_indexer.collection_request_tails.at("products"));
}

TEST(BatchedIndexerTest, MigratesLegacySnapshotDependenciesToBoundedCollectionTails) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    constexpr uint64_t backlog_size = 64;
    for (uint64_t request_id = 1; request_id <= backlog_size; request_id++) {
        auto req = make_req(request_id, request_id, "orders");
        source_indexer.req_res_map.emplace(request_id,
                                           BatchedIndexer::req_res_t(request_id, "", req, make_res(), request_id,
                                                                     1, 0, true, request_id));

        if (request_id == 1) {
            continue;
        }

        BatchedIndexer::refq_entry ref(0, request_id);
        for (uint64_t predecessor_id = 1; predecessor_id < request_id; predecessor_id++) {
            ref.waiting_on_requests.insert(predecessor_id);
        }
        source_indexer.reference_q.emplace_back(std::move(ref));
    }
    source_indexer.queued_writes = backlog_size;

    nlohmann::json legacy_state;
    source_indexer.serialize_state(legacy_state);
    legacy_state.erase("collection_request_tails");

    BatchedIndexer restored_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    restored_indexer.load_state(legacy_state);

    size_t restored_dependency_count = 0;
    size_t restored_max_dependencies = 0;
    for (const auto& ref : restored_indexer.reference_q) {
        restored_dependency_count += ref.waiting_on_requests.size();
        if (ref.waiting_on_requests.size() > restored_max_dependencies) {
            restored_max_dependencies = ref.waiting_on_requests.size();
        }
    }

    size_t indexed_dependency_count = 0;
    for (const auto& dependency_waiters : restored_indexer.reference_waiters) {
        indexed_dependency_count += dependency_waiters.second.size();
    }

    EXPECT_LE(restored_dependency_count, backlog_size - 1);
    EXPECT_LE(restored_max_dependencies, 1);
    EXPECT_EQ(restored_dependency_count, indexed_dependency_count);
    EXPECT_EQ(restored_indexer.reference_q.size(), restored_indexer.reference_q_by_request.size());
    ASSERT_FALSE(restored_indexer.reference_q.empty());
    EXPECT_EQ(1, restored_indexer.reference_q.back().waiting_on_requests.count(backlog_size - 1));

    nlohmann::json migrated_state;
    restored_indexer.serialize_state(migrated_state);

    size_t serialized_dependency_count = 0;
    size_t serialized_max_dependencies = 0;
    for (const auto& ref : migrated_state["reference_q"]) {
        const auto dependency_count = ref["waiting_on_requests"].size();
        serialized_dependency_count += dependency_count;
        if (dependency_count > serialized_max_dependencies) {
            serialized_max_dependencies = dependency_count;
        }
    }

    EXPECT_LE(serialized_dependency_count, backlog_size - 1);
    EXPECT_LE(serialized_max_dependencies, 1);
    EXPECT_TRUE(migrated_state.contains("collection_request_tails"));
    EXPECT_EQ(backlog_size, migrated_state["collection_request_tails"]["orders"].get<uint64_t>());
}

TEST(BatchedIndexerTest, PreservesIndependentSameCollectionDependenciesWhenMigratingLegacySnapshot) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    const auto add_request = [&source_indexer](const uint64_t request_id, const std::string& collection) {
        auto req = make_req(request_id, request_id, collection);
        source_indexer.req_res_map.emplace(request_id,
                                           BatchedIndexer::req_res_t(request_id, "", req, make_res(), request_id,
                                                                     1, 0, true, request_id));
    };

    add_request(10, "inventory");
    add_request(20, "orders");
    add_request(30, "orders");
    add_request(40, "orders");

    // Request 20 was blocked while orders referenced inventory. During a topology gap, request 30 bypassed it and
    // entered the main queue. Once the reference was restored, request 40 had to wait on both independent requests.
    BatchedIndexer::refq_entry blocked_request(0, 20);
    blocked_request.waiting_on_requests.insert(10);
    source_indexer.reference_q.emplace_back(std::move(blocked_request));

    BatchedIndexer::refq_entry follower_request(0, 40);
    follower_request.waiting_on_requests.insert(20);
    follower_request.waiting_on_requests.insert(30);
    source_indexer.reference_q.emplace_back(std::move(follower_request));
    source_indexer.queued_writes = 4;

    nlohmann::json legacy_state;
    source_indexer.serialize_state(legacy_state);
    legacy_state.erase("collection_request_tails");

    BatchedIndexer restored_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    restored_indexer.load_state(legacy_state);

    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> restored_dependencies;
    for (const auto& ref : restored_indexer.reference_q) {
        restored_dependencies.emplace(ref.start_ts, ref.waiting_on_requests);
    }

    ASSERT_EQ(1, restored_dependencies.count(20));
    ASSERT_EQ(1, restored_dependencies.at(20).count(10));
    ASSERT_EQ(1, restored_dependencies.count(40));

    // Migration may retain 20 directly or make 30 depend on 20 before reducing request 40 to the tail. Either way,
    // the restored dependency graph must still contain a path from request 40 to independently blocked request 20.
    const auto has_dependency_path = [&restored_dependencies](const uint64_t request_id,
                                                               const uint64_t dependency_id) {
        std::unordered_set<uint64_t> visited;
        std::vector<uint64_t> pending{request_id};
        while (!pending.empty()) {
            const auto current_request_id = pending.back();
            pending.pop_back();
            if (!visited.insert(current_request_id).second) {
                continue;
            }

            const auto dependencies_it = restored_dependencies.find(current_request_id);
            if (dependencies_it == restored_dependencies.end()) {
                continue;
            }

            for (const auto current_dependency_id : dependencies_it->second) {
                if (current_dependency_id == dependency_id) {
                    return true;
                }
                pending.push_back(current_dependency_id);
            }
        }
        return false;
    };

    EXPECT_TRUE(has_dependency_path(40, 20));
}

TEST(BatchedIndexerTest, SerializesLatestChunkLogIndex) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    auto req = make_req(123, 10, "products_se");
    indexer.req_res_map.emplace(req->start_ts,
                                BatchedIndexer::req_res_t(req->start_ts, "", req, make_res(), 0, 2, 1, true, 25));
    indexer.collection_request_tails["products_se"] = req->start_ts;

    nlohmann::json state;
    indexer.serialize_state(state);

    const auto& req_state = state["req_res_map"][std::to_string(req->start_ts)];
    EXPECT_EQ(25, req_state["latest_chunk_log_index"].get<uint64_t>());
    EXPECT_EQ(req->start_ts, state["collection_request_tails"]["products_se"].get<uint64_t>());

    BatchedIndexer restored_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    restored_indexer.load_state(state);
    EXPECT_EQ(req->start_ts, restored_indexer.collection_request_tails.at("products_se"));
}
