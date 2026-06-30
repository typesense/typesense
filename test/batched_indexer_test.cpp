#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>

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

    const auto wait_on = indexer.get_requests_to_wait_on(current_req->start_ts, "product_vehicle_fitments_se");

    EXPECT_EQ(1, wait_on.size());
    EXPECT_EQ(1, wait_on.count(earlier_req->start_ts));
    EXPECT_EQ(0, wait_on.count(later_req->start_ts));
}

TEST(BatchedIndexerTest, SerializesLatestChunkLogIndex) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    auto req = make_req(123, 10, "products_se");
    indexer.req_res_map.emplace(req->start_ts,
                                BatchedIndexer::req_res_t(req->start_ts, "", req, make_res(), 0, 2, 1, true, 25));

    nlohmann::json state;
    indexer.serialize_state(state);

    const auto& req_state = state["req_res_map"][std::to_string(req->start_ts)];
    EXPECT_EQ(25, req_state["latest_chunk_log_index"].get<uint64_t>());
}
