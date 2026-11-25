#include <gtest/gtest.h>
#include <braft/log.h>
#include <braft/raft.h>
#include <braft/snapshot.h>
#include <braft/log_manager.h>
#include <braft/fsm_caller.h>
#include <brpc/server.h>
#include <chrono>
#include <filesystem>
#include <future>
#include <string>

#define private public
#include "raft_server.h"
#include "test_tmp_path.h"
#undef private

namespace {
    std::shared_ptr<http_res> seed_indexer_request(BatchedIndexer& indexer, const uint64_t request_id,
                                                   const bool live_response) {
        auto req = std::make_shared<http_req>();
        req->start_ts = request_id;
        req->log_index = request_id;
        req->params["collection"] = "products";

        auto res = std::make_shared<http_res>(nullptr);
        res->is_alive = live_response;
        res->final = !live_response;
        indexer.req_res_map.emplace(
            request_id, BatchedIndexer::req_res_t(request_id, "", req, res, request_id,
                                                  1, 0, true, request_id));
        indexer.queues[0].emplace_back(request_id);
        indexer.queued_writes++;
        return res;
    }

    bool matches_either_ip_version(const std::string& result, 
                                 const std::string& ipv4_version,
                                 const std::string& ipv6_version) {
        return result == ipv4_version || result == ipv6_version;
    }

    bool is_ipv4(const std::string& str) {
        struct sockaddr_in sa;
        return inet_pton(AF_INET, str.c_str(), &(sa.sin_addr)) != 0;
    }

    bool is_ipv6_with_brackets(const std::string& str) {
        if (str.length() < 2 || str[0] != '[' || str[str.length() - 1] != ']') {
            return false;
        }

        std::string ipv6 = str.substr(1, str.length() - 2); // Remove [ and ]
        struct sockaddr_in6 sa;
        return inet_pton(AF_INET6, ipv6.c_str(), &(sa.sin6_addr)) != 0;
    }

    class NoopStateMachine : public braft::StateMachine {
    public:
        std::atomic<size_t> applied{0};
        void on_apply(braft::Iterator& iter) override {
            for(; iter.valid(); iter.next()) {
                applied++;
                braft::AsyncClosureGuard done_guard(iter.done());
            }
        }

        void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override {
            done->Run();
        }

        int on_snapshot_load(braft::SnapshotReader* reader) override {
            return 0;
        }
    };

    // a braft node with a peering server, started with an empty `initial_conf` like a node with unresolved peers
    struct TestRaftNode {
        brpc::Server server;
        NoopStateMachine fsm;
        braft::Node* node = nullptr;

        int start(const std::string& peer, const std::string& raft_dir) {
            braft::PeerId peer_id(peer);
            if(braft::add_service(&server, peer_id.addr) != 0 || server.Start(peer_id.addr, nullptr) != 0) {
                return -1;
            }

            braft::NodeOptions node_options;
            node_options.election_timeout_ms = 300;
            node_options.snapshot_interval_s = -1;
            node_options.fsm = &fsm;
            node_options.node_owns_fsm = false;
            node_options.log_uri = "local://" + raft_dir + "/log";
            node_options.raft_meta_uri = "local://" + raft_dir + "/meta";
            node_options.snapshot_uri = "local://" + raft_dir + "/snapshot";
            node_options.disable_cli = true;

            node = new braft::Node("default_group", peer_id);
            return node->init(node_options);
        }

        bool wait_for_leader() {
            for(size_t i = 0; i < 100 && !node->is_leader(); i++) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            return node->is_leader();
        }

        std::vector<braft::PeerId> peers() {
            std::vector<braft::PeerId> peers;
            node->list_peers(&peers);
            return peers;
        }

        ~TestRaftNode() {
            if(node) {
                node->shutdown(nullptr);
                node->join();
                delete node;
            }
            server.Stop(0);
            server.Join();
        }
    };

    void append_configuration_entry(const std::string& log_dir, const std::string& peer) {
        braft::ConfigurationManager configuration_manager;
        braft::SegmentLogStorage log_storage(log_dir);
        ASSERT_EQ(0, log_storage.init(&configuration_manager));

        braft::LogEntry* entry = new braft::LogEntry();
        entry->AddRef();
        entry->type = braft::ENTRY_TYPE_CONFIGURATION;
        entry->id = braft::LogId(1, 1);
        entry->peers = new std::vector<braft::PeerId>{braft::PeerId(peer)};
        ASSERT_EQ(0, log_storage.append_entry(entry));
        entry->Release();
    }

    void save_snapshot(const std::string& snapshot_dir, const std::string& peer) {
        braft::LocalSnapshotStorage snapshot_storage(snapshot_dir);
        ASSERT_EQ(0, snapshot_storage.init());

        braft::SnapshotWriter* writer = snapshot_storage.create();
        ASSERT_NE(nullptr, writer);

        braft::SnapshotMeta meta;
        meta.set_last_included_index(5);
        meta.set_last_included_term(1);
        meta.add_peers(peer);
        ASSERT_EQ(0, writer->save_meta(meta));
        ASSERT_EQ(0, snapshot_storage.close(writer));
    }

    void gzip_compress(const std::string& payload, std::string& compressed) {
        z_stream compressor{};
        ASSERT_EQ(Z_OK, deflateInit2(&compressor, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                                     16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY));
        compressed.assign(compressBound(payload.size()) + 32, '\0');
        compressor.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(payload.data()));
        compressor.avail_in = payload.size();
        compressor.next_out = reinterpret_cast<Bytef *>(compressed.data());
        compressor.avail_out = compressed.size();
        ASSERT_EQ(Z_STREAM_END, deflate(&compressor, Z_FINISH));
        compressed.resize(compressor.total_out);
        deflateEnd(&compressor);
    }
}

TEST(RaftServerTest, DecompressesHighlyCompressibleGzipAcrossChunks) {
    const std::string payload(1024 * 1024, 'a');
    std::string compressed;
    ASSERT_NO_FATAL_FAILURE(gzip_compress(payload, compressed));

    auto req = std::make_shared<http_req>();
    req->body = compressed.substr(0, compressed.size() / 2);
    req->last_chunk_aggregate = false;
    ASSERT_TRUE(ReplicationState::handle_gzip(req).ok());
    std::string result = req->body;
    req->body = compressed.substr(compressed.size() / 2);
    req->last_chunk_aggregate = true;
    ASSERT_TRUE(ReplicationState::handle_gzip(req).ok());
    result += req->body;
    EXPECT_EQ(payload, result);
}

TEST(RaftServerTest, RejectsTruncatedGzipStreamWhenLastChunkCarriesRemainingBytes) {
    std::string compressed;
    ASSERT_NO_FATAL_FAILURE(gzip_compress(std::string(1024 * 1024, 'a'), compressed));
    // drop the 8-byte gzip trailer (CRC32 + ISIZE), so the stream never reaches Z_STREAM_END
    const std::string truncated = compressed.substr(0, compressed.size() - 8);

    auto req = std::make_shared<http_req>();
    req->body = truncated.substr(0, truncated.size() / 2);
    req->last_chunk_aggregate = false;
    ASSERT_TRUE(ReplicationState::handle_gzip(req).ok());
    ASSERT_TRUE(req->zstream_initialized);

    req->body = truncated.substr(truncated.size() / 2);
    req->last_chunk_aggregate = true;
    auto res = ReplicationState::handle_gzip(req);
    ASSERT_FALSE(res.ok());
    ASSERT_EQ(400, res.code());
    ASSERT_EQ("Incomplete gzip stream.", res.error());
    ASSERT_FALSE(req->zstream_initialized);
}

TEST(RaftServerTest, RejectsTruncatedGzipStreamWhenLastChunkIsEmpty) {
    std::string compressed;
    ASSERT_NO_FATAL_FAILURE(gzip_compress(std::string(1024 * 1024, 'a'), compressed));

    auto req = std::make_shared<http_req>();
    req->body = compressed.substr(0, compressed.size() / 2);
    req->last_chunk_aggregate = false;
    ASSERT_TRUE(ReplicationState::handle_gzip(req).ok());
    ASSERT_TRUE(req->zstream_initialized);

    req->body = "";
    req->last_chunk_aggregate = true;
    auto res = ReplicationState::handle_gzip(req);
    ASSERT_FALSE(res.ok());
    ASSERT_EQ(400, res.code());
    ASSERT_EQ("Incomplete gzip stream.", res.error());
    ASSERT_FALSE(req->zstream_initialized);
}

TEST(RaftServerTest, SnapshotLoadGateBlocksReadinessAndCatchupRefresh) {
    auto& config = Config::get_instance();
    ReplicationState replication_state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);

    replication_state.read_caught_up = true;
    replication_state.write_caught_up = true;
    replication_state.snapshot_load_blocks_readiness = true;
    EXPECT_FALSE(replication_state.is_read_caught_up());
    EXPECT_FALSE(replication_state.is_write_caught_up());

    std::unique_lock snapshot_load_lock(replication_state.snapshot_load_mutex);
    replication_state.snapshot_load_blocks_readiness = false;
    auto refresh = std::async(std::launch::async, [&replication_state]() {
        replication_state.refresh_catchup_status(false);
    });

    EXPECT_EQ(std::future_status::timeout, refresh.wait_for(std::chrono::milliseconds(50)));
    snapshot_load_lock.unlock();
    EXPECT_EQ(std::future_status::ready, refresh.wait_for(std::chrono::seconds(1)));
    EXPECT_FALSE(replication_state.is_read_caught_up());
    EXPECT_FALSE(replication_state.is_write_caught_up());
}

TEST(RaftServerTest, StorageFaultKeepsReadinessClosedAfterCatchupRefresh) {
    auto& config = Config::get_instance();
    ReplicationState state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false, &config, 1, 1);
    state.read_caught_up = true;
    state.write_caught_up = true;
    EXPECT_TRUE(state.is_alive());
    EXPECT_TRUE(state.is_read_caught_up());
    EXPECT_TRUE(state.is_write_caught_up());
    state.storage_fault = true;
    state.refresh_catchup_status(false);
    state.read_caught_up = true;
    state.write_caught_up = true;
    EXPECT_FALSE(state.is_alive());
    EXPECT_FALSE(state.is_read_caught_up());
    EXPECT_FALSE(state.is_write_caught_up());
}

TEST(RaftServerTest, FailedClosuresCompleteOnceAndReleaseEveryPendingTask) {
    auto& config = Config::get_instance();
    ReplicationState state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false, &config, 1, 1);
    auto req = std::make_shared<http_req>();
    auto res = std::make_shared<http_res>(nullptr);
    state.pending_writes = 2;
    auto* first = new ReplicationClosure(req, res, &state);
    auto* second = new ReplicationClosure(req, res, &state);
    first->status().set_error(EIO, "failed");
    second->status().set_error(EIO, "failed");
    first->Run();
    EXPECT_EQ(1, state.pending_writes.load());
    EXPECT_EQ(503, res->status_code);
    EXPECT_TRUE(res->final);
    EXPECT_NE(std::string::npos, res->body.find("uncertain"));
    EXPECT_TRUE(req->ready);
    EXPECT_TRUE(res->ready);
    req->ready = false;
    res->ready = false;
    second->Run();
    EXPECT_EQ(0, state.pending_writes.load());
    EXPECT_FALSE(req->ready);
    EXPECT_FALSE(res->ready);
}

TEST(RaftServerTest, SuccessfulClosureReleasesPendingWithoutCompletingResponse) {
    auto& config = Config::get_instance();
    ReplicationState state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false, &config, 1, 1);
    auto req = std::make_shared<http_req>();
    auto res = std::make_shared<http_res>(nullptr);
    state.pending_writes = 1;
    (new ReplicationClosure(req, res, &state))->Run();
    EXPECT_EQ(0, state.pending_writes.load());
    EXPECT_FALSE(res->terminal_error_claimed);
    EXPECT_FALSE(req->ready);
    EXPECT_FALSE(res->ready);
}

TEST(RaftServerTest, FailedClosureAndSnapshotCancellationShareResponseOwnership) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    ReplicationState state(nullptr, &indexer, nullptr, nullptr, nullptr, nullptr, false, &config, 1, 1);
    auto res = seed_indexer_request(indexer, 42, true);
    auto req = indexer.req_res_map.at(42).req;
    state.pending_writes = 1;
    auto* closure = new ReplicationClosure(req, res, &state);
    closure->status().set_error(EIO, "failed");
    closure->Run();
    req->ready = false;
    res->ready = false;
    {
        std::unique_lock lock(indexer.lifecycle_mutex);
        indexer.clear_state_unlocked();
    }
    EXPECT_FALSE(req->ready);
    EXPECT_FALSE(res->ready);
    EXPECT_NE(std::string::npos, res->body.find("uncertain"));
}

TEST(RaftServerTest, FailedClosureOwnsResponseBeforePartialUploadGarbageCollection) {
    char directory[] = "/tmp/typesense-raft-gc-XXXXXX";
    ASSERT_NE(nullptr, mkdtemp(directory));
    {
        Store store(directory);
        std::atomic<bool> skip_writes(false);
        auto& config = Config::get_instance();
        BatchedIndexer indexer(nullptr, &store, &store, 1, config, skip_writes);
        ReplicationState state(nullptr, &indexer, &store, nullptr, nullptr, nullptr, false, &config, 1, 1);
        auto res = seed_indexer_request(indexer, 42, true);
        auto req = indexer.req_res_map.at(42).req;
        indexer.req_res_map.at(42).is_complete = false;
        indexer.req_res_map.at(42).last_updated = 0;
        indexer.queues[0].clear();
        indexer.queued_writes = 0;
        state.pending_writes = 1;
        auto* closure = new ReplicationClosure(req, res, &state);
        closure->status().set_error(EIO, "failed");
        closure->Run();
        indexer.last_gc_run = std::chrono::high_resolution_clock::now() - std::chrono::seconds(120);
        std::thread worker([&indexer] { indexer.run(); });
        bool removed = false;
        for (int i = 0; i < 5000; ++i) {
            {
                std::unique_lock lock(indexer.mutex);
                removed = indexer.req_res_map.empty();
            }
            if (removed) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        indexer.stop();
        worker.join();
        EXPECT_TRUE(removed);
        EXPECT_NE(std::string::npos, res->body.find("uncertain"));
        EXPECT_TRUE(res->terminal_error_claimed);
    }
    std::filesystem::remove_all(directory);
}

TEST(RaftServerTest, ExpectedTermRejectionCompletesAndShutdownDrains) {
    const std::string dir = test_tmp_path("/raft_term_rejection");
    const std::string peer = "127.0.0.1:18137:18138";
    std::filesystem::remove_all(dir);
    append_configuration_entry(dir + "/log", peer);
    std::filesystem::create_directories(dir + "/db");
    {
        TestRaftNode raft_node;
        ASSERT_EQ(0, raft_node.start(peer, dir));
        ASSERT_TRUE(raft_node.wait_for_leader());
        Store store(dir + "/db");
        std::atomic<bool> skip_writes(false);
        auto& config = Config::get_instance();
        BatchedIndexer indexer(nullptr, &store, &store, 1, config, skip_writes);
        auto state = std::make_unique<ReplicationState>(nullptr, &indexer, &store, nullptr, nullptr,
                                                        nullptr, false, &config, 1, 1);
        state->node = raft_node.node;
        auto request = std::make_shared<http_req>();
        auto response = std::make_shared<http_res>(nullptr);
        butil::IOBuf payload;
        payload.append(request->to_json());
        braft::Task task;
        task.data = &payload;
        task.done = new ReplicationClosure(request, response, state.get());
        task.expected_term = INT64_MAX;
        state->pending_writes = 1;
        raft_node.node->apply(task);
        for (int i = 0; i < 5000 && state->pending_writes.load() != 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (state->pending_writes.load() != 0) {
            state.release(); // the rejected task may still invoke its closure
            FAIL() << "rejected task did not complete";
        }
        EXPECT_EQ(503, response->status_code);
        EXPECT_TRUE(response->terminal_error_claimed);
        EXPECT_EQ(0, raft_node.fsm.applied.load());
        state->shutdown();
        raft_node.node = nullptr; // shutdown transferred and deleted the node
    }
    std::filesystem::remove_all(dir);
}

TEST(RaftServerTest, ReadOnlyStoreStopsCommittedBatchForLeaderAndFollower) {
    class Stable : public braft::LogManager::StableClosure {
    public:
        std::promise<void> complete;
        void Run() override { complete.set_value(); }
    };
    for (const bool follower : {false, true}) {
        char directory[] = "/tmp/typesense-raft-fsm-XXXXXX";
        ASSERT_NE(nullptr, mkdtemp(directory));
        {
            const std::string db_dir = std::string(directory) + "/db";
            const std::string log_dir = std::string(directory) + "/log";
            std::filesystem::create_directory(db_dir);
            Store store(db_dir);
            store.close();
            ASSERT_TRUE(rocksdb::DB::OpenForReadOnly(store.options, db_dir, &store.db).ok());
            std::atomic<bool> skip_writes(false);
            auto& config = Config::get_instance();
            BatchedIndexer indexer(nullptr, &store, &store, 1, config, skip_writes);
            auto state = std::make_unique<ReplicationState>(nullptr, &indexer, &store, nullptr, nullptr,
                                                             nullptr, false, &config, 1, 1);
            braft::ConfigurationManager configuration;
            braft::SegmentLogStorage storage(log_dir);
            braft::LogManager logs;
            braft::LogManagerOptions log_options;
            log_options.log_storage = &storage;
            log_options.configuration_manager = &configuration;
            ASSERT_EQ(0, logs.init(log_options));
            braft::ClosureQueue queue(false);
            queue.reset_first_index(1);
            std::vector<std::shared_ptr<http_res>> responses;
            std::vector<braft::LogEntry*> entries;
            for (int64_t i = 1; i <= 2; ++i) {
                auto req = std::make_shared<http_req>();
                req->start_ts = i;
                auto* entry = new braft::LogEntry();
                entry->AddRef();
                entry->id = braft::LogId(i, 1);
                entry->type = braft::ENTRY_TYPE_DATA;
                entry->data.append(req->to_json());
                entries.push_back(entry);
                if (follower) {
                    queue.append_pending_closure(nullptr);
                } else {
                    auto res = std::make_shared<http_res>(nullptr);
                    responses.push_back(res);
                    queue.append_pending_closure(new ReplicationClosure(req, res, state.get()));
                }
            }
            state->pending_writes = follower ? 0 : 2;
            auto stable = std::make_unique<Stable>();
            auto stable_done = stable->complete.get_future();
            logs.append_entries(&entries, stable.get());
            if (stable_done.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
                stable.release(); // the log manager may still invoke the callback
                FAIL() << "log append did not finish";
            }
            ASSERT_TRUE(stable->status().ok());
            braft::FSMCaller caller;
            braft::FSMCallerOptions options;
            options.log_manager = &logs;
            options.fsm = state.get();
            options.closure_queue = &queue;
            ASSERT_EQ(0, caller.init(options));
            ASSERT_EQ(0, caller.on_committed(2));
            ASSERT_EQ(0, caller.shutdown());
            caller.join();
            EXPECT_TRUE(state->storage_fault);
            EXPECT_TRUE(indexer.req_res_map.empty());
            braft::LogManagerStatus status;
            logs.get_status(&status);
            EXPECT_EQ(0, status.known_applied_index);
            for (int i = 0; i < 5000 && state->pending_writes.load() != 0; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            if (state->pending_writes.load() != 0) {
                ADD_FAILURE() << "pending closures did not finish";
                state.release(); // callbacks may still access the state
            } else {
                for (const auto& response : responses) {
                    EXPECT_EQ(503, response->status_code);
                    EXPECT_TRUE(response->terminal_error_claimed);
                }
            }
            logs.shutdown();
        }
        std::filesystem::remove_all(directory);
    }
}

TEST(RaftServerTest, RestoresBatchedIndexerStateByStoreStatusAndFailsClosed) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    ReplicationState replication_state(nullptr, &indexer, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);

    auto read_error_res = seed_indexer_request(indexer, 10, true);
    EXPECT_EQ(1, replication_state.restore_batched_indexer_state(StoreStatus::ERROR, "", false));
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());
    EXPECT_EQ(503, read_error_res->status_code);
    EXPECT_TRUE(read_error_res->final);

    seed_indexer_request(indexer, 20, false);
    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(StoreStatus::NOT_FOUND, "", false));
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());

    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    seed_indexer_request(source_indexer, 30, false);
    nlohmann::json valid_state;
    source_indexer.serialize_state(valid_state);

    auto partially_invalid_state = valid_state;
    partially_invalid_state["req_res_map"]["40"] = {
        {"start_ts", 40},
        {"req", 123},
    };
    seed_indexer_request(indexer, 20, false);
    EXPECT_EQ(1, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, partially_invalid_state.dump(), false));
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());

    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, valid_state.dump(), false));
    ASSERT_EQ(1, indexer.req_res_map.size());
    EXPECT_EQ(1, indexer.req_res_map.count(30));

    auto reload_failure_res = seed_indexer_request(indexer, 50, true);
    replication_state.read_caught_up = true;
    replication_state.write_caught_up = true;
    replication_state.snapshot_load_blocks_readiness = false;
    {
        std::unique_lock lifecycle_lock(indexer.lifecycle_mutex);
        EXPECT_EQ(-7, replication_state.fail_snapshot_load_unlocked(-7));
    }
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());
    EXPECT_EQ(503, reload_failure_res->status_code);
    EXPECT_TRUE(reload_failure_res->final);
    EXPECT_FALSE(replication_state.is_read_caught_up());
    EXPECT_FALSE(replication_state.is_write_caught_up());
}

TEST(RaftServerTest, RestoresEmptyAndLegacyNullBatchedIndexerState) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    nlohmann::json empty_state;
    source_indexer.serialize_state(empty_state);
    EXPECT_TRUE(empty_state["req_res_map"].is_object());
    EXPECT_TRUE(empty_state["req_res_map"].empty());

    BatchedIndexer restored_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    ReplicationState replication_state(nullptr, &restored_indexer, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);
    seed_indexer_request(restored_indexer, 10, false);
    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, empty_state.dump(), false));
    EXPECT_TRUE(restored_indexer.req_res_map.empty());
    EXPECT_TRUE(restored_indexer.queues[0].empty());

    auto legacy_empty_state = empty_state;
    legacy_empty_state["req_res_map"] = nullptr;
    seed_indexer_request(restored_indexer, 20, false);
    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, legacy_empty_state.dump(), false));
    EXPECT_TRUE(restored_indexer.req_res_map.empty());
    EXPECT_TRUE(restored_indexer.queues[0].empty());
}

TEST(RaftServerTest, ResolveNodesConfigWithHostNames) {
    ASSERT_EQ("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
              ReplicationState::resolve_node_hosts("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108"));

    // Test localhost resolution - should accept either IPv4 or IPv6
    std::string localhost_result1 = ReplicationState::resolve_node_hosts("localhost:8107:8108,localhost:7107:7108,localhost:6107:6108");
    ASSERT_TRUE(matches_either_ip_version(
        localhost_result1,
        "127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
        "[::1]:8107:8108,[::1]:7107:7108,[::1]:6107:6108"
    )) << "Result was: " << localhost_result1;

    std::string localhost_result2 = ReplicationState::resolve_node_hosts("localhost:8107:8108localhost:7107:7108,localhost:6107:6108");
    ASSERT_TRUE(matches_either_ip_version(
        localhost_result2,
        "localhost:8107:8108localhost:7107:7108,127.0.0.1:6107:6108",
        "localhost:8107:8108localhost:7107:7108,[::1]:6107:6108"
    )) << "Result was: " << localhost_result2;

    // hostname must be less than 64 chars
    ASSERT_EQ("",
              ReplicationState::resolve_node_hosts("typesense-node-2.typesense-service.typesense-"
                                                   "namespace.svc.cluster.local:6107:6108"));
}

TEST(RaftServerTest, ResolveNodesConfigFailsWhenAnyHostIsUnresolved) {
    // a partial peer list must not be returned, regardless of the position of the unresolved host
    ASSERT_EQ("", ReplicationState::resolve_node_hosts("non.existent.hostname.invalid:8107:8108,"
                                                       "127.0.0.1:7107:7108,127.0.0.1:6107:6108"));
    ASSERT_EQ("", ReplicationState::resolve_node_hosts("127.0.0.1:8107:8108,"
                                                       "non.existent.hostname.invalid:7107:7108,127.0.0.1:6107:6108"));
    ASSERT_EQ("", ReplicationState::resolve_node_hosts("127.0.0.1:8107:8108,127.0.0.1:7107:7108,"
                                                       "non.existent.hostname.invalid:6107:6108"));

    butil::EndPoint peering_endpoint;
    butil::str2endpoint("127.0.0.1", 8107, &peering_endpoint);
    ASSERT_EQ("", ReplicationState::to_nodes_config(peering_endpoint, 8108,
                                                    "127.0.0.1:8107:8108,non.existent.hostname.invalid:7107:7108"));
}

TEST(RaftServerTest, PersistedRaftState) {
    const std::string raft_dir = test_tmp_path("/raft_persisted_state");
    const std::string snapshot_dir = raft_dir + "/snapshot";
    std::filesystem::remove_all(raft_dir);

    // fresh node
    ASSERT_FALSE(ReplicationState::has_persisted_raft_state(raft_dir));

    // unfinished snapshot is neither a snapshot nor persisted state
    std::filesystem::create_directories(snapshot_dir + "/temp");
    ASSERT_FALSE(ReplicationState::has_completed_snapshot(snapshot_dir));
    ASSERT_FALSE(ReplicationState::has_persisted_raft_state(raft_dir));

    // log with an unfinished snapshot: start() must still initialize the db, since braft won't load a snapshot
    append_configuration_entry(raft_dir + "/log", "127.0.0.1:8107:8108");
    ASSERT_FALSE(ReplicationState::has_completed_snapshot(snapshot_dir));
    ASSERT_TRUE(ReplicationState::has_persisted_raft_state(raft_dir));

    // completed snapshot without log
    std::filesystem::remove_all(raft_dir);
    save_snapshot(snapshot_dir, "127.0.0.1:8107:8108");
    ASSERT_TRUE(ReplicationState::has_completed_snapshot(snapshot_dir));
    ASSERT_TRUE(ReplicationState::has_persisted_raft_state(raft_dir));

    std::filesystem::remove_all(raft_dir);
}

TEST(RaftServerTest, LogBackedMembershipIsRecoveredWithoutInitialConf) {
    const std::string raft_dir = test_tmp_path("/raft_log_membership");
    const std::string peer = "127.0.0.1:18107:18108";
    std::filesystem::remove_all(raft_dir);

    append_configuration_entry(raft_dir + "/log", peer);
    // an interrupted snapshot must not get in the way of recovery from the log
    std::filesystem::create_directories(raft_dir + "/snapshot/temp");
    ASSERT_TRUE(ReplicationState::has_persisted_raft_state(raft_dir));

    {
        TestRaftNode raft_node;
        ASSERT_EQ(0, raft_node.start(peer, raft_dir));
        ASSERT_TRUE(raft_node.wait_for_leader());
        ASSERT_EQ(std::vector<braft::PeerId>{braft::PeerId(peer)}, raft_node.peers());
    }

    std::filesystem::remove_all(raft_dir);
}

TEST(RaftServerTest, SnapshotBackedMembershipIsRecoveredWithoutInitialConf) {
    const std::string raft_dir = test_tmp_path("/raft_snapshot_membership");
    const std::string peer = "127.0.0.1:18117:18118";
    std::filesystem::remove_all(raft_dir);

    save_snapshot(raft_dir + "/snapshot", peer);
    ASSERT_TRUE(ReplicationState::has_persisted_raft_state(raft_dir));

    {
        TestRaftNode raft_node;
        ASSERT_EQ(0, raft_node.start(peer, raft_dir));
        ASSERT_TRUE(raft_node.wait_for_leader());
        ASSERT_EQ(std::vector<braft::PeerId>{braft::PeerId(peer)}, raft_node.peers());
    }

    std::filesystem::remove_all(raft_dir);
}

TEST(RaftServerTest, FailedPeerRefreshPreservesLiveMembership) {
    const std::string raft_dir = test_tmp_path("/raft_refresh_membership");
    const std::string peer = "127.0.0.1:18127:18128";
    std::filesystem::remove_all(raft_dir);

    append_configuration_entry(raft_dir + "/log", peer);

    TestRaftNode raft_node;
    ASSERT_EQ(0, raft_node.start(peer, raft_dir));
    ASSERT_TRUE(raft_node.wait_for_leader());

    auto& config = Config::get_instance();
    ReplicationState replication_state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);
    replication_state.node = raft_node.node;
    std::atomic<bool> reset_peers_on_error(true);

    // unresolved host
    replication_state.refresh_nodes(ReplicationState::to_nodes_config(raft_node.node->node_id().peer_id.addr, 18128,
                                    peer + ",non.existent.hostname.invalid:7107:7108"), 1, reset_peers_on_error);
    // braft parses the valid prefix of a malformed config: applying it would remove this node from the cluster
    replication_state.refresh_nodes("127.0.0.1:19107:19108,bad:peer:entry:here", 1, reset_peers_on_error);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    replication_state.node = nullptr;

    ASSERT_TRUE(raft_node.node->is_leader());
    ASSERT_EQ(std::vector<braft::PeerId>{braft::PeerId(peer)}, raft_node.peers());

    std::filesystem::remove_all(raft_dir);
}

TEST(RaftServerTest, FreshNodeWaitsForUnresolvedHostsUntilQuit) {
    const std::string raft_dir = test_tmp_path("/raft_fresh_node");
    std::filesystem::remove_all(raft_dir);

    auto& config = Config::get_instance();
    ReplicationState replication_state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);

    butil::EndPoint peering_endpoint;
    butil::str2endpoint("127.0.0.1", 8107, &peering_endpoint);

    std::atomic<bool> quit_abruptly(false);
    auto start = std::async(std::launch::async, [&]() {
        return replication_state.start(peering_endpoint, 8108, 5000, 4194304, raft_dir,
                                       "127.0.0.1:8107:8108,non.existent.hostname.invalid:7107:7108",
                                       quit_abruptly);
    });

    // must not start with a partial peer list
    ASSERT_EQ(std::future_status::timeout, start.wait_for(std::chrono::milliseconds(500)));

    quit_abruptly = true;
    ASSERT_EQ(std::future_status::ready, start.wait_for(std::chrono::seconds(3)));
    ASSERT_EQ(-1, start.get());
    ASSERT_EQ(nullptr, replication_state.node);

    std::filesystem::remove_all(raft_dir);
}

TEST(RaftServerTest, ResolveNodesConfigWithIPv6) {
    // Basic IPv6 addresses
    ASSERT_EQ("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108",
              ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108"));

    // IPv6 with IPv4 mixed
    ASSERT_EQ("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108",
              ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108"));

    // IPv6 localhost
    ASSERT_EQ("[::1]:8107:8108",
              ReplicationState::resolve_node_hosts("[::1]:8107:8108"));

    // Malformed IPv6 inputs should be passed through unchanged
    ASSERT_EQ("[2001:db8::1:8107:8108",  // Missing closing bracket
              ReplicationState::resolve_node_hosts("[2001:db8::1:8107:8108"));

    // IPv6 with zone index
    ASSERT_EQ("[fe80::1%eth0]:8107:8108",
              ReplicationState::resolve_node_hosts("[fe80::1%eth0]:8107:8108"));

    // Test with real IPv6 hostname resolution - need to skip if resolution fails
    std::string ipv6_result = ReplicationState::resolve_node_hosts("ipv6.test-ipv6.com:8107:8108");
    if (!ipv6_result.empty()) {
        EXPECT_TRUE(ipv6_result.find('[') == 0);  // Should start with '[' for IPv6
        EXPECT_TRUE(ipv6_result.find("]:8107:8108") != std::string::npos);
    }
}

TEST(Hostname2IPStrTest, IPAddresses) {
    // Test IPv4 addresses - should return unchanged
    ASSERT_EQ("127.0.0.1", ReplicationState::hostname2ipstr("127.0.0.1"));
    ASSERT_EQ("192.168.1.1", ReplicationState::hostname2ipstr("192.168.1.1"));

    // Test IPv6 addresses - should return unchanged if already in brackets
    ASSERT_EQ("[::1]", ReplicationState::hostname2ipstr("[::1]"));
    ASSERT_EQ("[2001:db8::1]", ReplicationState::hostname2ipstr("[2001:db8::1]"));
}

TEST(Hostname2IPStrTest, Localhost) {
    std::string result = ReplicationState::hostname2ipstr("localhost");

    // Should resolve to either 127.0.0.1 or [::1]
    ASSERT_TRUE(result == "127.0.0.1" || result == "[::1]")
        << "localhost resolved to: " << result;
}

TEST(Hostname2IPStrTest, InvalidHostnames) {
    // Test hostname that's too long (>64 chars)
    std::string long_hostname(65, 'a');
    ASSERT_EQ("", ReplicationState::hostname2ipstr(long_hostname));

    // Test non-existent hostname
    ASSERT_EQ("", ReplicationState::hostname2ipstr("non.existent.hostname.local"));
}

TEST(Hostname2IPStrTest, PublicHostnames) {
    // Test IPv6-only hostname resolution
    std::string ipv6_result = ReplicationState::hostname2ipstr("ipv6.test-ipv6.com");
    if (!ipv6_result.empty() && ipv6_result != "ipv6.test-ipv6.com") {
        EXPECT_TRUE(is_ipv6_with_brackets(ipv6_result))
            << "ipv6.test-ipv6.com did not resolve to IPv6: " << ipv6_result;
    }

    // Test IPv4-only hostname resolution
    std::string ipv4_result = ReplicationState::hostname2ipstr("ipv4.test-ipv6.com");
    if (!ipv4_result.empty() && ipv4_result != "ipv4.test-ipv6.com") {
        EXPECT_TRUE(is_ipv4(ipv4_result))
            << "ipv4.test-ipv6.com did not resolve to IPv4: " << ipv4_result;
    }
}
