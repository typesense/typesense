#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <string>
#include <thread>

#define private public
#include "raft_server.h"
#undef private

namespace {
    class NoopStateMachine : public braft::StateMachine {
    public:
        void on_apply(braft::Iterator& iter) override {
            for(; iter.valid(); iter.next()) {
                braft::AsyncClosureGuard closure_guard(iter.done());
            }
        }
    };

    braft::NodeOptions make_node_options(const std::string& raft_dir, const std::string& conf,
                                         const int election_timeout_ms, braft::StateMachine* fsm) {
        braft::NodeOptions options;
        options.initial_conf.parse_from(conf);
        options.election_timeout_ms = election_timeout_ms;
        options.fsm = fsm;
        options.node_owns_fsm = false;
        options.snapshot_interval_s = -1;
        options.disable_cli = true;
        options.log_uri = "local://" + raft_dir + "/log";
        options.raft_meta_uri = "local://" + raft_dir + "/meta";
        options.snapshot_uri = "local://" + raft_dir + "/snapshot";
        return options;
    }

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

    // Test non-existent hostname - implementation returns original hostname
    ASSERT_EQ("non.existent.hostname.local",
              ReplicationState::hostname2ipstr("non.existent.hostname.local"));
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

TEST(RaftServerTest, UncommittedWriteReleasesPendingWritesOnStepDown) {
    butil::AtExitManager exit_manager;
    const std::string raft_dir_a = "/tmp/typesense_test/raft_uncommitted_write/a";
    const std::string raft_dir_b = "/tmp/typesense_test/raft_uncommitted_write/b";
    system("rm -rf /tmp/typesense_test/raft_uncommitted_write && mkdir -p /tmp/typesense_test/raft_uncommitted_write");

    butil::EndPoint endpoint_a;
    butil::EndPoint endpoint_b;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 18917, &endpoint_a));
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 18918, &endpoint_b));
    const std::string conf = "127.0.0.1:18917:0,127.0.0.1:18918:0";

    brpc::Server rpc_server_a;
    brpc::Server rpc_server_b;
    ASSERT_EQ(0, braft::add_service(&rpc_server_a, endpoint_a));
    ASSERT_EQ(0, braft::add_service(&rpc_server_b, endpoint_b));
    ASSERT_EQ(0, rpc_server_a.Start(endpoint_a, nullptr));
    ASSERT_EQ(0, rpc_server_b.Start(endpoint_b, nullptr));

    auto& config = Config::get_instance();
    ThreadPool thread_pool(1);
    HttpServer server("test", "127.0.0.1", 0, "", "", 0, false, {}, &thread_pool);
    ReplicationState replication_state(&server, nullptr, nullptr, nullptr, &thread_pool, nullptr, false,
                                       &config, 1, 1);
    replication_state.raft_dir_path = raft_dir_a;

    // b never times out first, so a is elected leader
    NoopStateMachine fsm_b;
    braft::Node* node_b = new braft::Node("default_group", braft::PeerId(endpoint_b, 0));
    ASSERT_EQ(0, node_b->init(make_node_options(raft_dir_b, conf, 60000, &fsm_b)));

    braft::Node* node_a = new braft::Node("default_group", braft::PeerId(endpoint_a, 0));
    ASSERT_EQ(0, node_a->init(make_node_options(raft_dir_a, conf, 1000, &replication_state)));
    replication_state.node = node_a;

    for(size_t i = 0; i < 200 && !replication_state.has_leader_term(); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(replication_state.has_leader_term());

    // with the only follower gone, a's next entry can never reach a majority
    node_b->shutdown(nullptr);
    node_b->join();

    auto req = std::make_shared<http_req>();
    req->http_method = "POST";
    req->path_without_query = "/collections/products/documents";
    req->body = R"({"id":"0"})";
    auto res = std::make_shared<http_res>(nullptr);

    ASSERT_TRUE(node_a->is_leader());
    replication_state.write(req, res);
    ASSERT_EQ(1, replication_state.pending_writes.load());

    // braft fails the entry on step-down and destroys its ReplicationClosure, releasing its hold on req
    for(size_t i = 0; i < 200 && req.use_count() > 1; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_FALSE(node_a->is_leader());
    ASSERT_EQ(1, req.use_count());

    // shutdown() waits for this to reach 0
    EXPECT_EQ(0, replication_state.pending_writes.load());

    replication_state.node = nullptr;
    node_a->shutdown(nullptr);
    node_a->join();
    delete node_a;
    delete node_b;

    rpc_server_a.Stop(0);
    rpc_server_b.Stop(0);
    rpc_server_a.Join();
    rpc_server_b.Join();
    thread_pool.shutdown();
}
