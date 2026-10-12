#include <gtest/gtest.h>
#include <braft/log.h>
#include <braft/raft.h>
#include <braft/snapshot.h>
#include <brpc/server.h>
#include <chrono>
#include <filesystem>
#include <future>
#include <string>

#define private public
#include "raft_server.h"
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
        void on_apply(braft::Iterator& iter) override {
            for(; iter.valid(); iter.next()) {
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
    const std::string raft_dir = "/tmp/typesense_test/raft_persisted_state";
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
    const std::string raft_dir = "/tmp/typesense_test/raft_log_membership";
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
    const std::string raft_dir = "/tmp/typesense_test/raft_snapshot_membership";
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
    const std::string raft_dir = "/tmp/typesense_test/raft_refresh_membership";
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
    const std::string raft_dir = "/tmp/typesense_test/raft_fresh_node";
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
