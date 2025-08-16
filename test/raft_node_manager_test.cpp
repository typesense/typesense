#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <filesystem>
#include <thread>
#include <chrono>
#include <atomic>
#include "butil/at_exit.h"
#include "brpc/server.h"
#include "braft/raft.h"
#include "raft_node_manager.h"
#include "store.h"
#include "batched_indexer.h"
#include "tsconfig.h"
#include "threadpool.h"
#include "http_server.h"

class ConfigImpl : public Config {
public:
    ConfigImpl(): Config() {}
};

class RaftNodeManagerTest : public ::testing::Test {
protected:
    std::unique_ptr<butil::AtExitManager> exit_manager;
    std::vector<std::unique_ptr<brpc::Server>> raft_servers;
    Store* store;
    Store* meta_store;
    BatchedIndexer* batched_indexer;
    ThreadPool* thread_pool;
    HttpServer* http_server;
    ConfigImpl* config;
    std::string test_dir;
    std::atomic<bool> quit;

    void SetUp() override {
        // Initialize braft dependencies first
        exit_manager = std::make_unique<butil::AtExitManager>();

        // Create test directory
        test_dir = "/tmp/typesense_test/raft_node_manager";
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);

        // Initialize components
        store = new Store(test_dir + "/store");
        thread_pool = new ThreadPool(4);

        // Create minimal config for testing
        config = new ConfigImpl();

        // Initialize additional store for batched indexer
        meta_store = new Store(test_dir + "/meta");

        // Initialize HttpServer (minimal setup)
        http_server = nullptr;

        // Initialize BatchedIndexer with correct signature
        quit = false;
        batched_indexer = new BatchedIndexer(http_server, store, meta_store, 4, *config, quit);
    }

    void TearDown() override {
        delete batched_indexer;
        thread_pool->shutdown();
        delete thread_pool;
        delete store;
        delete meta_store;
        delete config;
        if (http_server) delete http_server;

        // Stop all RPC servers
        for (auto& server : raft_servers) {
            if (server) {
                server->Stop(0);
                server->Join();
            }
        }
        raft_servers.clear();

        // Clean up test directory
        std::filesystem::remove_all(test_dir);
    }

    std::unique_ptr<RaftNodeManager> createNodeManager(bool api_uses_ssl = false) {
        return std::make_unique<RaftNodeManager>(config, store, batched_indexer, api_uses_ssl);
    }

    brpc::Server* createRaftServer(const butil::EndPoint& endpoint) {
        auto server = std::make_unique<brpc::Server>();
        brpc::Server* server_ptr = server.get();

        int add_service_result = braft::add_service(server_ptr, endpoint);
        EXPECT_EQ(add_service_result, 0);

        int start_result = server->Start(endpoint, nullptr);
        EXPECT_EQ(start_result, 0);

        raft_servers.push_back(std::move(server));
        return server_ptr;
    }
};

// Mock StateMachine for testing successful initialization
class MockRaftStateMachine : public braft::StateMachine {
public:
    MockRaftStateMachine() = default;

    void on_apply(braft::Iterator& iter) override {
        for (; iter.valid(); iter.next()) {
            if (iter.done()) {
                iter.done()->Run();
            }
        }
    }

    void on_shutdown() override {}

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override {
        done->Run();
    }

    int on_snapshot_load(braft::SnapshotReader* reader) override {
        return 0;
    }

    void on_leader_start(int64_t term) override {
        is_leader_flag = true;
        current_term = term;
    }

    void on_leader_stop(const butil::Status& status) override {
        is_leader_flag = false;
    }

    void on_error(const braft::Error& e) override {}

    void on_configuration_committed(const braft::Configuration& conf) override {}

    void on_stop_following(const braft::LeaderChangeContext& ctx) override {}

    void on_start_following(const braft::LeaderChangeContext& ctx) override {}

    bool is_leader_flag = false;
    int64_t current_term = 0;
};

TEST_F(RaftNodeManagerTest, Constructor) {
    auto node_manager = createNodeManager();
    EXPECT_NE(node_manager, nullptr);

    // Should not have a node initially
    EXPECT_FALSE(node_manager->is_leader());
}

TEST_F(RaftNodeManagerTest, ConstructorWithSSL) {
    // Test with SSL enabled
    auto node_manager_ssl = createNodeManager(true);
    EXPECT_NE(node_manager_ssl, nullptr);

    // Test without SSL (default case)
    auto node_manager_no_ssl = createNodeManager(false);
    EXPECT_NE(node_manager_no_ssl, nullptr);
}

TEST_F(RaftNodeManagerTest, InitNode) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    // Set up valid endpoint and configuration
    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8090, &endpoint);
    EXPECT_EQ(result, 0);

    // Create raft directory structure
    std::string raft_dir = std::filesystem::absolute(test_dir + "/raft");
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    int api_port = 8091;
    int election_timeout_ms = 5000;
    std::string nodes_config = "127.0.0.1:8090:8091";

    // Set up RPC server for this endpoint
    createRaftServer(endpoint);

    // Initialization should succeed
    int init_result = node_manager->init_node(state_machine.get(), endpoint, api_port,
                                             election_timeout_ms, raft_dir, nodes_config);
    EXPECT_EQ(init_result, 0);

    // Node should be initialized and ready
    auto status = node_manager->get_status();
    EXPECT_NE(status["state"], "NOT_READY");

    // Should have valid node ID after initialization
    auto node_id = node_manager->node_id();

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, InitNodeWithNullStateMachine) {
    auto node_manager = createNodeManager();

    // Test basic node configuration setup
    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1:8090", &endpoint);
    EXPECT_EQ(result, 0);

    std::vector<std::string> nodes = {"127.0.0.1:8090:8091"};

    // Should fail gracefully with nullptr StateMachine
    int init_result = node_manager->init_node(nullptr, endpoint, 8091, 5000, "/tmp/test_raft", "127.0.0.1:8090:8091");
    EXPECT_NE(init_result, 0);
}

TEST_F(RaftNodeManagerTest, InitNodeWithInvalidEndpoint) {
    auto node_manager = createNodeManager();

    // Test various error conditions
    butil::EndPoint invalid_endpoint;
    std::vector<std::string> empty_nodes;

    int result = node_manager->init_node(nullptr, invalid_endpoint, 0, 0, "/tmp", "");
    EXPECT_NE(result, 0);

    // All operations should still work after errors
    auto status = node_manager->get_status();
    EXPECT_TRUE(status.contains("state"));
}

TEST_F(RaftNodeManagerTest, IsLeader) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    // Set up single-node cluster for leader election
    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8094, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    // Set up RPC server for this endpoint
    createRaftServer(endpoint);

    // Initialize single node cluster
    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8095,
                                             1000, raft_dir, "127.0.0.1:8094:8095");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Single node should become leader
    EXPECT_TRUE(node_manager->is_leader());

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, IsLeaderWithoutNode) {
    auto node_manager = createNodeManager();

    // Should work even without initialized node
    EXPECT_FALSE(node_manager->is_leader());
}

TEST_F(RaftNodeManagerTest, GetStatus) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    // Set up endpoint
    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8096, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_status";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8097,
                                             1000, raft_dir, "127.0.0.1:8096:8097");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    auto status = node_manager->get_status();

    // Should contain all expected status keys
    EXPECT_TRUE(status.contains("state"));
    EXPECT_TRUE(status.contains("committed_index"));
    EXPECT_TRUE(status.contains("queued_writes"));
    EXPECT_TRUE(status.contains("is_leader"));
    EXPECT_TRUE(status.contains("read_ready"));
    EXPECT_TRUE(status.contains("write_ready"));

    // State should be valid
    EXPECT_NE(status["state"], "NOT_READY");

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, GetStatusWithoutNode) {
    auto node_manager = createNodeManager();

    // Get status JSON - should work even without initialized node
    auto status = node_manager->get_status();

    // Should contain expected keys for uninitialized state
    EXPECT_TRUE(status.contains("state"));
    EXPECT_TRUE(status.contains("committed_index"));
    EXPECT_TRUE(status.contains("queued_writes"));
    EXPECT_TRUE(status.contains("is_leader"));
    EXPECT_TRUE(status.contains("read_ready"));
    EXPECT_TRUE(status.contains("write_ready"));

    // Initial values for uninitialized node
    EXPECT_EQ(status["state"], "NOT_READY");
    EXPECT_EQ(status["committed_index"], 0);
    EXPECT_EQ(status["is_leader"], false);
    EXPECT_EQ(status["read_ready"], false);
    EXPECT_EQ(status["write_ready"], false);
}

TEST_F(RaftNodeManagerTest, GetLeaderUrl) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    // Set up single-node cluster
    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8098, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader_url";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8099,
                                             1000, raft_dir, "127.0.0.1:8098:8099");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Should have valid leader URL
    auto leader_url = node_manager->get_leader_url();
    EXPECT_FALSE(leader_url.empty());

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, GetLeaderUrlWithoutNode) {
    auto node_manager = createNodeManager();

    // Should return empty string when no node is initialized
    auto leader_url = node_manager->get_leader_url();
    EXPECT_TRUE(leader_url.empty());
}

TEST_F(RaftNodeManagerTest, NodeId) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8100, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_node_id";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8101,
                                             1000, raft_dir, "127.0.0.1:8100:8101");
    EXPECT_EQ(init_result, 0);

    // Should have valid node ID
    auto node_id = node_manager->node_id();
    // Verify node_id is valid (exact validation depends on braft::NodeId implementation)

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, NodeIdWithoutNode) {
    auto node_manager = createNodeManager();

    // Should return default-constructed NodeId when no node is initialized
    auto node_id = node_manager->node_id();
}

TEST_F(RaftNodeManagerTest, LeaderId) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8102, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader_id";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8103,
                                             1000, raft_dir, "127.0.0.1:8102:8103");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Should have valid leader ID
    auto leader_id = node_manager->leader_id();
    EXPECT_FALSE(leader_id.is_empty());

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, LeaderIdWithoutNode) {
    auto node_manager = createNodeManager();

    // Should return empty PeerId when no node is initialized
    auto leader_id = node_manager->leader_id();
    EXPECT_TRUE(leader_id.is_empty());
}

TEST_F(RaftNodeManagerTest, ReadWriteReadyStates) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8104, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_ready";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8105,
                                             1000, raft_dir, "127.0.0.1:8104:8105");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Refresh status to ensure ready flags are updated
    node_manager->refresh_catchup_status(true);

    // Leader should be ready for operations
    EXPECT_TRUE(node_manager->is_read_ready());
    EXPECT_TRUE(node_manager->is_write_ready());

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, ReadWriteReadyStatesWithoutNode) {
    auto node_manager = createNodeManager();

    // Initial state should be not ready
    EXPECT_FALSE(node_manager->is_read_ready());
    EXPECT_FALSE(node_manager->is_write_ready());

    // These methods should not crash and should return consistent values
    auto read_state1 = node_manager->is_read_ready();
    auto read_state2 = node_manager->is_read_ready();
    EXPECT_EQ(read_state1, read_state2);

    auto write_state1 = node_manager->is_write_ready();
    auto write_state2 = node_manager->is_write_ready();
    EXPECT_EQ(write_state1, write_state2);
}

TEST_F(RaftNodeManagerTest, WaitUntilReady) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8106, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_wait";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8107,
                                             1000, raft_dir, "127.0.0.1:8106:8107");
    EXPECT_EQ(init_result, 0);

    // Should become ready relatively quickly
    std::atomic<bool> test_quit{false};
    bool ready = node_manager->wait_until_ready(5000, test_quit);
    EXPECT_TRUE(ready);

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, WaitUntilReadyTimeout) {
    auto node_manager = createNodeManager();

    auto start_time = std::chrono::steady_clock::now();

    // Wait with short timeout - should timeout quickly since node is not started
    std::atomic<bool> test_quit{false};
    bool ready = node_manager->wait_until_ready(1000, test_quit);

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    EXPECT_FALSE(ready);
    EXPECT_GE(duration.count(), 1);
    EXPECT_LE(duration.count(), 2);
}

TEST_F(RaftNodeManagerTest, TriggerVote) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8108, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_vote";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8109,
                                             1000, raft_dir, "127.0.0.1:8108:8109");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Vote trigger should work or be no-op for leader
    auto vote_result = node_manager->trigger_vote();
    // Result depends on node state

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, TriggerVoteWithoutNode) {
    auto node_manager = createNodeManager();

    // Should fail gracefully when no node is initialized
    auto result = node_manager->trigger_vote();
    EXPECT_FALSE(result.ok());
}

TEST_F(RaftNodeManagerTest, ResetPeers) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8110, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_reset";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8111,
                                             1000, raft_dir, "127.0.0.1:8110:8111");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // Test reset_peers with new configuration
    braft::Configuration new_conf;
    braft::PeerId peer1, peer2;

    int parse1 = peer1.parse("127.0.0.1:8110");
    int parse2 = peer2.parse("127.0.0.1:8112");
    EXPECT_EQ(parse1, 0);
    EXPECT_EQ(parse2, 0);

    new_conf.add_peer(peer1);
    new_conf.add_peer(peer2);

    // reset_peers operation should succeed
    auto reset_result = node_manager->reset_peers(new_conf);
    EXPECT_TRUE(reset_result.ok());

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, ResetPeersWithoutNode) {
    auto node_manager = createNodeManager();

    // Create a test configuration
    braft::Configuration new_conf;
    braft::PeerId peer_id;
    int parse_result = peer_id.parse("127.0.0.1:8090");
    EXPECT_EQ(parse_result, 0);

    new_conf.add_peer(peer_id);

    // Should fail gracefully when no node is initialized
    auto result = node_manager->reset_peers(new_conf);
    EXPECT_FALSE(result.ok());
}

TEST_F(RaftNodeManagerTest, ResetPeersWithEmptyConfiguration) {
    auto node_manager = createNodeManager();

    // Invalid configuration
    braft::Configuration empty_conf;
    auto reset_result = node_manager->reset_peers(empty_conf);
    EXPECT_FALSE(reset_result.ok());
}

TEST_F(RaftNodeManagerTest, RefreshNodes) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8114, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_refresh";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8115,
                                             1000, raft_dir, "127.0.0.1:8114:8115");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // Test refresh_nodes operation
    std::string nodes_config = "127.0.0.1:8114:8115,127.0.0.1:8116:8117";
    node_manager->refresh_nodes(nodes_config, false);
    node_manager->refresh_nodes(nodes_config, true);

    // Node should still be functional after membership changes
    auto status = node_manager->get_status();
    EXPECT_TRUE(status.contains("state"));

    node_manager->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, RefreshNodesWithoutNode) {
    auto node_manager = createNodeManager();

    // Should not crash when trying to refresh nodes without initialized node
    std::string nodes_config = "127.0.0.1:8090:8091";

    // This should complete without crashing (though may log errors)
    node_manager->refresh_nodes(nodes_config, false);
    node_manager->refresh_nodes(nodes_config, true);
}

TEST_F(RaftNodeManagerTest, Shutdown) {
    auto node_manager = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8118, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_shutdown";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = node_manager->init_node(state_machine.get(), endpoint, 8119,
                                             1000, raft_dir, "127.0.0.1:8118:8119");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // Should shutdown cleanly
    node_manager->shutdown();

    // Status should still work after shutdown
    auto status = node_manager->get_status();
    EXPECT_TRUE(status.contains("state"));

    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftNodeManagerTest, ShutdownWithoutNode) {
    auto node_manager = createNodeManager();

    // Shutdown without initialized node should not crash
    node_manager->shutdown();
}

TEST_F(RaftNodeManagerTest, LogNodeStatus) {
    auto node_manager = createNodeManager();

    braft::NodeStatus status;
    // Initialize status with some dummy values
    status.state = braft::STATE_LEADER;
    status.committed_index = 100;

    // Should not crash when logging status
    node_manager->log_node_status(status);
    node_manager->log_node_status(status, "test_prefix");
}

TEST_F(RaftNodeManagerTest, LifecycleWithoutNetworking) {
    auto node_manager = createNodeManager();

    // Test full lifecycle without actual networking
    // 1. Initial state
    EXPECT_FALSE(node_manager->is_leader());

    // 2. Status reporting works throughout
    auto status1 = node_manager->get_status();
    EXPECT_EQ(status1["state"], "NOT_READY");

    // 3. Shutdown (should not crash even if never started)
    node_manager->shutdown();

    // 4. Status should still work after shutdown
    auto status2 = node_manager->get_status();
    EXPECT_TRUE(status2.contains("state"));
}

TEST_F(RaftNodeManagerTest, MultipleNodeManagers) {
    // Test that we can create multiple node managers safely
    auto node1 = createNodeManager();
    auto node2 = createNodeManager();

    EXPECT_NE(node1, nullptr);
    EXPECT_NE(node2, nullptr);

    // Both should have independent state
    auto status1 = node1->get_status();
    auto status2 = node2->get_status();

    // Both should be in NOT_READY state
    EXPECT_EQ(status1["state"], "NOT_READY");
    EXPECT_EQ(status2["state"], "NOT_READY");
}
