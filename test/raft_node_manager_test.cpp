#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <filesystem>
#include <thread>
#include <chrono>
#include <atomic>
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
    Store* store;
    Store* meta_store;
    BatchedIndexer* batched_indexer;
    ThreadPool* thread_pool;
    HttpServer* http_server;
    ConfigImpl* config;
    std::string test_dir;
    std::atomic<bool> quit;

    void SetUp() override {
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
        http_server = nullptr;  // Will create per test if needed

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

        // Clean up test directory
        std::filesystem::remove_all(test_dir);
    }

    std::unique_ptr<RaftNodeManager> createNodeManager(bool api_uses_ssl = false) {
        return std::make_unique<RaftNodeManager>(config, store, batched_indexer, api_uses_ssl);
    }
};

TEST_F(RaftNodeManagerTest, ConstructorInitializesCorrectly) {
    LOG(INFO) << "Creating node manager...";
    auto node_manager = createNodeManager();
    LOG(INFO) << "Node manager created.";

    EXPECT_NE(node_manager, nullptr);
    LOG(INFO) << "About to call is_leader()...";

    // Should not have a node initially (is_leader should work even without node)
    EXPECT_FALSE(node_manager->is_leader());
    LOG(INFO) << "is_leader() call completed.";
}

TEST_F(RaftNodeManagerTest, InitialStatusReporting) {
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

TEST_F(RaftNodeManagerTest, LeaderUrlWithoutNode) {
    auto node_manager = createNodeManager();

    // Should return empty string when no node is initialized
    auto leader_url = node_manager->get_leader_url();
    EXPECT_TRUE(leader_url.empty());
}

TEST_F(RaftNodeManagerTest, NodeIdWithoutNode) {
    auto node_manager = createNodeManager();

    // Should return default-constructed NodeId when no node is initialized
    auto node_id = node_manager->node_id();
    // braft::NodeId should be valid even when default constructed
}

TEST_F(RaftNodeManagerTest, LeaderIdWithoutNode) {
    auto node_manager = createNodeManager();

    // Should return empty PeerId when no node is initialized
    auto leader_id = node_manager->leader_id();
    EXPECT_TRUE(leader_id.is_empty());
}

TEST_F(RaftNodeManagerTest, NodeInitializationProcess) {
    auto node_manager = createNodeManager();

    // Test basic node configuration setup
    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1:8090", &endpoint);
    EXPECT_EQ(result, 0);

    std::vector<std::string> nodes = {"127.0.0.1:8090:8091"};

    // Note: init_node requires a StateMachine, so we'll test that it handles nullptr gracefully
    // This tests the error handling path
    int init_result = node_manager->init_node(nullptr, endpoint, 8091, 5000, "/tmp/test_raft", "127.0.0.1:8090:8091");

    // Should fail gracefully with nullptr StateMachine
    EXPECT_NE(init_result, 0);
}

TEST_F(RaftNodeManagerTest, LeaderStatusWithoutNode) {
    auto node_manager = createNodeManager();

    // is_leader should work even without initialized node
    EXPECT_FALSE(node_manager->is_leader());
}

TEST_F(RaftNodeManagerTest, ShutdownWithoutNode) {
    auto node_manager = createNodeManager();

    // Shutdown without initialized node should not crash
    node_manager->shutdown();
    // Should complete without error
}

TEST_F(RaftNodeManagerTest, WaitUntilReadyTimeout) {
    auto node_manager = createNodeManager();

    auto start_time = std::chrono::steady_clock::now();

    // Wait with short timeout - should timeout quickly since node is not started
    std::atomic<bool> test_quit{false};
    bool ready = node_manager->wait_until_ready(1000, test_quit); // 1 second timeout

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    EXPECT_FALSE(ready); // Should timeout
    EXPECT_GE(duration.count(), 1); // Should take at least 1 second
    EXPECT_LE(duration.count(), 2); // Should not take much longer than 1 second
}

TEST_F(RaftNodeManagerTest, TriggerVoteWithoutNode) {
    auto node_manager = createNodeManager();

    // Should fail gracefully when no node is initialized
    auto result = node_manager->trigger_vote();
    EXPECT_FALSE(result.ok());
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

TEST_F(RaftNodeManagerTest, NodeStatusCallback) {
    auto node_manager = createNodeManager();

    // Test getting node status (low-level braft call)
    braft::NodeStatus status;

    // This should work even without initialized node - will set status appropriately
    node_manager->get_status(&status);

    // The status should be in some valid state (even if uninitialized)
    // The exact state depends on braft's behavior for uninitialized nodes
}

TEST_F(RaftNodeManagerTest, RefreshNodesWithoutNode) {
    auto node_manager = createNodeManager();

    // Should not crash when trying to refresh nodes without initialized node
    std::string nodes_config = "127.0.0.1:8090:8091";

    // This should complete without crashing (though may log errors)
    node_manager->refresh_nodes(nodes_config, false);
    node_manager->refresh_nodes(nodes_config, true);
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

TEST_F(RaftNodeManagerTest, ReadWriteReadyStates) {
    auto node_manager = createNodeManager();

    // Initial state should be not ready
    EXPECT_FALSE(node_manager->is_read_ready());
    EXPECT_FALSE(node_manager->is_write_ready());

    // These methods should not crash and should return consistent values
    auto read_state1 = node_manager->is_read_ready();
    auto read_state2 = node_manager->is_read_ready();
    EXPECT_EQ(read_state1, read_state2); // Should be consistent

    auto write_state1 = node_manager->is_write_ready();
    auto write_state2 = node_manager->is_write_ready();
    EXPECT_EQ(write_state1, write_state2); // Should be consistent
}

TEST_F(RaftNodeManagerTest, ApiSslConfiguration) {
    // Test with SSL enabled
    auto node_manager_ssl = createNodeManager(true);

    // Should initialize correctly with SSL
    EXPECT_NE(node_manager_ssl, nullptr);

    // Status should work the same
    auto status = node_manager_ssl->get_status();
    EXPECT_TRUE(status.contains("state"));

    // Leader URL should use https protocol when SSL is enabled
    // (though it will be empty without a leader)
    auto leader_url = node_manager_ssl->get_leader_url();
    EXPECT_TRUE(leader_url.empty()); // No leader, so empty

    // Test without SSL (default case)
    auto node_manager_no_ssl = createNodeManager(false);
    EXPECT_NE(node_manager_no_ssl, nullptr);
}

TEST_F(RaftNodeManagerTest, NodeManagerLifecycle) {
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

    // Both should be safely destructible
}

TEST_F(RaftNodeManagerTest, ErrorHandling) {
    auto node_manager = createNodeManager();

    // Test various error conditions don't crash

    // Invalid endpoint
    butil::EndPoint invalid_endpoint;
    std::vector<std::string> empty_nodes;

    int result = node_manager->init_node(nullptr, invalid_endpoint, 0, 0, "/tmp", "");
    EXPECT_NE(result, 0); // Should fail

    // Invalid configuration
    braft::Configuration empty_conf;
    auto reset_result = node_manager->reset_peers(empty_conf);
    EXPECT_FALSE(reset_result.ok()); // Should fail

    // All operations should still work after errors
    auto status = node_manager->get_status();
    EXPECT_TRUE(status.contains("state"));
}
