#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <filesystem>
#include <atomic>
#include <thread>
#include <chrono>
#include "butil/at_exit.h"
#include "brpc/server.h"
#include "braft/raft.h"
#include "raft_server.h"
#include "http_server.h"
#include "batched_indexer.h"
#include "store.h"
#include "threadpool.h"
#include "http_data.h"
#include "tsconfig.h"
#include "collection_manager.h"
#include "option.h"

class ConfigImpl : public Config {
public:
    ConfigImpl(): Config() {}
};

class RaftServerTest : public ::testing::Test {
protected:
    std::unique_ptr<butil::AtExitManager> exit_manager;
    std::vector<std::unique_ptr<brpc::Server>> raft_servers;
    Store* store;
    Store* analytics_store;
    ThreadPool* thread_pool;
    ThreadPool* thread_pool_for_server;
    HttpServer* http_server;
    BatchedIndexer* batched_indexer;
    http_message_dispatcher* message_dispatcher;
    ConfigImpl* config;
    std::string test_dir;
    std::atomic<bool> quit;

    void SetUp() override {
        // Initialize braft dependencies first
        exit_manager = std::make_unique<butil::AtExitManager>();

        // Create test directory
        test_dir = "/tmp/typesense_test/raft_server";
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);

        // Initialize components
        store = new Store(test_dir + "/store");
        analytics_store = new Store(test_dir + "/analytics");

        // Initialize CollectionManager - this is crucial for database operations
        CollectionManager::get_instance().init(store, 1.0, "auth_key", quit);

        thread_pool = new ThreadPool(4);

        // Create minimal config for testing
        config = new ConfigImpl();

        // Create minimal real objects for testing
        thread_pool_for_server = new ThreadPool(2);
        http_server = new HttpServer("test", "127.0.0.1", 8080, "", "", 0, false, {}, thread_pool_for_server);
        message_dispatcher = http_server->get_message_dispatcher();

        // Initialize other components
        batched_indexer = new BatchedIndexer(http_server, store, analytics_store, 4, *config, quit);

        quit = false;
    }

    void TearDown() override {
        delete batched_indexer;
        thread_pool->shutdown();
        delete thread_pool;
        thread_pool_for_server->shutdown();
        delete thread_pool_for_server;
        delete store;
        delete analytics_store;
        delete config;
        delete http_server;

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

    std::unique_ptr<RaftStateMachine> createRaftStateMachine() {
        return std::make_unique<RaftStateMachine>(
            http_server, batched_indexer, store, analytics_store,
            thread_pool, message_dispatcher, false, config, 4, 1000
        );
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

TEST_F(RaftServerTest, Constructor) {
    auto raft_server = createRaftStateMachine();

    EXPECT_NE(raft_server, nullptr);
    EXPECT_EQ(raft_server->get_store(), store);
    EXPECT_EQ(raft_server->get_config(), config);
    EXPECT_EQ(raft_server->get_batched_indexer(), batched_indexer);
    EXPECT_EQ(raft_server->get_message_dispatcher(), message_dispatcher);
}

TEST_F(RaftServerTest, Start) {
    auto raft_server = createRaftStateMachine();

    // Set up raft startup parameters
    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9000, &peering_endpoint);
    EXPECT_EQ(result, 0);

    int api_port = 9001;
    int election_timeout_ms = 1000;
    int snapshot_max_byte_count_per_rpc = 128 * 1024;
    std::string raft_dir = test_dir + "/raft_startup";
    std::string nodes_config = "127.0.0.1:9000:9001";
    std::atomic<bool> quit_abruptly{false};

    // Create raft directory with proper structure
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    // Set up RPC server for this endpoint
    createRaftServer(peering_endpoint);

    // RaftStateMachine should start successfully
    int start_result = raft_server->start(peering_endpoint, api_port, election_timeout_ms,
                                                 snapshot_max_byte_count_per_rpc, raft_dir,
                                                 nodes_config, quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // State machine should be alive and ready
    EXPECT_TRUE(raft_server->is_alive());

    // Should have proper raft state
    auto status = raft_server->get_status();
    EXPECT_TRUE(status.contains("state"));
    std::string state = status["state"];
    EXPECT_TRUE(state == "LEADER" || state == "FOLLOWER" || state == "CANDIDATE");

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, IsAlive) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9002, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_alive";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9003, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9002:9003", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Should be alive after starting
    EXPECT_TRUE(raft_server->is_alive());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, IsAliveWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Initially not alive
    EXPECT_FALSE(raft_server->is_alive());
}

TEST_F(RaftServerTest, IsLeader) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9004, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9005, 800,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9004:9005", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    // Single node should elect itself as leader
    EXPECT_TRUE(raft_server->is_leader());

    auto status = raft_server->get_status();
    EXPECT_EQ(status["state"], "LEADER");

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, IsLeaderWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Initially not leader
    EXPECT_FALSE(raft_server->is_leader());
}

TEST_F(RaftServerTest, ReadWriteCaughtUp) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9006, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_ready";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9007, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9006:9007", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Should be ready for operations
    EXPECT_TRUE(raft_server->is_read_caught_up());
    EXPECT_TRUE(raft_server->is_write_caught_up());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, ReadWriteCaughtUpWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Initially not ready
    EXPECT_FALSE(raft_server->is_read_caught_up());
    EXPECT_FALSE(raft_server->is_write_caught_up());
}

TEST_F(RaftServerTest, GetStatus) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9008, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_status";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9009, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9008:9009", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for startup to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // Should be able to get status
    auto status = raft_server->get_status();
    EXPECT_TRUE(status.contains("state"));

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, GetStatusWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Get status JSON
    auto status = raft_server->get_status();

    // Should contain expected keys
    EXPECT_TRUE(status.contains("state"));
    EXPECT_TRUE(status.contains("committed_index"));
    EXPECT_TRUE(status.contains("queued_writes"));
    EXPECT_TRUE(status.contains("is_leader"));
    EXPECT_TRUE(status.contains("read_ready"));
    EXPECT_TRUE(status.contains("write_ready"));

    // Initial values
    EXPECT_EQ(status["state"], "NOT_READY");
    EXPECT_EQ(status["committed_index"], 0);
    EXPECT_EQ(status["is_leader"], false);
    EXPECT_EQ(status["read_ready"], false);
    EXPECT_EQ(status["write_ready"], false);
}

TEST_F(RaftServerTest, GetLeaderUrl) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9010, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader_url";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9011, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9010:9011", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Should have leader URL
    auto leader_url = raft_server->get_leader_url();
    EXPECT_FALSE(leader_url.empty());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, GetLeaderUrlWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // No leader initially
    auto leader_url = raft_server->get_leader_url();
    EXPECT_TRUE(leader_url.empty());
}

TEST_F(RaftServerTest, HasLeaderTerm) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9012, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader_term";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9013, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9012:9013", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Should have leader term
    EXPECT_TRUE(raft_server->has_leader_term());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, HasLeaderTermWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // No leader initially
    bool has_leader = raft_server->has_leader_term();
    EXPECT_FALSE(has_leader);
}

TEST_F(RaftServerTest, Write) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9014, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_write";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9015, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9014:9015", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Node should become leader
    EXPECT_TRUE(raft_server->is_leader());

    // Should be ready for operations
    EXPECT_TRUE(raft_server->is_read_caught_up());
    EXPECT_TRUE(raft_server->is_write_caught_up());

    // Test write request processing
    auto request = std::make_shared<http_req>();
    static int dummy_generator = 0;
    auto response = std::make_shared<http_res>(&dummy_generator);

    // Set up request for write
    request->body = "{\"action\": \"create\", \"collection\": \"test\", \"id\": \"1\"}";
    request->path_without_query = "/collections/test/documents";
    request->route_hash = 12345;
    request->http_method = "POST";
    response->final = false;

    // Write request should be processed
    raft_server->write(request, response);

    // Give time for async processing
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, WriteWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Create test request and response
    auto request = std::make_shared<http_req>();
    static int dummy_generator = 0;
    auto response = std::make_shared<http_res>(&dummy_generator);

    // Set up request properties that write() method expects
    request->body = "{\"test\": \"data\"}";
    request->path_without_query = "/test";
    request->route_hash = 12345;
    response->final = false;

    // This should not crash even without a started node
    raft_server->write(request, response);

    // Response should have error status code (since no leader found)
    EXPECT_EQ(response->status_code, 500);
    EXPECT_NE(response->body.find("Could not find a leader"), std::string::npos);
}

TEST_F(RaftServerTest, InitDb) {
    auto raft_server = createRaftStateMachine();

    // Database initialization should succeed
    int init_result = raft_server->init_db();
    EXPECT_EQ(init_result, 0);

    // State machine should remain functional after init_db
    EXPECT_NE(raft_server, nullptr);

    // Should be able to get status after initialization
    auto status = raft_server->get_status();
    EXPECT_TRUE(status.contains("state"));

    // Other operations should work correctly
    EXPECT_FALSE(raft_server->is_alive());
}

TEST_F(RaftServerTest, TriggerVote) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9016, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_vote";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9017, 800,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9016:9017", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    // Should be able to trigger vote (no-op for leader)
    raft_server->trigger_vote();
    // Result depends on node state

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, TriggerVoteWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Should fail without initialized node
    bool vote_result = raft_server->trigger_vote();
    EXPECT_FALSE(vote_result);
}

TEST_F(RaftServerTest, ResetPeers) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9018, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_reset";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9019, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9018:9019", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // reset_peers should work
    raft_server->reset_peers();
    // Result depends on implementation

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, ResetPeersWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Should fail without initialized node
    bool reset_result = raft_server->reset_peers();
    EXPECT_FALSE(reset_result);
}

TEST_F(RaftServerTest, RefreshNodes) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9020, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_refresh";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9021, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9020:9021", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for cluster to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Test refresh_nodes operations
    std::string nodes_config = "127.0.0.1:9020:9021,127.0.0.1:9022:9023";
    std::atomic<bool> reset_peers{false};

    // refresh_nodes should work
    raft_server->refresh_nodes(nodes_config, 0, reset_peers);

    // refresh_nodes with reset_peers should work
    reset_peers = true;
    raft_server->refresh_nodes(nodes_config, 0, reset_peers);

    // Node should remain functional after membership changes
    auto status = raft_server->get_status();
    EXPECT_TRUE(status.contains("state"));

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, RefreshNodesWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Test node refresh operations
    std::string nodes_config = "127.0.0.1:8090:8091";
    std::atomic<bool> reset_peers{false};

    // This should not crash even without initialized node
    raft_server->refresh_nodes(nodes_config, 0, reset_peers);
}

TEST_F(RaftServerTest, RefreshCatchupStatus) {
    auto raft_server = createRaftStateMachine();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9024, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_catchup";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRaftServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9025, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9024:9025", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Test refresh methods (should not crash)
    raft_server->refresh_catchup_status(false);
    raft_server->refresh_catchup_status(true);

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, RefreshCatchupStatusWithoutStarting) {
    auto raft_server = createRaftStateMachine();

    // Should not crash when calling refresh_catchup_status without starting
    raft_server->refresh_catchup_status(true);
    raft_server->refresh_catchup_status(false);

    // Should return false for readiness flags without starting
    EXPECT_FALSE(raft_server->is_read_caught_up());
    EXPECT_FALSE(raft_server->is_write_caught_up());
}

TEST_F(RaftServerTest, InitNode) {
    auto raft_server = createNodeManager();
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
    int init_result = raft_server->init_node(state_machine.get(), endpoint, api_port,
                                             election_timeout_ms, raft_dir, nodes_config);
    EXPECT_EQ(init_result, 0);

    // Node should be initialized and ready
    auto status = raft_server->get_status();
    EXPECT_NE(status["state"], "NOT_READY");

    // Should have valid node ID after initialization
    auto node_id = raft_server->node_id();

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, InitNodeWithNullStateMachine) {
    auto raft_server = createNodeManager();

    // Test basic node configuration setup
    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1:8090", &endpoint);
    EXPECT_EQ(result, 0);

    std::vector<std::string> nodes = {"127.0.0.1:8090:8091"};

    // Should fail gracefully with nullptr StateMachine
    int init_result = raft_server->init_node(nullptr, endpoint, 8091, 5000, "/tmp/test_raft", "127.0.0.1:8090:8091");
    EXPECT_NE(init_result, 0);
}

TEST_F(RaftServerTest, InitNodeWithInvalidEndpoint) {
    auto raft_server = createNodeManager();

    // Test various error conditions
    butil::EndPoint invalid_endpoint;
    std::vector<std::string> empty_nodes;

    int result = raft_server->init_node(nullptr, invalid_endpoint, 0, 0, "/tmp", "");
    EXPECT_NE(result, 0);

    // All operations should still work after errors
    auto status = raft_server->get_status();
    EXPECT_TRUE(status.contains("state"));
}

TEST_F(RaftServerTest, IsLeader) {
    auto raft_server = createNodeManager();
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
    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8095,
                                             1000, raft_dir, "127.0.0.1:8094:8095");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Single node should become leader
    EXPECT_TRUE(raft_server->is_leader());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, IsLeaderWithoutNode) {
    auto raft_server = createNodeManager();

    // Should work even without initialized node
    EXPECT_FALSE(raft_server->is_leader());
}

TEST_F(RaftServerTest, GetStatus) {
    auto raft_server = createNodeManager();
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

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8097,
                                             1000, raft_dir, "127.0.0.1:8096:8097");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    auto status = raft_server->get_status();

    // Should contain all expected status keys
    EXPECT_TRUE(status.contains("state"));
    EXPECT_TRUE(status.contains("committed_index"));
    EXPECT_TRUE(status.contains("queued_writes"));
    EXPECT_TRUE(status.contains("is_leader"));
    EXPECT_TRUE(status.contains("read_ready"));
    EXPECT_TRUE(status.contains("write_ready"));

    // State should be valid
    EXPECT_NE(status["state"], "NOT_READY");

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, GetStatusWithoutNode) {
    auto raft_server = createNodeManager();

    // Get status JSON - should work even without initialized node
    auto status = raft_server->get_status();

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

TEST_F(RaftServerTest, GetLeaderUrl) {
    auto raft_server = createNodeManager();
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

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8099,
                                             1000, raft_dir, "127.0.0.1:8098:8099");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Should have valid leader URL
    auto leader_url = raft_server->get_leader_url();
    EXPECT_FALSE(leader_url.empty());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, GetLeaderUrlWithoutNode) {
    auto raft_server = createNodeManager();

    // Should return empty string when no node is initialized
    auto leader_url = raft_server->get_leader_url();
    EXPECT_TRUE(leader_url.empty());
}

TEST_F(RaftServerTest, NodeId) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8100, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_node_id";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8101,
                                             1000, raft_dir, "127.0.0.1:8100:8101");
    EXPECT_EQ(init_result, 0);

    // Should have valid node ID
    auto node_id = raft_server->node_id();
    // Verify node_id is valid (exact validation depends on braft::NodeId implementation)

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, NodeIdWithoutNode) {
    auto raft_server = createNodeManager();

    // Should return default-constructed NodeId when no node is initialized
    auto node_id = raft_server->node_id();
}

TEST_F(RaftServerTest, LeaderId) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8102, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader_id";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8103,
                                             1000, raft_dir, "127.0.0.1:8102:8103");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Should have valid leader ID
    auto leader_id = raft_server->leader_id();
    EXPECT_FALSE(leader_id.is_empty());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, LeaderIdWithoutNode) {
    auto raft_server = createNodeManager();

    // Should return empty PeerId when no node is initialized
    auto leader_id = raft_server->leader_id();
    EXPECT_TRUE(leader_id.is_empty());
}

TEST_F(RaftServerTest, ReadWriteReadyStates) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8104, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_ready";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8105,
                                             1000, raft_dir, "127.0.0.1:8104:8105");
    EXPECT_EQ(init_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Leader should be ready for operations
    EXPECT_TRUE(raft_server->is_read_ready());
    EXPECT_TRUE(raft_server->is_write_ready());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, ReadWriteReadyStatesWithoutNode) {
    auto raft_server = createNodeManager();

    // Initial state should be not ready
    EXPECT_FALSE(raft_server->is_read_ready());
    EXPECT_FALSE(raft_server->is_write_ready());

    // These methods should not crash and should return consistent values
    auto read_state1 = raft_server->is_read_ready();
    auto read_state2 = raft_server->is_read_ready();
    EXPECT_EQ(read_state1, read_state2);

    auto write_state1 = raft_server->is_write_ready();
    auto write_state2 = raft_server->is_write_ready();
    EXPECT_EQ(write_state1, write_state2);
}

TEST_F(RaftServerTest, WaitUntilReady) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8106, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_wait";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8107,
                                             1000, raft_dir, "127.0.0.1:8106:8107");
    EXPECT_EQ(init_result, 0);

    // Should become ready relatively quickly
    std::atomic<bool> test_quit{false};
    bool ready = raft_server->wait_until_ready(5000, test_quit);
    EXPECT_TRUE(ready);

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, WaitUntilReadyTimeout) {
    auto raft_server = createNodeManager();

    auto start_time = std::chrono::steady_clock::now();

    // Wait with short timeout - should timeout quickly since node is not started
    std::atomic<bool> test_quit{false};
    bool ready = raft_server->wait_until_ready(1000, test_quit);

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    EXPECT_FALSE(ready);
    EXPECT_GE(duration.count(), 1);
    EXPECT_LE(duration.count(), 2);
}

TEST_F(RaftServerTest, TriggerVote) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8108, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_vote";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8109,
                                             1000, raft_dir, "127.0.0.1:8108:8109");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Vote trigger should work or be no-op for leader
    auto vote_result = raft_server->trigger_vote();
    // Result depends on node state

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, TriggerVoteWithoutNode) {
    auto raft_server = createNodeManager();

    // Should fail gracefully when no node is initialized
    auto result = raft_server->trigger_vote();
    EXPECT_FALSE(result.ok());
}

TEST_F(RaftServerTest, ResetPeers) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8110, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_reset";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8111,
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
    auto reset_result = raft_server->reset_peers(new_conf);
    EXPECT_TRUE(reset_result.ok());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, ResetPeersWithoutNode) {
    auto raft_server = createNodeManager();

    // Create a test configuration
    braft::Configuration new_conf;
    braft::PeerId peer_id;
    int parse_result = peer_id.parse("127.0.0.1:8090");
    EXPECT_EQ(parse_result, 0);

    new_conf.add_peer(peer_id);

    // Should fail gracefully when no node is initialized
    auto result = raft_server->reset_peers(new_conf);
    EXPECT_FALSE(result.ok());
}

TEST_F(RaftServerTest, ResetPeersWithEmptyConfiguration) {
    auto raft_server = createNodeManager();

    // Invalid configuration
    braft::Configuration empty_conf;
    auto reset_result = raft_server->reset_peers(empty_conf);
    EXPECT_FALSE(reset_result.ok());
}

TEST_F(RaftServerTest, RefreshNodes) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8114, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_refresh";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8115,
                                             1000, raft_dir, "127.0.0.1:8114:8115");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // Test refresh_nodes operation
    std::string nodes_config = "127.0.0.1:8114:8115,127.0.0.1:8116:8117";
    raft_server->refresh_nodes(nodes_config, false);
    raft_server->refresh_nodes(nodes_config, true);

    // Node should still be functional after membership changes
    auto status = raft_server->get_status();
    EXPECT_TRUE(status.contains("state"));

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, RefreshNodesWithoutNode) {
    auto raft_server = createNodeManager();

    // Should not crash when trying to refresh nodes without initialized node
    std::string nodes_config = "127.0.0.1:8090:8091";

    // This should complete without crashing (though may log errors)
    raft_server->refresh_nodes(nodes_config, false);
    raft_server->refresh_nodes(nodes_config, true);
}

TEST_F(RaftServerTest, Shutdown) {
    auto raft_server = createNodeManager();
    auto state_machine = std::make_unique<MockRaftStateMachine>();

    butil::EndPoint endpoint;
    int result = butil::str2endpoint("127.0.0.1", 8118, &endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_shutdown";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");

    createRaftServer(endpoint);

    int init_result = raft_server->init_node(state_machine.get(), endpoint, 8119,
                                             1000, raft_dir, "127.0.0.1:8118:8119");
    EXPECT_EQ(init_result, 0);

    // Wait for node to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // Should shutdown cleanly
    raft_server->shutdown();

    // Status should still work after shutdown
    auto status = raft_server->get_status();
    EXPECT_TRUE(status.contains("state"));

    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, ShutdownWithoutNode) {
    auto raft_server = createNodeManager();

    // Shutdown without initialized node should not crash
    raft_server->shutdown();
}

TEST_F(RaftServerTest, LogNodeStatus) {
    auto raft_server = createNodeManager();

    braft::NodeStatus status;
    // Initialize status with some dummy values
    status.state = braft::STATE_LEADER;
    status.committed_index = 100;

    // Should not crash when logging status
    raft_server->log_node_status(status);
    raft_server->log_node_status(status, "test_prefix");
}

TEST_F(RaftServerTest, LifecycleWithoutNetworking) {
    auto raft_server = createNodeManager();

    // Test full lifecycle without actual networking
    // 1. Initial state
    EXPECT_FALSE(raft_server->is_leader());

    // 2. Status reporting works throughout
    auto status1 = raft_server->get_status();
    EXPECT_EQ(status1["state"], "NOT_READY");

    // 3. Shutdown (should not crash even if never started)
    raft_server->shutdown();

    // 4. Status should still work after shutdown
    auto status2 = raft_server->get_status();
    EXPECT_TRUE(status2.contains("state"));
}

TEST_F(RaftServerTest, MultipleNodeManagers) {
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
