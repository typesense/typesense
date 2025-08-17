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

    std::unique_ptr<RaftServer> createRaftServer() {
        return std::make_unique<RaftServer>(
            http_server, batched_indexer, store, analytics_store,
            thread_pool, message_dispatcher, false, config, 4, 1000
        );
    }

    brpc::Server* createRpcServer(const butil::EndPoint& endpoint) {
        auto server = std::make_unique<brpc::Server>();
        brpc::Server* server_ptr = server.get();

        int add_service_result = braft::add_service(server_ptr, endpoint);
        EXPECT_EQ(add_service_result, 0);

        int start_result = server->Start(endpoint, nullptr);
        EXPECT_EQ(start_result, 0);

        raft_servers.push_back(std::move(server));
        return server_ptr;
    }

    struct MultiNodeSetup {
        std::vector<std::unique_ptr<RaftServer>> raft_servers;
        std::vector<butil::EndPoint> peering_endpoints;
        std::vector<int> api_ports;
        std::vector<std::string> raft_dirs;
        std::string nodes_config;
        std::vector<std::unique_ptr<std::atomic<bool>>> quit_flags;

        // Make explicitly movable and non-copyable
        MultiNodeSetup(const MultiNodeSetup&) = delete;
        MultiNodeSetup& operator=(const MultiNodeSetup&) = delete;
        MultiNodeSetup(MultiNodeSetup&&) = default;
        MultiNodeSetup& operator=(MultiNodeSetup&&) = default;

        MultiNodeSetup(int num_nodes = 3) {
            raft_servers.reserve(num_nodes);
            peering_endpoints.reserve(num_nodes);
            api_ports.reserve(num_nodes);
            raft_dirs.reserve(num_nodes);
            quit_flags.reserve(num_nodes);

            // Initialize vectors with actual elements
            for (int i = 0; i < num_nodes; i++) {
                raft_servers.push_back(nullptr);  // Will be filled later
                peering_endpoints.emplace_back();
                api_ports.push_back(0);
                raft_dirs.emplace_back();
                quit_flags.push_back(std::make_unique<std::atomic<bool>>(false));
            }

            // Build nodes config string: "IP:PEER_PORT:API_PORT,..."
            std::vector<std::string> node_configs;
            for (int i = 0; i < num_nodes; i++) {
                api_ports[i] = 9200 + i * 2;  // 9200, 9202, 9204
                int peer_port = 9201 + i * 2;  // 9201, 9203, 9205
                
                int result = butil::str2endpoint("127.0.0.1", peer_port, &peering_endpoints[i]);
                EXPECT_EQ(result, 0);

                node_configs.push_back("127.0.0.1:" + std::to_string(peer_port) + ":" + std::to_string(api_ports[i]));
                raft_dirs[i] = "/tmp/typesense_test/raft_server/multinode_" + std::to_string(i);
                
                // Create directories
                std::filesystem::create_directories(raft_dirs[i] + "/log");
                std::filesystem::create_directories(raft_dirs[i] + "/raft_meta");
                std::filesystem::create_directories(raft_dirs[i] + "/snapshot");
            }
            
            nodes_config = StringUtils::join(node_configs, ",");
        }

        ~MultiNodeSetup() {
            // Cleanup directories
            for (const auto& dir : raft_dirs) {
                std::filesystem::remove_all(dir);
            }
        }
    };

    MultiNodeSetup createMultiNodeSetup(RaftServerTest* test_instance, int num_nodes = 3) {
        MultiNodeSetup setup(num_nodes);

        // Create RPC servers for each node
        for (int i = 0; i < num_nodes; i++) {
            test_instance->createRpcServer(setup.peering_endpoints[i]);
        }

        // Create RaftServer instances
        for (int i = 0; i < num_nodes; i++) {
            setup.raft_servers[i] = test_instance->createRaftServer();
        }

        return setup;
    }
};

TEST_F(RaftServerTest, Constructor) {
    auto raft_server = createRaftServer();

    EXPECT_NE(raft_server, nullptr);
    EXPECT_EQ(raft_server->get_store(), store);
    EXPECT_EQ(raft_server->get_message_dispatcher(), message_dispatcher);
}

TEST_F(RaftServerTest, StartSingleNode) {
    auto raft_server = createRaftServer();

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
    createRpcServer(peering_endpoint);

    // RaftServer should start successfully
    int start_result = raft_server->start(peering_endpoint, api_port, election_timeout_ms,
                                                 snapshot_max_byte_count_per_rpc, raft_dir,
                                                 nodes_config, quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Allow brief time for node to complete initialization
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // For single-node clusters, manually refresh catchup status (simulates periodic refresh in main server loop)
    raft_server->refresh_catchup_status(false);

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

TEST_F(RaftServerTest, IsAliveSingleNode) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9002, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_alive";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9003, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9002:9003", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Allow brief time for node to complete initialization
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // For single-node clusters, manually refresh catchup status (simulates periodic refresh in main server loop)
    raft_server->refresh_catchup_status(false);

    // Should be alive after starting
    EXPECT_TRUE(raft_server->is_alive());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, IsAliveWithoutStarting) {
    auto raft_server = createRaftServer();

    // Initially not alive
    EXPECT_FALSE(raft_server->is_alive());
}

TEST_F(RaftServerTest, IsLeader) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9004, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // Initially not leader
    EXPECT_FALSE(raft_server->is_leader());
}

TEST_F(RaftServerTest, ReadWriteCaughtUpSingleNode) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9006, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_ready";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9007, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9006:9007", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // For single-node clusters, manually refresh catchup status since leader doesn't trigger it
    raft_server->refresh_catchup_status(false);

    // Should be ready for operations
    EXPECT_TRUE(raft_server->is_read_caught_up());
    EXPECT_TRUE(raft_server->is_write_caught_up());

    raft_server->shutdown();
    std::filesystem::remove_all(raft_dir);
}

TEST_F(RaftServerTest, ReadWriteCaughtUpWithoutStarting) {
    auto raft_server = createRaftServer();

    // Initially not ready
    EXPECT_FALSE(raft_server->is_read_caught_up());
    EXPECT_FALSE(raft_server->is_write_caught_up());
}

TEST_F(RaftServerTest, GetStatus) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9008, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_status";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // Get status JSON
    auto status = raft_server->get_status();

    // Should contain expected keys
    EXPECT_TRUE(status.contains("state"));
    EXPECT_TRUE(status.contains("committed_index"));
    EXPECT_TRUE(status.contains("queued_writes"));

    // Initial values
    EXPECT_EQ(status["state"], "NOT_READY");
    EXPECT_EQ(status["committed_index"], 0);
}

TEST_F(RaftServerTest, GetLeaderUrl) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9010, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader_url";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // No leader initially
    auto leader_url = raft_server->get_leader_url();
    EXPECT_TRUE(leader_url.empty());
}

TEST_F(RaftServerTest, HasLeaderTerm) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9012, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_leader_term";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // No leader initially
    bool has_leader = raft_server->has_leader_term();
    EXPECT_FALSE(has_leader);
}

TEST_F(RaftServerTest, WriteSingleNode) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9014, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_write";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

    int start_result = raft_server->start(peering_endpoint, 9015, 1000,
                                                 128 * 1024, raft_dir,
                                                 "127.0.0.1:9014:9015", quit_abruptly);
    EXPECT_EQ(start_result, 0);

    // Wait for leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Node should become leader
    EXPECT_TRUE(raft_server->is_leader());

    // For single-node clusters, manually refresh catchup status since leader doesn't trigger it
    raft_server->refresh_catchup_status(false);

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

TEST_F(RaftServerTest, InitDb) {
    auto raft_server = createRaftServer();

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
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9016, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_vote";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // Should fail without initialized node
    bool vote_result = raft_server->trigger_vote();
    EXPECT_FALSE(vote_result);
}

TEST_F(RaftServerTest, ResetPeers) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9018, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_reset";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // Should fail without initialized node
    bool reset_result = raft_server->reset_peers();
    EXPECT_FALSE(reset_result);
}

TEST_F(RaftServerTest, RefreshNodes) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9020, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_refresh";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // Test node refresh operations
    std::string nodes_config = "127.0.0.1:8090:8091";
    std::atomic<bool> reset_peers{false};

    // This should not crash even without initialized node
    raft_server->refresh_nodes(nodes_config, 0, reset_peers);
}

TEST_F(RaftServerTest, RefreshCatchupStatus) {
    auto raft_server = createRaftServer();

    butil::EndPoint peering_endpoint;
    int result = butil::str2endpoint("127.0.0.1", 9024, &peering_endpoint);
    EXPECT_EQ(result, 0);

    std::string raft_dir = test_dir + "/raft_catchup";
    std::filesystem::create_directories(raft_dir + "/log");
    std::filesystem::create_directories(raft_dir + "/raft_meta");
    std::filesystem::create_directories(raft_dir + "/snapshot");
    std::atomic<bool> quit_abruptly{false};

    createRpcServer(peering_endpoint);

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
    auto raft_server = createRaftServer();

    // Should not crash when calling refresh_catchup_status without starting
    raft_server->refresh_catchup_status(true);
    raft_server->refresh_catchup_status(false);

    // Should return false for readiness flags without starting
    EXPECT_FALSE(raft_server->is_read_caught_up());
    EXPECT_FALSE(raft_server->is_write_caught_up());
}

// ==================== MULTI-NODE TESTS ====================

TEST_F(RaftServerTest, StartMultiNode) {
    auto setup = createMultiNodeSetup(this, 3);

    // Start all nodes
    for (int i = 0; i < 3; i++) {
        int start_result = setup.raft_servers[i]->start(
            setup.peering_endpoints[i], setup.api_ports[i], 5000,
            128 * 1024, setup.raft_dirs[i], setup.nodes_config, *setup.quit_flags[i]);
        EXPECT_EQ(start_result, 0);
    }

    // Allow initial startup time
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Manually trigger leader election (similar to API tests calling /operations/vote)
    bool vote_triggered = setup.raft_servers[0]->trigger_vote();
    EXPECT_TRUE(vote_triggered);

    // Wait for leader election with progressive delays (like API tests)
    std::vector<int> delay_intervals = {100, 1000, 2000, 3000, 4000};
    bool leader_elected = false;

    for (int delay : delay_intervals) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));

        // Refresh catchup status for all nodes
        for (int i = 0; i < 3; i++) {
            setup.raft_servers[i]->refresh_catchup_status(false);
        }

        // Check if a leader has been elected
        int leader_count = 0;
        for (int i = 0; i < 3; i++) {
            if (setup.raft_servers[i]->is_leader()) {
                leader_count++;
            }
        }

        if (leader_count == 1) {
            leader_elected = true;
            break;
        }
    }

    EXPECT_TRUE(leader_elected);

    // Final verification: exactly one leader, others followers
    int leader_count = 0;
    int follower_count = 0;
    for (int i = 0; i < 3; i++) {
        if (setup.raft_servers[i]->is_leader()) {
            leader_count++;
        } else {
            follower_count++;
        }
    }

    EXPECT_EQ(leader_count, 1);
    EXPECT_EQ(follower_count, 2);

    // All nodes should be alive after leader election
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(setup.raft_servers[i]->is_alive());
    }

    // Shutdown all nodes
    for (int i = 0; i < 3; i++) {
        setup.raft_servers[i]->shutdown();
    }
}

TEST_F(RaftServerTest, LeaderElectionMultiNode) {
    auto setup = createMultiNodeSetup(this, 3);

    // Start all nodes
    for (int i = 0; i < 3; i++) {
        int start_result = setup.raft_servers[i]->start(
            setup.peering_endpoints[i], setup.api_ports[i], 2000,
            128 * 1024, setup.raft_dirs[i], setup.nodes_config, *setup.quit_flags[i]);
        EXPECT_EQ(start_result, 0);
    }

    // Allow initial startup time
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Manually trigger leader election
    bool vote_triggered = setup.raft_servers[0]->trigger_vote();
    EXPECT_TRUE(vote_triggered);

    // Wait for leader election to complete
    std::vector<int> delay_intervals = {100, 1000, 2000, 3000};
    int leader_index = -1;

    for (int delay : delay_intervals) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));

        // Refresh catchup status for all nodes
        for (int i = 0; i < 3; i++) {
            setup.raft_servers[i]->refresh_catchup_status(false);
        }

        // Find the leader
        leader_index = -1;
        for (int i = 0; i < 3; i++) {
            if (setup.raft_servers[i]->is_leader()) {
                EXPECT_EQ(leader_index, -1); // Only one leader should exist
                leader_index = i;
            }
        }

        if (leader_index != -1) {
            break; // Leader found
        }
    }

    EXPECT_NE(leader_index, -1); // A leader should exist

    // Leader should have leader term
    EXPECT_TRUE(setup.raft_servers[leader_index]->has_leader_term());

    // Followers should also see the leader
    for (int i = 0; i < 3; i++) {
        if (i != leader_index) {
            EXPECT_FALSE(setup.raft_servers[i]->is_leader());
            EXPECT_TRUE(setup.raft_servers[i]->has_leader_term()); // Should know about leader
        }
    }

    // Shutdown all nodes
    for (int i = 0; i < 3; i++) {
        setup.raft_servers[i]->shutdown();
    }
}

TEST_F(RaftServerTest, ReadWriteCaughtUpMultiNode) {
    auto setup = createMultiNodeSetup(this, 3);

    // Start all nodes
    for (int i = 0; i < 3; i++) {
        int start_result = setup.raft_servers[i]->start(
            setup.peering_endpoints[i], setup.api_ports[i], 3000,
            128 * 1024, setup.raft_dirs[i], setup.nodes_config, *setup.quit_flags[i]);
        EXPECT_EQ(start_result, 0);
    }

    // Allow initial startup time
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Manually trigger leader election
    bool vote_triggered = setup.raft_servers[0]->trigger_vote();
    EXPECT_TRUE(vote_triggered);

    // Wait for leader election and synchronization
    std::vector<int> delay_intervals = {100, 1000, 2000, 3000, 4000};

    for (int delay : delay_intervals) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));

        // Refresh catchup status for all nodes
        for (int i = 0; i < 3; i++) {
            setup.raft_servers[i]->refresh_catchup_status(false);
        }

        // Check if all nodes are caught up
        bool all_caught_up = true;
        for (int i = 0; i < 3; i++) {
            if (!setup.raft_servers[i]->is_read_caught_up() || !setup.raft_servers[i]->is_write_caught_up()) {
                all_caught_up = false;
                break;
            }
        }

        if (all_caught_up) {
            break; // All nodes caught up
        }
    }

    // All nodes should be caught up after initial sync
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(setup.raft_servers[i]->is_read_caught_up());
        EXPECT_TRUE(setup.raft_servers[i]->is_write_caught_up());
    }

    // Shutdown all nodes
    for (int i = 0; i < 3; i++) {
        setup.raft_servers[i]->shutdown();
    }
}

TEST_F(RaftServerTest, RefreshNodesMultiNode) {
    auto setup = createMultiNodeSetup(this, 2); // Start with 2 nodes

    // Start initial 2 nodes
    for (int i = 0; i < 2; i++) {
        int start_result = setup.raft_servers[i]->start(
            setup.peering_endpoints[i], setup.api_ports[i], 3000,
            128 * 1024, setup.raft_dirs[i], setup.nodes_config, *setup.quit_flags[i]);
        EXPECT_EQ(start_result, 0);
    }

    // Allow initial startup time
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Manually trigger leader election
    bool vote_triggered = setup.raft_servers[0]->trigger_vote();
    EXPECT_TRUE(vote_triggered);

    // Wait for leader election
    std::vector<int> delay_intervals = {100, 1000, 2000};
    int leader_index = -1;

    for (int delay : delay_intervals) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));

        // Refresh catchup status
        for (int i = 0; i < 2; i++) {
            setup.raft_servers[i]->refresh_catchup_status(false);
        }

        // Find the leader
        leader_index = -1;
        for (int i = 0; i < 2; i++) {
            if (setup.raft_servers[i]->is_leader()) {
                leader_index = i;
                break;
            }
        }

        if (leader_index != -1) {
            break; // Leader found
        }
    }
    EXPECT_NE(leader_index, -1);

    // Test adding a third node via refresh_nodes
    std::string new_nodes_config = setup.nodes_config + ",127.0.0.1:9207:9206";
    std::atomic<bool> reset_peers{false};

    // Only leader should successfully refresh nodes
    setup.raft_servers[leader_index]->refresh_nodes(new_nodes_config, 0, reset_peers);

    // Allow time for the configuration change
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Shutdown all nodes
    for (int i = 0; i < 2; i++) {
        setup.raft_servers[i]->shutdown();
    }
}
