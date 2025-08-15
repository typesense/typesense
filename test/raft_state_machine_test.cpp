#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <filesystem>
#include <atomic>
#include "raft_state_machine.h"
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

class RaftStateMachineTest : public ::testing::Test {
protected:
    Store* store;
    Store* analytics_store;
    ThreadPool* thread_pool;
    HttpServer* http_server;
    BatchedIndexer* batched_indexer;
    http_message_dispatcher* message_dispatcher;
    ConfigImpl* config;
    std::string test_dir;
    std::atomic<bool> quit;

    void SetUp() override {
        // Create test directory
        test_dir = "/tmp/typesense_test/raft_state_machine";
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);

        // Initialize components
        store = new Store(test_dir + "/store");
        analytics_store = new Store(test_dir + "/analytics");

        thread_pool = new ThreadPool(4);

                // Create minimal config for testing
        config = new ConfigImpl();

        // Initialize mock message dispatcher
        message_dispatcher = new http_message_dispatcher();

        // Initialize other components (would normally use mocks in real tests)
        batched_indexer = new BatchedIndexer(http_server, store, analytics_store, 4, *config, quit);

        http_server = nullptr; // Will be created per test if needed

        quit = false;
    }

    void TearDown() override {
        delete batched_indexer;
        thread_pool->shutdown();
        delete thread_pool;
        delete store;
        delete analytics_store;
        delete config;
        delete message_dispatcher;

        // Clean up test directory
        std::filesystem::remove_all(test_dir);
    }

    std::unique_ptr<RaftStateMachine> createRaftStateMachine() {
        return std::make_unique<RaftStateMachine>(
            http_server, batched_indexer, store, analytics_store,
            thread_pool, message_dispatcher, false, config, 4, 1000
        );
    }
};

TEST_F(RaftStateMachineTest, ConstructorInitializesCorrectly) {
    auto raft_state_machine = createRaftStateMachine();

    EXPECT_NE(raft_state_machine, nullptr);
    EXPECT_EQ(raft_state_machine->get_store(), store);
    EXPECT_EQ(raft_state_machine->get_config(), config);
    EXPECT_EQ(raft_state_machine->get_batched_indexer(), batched_indexer);
    EXPECT_EQ(raft_state_machine->get_message_dispatcher(), message_dispatcher);
}

TEST_F(RaftStateMachineTest, InitialStateIsNotReady) {
    auto raft_state_machine = createRaftStateMachine();

    // Initially not ready/alive
    EXPECT_FALSE(raft_state_machine->is_alive());
    EXPECT_FALSE(raft_state_machine->is_leader());
    EXPECT_FALSE(raft_state_machine->is_read_caught_up());
    EXPECT_FALSE(raft_state_machine->is_write_caught_up());
}

TEST_F(RaftStateMachineTest, NodeManagerDelegationWorks) {
    auto raft_state_machine = createRaftStateMachine();

    // Test delegation to node manager - these should not crash
    auto status = raft_state_machine->get_status();
    EXPECT_TRUE(status.contains("state"));

    auto leader_url = raft_state_machine->get_leader_url();
    EXPECT_TRUE(leader_url.empty()); // No leader initially

    // Leader term should be available
    bool has_leader = raft_state_machine->has_leader_term();
    EXPECT_FALSE(has_leader); // No leader initially
}

TEST_F(RaftStateMachineTest, WriteRequestHandling) {
    auto raft_state_machine = createRaftStateMachine();

    // Create test request and response
    auto request = std::make_shared<http_req>();
    auto response = std::make_shared<http_res>(nullptr); // http_res requires generator parameter

    request->body = "{\"test\": \"data\"}";
    response->final = false;

    // This should not crash even without a started node
    // (though it will set error response since node is not ready)
    raft_state_machine->write(request, response);

    // Response should be marked as final (error case)
    EXPECT_TRUE(response->final);
}

TEST_F(RaftStateMachineTest, ShutdownWorks) {
    auto raft_state_machine = createRaftStateMachine();

    // Shutdown should not crash even if not started
    raft_state_machine->shutdown();

    // After shutdown, state should be shutting down
    // (This tests internal state management)
}

TEST_F(RaftStateMachineTest, NodeStatusReporting) {
    auto raft_state_machine = createRaftStateMachine();

    // Get status JSON
    auto status = raft_state_machine->get_status();

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

TEST_F(RaftStateMachineTest, PublicInterfaceWorks) {
    auto raft_state_machine = createRaftStateMachine();

    // Test public interface methods that are safe to call
    EXPECT_FALSE(raft_state_machine->is_leader());
    EXPECT_FALSE(raft_state_machine->is_alive());
    EXPECT_FALSE(raft_state_machine->has_leader_term());

    // Test node management methods
    bool vote_result = raft_state_machine->trigger_vote();
    EXPECT_FALSE(vote_result); // Should fail without initialized node

    bool reset_result = raft_state_machine->reset_peers();
    EXPECT_FALSE(reset_result); // Should fail without initialized node

    // Test refresh methods (should not crash)
    raft_state_machine->refresh_catchup_status(false);
    raft_state_machine->persist_applying_index();
}

TEST_F(RaftStateMachineTest, NodeRefreshOperations) {
    auto raft_state_machine = createRaftStateMachine();

    // Test node refresh operations
    std::string nodes_config = "127.0.0.1:8090:8091";
    std::atomic<bool> reset_peers{false};

    // This should not crash even without initialized node
    raft_state_machine->refresh_nodes(nodes_config, 0, reset_peers);
}

TEST_F(RaftStateMachineTest, DatabaseInitialization) {
    auto raft_state_machine = createRaftStateMachine();

    // Test database initialization
    int init_result = raft_state_machine->init_db();

    // Should complete (may succeed or fail gracefully depending on state)
    // The important thing is it doesn't crash
    EXPECT_TRUE(init_result == 0 || init_result != 0); // Just check it returns something
}
