#pragma once

#include <memory>
#include <atomic>
#include <butil/endpoint.h>
#include <nlohmann/json.hpp>
#include "http_data.h"

// Forward declarations
class ReplicationState;
class RaftNodeManager;
class HttpServer;
class BatchedIndexer;
class Store;
class ThreadPool;
class http_message_dispatcher;
class Config;

/**
 * RaftCoordinator manages the lifecycle and integration of Raft components.
 * It owns both the ReplicationState (StateMachine) and RaftNodeManager,
 * eliminating circular dependencies and providing clear separation of concerns.
 */
class RaftCoordinator {
private:
    // Owned components
    std::unique_ptr<ReplicationState> state_machine;
    std::unique_ptr<RaftNodeManager> node_manager;

public:
    /**
     * Constructor - creates and wires together the Raft components
     */
    RaftCoordinator(HttpServer* server,
                   BatchedIndexer* batched_indexer,
                   Store* store,
                   Store* analytics_store,
                   ThreadPool* thread_pool,
                   http_message_dispatcher* message_dispatcher,
                   bool api_uses_ssl,
                   const Config* config,
                   size_t num_collections_parallel_load,
                   size_t num_documents_parallel_load);

    /**
     * Destructor - ensures proper shutdown order
     */
    ~RaftCoordinator();

    /**
     * Start the entire Raft system
     */
    int start(const butil::EndPoint& peering_endpoint,
             int api_port,
             int election_timeout_ms,
             int snapshot_max_byte_count_per_rpc,
             const std::string& raft_dir,
             const std::string& nodes,
             const std::atomic<bool>& quit_abruptly);

    /**
     * Shutdown the entire Raft system
     */
    void shutdown();

    /**
     * Get access to the state machine (for external API calls)
     */
    ReplicationState* get_state_machine() { return state_machine.get(); }
    const ReplicationState* get_state_machine() const { return state_machine.get(); }

    /**
     * Get access to the node manager (for direct node operations if needed)
     */
    RaftNodeManager* get_node_manager() { return node_manager.get(); }
    const RaftNodeManager* get_node_manager() const { return node_manager.get(); }

    // Convenience methods that delegate to the state machine
    void write(const std::shared_ptr<http_req>& request,
              const std::shared_ptr<http_res>& response) {
        if (state_machine) state_machine->write(request, response);
    }

    void read(const std::shared_ptr<http_res>& response) {
        if (state_machine) state_machine->read(response);
    }

    bool is_leader() const {
        return state_machine ? state_machine->is_leader() : false;
    }

    bool is_alive() const {
        return state_machine ? state_machine->is_alive() : false;
    }

    nlohmann::json get_status() const {
        return state_machine ? state_machine->get_status() : nlohmann::json{};
    }
};
