#pragma once

#include <memory>
#include <atomic>
#include <butil/endpoint.h>
#include <nlohmann/json.hpp>
#include "http_data.h"
#include "raft_state_machine.h"

// Forward declarations
class RaftNodeManager;
class HttpServer;
class BatchedIndexer;
class Store;
class ThreadPool;
class http_message_dispatcher;
class Config;

/**
 * RaftServer manages the lifecycle and integration of Raft components.
 * It owns both the ReplicationState (StateMachine) and RaftNodeManager,
 * eliminating circular dependencies and providing clear separation of concerns.
 */
class RaftServer {
private:
    // Owned components
    std::unique_ptr<ReplicationState> state_machine;
    std::unique_ptr<RaftNodeManager> node_manager;

public:
    /**
     * Constructor - creates and wires together the Raft components
     */
    RaftServer(HttpServer* server,
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
    ~RaftServer();

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

    // Accessor methods for external components
    Store* get_store() {
        return state_machine ? state_machine->get_store() : nullptr;
    }

    const Config* get_config() const {
        return state_machine ? state_machine->get_config() : nullptr;
    }

    BatchedIndexer* get_batched_indexer() {
        return state_machine ? state_machine->get_batched_indexer() : nullptr;
    }

    http_message_dispatcher* get_message_dispatcher() const {
        return state_machine ? state_machine->get_message_dispatcher() : nullptr;
    }

    // Complete delegation methods to the state machine
    void write(const std::shared_ptr<http_req>& request,
              const std::shared_ptr<http_res>& response) {
        if (state_machine) state_machine->write(request, response);
    }

    void read(const std::shared_ptr<http_res>& response) {
        if (state_machine) state_machine->read(response);
    }

    // Node status and leadership
    bool is_leader() const {
        return state_machine ? state_machine->is_leader() : false;
    }

    bool is_alive() const {
        return state_machine ? state_machine->is_alive() : false;
    }

    nlohmann::json get_status() const {
        return state_machine ? state_machine->get_status() : nlohmann::json{};
    }

    std::string get_leader_url() const {
        return state_machine ? state_machine->get_leader_url() : "";
    }

    // Node management operations
    void refresh_nodes(const std::string& nodes, size_t raft_counter,
                      const std::atomic<bool>& reset_peers_on_error) {
        if (state_machine) state_machine->refresh_nodes(nodes, raft_counter, reset_peers_on_error);
    }

    void refresh_catchup_status(bool log_msg) {
        if (state_machine) state_machine->refresh_catchup_status(log_msg);
    }

    bool trigger_vote() {
        return state_machine ? state_machine->trigger_vote() : false;
    }

    bool reset_peers() {
        return state_machine ? state_machine->reset_peers() : false;
    }

    // Snapshot operations
    void do_snapshot(const std::string& snapshot_path,
                    const std::shared_ptr<http_req>& req,
                    const std::shared_ptr<http_res>& res) {
        if (state_machine) state_machine->do_snapshot(snapshot_path, req, res);
    }

    void do_snapshot(const std::string& nodes) {
        if (state_machine) state_machine->do_snapshot(nodes);
    }

    // State checks
    bool is_read_caught_up() const {
        return state_machine ? state_machine->is_read_caught_up() : false;
    }

    bool is_write_caught_up() const {
        return state_machine ? state_machine->is_write_caught_up() : false;
    }

    uint64_t node_state() const {
        return state_machine ? state_machine->node_state() : 0;
    }

    // Utility methods
    void persist_applying_index() {
        if (state_machine) state_machine->persist_applying_index();
    }

    int64_t get_num_queued_writes() {
        return state_machine ? state_machine->get_num_queued_writes() : 0;
    }

    /**
     * Integration method for HttpServer - handles server lifecycle
     */
    int run_http_server(HttpServer* server) {
        if (!state_machine) {
            return -1;
        }
        return server->run(state_machine.get());
    }

private:

    // HttpServer needs direct access for integration
    friend class HttpServer;
};
