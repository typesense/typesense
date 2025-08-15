#pragma once

#include <string>
#include <memory>
#include <atomic>
#include <shared_mutex>
#include "json.hpp"
#include <braft/raft.h>
#include <butil/endpoint.h>

class Config;
class Store;
class BatchedIndexer;

/**
 * RaftNodeManager owns and manages the braft::Node instance.
 * It encapsulates all node operations and provides a clean interface
 * for node management, making it easier to test and mock.
 */
class RaftNodeManager {
private:
    // Node ownership
    braft::Node* volatile node;
    mutable std::shared_mutex node_mutex;

    // Dependencies (not owned)
    const Config* config;
    Store* store;
    BatchedIndexer* batched_indexer;

    // Node configuration
    butil::EndPoint peering_endpoint;
    int api_port;
    int election_timeout_ms;
    bool api_uses_ssl;
    std::string nodes_config; // Store the nodes configuration for single node checks

    // Leader tracking
    butil::atomic<int64_t> leader_term;

    // Health status
    std::atomic<bool> read_caught_up;
    std::atomic<bool> write_caught_up;

public:
    /**
     * Constructor
     */
    RaftNodeManager(const Config* config,
                   Store* store,
                   BatchedIndexer* batched_indexer,
                   bool api_uses_ssl);

    /**
     * Destructor - ensures proper node cleanup
     */
    ~RaftNodeManager();

    /**
     * Initialize and start the Raft node
     * @param fsm The state machine to attach to the node
     * @param peering_endpoint The endpoint for this node
     * @param api_port The API port for this node
     * @param election_timeout_ms Election timeout in milliseconds
     * @param raft_dir Directory for Raft data
     * @param nodes Initial cluster configuration
     * @return 0 on success, error code on failure
     */
    int init_node(braft::StateMachine* fsm,
                  const butil::EndPoint& peering_endpoint,
                  int api_port,
                  int election_timeout_ms,
                  const std::string& raft_dir,
                  const std::string& nodes);

    /**
     * Wait for the node to become ready (leader or follower with leader)
     * @param timeout_ms Maximum time to wait in milliseconds
     * @param quit_signal External signal to abort waiting
     * @return true if ready, false if timeout or quit
     */
    bool wait_until_ready(int timeout_ms, const std::atomic<bool>& quit_signal);

    /**
     * Shutdown the node gracefully
     */
    void shutdown();

    /**
     * Apply a task to the Raft log
     * @param task The task to apply
     */
    void apply(braft::Task& task);

    /**
     * Trigger a snapshot
     * @param done Closure to call when snapshot completes
     */
    void snapshot(braft::Closure* done);

    /**
     * Change cluster peers configuration
     * @param new_conf New configuration
     * @param done Closure to call when complete
     */
    void change_peers(const braft::Configuration& new_conf, braft::Closure* done);

    /**
     * Reset peers (unsafe - only for single node recovery)
     * @param new_conf New configuration
     * @return Status of the operation
     */
    butil::Status reset_peers(const braft::Configuration& new_conf);

    /**
     * Trigger an election
     * @return Status of the operation
     */
    butil::Status trigger_vote();

    /**
     * Get current node status
     * @param status Output parameter for status
     */
    void get_status(braft::NodeStatus* status) const;

    /**
     * Check if this node is the leader (thread-safe)
     */
    bool is_leader() const;

    /**
     * Get the leader's peer ID
     */
    braft::PeerId leader_id() const;

    /**
     * Get node ID
     */
    braft::NodeId node_id() const;

    /**
     * Check if node is ready to serve reads
     */
    bool is_read_ready() const { return read_caught_up; }

    /**
     * Check if node is ready to serve writes
     */
    bool is_write_ready() const { return write_caught_up; }

    /**
     * Update catchup status based on current state
     * @param log_msg Whether to log status messages
     */
    void refresh_catchup_status(bool log_msg);

    /**
     * Get current leader term
     */
    int64_t get_leader_term() const {
        return leader_term.load(butil::memory_order_acquire);
    }

    /**
     * Set leader term (called by state machine)
     */
    void set_leader_term(int64_t term) {
        leader_term.store(term, butil::memory_order_release);
    }

    /**
     * Get JSON status for monitoring
     */
    nlohmann::json get_status() const;

    /**
     * Get URL for the current leader
     */
    std::string get_leader_url() const;

    /**
     * Refresh node membership
     * @param nodes New nodes configuration
     * @param allow_single_node_reset Allow reset for single node
     */
    void refresh_nodes(const std::string& nodes, bool allow_single_node_reset);

    /**
     * Log current node status (for debugging/monitoring)
     * @param node_status The status to log
     * @param prefix Optional prefix for the log message
     */
    void log_node_status(const braft::NodeStatus& node_status, const std::string& prefix = "") const;

private:
    /**
     * Check health with leader (for followers)
     */
    void check_leader_health(const braft::NodeStatus& local_status);

};

// Helper closure for refresh_nodes
class RefreshNodesClosure : public braft::Closure {
public:
    void Run();
};