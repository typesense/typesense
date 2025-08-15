#include "store.h"
#include "raft_server.h"
#include <butil/files/file_enumerator.h>
#include <thread>
#include <algorithm>
#include <string_utils.h>
#include <file_utils.h>
#include <collection_manager.h>
#include <http_client.h>
#include <conversation_model_manager.h>
#include "rocksdb/utilities/checkpoint.h"
#include "thread_local_vars.h"
#include "core_api.h"
#include "personalization_model_manager.h"

// Raft Server - Slim Coordinator
// This file now coordinates the extracted modules:
// - raft_config.cpp: DNS & Configuration  
// - raft_http.cpp: HTTP Processing
// - raft_lifecycle_manager.cpp: Raft Lifecycle & Snapshots
// - raft_node_manager.cpp: Node Management & Status

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);

    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

void ReplicationClosure::Run() {
    // Auto delete `this` after Run() - handled by coordination layer
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

// Constructor - Initialize the coordination layer
ReplicationState::ReplicationState(HttpServer* server, BatchedIndexer* batched_indexer,
                                 Store *store, Store* analytics_store, ThreadPool* thread_pool,
                                 http_message_dispatcher *message_dispatcher,
                                 bool api_uses_ssl, const Config* config,
                                 size_t num_collections_parallel_load, size_t num_documents_parallel_load):
        node(nullptr), leader_term(-1), server(server), batched_indexer(batched_indexer),
        store(store), analytics_store(analytics_store),
        thread_pool(thread_pool), message_dispatcher(message_dispatcher), api_uses_ssl(api_uses_ssl),
        config(config),
        num_collections_parallel_load(num_collections_parallel_load),
        num_documents_parallel_load(num_documents_parallel_load),
        read_caught_up(false), write_caught_up(false),
        ready(false), shutting_down(false), pending_writes(0), snapshot_in_progress(false),
        last_snapshot_ts(std::time(nullptr)), snapshot_interval_s(config->get_snapshot_interval_seconds()) {
    
    LOG(INFO) << "ReplicationState coordinator initialized";
}

// Core coordination method - delegates to lifecycle manager
int ReplicationState::start(const butil::EndPoint & peering_endpoint, const int api_port,
                            int election_timeout_ms, int snapshot_max_byte_count_per_rpc,
                            const std::string & raft_dir, const std::string & nodes,
                            const std::atomic<bool>& quit_abruptly) {

    LOG(INFO) << "Starting Raft coordination layer";
    
    // Set coordinator state
    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;
    this->peering_endpoint = peering_endpoint;
    this->read_caught_up = false;
    this->write_caught_up = false;

    // Configure braft flags
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;
    braft::FLAGS_raft_max_byte_count_per_rpc = snapshot_max_byte_count_per_rpc;
    braft::FLAGS_raft_rpc_channel_connect_timeout_ms = 2000;

    // Delegate actual raft node startup to lifecycle manager
    int result = start_raft_node(peering_endpoint, api_port, election_timeout_ms, 
                                snapshot_max_byte_count_per_rpc, raft_dir, nodes, quit_abruptly);
    
    if (result == 0) {
        LOG(INFO) << "Raft coordination layer start completed";
    }
    
    return result;
}

// Coordination layer delegates to appropriate modules

// Simple getters and utility methods
http_message_dispatcher* ReplicationState::get_message_dispatcher() const {
    return message_dispatcher;
}

Store* ReplicationState::get_store() {
    return store;
}

// NOTE: All large method implementations moved to specialized modules:
// - DNS & Configuration methods → raft_config.cpp
// - HTTP Processing methods → raft_http.cpp  
// - Lifecycle & Snapshot methods → raft_lifecycle_manager.cpp
// - Node Management methods → raft_node_manager.cpp

// The main coordinator maintains only essential coordination logic
// All method implementations have been moved to the appropriate module files

// This completes the slim coordinator - all complex logic has been extracted
