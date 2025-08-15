#include "raft_server.h"
#include "raft_node_manager.h"
#include <logger.h>
#include <braft/raft.h>

RaftServer::RaftServer(HttpServer* server,
                       BatchedIndexer* batched_indexer,
                       Store* store,
                       Store* analytics_store,
                       ThreadPool* thread_pool,
                       http_message_dispatcher* message_dispatcher,
                       bool api_uses_ssl,
                       const Config* config,
                       size_t num_collections_parallel_load,
                       size_t num_documents_parallel_load) {

    LOG(INFO) << "Creating RaftServer components";

    // Create RaftNodeManager first (no dependencies)
    node_manager = std::make_unique<RaftNodeManager>(config, store, batched_indexer, api_uses_ssl);

    // Create ReplicationState with dependency injection of RaftNodeManager
    state_machine = std::make_unique<ReplicationState>(
        server, batched_indexer, store, analytics_store,
        thread_pool, message_dispatcher, api_uses_ssl, config,
        num_collections_parallel_load, num_documents_parallel_load,
        node_manager.get()  // Inject the node manager
    );

    LOG(INFO) << "RaftServer initialized with clean component separation";
}

RaftServer::~RaftServer() {
    shutdown();
}

int RaftServer::start(const butil::EndPoint& peering_endpoint,
                      int api_port,
                      int election_timeout_ms,
                      int snapshot_max_byte_count_per_rpc,
                      const std::string& raft_dir,
                      const std::string& nodes,
                      const std::atomic<bool>& quit_abruptly) {

    LOG(INFO) << "Starting RaftServer";

    // Configure braft flags
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;
    braft::FLAGS_raft_max_byte_count_per_rpc = snapshot_max_byte_count_per_rpc;
    braft::FLAGS_raft_rpc_channel_connect_timeout_ms = 2000;

    // Initialize the state machine's configuration
    int state_machine_init = state_machine->initialize(
        peering_endpoint, api_port, election_timeout_ms, raft_dir
    );
    if (state_machine_init != 0) {
        return state_machine_init;
    }

    // Initialize the raft node through node manager, wiring it to the state machine
    int result = node_manager->init_node(state_machine.get(), peering_endpoint,
                                        api_port, election_timeout_ms, raft_dir, nodes);
    if (result != 0) {
        return result;
    }

    // Wait for node to be ready
    const int WAIT_FOR_RAFT_TIMEOUT_MS = 60 * 1000;
    if (!node_manager->wait_until_ready(WAIT_FOR_RAFT_TIMEOUT_MS, quit_abruptly)) {
        return -1;
    }

    // Initialize database after node is ready
    if (state_machine->init_db() != 0) {
        return -1;
    }

    LOG(INFO) << "RaftServer started successfully";
    return 0;
}

void RaftServer::shutdown() {
    LOG(INFO) << "Shutting down RaftServer";
    state_machine->shutdown();

    // Note: RaftNodeManager shutdown is handled by ReplicationState::shutdown()
    // to maintain proper shutdown order and avoid double-shutdown

    LOG(INFO) << "RaftServer shutdown complete";
}
