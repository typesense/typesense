#include "raft_state_machine.h"
#include "raft_config.h"
#include "raft_http.h"
#include "store.h"
#include <butil/files/file_enumerator.h>
#include <butil/files/file_path.h>
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
#include <logger.h>

// ============================================================================
// RAFT STATE MACHINE
// ============================================================================
//
// This file implements the full RaftStateMachine class that serves as:
// 1. HTTP Request Processing Layer - handles HTTP validation, routing, forwarding
// 2. braft::StateMachine Interface - processes Raft log entries and snapshots
// 3. Application Business Logic - coordinates between storage, indexing, and HTTP
// 4. Integration Point - bridges RaftNodeManager with application components
//
// Architecture:
//   HTTP Requests → RaftStateMachine (business validation) → Raft Log →
//   on_apply() → BatchedIndexer → Store → Database

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);
    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

// ============================================================================
// CONSTRUCTOR & INITIALIZATION
// ============================================================================

RaftStateMachine::RaftStateMachine(HttpServer* server, BatchedIndexer* batched_indexer,
                                   Store *store, Store* analytics_store, ThreadPool* thread_pool,
                                   http_message_dispatcher *message_dispatcher,
                                   bool api_uses_ssl, const Config* config,
                                   size_t num_collections_parallel_load, size_t num_documents_parallel_load):
        server(server), batched_indexer(batched_indexer),
        store(store), analytics_store(analytics_store),
        thread_pool(thread_pool), message_dispatcher(message_dispatcher),
        api_uses_ssl(api_uses_ssl),
        config(config),
        num_collections_parallel_load(num_collections_parallel_load),
        num_documents_parallel_load(num_documents_parallel_load),
        ready(false), shutting_down(false), pending_writes(0), snapshot_in_progress(false),
        snapshot_interval_s(config->get_snapshot_interval_seconds()),
        last_snapshot_ts(std::time(nullptr)) {

    node_manager = std::make_unique<RaftNodeManager>(config, store, batched_indexer, api_uses_ssl);

    LOG(INFO) << "RaftStateMachine initialized";
}

int RaftStateMachine::start(const butil::EndPoint& peering_endpoint,
                            int api_port,
                            int election_timeout_ms,
                            int snapshot_max_byte_count_per_rpc,
                            const std::string& raft_dir,
                            const std::string& nodes,
                            const std::atomic<bool>& quit_abruptly) {

    LOG(INFO) << "Starting RaftStateMachine";

    // Configure braft flags
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;
    braft::FLAGS_raft_max_byte_count_per_rpc = snapshot_max_byte_count_per_rpc;
    braft::FLAGS_raft_rpc_channel_connect_timeout_ms = 2000;

    // Set state machine configuration
    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;
    this->peering_endpoint = peering_endpoint;

    // Initialize the raft node through node manager, wiring it to this state machine
    int result = node_manager->init_node(this, peering_endpoint,
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
    if (init_db() != 0) {
        return -1;
    }

    LOG(INFO) << "RaftStateMachine started successfully";
    return 0;
}

// Initialize database after node is ready (called after Raft node startup)
int RaftStateMachine::init_db() {
    LOG(INFO) << "Loading collections from disk...";

    Option<bool> init_op = CollectionManager::get_instance().load(
        num_collections_parallel_load, num_documents_parallel_load
    );

    if(!init_op.ok()) {
        LOG(ERROR) << "Failed to load collections: " << init_op.error();
        return 1;
    }

    LOG(INFO) << "Finished loading collections from disk";

    // Initialize conversation models
    auto conversation_models_init = ConversationModelManager::init(store);
    if(!conversation_models_init.ok()) {
        LOG(INFO) << "Failed to initialize conversation model manager: " << conversation_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << conversation_models_init.get() << " conversation model(s)";
    }

    // Initialize batched indexer state
    if(batched_indexer) {
        LOG(INFO) << "Initializing batched indexer from snapshot state...";
        std::string batched_indexer_state_str;
        StoreStatus s = store->get(BATCHED_INDEXER_STATE_KEY, batched_indexer_state_str);
        if(s == FOUND) {
            nlohmann::json batch_indexer_state = nlohmann::json::parse(batched_indexer_state_str);
            batched_indexer->load_state(batch_indexer_state);
        }
    }

    // Initialize personalization models
    auto personalization_models_init = PersonalizationModelManager::init(store);
    if(!personalization_models_init.ok()) {
        LOG(INFO) << "Failed to initialize personalization model manager: " << personalization_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << personalization_models_init.get() << " personalization model(s)";
    }

    return 0;
}

// ============================================================================
// HTTP PROCESSING & BUSINESS LOGIC
// ============================================================================

// Primary entry point for write requests from HTTP layer
void RaftStateMachine::write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    // Shutdown Check
    if(shutting_down) {
        response->set_503("Shutting down.");
        response->final = true;
        response->is_alive = false;
        request->notify();
        return ;
    }

    // Resource Validation: Reject write if disk space/memory is running out
    auto resource_check = cached_resource_stat_t::get_instance().has_enough_resources(raft_dir_path,
                                  config->get_disk_used_max_percentage(), config->get_memory_used_max_percentage());

    if (resource_check != cached_resource_stat_t::OK && request->do_resource_check()) {
        response->set_422("Rejecting write: running out of resource type: " +
                          std::string(magic_enum::enum_name(resource_check)));
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    // Configuration Validation
    if(config->get_skip_writes() && request->path_without_query != "/config") {
        response->set_422("Skipping writes.");
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    // Route-specific Validation
    route_path* rpath = nullptr;
    bool route_found = server->get_route(request->route_hash, &rpath);

    if(route_found && rpath->handler == patch_update_collection) {
        if(get_alter_in_progress(request->params["collection"])) {
            response->set_422("Another collection update operation is in progress.");
            response->final = true;
            auto req_res = new async_req_res_t(request, response, true);
            return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        }
    }

    // Leadership check
    if(!node_manager || !node_manager->is_leader()) {
        return write_to_leader(request, response);
    }

    // Gzip Processing: Check if it's first gzip chunk or is gzip stream initialized
    if(((request->body.size() > 2) &&
        (31 == (int)request->body[0] && -117 == (int)request->body[1])) || request->zstream_initialized) {
        auto res = raft::http::handle_gzip(request);

        if(!res.ok()) {
            response->set_422(res.error());
            response->final = true;
            auto req_res = new async_req_res_t(request, response, true);
            return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        }
    }

    // Raft Submission: Serialize request to replicated WAL so all nodes in the cluster receive it
    // NOTE: actual write must be done only in the `on_apply` method to maintain consistency
    butil::IOBufBuilder bufBuilder;
    bufBuilder << request->to_json();

    // Apply this log as a braft::Task
    braft::Task task;
    task.data = &bufBuilder.buf();
    task.done = new ReplicationClosure(request, response);  // Callback for completion
    task.expected_term = node_manager ? node_manager->get_leader_term() : -1; // To avoid ABA problem

    // Submit to Raft cluster
    if(node_manager) {
        node_manager->apply(task);
    }

    pending_writes++;
}

void RaftStateMachine::read(const std::shared_ptr<http_res>& response) {
    // NOT USED: For consistency, reads to followers could be rejected.
    // Currently, we don't implement reads via raft - they go directly to followers.
}

// Handle write requests when this node is not the leader
void RaftStateMachine::write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    if(!node_manager) {
        response->set_500("Node manager not initialized.");
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    braft::PeerId leader_id = node_manager->leader_id();
    if(leader_id.is_empty()) {
        // Handle no leader scenario
        LOG(ERROR) << "Rejecting write: could not find a leader.";

        if(response->proxied_stream) {
            // Streaming in progress: ensure graceful termination (cannot start response again)
            LOG(ERROR) << "Terminating streaming request gracefully.";
            response->is_alive = false;
            request->notify();
            return ;
        }

        response->set_500("Could not find a leader.");
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    if (response->proxied_stream) {
        // Indicates async request body of in-flight request
        request->notify();
        return ;
    }

    // Extract HTTP request details for forwarding
    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t *>(response->generator.load());
    HttpServer* server = custom_generator->h2o_handler->http_server;

    auto raw_req = request->_req;
    const std::string& path = std::string(raw_req->path.base, raw_req->path.len);
    const std::string& scheme = std::string(raw_req->scheme->name.base, raw_req->scheme->name.len);
    const std::string url = raft::config::get_node_url_path(leader_id, path, scheme);

    // Forward request to leader asynchronously
    thread_pool->enqueue([request, response, server, path, url, this]() {
        pending_writes++;

        std::map<std::string, std::string> res_headers;

        // Handle different HTTP methods
        if(request->http_method == "POST") {
            std::vector<std::string> path_parts;
            StringUtils::split(path, path_parts, "/");

            if(path_parts.back().rfind("import", 0) == 0) {
                // Imports are handled asynchronously
                response->proxied_stream = true;
                long status = HttpClient::post_response_async(url, request, response, server, true);

                if(status == 500) {
                    response->content_type_header = res_headers["content-type"];
                    response->set_500("");
                } else {
                    return ;
                }
            } else {
                std::string api_res;
                long status = HttpClient::post_response(url, request->body, api_res, res_headers, {}, 0, true);
                response->content_type_header = res_headers["content-type"];
                response->set_body(status, api_res);
            }
        } else if(request->http_method == "PUT") {
            std::string api_res;
            long status = HttpClient::put_response(url, request->body, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "DELETE") {
            std::string api_res;
            long status = HttpClient::delete_response(url, api_res, res_headers, 0, true); // timeout: 0 since delete can take a long time
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "PATCH") {
            std::string api_res;
            long status = HttpClient::patch_response(url, request->body, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else {
            const std::string& err = "Forwarding for http method not implemented: " + request->http_method;
            LOG(ERROR) << err;
            response->set_500(err);
        }

        auto req_res = new async_req_res_t(request, response, true);
        message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        pending_writes--;
    });
}

// ============================================================================
// NODE MANAGEMENT DELEGATION (to RaftNodeManager)
// ============================================================================

void RaftStateMachine::refresh_nodes(const std::string& nodes, size_t raft_counter,
                                   const std::atomic<bool>& reset_peers_on_error) {
    if (node_manager) {
        bool allow_single_node_reset = (raft_counter > 0 && reset_peers_on_error.load());
        node_manager->refresh_nodes(nodes, allow_single_node_reset);
    }
}

void RaftStateMachine::refresh_catchup_status(bool log_msg) {
    if (node_manager) {
        node_manager->refresh_catchup_status(log_msg);
    }
}

bool RaftStateMachine::trigger_vote() {
    if (node_manager) {
        auto status = node_manager->trigger_vote();
        LOG(INFO) << "Triggered vote. Ok? " << status.ok() << ", status: " << status;
        return status.ok();
    }
    return false;
}

bool RaftStateMachine::reset_peers() {
    if(!node_manager) {
        return false;
    }

    const Option<std::string> & refreshed_nodes_op = Config::fetch_nodes_config(config->get_nodes());
    if(!refreshed_nodes_op.ok()) {
        LOG(WARNING) << "Error while fetching peer configuration: " << refreshed_nodes_op.error();
        return false;
    }

    const std::string& nodes_config = raft::config::to_nodes_config(peering_endpoint,
                                                                    config->get_api_port(),
                                                                    refreshed_nodes_op.get());

    if(nodes_config.empty()) {
        LOG(WARNING) << "No nodes resolved from peer configuration.";
        return false;
    }

    braft::Configuration peer_config;
    peer_config.parse_from(nodes_config);

    auto status = node_manager->reset_peers(peer_config);
    LOG(INFO) << "Reset peers. Ok? " << status.ok() << ", status: " << status;
    LOG(INFO) << "New peer config is: " << peer_config;
    return status.ok();
}

void RaftStateMachine::persist_applying_index() {
    if(batched_indexer) {
        batched_indexer->persist_applying_index();
    }
}

uint64_t RaftStateMachine::node_state() const {
    if(!node_manager) {
        return 0;
    }

    braft::NodeStatus node_status;
    node_manager->get_status(&node_status);
    return node_status.state;
}

// ============================================================================
// SNAPSHOT MANAGEMENT (Application-Level)
// ============================================================================

void RaftStateMachine::do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req,
                                   const std::shared_ptr<http_res>& res) {
    if(!node_manager) {
        res->set_500("Could not trigger a snapshot, as node manager is not initialized.");
        auto req_res = new async_req_res_t(req, res, true);
        get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return ;
    }

    if(snapshot_in_progress) {
        res->set_409("Another snapshot is in progress.");
        auto req_res = new async_req_res_t(req, res, true);
        get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return ;
    }

    LOG(INFO) << "Triggering an on demand snapshot"
              << (!snapshot_path.empty() ? " with external snapshot path..." : "...");

    thread_pool->enqueue([&snapshot_path, req, res, this]() {
        OnDemandSnapshotClosure* snapshot_closure = new OnDemandSnapshotClosure(this, req, res, snapshot_path,
                                                                                raft_dir_path);
        ext_snapshot_path = snapshot_path;

        if(!node_manager) {
            // Handle the case where node_manager becomes null after initial check
            req->last_chunk_aggregate = true;
            res->final = true;
            res->set_500("Node manager is not initialized.");
            auto req_res = new async_req_res_t(req, res, true);
            get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
            delete snapshot_closure;
            return;
        }

        node_manager->snapshot(snapshot_closure);
    });
}

void RaftStateMachine::do_snapshot(const std::string& nodes) {
    auto current_ts = std::time(nullptr);
    if(current_ts - last_snapshot_ts < snapshot_interval_s) {
        return;
    }

    LOG(INFO) << "Snapshot timer is active, current_ts: " << current_ts << ", last_snapshot_ts: " << last_snapshot_ts;

    if(is_leader()) {
        // Run the snapshot only if there are no other recovering followers
        std::vector<braft::PeerId> peers;
        braft::Configuration peer_config;
        peer_config.parse_from(nodes);
        peer_config.list_peers(&peers);

        std::string my_addr = node_manager->node_id().peer_id.to_string();

        bool all_peers_healthy = true;

        // Iterate peers and check health status
        for(const auto& peer: peers) {
            const std::string& peer_addr = peer.to_string();

            if(my_addr == peer_addr) {
                continue; // Skip self
            }

            const std::string protocol = api_uses_ssl ? "https" : "http";
            std::string url = raft::config::get_node_url_path(peer, "/health", protocol);
            std::string api_res;
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::get_response(url, api_res, res_headers, {}, 5*1000, true);
            bool peer_healthy = (status_code == 200);

            if(!peer_healthy) {
                LOG(WARNING) << "Peer " << peer_addr << " reported unhealthy during snapshot pre-check.";
            }

            all_peers_healthy = all_peers_healthy && peer_healthy;
        }

        if(!all_peers_healthy) {
            LOG(WARNING) << "Unable to trigger snapshot as one or more of the peers reported unhealthy.";
            return ;
        }
    }

    TimedSnapshotClosure* snapshot_closure = new TimedSnapshotClosure(this);
    if(node_manager) {
        node_manager->snapshot(snapshot_closure);
    }
    last_snapshot_ts = current_ts;
}

// ============================================================================
// BRAFT::STATEMACHINE INTERFACE IMPLEMENTATION
// ============================================================================

// Apply committed entries to the state machine (core Raft callback)
void RaftStateMachine::on_apply(braft::Iterator& iter) {
    // NOTE: this is executed on a different thread and runs concurrent to http thread
    for(; iter.valid(); iter.next()) {
        // Guard invokes done->Run() asynchronously to avoid blocking
        braft::AsyncClosureGuard closure_guard(iter.done());

        const std::shared_ptr<http_req>& request_generated = iter.done() ?
            dynamic_cast<ReplicationClosure*>(iter.done())->get_request() :
            std::make_shared<http_req>();

        const std::shared_ptr<http_res>& response_generated = iter.done() ?
            dynamic_cast<ReplicationClosure*>(iter.done())->get_response() :
            std::make_shared<http_res>(nullptr);

        if(!iter.done()) {
            // Log entry - deserialize request
            request_generated->load_from_json(iter.data().to_string());
        }

        request_generated->log_index = iter.index();

        // Queue for batch processing to avoid blocking Raft thread
        batched_indexer->enqueue(request_generated, response_generated);

        if(iter.done()) {
            pending_writes--;
        }
    }
}

// Create snapshot of current state machine (core Raft callback)
void RaftStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    LOG(INFO) << "on_snapshot_save";

    snapshot_in_progress = true;
    std::string db_snapshot_path = writer->get_path() + "/" + db_snapshot_name;
    std::string analytics_db_snapshot_path = writer->get_path() + "/" + analytics_db_snapshot_name;

    {
        // Grab batch indexer lock so that we can take a clean snapshot
        std::shared_mutex& pause_mutex = batched_indexer->get_pause_mutex();
        std::unique_lock lk(pause_mutex);

        nlohmann::json batch_index_state;
        batched_indexer->serialize_state(batch_index_state);
        store->insert(BATCHED_INDEXER_STATE_KEY, batch_index_state.dump());

        // We will delete all the skip indices in meta store and flush that DB
        // This will block writes, but should be pretty fast
        batched_indexer->clear_skip_indices();

        rocksdb::Checkpoint* checkpoint = nullptr;
        rocksdb::Status status = store->create_check_point(&checkpoint, db_snapshot_path);
        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);

        if(!status.ok()) {
            LOG(ERROR) << "Failure during checkpoint creation, msg:" << status.ToString();
            done->status().set_error(EIO, "Checkpoint creation failure.");
        }

        if(analytics_store) {
            // To ensure that in-memory table is sent to disk (we don't use WAL)
            analytics_store->flush();

            rocksdb::Checkpoint* checkpoint2 = nullptr;
            status = analytics_store->create_check_point(&checkpoint2, analytics_db_snapshot_path);
            std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint2);

            if(!status.ok()) {
                LOG(ERROR) << "AnalyticsStore : Failure during checkpoint creation, msg:" << status.ToString();
                done->status().set_error(EIO, "AnalyticsStore : Checkpoint creation failure.");
            }
        }
    }

    SnapshotArg* arg = new SnapshotArg;
    arg->replication_state = this;
    arg->writer = writer;
    arg->state_dir_path = raft_dir_path;
    arg->db_snapshot_path = db_snapshot_path;
    arg->done = done;

    if(analytics_store) {
        arg->analytics_db_snapshot_path = analytics_db_snapshot_path;
    }

    if(!ext_snapshot_path.empty()) {
        arg->ext_snapshot_path = ext_snapshot_path;
    }

    // Start a new bthread to avoid blocking StateMachine for slower operations that don't need a blocking view
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

// Load snapshot to restore state machine (core Raft callback)
int RaftStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    // Critical safety check - leader should NEVER load a snapshot
    CHECK(!node_manager || !node_manager->is_leader())
        << "Leader is not supposed to load snapshot";

    LOG(INFO) << "on_snapshot_load";

    // Ensure reads/writes are rejected during reload, as `store->reload()` unique locks the DB handle
    if(node_manager) {
        // This will set read_caught_up and write_caught_up to false internally
        node_manager->refresh_catchup_status(false);
    }

    // Load analytics snapshot from leader, replacing the running StateMachine
    std::string analytics_snapshot_path = reader->get_path();
    analytics_snapshot_path.append(std::string("/") + analytics_db_snapshot_name);

    if(analytics_store && directory_exists(analytics_snapshot_path)) {
        // Analytics db snapshot could be missing (older version or disabled earlier)
        int reload_store = analytics_store->reload(true, analytics_snapshot_path,
                                                   config->get_analytics_db_ttl());
        if(reload_store != 0) {
            LOG(ERROR) << "Failed to reload analytics db snapshot";
            return reload_store;
        }
    }

    // Load main DB snapshot
    std::string db_snapshot_path = reader->get_path();
    db_snapshot_path.append(std::string("/") + db_snapshot_name);

    int reload_store = store->reload(true, db_snapshot_path);
    if(reload_store != 0) {
        return reload_store;
    }

    // Reinitialize database from loaded snapshot
    return init_db();
}

// ============================================================================
// INTERNAL UTILITY METHODS
// ============================================================================

void RaftStateMachine::shutdown() {
    LOG(INFO) << "Set shutting_down = true";
    shutting_down = true;

    // Wait for pending writes to drop to zero
    LOG(INFO) << "Waiting for in-flight writes to finish...";
    while(pending_writes.load() != 0) {
        LOG(INFO) << "pending_writes: " << pending_writes;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    LOG(INFO) << "RaftStateMachine shutdown, store sequence: " << store->get_latest_seq_number();

    // Shutdown the RaftNodeManager
    if (node_manager) {
        node_manager->shutdown();
    }
}

void RaftStateMachine::do_dummy_write() {
    if(!node_manager) {
        LOG(ERROR) << "Could not do a dummy write, as node manager is not initialized";
        return;
    }

    braft::PeerId leader_id = node_manager->leader_id();
    if(leader_id.is_empty()) {
        LOG(ERROR) << "Could not do a dummy write, as node does not have a leader";
        return;
    }

    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = raft::config::get_node_url_path(leader_id, "/health", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::post_response(url, "", api_res, res_headers, {}, 4000, true);

    LOG(INFO) << "Dummy write to " << url << ", status = " << status_code << ", response = " << api_res;
}

// Static method for snapshot file operations (runs in separate thread)
void* RaftStateMachine::save_snapshot(void* arg) {
    LOG(INFO) << "save_snapshot called";

    SnapshotArg* sa = static_cast<SnapshotArg*>(arg);
    std::unique_ptr<SnapshotArg> arg_guard(sa);

    // Add the db snapshot files to writer state
    butil::FileEnumerator dir_enum(butil::FilePath(sa->db_snapshot_path), false, butil::FileEnumerator::FILES);

    for (butil::FilePath file = dir_enum.Next(); !file.empty(); file = dir_enum.Next()) {
        std::string file_name = std::string(db_snapshot_name) + "/" + file.BaseName().value();
        if (sa->writer->add_file(file_name) != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer.");
            sa->replication_state->snapshot_in_progress = false;
            return nullptr;
        }
    }

    if(!sa->analytics_db_snapshot_path.empty()) {
        // Add analytics db snapshot files to writer state
        butil::FileEnumerator analytics_dir_enum(butil::FilePath(sa->analytics_db_snapshot_path), false,
                                                 butil::FileEnumerator::FILES);
        for (butil::FilePath file = analytics_dir_enum.Next(); !file.empty(); file = analytics_dir_enum.Next()) {
            auto file_name = std::string(analytics_db_snapshot_name) + "/" + file.BaseName().value();
            if (sa->writer->add_file(file_name) != 0) {
                sa->done->status().set_error(EIO, "Fail to add analytics file to writer.");
                sa->replication_state->snapshot_in_progress = false;
                return nullptr;
            }
        }
    }

    sa->done->Run();

    // NOTE: *must* do a dummy write here since snapshots cannot be triggered if no write has happened since the
    // last snapshot. By doing a dummy write right after a snapshot, we ensure that this can never be the case.
    sa->replication_state->do_dummy_write();

    LOG(INFO) << "save_snapshot done";

    return nullptr;
}

// ============================================================================
// SNAPSHOT CLOSURE IMPLEMENTATIONS
// ============================================================================

void ReplicationClosure::Run() {
    // Auto delete `this` after Run() - handled by coordination layer
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

OnDemandSnapshotClosure::OnDemandSnapshotClosure(RaftStateMachine* replication_state,
                                                 const std::shared_ptr<http_req>& req,
                                                 const std::shared_ptr<http_res>& res,
                                                 const std::string& ext_snapshot_path,
                                                 const std::string& state_dir_path)
    : replication_state(replication_state), req(req), res(res),
      ext_snapshot_path(ext_snapshot_path), state_dir_path(state_dir_path) {}

void OnDemandSnapshotClosure::Run() {
    // Auto delete this after Done()
    std::unique_ptr<OnDemandSnapshotClosure> self_guard(this);

    bool ext_snapshot_succeeded = false;

    // Copy snapshot to external path if requested
    if(!ext_snapshot_path.empty()) {
        const butil::FilePath& dest_state_dir = butil::FilePath(ext_snapshot_path + "/state");

        if(!butil::DirectoryExists(dest_state_dir)) {
            butil::CreateDirectory(dest_state_dir, true);
        }

        const butil::FilePath& src_snapshot_dir = butil::FilePath(state_dir_path + "/snapshot");
        const butil::FilePath& src_meta_dir = butil::FilePath(state_dir_path + "/meta");

        bool snapshot_copied = butil::CopyDirectory(src_snapshot_dir, dest_state_dir, true);
        bool meta_copied = butil::CopyDirectory(src_meta_dir, dest_state_dir, true);

        ext_snapshot_succeeded = snapshot_copied && meta_copied;
    }

    // Clear external snapshot path and mark complete
    // Order is important, because the atomic boolean guards write to the path
    replication_state->set_ext_snapshot_path("");
    replication_state->set_snapshot_in_progress(false);

    req->last_chunk_aggregate = true;
    res->final = true;

    nlohmann::json response;
    uint32_t status_code;

    if(!status().ok()) {
        // In case of internal raft error
        LOG(ERROR) << "On demand snapshot failed, error: " << status().error_str() << ", code: " << status().error_code();
        status_code = 500;
        response["success"] = false;
        response["error"] = status().error_str();
    } else if(!ext_snapshot_succeeded && !ext_snapshot_path.empty()) {
        LOG(ERROR) << "On demand snapshot failed, error: copy failed.";
        status_code = 500;
        response["success"] = false;
        response["error"] = "Copy failed.";
    } else {
        LOG(INFO) << "On demand snapshot succeeded!";
        status_code = 201;
        response["success"] = true;
    }

    res->status_code = status_code;
    res->body = response.dump();

    auto req_res = new async_req_res_t(req, res, true);
    replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);

    // Wait for response to be sent
    res->wait();
}

TimedSnapshotClosure::TimedSnapshotClosure(RaftStateMachine* replication_state)
    : replication_state(replication_state) {}

void TimedSnapshotClosure::Run() {
    std::unique_ptr<TimedSnapshotClosure> self_guard(this);

    if(status().ok()) {
        LOG(INFO) << "Timed snapshot succeeded";
    } else {
        LOG(ERROR) << "Timed snapshot failed: " << status().error_str();
    }

    replication_state->set_snapshot_in_progress(false);
}
