#include "raft_server.h"
#include "raft_config.h"
#include "raft_http.h"
#include "store.h"
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

// Raft Server - Main Implementation
// This file implements the ReplicationState class using RaftNodeManager for node management
// Extracted functionality is available through:
// - raft_config namespace: DNS & Configuration utilities
// - raft_http namespace: HTTP Processing utilities
// - RaftNodeManager class: Node Management & Status

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

// Constructor - Initialize ReplicationState with dependency injection
ReplicationState::ReplicationState(HttpServer* server, BatchedIndexer* batched_indexer,
                                 Store *store, Store* analytics_store, ThreadPool* thread_pool,
                                 http_message_dispatcher *message_dispatcher,
                                 bool api_uses_ssl, const Config* config,
                                 size_t num_collections_parallel_load, size_t num_documents_parallel_load,
                                 RaftNodeManager* node_manager):
        node_manager(node_manager),
        server(server), batched_indexer(batched_indexer),
        store(store), analytics_store(analytics_store),
        thread_pool(thread_pool), message_dispatcher(message_dispatcher), api_uses_ssl(api_uses_ssl),
        config(config),
        num_collections_parallel_load(num_collections_parallel_load),
        num_documents_parallel_load(num_documents_parallel_load),
        ready(false), shutting_down(false), pending_writes(0), snapshot_in_progress(false),
        last_snapshot_ts(std::time(nullptr)), snapshot_interval_s(config->get_snapshot_interval_seconds()) {

    LOG(INFO) << "ReplicationState initialized with injected RaftNodeManager";
}

// Initialize state machine configuration (called by RaftCoordinator)
int ReplicationState::initialize(const butil::EndPoint& peering_endpoint,
                                int api_port,
                                int election_timeout_ms,
                                const std::string& raft_dir) {

    LOG(INFO) << "Initializing ReplicationState configuration";

    // Set state machine configuration
    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;
    this->peering_endpoint = peering_endpoint;

    LOG(INFO) << "ReplicationState configuration initialized";
    return 0;
}

// Main ReplicationState method implementations

// Simple getters and utility methods
http_message_dispatcher* ReplicationState::get_message_dispatcher() const {
    return message_dispatcher;
}

// Process write requests through Raft
void ReplicationState::write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    if(shutting_down) {
        response->set_503("Shutting down.");
        response->final = true;
        response->is_alive = false;
        request->notify();
        return ;
    }

    // reject write if disk space is running out
    auto resource_check = cached_resource_stat_t::get_instance().has_enough_resources(raft_dir_path,
                                  config->get_disk_used_max_percentage(), config->get_memory_used_max_percentage());

    if (resource_check != cached_resource_stat_t::OK && request->do_resource_check()) {
        response->set_422("Rejecting write: running out of resource type: " +
                          std::string(magic_enum::enum_name(resource_check)));
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    if(config->get_skip_writes() && request->path_without_query != "/config") {
        response->set_422("Skipping writes.");
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

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

    if(!node_manager || !node_manager->is_leader()) {
        return write_to_leader(request, response);
    }

    //check if it's first gzip chunk or is gzip stream initialized
    if(((request->body.size() > 2) &&
        (31 == (int)request->body[0] && -117 == (int)request->body[1])) || request->zstream_initialized) {
        auto res = raft_http::handle_gzip(request);

        if(!res.ok()) {
            response->set_422(res.error());
            response->final = true;
            auto req_res = new async_req_res_t(request, response, true);
            return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        }
    }

    // Serialize request to replicated WAL so that all the nodes in the group receive it as well.
    // NOTE: actual write must be done only on the `on_apply` method to maintain consistency.

    butil::IOBufBuilder bufBuilder;
    bufBuilder << request->to_json();

    // Apply this log as a braft::Task
    braft::Task task;
    task.data = &bufBuilder.buf();
    // This callback would be invoked when the task actually executes or fails
    task.done = new ReplicationClosure(request, response);

    // To avoid ABA problem
    task.expected_term = node_manager ? node_manager->get_leader_term() : -1;

    // Now the task is applied to the group
    if(node_manager) {
        node_manager->apply(task);
    }

    pending_writes++;
}

void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    // NOT USED:
    // For consistency, reads to followers could be rejected.
    // Currently, we don't do implement reads via raft.
}

// Delegate node management methods to node_manager
void ReplicationState::refresh_nodes(const std::string& nodes, size_t raft_counter,
                                   const std::atomic<bool>& reset_peers_on_error) {
    if (node_manager) {
        bool allow_single_node_reset = (raft_counter > 0 && reset_peers_on_error.load());
        node_manager->refresh_nodes(nodes, allow_single_node_reset);
    }
}

void ReplicationState::refresh_catchup_status(bool log_msg) {
    if (node_manager) {
        node_manager->refresh_catchup_status(log_msg);
    }
}

bool ReplicationState::trigger_vote() {
    if (node_manager) {
        auto status = node_manager->trigger_vote();
        LOG(INFO) << "Triggered vote. Ok? " << status.ok() << ", status: " << status;
        return status.ok();
    }
    return false;
}

bool ReplicationState::reset_peers() {
    if(!node_manager) {
        return false;
    }

    const Option<std::string> & refreshed_nodes_op = Config::fetch_nodes_config(config->get_nodes());
    if(!refreshed_nodes_op.ok()) {
        LOG(WARNING) << "Error while fetching peer configuration: " << refreshed_nodes_op.error();
        return false;
    }

    const std::string& nodes_config = raft_config::to_nodes_config(peering_endpoint,
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

void ReplicationState::persist_applying_index() {
    if(batched_indexer) {
        batched_indexer->persist_applying_index();
    }
}

uint64_t ReplicationState::node_state() const {
    if(!node_manager) {
        return 0;
    }

    braft::NodeStatus node_status;
    node_manager->get_status(&node_status);
    return node_status.state;
}

void ReplicationState::write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
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
            // streaming in progress: ensure graceful termination (cannot start response again)
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
        // indicates async request body of in-flight request
        request->notify();
        return ;
    }

    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t *>(response->generator.load());
    HttpServer* server = custom_generator->h2o_handler->http_server;

    auto raw_req = request->_req;
    const std::string& path = std::string(raw_req->path.base, raw_req->path.len);
    const std::string& scheme = std::string(raw_req->scheme->name.base, raw_req->scheme->name.len);
    const std::string url = raft_config::get_node_url_path(leader_id, path, scheme);

    thread_pool->enqueue([request, response, server, path, url, this]() {
        pending_writes++;

        std::map<std::string, std::string> res_headers;

        if(request->http_method == "POST") {
            std::vector<std::string> path_parts;
            StringUtils::split(path, path_parts, "/");

            if(path_parts.back().rfind("import", 0) == 0) {
                // imports are handled asynchronously
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
            // timeout: 0 since delete can take a long time
            long status = HttpClient::delete_response(url, api_res, res_headers, 0, true);
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

// Snapshot management methods
void ReplicationState::do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req,
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
        node_manager->snapshot(snapshot_closure);
    });
}

void ReplicationState::do_snapshot(const std::string& nodes) {
    auto current_ts = std::time(nullptr);
    if(current_ts - last_snapshot_ts < snapshot_interval_s) {
        return;
    }

    LOG(INFO) << "Snapshot timer is active, current_ts: " << current_ts << ", last_snapshot_ts: " << last_snapshot_ts;

    if(is_leader()) {
        // run the snapshot only if there are no other recovering followers
        std::vector<braft::PeerId> peers;
        braft::Configuration peer_config;
        peer_config.parse_from(nodes);
        peer_config.list_peers(&peers);

        std::string my_addr = node_manager->node_id().peer_id.to_string();

        bool all_peers_healthy = true;

        // iterate peers and check health status
        for(const auto& peer: peers) {
            const std::string& peer_addr = peer.to_string();

            if(my_addr == peer_addr) {
                // skip self
                continue;
            }

            const std::string protocol = api_uses_ssl ? "https" : "http";
            std::string url = raft_config::get_node_url_path(peer, "/health", protocol);
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
