#include "store.h"
#include "raft_server.h"
#include <string_utils.h>
#include <http_client.h>
#include <collection_manager.h>
#include "core_api.h"
#include <zlib.h>

// Raft HTTP Request Processing Module  
// Extracted from raft_server.cpp for better organization

Option<bool> ReplicationState::handle_gzip(const std::shared_ptr<http_req>& request) {
    if (!request->zstream_initialized) {
        request->zs.zalloc = Z_NULL;
        request->zs.zfree = Z_NULL;
        request->zs.opaque = Z_NULL;
        request->zs.avail_in = 0;
        request->zs.next_in = Z_NULL;

        if (inflateInit2(&request->zs, 16 + MAX_WBITS) != Z_OK) {
            return Option<bool>(400, "inflateInit failed while decompressing");
        }

        request->zstream_initialized = true;
    }

    std::string outbuffer;
    outbuffer.resize(10 * request->body.size());

    request->zs.next_in = (Bytef *) request->body.c_str();
    request->zs.avail_in = request->body.size();
    std::size_t size_uncompressed = 0;
    int ret = 0;
    do {
        request->zs.avail_out = static_cast<unsigned int>(outbuffer.size());
        request->zs.next_out = reinterpret_cast<Bytef *>(&outbuffer[0] + size_uncompressed);
        ret = inflate(&request->zs, Z_FINISH);
        if (ret != Z_STREAM_END && ret != Z_OK && ret != Z_BUF_ERROR) {
            std::string error_msg = request->zs.msg;
            inflateEnd(&request->zs);
            return Option<bool>(400, error_msg);
        }

        size_uncompressed += (outbuffer.size() - request->zs.avail_out);
    } while (request->zs.avail_out == 0);

    if (ret == Z_STREAM_END) {
        request->zstream_initialized = false;
        inflateEnd(&request->zs);
    }

    outbuffer.resize(size_uncompressed);

    request->body = outbuffer;
    request->chunk_len = outbuffer.size();

    return Option<bool>(true);
}

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

    std::shared_lock lock(node_mutex);

    if(!node) {
        return ;
    }

    if (!node->is_leader()) {
        return write_to_leader(request, response);
    }

    //check if it's first gzip chunk or is gzip stream initialized
    if(((request->body.size() > 2) &&
        (31 == (int)request->body[0] && -117 == (int)request->body[1])) || request->zstream_initialized) {
        auto res = handle_gzip(request);

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
    task.expected_term = leader_term.load(butil::memory_order_relaxed);

    // Now the task is applied to the group
    node->apply(task);

    pending_writes++;
}

void ReplicationState::write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    // no lock on `node` needed as caller uses the lock
    if(!node || node->leader_id().is_empty()) {
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

    const braft::PeerId& leader_addr = node->leader_id();

    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t *>(response->generator.load());
    HttpServer* server = custom_generator->h2o_handler->http_server;

    auto raw_req = request->_req;
    const std::string& path = std::string(raw_req->path.base, raw_req->path.len);
    const std::string& scheme = std::string(raw_req->scheme->name.base, raw_req->scheme->name.len);
    const std::string url = get_node_url_path(leader_addr, path, scheme);

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



void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    // NOT USED:
    // For consistency, reads to followers could be rejected.
    // Currently, we don't do implement reads via raft.
}
