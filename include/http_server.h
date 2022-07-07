#pragma once

#define H2O_USE_LIBUV 0

extern "C" {
    #include "h2o.h"
    #include "h2o/http1.h"
    #include "h2o/http2.h"
    #include "h2o/multithread.h"
}

#include <map>
#include <string>
#include <cstdio>
#include "http_data.h"
#include "option.h"
#include "threadpool.h"

class ReplicationState;
class HttpServer;

struct h2o_custom_req_handler_t {
    h2o_handler_t super;
    HttpServer* http_server;
};

struct h2o_custom_generator_t {
    h2o_generator_t h2o_generator;
    h2o_custom_req_handler_t* h2o_handler;
    route_path* rpath;
    std::shared_ptr<http_req> request;
    std::shared_ptr<http_res> response;

    std::shared_ptr<http_req>& req() {
        return request;
    }

    std::shared_ptr<http_res>& res() {
        return response;
    }
};

struct stream_response_state_t {
private:

    h2o_req_t* req = nullptr;

public:

    bool is_req_early_exit = false;
    bool is_req_http1 = true;

    bool is_res_start = true;
    h2o_send_state_t send_state = H2O_SEND_STATE_IN_PROGRESS;
    h2o_iovec_t res_body{};

    h2o_generator_t* generator = nullptr;

    explicit stream_response_state_t(h2o_req_t* _req): req(_req) {
        if(req != nullptr) {
            is_res_start = (req->res.status == 0);
        }
    }

    void set_response(uint32_t status_code, const std::string& content_type, const std::string& body) {
        res_body = h2o_strdup(&req->pool, body.c_str(), SIZE_MAX);

        if(is_res_start) {
            req->res.status = status_code;
            req->res.reason = http_res::get_status_reason(status_code);
            h2o_add_header(&req->pool, &req->res.headers, H2O_TOKEN_CONTENT_TYPE, NULL,
                           content_type.c_str(), content_type.size());
        }
    }

    h2o_req_t* get_req() {
        return req;
    }
};

struct deferred_req_res_t {
    const std::shared_ptr<http_req> req;
    const std::shared_ptr<http_res> res;
    HttpServer* server;

    // used to manage lifecycle of async actions
    bool destroy_after_use;

    deferred_req_res_t(const std::shared_ptr<http_req> &req, const std::shared_ptr<http_res> &res,
                       HttpServer *server, bool destroy_after_use) :
            req(req), res(res), server(server), destroy_after_use(destroy_after_use) {}

};

struct async_req_res_t {
    // NOTE: care must be taken to ensure that concurrent writes are protected as some fields are also used by http lib
private:
    // not exposed or accessed, here only for reference counting
    const std::shared_ptr<http_req> req;
    const std::shared_ptr<http_res> res;

public:

    // used to manage lifecycle of async actions
    const bool destroy_after_use;

    // stores http lib related datastructures to avoid race conditions between indexing and http write threads
    stream_response_state_t res_state;

    async_req_res_t(const std::shared_ptr<http_req>& h_req, const std::shared_ptr<http_res>& h_res,
                    const bool destroy_after_use) :
            req(h_req), res(h_res), destroy_after_use(destroy_after_use),
            res_state((std::shared_lock(res->mres), h_req->is_diposed ? nullptr : h_req->_req)) {

        std::shared_lock lk(res->mres);

        if(!res->is_alive || req->_req == nullptr || res->generator == nullptr) {
            return;
        }

        // ***IMPORTANT***
        // We limit writing to fields of `res_state.req` to prevent race conditions with http thread
        // Check `HttpServer::stream_response()` for overlapping writes.

        h2o_custom_generator_t* res_generator = static_cast<h2o_custom_generator_t*>(res->generator.load());

        res_state.is_req_early_exit = (res_generator->rpath->async_req && res->final && !req->last_chunk_aggregate);
        res_state.is_req_http1 = req->is_http_v1;
        res_state.send_state = res->final ? H2O_SEND_STATE_FINAL : H2O_SEND_STATE_IN_PROGRESS;
        res_state.generator = (res_generator == nullptr) ? nullptr : &res_generator->h2o_generator;
        res_state.set_response(res->status_code, res->content_type_header, res->body);
    }

    bool is_alive() {
        return res->is_alive;
    }

    void req_notify() {
        return req->notify();
    }

    void res_notify() {
        return res->notify();
    }
};

struct defer_processing_t {
    const std::shared_ptr<http_req> req;
    const std::shared_ptr<http_res> res;
    size_t timeout_ms;
    HttpServer* server;

    defer_processing_t(const std::shared_ptr<http_req> &req, const std::shared_ptr<http_res> &res,
                       size_t timeout_ms, HttpServer* server)
            : req(req), res(res), timeout_ms(timeout_ms), server(server) {}

};

class HttpServer {
private:
    h2o_globalconf_t config;
    h2o_compress_args_t compress_args;
    h2o_context_t ctx;
    h2o_accept_ctx_t* accept_ctx;
    h2o_hostconf_t *hostconf;
    h2o_socket_t* listener_socket;

    static const size_t ACTIVE_STREAM_WINDOW_SIZE = 196605;
    static const size_t REQ_TIMEOUT_MS = 60000;

    const uint64_t SSL_REFRESH_INTERVAL_MS;

    h2o_custom_timer_t ssl_refresh_timer;
    h2o_custom_timer_t metrics_refresh_timer;

    http_message_dispatcher* message_dispatcher;

    ReplicationState* replication_state;

    std::atomic<bool> exit_loop;

    std::string version;

    // must be a vector since order of routes entered matter
    std::vector<std::pair<uint64_t, route_path>> route_hash_to_path;

    // also have a hashmap for quick lookup of individual routes
    std::unordered_map<uint64_t, route_path> route_hash_to_path_map;

    const std::string listen_address;

    const uint32_t listen_port;

    std::string ssl_cert_path;

    std::string ssl_cert_key_path;

    bool cors_enabled;

    std::set<std::string> cors_domains;

    ThreadPool* thread_pool;

    ThreadPool* meta_thread_pool;

    bool (*auth_handler)(std::map<std::string, std::string>& params,
                         std::vector<nlohmann::json>& embedded_params_vec,
                         const std::string& body, const route_path& rpath,
                         const std::string& auth_key);

    static void on_accept(h2o_socket_t *listener, const char *err);

    int setup_ssl(const char *cert_file, const char *key_file);

    static bool initialize_ssl_ctx(const char *cert_file, const char *key_file, h2o_accept_ctx_t* accept_ctx);

    static void on_ssl_refresh_timeout(h2o_timer_t *entry);

    static void on_ssl_ctx_delete_timeout(h2o_timer_t *entry);

    static void on_metrics_refresh_timeout(h2o_timer_t *entry);

    int create_listener();

    h2o_pathconf_t *register_handler(h2o_hostconf_t *hostconf, const char *path,
                                     int (*on_req)(h2o_handler_t *, h2o_req_t *));

    static int catch_all_handler(h2o_handler_t *_h2o_handler, h2o_req_t *req);

    static void response_proceed(h2o_generator_t *generator, h2o_req_t *req);

    static void response_abort(h2o_generator_t *generator, h2o_req_t *req);

    static void on_res_generator_dispose(void *self);

    static int send_response(h2o_req_t *req, int status_code, const std::string & message);

    static int async_req_cb(void *ctx, h2o_iovec_t chunk, int is_end_stream);

    static bool is_write_request(const std::string& root_resource, const std::string& http_method);

public:
    HttpServer(const std::string & version,
               const std::string & listen_address, uint32_t listen_port,
               const std::string & ssl_cert_path,
               const std::string & ssl_cert_key_path,
               const uint64_t ssl_refresh_interval_ms,
               bool cors_enabled, const std::set<std::string>& cors_domains,
               ThreadPool* thread_pool);

    ~HttpServer();

    http_message_dispatcher* get_message_dispatcher() const;

    ReplicationState* get_replication_state() const;

    bool is_alive() const;

    bool is_leader() const;

    uint64_t node_state() const;

    nlohmann::json node_status();

    void set_auth_handler(bool (*handler)(std::map<std::string, std::string>& params,
                                          std::vector<nlohmann::json>& embedded_params_vec, const std::string& body,
                                          const route_path & rpath, const std::string & auth_key));

    void get(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void post(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void put(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void patch(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void del(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void on(const std::string & message, bool (*handler)(void*));

    void send_message(const std::string & type, void* data);

    static void stream_response(stream_response_state_t& state);

    uint64_t find_route(const std::vector<std::string> & path_parts, const std::string & http_method,
                    route_path** found_rpath);

    bool get_route(uint64_t hash, route_path** found_rpath);

    int run(ReplicationState* replication_state);

    void stop();

    bool has_exited() const;

    void clear_timeouts(const std::vector<h2o_timer_t*> & timers, bool trigger_callback = true);

    static bool on_stop_server(void *data);

    static bool on_stream_response_message(void *data);

    static bool on_request_proceed_message(void *data);

    static bool on_deferred_processing_message(void *data);

    std::string get_version();

    ThreadPool* get_thread_pool() const;

    ThreadPool* get_meta_thread_pool() const;

    static constexpr const char* STOP_SERVER_MESSAGE = "STOP_SERVER";
    static constexpr const char* STREAM_RESPONSE_MESSAGE = "STREAM_RESPONSE";
    static constexpr const char* REQUEST_PROCEED_MESSAGE = "REQUEST_PROCEED";
    static constexpr const char* DEFER_PROCESSING_MESSAGE = "DEFER_PROCESSING";

    static int process_request(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response,
                               route_path *rpath, const h2o_custom_req_handler_t *req_handler,
                               bool use_meta_thread_pool);

    static void on_deferred_process_request(h2o_timer_t *entry);

    void defer_processing(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res, size_t timeout_ms);

    void do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);

    bool trigger_vote();

    void persist_applying_index();

    int64_t get_num_queued_writes();
};
