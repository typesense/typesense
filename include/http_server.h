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
    h2o_generator_t super;
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

struct deferred_req_res_t {
    const std::shared_ptr<http_req> req;
    const std::shared_ptr<http_res> res;
    HttpServer* server;

    // used to manage lifecycle of non-async responses
    bool destroy_after_stream_response;

    deferred_req_res_t(const std::shared_ptr<http_req> &req, const std::shared_ptr<http_res> &res,
                       HttpServer *server, bool destroy_after_stream_response = false) :
            req(req), res(res), server(server), destroy_after_stream_response(destroy_after_stream_response) {}

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

    // must be a vector since order of routes matter
    std::vector<std::pair<uint64_t, route_path>> routes;

    const std::string listen_address;

    const uint32_t listen_port;

    std::string ssl_cert_path;

    std::string ssl_cert_key_path;

    bool cors_enabled;

    ThreadPool* thread_pool;

    bool (*auth_handler)(std::map<std::string, std::string>& params, const std::string& body, const route_path& rpath,
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

public:
    HttpServer(const std::string & version,
               const std::string & listen_address, uint32_t listen_port,
               const std::string & ssl_cert_path,
               const std::string & ssl_cert_key_path,
               const uint64_t ssl_refresh_interval_ms,
               bool cors_enabled, ThreadPool* thread_pool);

    ~HttpServer();

    http_message_dispatcher* get_message_dispatcher() const;

    ReplicationState* get_replication_state() const;

    bool is_alive() const;

    uint64_t node_state() const;

    void set_auth_handler(bool (*handler)(std::map<std::string, std::string>& params, const std::string& body,
                                          const route_path & rpath, const std::string & auth_key));

    void get(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void post(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void put(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void patch(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void del(const std::string & path, bool (*handler)(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res), bool async_req=false, bool async_res=false);

    void on(const std::string & message, bool (*handler)(void*));

    void send_message(const std::string & type, void* data);

    void send_response(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response);

    static void destroy_request_response(const std::shared_ptr<http_req>& request,
                                         const std::shared_ptr<http_res>& response);

    static void stream_response(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response);

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

    static constexpr const char* STOP_SERVER_MESSAGE = "STOP_SERVER";
    static constexpr const char* STREAM_RESPONSE_MESSAGE = "STREAM_RESPONSE";
    static constexpr const char* REQUEST_PROCEED_MESSAGE = "REQUEST_PROCEED";
    static constexpr const char* DEFER_PROCESSING_MESSAGE = "DEFER_PROCESSING";

    static int process_request(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response, route_path *rpath,
                               const h2o_custom_req_handler_t *req_handler);

    static void on_deferred_process_request(h2o_timer_t *entry);

    void defer_processing(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res, size_t timeout_ms);

    void do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);

    bool trigger_vote();

    void persist_applying_index();
};
