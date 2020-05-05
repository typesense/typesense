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

class ReplicationState;
class HttpServer;

struct h2o_custom_timeout_entry_t {
    h2o_timeout_entry_t timeout_entry{};
    HttpServer *server;

    h2o_custom_timeout_entry_t(): server(nullptr) {}

    explicit h2o_custom_timeout_entry_t(HttpServer *server): server(server) {

    }
};

class HttpServer {
private:
    h2o_globalconf_t config;
    h2o_compress_args_t compress_args;
    h2o_context_t ctx;
    h2o_accept_ctx_t* accept_ctx;
    h2o_hostconf_t *hostconf;
    h2o_socket_t* listener_socket;

    h2o_timeout_t ssl_refresh_timeout;
    h2o_custom_timeout_entry_t custom_timeout_entry;

    http_message_dispatcher* message_dispatcher;

    ReplicationState* replication_state;

    bool exit_loop = false;

    std::string version;

    std::vector<std::pair<uint64_t, route_path>> routes;

    const std::string listen_address;

    const uint32_t listen_port;

    std::string ssl_cert_path;

    std::string ssl_cert_key_path;

    bool cors_enabled;

    bool (*auth_handler)(http_req& req, const route_path& rpath, const std::string& auth_key);

    static void on_accept(h2o_socket_t *listener, const char *err);

    int setup_ssl(const char *cert_file, const char *key_file);

    static void on_ssl_refresh_timeout(h2o_timeout_entry_t *entry);

    int create_listener();

    h2o_pathconf_t *register_handler(h2o_hostconf_t *hostconf, const char *path,
                                     int (*on_req)(h2o_handler_t *, h2o_req_t *));

    static std::map<std::string, std::string> parse_query(const std::string& query);

    static int catch_all_handler(h2o_handler_t *self, h2o_req_t *req);

    static int send_response(h2o_req_t *req, int status_code, const std::string & message);

public:
    HttpServer(const std::string & version,
               const std::string & listen_address, uint32_t listen_port,
               const std::string & ssl_cert_path, const std::string & ssl_cert_key_path, bool cors_enabled);

    ~HttpServer();

    http_message_dispatcher* get_message_dispatcher() const;

    ReplicationState* get_replication_state() const;

    bool is_alive() const;

    uint64_t node_state() const;

    void set_auth_handler(bool (*handler)(http_req & req, const route_path & rpath,
                          const std::string & auth_key));

    void get(const std::string & path, bool (*handler)(http_req & req, http_res & res), bool async = false);

    void post(const std::string & path, bool (*handler)(http_req & req, http_res & res), bool async = false);

    void put(const std::string & path, bool (*handler)(http_req & req, http_res & res), bool async = false);

    void del(const std::string & path, bool (*handler)(http_req & req, http_res & res), bool async = false);

    void on(const std::string & message, bool (*handler)(void*));

    void send_message(const std::string & type, void* data);

    void send_response(http_req* request, const http_res* response);

    uint64_t find_route(const std::vector<std::string> & path_parts, const std::string & http_method,
                    route_path** found_rpath);

    bool get_route(uint64_t hash, route_path** found_rpath);

    int run(ReplicationState* replication_state);

    void stop();

    void clear_timeouts(const std::vector<h2o_timeout_t*> & timeouts, bool trigger_callback = true);

    static bool on_stop_server(void *data);

    std::string get_version();

    static constexpr const char* AUTH_HEADER = "x-typesense-api-key";
    static constexpr const char* STOP_SERVER_MESSAGE = "STOP_SERVER";
};