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
#include <stdio.h>
#include "http_data.h"
#include "collection.h"
#include "collection_manager.h"

struct request_response {
    http_req* req;
    http_res* response;
};

class HttpServer {
private:
    h2o_globalconf_t config;
    h2o_context_t ctx;
    h2o_accept_ctx_t* accept_ctx;
    h2o_hostconf_t *hostconf;
    h2o_socket_t* listener_socket;
    h2o_multithread_queue_t* message_queue;
    h2o_multithread_receiver_t* message_receiver;
    bool exit_loop = false;

    std::vector<route_path> routes;

    std::map<std::string, void (*)(void*)> message_handlers;

    const std::string listen_address;

    const uint32_t listen_port;

    std::string ssl_cert_path;

    std::string ssl_cert_key_path;

    static void on_accept(h2o_socket_t *listener, const char *err);

    int setup_ssl(const char *cert_file, const char *key_file);

    int create_listener();

    h2o_pathconf_t *register_handler(h2o_hostconf_t *hostconf, const char *path,
                                     int (*on_req)(h2o_handler_t *, h2o_req_t *));

    static const char* get_status_reason(uint32_t status_code);

    static std::map<std::string, std::string> parse_query(const std::string& query);

    static int catch_all_handler(h2o_handler_t *self, h2o_req_t *req);

    static void on_message(h2o_multithread_receiver_t *receiver, h2o_linklist_t *messages);

    static int send_401_unauthorized(h2o_req_t *req);

    static constexpr const char* SEND_RESPONSE_MSG = "send_response";

public:
    HttpServer(std::string listen_address, uint32_t listen_port,
               std::string ssl_cert_path, std::string ssl_cert_key_path);

    ~HttpServer();

    void get(const std::string & path, void (*handler)(http_req & req, http_res & res), bool authenticated, bool async = false);

    void post(const std::string & path, void (*handler)(http_req & req, http_res & res), bool authenticated, bool async = false);

    void put(const std::string & path, void (*handler)(http_req & req, http_res & res), bool authenticated, bool async = false);

    void del(const std::string & path, void (*handler)(http_req & req, http_res & res), bool authenticated, bool async = false);

    void on(const std::string & message, void (*handler)(void*));

    void send_message(const std::string & type, void* data);

    void send_response(http_req* request, const http_res* response);

    int run();

    void stop();

    void clear_timeouts(std::vector<h2o_timeout_t*> & timeouts);

    static void on_stop_server(void *data);

    static constexpr const char* AUTH_HEADER = "x-typesense-api-key";
    static constexpr const char* STOP_SERVER_MESSAGE = "STOP_SERVER";

};