#pragma once

#define H2O_USE_LIBUV 0

extern "C" {
    #include "h2o.h"
    #include "h2o/http1.h"
    #include "h2o/http2.h"
}

#include <map>
#include <string>
#include <stdio.h>
#include "http_data.h"
#include "collection.h"
#include "collection_manager.h"

class HttpServer {
private:
    h2o_globalconf_t config;
    h2o_context_t ctx;
    h2o_accept_ctx_t* accept_ctx;

    std::vector<route_path> routes;

    const std::string listen_address;

    const uint32_t listen_port;

    h2o_hostconf_t *hostconf;

    static void on_accept(h2o_socket_t *listener, const char *err);

    int create_listener();

    h2o_pathconf_t *register_handler(h2o_hostconf_t *hostconf, const char *path,
                                     int (*on_req)(h2o_handler_t *, h2o_req_t *));

    static const char* get_status_reason(uint32_t status_code);

    static std::map<std::string, std::string> parse_query(const std::string& query);

    static int catch_all_handler(h2o_handler_t *self, h2o_req_t *req);

    static int send_401_unauthorized(h2o_req_t *req);

public:
    HttpServer(std::string listen_address, uint32_t listen_port);

    ~HttpServer();

    void get(const std::string & path, void (*handler)(http_req & req, http_res &), bool authenticated);

    void post(const std::string & path, void (*handler)(http_req &, http_res &), bool authenticated);

    void put(const std::string & path, void (*handler)(http_req &, http_res &), bool authenticated);

    void del(const std::string & path, void (*handler)(http_req &, http_res &), bool authenticated);

    int run();

    static constexpr const char* AUTH_HEADER = "x-typesense-api-key";
};