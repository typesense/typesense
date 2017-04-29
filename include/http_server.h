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
#include "collection.h"
#include "collection_manager.h"

struct http_res {
    uint32_t status_code;
    std::string body;

    void send_200(const std::string & res_body) {
        status_code = 200;
        body = res_body;
    }

    void send_201(const std::string & res_body) {
        status_code = 201;
        body = res_body;
    }

    void send_400(const std::string & message) {
        status_code = 400;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_404() {
        status_code = 404;
        body = "{\"message\": \"Not Found\"}";
    }

    void send_409(const std::string & message) {
        status_code = 400;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_500(const std::string & res_body) {
        status_code = 500;
        body = res_body;
    }
};

struct http_req {
    std::map<std::string, std::string> params;
    std::string body;
};

struct route_path {
    std::string http_method;
    std::vector<std::string> path_parts;
    void (*handler)(http_req & req, http_res &);

    inline bool operator< (const route_path& rhs) const {
        return true;
    }
};

class HttpServer {
private:
    static h2o_globalconf_t config;
    static h2o_context_t ctx;
    static h2o_accept_ctx_t accept_ctx;
    static std::vector<route_path> routes;

    h2o_hostconf_t *hostconf;

    static void on_accept(h2o_socket_t *listener, const char *err);

    int create_listener();

    h2o_pathconf_t *register_handler(h2o_hostconf_t *hostconf, const char *path,
                                     int (*on_req)(h2o_handler_t *, h2o_req_t *));

    static const char* get_status_reason(uint32_t status_code);

    static std::map<std::string, std::string> parse_query(const std::string& query);

    static int catch_all_handler(h2o_handler_t *self, h2o_req_t *req);

public:
    HttpServer();

    ~HttpServer();

    void get(const std::string & path, void (*handler)(http_req & req, http_res &));

    void post(const std::string & path, void (*handler)(http_req &, http_res &));

    void put(const std::string & path, void (*handler)(http_req &, http_res &));

    void del();

    int run();
};