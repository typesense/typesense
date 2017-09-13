#pragma once

#include <stdint.h>
#include <string>
#include <map>
#include <vector>

#define H2O_USE_LIBUV 0
extern "C" {
    #include "h2o.h"
}

class HttpServer;

struct http_res {
    uint32_t status_code;
    std::string body;
    HttpServer* server;

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

    void send_401(const std::string & message) {
        status_code = 400;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_403() {
        status_code = 403;
        body = "{\"message\": \"Forbidden\"}";
    }

    void send_404() {
        status_code = 404;
        body = "{\"message\": \"Not Found\"}";
    }

    void send_409(const std::string & message) {
        status_code = 409;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_422(const std::string & message) {
        status_code = 422;
        body = "{\"message\": \"" + message + "\"}";
    }

    void send_500(const std::string & res_body) {
        status_code = 500;
        body = res_body;
    }

    void send(uint32_t code, const std::string & message) {
        status_code = code;
        body = "{\"message\": \"" + message + "\"}";
    }
};

struct http_req {
    h2o_req_t* _req;
    std::map<std::string, std::string> params;
    std::string body;
};

struct route_path {
    std::string http_method;
    std::vector<std::string> path_parts;
    void (*handler)(http_req &, http_res &);
    bool authenticated;
    bool async;

    inline bool operator< (const route_path& rhs) const {
        return true;
    }
};