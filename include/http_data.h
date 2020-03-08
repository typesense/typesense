#pragma once

#include <stdint.h>
#include <string>
#include <map>
#include <vector>
#include "json.hpp"

#define H2O_USE_LIBUV 0
extern "C" {
    #include "h2o.h"
}

class HttpServer;

struct http_res {
    uint32_t status_code;
    std::string content_type_header;
    std::string body;
    HttpServer* server;
    bool final;

    http_res(): content_type_header("application/json; charset=utf-8"), final(true) {

    }

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
    std::string path_without_query;
    std::map<std::string, std::string> params;
    std::string body;

    http_req() {}

    http_req(h2o_req_t* _req, const std::string & path_without_query, const std::map<std::string, std::string> & params,
            std::string body): _req(_req), path_without_query(path_without_query), params(params), body(body) {}

    void deserialize(const std::string& serialized_content) {
        nlohmann::json content = nlohmann::json::parse(serialized_content);
        path_without_query = content["path"];
        body = content["body"];

        for (nlohmann::json::iterator it = content["params"].begin(); it != content["params"].end(); ++it) {
            params.emplace(it.key(), it.value());
        }

        _req = nullptr;
    }

    std::string serialize() const {
        nlohmann::json content;
        content["path"] = path_without_query;
        content["params"] = params;
        content["body"] = body;

        return content.dump();
    }
};

struct route_path {
    std::string http_method;
    std::vector<std::string> path_parts;
    void (*handler)(http_req &, http_res &);
    bool async;

    inline bool operator< (const route_path& rhs) const {
        return true;
    }
};