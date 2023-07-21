#pragma once

#include <unordered_map>
#include <string>
#include "http_client.h"
#include "lru/lru.hpp"


struct http_proxy_res_t {
    std::string body;
    std::map<std::string, std::string> headers;
    long status_code;

    bool operator==(const http_proxy_res_t& other) const {
        return body == other.body && headers == other.headers && status_code == other.status_code;
    }

    bool operator!=(const http_proxy_res_t& other) const {
        return !(*this == other);
    }
};


class HttpProxy {
    // singleton class for http proxy
    public:
        static const size_t default_timeout_ms = 60000;
        static const size_t default_num_try = 2;

        static HttpProxy& get_instance() {
            static HttpProxy instance;
            return instance;
        }
        HttpProxy(const HttpProxy&) = delete;
        void operator=(const HttpProxy&) = delete;
        HttpProxy(HttpProxy&&) = delete;
        void operator=(HttpProxy&&) = delete;
        http_proxy_res_t send(const std::string& url, const std::string& method, const std::string& req_body, std::unordered_map<std::string, std::string>& req_headers);
    private:
        HttpProxy();
        ~HttpProxy() = default;
        http_proxy_res_t call(const std::string& url, const std::string& method,
                              const std::string& req_body = "", const std::unordered_map<std::string, std::string>& req_headers = {},
                              const size_t timeout_ms = default_timeout_ms);


        // lru cache for http requests
        std::shared_mutex mutex;
        LRU::TimedCache<uint64_t, http_proxy_res_t> cache;
};