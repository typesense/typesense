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
        static HttpProxy& get_instance() {
            static HttpProxy instance;
            return instance;
        }
        HttpProxy(const HttpProxy&) = delete;
        void operator=(const HttpProxy&) = delete;
        HttpProxy(HttpProxy&&) = delete;
        void operator=(HttpProxy&&) = delete;
        http_proxy_res_t send(const std::string& url, const std::string& method, const std::string& body, const std::unordered_map<std::string, std::string>& headers);
    private:
        HttpProxy();
        ~HttpProxy() = default;


        // lru cache for http requests
        LRU::TimedCache<uint64_t, http_proxy_res_t> cache;
};