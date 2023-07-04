#include "http_proxy.h"
#include "logger.h"
#include <chrono>

using namespace std::chrono_literals;

HttpProxy::HttpProxy() : cache(30s){
}


http_proxy_res_t HttpProxy::call(const std::string& url, const std::string& method, const std::string& body, 
                                const std::unordered_map<std::string, std::string>& headers, const size_t timeout_ms) {
    HttpClient& client = HttpClient::get_instance();
    http_proxy_res_t res;

    if(method == "GET") {
        res.status_code = client.get_response(url, res.body, res.headers, headers, timeout_ms);
    } else if(method == "POST") {
        res.status_code = client.post_response(url, body, res.body, res.headers, headers, timeout_ms);
    } else if(method == "PUT") {
        res.status_code = client.put_response(url, body, res.body, res.headers, timeout_ms);
    } else if(method == "DELETE") {
        res.status_code = client.delete_response(url, res.body, res.headers, timeout_ms);
    } else {
        res.status_code = 400;
        nlohmann::json j;
        j["message"] = "Parameter `method` must be one of GET, POST, PUT, DELETE.";
        res.body =  j.dump();
    }
    return res;
}


http_proxy_res_t HttpProxy::send(const std::string& url, const std::string& method, const std::string& body, std::unordered_map<std::string, std::string>& headers) {
    // check if url is in cache
    uint64_t key = StringUtils::hash_wy(url.c_str(), url.size());
    key = StringUtils::hash_combine(key, StringUtils::hash_wy(method.c_str(), method.size()));
    key = StringUtils::hash_combine(key, StringUtils::hash_wy(body.c_str(), body.size()));
    for(auto& header : headers){
        key = StringUtils::hash_combine(key, StringUtils::hash_wy(header.first.c_str(), header.first.size()));
        key = StringUtils::hash_combine(key, StringUtils::hash_wy(header.second.c_str(), header.second.size()));
    }
    if(cache.contains(key)){
        return cache[key];
    }

    size_t timeout_ms = 30000;
    size_t num_retries = 2;

    if(headers.find("remote_embedding_timeout_ms") != headers.end()) {
        std::stringstream ss(headers["remote_embedding_timeout_ms"]);
        ss >> timeout_ms;

        headers.erase("remote_embedding_timeout_ms");
    }


    if(headers.find("remote_embedding_num_retries") != headers.end()) {
        std::stringstream ss(headers["remote_embedding_num_retries"]);
        ss >> num_retries;

        headers.erase("remote_embedding_num_retries");
    }

    http_proxy_res_t res;
    for(size_t i = 0;i < num_retries;i++) {
        res = call(url, method, body, headers, timeout_ms);
        
        if(res.status_code != 500) {
            break;
        }
    }


    if(res.status_code == 500){
        nlohmann::json j;
        j["message"] = "Server error on remote server. Please try again later.";
        res.body = j.dump();
    }
    

    // add to cache 
    if(res.status_code != 500){
        cache.insert(key, res);
    }

 
    return res;
}