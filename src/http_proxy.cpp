#include "http_proxy.h"
#include "logger.h"
#include <chrono>

using namespace std::chrono_literals;

HttpProxy::HttpProxy() : cache(30s){
}


http_proxy_res_t HttpProxy::call(const std::string& url, const std::string& method,
                                 const std::string& req_body,
                                 const std::unordered_map<std::string, std::string>& req_headers,
                                 const size_t timeout_ms) {
    HttpClient& client = HttpClient::get_instance();
    http_proxy_res_t res;
    if(method == "GET") {
        res.status_code = client.get_response(url, res.body, res.headers, req_headers, timeout_ms);
    } else if(method == "POST") {
        res.status_code = client.post_response(url, req_body, res.body, res.headers, req_headers, timeout_ms);
    } else if(method == "PUT") {
        res.status_code = client.put_response(url, req_body, res.body, res.headers, timeout_ms);
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


http_proxy_res_t HttpProxy::send(const std::string& url, const std::string& method, const std::string& req_body,
                                 std::unordered_map<std::string, std::string>& req_headers) {
    // check if url is in cache
    uint64_t key = StringUtils::hash_wy(url.c_str(), url.size());
    key = StringUtils::hash_combine(key, StringUtils::hash_wy(method.c_str(), method.size()));
    key = StringUtils::hash_combine(key, StringUtils::hash_wy(req_body.c_str(), req_body.size()));

    size_t timeout_ms = default_timeout_ms;
    size_t num_try = default_num_try;

    if(req_headers.find("timeout_ms") != req_headers.end()){
        timeout_ms = std::stoul(req_headers.at("timeout_ms"));
        req_headers.erase("timeout_ms");
    }

    if(req_headers.find("num_try") != req_headers.end()){
        num_try = std::stoul(req_headers.at("num_try"));
        req_headers.erase("num_try");
    }

    for(auto& header : req_headers){
        key = StringUtils::hash_combine(key, StringUtils::hash_wy(header.first.c_str(), header.first.size()));
        key = StringUtils::hash_combine(key, StringUtils::hash_wy(header.second.c_str(), header.second.size()));
    }

    std::shared_lock slock(mutex);
    if(cache.contains(key)){
        return cache[key];
    }

    slock.unlock();

    http_proxy_res_t res;
    for(size_t i = 0; i < num_try; i++){
        res = call(url, method, req_body, req_headers, timeout_ms);

        if(res.status_code != 408 && res.status_code < 500){
            break;
        }

        LOG(ERROR) << "Proxy call failed, status_code: " << res.status_code
                   << ", timeout_ms:  " << timeout_ms << ", try: " << i+1 << ", num_try: " << num_try;
    }

    if(res.status_code == 408){
        nlohmann::json j;
        j["message"] = "Server error on remote server. Please try again later.";
        res.body = j.dump();
    }

    // add to cache 
    if(res.status_code == 200){
        std::unique_lock ulock(mutex);
        cache.insert(key, res);
    }

    return res;
}