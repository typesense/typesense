#include "http_proxy.h"
#include "logger.h"
#include <chrono>

using namespace std::chrono_literals;

HttpProxy::HttpProxy() : cache(30s){
}


http_proxy_res_t HttpProxy::send(const std::string& url, const std::string& method, const std::string& body, const std::unordered_map<std::string, std::string>& headers) {
    // check if url is in cache
    std::string key;
    key += url;
    key += method;
    key += body;
    for (auto& header : headers) {
        key += header.first;
        key += header.second;
    }
    if (cache.contains(key)) {
        return cache[key];
    }
    // if not, make http request
    HttpClient& client = HttpClient::get_instance();
    http_proxy_res_t res;

    if(method == "GET") {
        res.status_code = client.get_response(url, res.body, res.headers, headers);
    } else if(method == "POST") {
        res.status_code = client.post_response(url, body, res.body, res.headers, headers);
    } else if(method == "PUT") {
        res.status_code = client.put_response(url, body, res.body, res.headers);
    } else if(method == "DELETE") {
        res.status_code = client.delete_response(url, res.body, res.headers);
    } else {
        res.status_code = 400;
        res.body = "Invalid method: " + method;
    }

    // add to cache
    cache.insert(key, res);

    return res;
}