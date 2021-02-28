#pragma once

#include <string>
#include <map>
#include <curl/curl.h>
#include "http_data.h"
#include "http_server.h"

/*
  NOTE: This is a really primitive blocking client meant only for specific Typesense use cases.
*/
class HttpClient {
private:
    static std::string api_key;
    static std::string ca_cert_path;

    HttpClient() = default;

    ~HttpClient() = default;

    static size_t curl_write(char *contents, size_t size, size_t nmemb, std::string *s);

    static size_t curl_write_async(char *buffer, size_t size, size_t nmemb, void* context);

    static size_t curl_write_async_done(void* context, curl_socket_t item);

    static CURL* init_curl(const std::string& url, std::string& response);

    static CURL* init_curl_async(const std::string& url, deferred_req_res_t* req_res, curl_slist*& chunk);

    static size_t curl_req_send_callback(char* buffer, size_t size, size_t nitems, void *userdata);

    static long perform_curl(CURL *curl, std::map<std::string, std::string>& res_headers);

public:
    static HttpClient & get_instance() {
        static HttpClient instance;
        return instance;
    }

    HttpClient(HttpClient const&) = delete;
    void operator=(HttpClient const&) = delete;

    void init(const std::string & api_key);

    static long get_response(const std::string& url, std::string& response,
                             std::map<std::string, std::string>& res_headers, long timeout_ms=4000);

    static long delete_response(const std::string& url, std::string& response,
                                std::map<std::string, std::string>& res_headers, long timeout_ms=120000);

    static long post_response(const std::string & url, const std::string & body, std::string & response,
                              std::map<std::string, std::string>& res_headers, long timeout_ms=4000);

    static long post_response_async(const std::string &url, const std::shared_ptr<http_req> request,
                                    const std::shared_ptr<http_res> response,
                                    HttpServer* server);

    static long put_response(const std::string & url, const std::string & body, std::string & response,
                             std::map<std::string, std::string>& res_headers, long timeout_ms=4000);

    static long patch_response(const std::string & url, const std::string & body, std::string & response,
                             std::map<std::string, std::string>& res_headers, long timeout_ms=4000);

    static void extract_response_headers(CURL* curl, std::map<std::string, std::string> &res_headers);
};
