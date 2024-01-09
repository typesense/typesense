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

    static size_t curl_write_stream(char *buffer, size_t size, size_t nmemb, void *context);

    static size_t curl_write_async_done(void* context, curl_socket_t item);

    static size_t curl_write_stream_done(void* context, curl_socket_t item);

    static size_t curl_write_download(void *ptr, size_t size, size_t nmemb, FILE *stream);

    static CURL* init_curl(const std::string& url, std::string& response, const size_t timeout_ms = 0);

    static CURL* init_curl_async(const std::string& url, deferred_req_res_t* req_res, curl_slist*& chunk,
                                 bool send_ts_api_header);
    static CURL* init_curl_stream(const std::string& url, async_stream_response_t& res, long timeout_ms);

    static size_t curl_req_send_callback(char* buffer, size_t size, size_t nitems, void *userdata);

    static long perform_curl(CURL *curl, std::map<std::string, std::string>& res_headers,
                             struct curl_slist *chunk = nullptr,
                             bool send_ts_api_header = false);
public:
    static HttpClient & get_instance() {
        static HttpClient instance;
        return instance;
    }

    HttpClient(HttpClient const&) = delete;
    void operator=(HttpClient const&) = delete;

    void init(const std::string & api_key);

    static long download_file(const std::string& url, const std::string& file_path);

    static long get_response(const std::string& url, std::string& response,
                             std::map<std::string, std::string>& res_headers,
                             const std::unordered_map<std::string, std::string>& headers = {},
                             long timeout_ms=4000,
                             bool send_ts_api_header = false);

    static long delete_response(const std::string& url, std::string& response,
                                std::map<std::string, std::string>& res_headers, long timeout_ms=4000,
                                bool send_ts_api_header = false);

    static long post_response(const std::string & url, const std::string & body, std::string & response,
                              std::map<std::string, std::string>& res_headers,
                              const std::unordered_map<std::string, std::string>& headers = {},
                              long timeout_ms=4000,
                              bool send_ts_api_header = false);

    static long post_response_async(const std::string &url, const std::shared_ptr<http_req> request,
                                    const std::shared_ptr<http_res> response,
                                    HttpServer* server,
                                    bool send_ts_api_header = false);

    static long post_response_stream(const std::string &url, const std::string &body, async_stream_response_t &response,
                                     std::map<std::string, std::string>& res_headers,
                                     const std::unordered_map<std::string, std::string>& headers, long timeout_ms=4000);

    static long put_response(const std::string & url, const std::string & body, std::string & response,
                             std::map<std::string, std::string>& res_headers, long timeout_ms=4000,
                             bool send_ts_api_header = false);

    static long patch_response(const std::string & url, const std::string & body, std::string & response,
                               std::map<std::string, std::string>& res_headers, long timeout_ms=4000,
                               bool send_ts_api_header = false);

    static void extract_response_headers(CURL* curl, std::map<std::string, std::string> &res_headers);
};
