#pragma once

#include <cstdint>
#include <string>
#include <map>
#include <curl/curl.h>
#include "http_data.h"
#include "http_server.h"

/*
  NOTE: This is a really primitive blocking client meant only for specific Typesense use cases.
*/

// timing split of the last blocking transfer on this thread, num_connects > 0 means a fresh connection
struct http_transfer_metrics_t {
    double namelookup_ms = 0;
    double connect_ms = 0;
    double appconnect_ms = 0;
    double starttransfer_ms = 0;
    double total_ms = 0;
    long num_connects = 0;
};

class HttpClient {
public:
    enum class SSLVerifyMode {
        VERIFY,
        NO_VERIFY
    };

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

    static CURL* init_curl(const std::string& url, std::string& response, const size_t timeout_ms = 0,
                           SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static CURL* init_curl_async(const std::string& url, deferred_req_res_t* req_res, curl_slist*& chunk,
                                 bool send_ts_api_header,
                                 SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static CURL* init_curl_stream(const std::string& url, async_stream_response_t& res, long timeout_ms,
                                  SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static CURL* init_curl_sse(const std::string& url, long timeout_ms,
                                  deferred_req_res_t* req_res,
                                  SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static void configure_ssl(CURL* curl, const std::string& url, SSLVerifyMode ssl_verify_mode);

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

    void init(const std::string & api_key, const std::string& ca_cert_path = "");

    // call before curl_global_cleanup, waits briefly for leased handles and drains the idle pool
    static void shutdown_curl_pool();

    static http_transfer_metrics_t get_last_transfer_metrics();

    // compare before and after a call to attribute the metrics above to it
    static uint64_t get_transfer_count();

    static size_t get_idle_handle_count();

    // test seams over the pool internals
    static CURL* lease_handle_for_test();
    static void release_handle_for_test(CURL* curl);

    static long download_file(const std::string& url, const std::string& file_path,
                              SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long get_response(const std::string& url, std::string& response,
                             std::map<std::string, std::string>& res_headers,
                             const std::unordered_map<std::string, std::string>& headers = {},
                             long timeout_ms=4000,
                             bool send_ts_api_header = false,
                             SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long get_response_verified(const std::string& url, std::string& response,
                                      std::map<std::string, std::string>& res_headers,
                                      const std::unordered_map<std::string, std::string>& headers = {},
                                      long timeout_ms=4000,
                                      bool send_ts_api_header = false);

    static long delete_response(const std::string& url, std::string& response,
                                std::map<std::string, std::string>& res_headers, long timeout_ms=4000,
                                bool send_ts_api_header = false,
                                SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long post_response(const std::string & url, const std::string & body, std::string & response,
                              std::map<std::string, std::string>& res_headers,
                              const std::unordered_map<std::string, std::string>& headers = {},
                              long timeout_ms=4000,
                              bool send_ts_api_header = false,
                              SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long post_response_verified(const std::string & url, const std::string & body, std::string & response,
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
                                    const std::unordered_map<std::string, std::string>& headers, long timeout_ms=4000,
                                    SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long post_response_sse(const std::string &url, const std::string &body,
                                const std::unordered_map<std::string, std::string>& headers, long timeout_ms=4000,
                                const std::shared_ptr<http_req> request = nullptr,
                                const std::shared_ptr<http_res> response = nullptr,
                                HttpServer* server = nullptr,
                                SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long post_response_sse_verified(const std::string &url, const std::string &body,
                                const std::unordered_map<std::string, std::string>& headers, long timeout_ms=4000,
                                const std::shared_ptr<http_req> request = nullptr,
                                const std::shared_ptr<http_res> response = nullptr,
                                HttpServer* server = nullptr);

    static long put_response(const std::string & url, const std::string & body, std::string & response,
                             std::map<std::string, std::string>& res_headers, long timeout_ms=4000,
                             bool send_ts_api_header = false,
                             SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long patch_response(const std::string & url, const std::string & body, std::string & response,
                               std::map<std::string, std::string>& res_headers, long timeout_ms=4000,
                               bool send_ts_api_header = false,
                               SSLVerifyMode ssl_verify_mode = SSLVerifyMode::NO_VERIFY);

    static long download_file_verified(const std::string& url, const std::string& file_path);

    static void extract_response_headers(CURL* curl, std::map<std::string, std::string> &res_headers);

    static inline std::string get_api_key() {
        return api_key;
    }
};
