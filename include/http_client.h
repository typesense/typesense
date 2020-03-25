#pragma once

#include <string>
#include <curl/curl.h>

/*
  NOTE: This is a really primitive blocking client meant only for specific Typesense use cases.
*/
class HttpClient {
private:
    static std::string api_key;
    static std::string ca_cert_path;

    HttpClient() = default;

    ~HttpClient() = default;

    static size_t curl_write (void *contents, size_t size, size_t nmemb, std::string *s);

    static CURL* init_curl(const std::string & url, std::string & buffer);

    static long perform_curl(CURL *curl);

public:
    static HttpClient & get_instance() {
        static HttpClient instance;
        return instance;
    }

    HttpClient(HttpClient const&) = delete;
    void operator=(HttpClient const&) = delete;

    void init(const std::string & api_key);

    static long get_response(const std::string & url, std::string & response, long timeout_ms=4000);

    static long delete_response(const std::string & url, std::string & response, long timeout_ms=120000);

    static long post_response(const std::string & url, const std::string & body, std::string & response,
                              long timeout_ms=4000);
};
