#pragma once

#include <string>
#include <curl/curl.h>

/*
  NOTE: This is a really primitive blocking client meant only for specific Typesense use cases.
*/
class HttpClient {
private:
    std::string buffer;
    std::string url;
public:
    HttpClient(std::string url): url(url) {

    }

    static size_t curl_write (void *contents, size_t size, size_t nmemb, std::string *s) {
        s->append((char*)contents, size*nmemb);
        return size*nmemb;
    }

    long get_reponse(std::string & response) {
        CURL *curl = curl_easy_init();
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);  // to allow self-signed certs
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpClient::curl_write);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);

        struct curl_slist *chunk = NULL;
        chunk = curl_slist_append(chunk, "x-typesense-api-key: abcd");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

        curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_easy_cleanup(curl);
        response = buffer;
        return http_code;
    }
};