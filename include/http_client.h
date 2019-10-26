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
    std::string api_key;

    std::string ca_cert_path;

    inline bool file_exists (const std::string & name) {
        struct stat buffer;
        return (stat (name.c_str(), &buffer) == 0);
    }

public:
    HttpClient(std::string url, std::string api_key): url(url), api_key(api_key) {
        // try to locate ca cert file (from: https://serverfault.com/a/722646/117601)
        std::vector<std::string> locations = {
            "/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
            "/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
            "/etc/ssl/ca-bundle.pem",                            // OpenSUSE
            "/etc/pki/tls/cacert.pem",                           // OpenELEC
            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
            "/usr/local/etc/openssl/cert.pem",                   // OSX
        };

        ca_cert_path = "";

        for(const std::string & location: locations) {
            if(file_exists(location)) {
                ca_cert_path = location;
                break;
            }
        }
    }

    static size_t curl_write (void *contents, size_t size, size_t nmemb, std::string *s) {
        s->append((char*)contents, size*nmemb);
        return size*nmemb;
    }

    long get_reponse(std::string & response) {
        CURL *curl = curl_easy_init();

        if(!ca_cert_path.empty()) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_path.c_str());
        } else {
            LOG(ERR) << "Unable to locate system SSL certificates.";
            return 0;
        }

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);  // to allow self-signed certs
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpClient::curl_write);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);

        struct curl_slist *chunk = NULL;
        std::string api_key_header = std::string("x-typesense-api-key: ") + api_key;
        chunk = curl_slist_append(chunk, api_key_header.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

        curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_easy_cleanup(curl);
        response = buffer;
        return http_code;
    }
};