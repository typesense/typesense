#include "http_client.h"
#include "file_utils.h"
#include "logger.h"
#include <vector>

std::string HttpClient::api_key = "";
std::string HttpClient::ca_cert_path = "";

long HttpClient::post_response(const std::string &url, const std::string &body, std::string &response, long timeout_ms) {
    CURL *curl = init_curl(url, response);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    return perform_curl(curl);
}

long HttpClient::put_response(const std::string &url, const std::string &body, std::string &response, long timeout_ms) {
    CURL *curl = init_curl(url, response);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    return perform_curl(curl);
}

long HttpClient::delete_response(const std::string &url, std::string &response, long timeout_ms) {
    CURL *curl = init_curl(url, response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

    if(curl == nullptr) {
        return 0;
    }

    return perform_curl(curl);
}

long HttpClient::get_response(const std::string &url, std::string &response, long timeout_ms) {
    CURL *curl = init_curl(url, response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);

    if(curl == nullptr) {
        return 0;
    }

    return perform_curl(curl);
}

void HttpClient::init(const std::string &api_key) {
    HttpClient::api_key = api_key;

    // try to locate ca cert file (from: https://serverfault.com/a/722646/117601)
    std::vector<std::string> locations = {
            "/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
            "/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
            "/etc/ssl/ca-bundle.pem",                            // OpenSUSE
            "/etc/pki/tls/cacert.pem",                           // OpenELEC
            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
            "/usr/local/etc/openssl/cert.pem",                   // OSX
    };

    HttpClient::ca_cert_path = "";

    for(const std::string & location: locations) {
        if(file_exists(location)) {
            HttpClient::ca_cert_path = location;
            break;
        }
    }
}

long HttpClient::perform_curl(CURL *curl) {
    struct curl_slist *chunk = nullptr;
    std::string api_key_header = std::string("x-typesense-api-key: ") + HttpClient::api_key;
    chunk = curl_slist_append(chunk, api_key_header.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);
    return http_code;
}

CURL *HttpClient::init_curl(const std::string &url, std::string &buffer) {
    CURL *curl = curl_easy_init();

    if(!ca_cert_path.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_path.c_str());
    } else {
        LOG(ERROR) << "Unable to locate system SSL certificates.";
        return nullptr;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 300);

    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);  // to allow self-signed certs
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpClient::curl_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);

    return curl;
}

size_t HttpClient::curl_write(void *contents, size_t size, size_t nmemb, std::string *s) {
    s->append((char*)contents, size*nmemb);
    return size*nmemb;
}
