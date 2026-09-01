#include "http_client.h"
#include "file_utils.h"
#include "logger.h"
#include <vector>
#include <json.hpp>
#include "http_data.h"

std::string HttpClient::api_key = "";
std::string HttpClient::ca_cert_path = "";

struct client_state_t: public req_state_t {
    CURL* curl;

    client_state_t(CURL* curl): curl(curl) {

    }
};

namespace {
    long get_curl_failure_status_code(CURLcode res_code) {
        return res_code == CURLE_OPERATION_TIMEDOUT ? 408 : 500;
    }

    void log_curl_failure(CURL* curl, CURLcode res_code) {
        char* url = nullptr;
        char* method = nullptr;

        curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &url);
        curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_METHOD, &method);

        const char* effective_url = url == nullptr ? "" : url;
        const char* effective_method = method == nullptr ? "" : method;

        if(res_code == CURLE_OPERATION_TIMEDOUT) {
            double total_time = 0;
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total_time);
            LOG(ERROR) << "CURL timeout. Time taken: " << total_time << ", method: " << effective_method
                       << ", url: " << effective_url;
            return;
        }

        LOG(ERROR) << "CURL failed. Code: " << res_code << ", strerror: " << curl_easy_strerror(res_code)
                   << ", method: " << effective_method << ", url: " << effective_url;
    }

    std::string get_curl_failure_response_body(long status_code) {
        const std::string message = status_code == 408 ?
            "Remote server request timed out." :
            "Server error on remote server. Please try again later.";

        nlohmann::json res;
        res["message"] = message;
        res["error"]["message"] = message;
        return res.dump();
    }

    void fail_sse_response(deferred_req_res_t* req_res, long status_code, const std::string& response_body) {
        if(req_res == nullptr || req_res->req == nullptr || req_res->res == nullptr) {
            return;
        }

        std::string content_type = "application/json; charset=utf-8";
        if(req_res->req->async_res_set_headers_callback) {
            req_res->req->async_res_set_headers_callback(response_body, req_res->req, status_code, content_type);
        }

        if(req_res->req->async_res_done_callback) {
            req_res->req->async_res_done_callback(req_res->req, req_res->res);
            return;
        }

        req_res->res->status_code = status_code;
        req_res->res->content_type_header = content_type;
        req_res->res->body = response_body;
        req_res->res->final = true;

        if(req_res->server != nullptr) {
            req_res->res->wait();
            auto* async_req_res = new async_req_res_t(req_res->req, req_res->res, true);
            HttpServer::deliver_stream_response(async_req_res);
        }
    }
}

long HttpClient::post_response(const std::string &url, const std::string &body, std::string &response,
                               std::map<std::string, std::string>& res_headers,
                               const std::unordered_map<std::string, std::string>& headers, long timeout_ms,
                               bool send_ts_api_header, SSLVerifyMode ssl_verify_mode) {
    CURL *curl = init_curl(url, response, timeout_ms, ssl_verify_mode);
    if(curl == nullptr) {
        return 500;
    }

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());

    struct curl_slist *chunk = nullptr;
    for(const auto& header: headers) {
        std::string header_str = header.first + ": " + header.second;
        chunk = curl_slist_append(chunk, header_str.c_str());
    }

    return perform_curl(curl, res_headers, chunk, send_ts_api_header);
}

long HttpClient::post_response_verified(const std::string &url, const std::string &body, std::string &response,
                                        std::map<std::string, std::string>& res_headers,
                                        const std::unordered_map<std::string, std::string>& headers, long timeout_ms,
                                        bool send_ts_api_header) {
    return post_response(url, body, response, res_headers, headers, timeout_ms, send_ts_api_header,
                         SSLVerifyMode::VERIFY);
}

long HttpClient::post_response_stream(const std::string &url, const std::string &body, async_stream_response_t &response,
                                     std::map<std::string, std::string>& res_headers,
                                     const std::unordered_map<std::string, std::string>& headers, long timeout_ms,
                                     SSLVerifyMode ssl_verify_mode) {
    struct curl_slist* chunk = nullptr;

    CURL *curl = init_curl_stream(url, response, timeout_ms, ssl_verify_mode);
    if(curl == nullptr) {
        return 500;
    }

    for(const auto& header: headers) {
        std::string header_str = header.first + ": " + header.second;
        chunk = curl_slist_append(chunk, header_str.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    CURLcode res_code = curl_easy_perform(curl);

    long status_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);
    if(res_code != CURLE_OK || status_code == 0) {
        std::unique_lock<std::mutex> lock(response.mutex);
        response.ready = true;
        response.cv.notify_one();
        if(status_code == 0) {
            status_code = res_code == CURLE_OPERATION_TIMEDOUT ? 408 : 500;
        }
    }
    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);

    return status_code;
}

long HttpClient::post_response_sse(const std::string &url, const std::string &body,
                                const std::unordered_map<std::string, std::string>& headers, long timeout_ms,
                                const std::shared_ptr<http_req> request,
                                const std::shared_ptr<http_res> response,
                                HttpServer* server,
                                SSLVerifyMode ssl_verify_mode) {
    struct curl_slist* chunk = nullptr;
    deferred_req_res_t* req_res = new deferred_req_res_t(request, response, server, false);
    std::unique_ptr<deferred_req_res_t> req_res_guard(req_res);

    CURL *curl = init_curl_sse(url, timeout_ms, req_res, ssl_verify_mode);
    if(curl == nullptr) {
        return 500;
    }

    for(const auto& header: headers) {
        std::string header_str = header.first + ": " + header.second;
        chunk = curl_slist_append(chunk, header_str.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    CURLcode res_code = curl_easy_perform(curl);

    long status_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);
    if(res_code != CURLE_OK || status_code == 0) {
        if(res_code != CURLE_OK) {
            log_curl_failure(curl, res_code);
            status_code = get_curl_failure_status_code(res_code);
        } else {
            status_code = 500;
        }

        if(req_res->res == nullptr || req_res->res->status_code == 0) {
            fail_sse_response(req_res, status_code, get_curl_failure_response_body(status_code));
        }
    }
    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);

    return status_code;
}

long HttpClient::post_response_sse_verified(const std::string &url, const std::string &body,
                                const std::unordered_map<std::string, std::string>& headers, long timeout_ms,
                                const std::shared_ptr<http_req> request,
                                const std::shared_ptr<http_res> response,
                                HttpServer* server) {
    return post_response_sse(url, body, headers, timeout_ms, request, response, server, SSLVerifyMode::VERIFY);
}

long HttpClient::post_response_async(const std::string &url, const std::shared_ptr<http_req> request,
                                     const std::shared_ptr<http_res> response, HttpServer* server,
                                     bool send_ts_api_header) {
    deferred_req_res_t* req_res = new deferred_req_res_t(request, response, server, false);
    std::unique_ptr<deferred_req_res_t> req_res_guard(req_res);
    struct curl_slist* chunk = nullptr;

    CURL *curl = init_curl_async(url, req_res, chunk, send_ts_api_header);
    if(curl == nullptr) {
        return 500;
    }

    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    curl_slist_free_all(chunk);

    return 0;
}

long HttpClient::put_response(const std::string &url, const std::string &body, std::string &response,
                              std::map<std::string, std::string>& res_headers, long timeout_ms,
                              bool send_ts_api_header, SSLVerifyMode ssl_verify_mode) {
    CURL *curl = init_curl(url, response, timeout_ms, ssl_verify_mode);
    if(curl == nullptr) {
        return 500;
    }

    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    return perform_curl(curl, res_headers, nullptr, send_ts_api_header);
}

long HttpClient::patch_response(const std::string &url, const std::string &body, std::string &response,
                              std::map<std::string, std::string>& res_headers, long timeout_ms,
                              bool send_ts_api_header, SSLVerifyMode ssl_verify_mode) {
    CURL *curl = init_curl(url, response, timeout_ms, ssl_verify_mode);
    if(curl == nullptr) {
        return 500;
    }

    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    return perform_curl(curl, res_headers, nullptr, send_ts_api_header);
}

long HttpClient::delete_response(const std::string &url, std::string &response,
                                 std::map<std::string, std::string>& res_headers, long timeout_ms,
                                 bool send_ts_api_header, SSLVerifyMode ssl_verify_mode) {
    CURL *curl = init_curl(url, response, timeout_ms, ssl_verify_mode);
    if(curl == nullptr) {
        return 500;
    }

    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    return perform_curl(curl, res_headers, nullptr, send_ts_api_header);
}

long HttpClient::get_response(const std::string &url, std::string &response,
                              std::map<std::string, std::string>& res_headers,
                              const std::unordered_map<std::string, std::string>& headers,
                              long timeout_ms, bool send_ts_api_header, SSLVerifyMode ssl_verify_mode) {
    CURL *curl = init_curl(url, response, timeout_ms, ssl_verify_mode);
    if(curl == nullptr) {
        return 500;
    }
    struct curl_slist *chunk = nullptr;
    for(const auto& header: headers) {
        std::string header_str = header.first + ": " + header.second;
        chunk = curl_slist_append(chunk, header_str.c_str());
    }

    // follow redirects
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    return perform_curl(curl, res_headers, chunk, send_ts_api_header);
}

long HttpClient::get_response_verified(const std::string &url, std::string &response,
                                       std::map<std::string, std::string>& res_headers,
                                       const std::unordered_map<std::string, std::string>& headers,
                                       long timeout_ms, bool send_ts_api_header) {
    return get_response(url, response, res_headers, headers, timeout_ms, send_ts_api_header,
                        SSLVerifyMode::VERIFY);
}

void HttpClient::init(const std::string &api_key, const std::string& ca_cert_path) {
    HttpClient::api_key = api_key;
    if(!ca_cert_path.empty()) {
        HttpClient::ca_cert_path = ca_cert_path;
        return;
    }

    // try to locate ca cert file (from: https://serverfault.com/a/722646/117601)
    std::vector<std::string> locations = {
        "/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
        "/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
        "/etc/ssl/ca-bundle.pem",                            // OpenSUSE
        "/etc/pki/tls/cacert.pem",                           // OpenELEC
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
        "/usr/local/etc/openssl/cert.pem",                   // OSX
        "/usr/local/etc/openssl@1.1/cert.pem",               // OSX
    };

    HttpClient::ca_cert_path = "";

    for(const std::string & location: locations) {
        if(file_exists(location)) {
            HttpClient::ca_cert_path = location;
            break;
        }
    }
}

long HttpClient::perform_curl(CURL *curl, std::map<std::string, std::string>& res_headers, struct curl_slist *chunk,
                              bool send_ts_api_header) {

    if(send_ts_api_header) {
        std::string api_key_header = std::string("x-typesense-api-key: ") + HttpClient::api_key;
        chunk = curl_slist_append(chunk, api_key_header.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        char* url = nullptr;
        char *method = nullptr;

        curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &url);
        curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_METHOD, &method);

        long status_code = 0;

        if(res == CURLE_OPERATION_TIMEDOUT) {
            double total_time;
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total_time);
            LOG(ERROR) << "CURL timeout. Time taken: " << total_time << ", method: " << method << ", url: " << url;
            status_code = 408;
        } else {
            LOG(ERROR) << "CURL failed. Code: " << res << ", strerror: " << curl_easy_strerror(res)
                       << ", method: " << method << ", url: " << url;
            status_code = 500;
        }

        curl_easy_cleanup(curl);
        curl_slist_free_all(chunk);

        return status_code;
    }

    long http_code = 500;
    curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);

    extract_response_headers(curl, res_headers);

    curl_easy_cleanup(curl);
    curl_slist_free_all(chunk);

    return http_code == 0 ? 500 : http_code;
}

void HttpClient::extract_response_headers(CURL* curl, std::map<std::string, std::string> &res_headers) {
    char* content_type;
    CURLcode res = curl_easy_getinfo (curl, CURLINFO_CONTENT_TYPE, &content_type);
    if(res == CURLE_OK && content_type != nullptr) {
        res_headers.emplace("content-type", content_type);
    }
}

size_t HttpClient::curl_req_send_callback(char* buffer, size_t size, size_t nitems, void* userdata) {
    //LOG(INFO) << "curl_req_send_callback";
    // callback for request body to be sent to remote host
    deferred_req_res_t* req_res = static_cast<deferred_req_res_t *>(userdata);

    if(!req_res->res->is_alive) {
        // underlying client request is dead, don't proxy anymore data to upstream (leader)
        //LOG(INFO) << "req_res->req->req is: null";
        return 0;
    }

    size_t max_req_bytes = (size * nitems);

    const char* total_body_buf = req_res->req->body.c_str();
    size_t available_body_bytes = (req_res->req->body.size() - req_res->req->body_index);

    // copy data into `buffer` not exceeding max_req_bytes
    size_t bytes_to_read = std::min(max_req_bytes, available_body_bytes);

    memcpy(buffer, total_body_buf + req_res->req->body_index, bytes_to_read);

    req_res->req->body_index += bytes_to_read;

    /*LOG(INFO) << "Wrote " << bytes_to_read << " bytes to request body (max_buffer_bytes=" << max_req_bytes << ")";
    LOG(INFO) << "req_res->req->body_index: " << req_res->req->body_index
              << ", req_res->req->body.size(): " << req_res->req->body.size();*/

    if(req_res->req->body_index == req_res->req->body.size()) {
        //LOG(INFO) << "Current body buffer has been consumed fully.";

        req_res->req->body_index = 0;
        req_res->req->body = "";

        HttpServer::deliver_request_proceed(req_res);

        if(!req_res->req->last_chunk_aggregate) {
            //LOG(INFO) << "Waiting for request body to be ready";
            req_res->req->wait();
            //LOG(INFO) << "Request body is ready";
            //LOG(INFO) << "Buffer refilled, unpausing request forwarding, body_size=" << req_res->req->body.size();
        }
    }

    return bytes_to_read;
}

size_t HttpClient::curl_write_async(char *buffer, size_t size, size_t nmemb, void *context) {
    // callback for response body to be sent back to client
    //LOG(INFO) << "curl_write_async";
    deferred_req_res_t* req_res = static_cast<deferred_req_res_t *>(context);

    if(!req_res->res->is_alive) {
        // underlying client request is dead, don't try to send anymore data
        return 0;
    }

    size_t res_size = size * nmemb;
    std::string resp_str = std::string(buffer, res_size);

    // set headers if not already set
    if(req_res->res->status_code == 0) {
        client_state_t* client_state = dynamic_cast<client_state_t*>(req_res->req->data);
        CURL* curl = client_state->curl;
        long http_code = 500;
        CURLcode res = curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
        char* content_type;
        res = curl_easy_getinfo (curl, CURLINFO_CONTENT_TYPE, &content_type);
        std::string content_type_str = content_type ? content_type : "";

        if(req_res->req->async_res_set_headers_callback && res == CURLE_OK) {
            auto output = req_res->req->async_res_set_headers_callback(resp_str, req_res->req, http_code, content_type_str);
            if(!output) {
                // if the callback returns false, don't set headers, early return
                return res_size;
            }
        }

        if(res == CURLE_OK) {
            req_res->res->status_code = http_code;
        }

        if(res == CURLE_OK && content_type != nullptr) {
            req_res->res->content_type_header = content_type_str;
        }
    }

    // we've got response from remote host: write to client and ask for more request body


    req_res->res->final = false;

    if(req_res->req->async_res_write_callback) {
        // middleware callback to modify response body
        req_res->req->async_res_write_callback(resp_str, req_res->req, req_res->res);
    }

    req_res->res->body = resp_str;

    //LOG(INFO) << "curl_write_async response, res body size: " << req_res->res->body.size();

    // wait for previous chunk to finish (if any)
    //LOG(INFO) << "Waiting on req_res " << req_res->res;
    req_res->res->wait();

    async_req_res_t* async_req_res = new async_req_res_t(req_res->req, req_res->res, true);
    HttpServer::deliver_stream_response(async_req_res);

    // wait until response is sent
    //LOG(INFO) << "Response sent";

    return res_size;
}

size_t HttpClient::curl_write_stream(char *buffer, size_t size, size_t nmemb, void *context) {
    size_t res_size = size * nmemb;
    auto res = reinterpret_cast<async_stream_response_t*>(context);
    res->response_chunks.emplace_back(std::string(buffer, res_size));
    return res_size;
}

size_t HttpClient::curl_write_stream_done(void *context, curl_socket_t item) {
    auto res = reinterpret_cast<async_stream_response_t*>(context);

    std::unique_lock<std::mutex> lock(res->mutex);
    res->ready = true;
    res->cv.notify_one();

    close(item);
    return 0;
}

size_t HttpClient::curl_write_async_done(void *context, curl_socket_t item) {
    //LOG(INFO) << "curl_write_async_done";
    deferred_req_res_t* req_res = static_cast<deferred_req_res_t *>(context);
    if(req_res->req->is_write) {
       req_res->server->decr_pending_writes();
    }

    if(req_res->res->status_code == 0) {
        close(item);
        return 0;
    }

    if(req_res->req->async_res_done_callback) {
        bool output = req_res->req->async_res_done_callback(req_res->req, req_res->res);
        if(!output) {
            // if the callback returns false, don't send response, early return
            //LOG(INFO) << "async_res_done_callback returned false";
            return 0;
        }
    }

    if(!req_res->res->is_alive) {
        // underlying client request is dead, don't try to send anymore data
        // also, close the socket as we've overridden the close socket handler!
        close(item);
        return 0;
    }



    req_res->res->body = "";
    req_res->res->final = true;

    // wait until final response is flushed or response object will be destroyed by caller
    //LOG(INFO) << "Waiting on req_res " << req_res->res;
    req_res->res->wait();

    async_req_res_t* async_req_res = new async_req_res_t(req_res->req, req_res->res, true);
    HttpServer::deliver_stream_response(async_req_res);

    // Close the socket as we've overridden the close socket handler!
    close(item);

    return 0;
}

void HttpClient::configure_ssl(CURL* curl, const std::string& url, SSLVerifyMode ssl_verify_mode) {
    if(curl == nullptr || url.compare(0, 8, "https://") != 0) {
        return;
    }

    if(!ca_cert_path.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, ca_cert_path.c_str());
    } else {
        LOG(WARNING) << "Unable to locate system SSL certificates.";
    }

    if(ssl_verify_mode == SSLVerifyMode::VERIFY) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    } else {
        // Legacy internal cluster/self-signed paths still opt out until they are migrated.
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
}

CURL *HttpClient::init_curl_stream(const std::string& url, async_stream_response_t& res, long timeout_ms,
                                   SSLVerifyMode ssl_verify_mode) {
    CURL* curl = curl_easy_init();

    configure_ssl(curl, url, ssl_verify_mode);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 4000);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE);

    curl_easy_setopt(curl, CURLOPT_USERAGENT, "Typesense/1.0");

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpClient::curl_write_stream);  

    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res);
    
    curl_easy_setopt(curl, CURLOPT_CLOSESOCKETFUNCTION, HttpClient::curl_write_stream_done);

    curl_easy_setopt(curl, CURLOPT_CLOSESOCKETDATA, &res);

    return curl;
}


CURL *HttpClient::init_curl_sse(const std::string& url, long timeout_ms,
                                deferred_req_res_t* req_res,
                                SSLVerifyMode ssl_verify_mode) {
    CURL* curl = curl_easy_init();
    if(curl == nullptr) {
        return nullptr;
    }

    configure_ssl(curl, url, ssl_verify_mode);

    req_res->req->data = new client_state_t(curl);  // destruction of data is managed by req destructor


    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 4000);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE);

    curl_easy_setopt(curl, CURLOPT_USERAGENT, "Typesense/1.0");

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpClient::curl_write_async);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, req_res);

    curl_easy_setopt(curl, CURLOPT_CLOSESOCKETFUNCTION, HttpClient::curl_write_async_done);
    curl_easy_setopt(curl, CURLOPT_CLOSESOCKETDATA, req_res);
    return curl;
}

CURL *HttpClient::init_curl_async(const std::string& url, deferred_req_res_t* req_res, curl_slist*& chunk,
                                  bool send_ts_api_header, SSLVerifyMode ssl_verify_mode) {
    CURL *curl = curl_easy_init();

    if(curl == nullptr) {
        return nullptr;
    }

    req_res->req->data = new client_state_t(curl);  // destruction of data is managed by req destructor

    if(send_ts_api_header) {
        std::string api_key_header = std::string("x-typesense-api-key: ") + HttpClient::api_key;
        chunk = curl_slist_append(chunk, api_key_header.c_str());
    }

    // Use POSTFIELDSIZE_LARGE (curl_off_t) to handle bodies over 2 GB safely.
    // h2o uses SIZE_MAX as a sentinel for "no Content-Length header"; map that to -1
    // so libcurl falls back to chunked encoding when the size is genuinely unknown.
    curl_off_t post_size = (req_res->req->_req->content_length == SIZE_MAX)
        ? -1
        : static_cast<curl_off_t>(req_res->req->_req->content_length);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, post_size);

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

    // Enabling this causes issues in mixed mode: client using http/1 but follower -> leader using http/2
    //curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE);

    // callback called every time request body is needed
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, HttpClient::curl_req_send_callback);

    // context to callback
    curl_easy_setopt(curl, CURLOPT_READDATA, (void *)req_res);

    configure_ssl(curl, url, ssl_verify_mode);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 4000);

    curl_easy_setopt(curl, CURLOPT_USERAGENT, "Typesense/1.0");

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpClient::curl_write_async);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, req_res);

    curl_easy_setopt(curl, CURLOPT_CLOSESOCKETFUNCTION, HttpClient::curl_write_async_done);
    curl_easy_setopt(curl, CURLOPT_CLOSESOCKETDATA, req_res);

    return curl;
}

CURL *HttpClient::init_curl(const std::string& url, std::string& response, const size_t timeout_ms,
                            SSLVerifyMode ssl_verify_mode) {
    CURL *curl = curl_easy_init();

    if(curl == nullptr) {
        nlohmann::json res;
        res["message"] = "Failed to initialize HTTP client.";
        response = res.dump();
        return nullptr;
    }

    configure_ssl(curl, url, ssl_verify_mode);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 4000);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE);

    curl_easy_setopt(curl, CURLOPT_USERAGENT, "Typesense/1.0");

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpClient::curl_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

    return curl;
}

size_t HttpClient::curl_write(char *contents, size_t size, size_t nmemb, std::string *s) {
    s->append(contents, size*nmemb);
    return size*nmemb;
}

size_t HttpClient::curl_write_download(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t written = fwrite(ptr, size, nmemb, stream);
    return written;
}

long HttpClient::download_file(const std::string& url, const std::string& file_path,
                               SSLVerifyMode ssl_verify_mode) {
    CURL *curl = curl_easy_init();
    

    if(curl == nullptr) {
        return -1;
    }

    FILE *fp = fopen(file_path.c_str(), "wb");

    if(fp == nullptr) {
        LOG(ERROR) << "Unable to open file for writing: " << file_path;
        curl_easy_cleanup(curl);
        return -1;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 30000); //30s
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 30L);
    configure_ssl(curl, url, ssl_verify_mode);

    curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_download);
    // follow redirects
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode res_code = curl_easy_perform(curl);

    if(res_code != CURLE_OK) {
        LOG(ERROR) << "Unable to download file: " << url << " to " << file_path << " - " << curl_easy_strerror(res_code);
        curl_easy_cleanup(curl);
        fclose(fp);
        return -1;
    }
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_easy_cleanup(curl);
    fclose(fp);

    return http_code;
}

long HttpClient::download_file_verified(const std::string& url, const std::string& file_path) {
    return download_file(url, file_path, SSLVerifyMode::VERIFY);
}
