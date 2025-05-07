#pragma once

#include "core_api.h"
#include "store.h"
#include "http_data.h"
#include "http_server.h"
#include <chrono>

class AsyncDocRequestHandler {
private:
    int async_batch_interval = -1;
    std::vector<std::pair<std::shared_ptr<http_req>, std::string>> async_request_batch;
    std::chrono::steady_clock::time_point last_batch_flush_secs;
    Store* async_req_store; //to store failed single doc async request status

    AsyncDocRequestHandler() {}
    ~AsyncDocRequestHandler() {}

public:
    static AsyncDocRequestHandler& get_instance() {
        static AsyncDocRequestHandler instance;
        return instance;
    }

    static const constexpr char* ASYNC_DOC_REQ_PREFIX = "$ADQ_";

    AsyncDocRequestHandler(AsyncDocRequestHandler const&) = delete;
    void operator=(AsyncDocRequestHandler const&) = delete;

    void init(Store* async_store, int batch_interval);
    void check_handle_async_doc_request();
    void enqueue(const std::shared_ptr<http_req>& req, const std::string& req_hash);
    Option<std::string> get_req_status(const std::string& req);
    bool is_enabled();
};