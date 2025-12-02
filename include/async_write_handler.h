#pragma once

#include "core_api.h"
#include "store.h"
#include "http_data.h"
#include "http_server.h"
#include <chrono>
#include "sparsepp.h"

class AsyncWriteHandler {
    struct async_req_coll_action_t {
        std::string coll;
        std::string action;

        bool operator==(const async_req_coll_action_t& other) const {
            return (coll == other.coll) && (action == other.action);
        }

        // Hash function for the struct
        struct Hash {
            std::size_t operator()(const async_req_coll_action_t& value) const {
                return std::hash<std::string>{}(value.coll + value.action);
            }
        };
    };

    struct async_req_t {
        nlohmann::json req;
        std::string req_id;
    };

private:
    int async_batch_interval = -1;
    spp::sparse_hash_map<async_req_coll_action_t, std::vector<async_req_t>, async_req_coll_action_t::Hash> async_request_batch;
    std::chrono::steady_clock::time_point last_batch_flush_secs;
    std::chrono::steady_clock::time_point last_db_size_check_secs;
    Store* async_req_store; //to store failed single doc async request status
    unsigned int async_db_size;
    unsigned int async_db_size_check_interval;
    unsigned int async_req_count = 0;

    AsyncWriteHandler() {}
    ~AsyncWriteHandler() {}

public:
    static AsyncWriteHandler& get_instance() {
        static AsyncWriteHandler instance;
        return instance;
    }

    static const constexpr char* ASYNC_DOC_REQ_PREFIX = "$ADQ_";

    AsyncWriteHandler(AsyncWriteHandler const&) = delete;
    void operator=(AsyncWriteHandler const&) = delete;

    void init(Store* async_store, int batch_interval, uint32_t db_size = 100000, uint32_t db_interval = 3600);
    void process_async_writes();
    void check_and_truncate();
    nlohmann::json enqueue(const std::shared_ptr<http_req>& req, const std::string& reqid);
    Option<std::string> get_req_status(const std::string& req);
    void get_last_n_req_status(int n, nlohmann::json& res);
    bool is_enabled();
    uint32_t get_async_batch_size();
};