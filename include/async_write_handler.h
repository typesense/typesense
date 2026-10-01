#pragma once

#include "core_api.h"
#include "store.h"
#include "http_data.h"
#include "http_server.h"
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include "sparsepp.h"

class AsyncWriteHandler {
    struct async_req_coll_action_t {
        std::string coll;
        std::string action;
        int dirty_values;
        size_t remote_embedding_timeout_ms;
        size_t remote_embedding_num_tries;

        bool operator==(const async_req_coll_action_t& other) const {
            return coll == other.coll && action == other.action && dirty_values == other.dirty_values &&
                   remote_embedding_timeout_ms == other.remote_embedding_timeout_ms &&
                   remote_embedding_num_tries == other.remote_embedding_num_tries;
        }

        struct Hash {
            std::size_t operator()(const async_req_coll_action_t& value) const {
                size_t seed = std::hash<std::string>{}(value.coll);
                seed ^= std::hash<std::string>{}(value.action) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                seed ^= std::hash<int>{}(value.dirty_values) + 0x9e3779b9 +
                        (seed << 6) + (seed >> 2);
                seed ^= std::hash<size_t>{}(value.remote_embedding_timeout_ms) + 0x9e3779b9 +
                        (seed << 6) + (seed >> 2);
                seed ^= std::hash<size_t>{}(value.remote_embedding_num_tries) + 0x9e3779b9 +
                        (seed << 6) + (seed >> 2);
                return seed;
            }
        };
    };

    struct async_req_t {
        std::string body;
        std::string req_id;
    };

    using async_request_batch_t =
        spp::sparse_hash_map<async_req_coll_action_t, std::vector<async_req_t>, async_req_coll_action_t::Hash>;

private:
    int async_batch_interval = -1;
    async_request_batch_t async_request_batch;
    std::optional<std::chrono::steady_clock::time_point> batch_started_at;
    std::chrono::steady_clock::time_point last_db_size_check_secs;
    Store* async_req_store; // Stores pending requests and failed request status.
    unsigned int async_db_size;
    unsigned int async_db_size_check_interval;
    unsigned int async_req_count = 0;
    mutable std::mutex mutex;

    static std::string get_pending_key(const std::string& req_id);
    static std::string get_failure_key(const std::string& req_id);
    static nlohmann::json serialize_pending_request(const async_req_coll_action_t& group,
                                                    const async_req_t& request);
    static Option<std::pair<async_req_coll_action_t, async_req_t>> deserialize_pending_request(
        const std::string& req_id, const std::string& value);
    void load_pending_requests();
    void load_failure_count();
    bool finalize_requests(const std::vector<async_req_t>& requests,
                           const std::map<size_t, std::string>& failures);
    void requeue_requests(const async_req_coll_action_t& group, std::vector<async_req_t>&& requests);

    AsyncWriteHandler() {}
    ~AsyncWriteHandler() {}

public:
    static AsyncWriteHandler& get_instance() {
        static AsyncWriteHandler instance;
        return instance;
    }

    static const constexpr char* ASYNC_DOC_REQ_PREFIX = "$ADQ_";
    static const constexpr char* ASYNC_DOC_PENDING_PREFIX = "$ADP_";

    AsyncWriteHandler(AsyncWriteHandler const&) = delete;
    void operator=(AsyncWriteHandler const&) = delete;

    void init(Store* async_store, int batch_interval, uint32_t db_size = 100000, uint32_t db_interval = 3600);
    void process_async_writes(bool force = false);
    void check_and_truncate();
    Option<nlohmann::json> enqueue(const std::shared_ptr<http_req>& req, const std::string& reqid);
    Option<std::string> get_req_status(const std::string& req);
    void get_last_n_req_status(int n, nlohmann::json& res);
    bool is_enabled();
    uint32_t get_async_batch_size();
};
