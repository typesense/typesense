#pragma once

#include <unordered_map>
#include <deque>
#include "store.h"
#include "http_data.h"
#include "threadpool.h"
#include "http_server.h"

class BatchedIndexer {
private:
    struct req_res_t {
        std::string prev_req_body;  // used to handle partial JSON documents caused by chunking
        std::shared_ptr<http_req> req;
        std::shared_ptr<http_res> res;
        uint64_t batch_begin_ts;

        uint32_t num_chunks;
        uint32_t next_chunk_index;   // index where next read must begin
        bool is_complete;           //  whether the req has been written to store fully

        req_res_t(const std::string& prev_req_body,
                  const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res,
                  uint64_t batch_begin_ts, uint32_t num_chunks, uint32_t next_chunk_index, bool is_complete):
                prev_req_body(prev_req_body), req(req), res(res), batch_begin_ts(batch_begin_ts),
                num_chunks(num_chunks), next_chunk_index(next_chunk_index), is_complete(is_complete) {

        }

        req_res_t(): req(nullptr), res(nullptr), batch_begin_ts(0), num_chunks(0),
                     next_chunk_index(0), is_complete(false) {};
    };

    HttpServer* server;
    Store* store;

    const size_t num_threads;

    std::mutex* qmutuxes;
    std::vector<std::deque<uint64_t>> queues;

    /* Variables to be serialized on snapshot                  /
    --------------------------------------------------------- */

    std::mutex mutex;
    std::unordered_map<uint64_t, req_res_t> req_res_map;

    std::atomic<int64_t> queued_writes = 0;

    /* ------------------------------------------------------- */

    std::chrono::high_resolution_clock::time_point last_gc_run;

    std::atomic<bool> quit;
    std::shared_mutex pause_mutex;

    static const size_t GC_INTERVAL_SECONDS = 60;
    static const size_t GC_PRUNE_MAX_SECONDS = 3600;

    static std::string get_req_prefix_key(uint64_t req_id);

public:

    static const constexpr char* RAFT_REQ_LOG_PREFIX = "$RL_";

    BatchedIndexer(HttpServer* server, Store* store, size_t num_threads);

    ~BatchedIndexer();

    void enqueue(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);

    int64_t get_queued_writes();

    void run();

    void stop();

    // requires external synchronization!
    void serialize_state(nlohmann::json& state);

    void load_state(const nlohmann::json& state);

    std::string get_collection_name(const std::shared_ptr<http_req>& req);

    std::shared_mutex& get_pause_mutex();
};
