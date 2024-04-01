#pragma once

#include <unordered_map>
#include <deque>
#include "store.h"
#include "http_data.h"
#include "threadpool.h"
#include "http_server.h"
#include "tsconfig.h"

class BatchedIndexer {
private:
    struct req_res_t {
        uint64_t start_ts;
        std::string prev_req_body;  // used to handle partial JSON documents caused by chunking
        std::shared_ptr<http_req> req;
        std::shared_ptr<http_res> res;
        uint64_t last_updated;

        uint32_t num_chunks;
        uint32_t next_chunk_index;   // index where next read must begin
        bool is_complete;           //  whether the req has been written to store fully

        req_res_t(uint64_t start_ts, const std::string& prev_req_body,
                  const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res,
                  uint64_t last_updated, uint32_t num_chunks, uint32_t next_chunk_index, bool is_complete):
                start_ts(start_ts), prev_req_body(prev_req_body), req(req), res(res), last_updated(last_updated),
                num_chunks(num_chunks), next_chunk_index(next_chunk_index), is_complete(is_complete) {

        }

        req_res_t(): req(nullptr), res(nullptr), last_updated(0), num_chunks(0),
                     next_chunk_index(0), is_complete(false) {};
    };

    struct await_t {
        std::mutex mcv;
        std::condition_variable cv;
    };

    struct refq_entry {
        uint64_t queue_id;
        uint64_t start_ts;

        refq_entry(uint64_t qid, uint64_t sts): queue_id(qid), start_ts(sts) {

        }
    };

    HttpServer* server;
    Store* store;
    Store* meta_store;

    const size_t num_threads;

    await_t* qmutuxes;
    std::vector<std::deque<uint64_t>> queues;

    std::unordered_map<std::string, std::unordered_set<std::string>> coll_to_references;
    await_t refq_wait;
    std::list<refq_entry> reference_q;

    /* Variables to be serialized on snapshot                  /
    --------------------------------------------------------- */

    std::mutex mutex;
    std::map<uint64_t, req_res_t> req_res_map;

    std::atomic<int64_t> queued_writes = 0;

    /* ------------------------------------------------------- */

    std::chrono::high_resolution_clock::time_point last_gc_run;

    std::atomic<bool> quit;
    std::shared_mutex pause_mutex;

    // Used to skip over a bad raft log entry which previously triggered a crash
    const static int64_t UNSET_SKIP_INDEX = -9999;
    std::atomic<int64_t> skip_index = UNSET_SKIP_INDEX;
    rocksdb::Iterator* skip_index_iter = nullptr;
    static constexpr const char* SKIP_INDICES_PREFIX = "$XP";

    std::string skip_index_upper_bound_key = std::string(SKIP_INDICES_PREFIX) + "`";  // cannot inline this
    rocksdb::Slice* skip_index_iter_upper_bound = nullptr;

    // When set, all writes (both live and log serialized) are skipped with 422 response
    const std::atomic<bool>& skip_writes;

    const Config& config;

    static const size_t GC_INTERVAL_SECONDS = 60;
    static const size_t GC_PRUNE_MAX_SECONDS = 3600;

    static std::string get_req_prefix_key(uint64_t req_id);

    static std::string get_req_suffix_key(uint64_t req_id);

public:

    static const constexpr char* RAFT_REQ_LOG_PREFIX = "$RL_";

    BatchedIndexer(HttpServer* server, Store* store, Store* meta_store, size_t num_threads,
                   const Config& config, const std::atomic<bool>& skip_writes);

    ~BatchedIndexer();

    void enqueue(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);

    int64_t get_queued_writes();

    void run();

    void stop();

    void populate_skip_index();

    void persist_applying_index();

    void clear_skip_indices();

    // requires external synchronization!
    void serialize_state(nlohmann::json& state);

    void load_state(const nlohmann::json& state);

    std::string get_collection_name(const std::shared_ptr<http_req>& req);

    std::shared_mutex& get_pause_mutex();
};
