#pragma once

#include <unordered_map>
#include <deque>
#include "store.h"
#include "http_data.h"
#include "threadpool.h"
#include "http_server.h"

class BatchedIndexer {
private:
    static const constexpr char* RAFT_REQ_LOG_PREFIX = "$RL_";

    struct req_res_t {
        std::shared_ptr<http_req> req;
        std::shared_ptr<http_res> res;
        uint64_t batch_begin_ts;

        req_res_t(const std::shared_ptr<http_req>& req,
                  const std::shared_ptr<http_res>& res, uint64_t batch_begin_ts):
                  req(req), res(res), batch_begin_ts(batch_begin_ts) {

        }

        req_res_t() {

        }
    };

    HttpServer* server;
    Store* store;

    ThreadPool* thread_pool;
    const size_t num_threads;

    std::vector<std::deque<uint64_t>> queues;
    std::mutex* qmutuxes;

    std::mutex mutex;
    std::unordered_map<uint64_t, uint32_t> request_to_chunk;
    std::unordered_map<uint64_t, req_res_t> req_res_map;

    std::chrono::high_resolution_clock::time_point last_gc_run;

    std::atomic<bool> quit;

    static const size_t GC_INTERVAL_SECONDS = 60;
    static const size_t GC_PRUNE_MAX_SECONDS = 3600;

    static std::string get_req_prefix_key(uint64_t req_id);

    std::atomic<int64_t> queued_writes = 0;

public:

    BatchedIndexer(HttpServer* server, Store* store, size_t num_threads);

    ~BatchedIndexer();

    void enqueue(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);

    int64_t get_queued_writes();

    void run();

    void stop();
};