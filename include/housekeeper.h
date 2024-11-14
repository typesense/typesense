#pragma once
#include <mutex>
#include <atomic>
#include <condition_variable>
#include "http_data.h"

class HouseKeeper {
private:
    mutable std::mutex mutex;
    std::condition_variable cv;

    std::atomic<bool> quit = false;
    std::atomic<uint32_t> remove_expired_keys_interval_s = 3600;
    std::atomic<uint32_t> memory_req_min_age_s = 6;
    std::atomic<uint32_t> memory_usage_interval_s = 3;

    // used to track in-flight queries so they can be logged during a crash / rapid memory growth
    std::mutex ifq_mutex;

    struct req_metadata_t {
        std::shared_ptr<http_req> req;
        uint64_t active_memory = 0;
        bool already_logged = false;

        req_metadata_t(const std::shared_ptr<http_req>& req, uint64_t active_memory):
                req(req), active_memory(active_memory) {

        }
    };

    std::map<uint64_t, req_metadata_t> in_flight_queries;
    std::atomic<uint64_t> active_memory_used = 0;

    HouseKeeper() {}

    ~HouseKeeper() {}

public:

    static HouseKeeper &get_instance() {
        static HouseKeeper instance;
        return instance;
    }

    HouseKeeper(HouseKeeper const &) = delete;

    void operator=(HouseKeeper const &) = delete;

    void init();

    uint64_t get_active_memory_used();

    void add_req(const std::shared_ptr<http_req>& req);

    void remove_req(uint64_t req_id);

    std::string get_query_log(const std::shared_ptr<http_req>& req);

    void log_bad_queries();

    void log_running_queries();

    void run();

    void stop();
};
