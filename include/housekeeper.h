#pragma once
#include <mutex>
#include <atomic>
#include <condition_variable>

class HouseKeeper {
private:
    mutable std::mutex mutex;
    std::condition_variable cv;

    std::atomic<bool> quit = false;
    std::atomic<uint32_t> hnsw_repair_interval_s = 1800;
    std::atomic<uint32_t> remove_expired_keys_interval_s = 3600;

    HouseKeeper() {}

    ~HouseKeeper() {}


public:

    static HouseKeeper &get_instance() {
        static HouseKeeper instance;
        return instance;
    }

    HouseKeeper(HouseKeeper const &) = delete;

    void operator=(HouseKeeper const &) = delete;

    void init(uint32_t interval_seconds);

    void run();

    void stop();
};
