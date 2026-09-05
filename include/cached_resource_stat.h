#pragma once
#include <cstdint>
#include <atomic>
#include <mutex>
#include <chrono>
#include <string>
#include <sys/statvfs.h>

class cached_resource_stat_t {
public:
    enum resource_check_t {
        OK,
        OUT_OF_DISK,
        OUT_OF_MEMORY
    };

private:
    const static size_t REFRESH_INTERVAL_SECS = 5;
    std::atomic<uint64_t> last_checked_ts = 0;
    std::mutex m;

    resource_check_t resource_status = OK;

    cached_resource_stat_t() = default;

    ~cached_resource_stat_t() = default;

    static cached_resource_stat_t::resource_check_t get_resource_status(const std::string& data_dir_path,
                                                                 const int disk_used_max_percentage,
                                                                 const int memory_used_max_percentage);

public:
    static cached_resource_stat_t& get_instance() {
        static cached_resource_stat_t instance;
        return instance;
    }

    // On Mac, we will only check for disk usage
    resource_check_t has_enough_resources(const std::string& data_dir_path,
                                          const int disk_used_max_percentage,
                                          const int memory_used_max_percentage);

    // Invalidates the cached result so the next call to `has_enough_resources` does a fresh
    // check. Intended for tests that need deterministic behaviour across calls within the
    // 5-second TTL.
    void invalidate_cache() {
        std::unique_lock lk(m);
        last_checked_ts = 0;
    }

    // Forces the cached resource status to a specific value and primes the TTL so that the
    // override survives subsequent calls within the cache window. Intended for tests that
    // need to simulate OUT_OF_MEMORY or OUT_OF_DISK on a host that is otherwise healthy.
    // Call `invalidate_cache()` afterwards to restore real-environment behaviour.
    void set_resource_status_for_testing(resource_check_t status) {
        std::unique_lock lk(m);
        resource_status = status;
        last_checked_ts = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
    }
};
