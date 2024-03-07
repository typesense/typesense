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
};
