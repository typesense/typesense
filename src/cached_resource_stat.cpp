#include "cached_resource_stat.h"
#include <fstream>
#include <system_metrics.h>
#include "logger.h"

cached_resource_stat_t::resource_check_t
cached_resource_stat_t::has_enough_resources(const std::string& data_dir_path,
                                             const int disk_used_max_percentage,
                                             const int memory_used_max_percentage) {

    if(disk_used_max_percentage == 100 && memory_used_max_percentage == 100) {
        return cached_resource_stat_t::OK;
    }

    std::unique_lock lk(m);

    uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    if((now - last_checked_ts) < REFRESH_INTERVAL_SECS) {
        return resource_status;
    }

    resource_status = get_resource_status(data_dir_path, disk_used_max_percentage, memory_used_max_percentage);
    last_checked_ts = now;
    return resource_status;
}

cached_resource_stat_t::resource_check_t
cached_resource_stat_t::get_resource_status(const std::string& data_dir_path, const int disk_used_max_percentage,
                                            const int memory_used_max_percentage) {
    uint64_t disk_total_bytes = 0;
    uint64_t disk_used_bytes = 0;

    // get disk usage
    struct statvfs st{};
    statvfs(data_dir_path.c_str(), &st);
    disk_total_bytes = st.f_blocks * st.f_frsize;
    disk_used_bytes = (st.f_blocks - st.f_bavail) * st.f_frsize;

    double disk_used_percentage = (double(disk_used_bytes)/double(disk_total_bytes)) * 100;
    if(disk_used_percentage > disk_used_max_percentage) {
        LOG(INFO) << "disk_total_bytes: " << disk_total_bytes << ", disk_used_bytes: " << disk_used_bytes
                  << ", disk_used_percentage: " << disk_used_percentage;

        return cached_resource_stat_t::OUT_OF_DISK;
    }

    uint64_t memory_total_bytes = SystemMetrics::get_instance().get_memory_total_bytes();
    uint64_t memory_used_bytes = SystemMetrics::get_instance().get_memory_used_bytes();

    //LOG(INFO) << "memory_total_bytes: " << memory_total_bytes << ", memory_used_bytes: " << memory_used_bytes;

    if(memory_used_bytes == 0) {
        // if there is an error in fetching the stat, we will return `OK`
        return cached_resource_stat_t::OK;
    }

    if(memory_used_bytes >= memory_total_bytes) {
        return cached_resource_stat_t::OUT_OF_MEMORY;
    }

    // compare with 500M or `100 - memory_used_max_percentage` of total memory, whichever is lower
    uint64_t memory_free_min_bytes = std::min<uint64_t>(500ULL * 1024 * 1024,
                                                        ((100ULL - memory_used_max_percentage) * memory_total_bytes) / 100);
    uint64_t free_mem = (memory_total_bytes - memory_used_bytes);

    if(free_mem < memory_free_min_bytes) {
        auto memory_available_bytes = (memory_total_bytes - memory_used_bytes);
        LOG(INFO) << "memory_total: " << memory_total_bytes << ", memory_available: " << memory_available_bytes
                  << ", all_memory_used: " << memory_used_bytes << ", free_mem: " << free_mem
                  << ", memory_free_min: " << memory_free_min_bytes;
        return cached_resource_stat_t::OUT_OF_MEMORY;
    }

    return cached_resource_stat_t::OK;
}
