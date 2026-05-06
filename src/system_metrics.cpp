#include "system_metrics.h"

#include <sys/resource.h>
#include <sys/statvfs.h>
#if __linux__
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <unistd.h>
#elif __APPLE__
#include <unistd.h>
#include <mach/vm_statistics.h>
#include <mach/mach_types.h>
#include <mach/mach_init.h>
#include <mach/mach_host.h>
#endif

#include "string_utils.h"

#ifndef ASAN_BUILD
#include "jemalloc.h"
#if __APPLE__
#define impl_mallctl je_mallctl
#else
#define impl_mallctl mallctl
#endif
#endif

void SystemMetrics::get(const std::string &data_dir_path, nlohmann::json &result) {
    // DISK METRICS
    struct statvfs st{};
    statvfs(data_dir_path.c_str(), &st);
    uint64_t disk_total_bytes = st.f_blocks * st.f_frsize;
    uint64_t disk_used_bytes = (st.f_blocks - st.f_bavail) * st.f_frsize;
    result["system_disk_total_bytes"] = std::to_string(disk_total_bytes);
    result["system_disk_used_bytes"] = std::to_string(disk_used_bytes);

    // MEMORY METRICS

    size_t sz, active = 1, allocated = 1, resident, metadata, mapped, retained;
    sz = sizeof(size_t);
    uint64_t epoch = 1;

#ifndef ASAN_BUILD
    // See: http://jemalloc.net/jemalloc.3.html#stats.active

    impl_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    impl_mallctl("epoch", &epoch, &sz, &epoch, sz);

    impl_mallctl("stats.active", &active, &sz, nullptr, 0);
    impl_mallctl("stats.allocated", &allocated, &sz, nullptr, 0);
    impl_mallctl("stats.resident", &resident, &sz, nullptr, 0);
    impl_mallctl("stats.metadata", &metadata, &sz, nullptr, 0);
    impl_mallctl("stats.mapped", &mapped, &sz, nullptr, 0);
    impl_mallctl("stats.retained", &retained, &sz, nullptr, 0);
#endif

    result["typesense_memory_active_bytes"] = std::to_string(active);
    result["typesense_memory_allocated_bytes"] = std::to_string(allocated);
    result["typesense_memory_resident_bytes"] = std::to_string(active);
    result["typesense_memory_metadata_bytes"] = std::to_string(metadata);
    result["typesense_memory_mapped_bytes"] = std::to_string(mapped);
    result["typesense_memory_retained_bytes"] = std::to_string(retained);

    // Fragmentation ratio is calculated very similar to how Redis does it:
    // https://github.com/redis/redis/blob/d6180c8c8674ffdae3d6efa5f946d85fe9163464/src/defrag.c#L900
    std::string frag_ratio = format_dp(1.0f - ((float)allocated / active));
    result["typesense_memory_fragmentation_ratio"] = frag_ratio;

    result["system_memory_total_bytes"] = std::to_string(get_memory_total_bytes());
    result["system_memory_used_bytes"] = std::to_string(get_memory_used_bytes());

#ifdef __linux__
    struct sysinfo sys_info;
    sysinfo(&sys_info);
    auto swap_used_bytes = sys_info.totalswap - sys_info.freeswap;
    result["system_memory_total_swap_bytes"] = std::to_string(sys_info.totalswap);
    result["system_memory_used_swap_bytes"] = std::to_string(swap_used_bytes);
#endif
    // CPU and Network metrics
#if __linux__
    const std::vector<cpu_stat_t>& cpu_stats = get_cpu_stats();

    for(size_t i = 0; i < cpu_stats.size(); i++) {
        std::string cpu_id = (i == 0) ? "" : std::to_string(i);
        result["system_cpu" + cpu_id + "_active_percentage"] = cpu_stats[i].active;
    }

    uint64_t received_bytes, sent_bytes;
    linux_get_network_data("/proc/net/dev", received_bytes, sent_bytes);
    result["system_network_received_bytes"] = std::to_string(received_bytes);
    result["system_network_sent_bytes"] = std::to_string(sent_bytes);
#endif
}

bool SystemMetrics::get_cgroup_memory(uint64_t& limit_bytes, uint64_t& usage_bytes,
                                      const std::string& proc_self_cgroup,
                                      const std::string& cgroup_root) {
#ifdef __linux__
    std::ifstream cg_file(proc_self_cgroup);
    if (!cg_file.is_open()) {
        return false;
    }

    std::string v2_rel_path, v1_rel_path;
    std::string line;
    while (std::getline(cg_file, line)) {
        if (line.rfind("0::", 0) == 0) {
            // cgroups v2 unified hierarchy: "0::<path>"
            v2_rel_path = line.substr(3);
        } else {
            // cgroups v1: "<id>:<controllers>:<path>"
            auto first = line.find(':');
            if (first != std::string::npos) {
                auto second = line.find(':', first + 1);
                if (second != std::string::npos) {
                    std::string controllers = line.substr(first + 1, second - first - 1);
                    if (controllers.find("memory") != std::string::npos) {
                        v1_rel_path = line.substr(second + 1);
                    }
                }
            }
        }
    }

    // cgroups v2: memory.max + memory.current
    if (!v2_rel_path.empty()) {
        std::string base = cgroup_root + v2_rel_path;
        std::ifstream max_file(base + "/memory.max");
        if (max_file.is_open()) {
            std::string val;
            if ((max_file >> val) && val != "max") {
                std::ifstream curr_file(base + "/memory.current");
                if (curr_file.is_open()) {
                    uint64_t current = 0;
                    if (curr_file >> current) {
                        limit_bytes = std::stoull(val);
                        usage_bytes = current;
                        return true;
                    }
                }
            }
        }
    }

    // cgroups v1: memory.limit_in_bytes + memory.usage_in_bytes
    // The unlimited sentinel is 0x7FFFFFFFFFFFF000 (~9.2EB); treat anything above 1EB as unlimited.
    if (!v1_rel_path.empty()) {
        std::string base = cgroup_root + "/memory" + v1_rel_path;
        std::ifstream lf(base + "/memory.limit_in_bytes");
        if (lf.is_open()) {
            uint64_t limit = 0;
            if ((lf >> limit) && limit < (1ULL << 60)) {
                std::ifstream uf(base + "/memory.usage_in_bytes");
                if (uf.is_open()) {
                    uint64_t usage = 0;
                    if (uf >> usage) {
                        limit_bytes = limit;
                        usage_bytes = usage;
                        return true;
                    }
                }
            }
        }
    }
#endif
    return false;
}

uint64_t SystemMetrics::get_memory_total_bytes() {
    uint64_t memory_total_bytes = 0;

#ifdef __APPLE__
    uint64_t pages = sysconf(_SC_PHYS_PAGES);
    uint64_t page_size = sysconf(_SC_PAGE_SIZE);
    memory_total_bytes = (pages * page_size);
#elif __linux__
    uint64_t cgroup_limit = 0, cgroup_usage = 0;
    if (SystemMetrics::get_cgroup_memory(cgroup_limit, cgroup_usage)) {
        return cgroup_limit;
    }
    struct sysinfo sys_info;
    sysinfo(&sys_info);
    memory_total_bytes = sys_info.totalram;
#endif

    return memory_total_bytes;
}

uint64_t SystemMetrics::get_proc_memory_active_bytes() {
    auto stats = get_cached_mallctl_stats();
    return stats.memory_active_bytes;
}

uint64_t SystemMetrics::get_memory_used_bytes() {
    uint64_t memory_used_bytes = 0;

#ifdef __APPLE__
    vm_size_t mach_page_size;
    mach_port_t mach_port;
    mach_msg_type_number_t count;
    vm_statistics64_data_t vm_stats;
    mach_port = mach_host_self();
    count = sizeof(vm_stats) / sizeof(natural_t);
    if (KERN_SUCCESS == host_page_size(mach_port, &mach_page_size) &&
        KERN_SUCCESS == host_statistics64(mach_port, HOST_VM_INFO,
                                          (host_info64_t)&vm_stats, &count)) {
        memory_used_bytes = ((int64_t)(vm_stats.active_count + vm_stats.wire_count) * (int64_t)mach_page_size);
    }
#elif __linux__
    // Prefer cgroup-scoped usage so the memory guard works correctly in containers.
    // Without this, sysinfo()/proc/meminfo report node-level memory and the guard
    // never fires because the node always has plenty of free RAM even when the
    // container itself is near its limit.
    uint64_t cgroup_limit = 0, cgroup_usage = 0;
    if (SystemMetrics::get_cgroup_memory(cgroup_limit, cgroup_usage)) {
        return cgroup_usage;
    }

    uint64_t memory_total_bytes = 0;
    uint64_t memory_available_bytes = 0;
    uint64_t swap_total_bytes = 0;
    uint64_t swap_free_bytes = 0;

    SystemMetrics::get_instance().get_proc_meminfo(memory_total_bytes, memory_available_bytes, swap_total_bytes, swap_free_bytes);

    memory_used_bytes = (memory_total_bytes - memory_available_bytes) + (swap_total_bytes - swap_free_bytes);

#endif

    return memory_used_bytes;
}

mallctl_stats_t SystemMetrics::get_cached_mallctl_stats() {
    std::unique_lock lock(mutex);

    uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    uint64_t seconds_since_last = (now - mallctl_stats_last_access);

    if(seconds_since_last > MALLCTL_STATS_UPDATE_INTERVAL_SECONDS) {
        size_t sz = sizeof(size_t);
        uint64_t epoch = 1;

#ifndef ASAN_BUILD
        impl_mallctl("epoch", &epoch, &sz, &epoch, sz);
        impl_mallctl("stats.mapped", &mallctl_stats.memory_mapped_bytes, &sz, nullptr, 0);
        impl_mallctl("stats.retained", &mallctl_stats.memory_retained_bytes, &sz, nullptr, 0);
        impl_mallctl("stats.active", &mallctl_stats.memory_active_bytes, &sz, nullptr, 0);
        impl_mallctl("stats.metadata", &mallctl_stats.memory_metadata_bytes, &sz, nullptr, 0);
#endif
    }

    mallctl_stats_last_access = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    return mallctl_stats;
}

void SystemMetrics::linux_get_network_data(const std::string & stat_path,
                                           uint64_t &received_bytes, uint64_t &sent_bytes) {
    //std::ifstream stat_file("/proc/net/dev");
    std::ifstream stat_file(stat_path);
    std::string line;

    // TODO: this probably needs to be handled better!
    const std::string STR_ENS5("ens5");
    const std::string STR_ETH0("eth0");

    /*
        Inter-|   Receive                                                |  Transmit
        face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
        ens5: 324278716  897631    0    0    0     0          0         0 93933882  575535    0    0    0     0       0          0
    */

    received_bytes = 0;
    sent_bytes = 0;

    while (std::getline(stat_file, line)) {
        StringUtils::trim(line);
        if (line.rfind(STR_ENS5, 0) == 0 || line.rfind(STR_ETH0, 0) == 0) {
            std::istringstream ss(line);
            std::string throwaway;

            // read interface label
            ss >> throwaway;

            uint64_t stat_value;

            // read stats
            for (int i = 0; i < NUM_NETWORK_STATS; i++) {
                ss >> stat_value;

                if(i == 0) {
                    received_bytes = stat_value;
                }

                if(i == 8) {
                    sent_bytes = stat_value;
                }
            }

            break;
        }
    }
}

void SystemMetrics::get_proc_meminfo(uint64_t& memory_total_bytes, uint64_t& memory_available_bytes,
                                     uint64_t& swap_total_bytes,
                                     uint64_t& swap_free_bytes) {
    std::string token;
    std::ifstream file("/proc/meminfo");

    while(file >> token) {
        if(token == "MemTotal:") {
            uint64_t value_kb;
            if(file >> value_kb) {
                memory_total_bytes = value_kb * 1024;
            }
        }

        else if(token == "MemAvailable:") {
            uint64_t value_kb;
            if(file >> value_kb) {
                memory_available_bytes = value_kb * 1024;
            }
        }

        else if(token == "SwapTotal:") {
            uint64_t value_kb;
            if(file >> value_kb) {
                swap_total_bytes = value_kb * 1024;
            }
        }

        else if(token == "SwapFree:") {
            uint64_t value_kb;
            if(file >> value_kb) {
                swap_free_bytes = value_kb * 1024;
            }

            // since "SwapFree" appears last in the file
            break;
        }
    }
}

uint64_t SystemMetrics::get_cached_jemalloc_unused_memory() {
    // Unused Memory = (stats.mapped + stats.retained) - (stats.active + stats.metadata)
#ifndef ASAN_BUILD
    const auto stats = get_cached_mallctl_stats();
    return (stats.memory_mapped_bytes + stats.memory_retained_bytes) -
           (stats.memory_active_bytes + stats.memory_metadata_bytes);
#else
    return 1;
#endif
}
