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

#include "jemalloc.h"

void SystemMetrics::get(const std::string &data_dir_path, nlohmann::json &result) {
    // DISK METRICS
    struct statvfs st{};
    statvfs(data_dir_path.c_str(), &st);
    uint64_t disk_total_bytes = st.f_blocks * st.f_frsize;
    uint64_t disk_used_bytes = (st.f_blocks - st.f_bavail) * st.f_frsize;
    result["disk_total_bytes"] = std::to_string(disk_total_bytes);
    result["disk_used_bytes"] = std::to_string(disk_used_bytes);

    // MEMORY METRICS

    size_t sz, active, allocated;

    /*
        stats.active:
        Returns the total number of bytes in active pages allocated by the application.

        stats.allocated
        Returns the total number of bytes allocated by the application. Will be larger than active.

        active/allocated == fragmentation ratio.
    */

#ifdef __APPLE__
    je_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    je_mallctl("stats.active", &active, &sz, nullptr, 0);
    je_mallctl("stats.allocated", &allocated, &sz, nullptr, 0);
#elif __linux__
    mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    mallctl("stats.active", &active, &sz, nullptr, 0);
    mallctl("stats.allocated", &allocated, &sz, nullptr, 0);
#else
    active = allocated = 0;
#endif

    if(active != 0 && allocated != 0) {
        // bytes allocated by allocator (jemalloc)
        result["typesense_memory_used_bytes"] = std::to_string(allocated);
        result["typesense_memory_active_bytes"] = std::to_string(active);

        // Defragmentation ratio is calculated very similar to Redis:
        // https://github.com/redis/redis/blob/d6180c8c8674ffdae3d6efa5f946d85fe9163464/src/defrag.c#L900
        std::string frag_percent = format_dp(((float)active / allocated)*100 - 100);
        result["fragmentation_percent"] = frag_percent;
    }

    rusage r_usage;
    getrusage(RUSAGE_SELF, &r_usage);

    // Bytes allocated by OS (resident set size) and is the number reported by tools such as htop.
    // `ru_maxrss` is in bytes on OSX but in kilobytes on Linux
#ifdef __APPLE__
    result["typesense_memory_used_rss_bytes"] = std::to_string(r_usage.ru_maxrss);
#elif __linux__
    result["typesense_memory_used_rss_bytes"] = std::to_string(r_usage.ru_maxrss * 1000);
#endif

    uint64_t memory_available_bytes = 0;
    uint64_t memory_total_bytes = 0;

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
        memory_available_bytes = (int64_t)(vm_stats.free_count) * (int64_t)mach_page_size;
    }

    uint64_t pages = sysconf(_SC_PHYS_PAGES);
    uint64_t page_size = sysconf(_SC_PAGE_SIZE);
    memory_total_bytes = (pages * page_size);
#elif __linux__
    struct sysinfo sys_info;
    sysinfo(&sys_info);
    memory_available_bytes = linux_get_mem_available_bytes();
    memory_total_bytes = sys_info.totalram;
#endif

    result["memory_available_bytes"] = std::to_string(memory_available_bytes);
    result["memory_total_bytes"] = std::to_string(memory_total_bytes);

    // CPU METRICS
#if __linux__
    const std::vector<cpu_stat_t>& cpu_stats = get_cpu_stats();

    for(size_t i = 0; i < cpu_stats.size(); i++) {
        std::string cpu_label = std::to_string(i+1);
        result["cpu" + cpu_label + "_active_percentage"] = cpu_stats[i].active;
        result["cpu" + cpu_label + "_idle_percentage"] = cpu_stats[i].idle;
    }
#endif
}
