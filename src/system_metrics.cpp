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

void SystemMetrics::get(const std::string &data_dir_path, nlohmann::json &result) {
    // DISK METRICS
    struct statvfs st{};
    statvfs(data_dir_path.c_str(), &st);
    uint64_t disk_total_bytes = st.f_blocks * st.f_frsize;
    uint64_t disk_used_bytes = (st.f_blocks - st.f_bavail) * st.f_frsize;
    result["disk_total_bytes"] = disk_total_bytes;
    result["disk_used_bytes"] = disk_used_bytes;

    // MEMORY METRICS

    rusage r_usage;
    getrusage(RUSAGE_SELF, &r_usage);
    result["memory_used_process_bytes"] = r_usage.ru_maxrss;

    uint64_t memory_free_bytes = 0;
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
        memory_free_bytes = (int64_t)(vm_stats.free_count) * (int64_t)mach_page_size;
    }

    uint64_t pages = sysconf(_SC_PHYS_PAGES);
    uint64_t page_size = sysconf(_SC_PAGE_SIZE);
    memory_total_bytes = (pages * page_size);
#elif __linux__
    struct sysinfo sys_info;
    sysinfo(&sys_info);
    memory_free_bytes = sys_info.freeram;
    memory_total_bytes = sys_info.totalram;
#endif

    result["memory_free_bytes"] = memory_free_bytes;
    result["memory_total_bytes"] = memory_total_bytes;

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
