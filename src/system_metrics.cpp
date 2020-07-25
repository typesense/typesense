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

#if __APPLE__
#define impl_mallctl je_mallctl
#else
#define impl_mallctl mallctl
#endif

void SystemMetrics::get(const std::string &data_dir_path, nlohmann::json &result) {
    // DISK METRICS
    struct statvfs st{};
    statvfs(data_dir_path.c_str(), &st);
    uint64_t disk_total_bytes = st.f_blocks * st.f_frsize;
    uint64_t disk_used_bytes = (st.f_blocks - st.f_bavail) * st.f_frsize;
    uint64_t disk_available_bytes = disk_total_bytes - disk_used_bytes;
    result["disk_total_bytes"] = std::to_string(disk_total_bytes);
    result["disk_available_bytes"] = std::to_string(disk_available_bytes);

    // MEMORY METRICS

    size_t sz, active = 1, allocated = 1, resident, metadata, mapped, retained;
    sz = sizeof(size_t);
    uint64_t epoch = 1;

    // See: http://jemalloc.net/jemalloc.3.html#stats.active

    impl_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    impl_mallctl("epoch", &epoch, &sz, &epoch, sz);

    impl_mallctl("stats.active", &active, &sz, nullptr, 0);
    impl_mallctl("stats.allocated", &allocated, &sz, nullptr, 0);
    impl_mallctl("stats.resident", &resident, &sz, nullptr, 0);
    impl_mallctl("stats.metadata", &metadata, &sz, nullptr, 0);
    impl_mallctl("stats.mapped", &mapped, &sz, nullptr, 0);
    impl_mallctl("stats.retained", &retained, &sz, nullptr, 0);

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

    result["system_memory_available_bytes"] = std::to_string(memory_available_bytes);
    result["system_memory_total_bytes"] = std::to_string(memory_total_bytes);

    // CPU METRICS
#if __linux__
    const std::vector<cpu_stat_t>& cpu_stats = get_cpu_stats();

    for(size_t i = 0; i < cpu_stats.size(); i++) {
        std::string cpu_id = (i == 0) ? "" : std::to_string(i);
        result["cpu" + cpu_id + "_active_percentage"] = cpu_stats[i].active;
    }
#endif
}
