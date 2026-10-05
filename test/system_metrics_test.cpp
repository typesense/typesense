#include <gtest/gtest.h>
#include "system_metrics.h"

#ifndef ASAN_BUILD
TEST(SystemMetricsTest, ResidentMemoryIncludesActivePagesAndAllocatorOverhead) {
    nlohmann::json metrics;
    SystemMetrics::get_instance().get("/tmp", metrics);
    const auto active = std::stoull(metrics["typesense_memory_active_bytes"].get<std::string>());
    const auto resident = std::stoull(metrics["typesense_memory_resident_bytes"].get<std::string>());
    EXPECT_GT(active, 0);
    EXPECT_GT(resident, active);
    EXPECT_EQ("0", metrics["typesense_jemalloc_stats_read_errors"]);
    EXPECT_GT(std::stoull(metrics["typesense_jemalloc_page_size"].get<std::string>()), 0);
    EXPECT_TRUE(metrics.contains("typesense_jemalloc_arena_0_dirty_decay_ms"));
    EXPECT_TRUE(metrics.contains("typesense_jemalloc_arena_0_muzzy_decay_ms"));
    EXPECT_TRUE(metrics.contains("typesense_jemalloc_dirty_pages"));
    EXPECT_TRUE(metrics.contains("typesense_jemalloc_background_thread_num_runs"));
}
#endif

TEST(SystemMetricsTest, ParsingNetworkStats) {
    std::string proc_net_dev_path = std::string(ROOT_DIR)+"test/resources/proc_net_dev.txt";
    uint64_t received_bytes, sent_bytes;
    SystemMetrics::get_instance().linux_get_network_data(proc_net_dev_path, received_bytes, sent_bytes);
    ASSERT_EQ(324278716, received_bytes);
    ASSERT_EQ(93933882, sent_bytes);
}

// 2500Mi limit, 1GiB used — the common Kubernetes container scenario.
TEST(SystemMetricsTest, CgroupV2FiniteLimitReturnsContainerValues) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/cgroup/";
    uint64_t limit = 0, usage = 0;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        res + "proc/proc_self_cgroup_v2.txt",
        res + "fs");
    ASSERT_TRUE(ok);
    ASSERT_EQ(2621440000ULL, limit);
    ASSERT_EQ(1073741824ULL, usage);
}

// cgroupsv2 "max" means unlimited — must fall through to v1.
TEST(SystemMetricsTest, CgroupV2UnlimitedFallsThroughToV1) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/cgroup/";
    uint64_t limit = 0, usage = 0;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        res + "proc/proc_self_cgroup_v2_unlimited_v1_limited.txt",
        res + "fs");
    ASSERT_TRUE(ok);
    ASSERT_EQ(2621440000ULL, limit);
    ASSERT_EQ(1073741824ULL, usage);
}

// cgroupsv1 with a real limit.
TEST(SystemMetricsTest, CgroupV1FiniteLimitReturnsContainerValues) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/cgroup/";
    uint64_t limit = 0, usage = 0;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        res + "proc/proc_self_cgroup_v1_only.txt",
        res + "fs");
    ASSERT_TRUE(ok);
    ASSERT_EQ(2621440000ULL, limit);
    ASSERT_EQ(1073741824ULL, usage);
}

// cgroupsv1 unlimited sentinel — must return false so caller uses sysinfo.
TEST(SystemMetricsTest, CgroupV1UnlimitedSentinelReturnsFalse) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/cgroup/";
    uint64_t limit = 99, usage = 99;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        res + "proc/proc_self_cgroup_v1_unlimited.txt",
        res + "fs");
    ASSERT_FALSE(ok);
    ASSERT_EQ(99ULL, limit);
    ASSERT_EQ(99ULL, usage);
}

// No cgroup files at all — bare-metal / non-containerised path.
TEST(SystemMetricsTest, NoCgroupFilesReturnsFalse) {
    uint64_t limit = 99, usage = 99;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        "/nonexistent_proc_self_cgroup",
        "/nonexistent_cgroup_root");
    ASSERT_FALSE(ok);
    ASSERT_EQ(99ULL, limit);
    ASSERT_EQ(99ULL, usage);
}
