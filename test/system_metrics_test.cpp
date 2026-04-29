#include <gtest/gtest.h>
#include "system_metrics.h"

TEST(SystemMetricsTest, ParsingNetworkStats) {
    std::string proc_net_dev_path = std::string(ROOT_DIR)+"test/resources/proc_net_dev.txt";
    uint64_t received_bytes, sent_bytes;
    SystemMetrics::get_instance().linux_get_network_data(proc_net_dev_path, received_bytes, sent_bytes);
    ASSERT_EQ(324278716, received_bytes);
    ASSERT_EQ(93933882, sent_bytes);
}

// 2500Mi limit, 1GiB used — the common Kubernetes container scenario.
TEST(SystemMetricsTest, CgroupV2FiniteLimitReturnsContainerValues) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/";
    uint64_t limit = 0, usage = 0;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        res + "cgroup_v2_memory_max.txt",
        res + "cgroup_v2_memory_current.txt",
        "/nonexistent_v1_limit",
        "/nonexistent_v1_usage");
    ASSERT_TRUE(ok);
    ASSERT_EQ(2621440000ULL, limit);
    ASSERT_EQ(1073741824ULL, usage);
}

// cgroupsv2 "max" means unlimited — must fall through to v1.
TEST(SystemMetricsTest, CgroupV2UnlimitedFallsThroughToV1) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/";
    uint64_t limit = 0, usage = 0;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        res + "cgroup_v2_memory_max_unlimited.txt",
        res + "cgroup_v2_memory_current.txt",
        res + "cgroup_v1_memory_limit.txt",
        res + "cgroup_v1_memory_usage.txt");
    ASSERT_TRUE(ok);
    ASSERT_EQ(2621440000ULL, limit);
    ASSERT_EQ(1073741824ULL, usage);
}

// cgroupsv1 with a real limit.
TEST(SystemMetricsTest, CgroupV1FiniteLimitReturnsContainerValues) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/";
    uint64_t limit = 0, usage = 0;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        "/nonexistent_v2_max",
        "/nonexistent_v2_current",
        res + "cgroup_v1_memory_limit.txt",
        res + "cgroup_v1_memory_usage.txt");
    ASSERT_TRUE(ok);
    ASSERT_EQ(2621440000ULL, limit);
    ASSERT_EQ(1073741824ULL, usage);
}

// cgroupsv1 unlimited sentinel — must return false so caller uses sysinfo.
TEST(SystemMetricsTest, CgroupV1UnlimitedSentinelReturnsFalse) {
    const std::string res = std::string(ROOT_DIR) + "test/resources/";
    uint64_t limit = 99, usage = 99;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        "/nonexistent_v2_max",
        "/nonexistent_v2_current",
        res + "cgroup_v1_memory_limit_unlimited.txt",
        res + "cgroup_v1_memory_usage.txt");
    ASSERT_FALSE(ok);
    ASSERT_EQ(99ULL, limit);
    ASSERT_EQ(99ULL, usage);
}

// No cgroup files at all — bare-metal / non-containerised path.
TEST(SystemMetricsTest, NoCgroupFilesReturnsFalse) {
    uint64_t limit = 99, usage = 99;
    bool ok = SystemMetrics::get_cgroup_memory(limit, usage,
        "/nonexistent_v2_max",
        "/nonexistent_v2_current",
        "/nonexistent_v1_limit",
        "/nonexistent_v1_usage");
    ASSERT_FALSE(ok);
    ASSERT_EQ(99ULL, limit);
    ASSERT_EQ(99ULL, usage);
}
