#include <gtest/gtest.h>
#include "system_metrics.h"

TEST(SystemMetricsTest, ParsingNetworkStats) {
    std::string proc_net_dev_path = std::string(ROOT_DIR)+"test/resources/proc_net_dev.txt";
    uint64_t received_bytes, sent_bytes;
    SystemMetrics::linux_get_network_data(proc_net_dev_path, received_bytes, sent_bytes);
    ASSERT_EQ(324278716, received_bytes);
    ASSERT_EQ(93933882, sent_bytes);
}