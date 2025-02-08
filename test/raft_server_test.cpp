#include <gtest/gtest.h>
#include <string>
#include "raft_server.h"

namespace {
    bool matches_either_ip_version(const std::string& result, 
                                 const std::string& ipv4_version,
                                 const std::string& ipv6_version) {
        return result == ipv4_version || result == ipv6_version;
    }
}

TEST(RaftServerTest, ResolveNodesConfigWithHostNames) {
    ASSERT_EQ("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
              ReplicationState::resolve_node_hosts("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108"));

    // Test localhost resolution - should accept either IPv4 or IPv6
    std::string localhost_result1 = ReplicationState::resolve_node_hosts("localhost:8107:8108,localhost:7107:7108,localhost:6107:6108");
    ASSERT_TRUE(matches_either_ip_version(
        localhost_result1,
        "127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
        "[::1]:8107:8108,[::1]:7107:7108,[::1]:6107:6108"
    )) << "Result was: " << localhost_result1;

    std::string localhost_result2 = ReplicationState::resolve_node_hosts("localhost:8107:8108localhost:7107:7108,localhost:6107:6108");
    ASSERT_TRUE(matches_either_ip_version(
        localhost_result2,
        "localhost:8107:8108localhost:7107:7108,127.0.0.1:6107:6108",
        "localhost:8107:8108localhost:7107:7108,[::1]:6107:6108"
    )) << "Result was: " << localhost_result2;

    // hostname must be less than 64 chars
    ASSERT_EQ("",
              ReplicationState::resolve_node_hosts("typesense-node-2.typesense-service.typesense-"
                                                   "namespace.svc.cluster.local:6107:6108"));
}

TEST(RaftServerTest, ResolveNodesConfigWithIPv6) {
    // Basic IPv6 addresses
    ASSERT_EQ("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108",
              ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108"));

    // IPv6 with IPv4 mixed
    ASSERT_EQ("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108",
              ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108"));

    // IPv6 localhost
    ASSERT_EQ("[::1]:8107:8108",
              ReplicationState::resolve_node_hosts("[::1]:8107:8108"));

    // Malformed IPv6 inputs should be passed through unchanged
    ASSERT_EQ("[2001:db8::1:8107:8108",  // Missing closing bracket
              ReplicationState::resolve_node_hosts("[2001:db8::1:8107:8108"));

    // IPv6 with zone index
    ASSERT_EQ("[fe80::1%eth0]:8107:8108",
              ReplicationState::resolve_node_hosts("[fe80::1%eth0]:8107:8108"));

    // Test with real IPv6 hostname resolution - need to skip if resolution fails
    std::string ipv6_result = ReplicationState::resolve_node_hosts("ipv6.test-ipv6.com:8107:8108");
    if (!ipv6_result.empty()) {
        EXPECT_TRUE(ipv6_result.find('[') == 0);  // Should start with '[' for IPv6
        EXPECT_TRUE(ipv6_result.find("]:8107:8108") != std::string::npos);
    }
}