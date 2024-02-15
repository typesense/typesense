#include <gtest/gtest.h>
#include <string>
#include "raft_server.h"

TEST(RaftServerTest, ResolveNodesConfigWithHostNames) {
    ASSERT_EQ("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
              ReplicationState::resolve_node_hosts("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108"));

    ASSERT_EQ("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
              ReplicationState::resolve_node_hosts("localhost:8107:8108,localhost:7107:7108,localhost:6107:6108"));

    ASSERT_EQ("localhost:8107:8108localhost:7107:7108,127.0.0.1:6107:6108",
              ReplicationState::resolve_node_hosts("localhost:8107:8108localhost:7107:7108,localhost:6107:6108"));

    // hostname must be less than 64 chars
    ASSERT_EQ("",
              ReplicationState::resolve_node_hosts("typesense-node-2.typesense-service.typesense-"
                                                   "namespace.svc.cluster.local:6107:6108"));
}