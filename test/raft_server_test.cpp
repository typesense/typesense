#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#define private public
#include "raft_server.h"
#undef private

namespace {
    std::shared_ptr<http_res> seed_indexer_request(BatchedIndexer& indexer, const uint64_t request_id,
                                                   const bool live_response) {
        auto req = std::make_shared<http_req>();
        req->start_ts = request_id;
        req->log_index = request_id;
        req->params["collection"] = "products";

        auto res = std::make_shared<http_res>(nullptr);
        res->is_alive = live_response;
        res->final = !live_response;
        indexer.req_res_map.emplace(
            request_id, BatchedIndexer::req_res_t(request_id, "", req, res, request_id,
                                                  1, 0, true, request_id));
        indexer.queues[0].emplace_back(request_id);
        indexer.queued_writes++;
        return res;
    }

    bool matches_either_ip_version(const std::string& result, 
                                 const std::string& ipv4_version,
                                 const std::string& ipv6_version) {
        return result == ipv4_version || result == ipv6_version;
    }

}

TEST(RaftServerTest, SnapshotLoadGateBlocksReadinessAndCatchupRefresh) {
    auto& config = Config::get_instance();
    ReplicationState replication_state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);

    replication_state.read_caught_up = true;
    replication_state.write_caught_up = true;
    replication_state.snapshot_load_blocks_readiness = true;
    EXPECT_FALSE(replication_state.is_read_caught_up());
    EXPECT_FALSE(replication_state.is_write_caught_up());

    std::unique_lock snapshot_load_lock(replication_state.snapshot_load_mutex);
    replication_state.snapshot_load_blocks_readiness = false;
    auto refresh = std::async(std::launch::async, [&replication_state]() {
        replication_state.refresh_catchup_status(false);
    });

    EXPECT_EQ(std::future_status::timeout, refresh.wait_for(std::chrono::milliseconds(50)));
    snapshot_load_lock.unlock();
    EXPECT_EQ(std::future_status::ready, refresh.wait_for(std::chrono::seconds(1)));
    EXPECT_FALSE(replication_state.is_read_caught_up());
    EXPECT_FALSE(replication_state.is_write_caught_up());
}

TEST(RaftServerTest, RestoresBatchedIndexerStateByStoreStatusAndFailsClosed) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    ReplicationState replication_state(nullptr, &indexer, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);

    auto read_error_res = seed_indexer_request(indexer, 10, true);
    EXPECT_EQ(1, replication_state.restore_batched_indexer_state(StoreStatus::ERROR, "", false));
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());
    EXPECT_EQ(503, read_error_res->status_code);
    EXPECT_TRUE(read_error_res->final);

    seed_indexer_request(indexer, 20, false);
    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(StoreStatus::NOT_FOUND, "", false));
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());

    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    seed_indexer_request(source_indexer, 30, false);
    nlohmann::json valid_state;
    source_indexer.serialize_state(valid_state);

    auto partially_invalid_state = valid_state;
    partially_invalid_state["req_res_map"]["40"] = {
        {"start_ts", 40},
        {"req", 123},
    };
    seed_indexer_request(indexer, 20, false);
    EXPECT_EQ(1, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, partially_invalid_state.dump(), false));
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());

    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, valid_state.dump(), false));
    ASSERT_EQ(1, indexer.req_res_map.size());
    EXPECT_EQ(1, indexer.req_res_map.count(30));

    auto reload_failure_res = seed_indexer_request(indexer, 50, true);
    replication_state.read_caught_up = true;
    replication_state.write_caught_up = true;
    replication_state.snapshot_load_blocks_readiness = false;
    {
        std::unique_lock lifecycle_lock(indexer.lifecycle_mutex);
        EXPECT_EQ(-7, replication_state.fail_snapshot_load_unlocked(-7));
    }
    EXPECT_TRUE(indexer.req_res_map.empty());
    EXPECT_TRUE(indexer.queues[0].empty());
    EXPECT_EQ(503, reload_failure_res->status_code);
    EXPECT_TRUE(reload_failure_res->final);
    EXPECT_FALSE(replication_state.is_read_caught_up());
    EXPECT_FALSE(replication_state.is_write_caught_up());
}

TEST(RaftServerTest, RestoresEmptyAndLegacyNullBatchedIndexerState) {
    std::atomic<bool> skip_writes(false);
    auto& config = Config::get_instance();
    BatchedIndexer source_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);

    nlohmann::json empty_state;
    source_indexer.serialize_state(empty_state);
    EXPECT_TRUE(empty_state["req_res_map"].is_object());
    EXPECT_TRUE(empty_state["req_res_map"].empty());

    BatchedIndexer restored_indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    ReplicationState replication_state(nullptr, &restored_indexer, nullptr, nullptr, nullptr, nullptr, false,
                                       &config, 1, 1);
    seed_indexer_request(restored_indexer, 10, false);
    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, empty_state.dump(), false));
    EXPECT_TRUE(restored_indexer.req_res_map.empty());
    EXPECT_TRUE(restored_indexer.queues[0].empty());

    auto legacy_empty_state = empty_state;
    legacy_empty_state["req_res_map"] = nullptr;
    seed_indexer_request(restored_indexer, 20, false);
    EXPECT_EQ(0, replication_state.restore_batched_indexer_state(
                     StoreStatus::FOUND, legacy_empty_state.dump(), false));
    EXPECT_TRUE(restored_indexer.req_res_map.empty());
    EXPECT_TRUE(restored_indexer.queues[0].empty());
}

TEST(RaftServerTest, ResolveNodesConfigWithHostNames) {
    auto numeric = ReplicationState::resolve_node_hosts("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108");
    ASSERT_TRUE(numeric.ok());
    EXPECT_EQ("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108", numeric.configuration);

    auto localhost = ReplicationState::resolve_node_hosts("localhost:8107:8108,localhost:7107:7108,localhost:6107:6108");
    ASSERT_TRUE(localhost.ok()) << localhost.diagnostic;
    EXPECT_TRUE(matches_either_ip_version(localhost.configuration,
        "127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
        "[::1]:8107:8108,[::1]:7107:7108,[::1]:6107:6108"));

    auto too_long = ReplicationState::resolve_node_hosts("typesense-node-2.typesense-service.typesense-namespace.svc.cluster.local:6107:6108");
    EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, too_long.status);
    auto empty = ReplicationState::resolve_node_hosts("");
    EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, empty.status);
    for(const auto& config : {"127.0.0.1:8107:8108,,127.0.0.2:7107:7108",
                              "127.0.0.1:8107:8108,"}) {
        auto result = ReplicationState::resolve_node_hosts(config);
        EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, result.status) << config;
        EXPECT_TRUE(result.configuration.empty()) << config;
    }
}

TEST(RaftServerTest, ResolveNodesConfigWithIPv6) {
    auto ipv6 = ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108");
    ASSERT_TRUE(ipv6.ok()) << ipv6.diagnostic;
    EXPECT_EQ("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108", ipv6.configuration);
    auto mixed = ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108");
    ASSERT_TRUE(mixed.ok()) << mixed.diagnostic;
    EXPECT_EQ("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108", mixed.configuration);
    auto ipv6_loopback = ReplicationState::resolve_node_hosts("[::1]:8107:8108");
    ASSERT_TRUE(ipv6_loopback.ok()) << ipv6_loopback.diagnostic;
    EXPECT_EQ("[::1]:8107:8108", ipv6_loopback.configuration);
    auto malformed = ReplicationState::resolve_node_hosts("[2001:db8::1:8107:8108");
    EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, malformed.status);
}

TEST(RaftServerTest, IncompleteDnsResolutionMustNotProduceAUsablePeerList) {
    const std::unordered_map<std::string, std::string> addresses = {
        {"node-a", "10.0.0.1"}, {"node-b", "10.0.0.2"}, {"node-c", "10.0.0.3"}
    };
    const auto lookup = [&addresses](const std::string& host) {
        auto it = addresses.find(host);
        return it == addresses.end() ? std::string() : it->second;
    };
    for(const auto& config : {"missing:8107:8108,node-b:7107:7108,node-c:6107:6108",
                              "node-a:8107:8108,missing:7107:7108,node-c:6107:6108",
                              "node-a:8107:8108,node-b:7107:7108,missing:6107:6108",
                              "missing-a:8107:8108,missing-b:7107:7108,missing-c:6107:6108"}) {
        auto result = ReplicationState::resolve_node_hosts(config, lookup);
        EXPECT_EQ(ReplicationState::PeerConfigStatus::unresolved_host, result.status) << config;
        EXPECT_TRUE(result.configuration.empty()) << config;
    }
    auto malformed = ReplicationState::resolve_node_hosts("node-a:8107:8108,not-a-peer,node-c:6107:6108", lookup);
    EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, malformed.status);
    EXPECT_TRUE(malformed.configuration.empty());
    auto precedence = ReplicationState::resolve_node_hosts("missing:8107:8108,not-a-peer", lookup);
    EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, precedence.status);

    bool empty_host_lookup_called = false;
    const auto empty_host_lookup = [&empty_host_lookup_called](const std::string&) {
        empty_host_lookup_called = true;
        return std::string("10.0.0.9");
    };
    for(const auto& config : {":8107:8108", ":8107"}) {
        auto empty_host = ReplicationState::resolve_node_hosts(config, empty_host_lookup);
        EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, empty_host.status) << config;
        EXPECT_TRUE(empty_host.configuration.empty()) << config;
        EXPECT_FALSE(empty_host_lookup_called) << config;
    }
}

TEST(RaftServerTest, ResolverInjectionPreservesValidIpv4Ipv6AndMixedLists) {
    const auto lookup = [](const std::string& host) {
        if(host == "v4-node") return std::string("192.0.2.10");
        if(host == "v6-node") return std::string("[2001:db8::10]");
        return std::string();
    };
    auto ipv4 = ReplicationState::resolve_node_hosts("v4-node:8107:8108", lookup);
    ASSERT_TRUE(ipv4.ok()) << ipv4.diagnostic;
    EXPECT_EQ("192.0.2.10:8107:8108", ipv4.configuration);
    auto ipv6 = ReplicationState::resolve_node_hosts("v6-node:7107:7108", lookup);
    ASSERT_TRUE(ipv6.ok()) << ipv6.diagnostic;
    EXPECT_EQ("[2001:db8::10]:7107:7108", ipv6.configuration);
    auto mixed = ReplicationState::resolve_node_hosts("v4-node:8107:8108,v6-node:7107:7108", lookup);
    ASSERT_TRUE(mixed.ok()) << mixed.diagnostic;
    EXPECT_EQ("192.0.2.10:8107:8108,[2001:db8::10]:7107:7108", mixed.configuration);
    auto whitespace = ReplicationState::resolve_node_hosts("v4-node:8108, v4-node:7108", lookup);
    ASSERT_TRUE(whitespace.ok()) << whitespace.diagnostic;
    EXPECT_EQ("192.0.2.10:8108,192.0.2.10:7108", whitespace.configuration);
    auto legacy = ReplicationState::resolve_node_hosts("192.0.2.10:8108");
    ASSERT_TRUE(legacy.ok()) << legacy.diagnostic;
    EXPECT_EQ("192.0.2.10:8108", legacy.configuration);
    auto legacy_hostname = ReplicationState::resolve_node_hosts("v4-node:8108", lookup);
    ASSERT_TRUE(legacy_hostname.ok()) << legacy_hostname.diagnostic;
    EXPECT_EQ("192.0.2.10:8108", legacy_hostname.configuration);
    auto missing_legacy_hostname = ReplicationState::resolve_node_hosts("missing:8108", lookup);
    EXPECT_EQ(ReplicationState::PeerConfigStatus::unresolved_host, missing_legacy_hostname.status);
    EXPECT_TRUE(missing_legacy_hostname.configuration.empty());
}

TEST(RaftServerTest, EmptyResolverInputAndAbsentNodesConfigurationDiffer) {
    auto empty_peer_list = ReplicationState::resolve_node_hosts("");
    EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, empty_peer_list.status);

    butil::EndPoint endpoint;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 8107, &endpoint));
    auto absent_nodes = ReplicationState::to_nodes_config(endpoint, 8108, "");
    ASSERT_TRUE(absent_nodes.ok()) << absent_nodes.diagnostic;
    EXPECT_EQ("127.0.0.1:8107:8108", absent_nodes.configuration);
}

TEST(RaftServerTest, BraftParseFailureRetainsOnlyTheValidPrefix) {
    // The parser's prefix behavior is the hazard IU-3 must keep from reaching
    // change_peers/reset_peers. This test characterizes Braft; it does not
    // treat a successful prefix parse as acceptable Typesense behavior.
    for(const auto& test_case : std::vector<std::pair<std::string, size_t>>{
            {"bad:8107:8108,127.0.0.2:7107:7108", 0},
            {"127.0.0.1:8107:8108,bad:7107:7108,127.0.0.3:6107:6108", 1},
            {"127.0.0.1:8107:8108,bad:7107:7108", 1},
            {"127.0.0.1:8107:8108,127.0.0.2:7107:7108,bad:6107:6108", 2},
            {"127.0.0.1:8107:8108,127.0.0.2:bad:7108", 1}}) {
        braft::Configuration parsed;
        EXPECT_NE(0, parsed.parse_from(test_case.first));
        std::vector<braft::PeerId> peers;
        parsed.list_peers(&peers);
        EXPECT_EQ(test_case.second, peers.size()) << test_case.first;
    }
}

TEST(Hostname2IPStrTest, IPAddresses) {
    // Test IPv4 addresses - should return unchanged
    ASSERT_EQ("127.0.0.1", ReplicationState::hostname2ipstr("127.0.0.1"));
    ASSERT_EQ("192.168.1.1", ReplicationState::hostname2ipstr("192.168.1.1"));

    // Test IPv6 addresses - should return unchanged if already in brackets
    ASSERT_EQ("[::1]", ReplicationState::hostname2ipstr("[::1]"));
    ASSERT_EQ("[2001:db8::1]", ReplicationState::hostname2ipstr("[2001:db8::1]"));
}

TEST(Hostname2IPStrTest, Localhost) {
    std::string result = ReplicationState::hostname2ipstr("localhost");

    // Should resolve to either 127.0.0.1 or [::1]
    ASSERT_TRUE(result == "127.0.0.1" || result == "[::1]")
        << "localhost resolved to: " << result;
}

TEST(Hostname2IPStrTest, InvalidHostnames) {
    // Test hostname that's too long (>64 chars)
    std::string long_hostname(65, 'a');
    ASSERT_EQ("", ReplicationState::hostname2ipstr(long_hostname));

    ASSERT_EQ("",
              ReplicationState::hostname2ipstr("non.existent.hostname.local",
                  [](const std::string&) { return std::string(); }));
}

TEST(Hostname2IPStrTest, ResolverResultsAreDeterministic) {
    EXPECT_EQ("192.0.2.10", ReplicationState::hostname2ipstr(
        "v4-node", [](const std::string&) { return std::string("192.0.2.10"); }));
    EXPECT_EQ("[2001:db8::10]", ReplicationState::hostname2ipstr(
        "v6-node", [](const std::string&) { return std::string("[2001:db8::10]"); }));
}
