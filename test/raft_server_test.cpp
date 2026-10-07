#include <gtest/gtest.h>
#include <chrono>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <butil/at_exit.h>
#include <api_acl.h>

#define private public
#include "raft_server.h"
#include <braft/log.h>
#include <braft/node.h>
#include <braft/snapshot.h>
#undef private

namespace {
    class TestRaftStateMachine : public braft::StateMachine {
    public:
        void on_apply(braft::Iterator& iter) override {
            while(iter.valid()) {
                iter.next();
            }
        }

        int on_snapshot_load(braft::SnapshotReader*) override {
            return 0;
        }
    };

    int get_available_port() {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if(fd < 0) return -1;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if(bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            close(fd);
            return -1;
        }
        socklen_t length = sizeof(address);
        if(getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
            close(fd);
            return -1;
        }
        const int port = ntohs(address.sin_port);
        close(fd);
        return port;
    }

    class LocalRaftNode {
    public:
        explicit LocalRaftNode(const int port = get_available_port(), const std::string& root = {}) :
            port(port), root(root.empty() ? "/tmp/typesense-raft-iu4-" + std::to_string(getpid()) + "-" +
                                         std::to_string(port) : root),
            node(nullptr), server_started(false) {}

        bool init(const std::string& initial_nodes = "__default__", const int election_timeout_ms = 10000) {
            if(port <= 0) return false;
            std::filesystem::create_directories(root);
            butil::EndPoint endpoint;
            if(butil::str2endpoint("127.0.0.1", port, &endpoint) != 0) return false;
            if(braft::add_service(&server, endpoint) != 0 || server.Start(endpoint, nullptr) != 0) return false;
            server_started = true;

            braft::Configuration initial_conf;
            const std::string nodes = initial_nodes == "__default__" ?
                "127.0.0.1:" + std::to_string(port) + ":8108,127.0.0.1:1:8108,127.0.0.1:2:8108" : initial_nodes;
            if(!nodes.empty() && initial_conf.parse_from(nodes) != 0) return false;
            braft::NodeOptions options;
            options.election_timeout_ms = election_timeout_ms;
            options.snapshot_interval_s = -1;
            options.initial_conf = initial_conf;
            options.fsm = &fsm;
            options.log_uri = "local://" + root + "/log";
            options.raft_meta_uri = "local://" + root + "/meta";
            options.snapshot_uri = "local://" + root + "/snapshot";
            options.disable_cli = true;
            node = new braft::Node("iu3_test", braft::PeerId(endpoint, 8108));
            if(node->init(options) != 0) {
                delete node;
                node = nullptr;
                return false;
            }
            return true;
        }

        ~LocalRaftNode() {
            if(node != nullptr) {
                node->shutdown(nullptr);
                node->join();
                delete node;
            }
            if(server_started) {
                server.Stop(0);
                server.Join();
            }
            std::filesystem::remove_all(root);
        }

        std::string description() const {
            std::ostringstream output;
            node->_impl->describe(output, false);
            return output.str();
        }

        const int port;
        const std::string root;
        brpc::Server server;
        TestRaftStateMachine fsm;
        braft::Node* node;
        bool server_started;
    };

    bool append_log_entry(const std::string& raft_root, const int64_t index, const int64_t term,
                          const std::vector<braft::PeerId>* peers = nullptr) {
        std::filesystem::create_directories(raft_root);
        braft::ConfigurationManager ignored_configuration_manager;
        std::unique_ptr<braft::SegmentLogStorage> log_storage(
            new braft::SegmentLogStorage(raft_root + "/log"));
        if(!log_storage || log_storage->init(&ignored_configuration_manager) != 0) return false;
        auto* entry = new braft::LogEntry;
        entry->type = peers ? braft::ENTRY_TYPE_CONFIGURATION : braft::ENTRY_TYPE_NO_OP;
        entry->id = braft::LogId(index, term);
        if(peers) entry->peers = new std::vector<braft::PeerId>(*peers);
        entry->AddRef();
        const int result = log_storage->append_entry(entry);
        entry->Release();
        return result == 0;
    }

    bool append_configuration_log(const std::string& raft_root, const std::vector<braft::PeerId>& peers) {
        return append_log_entry(raft_root, 1, 1, &peers);
    }

    bool save_snapshot_configuration(const std::string& raft_root, const int64_t index, const int64_t term,
                                     const std::vector<std::string>& peers,
                                     const std::vector<std::string>& old_peers = {}) {
        braft::LocalSnapshotStorage snapshot_storage(raft_root + "/snapshot");
        if(snapshot_storage.init() != 0) return false;
        braft::SnapshotWriter* writer = snapshot_storage.create();
        if(writer == nullptr) return false;
        braft::SnapshotMeta meta;
        meta.set_last_included_index(index);
        meta.set_last_included_term(term);
        for(const auto& peer : peers) meta.add_peers(peer);
        for(const auto& peer : old_peers) meta.add_old_peers(peer);
        const int saved = writer->save_meta(meta);
        const int closed = snapshot_storage.close(writer);
        return saved == 0 && closed == 0;
    }

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

    std::string raft_peers_line(const std::string& description) {
        const auto peers_position = description.find("peers:");
        if(peers_position == std::string::npos) return {};
        const auto line_end = description.find('\n', peers_position);
        std::string peers_line = description.substr(
            peers_position, line_end == std::string::npos ? line_end : line_end - peers_position);
        if(!peers_line.empty() && peers_line.back() == '\r') peers_line.pop_back();
        return peers_line;
    }

    class ScopedConfigRestore {
    public:
        explicit ScopedConfigRestore(Config& config) : config(config), nodes(config.nodes),
            proxy_allowlist(config.proxy_allow_only_peer_src_ips), proxy_ips(config.proxy_allowed_src_ips) {}
        ~ScopedConfigRestore() {
            config.nodes = nodes;
            config.proxy_allow_only_peer_src_ips = proxy_allowlist;
            config.proxy_allowed_src_ips = proxy_ips;
        }
    private:
        Config& config;
        std::string nodes;
        bool proxy_allowlist;
        std::vector<std::string> proxy_ips;
    };

    class ScopedApiAclDestinationPolicy {
    public:
        explicit ScopedApiAclDestinationPolicy(const Config& config) :
            acl(APIAcl::instance()), configured_policy(config.get_proxy_disallowed_dest_cidrs()) {
            acl.set_disallowed_dest_cidrs("");
        }
        ~ScopedApiAclDestinationPolicy() {
            acl.set_disallowed_dest_cidrs(configured_policy);
        }
    private:
        APIAcl& acl;
        std::string configured_policy;
    };

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

TEST(RaftServerTest, MalformedHostnameSyntaxIsRejectedBeforeDnsResolution) {
    size_t resolver_calls = 0;
    const auto resolver = [&resolver_calls](const std::string&) {
        ++resolver_calls;
        return std::string();
    };

    for(const auto& config : {
            "bad/host:8107:8108",
            "bad\\host:8107:8108",
            "bad host:8107:8108",
            ".node:8107:8108",
            "node..:8107:8108",
            "node..example:8107:8108",
            "missing:8107:8108,bad/host:7107:7108",
            "missing:8107:8108,bad host:7107:7108"}) {
        resolver_calls = 0;
        auto result = ReplicationState::resolve_node_hosts(config, resolver);
        EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, result.status) << config;
        EXPECT_TRUE(result.configuration.empty()) << config;
        EXPECT_EQ(0, resolver_calls) << config;
    }
}

TEST(RaftServerTest, ValidDnsHostFormsRemainRetryableWhenUnresolved) {
    std::vector<std::string> resolved_hosts;
    const auto resolver = [&resolved_hosts](const std::string& host) {
        resolved_hosts.push_back(host);
        return std::string();
    };

    for(const auto& host : {"node_1", "node", "node.example."}) {
        resolved_hosts.clear();
        auto result = ReplicationState::resolve_node_hosts(std::string(host) + ":8107:8108", resolver);
        EXPECT_EQ(ReplicationState::PeerConfigStatus::unresolved_host, result.status) << host;
        EXPECT_TRUE(result.configuration.empty()) << host;
        ASSERT_EQ(1, resolved_hosts.size()) << host;
        EXPECT_EQ(host, resolved_hosts.front());
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

TEST(RaftServerTest, ExplicitApiPortsRequireCompleteDecimalValuesBeforeDns) {
    size_t resolver_calls = 0;
    const auto resolver = [&resolver_calls](const std::string&) {
        ++resolver_calls;
        return std::string("192.0.2.10");
    };

    for(const auto& config : {
            "127.0.0.1:8107:",
            "127.0.0.1:8107:not-a-port",
            "127.0.0.1:8107:8108garbage",
            "127.0.0.1:8107:0",
            "127.0.0.1:8107:65536",
            "127.0.0.1:8107:99999",
            "127.0.0.1:8107:999999999999999999999999",
            "127.0.0.1:8107: 8108",
            "127.0.0.1:8107:+8108",
            "127.0.0.1:8107:-1",
            "missing-host:8107:8108garbage",
            "[2001:db8::1]:8107:8108garbage"}) {
        resolver_calls = 0;
        const auto result = ReplicationState::resolve_node_hosts(config, resolver);
        EXPECT_EQ(ReplicationState::PeerConfigStatus::invalid_configuration, result.status) << config;
        EXPECT_TRUE(result.configuration.empty()) << config;
        EXPECT_EQ(0, resolver_calls) << config;
    }

    for(const auto& config : {"127.0.0.1:8107:1", "127.0.0.1:8107:65535",
                              "127.0.0.1:8108", "missing-host:8108",
                              "[2001:db8::1]:8107:65535"}) {
        resolver_calls = 0;
        const auto result = ReplicationState::resolve_node_hosts(config, resolver);
        if(config == std::string("missing-host:8108")) {
            ASSERT_TRUE(result.ok()) << result.diagnostic;
            EXPECT_EQ("192.0.2.10:8108", result.configuration);
            EXPECT_EQ(1, resolver_calls);
        } else {
            EXPECT_TRUE(result.ok()) << result.diagnostic;
            EXPECT_EQ(0, resolver_calls);
        }
    }
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

TEST(RaftServerTest, EmptyInitialConfigRestoresLogBackedMembership) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-log-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    const std::string self = "127.0.0.1:" + std::to_string(port) + ":8108";
    const braft::PeerId self_peer(self);
    const braft::PeerId other_peer("127.0.0.1:1:8108");
    ASSERT_TRUE(append_configuration_log(root, {self_peer, other_peer}));

    const auto production_probe = ReplicationState::inspect_durable_membership(root, self_peer);
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::recovered, production_probe.status);
    EXPECT_EQ(std::vector<std::string>{"127.0.0.1"}, production_probe.proxy_allowed_src_ips);
    LocalRaftNode node(port, root);
    ASSERT_TRUE(node.init("", 500));
    const std::string peers = raft_peers_line(node.description());
    EXPECT_NE(std::string::npos, peers.find(self));
    EXPECT_NE(std::string::npos, peers.find(other_peer.to_string()));
    EXPECT_FALSE(node.node->is_leader());
}

TEST(RaftServerTest, TemporarySnapshotDoesNotBlockLogBackedMembershipRecovery) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-temp-snapshot-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    const std::string self = "127.0.0.1:" + std::to_string(port) + ":8108";
    ASSERT_TRUE(append_configuration_log(root, {braft::PeerId(self), braft::PeerId("127.0.0.2:1:8108")}));
    const std::string temp_path = root + "/snapshot/temp";
    std::filesystem::create_directories(temp_path);
    std::ofstream(temp_path + "/partial.data") << "interrupted snapshot contents";

    const auto durable = ReplicationState::inspect_durable_membership(root, braft::PeerId(self));
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::recovered, durable.status) << durable.diagnostic;
    EXPECT_FALSE(std::filesystem::exists(temp_path))
        << "Braft should clean its unfinished temporary snapshot directory during inspection";

    LocalRaftNode node(port, root);
    ASSERT_TRUE(node.init("", 500));
    const std::string peers = raft_peers_line(node.description());
    EXPECT_NE(std::string::npos, peers.find(self));
    EXPECT_NE(std::string::npos, peers.find("127.0.0.2:1:8108"));
    EXPECT_FALSE(node.node->is_leader());
}

TEST(RaftServerTest, TemporarySnapshotWithoutMembershipIsFresh) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-temp-snapshot-fresh-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    const std::string temp_path = root + "/snapshot/temp";
    std::filesystem::create_directories(temp_path);
    std::ofstream(temp_path + "/partial.data") << "interrupted snapshot contents";

    const auto durable = ReplicationState::inspect_durable_membership(
        root, braft::PeerId("127.0.0.1:" + std::to_string(port) + ":8108"));
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::fresh, durable.status) << durable.diagnostic;
    EXPECT_FALSE(std::filesystem::exists(temp_path));
    std::filesystem::remove_all(root);
}

TEST(RaftServerTest, EmptyInitialConfigDoesNotElectFreshNode) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-fresh-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    const std::string self = "127.0.0.1:" + std::to_string(port) + ":8108";
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::fresh,
              ReplicationState::inspect_durable_membership(root, braft::PeerId(self)).status);
    LocalRaftNode node(port, root);
    ASSERT_TRUE(node.init("", 100));
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    EXPECT_FALSE(node.node->is_leader());
    EXPECT_EQ("peers:", raft_peers_line(node.description()));
}

TEST(RaftServerTest, EmptyInitialConfigRestoresSnapshotBackedMembership) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-snapshot-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    const std::string self = "127.0.0.1:" + std::to_string(port) + ":8108";
    ASSERT_TRUE(append_configuration_log(root, {braft::PeerId("127.0.0.1:1:8108")}));
    ASSERT_TRUE(save_snapshot_configuration(root, 2, 1, {self}));

    const braft::PeerId self_peer(self);
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::recovered,
              ReplicationState::inspect_durable_membership(root, self_peer).status);
    LocalRaftNode restarted(port, root);
    ASSERT_TRUE(restarted.init("", 500));
    EXPECT_EQ("peers: " + self_peer.to_string(), raft_peers_line(restarted.description()));
}

TEST(RaftServerTest, SnapshotConfigurationPrecedenceMatchesBraftLogManager) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string self = "127.0.0.1:" + std::to_string(port) + ":8108";
    const std::string excluded = "127.0.0.1:1:8108";
    const braft::PeerId self_peer(self);

    // A newer snapshot beyond the log tail discards an older log configuration.
    const std::string snapshot_wins_root = "/tmp/typesense-raft-iu4-snapshot-wins-" +
                                           std::to_string(getpid()) + "-" + std::to_string(port);
    ASSERT_TRUE(append_configuration_log(snapshot_wins_root, {braft::PeerId(excluded)}));
    ASSERT_TRUE(save_snapshot_configuration(snapshot_wins_root, 2, 1, {self}));
    const auto snapshot_wins = ReplicationState::inspect_durable_membership(snapshot_wins_root, self_peer);
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::recovered, snapshot_wins.status);
    LocalRaftNode restarted(port, snapshot_wins_root);
    ASSERT_TRUE(restarted.init("", 500));
    EXPECT_EQ("peers: " + self_peer.to_string(), raft_peers_line(restarted.description()));

    // Matching log term preserves a later configuration log, which excludes self.
    const int matching_port = get_available_port();
    ASSERT_GT(matching_port, 0);
    const braft::PeerId matching_self("127.0.0.1:" + std::to_string(matching_port) + ":8108");
    const std::string matching_root = "/tmp/typesense-raft-iu4-snapshot-match-" +
                                      std::to_string(getpid()) + "-" + std::to_string(matching_port);
    ASSERT_TRUE(append_configuration_log(matching_root, {braft::PeerId(excluded)}));
    ASSERT_TRUE(append_log_entry(matching_root, 2, 1));
    const std::vector<braft::PeerId> excluded_peer{braft::PeerId(excluded)};
    ASSERT_TRUE(append_log_entry(matching_root, 3, 1, &excluded_peer));
    ASSERT_TRUE(save_snapshot_configuration(matching_root, 2, 1, {matching_self.to_string()}));
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::invalid,
              ReplicationState::inspect_durable_membership(matching_root, matching_self).status);

    // A mismatched term truncates both sides of the snapshot boundary; snapshot wins.
    const int mismatch_port = get_available_port();
    ASSERT_GT(mismatch_port, 0);
    const std::string mismatch_root = "/tmp/typesense-raft-iu4-snapshot-mismatch-" +
                                      std::to_string(getpid()) + "-" + std::to_string(mismatch_port);
    ASSERT_TRUE(append_configuration_log(mismatch_root, {braft::PeerId(excluded)}));
    ASSERT_TRUE(append_log_entry(mismatch_root, 2, 2));
    ASSERT_TRUE(append_log_entry(mismatch_root, 3, 2, &excluded_peer));
    ASSERT_TRUE(save_snapshot_configuration(mismatch_root, 2, 1, {self}));
    EXPECT_EQ(ReplicationState::DurableMembershipStatus::recovered,
              ReplicationState::inspect_durable_membership(mismatch_root, self_peer).status);
    std::filesystem::remove_all(matching_root);
    std::filesystem::remove_all(mismatch_root);
}

TEST(RaftServerTest, RecoveredPeerAllowlistIsRestrictiveAndFailedRefreshKeepsIt) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-proxy-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    const std::string self = "127.0.0.1:" + std::to_string(port) + ":8108";
    ASSERT_TRUE(append_configuration_log(root, {braft::PeerId(self), braft::PeerId("127.0.0.2:1:8108")}));
    auto& config = Config::get_instance();
    ScopedConfigRestore restore_config(config);
    config.proxy_allow_only_peer_src_ips = true;
    ScopedApiAclDestinationPolicy restore_acl_policy(config);
    APIAcl& acl = APIAcl::instance();
    const auto durable = ReplicationState::inspect_durable_membership(root, braft::PeerId(self));
    ASSERT_EQ(ReplicationState::DurableMembershipStatus::recovered, durable.status);
    ReplicationState::apply_recovered_proxy_allowlist(durable);
    const auto allowed = config.get_proxy_allowed_src_ips();
    EXPECT_EQ((std::vector<std::string>{"127.0.0.1", "127.0.0.2"}), allowed);

    // The standard and SSE proxy handlers both pass the peer-only flag to this guard.
    EXPECT_TRUE(acl.is_allowed("127.0.0.1", "http://127.0.0.1:8108/path", allowed, true));
    EXPECT_FALSE(acl.is_allowed("192.0.2.1", "http://127.0.0.1:8108/path", allowed, true));

    ReplicationState state(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, false, &config, 1, 1);
    const std::atomic<bool> reset_on_error(true);
    state.refresh_nodes({ReplicationState::PeerConfigStatus::unresolved_host, self, "DNS unavailable"},
                        1, reset_on_error);
    EXPECT_EQ(allowed, config.get_proxy_allowed_src_ips());

    const int transitional_port = get_available_port();
    ASSERT_GT(transitional_port, 0);
    const std::string transitional_self = "127.0.0.1:" + std::to_string(transitional_port) + ":8108";
    const std::string transitional_root = "/tmp/typesense-raft-iu4-proxy-joint-" +
                                          std::to_string(getpid()) + "-" + std::to_string(transitional_port);
    ASSERT_TRUE(save_snapshot_configuration(transitional_root, 1, 1,
                                            {"127.0.0.2:" + std::to_string(transitional_port) + ":8108"},
                                            {transitional_self}));
    const auto transitional = ReplicationState::inspect_durable_membership(
        transitional_root, braft::PeerId(transitional_self));
    ASSERT_EQ(ReplicationState::DurableMembershipStatus::recovered, transitional.status);
    EXPECT_EQ((std::vector<std::string>{"127.0.0.2", "127.0.0.1"}),
              transitional.proxy_allowed_src_ips);
    std::filesystem::remove_all(transitional_root);

    const int ipv6_port = get_available_port();
    ASSERT_GT(ipv6_port, 0);
    const std::string ipv6_self = "[::1]:" + std::to_string(ipv6_port) + ":8108";
    const std::string ipv6_root = "/tmp/typesense-raft-iu4-proxy-ipv6-" +
                                  std::to_string(getpid()) + "-" + std::to_string(ipv6_port);
    ASSERT_TRUE(save_snapshot_configuration(ipv6_root, 1, 1, {ipv6_self}));
    const auto ipv6_durable = ReplicationState::inspect_durable_membership(ipv6_root, braft::PeerId(ipv6_self));
    ASSERT_EQ(ReplicationState::DurableMembershipStatus::recovered, ipv6_durable.status);
    EXPECT_TRUE(ipv6_durable.proxy_allowed_src_ips.empty());
    ReplicationState::apply_recovered_proxy_allowlist(ipv6_durable);
    EXPECT_FALSE(acl.is_allowed("192.0.2.1", "http://127.0.0.1:8108/path",
                                config.get_proxy_allowed_src_ips(), true));
    EXPECT_TRUE(acl.is_allowed("192.0.2.1", "http://127.0.0.1:8108/path", {}, false));
    std::filesystem::remove_all(root);
    std::filesystem::remove_all(ipv6_root);
}

TEST(RaftServerTest, NonemptyUnopenableSnapshotStorageIsNotFresh) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-corrupt-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    std::filesystem::create_directories(root + "/snapshot/unrecognized");
    std::ofstream(root + "/snapshot/unrecognized/invalid-meta") << "not a snapshot";

    EXPECT_EQ(ReplicationState::DurableMembershipStatus::invalid,
              ReplicationState::inspect_durable_membership(
                  root, braft::PeerId("127.0.0.1:" + std::to_string(port) + ":8108")).status);
    std::filesystem::remove_all(root);
}

TEST(RaftServerTest, MalformedCompletedSnapshotIsNotFresh) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-malformed-snapshot-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    const std::string self = "127.0.0.1:" + std::to_string(port) + ":8108";
    ASSERT_TRUE(append_configuration_log(root, {braft::PeerId(self)}));
    const std::string completed_snapshot = root + "/snapshot/snapshot_00000000000000000001";
    std::filesystem::create_directories(completed_snapshot);
    std::ofstream(completed_snapshot + "/__raft_snapshot_meta") << "not a valid Braft snapshot metadata file";

    EXPECT_EQ(ReplicationState::DurableMembershipStatus::invalid,
              ReplicationState::inspect_durable_membership(root, braft::PeerId(self)).status);
    std::filesystem::remove_all(root);
}

TEST(RaftServerTest, TemporarySnapshotPathMustBeDirectory) {
    butil::AtExitManager at_exit_manager;
    const int port = get_available_port();
    ASSERT_GT(port, 0);
    const std::string root = "/tmp/typesense-raft-iu4-temp-snapshot-file-" + std::to_string(getpid()) + "-" +
                             std::to_string(port);
    std::filesystem::create_directories(root + "/snapshot");
    std::ofstream(root + "/snapshot/temp") << "not a temporary snapshot directory";

    EXPECT_EQ(ReplicationState::DurableMembershipStatus::invalid,
              ReplicationState::inspect_durable_membership(
                  root, braft::PeerId("127.0.0.1:" + std::to_string(port) + ":8108")).status);
    std::filesystem::remove_all(root);
}

TEST(RaftServerTest, StartupRetryRereadsNodesFile) {
    const std::string path = "/tmp/typesense-raft-iu4-nodes-" + std::to_string(getpid());
    const std::string old_nodes = "127.0.0.1:1:8108";
    const std::string updated_nodes = "127.0.0.1:2:8108,127.0.0.1:3:8108";
    std::ofstream(path) << updated_nodes;
    std::string nodes_config = old_nodes;
    std::atomic<bool> quit(false);

    ASSERT_TRUE(ReplicationState::wait_for_nodes_retry(nodes_config, path, quit, std::chrono::milliseconds(0)));
    EXPECT_EQ(updated_nodes, nodes_config);
    std::filesystem::remove(path);
}

TEST(RaftServerTest, StartupRetryStopsPromptlyOnShutdown) {
    std::string nodes_config = "127.0.0.1:1:8108";
    std::atomic<bool> quit(false);
    std::thread signal_shutdown([&quit]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        quit.store(true);
    });
    const auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(ReplicationState::wait_for_nodes_retry(nodes_config, "", quit, std::chrono::seconds(5)));
    const auto elapsed = std::chrono::steady_clock::now() - start;
    signal_shutdown.join();
    EXPECT_LT(elapsed, std::chrono::seconds(2));
    EXPECT_EQ("127.0.0.1:1:8108", nodes_config);
}

TEST(RaftServerTest, FailedPeerResultsDoNotMutateLiveRaftMembership) {
    butil::AtExitManager at_exit_manager;
    LocalRaftNode local_node;
    ASSERT_TRUE(local_node.init());

    auto& config = Config::get_instance();
    ScopedConfigRestore restore_config(config);
    const std::vector<std::string> old_proxy_ips = config.get_proxy_allowed_src_ips();

    Store store(local_node.root + "/store");
    std::atomic<bool> skip_writes(false);
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    ReplicationState state(nullptr, &indexer, &store, nullptr, nullptr, nullptr, false, &config, 1, 1);
    state.node = local_node.node;
    state.peering_endpoint = local_node.node->node_id().peer_id.addr;

    const std::string self_peer = "127.0.0.1:" + std::to_string(local_node.port) + ":8108";
    const std::string initial_peers_line = raft_peers_line(local_node.description());
    ASSERT_FALSE(initial_peers_line.empty());
    ASSERT_NE(std::string::npos, initial_peers_line.find(self_peer));
    ASSERT_NE(std::string::npos, initial_peers_line.find("127.0.0.1:1:8108"));
    ASSERT_NE(std::string::npos, initial_peers_line.find("127.0.0.1:2:8108"));

    const std::atomic<bool> reset_on_error(true);
    ReplicationState::PeerConfigResult failed_prefix{
        ReplicationState::PeerConfigStatus::unresolved_host, self_peer, "unresolved later peer"};
    for(const auto& incomplete : {
            ReplicationState::PeerConfigResult{ReplicationState::PeerConfigStatus::unresolved_host,
                                                self_peer, "unresolved first peer"},
            ReplicationState::PeerConfigResult{ReplicationState::PeerConfigStatus::unresolved_host,
                                                self_peer, "unresolved middle peer"},
            ReplicationState::PeerConfigResult{ReplicationState::PeerConfigStatus::unresolved_host,
                                                self_peer, "unresolved last peer"}}) {
        state.refresh_nodes(incomplete, 1, reset_on_error);
        EXPECT_EQ(initial_peers_line, raft_peers_line(local_node.description()));
    }

    for(const auto& invalid_nodes : std::vector<std::string>{"not-a-peer," + self_peer,
                                                              self_peer + ",not-a-peer,127.0.0.1:3:8108",
                                                              self_peer + ",127.0.0.1:3:8108,not-a-peer",
                                                              self_peer + ",127.0.0.1:3:8108garbage",
                                                              self_peer + ",127.0.0.1:3:99999",
                                                              self_peer + ",", ""}) {
        ReplicationState::PeerConfigResult malformed{
            ReplicationState::PeerConfigStatus::resolved, invalid_nodes, ""};
        state.refresh_nodes(malformed, 1, reset_on_error);
        EXPECT_EQ(initial_peers_line, raft_peers_line(local_node.description())) << invalid_nodes;
    }

    state.last_snapshot_ts = 0;
    state.do_snapshot(failed_prefix);
    EXPECT_EQ(0u, state.last_snapshot_ts);

    const std::filesystem::path nodes_file = local_node.root + "/nodes.conf";
    {
        std::ofstream nodes(nodes_file);
        ASSERT_TRUE(nodes.good());
        nodes << "127.0.0.1:8107:8108,not-a-peer";
    }
    config.nodes = nodes_file.string();
    config.proxy_allow_only_peer_src_ips = true;
    state.peering_endpoint = local_node.node->node_id().peer_id.addr;
    EXPECT_FALSE(state.reset_peers());
    EXPECT_EQ(initial_peers_line, raft_peers_line(local_node.description()));
    EXPECT_EQ(old_proxy_ips, config.get_proxy_allowed_src_ips());

    {
        std::ofstream nodes(nodes_file);
        ASSERT_TRUE(nodes.good());
        nodes << self_peer << "garbage";
    }
    EXPECT_FALSE(state.reset_peers());
    EXPECT_EQ(initial_peers_line, raft_peers_line(local_node.description()));
    EXPECT_EQ(old_proxy_ips, config.get_proxy_allowed_src_ips());

    ReplicationState::PeerConfigResult valid_single_peer{
        ReplicationState::PeerConfigStatus::resolved, self_peer, ""};
    state.refresh_nodes(valid_single_peer, 0, reset_on_error);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    std::string final_description;
    do {
        final_description = raft_peers_line(local_node.description());
        if(final_description.find("peers: " + self_peer) != std::string::npos) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while(std::chrono::steady_clock::now() < deadline);
    EXPECT_EQ("peers: " + self_peer, final_description);
}

TEST(RaftServerTest, MalformedSelfApiPortCannotResetLiveMembership) {
    butil::AtExitManager at_exit_manager;
    LocalRaftNode local_node;
    ASSERT_TRUE(local_node.init());

    auto& config = Config::get_instance();
    ScopedConfigRestore restore_config(config);
    Store store(local_node.root + "/store");
    std::atomic<bool> skip_writes(false);
    BatchedIndexer indexer(nullptr, nullptr, nullptr, 1, config, skip_writes);
    ReplicationState state(nullptr, &indexer, &store, nullptr, nullptr, nullptr, false, &config, 1, 1);
    state.node = local_node.node;

    const std::string self_peer = "127.0.0.1:" + std::to_string(local_node.port) + ":8108";
    const std::string initial_peers_line = raft_peers_line(local_node.description());
    ASSERT_NE(std::string::npos, initial_peers_line.find(self_peer));
    ASSERT_NE(std::string::npos, initial_peers_line.find("127.0.0.1:1:8108"));
    ASSERT_NE(std::string::npos, initial_peers_line.find("127.0.0.1:2:8108"));

    const std::atomic<bool> reset_on_error(true);
    state.refresh_nodes({ReplicationState::PeerConfigStatus::resolved, self_peer + "garbage", ""},
                        1, reset_on_error);
    EXPECT_EQ(initial_peers_line, raft_peers_line(local_node.description()));
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
