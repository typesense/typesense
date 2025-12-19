#pragma once

#include <atomic>
#include <string>
#include <butil/endpoint.h>
#include <brpc/server.h>

class ReplicationState;
class Store;

class RaftServerManager {
public:
    // Singleton access
    static RaftServerManager& get_instance() {
        static RaftServerManager instance;
        return instance;
    }

    // Main function
    int start_raft_server(ReplicationState& replication_state, Store& store,
                          const std::string& state_dir, const std::string& path_to_nodes,
                          const std::string& peering_address, uint32_t peering_port,
                          const std::string& peering_subnet,
                          uint32_t api_port, int snapshot_interval_seconds,
                          int snapshot_max_byte_count_per_rpc,
                          const std::atomic<bool>& reset_peers_on_error);

private:
    // Constructor/destructor (singleton)
    RaftServerManager() = default;
    ~RaftServerManager() = default;

    // Non-copyable, non-movable
    RaftServerManager(const RaftServerManager&) = delete;
    RaftServerManager(RaftServerManager&&) = delete;
    RaftServerManager& operator=(const RaftServerManager&) = delete;
    RaftServerManager& operator=(RaftServerManager&&) = delete;

    // Helper functions
    void shutdown_peering_server(brpc::Server& peering_server);
    bool is_private_ipv4(uint32_t ip);
    bool is_private_ipv6(const struct in6_addr* addr);
    bool ipv6_prefix_match(const struct in6_addr* addr1, const struct in6_addr* addr2, uint32_t prefix_len);
    butil::EndPoint get_internal_endpoint(const std::string& subnet_cidr, uint32_t peering_port);
};
