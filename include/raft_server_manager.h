#pragma once

#include <atomic>
#include <string>
#include <butil/endpoint.h>
#include <brpc/server.h>

class ReplicationState;
class Store;

// Direct extraction of start_raft_server function from typesense_server_utils.cpp
// TODO: make class
int start_raft_server(ReplicationState& replication_state, Store& store,
                      const std::string& state_dir, const std::string& path_to_nodes,
                      const std::string& peering_address, uint32_t peering_port,
                      const std::string& peering_subnet,
                      uint32_t api_port, int snapshot_interval_seconds,
                      int snapshot_max_byte_count_per_rpc,
                      const std::atomic<bool>& reset_peers_on_error);

// Helper functions extracted from typesense_server_utils.cpp
// TODO: make private
bool is_private_ipv4(uint32_t ip);
bool is_private_ipv6(const struct in6_addr* addr);
bool ipv6_prefix_match(const struct in6_addr* addr1, const struct in6_addr* addr2, uint32_t prefix_len);
butil::EndPoint get_internal_endpoint(const std::string& subnet_cidr, uint32_t peering_port);
