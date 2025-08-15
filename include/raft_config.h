#pragma once

#include <string>
#include <butil/endpoint.h>
#include <braft/raft.h>

/**
 * Namespace for Raft configuration and DNS utilities
 */
namespace raft {
    namespace config {

    /**
     * Resolves a hostname to an IP string.
     *
     * @param hostname The hostname to resolve (max 64 characters)
     * @return Resolved IP address string. IPv6 addresses are wrapped in [].
     *         Returns empty string on failure or original hostname if already an IP.
     */
    std::string hostname2ipstr(const std::string& hostname);

    /**
     * Resolves all node hostnames in a comma-separated configuration string.
     * Expected format: "host1:port1:port2,host2:port1:port2,..."
     *
     * @param nodes_config Comma-separated list of node configurations
     * @return Resolved configuration string with IPs instead of hostnames.
     *         Returns empty string if all DNS resolutions fail.
     */
    std::string resolve_node_hosts(const std::string& nodes_config);

    /**
     * Converts endpoint and API port to nodes configuration string.
     * Can return empty string if DNS resolution fails on all nodes.
     *
     * @param peering_endpoint The peering endpoint for this node
     * @param api_port The API port for this node
     * @param nodes_config Existing nodes configuration (can be empty)
     * @return Formatted nodes configuration string with resolved IPs.
     */
    std::string to_nodes_config(const butil::EndPoint& peering_endpoint,
                                int api_port,
                                const std::string& nodes_config);

    /**
     * Constructs a URL for a given peer.
     *
     * @param peer_id The peer identifier
     * @param path The URL path (e.g., "/health")
     * @param protocol The protocol ("http" or "https")
     * @return Complete URL string (e.g., "http://127.0.0.1:8108/health")
     */
    std::string get_node_url_path(const braft::PeerId& peer_id,
                                  const std::string& path,
                                  const std::string& protocol);
    } // namespace config
} // namespace raft
