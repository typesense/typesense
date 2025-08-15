#include "store.h"
#include "raft_server.h"
#include <string_utils.h>
#include <logger.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

// DNS and Configuration Management Module
// Extracted from raft_server.cpp for better organization

std::string ReplicationState::hostname2ipstr(const std::string& hostname) {
    if(hostname.size() > 64) {
        LOG(ERROR) << "Host name is too long (must be < 64 characters): " << hostname;
        return "";
    }

    // Check if this is already an IPv6 address by looking for []
    if(hostname.find('[') == 0) {
        return hostname;
    }

    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;     // Allow both IPv4 and IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP

    int status = getaddrinfo(hostname.c_str(), nullptr, &hints, &result);
    if (status != 0) {
        LOG(ERROR) << "Unable to resolve host: " << hostname << ", error: " << gai_strerror(status);
        return hostname; // Return original hostname on error
    }

    char ip_str[INET6_ADDRSTRLEN];
    std::string resolved_ip;

    // Get the first resolved address
    if (result->ai_family == AF_INET) {
        // IPv4
        struct sockaddr_in *addr = (struct sockaddr_in *)result->ai_addr;
        inet_ntop(AF_INET, &(addr->sin_addr), ip_str, INET_ADDRSTRLEN);
        resolved_ip = ip_str;
    } else if (result->ai_family == AF_INET6) {
        // IPv6
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)result->ai_addr;
        inet_ntop(AF_INET6, &(addr->sin6_addr), ip_str, INET6_ADDRSTRLEN);
        resolved_ip = std::string("[") + ip_str + "]";
    }

    freeaddrinfo(result);

    if(resolved_ip.empty()) {
        return hostname; // Return original hostname if resolution didn't produce a valid IP
    }

    return resolved_ip;
}

std::string ReplicationState::resolve_node_hosts(const std::string& nodes_config) {
    std::vector<std::string> final_nodes_vec;
    std::vector<std::string> node_strings;
    StringUtils::split(nodes_config, node_strings, ",");

    for(const auto& node_str: node_strings) {
        // Check if this is already an IPv6 address node by looking for []
        if(node_str.find('[') == 0) {
            final_nodes_vec.push_back(node_str);
            continue;
        }

        // could be an IP or a hostname that must be resolved
        std::vector<std::string> node_parts;
        StringUtils::split(node_str, node_parts, ":");

        if(node_parts.size() != 3) {
            final_nodes_vec.push_back(node_str);
            continue;
        }

        std::string resolved_ip = hostname2ipstr(node_parts[0]);
        if(resolved_ip.empty()) {
            LOG(ERROR) << "Unable to resolve host: " << node_parts[0];
            continue;
        }

        final_nodes_vec.push_back(resolved_ip + ":" + node_parts[1] + ":" + node_parts[2]);
    }

    if(final_nodes_vec.empty()) {
        return "";
    }

    std::string final_nodes_config = StringUtils::join(final_nodes_vec, ",");
    return final_nodes_config;
}

// can return empty string if DNS resolution fails on all nodes
std::string ReplicationState::to_nodes_config(const butil::EndPoint& peering_endpoint, const int api_port,
                                              const std::string& nodes_config) {
    if(nodes_config.empty()) {
        // endpoint2str gives us "<ip>:<peering_port>", we just need to add ":<api_port>"
        return std::string(butil::endpoint2str(peering_endpoint).c_str()) + ":" + std::to_string(api_port);
    } else {
        return resolve_node_hosts(nodes_config);
    }
}
