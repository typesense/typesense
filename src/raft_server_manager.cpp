#include "raft_server_manager.h"
#include "raft_server.h"
#include "store.h"
#include "logger.h"
#include "config.h"
#include "string_utils.h"

#include <braft/raft.h>
#include <brpc/server.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <ifaddrs.h>

// Global from typesense_server_utils.cpp
// TODO: Make this a variable; this is a code smell
extern std::atomic<bool> quit_raft_service;

int RaftServerManager::start_raft_server(ReplicationState& replication_state, Store& store,
                      const std::string& state_dir, const std::string& path_to_nodes,
                      const std::string& peering_address, uint32_t peering_port, const std::string& peering_subnet,
                      uint32_t api_port, int snapshot_interval_seconds, int snapshot_max_byte_count_per_rpc,
                      const std::atomic<bool>& reset_peers_on_error) {

    if(path_to_nodes.empty()) {
        LOG(INFO) << "Since no --nodes argument is provided, starting a single node Typesense cluster.";
    }

    const Option<std::string>& nodes_config_op = Config::fetch_nodes_config(path_to_nodes);

    if(!nodes_config_op.ok()) {
        LOG(ERROR) << nodes_config_op.error();
        return -1;
    }

    butil::EndPoint peering_endpoint;
    int ip_conv_status = 0;

    if(!peering_address.empty()) {
        // If IPv6 address and not already wrapped in [], wrap it
        std::string normalized_addr = peering_address;
        if(peering_address.find(':') != std::string::npos &&
           peering_address.front() != '[' && peering_address.back() != ']') {
            normalized_addr = "[" + peering_address + "]";
        }

        ip_conv_status = butil::str2endpoint(normalized_addr.c_str(), peering_port, &peering_endpoint);

        if(ip_conv_status != 0) {
            LOG(ERROR) << "Failed to parse peering address `" << normalized_addr << "`";
            return -1;
        }
    } else {
        peering_endpoint = get_internal_endpoint(peering_subnet, peering_port);
    }

    // start peering server
    brpc::Server raft_server;

    if (braft::add_service(&raft_server, peering_endpoint) != 0) {
        LOG(ERROR) << "Failed to add peering service";
        return -1;  // Return error instead of exit
    }

    if (raft_server.Start(peering_endpoint, nullptr) != 0) {
        LOG(ERROR) << "Failed to start peering service";
        return -1;  // Return error instead of exit
    }

    size_t election_timeout_ms = 5000;

    if (replication_state.start(peering_endpoint, api_port, election_timeout_ms, snapshot_max_byte_count_per_rpc, state_dir,
                                nodes_config_op.get(), quit_raft_service) != 0) {
        LOG(ERROR) << "Failed to start peering state";
        // Clean up the server before returning
        raft_server.Stop(0);
        raft_server.Join();
        return -1;  // Return error instead of exit
    }

    LOG(INFO) << "Typesense peering service is running on " << raft_server.listen_address();
    LOG(INFO) << "Snapshot interval configured as: " << snapshot_interval_seconds << "s";
    LOG(INFO) << "Snapshot max byte count configured as: " << snapshot_max_byte_count_per_rpc;

    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    size_t raft_counter = 0;
    while (!brpc::IsAskedToQuit() && !quit_raft_service.load()) {
        if(raft_counter % 10 == 0) {
            // reset peer configuration periodically to identify change in cluster membership
            const Option<std::string> & refreshed_nodes_op = Config::fetch_nodes_config(path_to_nodes);
            if(!refreshed_nodes_op.ok()) {
                LOG(WARNING) << "Error while refreshing peer configuration: " << refreshed_nodes_op.error();
            } else {
                const std::string& nodes_config = ReplicationState::to_nodes_config(peering_endpoint, api_port,
                                                                                    refreshed_nodes_op.get());
                if(nodes_config.empty()) {
                    LOG(WARNING) << "No nodes resolved from peer configuration.";
                } else {
                    replication_state.refresh_nodes(nodes_config, raft_counter, reset_peers_on_error);
                    if(raft_counter % 60 == 0) {
                        replication_state.do_snapshot(nodes_config);
                    }
                }
            }
        }

        if(raft_counter % 3 == 0) {
            // update node catch up status periodically, take care of logging too verbosely
            bool log_msg = (raft_counter % 9 == 0);
            replication_state.refresh_catchup_status(log_msg);
        }

        raft_counter++;
        sleep(1);
    }

    LOG(INFO) << "Typesense peering service is going to quit.";

    // Stop application before server
    replication_state.shutdown();

    LOG(INFO) << "raft_server.stop()";
    raft_server.Stop(0);

    LOG(INFO) << "raft_server.join()";
    raft_server.Join();

    LOG(INFO) << "Typesense peering service has quit.";

    return 0;
}

bool RaftServerManager::is_private_ipv4(uint32_t ip) {
    uint8_t b1, b2;
    b1 = (uint8_t) (ip >> 24);
    b2 = (uint8_t) ((ip >> 16) & 0x0ff);

    // 10.x.y.z
    if (b1 == 10) {
        return true;
    }

    // 172.16.0.0 - 172.31.255.255
    if ((b1 == 172) && (b2 >= 16) && (b2 <= 31)) {
        return true;
    }

    // 192.168.0.0 - 192.168.255.255
    if ((b1 == 192) && (b2 == 168)) {
        return true;
    }

    return false;
}

bool RaftServerManager::is_private_ipv6(const struct in6_addr* addr) {
    // Check for fc00::/7 - Unique Local Address
    return (addr->s6_addr[0] & 0xfe) == 0xfc;
}

bool RaftServerManager::ipv6_prefix_match(const struct in6_addr* addr1, const struct in6_addr* addr2, uint32_t prefix_len) {
    const uint8_t* a1 = addr1->s6_addr;
    const uint8_t* a2 = addr2->s6_addr;

    // Compare whole bytes first
    const size_t whole_bytes = prefix_len / 8;
    for(size_t i = 0; i < whole_bytes && i < 16; i++) {
        if(a1[i] != a2[i]) return false;
    }

    // Then compare remaining bits if any
    if(prefix_len % 8) {
        const uint8_t mask = 0xff << (8 - (prefix_len % 8));
        if((a1[whole_bytes] & mask) != (a2[whole_bytes] & mask)) {
            return false;
        }
    }

    return true;
}

butil::EndPoint RaftServerManager::get_internal_endpoint(const std::string& subnet_cidr, uint32_t peering_port) {
    struct ifaddrs *ifap;
    getifaddrs(&ifap);

    butil::EndPoint subnet_endpoint;
    uint32_t netbits = 0;
    sa_family_t target_family = AF_UNSPEC;

    if(!subnet_cidr.empty()) {
        std::vector<std::string> subnet_parts;
        StringUtils::split(subnet_cidr, subnet_parts, "/");
        if(subnet_parts.size() == 2) {
            // If a v6 address, wrap in []
            auto subnet_addr = subnet_parts[0].find(':') != std::string::npos ? '[' + subnet_parts[0] + "]" : subnet_parts[0];
            const int retCode = butil::str2endpoint(subnet_addr.c_str(), 0, &subnet_endpoint);
            if(retCode == 0) {
                try {
                    netbits = std::stoul(subnet_parts[1]);
                    if(netbits > 0) {
                        target_family = butil::get_endpoint_type(subnet_endpoint);
                    }
                    LOG(INFO) << "Using subnet with address family: " << (target_family == AF_INET ? "IPv4" : "IPv6");
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Failed to parse subnet prefix length: " << subnet_parts[1];
                }
            }
        }
    }

    struct sockaddr_storage subnet_addr;
    socklen_t subnet_size;
    if(target_family != AF_UNSPEC) {
        butil::endpoint2sockaddr(subnet_endpoint, &subnet_addr, &subnet_size);
    }

    butil::EndPoint ipv4_endpoint;
    butil::EndPoint ipv6_endpoint;
    bool found_ipv4 = false;
    bool found_ipv6 = false;

    for(auto ifa = ifap; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) {
            continue;
        }

        // If subnet specified, only look at matching address family
        if(target_family != AF_UNSPEC && ifa->ifa_addr->sa_family != target_family) {
            continue;
        }

        if(ifa->ifa_addr->sa_family == AF_INET) {
            auto sa = (struct sockaddr_in*) ifa->ifa_addr;
            auto ipaddr = sa->sin_addr.s_addr;

            if(is_private_ipv4(ntohl(ipaddr))) {
                if(target_family == AF_INET) {
                    // Check if matches subnet
                    auto subnet_sa = (struct sockaddr_in*)&subnet_addr;
                    uint32_t mask = 0xFFFFFFFF << (32 - netbits);
                    if((ntohl(subnet_sa->sin_addr.s_addr) & mask) != (ntohl(ipaddr) & mask)) {
                        LOG(INFO) << "Skipping interface " << ifa->ifa_name << " as it does not match IPv4 subnet.";
                        continue;
                    }
                }

                // Create endpoint directly from sockaddr
                sa->sin_port = htons(peering_port);
                struct sockaddr_storage ss;
                memcpy(&ss, sa, sizeof(*sa));
                if(butil::sockaddr2endpoint(&ss, sizeof(*sa), &ipv4_endpoint) == 0) {
                    found_ipv4 = true;
                    if(target_family == AF_INET) {
                        break;  // Found match for specified subnet
                    }
                }
            }
        } else if(ifa->ifa_addr->sa_family == AF_INET6) {
            auto sa6 = (struct sockaddr_in6*) ifa->ifa_addr;

            if(is_private_ipv6(&sa6->sin6_addr)) {
                if(target_family == AF_INET6) {
                    // Check if matches subnet
                    auto subnet_sa6 = (struct sockaddr_in6*)&subnet_addr;
                    if(!ipv6_prefix_match(&subnet_sa6->sin6_addr, &sa6->sin6_addr, netbits)) {
                        LOG(INFO) << "Skipping interface " << ifa->ifa_name << " as it does not match IPv6 subnet.";
                        continue;
                    }
                }

                // Create endpoint directly from sockaddr
                sa6->sin6_port = htons(peering_port);
                struct sockaddr_storage ss;
                memcpy(&ss, sa6, sizeof(*sa6));
                if(butil::sockaddr2endpoint(&ss, sizeof(*sa6), &ipv6_endpoint) == 0) {
                    found_ipv6 = true;
                    if(target_family == AF_INET6) {
                        break;  // Found match for specified subnet
                    }
                }
            }
        }
    }

    freeifaddrs(ifap);

    // Return results based on what we found
    if(target_family == AF_INET6 && found_ipv6) {
        return ipv6_endpoint;
    } else if(target_family == AF_INET && found_ipv4) {
        return ipv4_endpoint;
    } else if(found_ipv4) {
        return ipv4_endpoint;
    } else if(found_ipv6) {
        return ipv6_endpoint;
    }

    // Return endpoint with loopback address if nothing found
    butil::EndPoint loopback;
    auto loopbackAddr = target_family == AF_INET6 ? "[::1]" : "127.0.0.1";
    butil::str2endpoint(loopbackAddr, peering_port, &loopback);
    LOG(WARNING) << "Found no matching interfaces, using loopback address.";
    return loopback;
}
