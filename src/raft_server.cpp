#include "raft_server.h"
#include "core_api.h"
#include "personalization_model_manager.h"
#include "rocksdb/utilities/checkpoint.h"
#include "store.h"
#include "thread_local_vars.h"
#include <algorithm>
#include <arpa/inet.h>
#include <butil/files/file_enumerator.h>
#include <collection_manager.h>
#include <conversation_model_manager.h>
#include <file_utils.h>
#include <housekeeper.h>
#include <http_client.h>
#include <string_utils.h>
#include <thread>
#include <filesystem>
#include <memory>
#include <braft/log.h>
#include <braft/snapshot.h>

namespace braft {
    DECLARE_int32(raft_do_snapshot_min_index_gap);
    DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
    DECLARE_bool(raft_enable_append_entries_cache);
    DECLARE_int32(raft_max_append_entries_cache_size);

    DECLARE_int32(raft_max_byte_count_per_rpc);
    DECLARE_int32(raft_rpc_channel_connect_timeout_ms);
}

namespace {
std::string lookup_hostname_ip(const std::string& host) {
    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int status = getaddrinfo(host.c_str(), nullptr, &hints, &result);
    if(status != 0) {
        LOG(ERROR) << "Unable to resolve host: " << host << ", error: " << gai_strerror(status);
        return {};
    }

    char ip_str[INET6_ADDRSTRLEN];
    std::string resolved_ip;
    if(result->ai_family == AF_INET) {
        auto* addr = reinterpret_cast<sockaddr_in*>(result->ai_addr);
        inet_ntop(AF_INET, &(addr->sin_addr), ip_str, INET_ADDRSTRLEN);
        resolved_ip = ip_str;
    } else if(result->ai_family == AF_INET6) {
        auto* addr = reinterpret_cast<sockaddr_in6*>(result->ai_addr);
        inet_ntop(AF_INET6, &(addr->sin6_addr), ip_str, INET6_ADDRSTRLEN);
        resolved_ip = std::string("[") + ip_str + "]";
    }
    freeaddrinfo(result);
    return resolved_ip;
}

bool parse_peer_configuration(const std::string& nodes, braft::Configuration& configuration) {
    if(nodes.empty()) {
        return false;
    }

    std::vector<std::string> node_entries;
    StringUtils::split(nodes, node_entries, ",", true, true);
    if(node_entries.empty() || std::any_of(node_entries.begin(), node_entries.end(),
                                           [](const std::string& entry) { return entry.empty(); })) {
        return false;
    }

    braft::Configuration parsed;
    if(parsed.parse_from(nodes) != 0 || parsed.empty()) {
        return false;
    }

    configuration = std::move(parsed);
    return true;
}
}

void ReplicationClosure::Run() {
    // nothing much to do here since responding to client is handled upstream
    // Auto delete `this` after Run()
    std::unique_ptr<ReplicationClosure> self_guard(this);
}

// State machine implementation

int ReplicationState::start(const butil::EndPoint & peering_endpoint, const int api_port,
                            int election_timeout_ms, int snapshot_max_byte_count_per_rpc,
                            const std::string & raft_dir, const std::string & nodes,
                            const std::string& nodes_config_path,
                            const std::atomic<bool>& quit_abruptly) {

    this->election_timeout_interval_ms = election_timeout_ms;
    this->raft_dir_path = raft_dir;
    this->peering_endpoint = peering_endpoint;

    braft::NodeOptions node_options;

    bool recovered_durable_membership = false;
    bool recovered_snapshot = false;
    bool membership_inspected = false;
    std::string current_nodes = nodes;

    while(true) {
        PeerConfigResult peer_config = to_nodes_config(peering_endpoint, api_port, current_nodes);

        if(peer_config.status == PeerConfigStatus::invalid_configuration) {
            LOG(ERROR) << "Invalid nodes configuration: " << peer_config.diagnostic;
            return -1;
        }

        if(peer_config.status == PeerConfigStatus::unresolved_host) {
            if(!membership_inspected) {
                membership_inspected = true;
                const auto durable = inspect_durable_membership(
                    raft_dir, braft::PeerId(peering_endpoint, api_port));
                if(durable.status == DurableMembershipStatus::recovered) {
                    if(quit_abruptly.load()) return -1;
                    recovered_durable_membership = true;
                    recovered_snapshot = durable.has_snapshot;
                    apply_recovered_proxy_allowlist(durable);
                    LOG(WARNING) << "Starting from durable Raft membership while configured peer DNS is unresolved: "
                                 << durable.diagnostic;
                    break;
                }
                if(durable.status == DurableMembershipStatus::invalid) {
                    LOG(ERROR) << "Cannot safely start while peer DNS is unresolved: " << durable.diagnostic;
                    return -1;
                }
            }

            LOG(WARNING) << "Waiting for complete DNS resolution of nodes configuration: `" << current_nodes << "`: "
                         << peer_config.diagnostic << "; will retry shortly...";
            if(!wait_for_nodes_retry(current_nodes, nodes_config_path, quit_abruptly)) return -1;
            continue;
        }

        const std::string& actual_nodes_config = peer_config.configuration;
        if(node_options.initial_conf.parse_from(actual_nodes_config) != 0) {
            LOG(ERROR) << "Validated nodes configuration could not be parsed by Braft: " << actual_nodes_config;
            return -1;
        }

        if(Config::get_instance().get_proxy_allow_only_peer_src_ips()) {
            Config::get_instance().update_proxy_src_ips(actual_nodes_config);
        }

        LOG(INFO) << "Nodes configuration: " << actual_nodes_config;
        break;
    }

    if(recovered_durable_membership) {
        node_options.initial_conf = braft::Configuration();
        LOG(INFO) << "Using persisted Raft membership; initial configuration is intentionally empty.";
    }

    this->read_caught_up = false;
    this->write_caught_up = false;

    // do snapshot only when the gap between applied index and last snapshot index is >= this number
    braft::FLAGS_raft_do_snapshot_min_index_gap = 1;

    // flags for controlling parallelism of append entries
    braft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
    braft::FLAGS_raft_enable_append_entries_cache = false;
    braft::FLAGS_raft_max_append_entries_cache_size = 8;

    // flag controls snapshot download size of each RPC
    braft::FLAGS_raft_max_byte_count_per_rpc = snapshot_max_byte_count_per_rpc;

    braft::FLAGS_raft_rpc_channel_connect_timeout_ms = 2000;

    // automatic snapshot is disabled since it caused issues during slow follower catch-ups
    node_options.snapshot_interval_s = -1;

    node_options.catchup_margin = config->get_healthy_read_lag();
    node_options.election_timeout_ms = election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.filter_before_copy_remote = true;
    std::string prefix = "local://" + raft_dir;
    node_options.log_uri = prefix + "/" + log_dir_name;
    node_options.raft_meta_uri = prefix + "/" + meta_dir_name;
    node_options.snapshot_uri = prefix + "/" + snapshot_dir_name;
    node_options.disable_cli = true;

    // api_port is used as the node identifier
    braft::Node* node = new braft::Node("default_group", braft::PeerId(peering_endpoint, api_port));

    std::string snapshot_dir = raft_dir + "/" + snapshot_dir_name;
    bool snapshot_exists = recovered_durable_membership ? recovered_snapshot : dir_enum_count(snapshot_dir) > 0;

    if(snapshot_exists) {
        // we will be assured of on_snapshot_load() firing and we will wait for that to init_db()
    } else {
        LOG(INFO) << "Snapshot does not exist. We will remove db dir and init db fresh.";

        int reload_store = store->reload(true, "");
        if(reload_store != 0) {
            return reload_store;
        }

        int init_db_status = init_db();
        if(init_db_status != 0) {
            LOG(ERROR) << "Failed to initialize DB.";
            return init_db_status;
        }
    }

    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init peering node";
        delete node;
        return -1;
    }

    braft::NodeStatus node_status;
    node->get_status(&node_status);

    LOG(INFO) << "Node last_index: " << node_status.last_index;

    std::unique_lock lock(node_mutex);
    this->node = node;
    return 0;
}

bool ReplicationState::wait_for_nodes_retry(std::string& nodes_config, const std::string& nodes_config_path,
                                            const std::atomic<bool>& quit_abruptly,
                                            const std::chrono::milliseconds retry_interval) {
    const auto deadline = std::chrono::steady_clock::now() + retry_interval;
    while(!quit_abruptly.load()) {
        const auto remaining = deadline - std::chrono::steady_clock::now();
        if(remaining <= std::chrono::steady_clock::duration::zero()) break;
        std::this_thread::sleep_for(std::min(
            remaining, std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::milliseconds(100))));
    }
    if(quit_abruptly.load()) return false;
    if(!nodes_config_path.empty()) {
        const auto refreshed = Config::fetch_nodes_config(nodes_config_path);
        if(!refreshed.ok()) {
            LOG(WARNING) << "Unable to reread nodes configuration while waiting for DNS: " << refreshed.error();
        } else {
            nodes_config = refreshed.get();
        }
    }
    return true;
}

ReplicationState::DurableMembershipResult ReplicationState::inspect_durable_membership(
        const std::string& raft_dir, const braft::PeerId& local_peer) {
    DurableMembershipResult result;
    const std::string snapshot_path = raft_dir + "/" + snapshot_dir_name;
    std::error_code fs_error;
    const bool snapshot_exists = std::filesystem::exists(snapshot_path, fs_error);
    if(fs_error) {
        result.diagnostic = "unable to inspect snapshot directory " + snapshot_path + ": " + fs_error.message();
        return result;
    }
    bool snapshot_had_entries = false;
    if(snapshot_exists) {
        if(!std::filesystem::is_directory(snapshot_path, fs_error) || fs_error) {
            result.diagnostic = "snapshot path is not an accessible directory: " + snapshot_path;
            return result;
        }
        std::filesystem::directory_iterator it(snapshot_path, fs_error);
        if(fs_error) {
            result.diagnostic = "unable to enumerate snapshot directory " + snapshot_path + ": " + fs_error.message();
            return result;
        }
        const std::filesystem::directory_iterator end;
        while(it != end) {
            const auto entry_path = it->path();
            const auto entry_status = it->symlink_status(fs_error);
            if(fs_error) {
                result.diagnostic = "unable to inspect snapshot entry " + entry_path.string() + ": " + fs_error.message();
                return result;
            }
            // Braft uses `temp` for an in-progress snapshot and removes it during init; it is not a
            // completed snapshot that failed to open.
            const bool unfinished_snapshot_directory = entry_path.filename() == "temp" &&
                                                       entry_status.type() == std::filesystem::file_type::directory;
            if(!unfinished_snapshot_directory) {
                snapshot_had_entries = true;
            }
            it.increment(fs_error);
            if(fs_error) {
                result.diagnostic = "unable to enumerate snapshot directory " + snapshot_path + ": " + fs_error.message();
                return result;
            }
        }
    }

    braft::ConfigurationManager configurations;
    std::unique_ptr<braft::SegmentLogStorage> logs(
        new braft::SegmentLogStorage(raft_dir + "/" + log_dir_name));
    if(logs->init(&configurations) != 0) {
        result.diagnostic = "unable to load durable Raft log configuration";
        return result;
    }
    const int64_t last_log_index = logs->last_log_index();

    std::unique_ptr<braft::LocalSnapshotStorage> snapshots(
        new braft::LocalSnapshotStorage(snapshot_path));
    if(snapshots->init() != 0) {
        result.diagnostic = "unable to initialize durable Raft snapshot storage";
        return result;
    }
    braft::SnapshotReader* reader = snapshots->open();
    if(reader == nullptr) {
        if(snapshot_had_entries) {
            result.diagnostic = "snapshot directory contains data but Braft cannot open its latest snapshot";
            return result;
        }
    } else {
        braft::SnapshotMeta meta;
        if(reader->load_meta(&meta) != 0 || meta.last_included_index() <= 0 ||
           meta.last_included_term() <= 0 || meta.peers_size() == 0) {
            snapshots->close(reader);
            result.diagnostic = "latest Raft snapshot has invalid or missing membership metadata";
            return result;
        }
        braft::ConfigurationEntry snapshot_configuration;
        snapshot_configuration.id = braft::LogId(meta.last_included_index(), meta.last_included_term());
        for(int i = 0; i < meta.peers_size(); ++i) {
            braft::PeerId peer;
            if(peer.parse(meta.peers(i)) != 0) {
                snapshots->close(reader);
                result.diagnostic = "latest Raft snapshot contains an invalid peer identity";
                return result;
            }
            snapshot_configuration.conf.add_peer(peer);
        }
        for(int i = 0; i < meta.old_peers_size(); ++i) {
            braft::PeerId peer;
            if(peer.parse(meta.old_peers(i)) != 0) {
                snapshots->close(reader);
                result.diagnostic = "latest Raft snapshot contains an invalid transitional peer identity";
                return result;
            }
            snapshot_configuration.old_conf.add_peer(peer);
        }
        const int64_t snapshot_index = meta.last_included_index();
        const int64_t local_snapshot_term = logs->get_term(snapshot_index);
        configurations.set_snapshot(snapshot_configuration);
        if(local_snapshot_term == 0) {
            configurations.truncate_prefix(snapshot_index + 1);
        } else if(local_snapshot_term != meta.last_included_term()) {
            configurations.truncate_prefix(snapshot_index + 1);
            configurations.truncate_suffix(snapshot_index);
        }
        snapshots->close(reader);
        result.has_snapshot = true;
    }

    const braft::ConfigurationEntry& latest = configurations.last_configuration();
    if(latest.conf.empty() && latest.old_conf.empty()) {
        if(last_log_index == 0 && !snapshot_had_entries) {
            result.status = DurableMembershipStatus::fresh;
            result.diagnostic = "no durable Raft log or snapshot membership exists";
        } else {
            result.diagnostic = "durable Raft data exists without a recoverable membership configuration";
        }
        return result;
    }
    if(!latest.conf.empty() && (latest.conf.contains(local_peer) || latest.old_conf.contains(local_peer))) {
        result.status = DurableMembershipStatus::recovered;
        result.diagnostic = "durable membership includes this node";
        std::vector<braft::PeerId> durable_peers;
        std::vector<braft::PeerId> old_durable_peers;
        latest.conf.list_peers(&durable_peers);
        latest.old_conf.list_peers(&old_durable_peers);
        durable_peers.insert(durable_peers.end(), old_durable_peers.begin(), old_durable_peers.end());
        for(const auto& peer : durable_peers) {
            if(butil::get_endpoint_type(peer.addr) != AF_INET) continue;
            std::string endpoint = butil::endpoint2str(peer.addr).c_str();
            const size_t port_separator = endpoint.rfind(':');
            if(port_separator == std::string::npos) continue;
            const std::string ip = endpoint.substr(0, port_separator);
            in_addr parsed_ip{};
            if(::inet_pton(AF_INET, ip.c_str(), &parsed_ip) == 1 &&
               std::find(result.proxy_allowed_src_ips.begin(), result.proxy_allowed_src_ips.end(), ip) ==
                   result.proxy_allowed_src_ips.end()) {
                result.proxy_allowed_src_ips.push_back(ip);
            }
        }
        return result;
    }
    result.diagnostic = "durable Raft membership does not include this node";
    return result;
}

void ReplicationState::apply_recovered_proxy_allowlist(const DurableMembershipResult& membership) {
    if(membership.status == DurableMembershipStatus::recovered &&
       Config::get_instance().get_proxy_allow_only_peer_src_ips()) {
        Config::get_instance().set_proxy_src_ips(membership.proxy_allowed_src_ips);
    }
}

// An absent configuration intentionally selects the local single-node peer.
ReplicationState::PeerConfigResult ReplicationState::to_nodes_config(
        const butil::EndPoint& peering_endpoint, const int api_port, const std::string& nodes_config) {
    if(nodes_config.empty()) {
        // endpoint2str gives us "<ip>:<peering_port>", we just need to add ":<api_port>"
        const std::string self_config = std::string(butil::endpoint2str(peering_endpoint).c_str()) + ":" +
                                        std::to_string(api_port);
        braft::Configuration parsed;
        if(parsed.parse_from(self_config) != 0) {
            return {PeerConfigStatus::invalid_configuration, {}, "invalid synthesized self peer: " + self_config};
        }
        return {PeerConfigStatus::resolved, self_config, {}};
    }
    return resolve_node_hosts(nodes_config);
}

std::string ReplicationState::hostname2ipstr(const std::string& hostname) {
    return hostname2ipstr(hostname, lookup_hostname_ip);
}

std::string ReplicationState::hostname2ipstr(
        const std::string& hostname,
        const std::function<std::string(const std::string&)>& address_lookup) {
    if(hostname.size() > 64) {
        LOG(ERROR) << "Host name is too long (must be < 64 characters): " << hostname;
        return "";
    }

    // Check if this is already an IPv6 address by looking for []
    if(hostname.find('[') == 0) {
        return hostname;
    }

    std::string resolved_ip = address_lookup(hostname);
    if(resolved_ip.empty()) {
        return {};
    }

    return resolved_ip;
}

ReplicationState::PeerConfigResult ReplicationState::resolve_node_hosts(const string& nodes_config) {
    return resolve_node_hosts(nodes_config, [](const std::string& hostname) {
        return lookup_hostname_ip(hostname);
    });
}

ReplicationState::PeerConfigResult ReplicationState::resolve_node_hosts(
        const string& nodes_config,
        const std::function<std::string(const std::string&)>& hostname_resolver) {
    auto failure = [](PeerConfigStatus status, const std::string& diagnostic) {
        return PeerConfigResult{status, {}, diagnostic};
    };
    if(nodes_config.empty()) {
        return failure(PeerConfigStatus::invalid_configuration, "peer list is empty");
    }

    std::vector<std::string> entries;
    StringUtils::split(nodes_config, entries, ",", true, true);
    std::vector<std::string> syntax_entries;
    syntax_entries.reserve(entries.size());
    const auto valid_hostname_syntax = [](const std::string& hostname) {
        if(hostname.empty()) {
            return false;
        }
        bool at_label_start = true;
        for(size_t i = 0; i < hostname.size(); ++i) {
            const unsigned char c = static_cast<unsigned char>(hostname[i]);
            if(c == '.') {
                // A single trailing dot is the DNS absolute-name form. Empty
                // labels anywhere else are malformed.
                if(at_label_start) {
                    return false;
                }
                at_label_start = true;
                continue;
            }
            const bool alpha_numeric = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                                       (c >= '0' && c <= '9');
            if(!alpha_numeric && c != '-' && c != '_') {
                return false;
            }
            at_label_start = false;
        }
        return !at_label_start || hostname.back() == '.';
    };
    for(size_t i = 0; i < entries.size(); ++i) {
        const std::string& entry = entries[i];
        if(entry.empty()) {
            return failure(PeerConfigStatus::invalid_configuration,
                           "empty peer entry at position " + std::to_string(i + 1));
        }
        std::string syntax_entry = entry;
        if(!entry.empty() && entry.front() != '[') {
            const size_t first_colon = entry.find(':');
            const size_t second_colon = first_colon == std::string::npos ? first_colon : entry.find(':', first_colon + 1);
            if(first_colon != std::string::npos && first_colon > 0) {
                const std::string host = entry.substr(0, first_colon);
                in_addr ipv4{};
                if(inet_pton(AF_INET, host.c_str(), &ipv4) != 1 && !valid_hostname_syntax(host)) {
                    return failure(PeerConfigStatus::invalid_configuration,
                                   "invalid host syntax in peer entry " + std::to_string(i + 1) + ": " + host);
                }
            }
            if(first_colon != std::string::npos &&
               (second_colon == std::string::npos || entry.find(':', second_colon + 1) == std::string::npos)) {
                const std::string host = entry.substr(0, first_colon);
                if(host.empty()) {
                    return failure(PeerConfigStatus::invalid_configuration,
                                   "empty host in peer entry " + std::to_string(i + 1) + ": " + entry);
                }
                in_addr ipv4{};
                if(inet_pton(AF_INET, host.c_str(), &ipv4) != 1) {
                    if(host.size() > 64) {
                        return failure(PeerConfigStatus::invalid_configuration,
                                       "entry " + std::to_string(i + 1) + " host exceeds 64 characters: " + host);
                    }
                    syntax_entry = "127.0.0.1" + entry.substr(first_colon);
                }
            }
        }
        braft::Configuration single_entry_validation;
        if(single_entry_validation.parse_from(syntax_entry) != 0) {
            return failure(PeerConfigStatus::invalid_configuration,
                           "invalid peer entry " + std::to_string(i + 1) + ": " + entry);
        }
        syntax_entries.push_back(std::move(syntax_entry));
    }

    const std::string syntax_config = StringUtils::join(syntax_entries, ",");
    braft::Configuration syntax_validation;
    if(syntax_validation.parse_from(syntax_config) != 0) {
        return failure(PeerConfigStatus::invalid_configuration,
                       "Braft rejected peer list syntax: " + nodes_config);
    }

    std::vector<std::string> resolved_entries;
    resolved_entries.reserve(entries.size());
    for(size_t i = 0; i < entries.size(); ++i) {
        const std::string& entry = entries[i];
        std::string resolved_entry = entry;
        if(!entry.empty() && entry.front() != '[') {
            const size_t first_colon = entry.find(':');
            const size_t second_colon = first_colon == std::string::npos ? first_colon : entry.find(':', first_colon + 1);
            if(first_colon != std::string::npos &&
               (second_colon == std::string::npos || entry.find(':', second_colon + 1) == std::string::npos)) {
                const std::string host = entry.substr(0, first_colon);
                in_addr ipv4{};
                if(inet_pton(AF_INET, host.c_str(), &ipv4) != 1) {
                    const std::string ip = hostname2ipstr(host, hostname_resolver);
                    if(ip.empty()) {
                        return failure(PeerConfigStatus::unresolved_host,
                                       "entry " + std::to_string(i + 1) + " host could not be resolved: " + host);
                    }
                    resolved_entry = ip + entry.substr(first_colon);
                }
            }
        }
        braft::Configuration resolved_entry_validation;
        if(resolved_entry_validation.parse_from(resolved_entry) != 0) {
            return failure(PeerConfigStatus::invalid_configuration,
                           "invalid resolved peer entry " + std::to_string(i + 1) + ": " + resolved_entry);
        }
        resolved_entries.push_back(std::move(resolved_entry));
    }

    const std::string final_config = StringUtils::join(resolved_entries, ",");
    braft::Configuration final_validation;
    if(final_validation.parse_from(final_config) != 0) {
        return failure(PeerConfigStatus::invalid_configuration,
                       "Braft rejected resolved peer list: " + final_config);
    }
    return {PeerConfigStatus::resolved, final_config, {}};
}

Option<bool> ReplicationState::handle_gzip(const std::shared_ptr<http_req>& request) {
    if (!request->zstream_initialized) {
        request->zs.zalloc = Z_NULL;
        request->zs.zfree = Z_NULL;
        request->zs.opaque = Z_NULL;
        request->zs.avail_in = 0;
        request->zs.next_in = Z_NULL;

        if (inflateInit2(&request->zs, 16 + MAX_WBITS) != Z_OK) {
            return Option<bool>(400, "inflateInit failed while decompressing");
        }

        request->zstream_initialized = true;
    }

    std::string outbuffer;
    outbuffer.resize(10 * request->body.size());

    request->zs.next_in = (Bytef *) request->body.c_str();
    request->zs.avail_in = request->body.size();
    std::size_t size_uncompressed = 0;
    int ret = 0;
    do {
        request->zs.avail_out = static_cast<unsigned int>(outbuffer.size());
        request->zs.next_out = reinterpret_cast<Bytef *>(&outbuffer[0] + size_uncompressed);
        ret = inflate(&request->zs, Z_FINISH);
        if (ret != Z_STREAM_END && ret != Z_OK && ret != Z_BUF_ERROR) {
            std::string error_msg = request->zs.msg;
            inflateEnd(&request->zs);
            return Option<bool>(400, error_msg);
        }

        size_uncompressed += (outbuffer.size() - request->zs.avail_out);
    } while (request->zs.avail_out == 0);

    if (ret == Z_STREAM_END) {
        request->zstream_initialized = false;
        inflateEnd(&request->zs);
    }

    outbuffer.resize(size_uncompressed);

    request->body = outbuffer;
    request->chunk_len = outbuffer.size();

    return Option<bool>(true);
}

void ReplicationState::write(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    if(shutting_down) {
        //LOG(INFO) << "write(), force shutdown";
        response->set_503("Shutting down.");
        response->final = true;
        response->is_alive = false;
        request->notify();
        return ;
    }

    // reject write if disk space is running out
    auto resource_check = cached_resource_stat_t::get_instance().has_enough_resources(raft_dir_path,
                                  config->get_disk_used_max_percentage(), config->get_memory_used_max_percentage());

    if (resource_check != cached_resource_stat_t::OK && request->do_resource_check()) {
        response->set_422("Rejecting write: running out of resource type: " +
                          std::string(magic_enum::enum_name(resource_check)));
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    if(config->get_skip_writes() && request->path_without_query != "/config") {
        response->set_422("Skipping writes.");
        response->final = true;
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    route_path* rpath = nullptr;
    bool route_found = server->get_route(request->route_hash, &rpath);

    if(route_found && rpath->handler == patch_update_collection) {
        if(get_alter_in_progress(request->params["collection"])) {
            // This is checked only during live writes from a http request: we do this because we want to only
            // throttle concurrent successive alter requests, but want to allow an alter that happens after another
            // finishes via raft log replay.
            response->set_422("Another collection update operation is in progress.");
            response->final = true;
            auto req_res = new async_req_res_t(request, response, true);
            return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        }
    }

    std::shared_lock lock(node_mutex);

    if(!node) {
        return ;
    }

    if (!node->is_leader()) {
        return write_to_leader(request, response);
    }

    //check if it's first gzip chunk or is gzip stream initialized
    if(((request->body.size() > 2) &&
        (31 == (int)request->body[0] && -117 == (int)request->body[1])) || request->zstream_initialized) {
        auto res = handle_gzip(request);

        if(!res.ok()) {
            response->set_422(res.error());
            response->final = true;
            auto req_res = new async_req_res_t(request, response, true);
            return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        }
    }

    // Serialize request to replicated WAL so that all the nodes in the group receive it as well.
    // NOTE: actual write must be done only on the `on_apply` method to maintain consistency.

    butil::IOBufBuilder bufBuilder;
    bufBuilder << request->to_json();

    //LOG(INFO) << "write() pre request ref count " << request.use_count();

    // Apply this log as a braft::Task

    braft::Task task;
    task.data = &bufBuilder.buf();
    // This callback would be invoked when the task actually executes or fails
    task.done = new ReplicationClosure(request, response);

    //LOG(INFO) << "write() post request ref count " << request.use_count();

    // To avoid ABA problem
    task.expected_term = leader_term.load(butil::memory_order_relaxed);

    //LOG(INFO) << ":::" << "body size before apply: " << request->body.size();

    // Now the task is applied to the group
    node->apply(task);

    pending_writes++;
}

void ReplicationState::write_to_leader(const std::shared_ptr<http_req>& request, const std::shared_ptr<http_res>& response) {
    // no lock on `node` needed as caller uses the lock
    if(!node || node->leader_id().is_empty()) {
        // Handle no leader scenario
        LOG(ERROR) << "Rejecting write: could not find a leader.";

        if(response->proxied_stream) {
            // streaming in progress: ensure graceful termination (cannot start response again)
            LOG(ERROR) << "Terminating streaming request gracefully.";
            response->is_alive = false;
            request->notify();
            return ;
        }

        response->set_500("Could not find a leader.");
        auto req_res = new async_req_res_t(request, response, true);
        return message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
    }

    if (response->proxied_stream) {
        // indicates async request body of in-flight request
        //LOG(INFO) << "Inflight proxied request, returning control to caller, body_size=" << request->body.size();
        request->notify();
        return ;
    }

    const braft::PeerId& leader_addr = node->leader_id();
    //LOG(INFO) << "Redirecting write to leader at: " << leader_addr;

    h2o_custom_generator_t* custom_generator = reinterpret_cast<h2o_custom_generator_t *>(response->generator.load());
    HttpServer* server = custom_generator->h2o_handler->http_server;

    auto raw_req = request->_req;
    const std::string& path = std::string(raw_req->path.base, raw_req->path.len);
    const std::string& scheme = std::string(raw_req->scheme->name.base, raw_req->scheme->name.len);
    const std::string url = get_node_url_path(leader_addr, path, scheme);

    thread_pool->enqueue([request, response, server, path, url, this]() {
        pending_writes++;

        std::map<std::string, std::string> res_headers;

        if(request->http_method == "POST") {
            std::vector<std::string> path_parts;
            StringUtils::split(path, path_parts, "/");

            if(path_parts.back().rfind("import", 0) == 0) {
                // imports are handled asynchronously
                response->proxied_stream = true;
                long status = HttpClient::post_response_async(url, request, response, server, true);

                if(status == 500) {
                    response->content_type_header = res_headers["content-type"];
                    response->set_500("");
                } else {
                    return ;
                }
            } else {
                std::string api_res;
                long status = HttpClient::post_response(url, request->body, api_res, res_headers, {}, 0, true);
                response->content_type_header = res_headers["content-type"];
                response->set_body(status, api_res);
            }
        } else if(request->http_method == "PUT") {
            std::string api_res;
            long status = HttpClient::put_response(url, request->body, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "DELETE") {
            std::string api_res;
            // timeout: 0 since delete can take a long time
            long status = HttpClient::delete_response(url, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else if(request->http_method == "PATCH") {
            std::string api_res;
            long status = HttpClient::patch_response(url, request->body, api_res, res_headers, 0, true);
            response->content_type_header = res_headers["content-type"];
            response->set_body(status, api_res);
        } else {
            const std::string& err = "Forwarding for http method not implemented: " + request->http_method;
            LOG(ERROR) << err;
            response->set_500(err);
        }

        auto req_res = new async_req_res_t(request, response, true);
        message_dispatcher->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        pending_writes--;
    });
}

std::string ReplicationState::get_node_url_path(const braft::PeerId& peer_id, const std::string& path,
                                                const std::string& protocol) const {
    const std::string endpoint_str = butil::endpoint2str(peer_id.addr).c_str();
    const size_t last_colon = endpoint_str.rfind(':');
    if (last_colon == std::string::npos) {
        LOG(ERROR) << "Invalid endpoint format: " << endpoint_str;
        return "";
    }

    // For IPv6, the IP part may contain colons and be wrapped in []
    const std::string ip_part = endpoint_str.substr(0, last_colon);

    std::string url = protocol + "://";
    url += ip_part;  // IP part (possibly with [] for IPv6)
    url += ":";
    url += std::to_string(peer_id.idx);

    // Add path ensuring there's exactly one / between URL parts
    if(!path.empty()) {
        if(path[0] == '/') {
            url += path;
        } else {
            url += "/" + path;
        }
    }

    return url;
}

void ReplicationState::on_apply(braft::Iterator& iter) {
    //LOG(INFO) << "ReplicationState::on_apply";
    // NOTE: this is executed on a different thread and runs concurrent to http thread
    // A batch of tasks are committed, which must be processed through
    // |iter|
    for (; iter.valid(); iter.next()) {
        // Guard invokes replication_arg->done->Run() asynchronously to avoid the callback blocking the main thread
        braft::AsyncClosureGuard closure_guard(iter.done());

        //LOG(INFO) << "Apply entry";

        const std::shared_ptr<http_req>& request_generated = iter.done() ?
                         dynamic_cast<ReplicationClosure*>(iter.done())->get_request() : std::make_shared<http_req>();

        //LOG(INFO) << "Post assignment " << request_generated.get() << ", use count: " << request_generated.use_count();

        const std::shared_ptr<http_res>& response_generated = iter.done() ?
                dynamic_cast<ReplicationClosure*>(iter.done())->get_response() : std::make_shared<http_res>(nullptr);

        if(!iter.done()) {
            // indicates log serialized request
            request_generated->load_from_json(iter.data().to_string());
        }

        request_generated->log_index = iter.index();

        // To avoid blocking the serial Raft write thread persist the log entry in local storage.
        // Actual operations will be done in collection-sharded batch indexing threads.

        batched_indexer->enqueue(request_generated, response_generated);

        if(iter.done()) {
            pending_writes--;
            //LOG(INFO) << "pending_writes: " << pending_writes;
        }
    }
}

void ReplicationState::read(const std::shared_ptr<http_res>& response) {
    // NOT USED:
    // For consistency, reads to followers could be rejected.
    // Currently, we don't do implement reads via raft.
}

void* ReplicationState::save_snapshot(void* arg) {
    LOG(INFO) << "save_snapshot called";

    SnapshotArg* sa = static_cast<SnapshotArg*>(arg);
    std::unique_ptr<SnapshotArg> arg_guard(sa);

    // add the db snapshot files to writer state
    butil::FileEnumerator dir_enum(butil::FilePath(sa->db_snapshot_path), false, butil::FileEnumerator::FILES);

    for (butil::FilePath file = dir_enum.Next(); !file.empty(); file = dir_enum.Next()) {
        std::string file_name = std::string(db_snapshot_name) + "/" + file.BaseName().value();
        if (sa->writer->add_file(file_name) != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to writer.");
            sa->replication_state->snapshot_in_progress = false;
            return nullptr;
        }
    }

    if(!sa->analytics_db_snapshot_path.empty()) {
        //add analytics db snapshot files to writer state
        butil::FileEnumerator analytics_dir_enum(butil::FilePath(sa->analytics_db_snapshot_path), false,
                                                 butil::FileEnumerator::FILES);
        for (butil::FilePath file = analytics_dir_enum.Next(); !file.empty(); file = analytics_dir_enum.Next()) {
            auto file_name = std::string(analytics_db_snapshot_name) + "/" + file.BaseName().value();
            if (sa->writer->add_file(file_name) != 0) {
                sa->done->status().set_error(EIO, "Fail to add analytics file to writer.");
                sa->replication_state->snapshot_in_progress = false;
                return nullptr;
            }
        }
    }

    sa->done->Run();

    // NOTE: *must* do a dummy write here since snapshots cannot be triggered if no write has happened since the
    // last snapshot. By doing a dummy write right after a snapshot, we ensure that this can never be the case.
    sa->replication_state->do_dummy_write();

    LOG(INFO) << "save_snapshot done";

    return nullptr;
}

// this method is serial to on_apply so guarantees a snapshot view of the state machine
void ReplicationState::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    LOG(INFO) << "on_snapshot_save";

    snapshot_in_progress = true;
    std::string db_snapshot_path = writer->get_path() + "/" + db_snapshot_name;
    std::string analytics_db_snapshot_path = writer->get_path() + "/" + analytics_db_snapshot_name;

    {
        // grab batch indexer lock so that we can take a clean snapshot
        std::shared_mutex& pause_mutex = batched_indexer->get_pause_mutex();
        std::unique_lock lk(pause_mutex);

        nlohmann::json batch_index_state;
        batched_indexer->serialize_state(batch_index_state);
        store->insert(BATCHED_INDEXER_STATE_KEY, batch_index_state.dump());

        // we will delete all the skip indices in meta store and flush that DB
        // this will block writes, but should be pretty fast
        batched_indexer->clear_skip_indices();

        rocksdb::Checkpoint* checkpoint = nullptr;
        rocksdb::Status status = store->create_check_point(&checkpoint, db_snapshot_path);
        std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);

        if(!status.ok()) {
            LOG(ERROR) << "Failure during checkpoint creation, msg:" << status.ToString();
            done->status().set_error(EIO, "Checkpoint creation failure.");
        }

        if(analytics_store) {
            // to ensure that in-memory table is sent to disk (we don't use WAL)
            analytics_store->flush();

            rocksdb::Checkpoint* checkpoint2 = nullptr;
            status = analytics_store->create_check_point(&checkpoint2, analytics_db_snapshot_path);
            std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint2);

            if(!status.ok()) {
                LOG(ERROR) << "AnalyticsStore : Failure during checkpoint creation, msg:" << status.ToString();
                done->status().set_error(EIO, "AnalyticsStore : Checkpoint creation failure.");
            }
        }
    }

    SnapshotArg* arg = new SnapshotArg;
    arg->replication_state = this;
    arg->writer = writer;
    arg->state_dir_path = raft_dir_path;
    arg->db_snapshot_path = db_snapshot_path;
    arg->done = done;

    if(analytics_store) {
        arg->analytics_db_snapshot_path = analytics_db_snapshot_path;
    }

    if(!ext_snapshot_path.empty()) {
        arg->ext_snapshot_path = ext_snapshot_path;
    }

    // Start a new bthread to avoid blocking StateMachine for slower operations that don't need a blocking view
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

int ReplicationState::init_db(const bool batched_indexer_workers_paused) {
    LOG(INFO) << "Loading collections from disk...";

    Option<bool> init_op = CollectionManager::get_instance().load(
        num_collections_parallel_load, num_documents_parallel_load
    );

    if(init_op.ok()) {
        LOG(INFO) << "Finished loading collections from disk.";
    } else {
        LOG(ERROR)<< "Typesense failed to start. " << "Could not load collections from disk: " << init_op.error();
        return 1;
    }

    // important to init conversation models only after all collections have been loaded
    auto conversation_models_init = ConversationModelManager::init(store);
    if(!conversation_models_init.ok()) {
        LOG(INFO) << "Failed to initialize conversation model manager: " << conversation_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << conversation_models_init.get() << " conversation model(s).";
    }

    if(batched_indexer != nullptr) {
        LOG(INFO) << "Initializing batched indexer from snapshot state...";
        std::string batched_indexer_state_str;
        StoreStatus s = store->get(BATCHED_INDEXER_STATE_KEY, batched_indexer_state_str);
        const int restore_status =
            restore_batched_indexer_state(s, batched_indexer_state_str, batched_indexer_workers_paused);
        if(restore_status != 0) {
            return restore_status;
        }
    }

    auto personalization_models_init = PersonalizationModelManager::init(store);
    if(!personalization_models_init.ok()) {
        LOG(INFO) << "Failed to initialize personalization model manager: " << personalization_models_init.error();
    } else {
        LOG(INFO) << "Loaded " << personalization_models_init.get() << " personalization model(s).";
    }

    return 0;
}

int ReplicationState::restore_batched_indexer_state(const StoreStatus status, const std::string& state,
                                                    const bool batched_indexer_workers_paused) {
    if(batched_indexer == nullptr) {
        return 0;
    }

    const auto restore = [&]() {
        if(status == StoreStatus::ERROR) {
            LOG(ERROR) << "Failed to read batched indexer state from the incoming store.";
            batched_indexer->clear_state_unlocked();
            return 1;
        }

        if(status == StoreStatus::NOT_FOUND) {
            // The incoming store is authoritative. A snapshot from an older version might not contain batched
            // indexer state at all, in which case no state from the replaced store may survive.
            batched_indexer->clear_state_unlocked();
            return 0;
        }

        try {
            auto batch_indexer_state = nlohmann::json::parse(state, nullptr, false);
            if(batch_indexer_state.is_discarded() || !batch_indexer_state.is_object() ||
               !batch_indexer_state.contains("req_res_map")) {
                LOG(ERROR) << "Invalid batched indexer state in the incoming store.";
                batched_indexer->clear_state_unlocked();
                return 1;
            }

            // Empty request maps were serialized as JSON null before they were explicitly initialized as objects.
            // Treat that legacy representation as an empty map so existing snapshots remain restorable.
            if(batch_indexer_state["req_res_map"].is_null()) {
                batch_indexer_state["req_res_map"] = nlohmann::json::object();
            } else if(!batch_indexer_state["req_res_map"].is_object()) {
                LOG(ERROR) << "Invalid batched indexer state in the incoming store.";
                batched_indexer->clear_state_unlocked();
                return 1;
            }

            batched_indexer->load_state_unlocked(batch_indexer_state);
            return 0;
        } catch(const std::exception& e) {
            LOG(ERROR) << "Failed to restore batched indexer state: " << e.what();
            // A nested type error can be detected after restoration has begun. Do not expose that partial state.
            batched_indexer->clear_state_unlocked();
            return 1;
        }
    };

    if(batched_indexer_workers_paused) {
        return restore();
    }

    std::unique_lock lifecycle_lock(batched_indexer->lifecycle_mutex);
    return restore();
}

int ReplicationState::fail_snapshot_load_unlocked(const int status) {
    snapshot_load_blocks_readiness = true;
    read_caught_up = false;
    write_caught_up = false;
    if(batched_indexer != nullptr) {
        batched_indexer->clear_state_unlocked();
    }
    return status == 0 ? 1 : status;
}

int ReplicationState::on_snapshot_load(braft::SnapshotReader* reader) {
    std::shared_lock lock(node_mutex);
    CHECK(!node || !node->is_leader()) << "Leader is not supposed to load snapshot";
    lock.unlock();

    LOG(INFO) << "on_snapshot_load";

    // `refresh_catchup_status` runs independently of braft snapshot callbacks. Serialize it with installation so it
    // cannot republish readiness from a stale raft status while the Store and in-memory state are being replaced.
    // A failed installation deliberately leaves the gate closed until a later snapshot succeeds.
    std::unique_lock snapshot_load_lock(snapshot_load_mutex);
    snapshot_load_blocks_readiness = true;

    // ensures that reads and writes are rejected, as `store->reload()` unique locks the DB handle
    read_caught_up = false;
    write_caught_up = false;

    // Batch workers can retain request-map references and RocksDB iterators after dequeueing. Stop enqueue, worker,
    // and GC activity before replacing the store, and keep it stopped until the in-memory indexer state has also
    // been replaced.
    std::unique_lock<std::shared_mutex> batched_indexer_lifecycle_lock;
    if(batched_indexer != nullptr) {
        batched_indexer_lifecycle_lock = std::unique_lock<std::shared_mutex>(batched_indexer->lifecycle_mutex);
    }

    // Load snapshot from leader, replacing the running StateMachine
    std::string analytics_snapshot_path = reader->get_path();
    analytics_snapshot_path.append(std::string("/") + analytics_db_snapshot_name);

    if(analytics_store && directory_exists(analytics_snapshot_path)) {
        // analytics db snapshot could be missing (older version or disabled earlier)
        int reload_store = analytics_store->reload(true, analytics_snapshot_path,
                                                   Config::get_instance().get_analytics_db_ttl());
        if (reload_store != 0) {
            LOG(ERROR) << "Failed to reload analytics db snapshot.";
            return fail_snapshot_load_unlocked(reload_store);
        }
    }

    std::string db_snapshot_path = reader->get_path();
    db_snapshot_path.append(std::string("/") + db_snapshot_name);

    int reload_store = store->reload(true, db_snapshot_path);
    if(reload_store != 0) {
        return fail_snapshot_load_unlocked(reload_store);
    }

    const int init_db_status = init_db(true);
    if(init_db_status != 0) {
        return fail_snapshot_load_unlocked(init_db_status);
    }

    read_caught_up = false;
    write_caught_up = false;
    snapshot_load_blocks_readiness = false;
    return 0;
}

void ReplicationState::refresh_nodes(const PeerConfigResult& peer_config, const size_t raft_counter,
                                     const std::atomic<bool>& reset_peers_on_error) {
    if(!peer_config.ok()) {
        LOG(WARNING) << "Skipping peer refresh: " << peer_config.diagnostic;
        return;
    }

    braft::Configuration new_conf;
    if(!parse_peer_configuration(peer_config.configuration, new_conf)) {
        LOG(WARNING) << "Skipping peer refresh with invalid or incomplete configuration: "
                     << peer_config.configuration;
        return;
    }

    std::shared_lock lock(node_mutex);

    if(!node) {
        LOG(WARNING) << "Node state is not initialized: unable to refresh nodes.";
        return ;
    }

    braft::NodeStatus nodeStatus;
    node->get_status(&nodeStatus);

    LOG(INFO) << "Term: " << nodeStatus.term
              << ", last_index: " << nodeStatus.last_index
              << ", committed: " << nodeStatus.committed_index
              << ", known_applied: " << nodeStatus.known_applied_index
              << ", applying: " << nodeStatus.applying_index
              << ", pending_writes: " << pending_writes
              << ", queued_writes: " << batched_indexer->get_queued_writes()
              << ", inflight_searches: " << HouseKeeper::get_instance().get_num_inflight_queries()
              << ", local_sequence: " << store->get_latest_seq_number();

    if(node->is_leader()) {
        RefreshNodesClosure* refresh_nodes_done = new RefreshNodesClosure;
        node->change_peers(new_conf, refresh_nodes_done);
    } else {
        if(node->leader_id().is_empty()) {
            // When node is not a leader, does not have a leader and is also a single-node cluster,
            // we forcefully reset its peers.
            // NOTE: `reset_peers()` is not a safe call to make as we give up on consistency and consensus guarantees.
            // We are doing this solely to handle single node cluster whose IP changes.
            // Examples: Docker container IP change, local DHCP leased IP change etc.

            std::vector<braft::PeerId> latest_nodes;
            new_conf.list_peers(&latest_nodes);

            if(latest_nodes.size() == 1 || (raft_counter > 0 && reset_peers_on_error)) {
                LOG(WARNING) << "Node with no leader. Resetting peers of size: " << latest_nodes.size();
                node->reset_peers(new_conf);
            } else {
                LOG(WARNING) << "Multi-node with no leader: refusing to reset peers.";
            }

            return ;
        }
    }
}

void ReplicationState::refresh_catchup_status(bool log_msg) {
    std::shared_lock snapshot_load_lock(snapshot_load_mutex);
    if(snapshot_load_blocks_readiness) {
        read_caught_up = write_caught_up = false;
        return;
    }

    std::shared_lock lock(node_mutex);
    if(node == nullptr ) {
        read_caught_up = write_caught_up = false;
        return ;
    }

    bool is_leader = node->is_leader();
    bool leader_or_follower = (is_leader || !node->leader_id().is_empty());
    if(!leader_or_follower) {
        read_caught_up = write_caught_up = false;
        return ;
    }

    braft::NodeStatus n_status;
    node->get_status(&n_status);
    lock.unlock();

    // `known_applied_index` guaranteed to be atleast 1 if raft log is available (after snapshot loading etc.)
    if(n_status.known_applied_index == 0) {
        LOG_IF(ERROR, log_msg) << "Node not ready yet (known_applied_index is 0).";
        read_caught_up = write_caught_up = false;
        return ;
    }

    // work around for: https://github.com/baidu/braft/issues/277#issuecomment-823080171
    int64_t current_index = (n_status.applying_index == 0) ? n_status.known_applied_index : n_status.applying_index;
    int64_t apply_lag = n_status.last_index - current_index;

    // in addition to raft level lag, we should also account for internal batched write queue
    int64_t num_queued_writes = batched_indexer->get_queued_writes();

    //LOG(INFO) << "last_index: " << n_status.applying_index << ", known_applied_index: " << n_status.known_applied_index;
    //LOG(INFO) << "apply_lag: " << apply_lag;

    int healthy_read_lag = config->get_healthy_read_lag();
    int healthy_write_lag = config->get_healthy_write_lag();

    if (apply_lag > healthy_read_lag) {
        LOG_IF(ERROR, log_msg) << apply_lag << " lagging entries > healthy read lag of " << healthy_read_lag;
        this->read_caught_up = false;
    } else {
        if(num_queued_writes > healthy_read_lag) {
            LOG_IF(ERROR, log_msg) << num_queued_writes << " queued writes > healthy read lag of " << healthy_read_lag;
            this->read_caught_up = false;
        } else {
            this->read_caught_up = true;
        }
    }

    if (apply_lag > healthy_write_lag) {
        LOG_IF(ERROR, log_msg) << apply_lag << " lagging entries > healthy write lag of " << healthy_write_lag;
        this->write_caught_up = false;
    } else {
        if(num_queued_writes > healthy_write_lag) {
            LOG_IF(ERROR, log_msg) << num_queued_writes << " queued writes > healthy write lag of " << healthy_write_lag;
            this->write_caught_up = false;
        } else {
            this->write_caught_up = true;
        }
    }

    if(is_leader || !this->read_caught_up) {
        // no need to re-check status with leader
        return ;
    }

    lock.lock();

    if(node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not get leader status, as node does not have a leader!";
        return ;
    }

    const braft::PeerId& leader_addr = node->leader_id();
    lock.unlock();

    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = get_node_url_path(leader_addr, "/status", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::get_response(url, api_res, res_headers, {}, 5*1000, true);
    if(status_code == 200) {
        // compare leader's applied log with local applied to see if we are lagging
        nlohmann::json leader_status = nlohmann::json::parse(api_res);
        if(leader_status.contains("committed_index")) {
            int64_t leader_committed_index = leader_status["committed_index"].get<int64_t>();
            if(leader_committed_index <= n_status.committed_index) {
                // this can happen due to network latency in making the /status call
                // we will refrain from changing current status
                return ;
            }
            this->read_caught_up = ((leader_committed_index - n_status.committed_index) < healthy_read_lag);
        } else {
            // we will refrain from changing current status
            LOG(ERROR) << "Error, `committed_index` key not found in /status response from leader.";
        }
    } else {
        // we will again refrain from changing current status
        LOG(ERROR) << "Error, /status end-point returned bad status code " << status_code;
    }
}

ReplicationState::ReplicationState(HttpServer* server, BatchedIndexer* batched_indexer,
                                   Store *store, Store* analytics_store, ThreadPool* thread_pool,
                                   http_message_dispatcher *message_dispatcher,
                                   bool api_uses_ssl, const Config* config,
                                   size_t num_collections_parallel_load, size_t num_documents_parallel_load):
        node(nullptr), leader_term(-1), server(server), batched_indexer(batched_indexer),
        store(store), analytics_store(analytics_store),
        thread_pool(thread_pool), message_dispatcher(message_dispatcher), api_uses_ssl(api_uses_ssl),
        config(config),
        num_collections_parallel_load(num_collections_parallel_load),
        num_documents_parallel_load(num_documents_parallel_load),
        read_caught_up(false), write_caught_up(false),
        snapshot_load_blocks_readiness(false),
        ready(false), shutting_down(false), pending_writes(0), snapshot_in_progress(false),
        last_snapshot_ts(std::time(nullptr)), snapshot_interval_s(config->get_snapshot_interval_seconds()) {

}

bool ReplicationState::is_alive() const {
    // for general health check we will only care about the `read_caught_up` threshold
    return read_caught_up;
}

uint64_t ReplicationState::node_state() const {
    std::shared_lock lock(node_mutex);

    if(node == nullptr) {
        return 0;
    }

    braft::NodeStatus node_status;
    node->get_status(&node_status);

    return node_status.state;
}

void ReplicationState::do_snapshot(const std::string& snapshot_path, const std::shared_ptr<http_req>& req,
                                   const std::shared_ptr<http_res>& res) {
    if(node == nullptr) {
        res->set_500("Could not trigger a snapshot, as node is not initialized.");
        auto req_res = new async_req_res_t(req, res, true);
        get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return ;
    }

    if(snapshot_in_progress) {
        res->set_409("Another snapshot is in progress.");
        auto req_res = new async_req_res_t(req, res, true);
        get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);
        return ;
    }

    LOG(INFO) << "Triggering an on demand snapshot"
              << (!snapshot_path.empty() ? " with external snapshot path..." : "...");

    thread_pool->enqueue([&snapshot_path, req, res, this]() {
        OnDemandSnapshotClosure* snapshot_closure = new OnDemandSnapshotClosure(this, req, res, snapshot_path,
                                                                                raft_dir_path);
        ext_snapshot_path = snapshot_path;
        std::shared_lock lock(this->node_mutex);
        node->snapshot(snapshot_closure);
    });
}

void ReplicationState::set_ext_snapshot_path(const std::string& snapshot_path) {
    this->ext_snapshot_path = snapshot_path;
}

void ReplicationState::set_snapshot_in_progress(const bool snapshot_in_progress) {
    this->snapshot_in_progress = snapshot_in_progress;
}

void ReplicationState::do_dummy_write() {
    std::shared_lock lock(node_mutex);

    if(!node || node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not do a dummy write, as node does not have a leader";
        return ;
    }

    const std::string & leader_addr = node->leader_id().to_string();
    lock.unlock();

    const std::string protocol = api_uses_ssl ? "https" : "http";
    std::string url = get_node_url_path(leader_addr, "/health", protocol);

    std::string api_res;
    std::map<std::string, std::string> res_headers;
    long status_code = HttpClient::post_response(url, "", api_res, res_headers, {}, 4000, true);

    LOG(INFO) << "Dummy write to " << url << ", status = " << status_code << ", response = " << api_res;
}

bool ReplicationState::trigger_vote() {
    std::shared_lock lock(node_mutex);

    if(node) {
        auto status = node->vote(election_timeout_interval_ms);
        LOG(INFO) << "Triggered vote. Ok? " << status.ok() << ", status: " << status;
        return status.ok();
    }

    return false;
}

bool ReplicationState::reset_peers() {
    std::shared_lock lock(node_mutex);

    if(node) {
        const Option<std::string> & refreshed_nodes_op = Config::fetch_nodes_config(config->get_nodes());
        if(!refreshed_nodes_op.ok()) {
            LOG(WARNING) << "Error while fetching peer configuration: " << refreshed_nodes_op.error();
            return false;
        }

        const auto peer_config_result = ReplicationState::to_nodes_config(
            peering_endpoint, Config::get_instance().get_api_port(), refreshed_nodes_op.get());
        if(!peer_config_result.ok()) {
            LOG(WARNING) << "Unable to reset peers from configuration: " << peer_config_result.diagnostic;
            return false;
        }
        const std::string& nodes_config = peer_config_result.configuration;

        braft::Configuration peer_config;
        if(!parse_peer_configuration(nodes_config, peer_config)) {
            LOG(WARNING) << "Unable to reset peers from invalid configuration: " << nodes_config;
            return false;
        }

        if(Config::get_instance().get_proxy_allow_only_peer_src_ips()) {
            Config::get_instance().update_proxy_src_ips(nodes_config);
        }

        std::vector<braft::PeerId> peers;
        peer_config.list_peers(&peers);

        auto status = node->reset_peers(peer_config);
        LOG(INFO) << "Reset peers. Ok? " << status.ok() << ", status: " << status;
        LOG(INFO) << "New peer config is: " << peer_config;
        return status.ok();
    }

    return false;
}

http_message_dispatcher* ReplicationState::get_message_dispatcher() const {
    return message_dispatcher;
}

Store* ReplicationState::get_store() {
    return store;
}

void ReplicationState::shutdown() {
    LOG(INFO) << "Set shutting_down = true";
    shutting_down = true;

    LOG(INFO) << "Waiting for in-flight writes to finish...";
    while(true) {
        const auto pending = pending_writes.load();
        const auto queued = batched_indexer->get_queued_writes();
        if(pending == 0 && queued == 0) {
            break;
        }

        LOG(INFO) << "pending_writes: " << pending << ", queued_writes: " << queued;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    LOG(INFO) << "Replication state shutdown, store sequence: " << store->get_latest_seq_number();
    std::unique_lock lock(node_mutex);

    if (node) {
        LOG(INFO) << "node->shutdown";
        node->shutdown(nullptr);

        // Blocking this thread until the node is eventually down.
        LOG(INFO) << "node->join";
        node->join();
        delete node;
        node = nullptr;
    }
}

void ReplicationState::persist_applying_index() {
    std::shared_lock lock(node_mutex);

    if(node == nullptr) {
        return ;
    }

    lock.unlock();

    batched_indexer->persist_applying_index();
}

int64_t ReplicationState::get_num_queued_writes() {
    return batched_indexer->get_queued_writes();
}

bool ReplicationState::is_leader() {
    std::shared_lock lock(node_mutex);

    if(!node) {
        return false;
    }

    return node->is_leader();
}

nlohmann::json ReplicationState::get_status() {
    nlohmann::json status;

    std::shared_lock lock(node_mutex);
    if(!node) {
        // `node` is not yet initialized (probably loading snapshot)
        status["state"] = "NOT_READY";
        status["committed_index"] = 0;
        status["queued_writes"] = 0;
        return status;
    }

    braft::NodeStatus node_status;
    node->get_status(&node_status);
    lock.unlock();

    status["state"] = braft::state2str(node_status.state);
    status["last_index"] = node_status.last_index;
    status["committed_index"] = node_status.committed_index;
    status["known_applied_index"] = node_status.known_applied_index;
    status["applying_index"] = node_status.applying_index;
    status["queued_writes"] = batched_indexer->get_queued_writes();

    return status;
}

void ReplicationState::do_snapshot(const PeerConfigResult& peer_config_result) {
    if(!peer_config_result.ok()) {
        LOG(WARNING) << "Skipping snapshot peer-health check: " << peer_config_result.diagnostic;
        return;
    }

    braft::Configuration peer_config;
    if(!parse_peer_configuration(peer_config_result.configuration, peer_config)) {
        LOG(WARNING) << "Skipping snapshot peer-health check with invalid or incomplete configuration: "
                     << peer_config_result.configuration;
        return;
    }

    auto current_ts = std::time(nullptr);
    if(current_ts - last_snapshot_ts < snapshot_interval_s) {
        //LOG(INFO) << "Skipping snapshot: not enough time has elapsed.";
        return;
    }

    LOG(INFO) << "Snapshot timer is active, current_ts: " << current_ts << ", last_snapshot_ts: " << last_snapshot_ts;

    if(is_leader()) {
        // run the snapshot only if there are no other recovering followers
        std::vector<braft::PeerId> peers;
        peer_config.list_peers(&peers);

        std::shared_lock lock(node_mutex);
        std::string my_addr = node->node_id().peer_id.to_string();
        lock.unlock();

        //LOG(INFO) << "my_addr: " << my_addr;
        bool all_peers_healthy = true;

        // iterate peers and check health status
        for(const auto& peer: peers) {
            const std::string& peer_addr = peer.to_string();
            //LOG(INFO) << "do_snapshot, peer_addr: " << peer_addr;

            if(my_addr == peer_addr) {
                // skip self
                //LOG(INFO) << "do_snapshot: skipping self, peer_addr: " << peer_addr;
                continue;
            }

            const std::string protocol = api_uses_ssl ? "https" : "http";
            std::string url = get_node_url_path(peer, "/health", protocol);
            std::string api_res;
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::get_response(url, api_res, res_headers, {}, 5*1000, true);
            bool peer_healthy = (status_code == 200);

            //LOG(INFO) << "do_snapshot, status_code: " << status_code;

            if(!peer_healthy) {
                LOG(WARNING) << "Peer " << peer_addr << " reported unhealthy during snapshot pre-check.";
            }

            all_peers_healthy = all_peers_healthy && peer_healthy;
        }

        if(!all_peers_healthy) {
            LOG(WARNING) << "Unable to trigger snapshot as one or more of the peers reported unhealthy.";
            return ;
        }
    }

    TimedSnapshotClosure* snapshot_closure = new TimedSnapshotClosure(this);
    std::shared_lock lock(node_mutex);
    node->snapshot(snapshot_closure);
    last_snapshot_ts = current_ts;
}

std::string ReplicationState::get_leader_url() const {
    std::shared_lock lock(node_mutex);

    if(!node) {
        LOG(ERROR) << "Could not get leader url as node is not initialized!";
        return "";
    }

    if(node->leader_id().is_empty()) {
        LOG(ERROR) << "Could not get leader url, as node does not have a leader!";
        return "";
    }

    const braft::PeerId& leader_addr = node->leader_id();
    lock.unlock();

    const std::string protocol = api_uses_ssl ? "https" : "http";
    return get_node_url_path(leader_addr, "/", protocol);
}

void ReplicationState::decr_pending_writes() {
    pending_writes--;
}

void TimedSnapshotClosure::Run() {
    // Auto delete this after Done()
    std::unique_ptr<TimedSnapshotClosure> self_guard(this);

    if(status().ok()) {
        LOG(INFO) << "Timed snapshot succeeded!";
    } else {
        LOG(ERROR) << "Timed snapshot failed, error: " << status().error_str() << ", code: " << status().error_code();
    }

    replication_state->set_snapshot_in_progress(false);
}

void OnDemandSnapshotClosure::Run() {
    // Auto delete this after Done()
    std::unique_ptr<OnDemandSnapshotClosure> self_guard(this);

    bool ext_snapshot_succeeded = false;

    // if an external snapshot is requested, copy latest snapshot directory into that
    if(!ext_snapshot_path.empty()) {
        const butil::FilePath& dest_state_dir = butil::FilePath(ext_snapshot_path + "/state");

        if(!butil::DirectoryExists(dest_state_dir)) {
            butil::CreateDirectory(dest_state_dir, true);
        }

        const butil::FilePath& src_snapshot_dir = butil::FilePath(state_dir_path + "/snapshot");
        const butil::FilePath& src_meta_dir = butil::FilePath(state_dir_path + "/meta");

        bool snapshot_copied = butil::CopyDirectory(src_snapshot_dir, dest_state_dir, true);
        bool meta_copied = butil::CopyDirectory(src_meta_dir, dest_state_dir, true);

        ext_snapshot_succeeded = snapshot_copied && meta_copied;
    }

    // order is important, because the atomic boolean guards write to the path
    replication_state->set_ext_snapshot_path("");
    replication_state->set_snapshot_in_progress(false);

    req->last_chunk_aggregate = true;
    res->final = true;

    nlohmann::json response;
    uint32_t status_code;

    if(!status().ok()) {
        // in case of internal raft error
        LOG(ERROR) << "On demand snapshot failed, error: " << status().error_str() << ", code: " << status().error_code();
        status_code = 500;
        response["success"] = false;
        response["error"] = status().error_str();
    } else if(!ext_snapshot_succeeded && !ext_snapshot_path.empty()) {
        LOG(ERROR) << "On demand snapshot failed, error: copy failed.";
        status_code = 500;
        response["success"] = false;
        response["error"] = "Copy failed.";
    } else {
        LOG(INFO) << "On demand snapshot succeeded!";
        status_code = 201;
        response["success"] = true;
    }

    res->status_code = status_code;
    res->body = response.dump();

    auto req_res = new async_req_res_t(req, res, true);
    replication_state->get_message_dispatcher()->send_message(HttpServer::STREAM_RESPONSE_MESSAGE, req_res);

    // wait for response to be sent
    res->wait();
}
