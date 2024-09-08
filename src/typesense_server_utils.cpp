#include <cstdlib>
#include <curl/curl.h>
#include <gflags/gflags.h>
#include <dlfcn.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <raft_server.h>
#include <fstream>
#include <execinfo.h>
#include <http_client.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <ifaddrs.h>
#include "analytics_manager.h"
#include "housekeeper.h"

#include "core_api.h"
#include "ratelimit_manager.h"
#include "embedder_manager.h"
#include "typesense_server_utils.h"
#include "threadpool.h"
#include "stopwords_manager.h"
#include "conversation_manager.h"
#include "vq_model_manager.h"

#ifndef ASAN_BUILD
#include "jemalloc.h"
#endif

#include "stackprinter.h"

HttpServer* server;
std::atomic<bool> quit_raft_service;

extern "C" {
// weak symbol: resolved at runtime by the linker if we are using jemalloc, nullptr otherwise
#ifdef __APPLE__
    int je_mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen) __attribute__((weak_import));
#else
    int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen) __attribute__((weak));
#endif
}

bool using_jemalloc() {
    // On OSX, jemalloc API is prefixed with "je_"
#ifdef __APPLE__
    return (je_mallctl != nullptr);
#else
    return (mallctl != nullptr);
#endif
}

void catch_interrupt(int sig) {
    LOG(INFO) << "Stopping Typesense server...";
    signal(sig, SIG_IGN);  // ignore for now as we want to shut down elegantly
    quit_raft_service = true;
}

void init_cmdline_options(cmdline::parser & options, int argc, char **argv) {
    options.set_program_name("./typesense-server");

    options.add<std::string>("data-dir", 'd', "Directory where data will be stored.", true);
    options.add<std::string>("api-key", 'a', "API key that allows all operations.", true);
    options.add<std::string>("search-only-api-key", 's', "[DEPRECATED: use API key management end-point] API key that allows only searches.", false);
    options.add<std::string>("health-rusage-api-key", '\0', "API key that allows access to health end-point with resource usage.", false);
    options.add<std::string>("analytics-dir", '\0', "Directory where Analytics will be stored.", false);
    options.add<uint32_t>("analytics-db-ttl", '\0', "TTL in seconds for events stored in analytics db", false);
    options.add<uint32_t>("analytics-minute-rate-limit", '\0', "per minute rate limit for /events endpoint", false);


    options.add<std::string>("api-address", '\0', "Address to which Typesense API service binds.", false, "0.0.0.0");
    options.add<uint32_t>("api-port", '\0', "Port on which Typesense API service listens.", false, 8108);

    options.add<std::string>("peering-address", '\0', "Internal IP address to which Typesense peering service binds.", false, "");
    options.add<uint32_t>("peering-port", '\0', "Port on which Typesense peering service listens.", false, 8107);
    options.add<std::string>("peering-subnet", '\0', "Internal subnet that Typesense should use for peering.", false, "");
    options.add<std::string>("nodes", '\0', "Path to file containing comma separated string of all nodes in the cluster.", false);

    options.add<std::string>("ssl-certificate", 'c', "Path to the SSL certificate file.", false, "");
    options.add<std::string>("ssl-certificate-key", 'k', "Path to the SSL certificate key file.", false, "");
    options.add<uint32_t>("ssl-refresh-interval-seconds", '\0', "Frequency of automatic reloading of SSL certs from disk.", false, 8 * 60 * 60);

    options.add<bool>("enable-cors", '\0', "Enable CORS requests.", false, true);
    options.add<std::string>("cors-domains", '\0', "Comma separated list of domains that are allowed for CORS.", false, "");

    options.add<float>("max-memory-ratio", '\0', "Maximum fraction of system memory to be used.", false, 1.0f);
    options.add<int>("snapshot-interval-seconds", '\0', "Frequency of replication log snapshots.", false, 3600);
    options.add<int>("snapshot-max-byte-count-per-rpc", '\0', "Maximum snapshot file size in bytes transferred for each RPC.", false, 4194304);
    options.add<size_t>("healthy-read-lag", '\0', "Reads are rejected if the updates lag behind this threshold.", false, 1000);
    options.add<size_t>("healthy-write-lag", '\0', "Writes are rejected if the updates lag behind this threshold.", false, 500);
    options.add<int>("log-slow-requests-time-ms", '\0', "When >= 0, requests that take longer than this duration are logged.", false, -1);

    options.add<uint32_t>("num-collections-parallel-load", '\0', "Number of collections that are loaded in parallel during start up.", false, 4);
    options.add<uint32_t>("num-documents-parallel-load", '\0', "Number of documents per collection that are indexed in parallel during start up.", false, 1000);

    options.add<uint32_t>("thread-pool-size", '\0', "Number of threads used for handling concurrent requests.", false, 4);

    options.add<std::string>("log-dir", '\0', "Path to the log directory.", false, "");

    options.add<std::string>("config", '\0', "Path to the configuration file.", false, "");

    options.add<bool>("enable-access-logging", '\0', "Enable access logging.", false, false);
    options.add<bool>("enable-search-logging", '\0', "Enable search logging.", false, false);
    options.add<bool>("enable-search-analytics", '\0', "Enable search analytics.", false, false);
    options.add<int>("disk-used-max-percentage", '\0', "Reject writes when used disk space exceeds this percentage. Default: 100 (never reject).", false, 100);
    options.add<int>("memory-used-max-percentage", '\0', "Reject writes when memory usage exceeds this percentage. Default: 100 (never reject).", false, 100);
    options.add<bool>("skip-writes", '\0', "Skip all writes except config changes. Default: false.", false, false);
    options.add<bool>("reset-peers-on-error", '\0', "Reset node's peers on clustering error. Default: false.", false, false);

    options.add<int>("log-slow-searches-time-ms", '\0', "When >= 0, searches that take longer than this duration are logged.", false, 30*1000);
    options.add<int>("cache-num-entries", '\0', "Number of entries to cache.", false, 1000);
    options.add<uint32_t>("analytics-flush-interval", '\0', "Frequency of persisting analytics data to disk (in seconds).", false, 3600);
    options.add<uint32_t>("housekeeping-interval", '\0', "Frequency of housekeeping background job (in seconds).", false, 1800);
    options.add<bool>("enable-lazy-filter", '\0', "Filter clause will be evaluated lazily.", false, false);
    options.add<uint32_t>("db-compaction-interval", '\0', "Frequency of RocksDB compaction (in seconds).", false, 604800);
    options.add<uint16_t>("filter-by-max-ops", '\0', "Maximum number of operations permitted in filtery_by.", false, Config::FILTER_BY_DEFAULT_OPERATIONS);

    options.add<int>("max-per-page", '\0', "Max number of hits per page", false, 250);

    // DEPRECATED
    options.add<std::string>("listen-address", 'h', "[DEPRECATED: use `api-address`] Address to which Typesense API service binds.", false, "0.0.0.0");
    options.add<uint32_t>("listen-port", 'p', "[DEPRECATED: use `api-port`] Port on which Typesense API service listens.", false, 8108);
    options.add<std::string>("master", 'm', "[DEPRECATED: use clustering via --nodes] Master's address in http(s)://<master_address>:<master_port> format "
                                            "to start as read-only replica.", false, "");
}

int init_root_logger(Config & config, const std::string & server_version) {
    google::InitGoogleLogging("typesense");

    std::string log_dir = config.get_log_dir();

    if(log_dir.empty()) {
        // use console logger if log dir is not specified
        FLAGS_logtostderr = true;
    } else {
        if(!directory_exists(log_dir)) {
            std::cerr << "Typesense failed to start. " << "Log directory " << log_dir << " does not exist.";
            return 1;
        }

        // flush log levels above -1 immediately (INFO=0)
        FLAGS_logbuflevel = -1;

        // available only on glog master (ensures that log file name is constant)
        FLAGS_timestamp_in_logfile_name = false;

        std::string log_path = log_dir + "/" + "typesense.log";

        // will log levels INFO **and above** to the given log file
        google::SetLogDestination(google::INFO, log_path.c_str());

        // don't create symlink for INFO log
        google::SetLogSymlink(google::INFO, "");

        // don't create separate log files for each level
        google::SetLogDestination(google::WARNING, "");
        google::SetLogDestination(google::ERROR, "");
        google::SetLogDestination(google::FATAL, "");

        std::cout << "Log directory is configured as: " << log_dir << std::endl;
    }

    return 0;
}

bool is_private_ip(uint32_t ip) {
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

const char* get_internal_ip(const std::string& subnet_cidr) {
    struct ifaddrs *ifap;
    getifaddrs(&ifap);

    uint32_t netip = 0, netbits = 0;

    if(!subnet_cidr.empty()) {
        std::vector<std::string> subnet_parts;
        StringUtils::split(subnet_cidr, subnet_parts, "/");
        if(subnet_parts.size() == 2) {
            butil::ip_t subnet_addr;
            auto res = butil::str2ip(subnet_parts[0].c_str(), &subnet_addr);
            if(res == 0) {
                netip = subnet_addr.s_addr;
                if(StringUtils::is_uint32_t(subnet_parts[1])) {
                    netbits = std::stoll(subnet_parts[1]);
                }
            }
        }
    }

    if(netip != 0 && netbits != 0) {
        LOG(INFO) << "Using subnet ip: " << netip << ", bits: " << netbits;
    }

    for(auto ifa = ifap; ifa; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr && ifa->ifa_addr->sa_family==AF_INET) {
            auto sa = (struct sockaddr_in *) ifa->ifa_addr;
            auto ipaddr = sa->sin_addr.s_addr;
            if(is_private_ip(ntohl(ipaddr))) {
                if(netip != 0 && netbits != 0) {
                    unsigned int mask = 0xFFFFFFFF << (32 - netbits);
                    if((ntohl(netip) & mask) != (ntohl(ipaddr) & mask)) {
                        LOG(INFO) << "Skipping interface " << ifa->ifa_name << " as it does not match peering subnet.";
                        continue;
                    }
                }
                char *ip = inet_ntoa(sa->sin_addr);
                freeifaddrs(ifap);
                return ip;
            }
        }
    }

    LOG(WARNING) << "Found no matching interfaces, using loopback address as internal IP.";

    freeifaddrs(ifap);
    return "127.0.0.1";
}

int start_raft_server(ReplicationState& replication_state, Store& store,
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

    butil::ip_t peering_ip;
    int ip_conv_status = 0;

    if(!peering_address.empty()) {
        ip_conv_status = butil::str2ip(peering_address.c_str(), &peering_ip);
    } else {
        const char* internal_ip = get_internal_ip(peering_subnet);
        ip_conv_status = butil::str2ip(internal_ip, &peering_ip);
    }

    if(ip_conv_status != 0) {
        LOG(ERROR) << "Failed to parse peering address `" << peering_address << "`";
        return -1;
    }

    butil::EndPoint peering_endpoint(peering_ip, peering_port);

    // start peering server
    brpc::Server raft_server;

    if (braft::add_service(&raft_server, peering_endpoint) != 0) {
        LOG(ERROR) << "Failed to add peering service";
        exit(-1);
    }

    if (raft_server.Start(peering_endpoint, nullptr) != 0) {
        LOG(ERROR) << "Failed to start peering service";
        exit(-1);
    }

    size_t election_timeout_ms = 5000;

    if (replication_state.start(peering_endpoint, api_port, election_timeout_ms, snapshot_max_byte_count_per_rpc, state_dir,
                                nodes_config_op.get(), quit_raft_service) != 0) {
        LOG(ERROR) << "Failed to start peering state";
        exit(-1);
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
                replication_state.refresh_nodes(nodes_config, raft_counter, reset_peers_on_error);

                if(raft_counter % 60 == 0) {
                    replication_state.do_snapshot(nodes_config);
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

int run_server(const Config & config, const std::string & version, void (*master_server_routes)()) {
    LOG(INFO) << "Starting Typesense " << version << std::flush;
#ifndef ASAN_BUILD
    if(using_jemalloc()) {
        LOG(INFO) << "Typesense is using jemalloc.";

        // Due to time based decay depending on application not being idle-ish, set `background_thread`
        // to help with releasing memory back to the OS and improve tail latency.
        // See: https://github.com/jemalloc/jemalloc/issues/1398
        bool background_thread = true;
#ifdef __APPLE__
        je_mallctl("background_thread", nullptr, nullptr, &background_thread, sizeof(bool));
#elif __linux__
        mallctl("background_thread", nullptr, nullptr, &background_thread, sizeof(bool));
#endif
    } else {
        LOG(WARNING) << "Typesense is NOT using jemalloc.";
    }
#endif

    quit_raft_service = false;

    if(!directory_exists(config.get_data_dir())) {
        LOG(ERROR) << "Typesense failed to start. " << "Data directory " << config.get_data_dir()
                 << " does not exist.";
        return 1;
    }

    if (config.get_enable_search_analytics() && !config.get_analytics_dir().empty() && !directory_exists(config.get_analytics_dir())) {
        LOG(ERROR) << "Typesense failed to start. " << "Analytics directory " << config.get_analytics_dir()
                 << " does not exist.";
        return 1;
    }

    if(!config.get_master().empty()) {
        LOG(ERROR) << "The --master option has been deprecated. Please use clustering for high availability. "
                   << "Look for the --nodes configuration in the documentation.";
        return 1;
    }

    if(!config.get_search_only_api_key().empty()) {
        LOG(WARNING) << "!!!! WARNING !!!!";
        LOG(WARNING) << "The --search-only-api-key has been deprecated. "
                        "The API key generation end-point should be used for generating keys with specific ACL.";
    }

    std::string data_dir = config.get_data_dir();
    std::string db_dir = config.get_data_dir() + "/db";
    std::string state_dir = config.get_data_dir() + "/state";
    std::string meta_dir = config.get_data_dir() + "/meta";
    std::string analytics_dir = config.get_analytics_dir();
    int32_t analytics_db_ttl = config.get_analytics_db_ttl();
    uint32_t analytics_minute_rate_limit = config.get_analytics_minute_rate_limit();

    size_t thread_pool_size = config.get_thread_pool_size();

    const size_t proc_count = std::max<size_t>(1, std::thread::hardware_concurrency());
    const size_t num_threads = thread_pool_size == 0 ? (proc_count * 8) : thread_pool_size;

    size_t num_collections_parallel_load = config.get_num_collections_parallel_load();
    num_collections_parallel_load = (num_collections_parallel_load == 0) ?
                                    (proc_count * 4) : num_collections_parallel_load;

    LOG(INFO) << "Thread pool size: " << num_threads;
    ThreadPool app_thread_pool(num_threads);
    ThreadPool server_thread_pool(num_threads);
    ThreadPool replication_thread_pool(num_threads);

    // primary DB used for storing the documents: we will not use WAL since Raft provides that
    Store store(db_dir, 24*60*60, 1024, true);

    // meta DB for storing house keeping things
    Store meta_store(meta_dir, 24*60*60, 1024, false);

    Store* analytics_store = nullptr;
    if(!analytics_dir.empty()) {
        //analytics DB for storing analytics events
        analytics_store = new Store(analytics_dir, 24*60*60, 1024, true, analytics_db_ttl);
        AnalyticsManager::get_instance().init(&store, analytics_store, analytics_minute_rate_limit);
    }

    curl_global_init(CURL_GLOBAL_SSL);
    HttpClient & httpClient = HttpClient::get_instance();
    httpClient.init(config.get_api_key());

    server = new HttpServer(
        version,
        config.get_api_address(),
        config.get_api_port(),
        config.get_ssl_cert(),
        config.get_ssl_cert_key(),
        config.get_ssl_refresh_interval_seconds() * 1000,
        config.get_enable_cors(),
        config.get_cors_domains(),
        &server_thread_pool
    );

    server->set_auth_handler(handle_authentication);

    server->on(HttpServer::STREAM_RESPONSE_MESSAGE, HttpServer::on_stream_response_message);
    server->on(HttpServer::REQUEST_PROCEED_MESSAGE, HttpServer::on_request_proceed_message);
    server->on(HttpServer::DEFER_PROCESSING_MESSAGE, HttpServer::on_deferred_processing_message);

    bool ssl_enabled = (!config.get_ssl_cert().empty() && !config.get_ssl_cert_key().empty());

    BatchedIndexer* batch_indexer = new BatchedIndexer(server, &store, &meta_store, num_threads,
                                                       config, config.get_skip_writes());

    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(&store, &app_thread_pool, config.get_max_memory_ratio(),
                           config.get_api_key(), quit_raft_service, config.get_filter_by_max_ops());

    StopwordsManager& stopwordsManager = StopwordsManager::get_instance();
    stopwordsManager.init(&store);

    RateLimitManager *rateLimitManager = RateLimitManager::getInstance();
    auto rate_limit_manager_init = rateLimitManager->init(&meta_store);

    if(!rate_limit_manager_init.ok()) {
        LOG(INFO) << "Failed to initialize rate limit manager: " << rate_limit_manager_init.error();
    }

    EmbedderManager::set_model_dir(config.get_data_dir() + "/models");

    // first we start the peering service

    ReplicationState replication_state(server, batch_indexer, &store, analytics_store,
                                       &replication_thread_pool, server->get_message_dispatcher(),
                                       ssl_enabled,
                                       &config,
                                       num_collections_parallel_load,
                                       config.get_num_documents_parallel_load());

    auto conversations_init = ConversationManager::get_instance().init(&replication_state);

    if(!conversations_init.ok()) {
        LOG(INFO) << "Failed to initialize conversation manager: " << conversations_init.error();
    }

    std::thread raft_thread([&replication_state, &store, &config, &state_dir,
                             &app_thread_pool, &server_thread_pool, &replication_thread_pool, batch_indexer]() {

        std::thread batch_indexing_thread([batch_indexer]() {
            batch_indexer->run();
        });

        std::thread event_sink_thread([&replication_state]() {
            AnalyticsManager::get_instance().run(&replication_state);
        });

        std::thread conversation_garbage_collector_thread([]() {
            LOG(INFO) << "Conversation garbage collector thread started.";
            ConversationManager::get_instance().run();
        });
          
        HouseKeeper::get_instance().init(config.get_housekeeping_interval());
        std::thread housekeeping_thread([]() {
            HouseKeeper::get_instance().run();
        });

        RemoteEmbedder::init(&replication_state);

        std::string path_to_nodes = config.get_nodes();
        start_raft_server(replication_state, store, state_dir, path_to_nodes,
                          config.get_peering_address(),
                          config.get_peering_port(),
                          config.get_peering_subnet(),
                          config.get_api_port(),
                          config.get_snapshot_interval_seconds(),
                          config.get_snapshot_max_byte_count_per_rpc(),
                          config.get_reset_peers_on_error());

        LOG(INFO) << "Shutting down batch indexer...";
        batch_indexer->stop();

        LOG(INFO) << "Waiting for batch indexing thread to be done...";
        batch_indexing_thread.join();

        LOG(INFO) << "Shutting down event sink thread...";
        AnalyticsManager::get_instance().stop();

        LOG(INFO) << "Waiting for event sink thread to be done...";
        event_sink_thread.join();

        LOG(INFO) << "Shutting down conversation garbage collector thread...";
        ConversationManager::get_instance().stop();

        LOG(INFO) << "Waiting for conversation garbage collector thread to be done...";
        conversation_garbage_collector_thread.join();

        LOG(INFO) << "Waiting for housekeeping thread to be done...";
        HouseKeeper::get_instance().stop();
        housekeeping_thread.join();

        LOG(INFO) << "Shutting down server_thread_pool";

        server_thread_pool.shutdown();

        LOG(INFO) << "Shutting down app_thread_pool.";

        app_thread_pool.shutdown();

        LOG(INFO) << "Shutting down replication_thread_pool.";
        replication_thread_pool.shutdown();

        server->stop();
    });

    LOG(INFO) << "Starting API service...";

    master_server_routes();
    int ret_code = server->run(&replication_state);

    // we are out of the event loop here

    LOG(INFO) << "Typesense API service has quit.";
    quit_raft_service = true;  // we set this once again in case API thread crashes instead of a signal
    raft_thread.join();

    LOG(INFO) << "Deleting batch indexer";

    delete batch_indexer;

    LOG(INFO) << "CURL clean up";

    curl_global_cleanup();

    LOG(INFO) << "Deleting server";

    delete server;

    LOG(INFO) << "CollectionManager dispose, this might take some time...";

    // We have to delete the models here, before CUDA driver is unloaded.
    VQModelManager::get_instance().delete_all_models();

    CollectionManager::get_instance().dispose();

    delete analytics_store;

    LOG(INFO) << "Bye.";

    return ret_code;
}

