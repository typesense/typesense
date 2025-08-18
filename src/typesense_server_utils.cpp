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
#include <butil/files/file_enumerator.h>
#include "analytics_manager.h"
#include "analytics_manager.h"
#include "housekeeper.h"
#include "raft_server_manager.h"

#include "core_api.h"
#include "ratelimit_manager.h"
#include "embedder_manager.h"
#include "typesense_server_utils.h"
#include "threadpool.h"
#include "stopwords_manager.h"
#include "conversation_manager.h"
#include "vq_model_manager.h"
#include "stemmer_manager.h"
#include "natural_language_search_model_manager.h"
#include "conversation_model.h"

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
    options.add<uint32_t>("cache-num-entries", '\0', "Number of entries to cache.", false, 1000);
    options.add<uint32_t>("embedding-cache-num-entries", '\0', "Number of entries to cache for embeddings.", false, 100);
    options.add<uint32_t>("analytics-flush-interval", '\0', "Frequency of persisting analytics data to disk (in seconds).", false, 3600);
    options.add<uint32_t>("housekeeping-interval", '\0', "Frequency of housekeeping background job (in seconds).", false, 1800);
    options.add<bool>("enable-lazy-filter", '\0', "Filter clause will be evaluated lazily.", false, false);
    options.add<uint32_t>("db-compaction-interval", '\0', "Frequency of RocksDB compaction (in seconds).", false, 604800);
    options.add<uint16_t>("filter-by-max-ops", '\0', "Maximum number of operations permitted in filtery_by.", false, Config::FILTER_BY_DEFAULT_OPERATIONS);

    options.add<int>("max-per-page", '\0', "Max number of hits per page", false, 250);
    options.add<uint32_t>("max-group-limit", '\0', "Max number of results to be returned per group", false, 99);

    //rocksdb options
    options.add<uint32_t>("db-write-buffer-size", '\0', "rocksdb write buffer size.", false);
    options.add<uint32_t>("db-max-write-buffer-number", '\0', "rocksdb max write buffer number.", false);
    options.add<uint32_t>("db-max-log-file-size", '\0', "rocksdb max logfile size.", false);
    options.add<uint32_t>("db-keep-log-file-num", '\0', "rocksdb number of log files to keep.", false);
    options.add<uint32_t>("max-indexing-concurrency", '\0', "maximum concurrency for batch indexing docs.", false);

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

    if (config.get_enable_search_analytics() && !config.get_analytics_dir().empty() &&
        !directory_exists(config.get_analytics_dir())) {
        LOG(INFO) << "Analytics directory " << config.get_analytics_dir() << " does not exist, will create it...";
        if(!create_directory(config.get_analytics_dir())) {
            LOG(ERROR) << "Could not create analytics directory. Quitting.";
            return 1;
        }
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

    size_t db_write_buffer_size = config.get_db_write_buffer_size();
    size_t db_max_write_buffer_number = config.get_db_max_write_buffer_number();
    size_t db_max_log_file_size = config.get_db_max_log_file_size();
    size_t db_keep_log_file_num = config.get_db_keep_log_file_num();

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
    Store store(db_dir, 24*60*60, 1024, true, 0, db_write_buffer_size, db_max_write_buffer_number,
                db_max_log_file_size, db_keep_log_file_num);

    // meta DB for storing house keeping things
    Store meta_store(meta_dir, 24*60*60, 1024, false);

    Store* analytics_store = nullptr;
    if(!analytics_dir.empty()) {
        // Analytics DB for storing analytics events

        // We want to keep rocksdb files inside a `db` directory inside `analytics_dir`.
        // Need to handle missing db subdir from older versions by creating and moving files inside
        std::string analytics_db_dir = analytics_dir + "/db";
        if(!directory_exists(analytics_db_dir)) {
            create_directory(analytics_db_dir);
            butil::FileEnumerator analytics_dir_enum(butil::FilePath(analytics_dir), false,
                                                     butil::FileEnumerator::FILES);
            for (butil::FilePath file = analytics_dir_enum.Next(); !file.empty(); file = analytics_dir_enum.Next()) {
                butil::FilePath dest_path(analytics_db_dir + "/" + file.BaseName().value());
                butil::Move(file, dest_path);
            }
        }

        analytics_store = new Store(analytics_db_dir, 24*60*60, 1024, true, analytics_db_ttl);
    }

    AnalyticsManager::get_instance().init(&store, analytics_store, analytics_minute_rate_limit);
    RemoteEmbedder::cache.capacity(config.get_embedding_cache_num_entries());

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

    StemmerManager& stemmerManager = StemmerManager::get_instance();
    stemmerManager.init(&store);

    RateLimitManager *rateLimitManager = RateLimitManager::getInstance();
    auto rate_limit_manager_init = rateLimitManager->init(&meta_store);

    if(!rate_limit_manager_init.ok()) {
        LOG(INFO) << "Failed to initialize rate limit manager: " << rate_limit_manager_init.error();
    }

    EmbedderManager::set_model_dir(config.get_data_dir() + "/models");

    EmbedderManager::get_instance().migrate_public_models();

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

    auto natural_language_search_init = NaturalLanguageSearchModelManager::init(&store);

    if(!natural_language_search_init.ok()) {
        LOG(INFO) << "Failed to initialize natural language search model manager: " << natural_language_search_init.error();
    } else {
        LOG(INFO) << "Loaded " << natural_language_search_init.get() << " natural language search model(s).";
    }

    std::thread raft_thread([&replication_state, &store, &config, &state_dir,
                             &app_thread_pool, &server_thread_pool, &replication_thread_pool, batch_indexer]() {

        std::thread batch_indexing_thread([batch_indexer]() {
            batch_indexer->run();
        });

        std::thread analytics_sink_thread([&replication_state]() {
            AnalyticsManager::get_instance().run(&replication_state);
        });

        std::thread conversation_garbage_collector_thread([]() {
            LOG(INFO) << "Conversation garbage collector thread started.";
            ConversationManager::get_instance().run();
        });

        HouseKeeper::get_instance().init();
        std::thread housekeeping_thread([]() {
            HouseKeeper::get_instance().run();
        });

        RemoteEmbedder::init(&replication_state);

        RaftServerManager& raft_manager = RaftServerManager::get_instance();
        std::string path_to_nodes = config.get_nodes();
        int raft_result = raft_manager.start_raft_server(replication_state, store, state_dir, path_to_nodes,
                                                         config.get_peering_address(),
                                                         config.get_peering_port(),
                                                         config.get_peering_subnet(),
                                                         config.get_api_port(),
                                                         config.get_snapshot_interval_seconds(),
                                                         config.get_snapshot_max_byte_count_per_rpc(),
                                                         config.get_reset_peers_on_error());
        if (raft_result != 0) {
            LOG(ERROR) << "Raft server failed to start, terminating process";
            exit(-1);
        }

        LOG(INFO) << "Shutting down batch indexer...";
        batch_indexer->stop();

        LOG(INFO) << "Waiting for batch indexing thread to be done...";
        batch_indexing_thread.join();

        LOG(INFO) << "Shutting down event sink thread...";
        AnalyticsManager::get_instance().stop();

        LOG(INFO) << "Waiting for event sink thread to be done...";
        analytics_sink_thread.join();

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

