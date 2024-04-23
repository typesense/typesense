#include "option.h"
#include "json.hpp"
#include "tsconfig.h"
#include "file_utils.h"
#include <fstream>

Option<bool> Config::update_config(const nlohmann::json& req_json) {
    bool found_config = false;

    if(req_json.count("log-slow-requests-time-ms") != 0) {
        if(!req_json["log-slow-requests-time-ms"].is_number_integer()) {
            return Option<bool>(400, "Configuration `log-slow-requests-time-ms` must be an integer.");
        }

        set_log_slow_requests_time_ms(req_json["log-slow-requests-time-ms"].get<int>());
        found_config = true;
    }

    if(req_json.count("log-slow-searches-time-ms") != 0) {
        if(!req_json["log-slow-searches-time-ms"].is_number_integer()) {
            return Option<bool>(400, "Configuration `log-slow-searches-time-ms` must be an integer.");
        }

        set_log_slow_searches_time_ms(req_json["log-slow-searches-time-ms"].get<int>());
        found_config = true;
    }

    if(req_json.count("enable-search-logging") != 0) {
        if(!req_json["enable-search-logging"].is_boolean()) {
            return Option<bool>(400, "Configuration `enable-search-logging` must be a boolean.");
        }

        set_enable_search_logging(req_json["enable-search-logging"].get<bool>());
        found_config = true;
    }

    if(req_json.count("healthy-read-lag") != 0) {
        if(!req_json["healthy-read-lag"].is_number_integer()) {
            return Option<bool>(400, "Configuration `healthy-read-lag` must be a positive integer.");
        }

        int read_lag = req_json["healthy-read-lag"].get<int>();
        if(read_lag <= 0) {
            return Option<bool>(400, "Configuration `healthy-read-lag` must be a positive integer.");
        }

        set_healthy_read_lag(read_lag);
        found_config = true;
    }

    if(req_json.count("healthy-write-lag") != 0) {
        if(!req_json["healthy-write-lag"].is_number_integer()) {
            return Option<bool>(400, "Configuration `healthy-write-lag` must be an integer.");
        }

        int write_lag = req_json["healthy-write-lag"].get<int>();
        if(write_lag <= 0) {
            return Option<bool>(400, "Configuration `healthy-write-lag` must be a positive integer.");
        }

        set_healthy_write_lag(write_lag);
        found_config = true;
    }

    if(req_json.count("cache-num-entries") != 0) {
        if(!req_json["cache-num-entries"].is_number_integer()) {
            return Option<bool>(400, "Configuration `cache-num-entries` must be an integer.");
        }

        int cache_entries_num = req_json["cache-num-entries"].get<int>();
        if(cache_entries_num <= 0) {
            return Option<bool>(400, "Configuration `cache-num-entries` must be a positive integer.");
        }

        set_cache_num_entries(cache_entries_num);
        found_config = true;
    }

    if(req_json.count("skip-writes") != 0) {
        if(!req_json["skip-writes"].is_boolean()) {
            return Option<bool>(400, ("Configuration `skip-writes` must be a boolean."));
        }

        bool skip_writes = req_json["skip-writes"].get<bool>();
        set_skip_writes(skip_writes);
        found_config = true;
    }

    return Option<bool>(true);
}

Option<std::string> Config::fetch_file_contents(const std::string & file_path) {
    if(!file_exists(file_path)) {
        return Option<std::string>(404, std::string("File does not exist at: ") + file_path);
    }

    std::ifstream infile(file_path);
    std::string content((std::istreambuf_iterator<char>(infile)), (std::istreambuf_iterator<char>()));
    infile.close();

    if(content.empty()) {
        return Option<std::string>(400, std::string("Empty file at: ") + file_path);
    }

    return Option<std::string>(content);
}

Option<std::string> Config::fetch_nodes_config(const std::string& path_to_nodes) {
    std::string nodes_config;

    if(!path_to_nodes.empty()) {
        const Option<std::string> & nodes_op = fetch_file_contents(path_to_nodes);

        if(!nodes_op.ok()) {
            return Option<std::string>(500, "Error reading file containing nodes configuration: " + nodes_op.error());
        } else {
            nodes_config = nodes_op.get();
            if(nodes_config.empty()) {
                return Option<std::string>(500, "File containing nodes configuration is empty.");
            } else {
                nodes_config = nodes_op.get();
            }
        }
    }

    return Option<std::string>(nodes_config);
}

void Config::load_config_env() {
    this->data_dir = get_env("TYPESENSE_DATA_DIR");
    this->log_dir = get_env("TYPESENSE_LOG_DIR");
    this->analytics_dir = get_env("TYPESENSE_ANALYTICS_DIR");
    this->api_key = get_env("TYPESENSE_API_KEY");

    // @deprecated
    this->search_only_api_key = get_env("TYPESENSE_SEARCH_ONLY_API_KEY");

    this->health_rusage_api_key = get_env("TYPESENSE_HEALTH_RUSAGE_API_KEY");

    if(!get_env("TYPESENSE_LISTEN_ADDRESS").empty()) {
        this->api_address = get_env("TYPESENSE_LISTEN_ADDRESS");
    }

    if(!get_env("TYPESENSE_LISTEN_PORT").empty()) {
        this->api_port = std::stoi(get_env("TYPESENSE_LISTEN_PORT"));
    }

    if(!get_env("TYPESENSE_API_ADDRESS").empty()) {
        this->api_address = get_env("TYPESENSE_API_ADDRESS");
    }

    if(!get_env("TYPESENSE_API_PORT").empty()) {
        this->api_port = std::stoi(get_env("TYPESENSE_API_PORT"));
    }

    if(!get_env("TYPESENSE_PEERING_ADDRESS").empty()) {
        this->peering_address = get_env("TYPESENSE_PEERING_ADDRESS");
    }

    if(!get_env("TYPESENSE_PEERING_PORT").empty()) {
        this->peering_port = std::stoi(get_env("TYPESENSE_PEERING_PORT"));
    }

    if(!get_env("TYPESENSE_PEERING_SUBNET").empty()) {
        this->peering_subnet = get_env("TYPESENSE_PEERING_SUBNET");
    }

    this->nodes = get_env("TYPESENSE_NODES");

    this->master = get_env("TYPESENSE_MASTER");
    this->ssl_certificate = get_env("TYPESENSE_SSL_CERTIFICATE");
    this->ssl_certificate_key = get_env("TYPESENSE_SSL_CERTIFICATE_KEY");

    std::string enable_cors_str = get_env("TYPESENSE_ENABLE_CORS");
    StringUtils::toupper(enable_cors_str);
    this->enable_cors = ("TRUE" == enable_cors_str || enable_cors_str.empty()) ? true : false;

    std::string cors_domains_value = get_env("TYPESENSE_CORS_DOMAINS");
    set_cors_domains(cors_domains_value);

    if(!get_env("TYPESENSE_MAX_MEMORY_RATIO").empty()) {
        this->max_memory_ratio = std::stof(get_env("TYPESENSE_MAX_MEMORY_RATIO"));
    }

    if(!get_env("TYPESENSE_SNAPSHOT_INTERVAL_SECONDS").empty()) {
        this->snapshot_interval_seconds = std::stoi(get_env("TYPESENSE_SNAPSHOT_INTERVAL_SECONDS"));
    }

    if(!get_env("TYPESENSE_HEALTHY_READ_LAG").empty()) {
        this->healthy_read_lag = std::stoi(get_env("TYPESENSE_HEALTHY_READ_LAG"));
    }

    if(!get_env("TYPESENSE_HEALTHY_WRITE_LAG").empty()) {
        this->healthy_write_lag = std::stoi(get_env("TYPESENSE_HEALTHY_WRITE_LAG"));
    }

    if(!get_env("TYPESENSE_LOG_SLOW_REQUESTS_TIME_MS").empty()) {
        this->log_slow_requests_time_ms = std::stoi(get_env("TYPESENSE_LOG_SLOW_REQUESTS_TIME_MS"));
    }

    if(!get_env("TYPESENSE_LOG_SLOW_SEARCHES_TIME_MS").empty()) {
        this->log_slow_searches_time_ms = std::stoi(get_env("TYPESENSE_LOG_SLOW_SEARCHES_TIME_MS"));
    }

    if(!get_env("TYPESENSE_NUM_COLLECTIONS_PARALLEL_LOAD").empty()) {
        this->num_collections_parallel_load = std::stoi(get_env("TYPESENSE_NUM_COLLECTIONS_PARALLEL_LOAD"));
    }

    if(!get_env("TYPESENSE_NUM_DOCUMENTS_PARALLEL_LOAD").empty()) {
        this->num_documents_parallel_load = std::stoi(get_env("TYPESENSE_NUM_DOCUMENTS_PARALLEL_LOAD"));
    }

    if(!get_env("TYPESENSE_CACHE_NUM_ENTRIES").empty()) {
        this->cache_num_entries = std::stoi(get_env("TYPESENSE_CACHE_NUM_ENTRIES"));
    }

    if(!get_env("TYPESENSE_ANALYTICS_FLUSH_INTERVAL").empty()) {
        this->analytics_flush_interval = std::stoi(get_env("TYPESENSE_ANALYTICS_FLUSH_INTERVAL"));
    }

    if(!get_env("TYPESENSE_HOUSEKEEPING_INTERVAL").empty()) {
        this->housekeeping_interval = std::stoi(get_env("TYPESENSE_HOUSEKEEPING_INTERVAL"));
    }

    if(!get_env("TYPESENSE_DB_COMPACTION_INTERVAL").empty()) {
        this->db_compaction_interval = std::stoi(get_env("TYPESENSE_DB_COMPACTION_INTERVAL"));
    }

    if(!get_env("TYPESENSE_THREAD_POOL_SIZE").empty()) {
        this->thread_pool_size = std::stoi(get_env("TYPESENSE_THREAD_POOL_SIZE"));
    }

    if(!get_env("TYPESENSE_SSL_REFRESH_INTERVAL_SECONDS").empty()) {
        this->ssl_refresh_interval_seconds = std::stoi(get_env("TYPESENSE_SSL_REFRESH_INTERVAL_SECONDS"));
    }

    if(!get_env("TYPESENSE_SNAPSHOT_MAX_BYTE_COUNT_PER_RPC").empty()) {
        this->snapshot_max_byte_count_per_rpc = std::stoi(get_env("TYPESENSE_SNAPSHOT_MAX_BYTE_COUNT_PER_RPC"));
    }

    this->enable_access_logging = ("TRUE" == get_env("TYPESENSE_ENABLE_ACCESS_LOGGING"));
    this->enable_search_analytics = ("TRUE" == get_env("TYPESENSE_ENABLE_SEARCH_ANALYTICS"));
    this->enable_search_logging = ("TRUE" == get_env("TYPESENSE_ENABLE_SEARCH_LOGGING"));

    if(!get_env("TYPESENSE_DISK_USED_MAX_PERCENTAGE").empty()) {
        this->disk_used_max_percentage = std::stoi(get_env("TYPESENSE_DISK_USED_MAX_PERCENTAGE"));
    }

    if(!get_env("TYPESENSE_MEMORY_USED_MAX_PERCENTAGE").empty()) {
        this->memory_used_max_percentage = std::stoi(get_env("TYPESENSE_MEMORY_USED_MAX_PERCENTAGE"));
    }

    if(!get_env("TYPESENSE_FILTER_BY_MAX_OPS").empty()) {
        this->filter_by_max_ops = std::stoi(get_env("TYPESENSE_FILTER_BY_MAX_OPS"));
    }

    this->skip_writes = ("TRUE" == get_env("TYPESENSE_SKIP_WRITES"));
    this->enable_lazy_filter = ("TRUE" == get_env("TYPESENSE_ENABLE_LAZY_FILTER"));
    this->reset_peers_on_error = ("TRUE" == get_env("TYPESENSE_RESET_PEERS_ON_ERROR"));

    if(!get_env("TYPESENSE_MAX_PER_PAGE").empty()) {
        this->max_per_page = std::stoi(get_env("TYPESENSE_MAX_PER_PAGE"));
    }
}

void Config::load_config_file(cmdline::parser& options) {
    this->config_file = options.exist("config") ? options.get<std::string>("config") : "";

    if(!options.exist("config")) {
        config_file_validity = 0;
        return;
    }

    this->config_file = options.get<std::string>("config");

    INIReader reader(this->config_file);

    if (reader.ParseError() != 0) {
        LOG(ERROR) << "Error while parsing config file, code = " << reader.ParseError();
        config_file_validity = -1;
        return ;
    }

    config_file_validity = 1;

    if(reader.Exists("server", "data-dir")) {
        this->data_dir = reader.Get("server", "data-dir", "");
    }

    if(reader.Exists("server", "log-dir")) {
        this->log_dir = reader.Get("server", "log-dir", "");
    }

    if(reader.Exists("server", "analytics-dir")) {
        this->analytics_dir = reader.Get("server", "analytics-dir", "");
    }

    if(reader.Exists("server", "api-key")) {
        this->api_key = reader.Get("server", "api-key", "");
    }

    // @deprecated
    if(reader.Exists("server", "search-only-api-key")) {
        this->search_only_api_key = reader.Get("server", "search-only-api-key", "");
    }

    if(reader.Exists("server", "health-rusage-api-key")) {
        this->health_rusage_api_key = reader.Get("server", "health-rusage-api-key", "");
    }

    if(reader.Exists("server", "listen-address")) {
        this->api_address = reader.Get("server", "listen-address", "");
    }

    if(reader.Exists("server", "api-address")) {
        this->api_address = reader.Get("server", "api-address", "");
    }

    if(reader.Exists("server", "master")) {
        this->master = reader.Get("server", "master", "");
    }

    if(reader.Exists("server", "ssl-certificate")) {
        this->ssl_certificate = reader.Get("server", "ssl-certificate", "");
    }

    if(reader.Exists("server", "ssl-certificate-key")) {
        this->ssl_certificate_key = reader.Get("server", "ssl-certificate-key", "");
    }

    if(reader.Exists("server", "listen-port")) {
        this->api_port = reader.GetInteger("server", "listen-port", 8108);
    }

    if(reader.Exists("server", "api-port")) {
        this->api_port = reader.GetInteger("server", "api-port", 8108);
    }

    if(reader.Exists("server", "enable-cors")) {
        auto enable_cors_value = reader.Get("server", "enable-cors", "true");
        StringUtils::tolowercase(enable_cors_value);
        this->enable_cors = enable_cors_value == "true";
    }

    if(reader.Exists("server", "cors-domains")) {
        std::string cors_value = reader.Get("server", "cors-domains", "");
        set_cors_domains(cors_value);
    }

    if(reader.Exists("server", "peering-address")) {
        this->peering_address = reader.Get("server", "peering-address", "");
    }

    if(reader.Exists("server", "peering-port")) {
        this->peering_port = reader.GetInteger("server", "peering-port", 8107);
    }

    if(reader.Exists("server", "peering-subnet")) {
        this->peering_subnet = reader.Get("server", "peering-subnet", "");
    }

    if(reader.Exists("server", "nodes")) {
        this->nodes = reader.Get("server", "nodes", "");
    }

    if(reader.Exists("server", "max-memory-ratio")) {
        this->max_memory_ratio = (float) reader.GetReal("server", "max-memory-ratio", 1.0f);
    }

    if(reader.Exists("server", "snapshot-interval-seconds")) {
        this->snapshot_interval_seconds = (int) reader.GetInteger("server", "snapshot-interval-seconds", 3600);
    }

    if(reader.Exists("server", "snapshot-max-byte-count-per-rpc")) {
        this->snapshot_max_byte_count_per_rpc = (int) reader.GetInteger("server", "snapshot-max-byte-count-per-rpc", 4194304);
    }

    if(reader.Exists("server", "healthy-read-lag")) {
        this->healthy_read_lag = (size_t) reader.GetInteger("server", "healthy-read-lag", 1000);
    }

    if(reader.Exists("server", "healthy-write-lag")) {
        this->healthy_write_lag = (size_t) reader.GetInteger("server", "healthy-write-lag", 100);
    }

    if(reader.Exists("server", "log-slow-requests-time-ms")) {
        this->log_slow_requests_time_ms = (int) reader.GetInteger("server", "log-slow-requests-time-ms", -1);
    }

    if(reader.Exists("server", "log-slow-searches-time-ms")) {
        this->log_slow_searches_time_ms = (int) reader.GetInteger("server", "log-slow-searches-time-ms", 30*1000);
    }

    if(reader.Exists("server", "num-collections-parallel-load")) {
        this->num_collections_parallel_load = (int) reader.GetInteger("server", "num-collections-parallel-load", 0);
    }

    if(reader.Exists("server", "num-documents-parallel-load")) {
        this->num_documents_parallel_load = (int) reader.GetInteger("server", "num-documents-parallel-load", 1000);
    }

    if(reader.Exists("server", "cache-num-entries")) {
        this->cache_num_entries = (int) reader.GetInteger("server", "cache-num-entries", 1000);
    }

    if(reader.Exists("server", "analytics-flush-interval")) {
        this->analytics_flush_interval = (int) reader.GetInteger("server", "analytics-flush-interval", 3600);
    }

    if(reader.Exists("server", "housekeeping-interval")) {
        this->housekeeping_interval = (int) reader.GetInteger("server", "housekeeping-interval", 1800);
    }

    if(reader.Exists("server", "db-compaction-interval")) {
        this->db_compaction_interval = (int) reader.GetInteger("server", "db-compaction-interval", 0);
    }

    if(reader.Exists("server", "thread-pool-size")) {
        this->thread_pool_size = (int) reader.GetInteger("server", "thread-pool-size", 0);
    }

    if(reader.Exists("server", "ssl-refresh-interval-seconds")) {
        this->ssl_refresh_interval_seconds = (int) reader.GetInteger("server", "ssl-refresh-interval-seconds", 8 * 60 * 60);
    }

    if(reader.Exists("server", "enable-access-logging")) {
        auto enable_access_logging_str = reader.Get("server", "enable-access-logging", "false");
        this->enable_access_logging = (enable_access_logging_str == "true");
    }

    if(reader.Exists("server", "enable-search-analytics")) {
        auto enable_search_analytics_str = reader.Get("server", "enable-search-analytics", "false");
        this->enable_search_analytics = (enable_search_analytics_str == "true");
    }

    if(reader.Exists("server", "enable-search-logging")) {
        auto enable_search_logging_str = reader.Get("server", "enable-search-logging", "false");
        this->enable_search_logging = (enable_search_logging_str == "true");
    }

    if(reader.Exists("server", "disk-used-max-percentage")) {
        this->disk_used_max_percentage = (int) reader.GetInteger("server", "disk-used-max-percentage", 100);
    }

    if(reader.Exists("server", "memory-used-max-percentage")) {
        this->memory_used_max_percentage = (int) reader.GetInteger("server", "memory-used-max-percentage", 100);
    }

    if(reader.Exists("server", "enable-lazy-filter")) {
        auto enable_lazy_filter_str = reader.Get("server", "enable-lazy-filter", "false");
        this->enable_lazy_filter = (enable_lazy_filter_str == "true");
    }

    if(reader.Exists("server", "skip-writes")) {
        auto skip_writes_str = reader.Get("server", "skip-writes", "false");
        this->skip_writes = (skip_writes_str == "true");
    }

    if(reader.Exists("server", "reset-peers-on-error")) {
        auto reset_peers_on_error_str = reader.Get("server", "reset-peers-on-error", "false");
        this->reset_peers_on_error = (reset_peers_on_error_str == "true");
    }

    if(reader.Exists("server", "max-per-page")) {
        this->max_per_page = reader.GetInteger("server", "max-per-page", 250);
    }

    if(reader.Exists("server", "filter-by-max-ops")) {
        this->filter_by_max_ops = (uint16_t) reader.GetInteger("server", "filter-by-max-ops", FILTER_BY_DEFAULT_OPERATIONS);
    }
}

void Config::load_config_cmd_args(cmdline::parser& options)  {
    if(options.exist("data-dir")) {
        this->data_dir = options.get<std::string>("data-dir");
    }

    if(options.exist("log-dir")) {
        this->log_dir = options.get<std::string>("log-dir");
    }

    if(options.exist("analytics-dir")) {
        this->analytics_dir = options.get<std::string>("analytics-dir");
    }

    if(options.exist("api-key")) {
        this->api_key = options.get<std::string>("api-key");
    }

    // @deprecated
    if(options.exist("search-only-api-key")) {
        this->search_only_api_key = options.get<std::string>("search-only-api-key");
    }

    if(options.exist("health-rusage-api-key")) {
        this->health_rusage_api_key = options.get<std::string>("health-rusage-api-key");
    }

    if(options.exist("listen-address")) {
        this->api_address = options.get<std::string>("listen-address");
    }

    if(options.exist("api-address")) {
        this->api_address = options.get<std::string>("api-address");
    }

    if(options.exist("master")) {
        this->master = options.get<std::string>("master");
    }

    if(options.exist("ssl-certificate")) {
        this->ssl_certificate = options.get<std::string>("ssl-certificate");
    }

    if(options.exist("ssl-certificate-key")) {
        this->ssl_certificate_key = options.get<std::string>("ssl-certificate-key");
    }

    if(options.exist("listen-port")) {
        this->api_port = options.get<uint32_t>("listen-port");
    }

    if(options.exist("api-port")) {
        this->api_port = options.get<uint32_t>("api-port");
    }

    if(options.exist("enable-cors")) {
        this->enable_cors = options.get<bool>("enable-cors");
    }

    if(options.exist("cors-domains")) {
        std::string cors_domains_value = options.get<std::string>("cors-domains");
        set_cors_domains(cors_domains_value);
    }

    if(options.exist("peering-address")) {
        this->peering_address = options.get<std::string>("peering-address");
    }

    if(options.exist("peering-port")) {
        this->peering_port = options.get<uint32_t>("peering-port");
    }

    if(options.exist("peering-subnet")) {
        this->peering_subnet = options.get<std::string>("peering-subnet");
    }

    if(options.exist("nodes")) {
        this->nodes = options.get<std::string>("nodes");
    }

    if(options.exist("max-memory-ratio")) {
        this->max_memory_ratio = options.get<float>("max-memory-ratio");
    }

    if(options.exist("snapshot-interval-seconds")) {
        this->snapshot_interval_seconds = options.get<int>("snapshot-interval-seconds");
    }

    if(options.exist("snapshot-max-byte-count-per-rpc")) {
        this->snapshot_max_byte_count_per_rpc = options.get<int>("snapshot-max-byte-count-per-rpc");
    }

    if(options.exist("healthy-read-lag")) {
        this->healthy_read_lag = options.get<size_t>("healthy-read-lag");
    }

    if(options.exist("healthy-write-lag")) {
        this->healthy_write_lag = options.get<size_t>("healthy-write-lag");
    }

    if(options.exist("log-slow-requests-time-ms")) {
        this->log_slow_requests_time_ms = options.get<int>("log-slow-requests-time-ms");
    }

    if(options.exist("log-slow-searches-time-ms")) {
        this->log_slow_searches_time_ms = options.get<int>("log-slow-searches-time-ms");
    }

    if(options.exist("num-collections-parallel-load")) {
        this->num_collections_parallel_load = options.get<uint32_t>("num-collections-parallel-load");
    }

    if(options.exist("num-documents-parallel-load")) {
        this->num_documents_parallel_load = options.get<uint32_t>("num-documents-parallel-load");
    }

    if(options.exist("cache-num-entries")) {
        this->cache_num_entries = options.get<uint32_t>("cache-num-entries");
    }

    if(options.exist("analytics-flush-interval")) {
        this->analytics_flush_interval = options.get<uint32_t>("analytics-flush-interval");
    }

    if(options.exist("housekeeping-interval")) {
        this->housekeeping_interval = options.get<uint32_t>("housekeeping-interval");
    }

    if(options.exist("db-compaction-interval")) {
        this->db_compaction_interval = options.get<uint32_t>("db-compaction-interval");
    }

    if(options.exist("thread-pool-size")) {
        this->thread_pool_size = options.get<uint32_t>("thread-pool-size");
    }

    if(options.exist("ssl-refresh-interval-seconds")) {
        this->ssl_refresh_interval_seconds = options.get<uint32_t>("ssl-refresh-interval-seconds");
    }

    if(options.exist("enable-access-logging")) {
        this->enable_access_logging = options.get<bool>("enable-access-logging");
    }

    if(options.exist("disk-used-max-percentage")) {
        this->disk_used_max_percentage = options.get<int>("disk-used-max-percentage");
    }

    if(options.exist("memory-used-max-percentage")) {
        this->memory_used_max_percentage = options.get<int>("memory-used-max-percentage");
    }

    if(options.exist("skip-writes")) {
        this->skip_writes = options.get<bool>("skip-writes");
    }

    if(options.exist("reset-peers-on-error")) {
        this->reset_peers_on_error = options.get<bool>("reset-peers-on-error");
    }

    if(options.exist("enable-search-analytics")) {
        this->enable_search_analytics = options.get<bool>("enable-search-analytics");
    }

    if(options.exist("enable-lazy-filter")) {
        this->enable_lazy_filter = options.get<bool>("enable-lazy-filter");
    }

    if(options.exist("enable-search-logging")) {
        this->enable_search_logging = options.get<bool>("enable-search-logging");
    }

    if(options.exist("max-per-page")) {
        this->max_per_page = options.get<int>("max-per-page");
    }

    if(options.exist("filter-by-max-ops")) {
        this->filter_by_max_ops = options.get<uint16_t>("filter-by-max-ops");

    }
}

