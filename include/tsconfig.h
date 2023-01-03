#pragma once

#include <atomic>
#include <cmdline.h>
#include "option.h"
#include "string_utils.h"
#include "INIReader.h"
#include "json.hpp"

class Config {
private:
    std::string data_dir;
    std::string log_dir;

    std::string api_key;

    // @deprecated
    std::string search_only_api_key;

    std::string api_address;
    uint32_t api_port;

    std::string peering_address;
    uint32_t peering_port;
    std::string peering_subnet;

    std::string nodes;

    std::string master;

    std::string ssl_certificate;
    std::string ssl_certificate_key;
    uint32_t ssl_refresh_interval_seconds;

    bool enable_cors;
    std::set<std::string> cors_domains;

    float max_memory_ratio;
    int snapshot_interval_seconds;
    int snapshot_max_byte_count_per_rpc;

    std::atomic<size_t> healthy_read_lag;
    std::atomic<size_t> healthy_write_lag;

    std::string config_file;
    int config_file_validity;

    std::atomic<int> log_slow_requests_time_ms;

    uint32_t num_collections_parallel_load;
    uint32_t num_documents_parallel_load;

    uint32_t thread_pool_size;

    bool enable_access_logging;

    int disk_used_max_percentage;
    int memory_used_max_percentage;

    std::atomic<bool> skip_writes;

    std::atomic<int> log_slow_searches_time_ms;

protected:

    Config() {
        this->api_address = "0.0.0.0";
        this->api_port = 8108;
        this->peering_port = 8107;
        this->enable_cors = true;
        this->max_memory_ratio = 1.0f;
        this->snapshot_interval_seconds = 3600;
        this->snapshot_max_byte_count_per_rpc = 4194304;
        this->healthy_read_lag = 1000;
        this->healthy_write_lag = 500;
        this->log_slow_requests_time_ms = -1;
        this->num_collections_parallel_load = 0;  // will be set dynamically if not overridden
        this->num_documents_parallel_load = 1000;
        this->thread_pool_size = 0; // will be set dynamically if not overridden
        this->ssl_refresh_interval_seconds = 8 * 60 * 60;
        this->enable_access_logging = false;
        this->disk_used_max_percentage = 100;
        this->memory_used_max_percentage = 100;
        this->skip_writes = false;
        this->log_slow_searches_time_ms = 30 * 1000;
    }

    Config(Config const&) {

    }

public:

    static Config & get_instance() {
        static Config instance;
        return instance;
    }

    void operator=(Config const&) = delete;

    // setters

    void set_data_dir(const std::string & data_dir) {
        this->data_dir = data_dir;
    }

    void set_log_dir(const std::string & log_dir) {
        this->log_dir = log_dir;
    }

    void set_api_key(const std::string & api_key) {
        this->api_key = api_key;
    }

    // @deprecated
    void set_search_only_api_key(const std::string & search_only_api_key) {
        this->search_only_api_key = search_only_api_key;
    }

    void set_listen_address(const std::string & listen_address) {
        this->api_address = listen_address;
    }

    void set_listen_port(int listen_port) {
        this->api_port = listen_port;
    }

    void set_master(const std::string & master) {
        this->master = master;
    }

    void set_ssl_cert(const std::string & ssl_cert) {
        this->ssl_certificate = ssl_cert;
    }

    void set_ssl_cert_key(const std::string & ssl_cert_key) {
        this->ssl_certificate_key = ssl_cert_key;
    }

    void set_enable_cors(bool enable_cors) {
        this->enable_cors = enable_cors;
    }

    void set_log_slow_requests_time_ms(int log_slow_requests_time_ms) {
        this->log_slow_requests_time_ms = log_slow_requests_time_ms;
    }

    void set_log_slow_searches_time_ms(int log_slow_searches_time_ms) {
        this->log_slow_searches_time_ms = log_slow_searches_time_ms;
    }

    void set_healthy_read_lag(size_t healthy_read_lag) {
        this->healthy_read_lag = healthy_read_lag;
    }

    void set_healthy_write_lag(size_t healthy_write_lag) {
        this->healthy_write_lag = healthy_write_lag;
    }

    void set_skip_writes(bool skip_writes) {
        this->skip_writes = skip_writes;
    }

    // getters

    std::string get_data_dir() const {
        return this->data_dir;
    }

    std::string get_log_dir() const {
        return this->log_dir;
    }

    std::string get_api_key() const {
        return this->api_key;
    }

    // @deprecated
    std::string get_search_only_api_key() const {
        return this->search_only_api_key;
    }

    std::string get_api_address() const {
        return this->api_address;
    }

    int get_api_port() const {
        return this->api_port;
    }

    std::string get_master() const {
        return this->master;
    }

    std::string get_ssl_cert() const {
        return this->ssl_certificate;
    }

    std::string get_ssl_cert_key() const {
        return this->ssl_certificate_key;
    }

    std::string get_config_file() const {
        return config_file;
    }

    bool get_enable_cors() const {
        return this->enable_cors;
    }

    std::set<std::string> get_cors_domains() const {
        return this->cors_domains;
    }

    std::string get_peering_address() const {
        return this->peering_address;
    }

    std::string get_peering_subnet() const {
        return this->peering_subnet;
    }

    int get_peering_port() const {
        return this->peering_port;
    }

    std::string get_nodes() const {
        return this->nodes;
    }

    float get_max_memory_ratio() const {
        return this->max_memory_ratio;
    }

    int get_snapshot_interval_seconds() const {
        return this->snapshot_interval_seconds;
    }

    int get_snapshot_max_byte_count_per_rpc() const {
        return this->snapshot_max_byte_count_per_rpc;
    }

    size_t get_healthy_read_lag() const {
        return this->healthy_read_lag;
    }

    size_t get_healthy_write_lag() const {
        return this->healthy_write_lag;
    }

    int get_log_slow_requests_time_ms() const {
        return this->log_slow_requests_time_ms;
    }

    int get_log_slow_searches_time_ms() const {
        return this->log_slow_searches_time_ms;
    }

    size_t get_num_collections_parallel_load() const {
        return this->num_collections_parallel_load;
    }

    size_t get_num_documents_parallel_load() const {
        return this->num_documents_parallel_load;
    }

    size_t get_thread_pool_size() const {
        return this->thread_pool_size;
    }

    size_t get_ssl_refresh_interval_seconds() const {
        return this->ssl_refresh_interval_seconds;
    }

    bool get_enable_access_logging() const {
        return this->enable_access_logging;
    }

    int get_disk_used_max_percentage() const {
        return this->disk_used_max_percentage;
    }

    int get_memory_used_max_percentage() const {
        return this->memory_used_max_percentage;
    }

    std::string get_access_log_path() const {
        if(this->log_dir.empty()) {
            return "";
        }

        return this->log_dir + "/typesense-access.log";
    }

    const std::atomic<bool>& get_skip_writes() const {
        return skip_writes;
    }

    // loaders

    std::string get_env(const char *name) {
        const char *ret = getenv(name);
        if (!ret) {
            return std::string();
        }
        return std::string(ret);
    }

    void load_config_env() {
        this->data_dir = get_env("TYPESENSE_DATA_DIR");
        this->log_dir = get_env("TYPESENSE_LOG_DIR");
        this->api_key = get_env("TYPESENSE_API_KEY");

        // @deprecated
        this->search_only_api_key = get_env("TYPESENSE_SEARCH_ONLY_API_KEY");

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

        if(!get_env("TYPESENSE_DISK_USED_MAX_PERCENTAGE").empty()) {
            this->disk_used_max_percentage = std::stoi(get_env("TYPESENSE_DISK_USED_MAX_PERCENTAGE"));
        }

        if(!get_env("TYPESENSE_MEMORY_USED_MAX_PERCENTAGE").empty()) {
            this->memory_used_max_percentage = std::stoi(get_env("TYPESENSE_MEMORY_USED_MAX_PERCENTAGE"));
        }

        this->skip_writes = ("TRUE" == get_env("TYPESENSE_SKIP_WRITES"));
    }

    void load_config_file(cmdline::parser & options) {
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

        if(reader.Exists("server", "api-key")) {
            this->api_key = reader.Get("server", "api-key", "");
        }

        // @deprecated
        if(reader.Exists("server", "search-only-api-key")) {
            this->search_only_api_key = reader.Get("server", "search-only-api-key", "");
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

        if(reader.Exists("server", "disk-used-max-percentage")) {
            this->disk_used_max_percentage = (int) reader.GetInteger("server", "disk-used-max-percentage", 100);
        }

        if(reader.Exists("server", "memory-used-max-percentage")) {
            this->memory_used_max_percentage = (int) reader.GetInteger("server", "memory-used-max-percentage", 100);
        }

        if(reader.Exists("server", "skip-writes")) {
            auto skip_writes_str = reader.Get("server", "skip-writes", "false");
            this->skip_writes = (skip_writes_str == "true");
        }
    }

    void load_config_cmd_args(cmdline::parser & options) {
        if(options.exist("data-dir")) {
            this->data_dir = options.get<std::string>("data-dir");
        }

        if(options.exist("log-dir")) {
            this->log_dir = options.get<std::string>("log-dir");
        }

        if(options.exist("api-key")) {
            this->api_key = options.get<std::string>("api-key");
        }

        // @deprecated
        if(options.exist("search-only-api-key")) {
            this->search_only_api_key = options.get<std::string>("search-only-api-key");
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
    }

    void set_cors_domains(std::string& cors_domains_value) {
        std::vector<std::string> cors_values_vec;
        StringUtils::split(cors_domains_value, cors_values_vec, ",");
        cors_domains.clear();
        cors_domains.insert(cors_values_vec.begin(), cors_values_vec.end());
    }

    // validation

    Option<bool> is_valid() {
        if(this->config_file_validity == -1) {
            return Option<bool>(500, "Error parsing the configuration file.");
        }

        if(data_dir.empty()) {
            return Option<bool>(500, "Data directory is not specified.");
        }

        if(api_key.empty()) {
            return Option<bool>(500, "API key is not specified.");
        }

        return Option<bool>(true);
    }

    Option<bool> update_config(const nlohmann::json& req_json);
};
