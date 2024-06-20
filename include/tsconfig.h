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
    std::string analytics_dir;
    int32_t analytics_db_ttl = 2419200; //four weeks in secs
    std::string api_key;

    // @deprecated
    std::string search_only_api_key;

    std::string health_rusage_api_key;

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

    std::atomic<uint32_t> cache_num_entries = 1000;

    std::atomic<bool> skip_writes;

    std::atomic<int> log_slow_searches_time_ms;

    std::atomic<bool> reset_peers_on_error;

    bool enable_search_analytics;

    uint32_t analytics_flush_interval;

    uint32_t housekeeping_interval;

    uint32_t db_compaction_interval;

    bool enable_lazy_filter;

    bool enable_search_logging;

    uint32_t max_per_page;
  
    uint16_t filter_by_max_ops;

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
        this->cache_num_entries = 1000;
        this->thread_pool_size = 0; // will be set dynamically if not overridden
        this->ssl_refresh_interval_seconds = 8 * 60 * 60;
        this->enable_access_logging = false;
        this->disk_used_max_percentage = 100;
        this->memory_used_max_percentage = 100;
        this->skip_writes = false;
        this->log_slow_searches_time_ms = 30 * 1000;
        this->reset_peers_on_error = false;

        this->enable_search_analytics = false;
        this->analytics_flush_interval = 3600;  // in seconds
        this->housekeeping_interval = 1800;     // in seconds
        this->db_compaction_interval = 0;     // in seconds, disabled

        this->enable_lazy_filter = false;

        this->enable_search_logging = false;
      
        this->max_per_page = 250;

        this->filter_by_max_ops = FILTER_BY_DEFAULT_OPERATIONS;
    }

    Config(Config const&) {

    }

public:

    static constexpr uint16_t FILTER_BY_DEFAULT_OPERATIONS = 100;

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

    void set_analytics_dir(const std::string& analytics_dir) {
        this->analytics_dir = analytics_dir;
    }

    void set_analytics_db_ttl(int32_t analytics_db_ttl) {
        this->analytics_db_ttl = analytics_db_ttl;
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

    void set_cache_num_entries(uint32_t cache_num_entries) {
        this->cache_num_entries = cache_num_entries;
    }

    void set_skip_writes(bool skip_writes) {
        this->skip_writes = skip_writes;
    }

    void set_reset_peers_on_error(bool reset_peers_on_error) {
        this->reset_peers_on_error = reset_peers_on_error;
    }

    void set_max_per_page(int max_per_page) {
        this->max_per_page = max_per_page;
    }

    // getters

    std::string get_data_dir() const {
        return this->data_dir;
    }

    std::string get_log_dir() const {
        return this->log_dir;
    }

    std::string get_analytics_dir() const {
        return this->analytics_dir;
    }

    int32_t get_analytics_db_ttl() const {
        return this->analytics_db_ttl;
    }

    std::string get_api_key() const {
        return this->api_key;
    }

    // @deprecated
    std::string get_search_only_api_key() const {
        return this->search_only_api_key;
    }

    std::string get_health_rusage_api_key() const {
        return this->health_rusage_api_key;
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

    const std::atomic<bool>& get_reset_peers_on_error() const {
        return reset_peers_on_error;
    }

    size_t get_num_collections_parallel_load() const {
        return this->num_collections_parallel_load;
    }

    size_t get_num_documents_parallel_load() const {
        return this->num_documents_parallel_load;
    }

    size_t get_cache_num_entries() const {
        return this->cache_num_entries;
    }

    size_t get_analytics_flush_interval() const {
        return this->analytics_flush_interval;
    }

    size_t get_housekeeping_interval() const {
        return this->housekeeping_interval;
    }

    size_t get_db_compaction_interval() const {
        return this->db_compaction_interval;
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

    bool get_enable_search_analytics() const {
        return this->enable_search_analytics;
    }

    bool get_enable_search_logging() const {
        return this->enable_search_logging;
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

    bool get_enable_lazy_filter() const {
        return enable_lazy_filter;
    }

    const std::atomic<bool>& get_skip_writes() const {
        return skip_writes;
    }

    int get_max_per_page() const {
        return this->max_per_page;
    }

    uint16_t get_filter_by_max_ops() const {
        return filter_by_max_ops;
    }

    // loaders

    std::string get_env(const char *name) {
        const char *ret = getenv(name);
        if (!ret) {
            return std::string();
        }
        return std::string(ret);
    }

    void load_config_env();

    void load_config_file(cmdline::parser & options);

    void load_config_cmd_args(cmdline::parser & options);

    void set_cors_domains(std::string& cors_domains_value) {
        std::vector<std::string> cors_values_vec;
        StringUtils::split(cors_domains_value, cors_values_vec, ",");
        cors_domains.clear();
        cors_domains.insert(cors_values_vec.begin(), cors_values_vec.end());
    }

    void set_enable_search_analytics(bool enable_search_analytics) {
        this->enable_search_analytics = enable_search_analytics;
    }

    void set_enable_search_logging(bool enable_search_logging) {
        this->enable_search_logging = enable_search_logging;
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

    static Option<std::string> fetch_file_contents(const std::string & file_path);

    static Option<std::string> fetch_nodes_config(const std::string& path_to_nodes);
};
