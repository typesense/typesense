#pragma once

#include <cmdline.h>
#include "option.h"
#include "string_utils.h"
#include "INIReader.h"

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
    std::string nodes;

    std::string master;

    std::string ssl_certificate;
    std::string ssl_certificate_key;

    bool enable_cors;

    float max_memory_ratio;
    int snapshot_interval_seconds;

    size_t catch_up_min_sequence_diff;
    size_t catch_up_threshold_percentage;

    std::string config_file;
    int config_file_validity;

    int log_slow_requests_time_ms;

protected:

    Config() {
        this->api_address = "0.0.0.0";
        this->api_port = 8108;
        this->peering_port = 8107;
        this->enable_cors = false;
        this->max_memory_ratio = 1.0f;
        this->snapshot_interval_seconds = 3600;
        this->catch_up_min_sequence_diff = 3000;
        this->catch_up_threshold_percentage = 95;
        this->log_slow_requests_time_ms = -1;
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

    std::string get_peering_address() const {
        return this->peering_address;
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

    int get_catch_up_min_sequence_diff() const {
        return this->catch_up_min_sequence_diff;
    }

    int get_catch_up_threshold_percentage() const {
        return this->catch_up_threshold_percentage;
    }

    int get_log_slow_requests_time_ms() const {
        return this->log_slow_requests_time_ms;
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
            this->api_address = get_env("TYPESENSE_LISTEN_ADDRESS");
        }

        if(!get_env("TYPESENSE_API_PORT").empty()) {
            this->api_port = std::stoi(get_env("TYPESENSE_API_PORT"));
        }

        if(!get_env("TYPESENSE_PEERING_ADDRESS").empty()) {
            this->api_address = get_env("TYPESENSE_PEERING_ADDRESS");
        }

        if(!get_env("TYPESENSE_PEERING_PORT").empty()) {
            this->peering_port = std::stoi(get_env("TYPESENSE_PEERING_PORT"));
        }

        this->nodes = get_env("TYPESENSE_NODES");

        this->master = get_env("TYPESENSE_MASTER");
        this->ssl_certificate = get_env("TYPESENSE_SSL_CERTIFICATE");
        this->ssl_certificate_key = get_env("TYPESENSE_SSL_CERTIFICATE_KEY");

        std::string enable_cors_str = get_env("TYPESENSE_ENABLE_CORS");
        StringUtils::toupper(enable_cors_str);
        this->enable_cors = ("TRUE" == enable_cors_str) ? true : false;

        if(!get_env("TYPESENSE_MAX_MEMORY_RATIO").empty()) {
            this->max_memory_ratio = std::stof(get_env("TYPESENSE_MAX_MEMORY_RATIO"));
        }

        if(!get_env("TYPESENSE_SNAPSHOT_INTERVAL_SECONDS").empty()) {
            this->snapshot_interval_seconds = std::stoi(get_env("TYPESENSE_SNAPSHOT_INTERVAL_SECONDS"));
        }

        if(!get_env("TYPESENSE_CATCH_UP_MIN_SEQUENCE_DIFF").empty()) {
            this->catch_up_min_sequence_diff = std::stoi(get_env("TYPESENSE_CATCH_UP_MIN_SEQUENCE_DIFF"));
        }

        if(!get_env("TYPESENSE_CATCH_UP_THRESHOLD_PERCENTAGE").empty()) {
            this->catch_up_threshold_percentage = std::stoi(get_env("TYPESENSE_CATCH_UP_THRESHOLD_PERCENTAGE"));
        }

        if(!get_env("TYPESENSE_LOG_SLOW_REQUESTS_TIME_MS").empty()) {
            this->log_slow_requests_time_ms = std::stoi(get_env("TYPESENSE_LOG_SLOW_REQUESTS_TIME_MS"));
        }
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
            this->enable_cors = reader.GetBoolean("server", "enable-cors", false);
        }

        if(reader.Exists("server", "peering-address")) {
            this->peering_address = reader.Get("server", "peering-address", "");
        }

        if(reader.Exists("server", "peering-port")) {
            this->peering_port = reader.GetInteger("server", "peering-port", 8107);
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

        if(reader.Exists("server", "catch-up-min-sequence-diff")) {
            this->catch_up_min_sequence_diff = (int) reader.GetInteger("server", "catch-up-min-sequence-diff", 3000);
        }

        if(reader.Exists("server", "catch-up-threshold-percentage")) {
            this->catch_up_threshold_percentage = (int) reader.GetInteger("server", "catch-up-threshold-percentage", 95);
        }

        if(reader.Exists("server", "log-slow-requests-time-ms")) {
            this->log_slow_requests_time_ms = (int) reader.GetInteger("server", "log-slow-requests-time-ms", -1);
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
            this->enable_cors = options.exist("enable-cors");
        }

        if(options.exist("peering-address")) {
            this->peering_address = options.get<std::string>("peering-address");
        }

        if(options.exist("peering-port")) {
            this->peering_port = options.get<uint32_t>("peering-port");
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

        if(options.exist("catch-up-min-sequence-diff")) {
            this->catch_up_min_sequence_diff = options.get<int>("catch-up-min-sequence-diff");
        }

        if(options.exist("catch-up-threshold-percentage")) {
            this->catch_up_threshold_percentage = options.get<int>("catch-up-threshold-percentage");
        }

        if(options.exist("log-slow-requests-time-ms")) {
            this->log_slow_requests_time_ms = options.exist("log-slow-requests-time-ms");
        }
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
};
