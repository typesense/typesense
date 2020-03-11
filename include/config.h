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
    std::string search_only_api_key;

    std::string listen_address;
    uint32_t listen_port;

    std::string raft_dir;
    uint32_t raft_port;
    std::string raft_peers;

    std::string master;

    std::string ssl_certificate;
    std::string ssl_certificate_key;

    bool enable_cors;

    size_t indices_per_collection;

    std::string config_file;
    int config_file_validity;

public:

    Config() {
        this->listen_address = "0.0.0.0";
        this->listen_port = 8108;
        this->raft_port = 8107;
        this->enable_cors = false;
        this->indices_per_collection = 4;
    }

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

    void set_search_only_api_key(const std::string & search_only_api_key) {
        this->search_only_api_key = search_only_api_key;
    }

    void set_listen_address(const std::string & listen_address) {
        this->listen_address = listen_address;
    }

    void set_listen_port(int listen_port) {
        this->listen_port = listen_port;
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

    void set_indices_per_collection(size_t indices_per_collection) {
        this->indices_per_collection  = indices_per_collection;
    }

    void set_raft_port(int raft_port) {
        this->raft_port = raft_port;
    }

    void set_raft_dir(const std::string & raft_dir) {
        this->raft_dir = raft_dir;
    }

    void set_raft_peers(const std::string & raft_peers) {
        this->raft_peers = raft_peers;
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

    std::string get_search_only_api_key() const {
        return this->search_only_api_key;
    }

    std::string get_listen_address() const {
        return this->listen_address;
    }

    int get_listen_port() const {
        return this->listen_port;
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

    size_t get_indices_per_collection() const {
        return indices_per_collection;
    }

    int get_raft_port() const {
        return this->raft_port;
    }

    std::string get_raft_dir() const {
        return this->raft_dir;
    }

    std::string get_raft_peers() const {
        return this->raft_peers;
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

        this->search_only_api_key = get_env("TYPESENSE_SEARCH_ONLY_API_KEY");

        if(!get_env("TYPESENSE_LISTEN_ADDRESS").empty()) {
            this->listen_address = get_env("TYPESENSE_LISTEN_ADDRESS");
        }

        if(!get_env("TYPESENSE_LISTEN_PORT").empty()) {
            this->listen_port = std::stoi(get_env("TYPESENSE_LISTEN_PORT"));
        }

        if(!get_env("TYPESENSE_RAFT_PORT").empty()) {
            this->raft_port = std::stoi(get_env("TYPESENSE_RAFT_PORT"));
        }

        this->raft_dir = get_env("TYPESENSE_RAFT_DIR");
        this->raft_peers = get_env("TYPESENSE_RAFT_PEERS");

        this->master = get_env("TYPESENSE_MASTER");
        this->ssl_certificate = get_env("TYPESENSE_SSL_CERTIFICATE");
        this->ssl_certificate_key = get_env("TYPESENSE_SSL_CERTIFICATE_KEY");

        std::string enable_cors_str = get_env("TYPESENSE_ENABLE_CORS");
        StringUtils::toupper(enable_cors_str);
        this->enable_cors = ("TRUE" == enable_cors_str) ? true : false;
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

        if(reader.Exists("server", "search-only-api-key")) {
            this->search_only_api_key = reader.Get("server", "search-only-api-key", "");
        }

        if(reader.Exists("server", "listen-address")) {
            this->listen_address = reader.Get("server", "listen-address", "");
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
            this->listen_port = reader.GetInteger("server", "listen-port", 8108);
        }

        if(reader.Exists("server", "enable-cors")) {
            this->enable_cors = reader.GetBoolean("server", "enable-cors", false);
        }

        if(reader.Exists("server", "raft-port")) {
            this->raft_port = reader.GetInteger("server", "raft-port", 8107);
        }

        if(reader.Exists("server", "raft-dir")) {
            this->raft_dir = reader.Get("server", "raft-dir", "");
        }

        if(reader.Exists("server", "raft-peers")) {
            this->raft_peers = reader.Get("server", "raft-peers", "");
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

        if(options.exist("search-only-api-key")) {
            this->search_only_api_key = options.get<std::string>("search-only-api-key");
        }

        if(options.exist("listen-address")) {
            this->listen_address = options.get<std::string>("listen-address");
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
            this->listen_port = options.get<uint32_t>("listen-port");
        }

        if(options.exist("enable-cors")) {
            this->enable_cors = options.exist("enable-cors");
        }

        if(options.exist("raft-port")) {
            this->raft_port = options.get<uint32_t>("raft-port");
        }

        if(options.exist("raft-dir")) {
            this->raft_dir = options.get<std::string>("raft-dir");
        }

        if(options.exist("raft-peers")) {
            this->raft_peers = options.get<std::string>("raft-peers");
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

        if(!raft_dir.empty() && raft_peers.empty()) {
            return Option<bool>(500, "Argument --raft-peers is not specified.");
        }

        if(!raft_peers.empty() && raft_dir.empty()) {
            return Option<bool>(500, "Argument --raft-dir is not specified.");
        }

        return Option<bool>(true);
    }
};
