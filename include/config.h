#pragma once

#include <cmdline.h>
#include "option.h"
#include "string_utils.h"

class Config {
private:
    std::string data_dir;
    std::string log_dir;

    std::string api_key;
    std::string search_only_api_key;

    std::string listen_address;
    uint32_t listen_port;

    std::string master;

    std::string ssl_certificate;
    std::string ssl_certificate_key;

    bool enable_cors;

public:

    Config() {
        this->listen_address = "0.0.0.0";
        this->listen_port = 8108;
        this->enable_cors = false;
    }

    // setters

    void set_data_dir(std::string & data_dir) {
        this->data_dir = data_dir;
    }

    void set_log_dir(std::string & log_dir) {
        this->log_dir = log_dir;
    }

    void set_api_key(std::string & api_key) {
        this->api_key = api_key;
    }

    void set_search_only_api_key(std::string & search_only_api_key) {
        this->search_only_api_key = search_only_api_key;
    }

    void set_listen_address(std::string & listen_address) {
        this->listen_address = listen_address;
    }

    void set_listen_port(int listen_port) {
        this->listen_port = listen_port;
    }

    void set_master(std::string & master) {
        this->master = master;
    }

    void set_ssl_cert(std::string & ssl_cert) {
        this->ssl_certificate = ssl_cert;
    }

    void set_ssl_cert_key(std::string & ssl_cert_key) {
        this->ssl_certificate_key = ssl_cert_key;
    }

    void set_enable_cors(bool enable_cors) {
        this->enable_cors = enable_cors;
    }
    
    // getters

    std::string get_data_dir() {
        return this->data_dir;
    }

    std::string get_log_dir() {
        return this->log_dir;
    }

    std::string get_api_key() {
        return this->api_key;
    }

    std::string get_search_only_api_key() {
        return this->search_only_api_key;
    }

    std::string get_listen_address() {
        return this->listen_address;
    }

    int get_listen_port() {
        return this->listen_port;
    }

    std::string get_master() {
        return this->master;
    }

    std::string get_ssl_cert() {
        return this->ssl_certificate;
    }

    std::string get_ssl_cert_key() {
        return this->ssl_certificate_key;
    }

    bool get_enable_cors() {
        return this->enable_cors;
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

        this->master = get_env("TYPESENSE_MASTER");
        this->ssl_certificate = get_env("TYPESENSE_SSL_CERTIFICATE");
        this->ssl_certificate_key = get_env("TYPESENSE_SSL_CERTIFICATE_KEY");

        std::string enable_cors_str = get_env("TYPESENSE_ENABLE_CORS");
        StringUtils::toupper(enable_cors_str);
        this->enable_cors = ("TRUE" == enable_cors_str) ? true : false;
    }

    void load_config_cmd_args(cmdline::parser & options) {
        this->data_dir = options.get<std::string>("data-dir");
        this->log_dir = options.get<std::string>("log-dir");
        this->api_key = options.get<std::string>("api-key");

        this->search_only_api_key = options.get<std::string>("search-only-api-key");

        if(options.exist("listen-address")) {
            this->listen_address = options.get<std::string>("listen-address");
        }

        if(options.exist("listen-port")) {
            this->listen_port = options.get<uint32_t>("listen-port");
        }

        this->master = options.get<std::string>("master");
        this->ssl_certificate = options.get<std::string>("ssl-certificate");
        this->ssl_certificate_key = options.get<std::string>("ssl-certificate-key");

        this->enable_cors = options.exist("enable-cors");
    }

    // validation

    Option<bool> is_valid() {
        if(data_dir.empty()) {
            return Option<bool>(404, "Data directory is not specified.");
        }

        if(api_key.empty()) {
            return Option<bool>(404, "API key is not specified.");
        }

        return Option<bool>(true);
    }

};