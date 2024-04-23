#include <gtest/gtest.h>
#include <stdlib.h>
#include <iostream>
#include <cmdline.h>
#include "typesense_server_utils.h"
#include "tsconfig.h"

std::vector<char*> get_argv(std::vector<std::string> & args) {
    std::vector<char*> argv;
    for (const auto& arg : args) {
        argv.push_back((char*)arg.data());
    }

    argv.push_back(nullptr);
    return argv;
}

class ConfigImpl : public Config {
public:
    ConfigImpl(): Config() {

    }
};

TEST(ConfigTest, LoadCmdLineArguments) {
    cmdline::parser options;

    std::vector<std::string> args = {
        "./typesense-server",
        "--data-dir=/tmp/data",
        "--api-key=abcd",
        "--listen-port=8080",
        "--max-per-page=250",
    };

    std::vector<char*> argv = get_argv(args);

    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    ConfigImpl config;
    config.load_config_cmd_args(options);

    ASSERT_EQ("abcd", config.get_api_key());
    ASSERT_EQ(8080, config.get_api_port());
    ASSERT_EQ("/tmp/data", config.get_data_dir());
    ASSERT_EQ(true, config.get_enable_cors());
}

TEST(ConfigTest, LoadEnvVars) {
    cmdline::parser options;
    putenv((char*)"TYPESENSE_DATA_DIR=/tmp/ts");
    putenv((char*)"TYPESENSE_LISTEN_PORT=9090");
    ConfigImpl config;
    config.load_config_env();

    ASSERT_EQ("/tmp/ts", config.get_data_dir());
    ASSERT_EQ(9090, config.get_api_port());
}

TEST(ConfigTest, BadConfigurationReturnsError) {
    ConfigImpl config1;
    config1.set_api_key("abcd");
    auto validation = config1.is_valid();

    ASSERT_EQ(false, validation.ok());
    ASSERT_EQ("Data directory is not specified.", validation.error());

    ConfigImpl config2;
    config2.set_data_dir("/tmp/ts");
    validation = config2.is_valid();

    ASSERT_EQ(false, validation.ok());
    ASSERT_EQ("API key is not specified.", validation.error());
}

TEST(ConfigTest, LoadConfigFile) {
    cmdline::parser options;

    std::vector<std::string> args = {
        "./typesense-server",
        std::string("--config=") + std::string(ROOT_DIR)+"test/valid_config.ini"
    };
    std::vector<char*> argv = get_argv(args);
    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    ConfigImpl config;
    config.load_config_file(options);

    auto validation = config.is_valid();
    ASSERT_EQ(true, validation.ok());

    ASSERT_EQ("/tmp/ts", config.get_data_dir());
    ASSERT_EQ("1234", config.get_api_key());
    ASSERT_EQ("/tmp/logs", config.get_log_dir());
    ASSERT_EQ(9090, config.get_api_port());
    ASSERT_EQ(true, config.get_enable_cors());
}

TEST(ConfigTest, LoadIncompleteConfigFile) {
    cmdline::parser options;

    std::vector<std::string> args = {
            "./typesense-server",
            std::string("--config=") + std::string(ROOT_DIR)+"test/valid_sparse_config.ini"
    };
    std::vector<char*> argv = get_argv(args);
    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    ConfigImpl config;

    auto validation = config.is_valid();

    ASSERT_EQ(false, validation.ok());
    ASSERT_EQ("Data directory is not specified.", validation.error());
}

TEST(ConfigTest, LoadBadConfigFile) {
    cmdline::parser options;

    std::vector<std::string> args = {
            "./typesense-server",
            std::string("--config=") + std::string(ROOT_DIR)+"test/bad_config.ini"
    };
    std::vector<char*> argv = get_argv(args);
    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    ConfigImpl config;
    config.load_config_file(options);

    auto validation = config.is_valid();
    ASSERT_EQ(false, validation.ok());
    ASSERT_EQ("Error parsing the configuration file.", validation.error());
}

TEST(ConfigTest, CmdLineArgsOverrideConfigFileAndEnvVars) {
    cmdline::parser options;

    std::vector<std::string> args = {
        "./typesense-server",
        "--data-dir=/tmp/data",
        "--api-key=abcd",
        "--listen-address=192.168.10.10",
        "--cors-domains=http://localhost:8108",
        "--max-per-page=250",
        std::string("--config=") + std::string(ROOT_DIR)+"test/valid_sparse_config.ini"
    };

    putenv((char*)"TYPESENSE_DATA_DIR=/tmp/ts");
    putenv((char*)"TYPESENSE_LOG_DIR=/tmp/ts_log");
    putenv((char*)"TYPESENSE_LISTEN_PORT=9090");
    putenv((char*)"TYPESENSE_LISTEN_ADDRESS=127.0.0.1");
    putenv((char*)"TYPESENSE_ENABLE_CORS=TRUE");
    putenv((char*)"TYPESENSE_CORS_DOMAINS=http://localhost:7108");

    std::vector<char*> argv = get_argv(args);
    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    ConfigImpl config;
    config.load_config_env();
    config.load_config_file(options);
    config.load_config_cmd_args(options);

    ASSERT_EQ("abcd", config.get_api_key());
    ASSERT_EQ("/tmp/data", config.get_data_dir());
    ASSERT_EQ("/tmp/ts_log", config.get_log_dir());
    ASSERT_EQ(9090, config.get_api_port());
    ASSERT_EQ(true, config.get_enable_cors());
    ASSERT_EQ("192.168.10.10", config.get_api_address());
    ASSERT_EQ("abcd", config.get_api_key());  // cli parameter overrides file config
    ASSERT_EQ(1, config.get_cors_domains().size());  // cli parameter overrides file config
    ASSERT_EQ("http://localhost:8108", *(config.get_cors_domains().begin()));
    ASSERT_EQ(250, config.get_max_per_page());
}

TEST(ConfigTest, CorsDefaults) {
    cmdline::parser options;

    std::vector<std::string> args = {
        "./typesense-server",
        "--data-dir=/tmp/data",
        "--api-key=abcd",
        "--listen-address=192.168.10.10",
        "--max-per-page=250",
        std::string("--config=") + std::string(ROOT_DIR)+"test/valid_sparse_config.ini"
    };

    std::vector<char*> argv = get_argv(args);
    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    ConfigImpl config;
    config.load_config_cmd_args(options);

    ASSERT_EQ(true, config.get_enable_cors());
    ASSERT_EQ(0, config.get_cors_domains().size());

    unsetenv("TYPESENSE_ENABLE_CORS");
    unsetenv("TYPESENSE_CORS_DOMAINS");

    ConfigImpl config2;
    config2.load_config_env();

    ASSERT_EQ(true, config2.get_enable_cors());
    ASSERT_EQ(0, config2.get_cors_domains().size());

    ConfigImpl config3;
    config3.load_config_file(options);

    ASSERT_EQ(true, config3.get_enable_cors());
    ASSERT_EQ(1, config3.get_cors_domains().size());
}
