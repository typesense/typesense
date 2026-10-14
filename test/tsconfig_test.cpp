#include <gtest/gtest.h>
#include <stdlib.h>
#include <iostream>
#include <limits>
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
    ASSERT_EQ("/tmp/ca.pem", config.get_http_client_ca_certificate());
    ASSERT_EQ(100, config.get_max_query_len());
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
    ASSERT_EQ("abcd", config.get_api_key());  // cli parameter curations file config
    ASSERT_EQ(1, config.get_cors_domains().size());  // cli parameter curations file config
    ASSERT_EQ("http://localhost:8108", *(config.get_cors_domains().begin()));
    ASSERT_EQ(250, config.get_max_per_page());
    ASSERT_EQ(99, config.get_max_group_limit());
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

namespace {
struct RestoreMaxQueryLengthEnv {
    const bool present = std::getenv("TYPESENSE_MAX_QUERY_LEN") != nullptr;
    const std::string value = present ? std::getenv("TYPESENSE_MAX_QUERY_LEN") : "";
    ~RestoreMaxQueryLengthEnv() {
        if(present) {
            setenv("TYPESENSE_MAX_QUERY_LEN", value.c_str(), 1);
        } else {
            unsetenv("TYPESENSE_MAX_QUERY_LEN");
        }
    }
};
}

TEST(ConfigTest, MaxQueryLengthDefaultsAndPrecedence) {
    RestoreMaxQueryLengthEnv restore;
    unsetenv("TYPESENSE_MAX_QUERY_LEN");
    ConfigImpl config;
    config.load_config_env();
    EXPECT_EQ(0, config.get_max_query_len());
    setenv("TYPESENSE_MAX_QUERY_LEN", "200", 1);
    config.load_config_env();
    EXPECT_EQ(200, config.get_max_query_len());

    std::vector<std::string> args = {
        "./typesense-server", "--data-dir=/tmp/ts", "--api-key=abcd", "--shutdown-delay-seconds=0", "--max-query-len=0",
        std::string("--config=") + ROOT_DIR + "test/valid_config.ini"
    };
    auto argv = get_argv(args);
    cmdline::parser options;
    init_cmdline_options(options, argv.size() - 1, argv.data());
    ASSERT_TRUE(options.parse(argv.size() - 1, argv.data())) << options.error_full();
    config.load_config_file(options);
    EXPECT_EQ(100, config.get_max_query_len());
    config.load_config_cmd_args(options);
    EXPECT_EQ(0, config.get_max_query_len());
    EXPECT_TRUE(config.is_valid().ok());

    // An absent higher-priority value must not replace the environment setting.
    args = {"./typesense-server", "--data-dir=/tmp/ts", "--api-key=abcd", "--shutdown-delay-seconds=0", std::string("--config=") + ROOT_DIR + "test/valid_sparse_config.ini"};
    argv = get_argv(args);
    cmdline::parser sparse_options;
    init_cmdline_options(sparse_options, argv.size() - 1, argv.data());
    ASSERT_TRUE(sparse_options.parse(argv.size() - 1, argv.data())) << sparse_options.error_full();
    config.load_config_env();
    config.load_config_file(sparse_options);
    config.load_config_cmd_args(sparse_options);
    EXPECT_EQ(200, config.get_max_query_len());
}

TEST(ConfigTest, MaxQueryLengthRejectsMalformedEnvironmentAndCliValues) {
    RestoreMaxQueryLengthEnv restore;
    for(const std::string value : {"", "-1", "+1", " 1", "1 ", "1.5", "1x", "18446744073709551616"}) {
        SCOPED_TRACE(value);
        setenv("TYPESENSE_MAX_QUERY_LEN", value.c_str(), 1);
        ConfigImpl config;
        config.load_config_env();
        auto validation = config.is_valid();
        ASSERT_FALSE(validation.ok());
        EXPECT_EQ("Invalid value for `max-query-len`; expected a non-negative integer.", validation.error());

        std::vector<std::string> args = {"./typesense-server", "--data-dir=/tmp/ts", "--api-key=abcd", "--shutdown-delay-seconds=0", "--max-query-len=" + value};
        auto argv = get_argv(args);
        cmdline::parser options;
        init_cmdline_options(options, argv.size() - 1, argv.data());
        ASSERT_TRUE(options.parse(argv.size() - 1, argv.data())) << options.error_full();
        ConfigImpl cli_config;
        cli_config.load_config_cmd_args(options);
        validation = cli_config.is_valid();
        ASSERT_FALSE(validation.ok());
        EXPECT_EQ("Invalid value for `max-query-len`; expected a non-negative integer.", validation.error());
    }
}

TEST(ConfigTest, MaxQueryLengthValidOverrideAndUint64Boundary) {
    RestoreMaxQueryLengthEnv restore;
    setenv("TYPESENSE_MAX_QUERY_LEN", "invalid", 1);
    ConfigImpl config;
    config.load_config_env();
    EXPECT_FALSE(config.is_valid().ok());
    std::vector<std::string> args = {
        "./typesense-server", "--data-dir=/tmp/ts", "--api-key=abcd", "--shutdown-delay-seconds=0", "--max-query-len=18446744073709551615",
        std::string("--config=") + ROOT_DIR + "test/valid_config.ini"
    };
    auto argv = get_argv(args);
    cmdline::parser options;
    init_cmdline_options(options, argv.size() - 1, argv.data());
    ASSERT_TRUE(options.parse(argv.size() - 1, argv.data())) << options.error_full();
    config.load_config_file(options);
    ASSERT_TRUE(config.is_valid().ok());
    EXPECT_EQ(100, config.get_max_query_len());
    config.load_config_cmd_args(options);
    ASSERT_TRUE(config.is_valid().ok());
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), config.get_max_query_len());
    // Unknown runtime settings are ignored by the existing update endpoint.
    EXPECT_TRUE(config.update_config(nlohmann::json{{"max-query-len", 1}}).ok());
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), config.get_max_query_len());
}
