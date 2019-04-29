#include <gtest/gtest.h>
#include <stdlib.h>
#include <cmdline.h>
#include "typesense_server_utils.h"
#include "config.h"

std::vector<char*> get_argv(std::vector<std::string> & args) {
    std::vector<char*> argv;
    for (const auto& arg : args)
        argv.push_back((char*)arg.data());
    argv.push_back(nullptr);

    return argv;
}

TEST(ConfigTest, LoadCmdLineArguments) {
    cmdline::parser options;

    std::vector<std::string> args = {
        "./typesense-server",
        "--data-dir=/tmp/data",
        "--api-key=abcd",
        "--listen-port=8080",
    };

    std::vector<char*> argv = get_argv(args);

    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    Config config;
    config.load_config_cmd_args(options);

    ASSERT_EQ("abcd", config.get_api_key());
    ASSERT_EQ(8080, config.get_listen_port());
    ASSERT_EQ("/tmp/data", config.get_data_dir());
}

TEST(ConfigTest, LoadEnvVars) {
    cmdline::parser options;
    putenv((char*)"TYPESENSE_DATA_DIR=/tmp/ts");
    putenv((char*)"TYPESENSE_LISTEN_PORT=9090");
    Config config;
    config.load_config_env();

    ASSERT_EQ("/tmp/ts", config.get_data_dir());
    ASSERT_EQ(9090, config.get_listen_port());
}

TEST(ConfigTest, CmdLineArgsOverrideEnvVars) {
    cmdline::parser options;

    std::vector<std::string> args = {
            "./typesense-server",
            "--data-dir=/tmp/data",
            "--api-key=abcd"
    };

    putenv((char*)"TYPESENSE_DATA_DIR=/tmp/ts");
    putenv((char*)"TYPESENSE_LISTEN_PORT=9090");

    std::vector<char*> argv = get_argv(args);

    init_cmdline_options(options, argv.size() - 1, argv.data());
    options.parse(argv.size() - 1, argv.data());

    Config config;
    config.load_config_env();
    config.load_config_cmd_args(options);

    ASSERT_EQ("abcd", config.get_api_key());
    ASSERT_EQ("/tmp/data", config.get_data_dir());
    ASSERT_EQ(9090, config.get_listen_port());
}

TEST(ConfigTest, BadConfigurationReturnsError) {
    Config config1;
    config1.set_api_key("abcd");
    auto validation = config1.is_valid();

    ASSERT_EQ(false, validation.ok());
    ASSERT_EQ("Data directory is not specified.", validation.error());

    Config config2;
    config2.set_data_dir("/tmp/ts");
    validation = config2.is_valid();

    ASSERT_EQ(false, validation.ok());
    ASSERT_EQ("API key is not specified.", validation.error());
}
