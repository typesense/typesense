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

        size_t write_lag = req_json["healthy-write-lag"].get<int>();
        if(write_lag <= 0) {
            return Option<bool>(400, "Configuration `healthy-write-lag` must be a positive integer.");
        }

        set_healthy_write_lag(write_lag);
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