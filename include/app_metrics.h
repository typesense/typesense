#pragma once

#include "sparsepp.h"
#include "json.hpp"
#include "logger.h"
#include <string>

class AppMetrics {
private:
    // stores last complete window
    spp::sparse_hash_map<std::string, uint64_t>* counts;
    spp::sparse_hash_map<std::string, uint64_t>* durations;

    // stores the current window
    spp::sparse_hash_map<std::string, uint64_t>* current_counts;
    spp::sparse_hash_map<std::string, uint64_t>* current_durations;

    AppMetrics() {
        current_counts = new spp::sparse_hash_map<std::string, uint64_t>();
        counts = new spp::sparse_hash_map<std::string, uint64_t>();

        current_durations = new spp::sparse_hash_map<std::string, uint64_t>();
        durations = new spp::sparse_hash_map<std::string, uint64_t>();
    }

    ~AppMetrics() {
        delete current_counts;
        delete counts;

        delete current_durations;
        delete durations;
    }

public:

    static const uint64_t METRICS_REFRESH_INTERVAL_MS = 10 * 1000;

    static AppMetrics & get_instance() {
        static AppMetrics instance;
        return instance;
    }

    AppMetrics(AppMetrics const&) = delete;
    void operator=(AppMetrics const&) = delete;

    void increment_count(const std::string& identifier, uint64_t count) {
        (*current_counts)[identifier] += count;
    }

    void increment_duration(const std::string& identifier, uint64_t duration) {
        (*current_durations)[identifier] += duration;
    }

    void window_reset() {
        delete counts;
        counts = current_counts;
        current_counts = new spp::sparse_hash_map<std::string, uint64_t>();

        delete durations;
        durations = current_durations;
        current_durations = new spp::sparse_hash_map<std::string, uint64_t>();
    }

    void get(const std::string& count_key, const std::string& latency_key, nlohmann::json &result) {
        result[count_key] = nlohmann::json::object();
        for(const auto& kv: *counts) {
            result[count_key][kv.first] = (double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000));
        }

        result[latency_key] = nlohmann::json::object();
        for(const auto& kv: *durations) {
            auto counter_it = counts->find(kv.first);
            if(counter_it != counts->end() && counter_it->second != 0) {
                result[latency_key][kv.first] = (double(kv.second) / counter_it->second);
            }
        }
    }
};