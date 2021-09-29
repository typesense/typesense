#pragma once

#include "sparsepp.h"
#include "json.hpp"
#include "logger.h"
#include <string>
#include <shared_mutex>

class AppMetrics {
private:
    mutable std::shared_mutex mutex;

    static inline const std::string SEARCH_LABEL = "search";

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
        std::unique_lock lock(mutex);
        (*current_counts)[identifier] += count;
    }

    void increment_search_count(uint64_t count) {
        std::unique_lock lock(mutex);
        (*current_counts)[SEARCH_LABEL] += count;
    }

    void increment_duration(const std::string& identifier, uint64_t duration) {
        std::unique_lock lock(mutex);
        (*current_durations)[identifier] += duration;
    }

    void increment_search_duration(uint64_t duration) {
        std::unique_lock lock(mutex);
        (*current_durations)[SEARCH_LABEL] += duration;
    }

    void window_reset() {
        std::unique_lock lock(mutex);

        delete counts;
        counts = current_counts;
        current_counts = new spp::sparse_hash_map<std::string, uint64_t>();

        delete durations;
        durations = current_durations;
        current_durations = new spp::sparse_hash_map<std::string, uint64_t>();
    }

    void get(const std::string& rps_key, const std::string& latency_key, nlohmann::json &result) const {
        std::shared_lock lock(mutex);

        uint64_t total_counts = 0;
        auto SEARCH_RPS_KEY = SEARCH_LABEL + "_" + rps_key;
        auto SEARCH_LATENCY_KEY = SEARCH_LABEL + "_" + latency_key;

        result[rps_key] = nlohmann::json::object();
        for(const auto& kv: *counts) {
            if(kv.first == SEARCH_LABEL) {
                result[SEARCH_RPS_KEY] = double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000);
            } else {
                result[rps_key][kv.first] = (double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000));
                total_counts += kv.second;
            }
        }

        result["total_" + rps_key] = double(total_counts) / (METRICS_REFRESH_INTERVAL_MS / 1000);

        result[latency_key] = nlohmann::json::object();

        for(const auto& kv: *durations) {
            auto counter_it = counts->find(kv.first);
            if(counter_it != counts->end() && counter_it->second != 0) {
                if(kv.first == SEARCH_LABEL) {
                    result[SEARCH_LATENCY_KEY] = (double(kv.second) / counter_it->second);
                } else {
                    result[latency_key][kv.first] = (double(kv.second) / counter_it->second);
                }
            }
        }

        if(!result.contains(SEARCH_RPS_KEY)) {
            result[SEARCH_RPS_KEY] = 0;
        }

        if(!result.contains(SEARCH_LATENCY_KEY)) {
            result[SEARCH_LATENCY_KEY] = 0;
        }
    }
};