#include "app_metrics.h"
#include "core_api.h"

void AppMetrics::increment_write_metrics(uint64_t route_hash, uint64_t duration) {
    if(is_doc_import_route(route_hash)) {
        AppMetrics::get_instance().increment_duration(AppMetrics::IMPORT_LABEL, duration);
        AppMetrics::get_instance().increment_count(AppMetrics::IMPORT_LABEL, 1);
    }

    else if(is_doc_write_route(route_hash)) {
        AppMetrics::get_instance().increment_duration(AppMetrics::DOC_WRITE_LABEL, duration);
        AppMetrics::get_instance().increment_count(AppMetrics::DOC_WRITE_LABEL, 1);
    }

    else if(is_doc_del_route(route_hash)) {
        AppMetrics::get_instance().increment_duration(AppMetrics::DOC_DELETE_LABEL, duration);
        AppMetrics::get_instance().increment_count(AppMetrics::DOC_DELETE_LABEL, 1);
    }
}

void AppMetrics::get(const std::string& rps_key, const std::string& latency_key, nlohmann::json& result) const {
    std::shared_lock lock(mutex);

    uint64_t total_counts = 0;

    auto SEARCH_RPS_KEY = SEARCH_LABEL + "_" + rps_key;
    auto SEARCH_LATENCY_KEY = SEARCH_LABEL + "_" + latency_key;

    auto IMPORT_RPS_KEY = IMPORT_LABEL + "_" + rps_key;
    auto IMPORT_LATENCY_KEY = IMPORT_LABEL + "_" + latency_key;

    auto DOC_WRITE_RPS_KEY = DOC_WRITE_LABEL + "_" + rps_key;
    auto DOC_WRITE_LATENCY_KEY = DOC_WRITE_LABEL + "_" + latency_key;

    auto DOC_DELETE_RPS_KEY = DOC_DELETE_LABEL + "_" + rps_key;
    auto DOC_DELETE_LATENCY_KEY = DOC_DELETE_LABEL + "_" + latency_key;

    auto OVERLOADED_RPS_KEY = OVERLOADED_LABEL + "_" + rps_key;

    result[rps_key] = nlohmann::json::object();
    for(const auto& kv: *counts) {
        if(kv.first == SEARCH_LABEL) {
            result[SEARCH_RPS_KEY] = double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000);
        }

        else if(kv.first == IMPORT_LABEL) {
            result[IMPORT_RPS_KEY] = double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000);
        }

        else if(kv.first == DOC_WRITE_LABEL) {
            result[DOC_WRITE_RPS_KEY] = double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000);
        }

        else if(kv.first == DOC_DELETE_LABEL) {
            result[DOC_DELETE_RPS_KEY] = double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000);
        }

        else if(kv.first == OVERLOADED_LABEL) {
            result[OVERLOADED_RPS_KEY] = double(kv.second) / (METRICS_REFRESH_INTERVAL_MS / 1000);
        }

        else {
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
            }

            else if(kv.first == IMPORT_LABEL) {
                result[IMPORT_LATENCY_KEY] = (double(kv.second) / counter_it->second);
            }

            else if(kv.first == DOC_WRITE_LABEL) {
                result[DOC_WRITE_LATENCY_KEY] = (double(kv.second) / counter_it->second);
            }

            else if(kv.first == DOC_DELETE_LABEL) {
                result[DOC_DELETE_LATENCY_KEY] = (double(kv.second) / counter_it->second);
            }

            else {
                result[latency_key][kv.first] = (double(kv.second) / counter_it->second);
            }
        }
    }

    std::vector<std::string> keys_to_check = {
        SEARCH_RPS_KEY, IMPORT_RPS_KEY, DOC_WRITE_RPS_KEY, DOC_DELETE_RPS_KEY,
        SEARCH_LATENCY_KEY, IMPORT_LATENCY_KEY, DOC_WRITE_LATENCY_KEY, DOC_DELETE_LATENCY_KEY,
        OVERLOADED_RPS_KEY
    };

    for(auto& key: keys_to_check) {
        if(!result.contains(key)) {
            result[key] = 0;
        }
    }
}

void AppMetrics::window_reset() {
    std::unique_lock lock(mutex);

    delete counts;
    counts = current_counts;
    current_counts = new spp::sparse_hash_map<std::string, uint64_t>();

    delete durations;
    durations = current_durations;
    current_durations = new spp::sparse_hash_map<std::string, uint64_t>();
}

void AppMetrics::write_access_log(const uint64_t epoch_millis, const char* remote_ip, const std::string& path) {
    if(!access_log_path.empty()) {
        access_log << epoch_millis << "\t" << remote_ip << "\t" << path << "\n";
    }
}

void AppMetrics::flush_access_log() {
    if(!access_log_path.empty()) {
        access_log << std::flush;
    }
}
