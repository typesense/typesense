#include <map>
#include <collection_manager.h>
#include <system_metrics.h>
#include "housekeeper.h"

void HouseKeeper::run() {
    uint64_t prev_remove_expired_keys_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    uint64_t prev_db_compaction_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    uint64_t prev_memory_usage_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    while(!quit) {
        std::unique_lock lk(mutex);
        cv.wait_for(lk, std::chrono::milliseconds(3050), [&] { return quit.load(); });

        if(quit) {
            lk.unlock();
            break;
        }

        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        // update system memory usage
        if (now_ts_seconds - prev_memory_usage_s >= memory_usage_interval_s) {
            active_memory_used = SystemMetrics::get_memory_active_bytes();
            prev_memory_usage_s = now_ts_seconds;
            log_bad_queries();
        }

        // perform compaction on underlying store if enabled
        if(Config::get_instance().get_db_compaction_interval() > 0) {
            if(now_ts_seconds - prev_db_compaction_s >= Config::get_instance().get_db_compaction_interval()) {
                LOG(INFO) << "Starting DB compaction.";
                CollectionManager::get_instance().get_store()->compact_all();
                LOG(INFO) << "Finished DB compaction.";
                prev_db_compaction_s = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
            }
        }

        if (now_ts_seconds - prev_remove_expired_keys_s >= remove_expired_keys_interval_s) {
            // Do housekeeping for authmanager
            CollectionManager::get_instance().getAuthManager().do_housekeeping();

            prev_remove_expired_keys_s = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
        }

        lk.unlock();
    }
}

void HouseKeeper::stop() {
    quit = true;
    cv.notify_all();
}

void HouseKeeper::init() {
}

uint64_t HouseKeeper::get_active_memory_used() {
    return active_memory_used;
}

void HouseKeeper::add_req(const std::shared_ptr<http_req>& req) {
    std::unique_lock ifq_lock(ifq_mutex);
    in_flight_queries.emplace(req->start_ts, req_metadata_t(req, get_active_memory_used()));
}

void HouseKeeper::remove_req(uint64_t req_id) {
    std::unique_lock ifq_lock(ifq_mutex);
    in_flight_queries.erase(req_id);
}

std::string HouseKeeper::get_query_log(const std::shared_ptr<http_req>& req) {
    std::string search_payload = req->body;
    StringUtils::erase_char(search_payload, '\n');
    std::string query_string = "?";
    for(const auto& param_kv: req->params) {
        if(param_kv.first != http_req::AUTH_HEADER && param_kv.first != http_req::USER_HEADER) {
            query_string += param_kv.first + "=" + param_kv.second + "&";
        }
    }

    return std::string("id=") + std::to_string(req->start_ts) + ", qs=" + query_string + ", body=" + search_payload;
}

void HouseKeeper::log_running_queries() {
    std::unique_lock ifq_lock(ifq_mutex);
    if(in_flight_queries.empty()) {
        LOG(INFO) << "No in-flight search queries were found.";
        return ;
    }

    LOG(INFO) << "Dump of in-flight search queries:";

    for(const auto& kv: in_flight_queries) {
        LOG(INFO) << get_query_log(kv.second.req);
    }
}

void HouseKeeper::log_bad_queries() {
    std::unique_lock ifq_lock(ifq_mutex);

    auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    for(auto& kv: in_flight_queries) {
        auto req_ts = kv.first;

        if(now_ts_seconds - req_ts < memory_req_min_age_s) {
            // since we use a map it's already ordered ascending on timestamp
            break;
        }

        if(kv.second.already_logged) {
            continue;
        }

        // query that's atleast 10 seconds old: check if memory difference exceeds 1 GB
        int64_t memory_req_start = kv.second.active_memory;
        int64_t curr_memory = active_memory_used;
        int64_t memory_diff = curr_memory - memory_req_start;
        const int64_t one_gb = 1073741824;

        if(memory_diff > one_gb) {
            LOG(INFO) << "Detected bad query, start_ts: " << req_ts << ", memory_diff: " << memory_diff
                      << ", " << get_query_log(kv.second.req);
            kv.second.already_logged = true;
        }
    }
}
