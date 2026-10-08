#include <map>
#include <collection_manager.h>
#include <system_metrics.h>
#include <cached_resource_stat.h>
#include "housekeeper.h"

namespace {
// Set while the current thread holds `ifq_mutex`, so that a crash handler running on the
// same thread does not try to re-acquire the mutex and deadlock.
thread_local bool holds_ifq_mutex = false;

class ifq_lock_t {
    std::unique_lock<std::timed_mutex> lock;
public:
    explicit ifq_lock_t(std::timed_mutex& mutex): lock(mutex) {
        holds_ifq_mutex = true;
    }

    ~ifq_lock_t() {
        holds_ifq_mutex = false;
    }
};
}

void HouseKeeper::run() {
    auto next_purge_at = std::chrono::steady_clock::time_point::min();
    uint64_t prev_remove_expired_keys_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    uint64_t prev_db_compaction_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    uint64_t prev_memory_usage_s = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    while(!quit) {
        std::unique_lock lk(mutex);
        cv.wait_for(lk, std::chrono::milliseconds(3050), [&] {
            return quit.load() || (purge_requested.load() && std::chrono::steady_clock::now() >= next_purge_at);
        });

        if(quit) {
            lk.unlock();
            break;
        }

        // Cache invalidation can cause another request immediately. Keep the purge
        // cooldown independent of the resource cache, and retain requests until it expires.
        if(std::chrono::steady_clock::now() >= next_purge_at && purge_requested.exchange(false)) {
            lk.unlock();
            log_running_queries();
            SystemMetrics::get_instance().purge_jemalloc_unused_memory();
            cached_resource_stat_t::get_instance().invalidate_cache();
            next_purge_at = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            lk.lock();
        }

        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        // update system memory usage
        if (now_ts_seconds - prev_memory_usage_s >= memory_usage_interval_s) {
            active_memory_used = SystemMetrics::get_instance().get_proc_memory_active_bytes();
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

void HouseKeeper::request_memory_purge() {
    if(!purge_requested.exchange(true)) {
        cv.notify_one();
    }
}

void HouseKeeper::init() {
}

uint64_t HouseKeeper::get_active_memory_used() {
    return active_memory_used;
}

void HouseKeeper::add_req(const std::shared_ptr<http_req>& req) {
    // must be built here, on the request's own thread, before the search starts mutating `req->params`
    req_metadata_t req_metadata(get_query_log(req), get_active_memory_used());
    ifq_lock_t ifq_lock(ifq_mutex);
    in_flight_queries.emplace(req->start_ts, std::move(req_metadata));
}

void HouseKeeper::remove_req(uint64_t req_id) {
    ifq_lock_t ifq_lock(ifq_mutex);
    in_flight_queries.erase(req_id);
}

std::string HouseKeeper::get_query_log(const std::shared_ptr<http_req>& req) {
    std::string search_payload = req->body.substr(0, MAX_QUERY_LOG_BODY_SIZE);
    StringUtils::erase_char(search_payload, '\n');
    if(req->body.size() > MAX_QUERY_LOG_BODY_SIZE) {
        search_payload += "...[truncated]";
    }

    std::string query_string = "?";
    for(const auto& param_kv: req->params) {
        if(param_kv.first != http_req::AUTH_HEADER && param_kv.first != http_req::USER_HEADER) {
            query_string += param_kv.first + "=" + param_kv.second + "&";
        }
    }

    return std::string("id=") + std::to_string(req->start_ts) + ", qs=" + query_string + ", body=" + search_payload;
}

void HouseKeeper::log_running_queries() {
    ifq_lock_t ifq_lock(ifq_mutex);
    log_running_queries_unlocked();
}

void HouseKeeper::log_running_queries_on_crash() {
    if(holds_ifq_mutex) {
        LOG(ERROR) << "Crashed while holding the in-flight queries lock, skipping dump of in-flight search queries.";
        return ;
    }

    // another thread holding the lock could itself be stuck behind the crashed thread
    std::unique_lock ifq_lock(ifq_mutex, std::defer_lock);
    if(!ifq_lock.try_lock_for(std::chrono::seconds(1))) {
        LOG(ERROR) << "Unable to acquire the in-flight queries lock, skipping dump of in-flight search queries.";
        return ;
    }

    log_running_queries_unlocked();
}

void HouseKeeper::log_running_queries_unlocked() {
    if(in_flight_queries.empty()) {
        LOG(INFO) << "No in-flight search queries were found.";
        return ;
    }

    LOG(INFO) << "Dump of in-flight search queries:";

    for(const auto& kv: in_flight_queries) {
        LOG(INFO) << kv.second.query_log;
    }
}

void HouseKeeper::log_bad_queries() {
    ifq_lock_t ifq_lock(ifq_mutex);

    auto now_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    const auto memory_req_min_age = memory_req_min_age_s.load();
    const auto long_req_log = long_req_log_s.load();

    for(auto& kv: in_flight_queries) {
        auto req_ts = kv.first;
        uint64_t query_time_elapsed_s = now_ts_us >= req_ts ? (now_ts_us - req_ts) / 1000000 : 0;
        if(query_time_elapsed_s < memory_req_min_age) {
            // since we use a map it's already ordered ascending on timestamp
            break;
        }

        if(kv.second.already_logged) {
            continue;
        }

        // either long running query or exceeding 1 GB of memory
        int64_t memory_req_start = kv.second.active_memory;
        int64_t curr_memory = active_memory_used;
        int64_t memory_diff = curr_memory - memory_req_start;
        const int64_t one_gb = 1073741824;
        const bool high_memory = memory_diff > one_gb;
        const bool long_running = query_time_elapsed_s > long_req_log;

        if(high_memory || long_running) {
            LOG(INFO) << "Detected bad query, start_ts: " << req_ts << ", memory_diff: " << memory_diff
                      << ", " << kv.second.query_log;
            kv.second.already_logged = true;
        }
    }
}

size_t HouseKeeper::get_num_inflight_queries() {
  ifq_lock_t ifq_lock(ifq_mutex);
  return in_flight_queries.size();
}
