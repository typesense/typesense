#include "query_analytics.h"
#include "logger.h"
#include <algorithm>
#include <mutex>
#include "string_utils.h"

QueryAnalytics::QueryAnalytics(size_t k, bool enable_auto_aggregation, bool filterby_anlaytics)
                : k(k), max_size(k * 2), auto_aggregation_enabled(enable_auto_aggregation), filter_by_analytics(filterby_anlaytics) {

}

void QueryAnalytics::add(const std::string& key, const std::string& expanded_key,
                         const bool live_query, const std::string& user_id, uint64_t now_ts_us, const std::string& filter_str) {
    if(live_query) {
        // live query must be aggregated first to their final form as they could be prefix queries
        if(now_ts_us == 0) {
            now_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::system_clock::now().time_since_epoch()).count();
        }

        if(!umutex.try_lock()) {
            // instead of locking we just skip incrementing keys during consolidation time
            return ;
        }

        auto& queries = user_prefix_queries[user_id];
        if(queries.size() < 100) {
            // only live queries could send expanded queries
            const std::string& actual_key = expand_query ? expanded_key : key;
            std::string filter_by_str = filter_by_analytics ? filter_str : "";
            if(actual_key.size() < max_query_length) {
                queries.emplace_back(actual_key, now_ts_us, filter_by_str);
            }
        }

        umutex.unlock();

    } else {
        if (!lmutex.try_lock()) {
            // instead of locking we just skip incrementing keys during consolidation time
            return;
        }

        auto query = key;
        if(!filter_str.empty()) {
            query =  query + "-" + filter_str;
        }
        auto it = local_counts.find(query);

        if (it != local_counts.end()) {
            it.value()++;
        } else if (local_counts.size() < max_size && query.size() < max_query_length) {
            // skip count when map has become too large (to prevent abuse)
            local_counts.emplace(query, 1);
        }
        lmutex.unlock();
    }
}

void QueryAnalytics::serialize_as_docs(std::string& docs) {
    std::shared_lock lk(lmutex);

    std::string key_buffer;
    for(auto it = local_counts.begin(); it != local_counts.end(); ++it) {
        nlohmann::json doc;

        it.key(key_buffer);
        if(filter_by_analytics) {
            std::string filter_str;
            auto ind = key_buffer.find('-');
            if (ind != std::string::npos) {
                //filter string exists
                filter_str = key_buffer.substr(ind + 1, key_buffer.size() - ind);
                key_buffer = key_buffer.substr(0, ind);
            }
            doc["filter_by"] = filter_str;
        }

        doc["id"] = std::to_string(StringUtils::hash_wy(key_buffer.c_str(), key_buffer.size()));
        doc["q"] = key_buffer;
        doc["$operations"]["increment"]["count"] = it.value();
        docs += doc.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore) + "\n";
    }

    if(!docs.empty()) {
        docs.pop_back();
    }
}

void QueryAnalytics::reset_local_counts() {
    std::unique_lock lk(lmutex);
    local_counts.clear();
}

size_t QueryAnalytics::get_k() {
    return k;
}

void QueryAnalytics::compact_user_queries(uint64_t now_ts_us) {
    std::unique_lock lk(umutex);
    std::vector<std::string> keys_to_delete;

    for(auto& kv: user_prefix_queries) {
        auto& queries = kv.second;
        int64_t last_consolidated_index = -1;
        for(uint32_t i = 0; i < queries.size(); i++) {
            if(now_ts_us - queries[i].timestamp < QUERY_FINALIZATION_INTERVAL_MICROS) {
                break;
            }

            uint64_t diff_micros = (i == queries.size()-1) ? (now_ts_us - queries[i].timestamp) :
                                   (queries[i + 1].timestamp - queries[i].timestamp);

            if(diff_micros > QUERY_FINALIZATION_INTERVAL_MICROS) {
                add(queries[i].query, queries[i].query, false, "", queries[i].timestamp, queries[i].filter_by_str);
                last_consolidated_index = i;
            }
        }

        queries.erase(queries.begin(), queries.begin() + last_consolidated_index+1);

        if(queries.empty()) {
            keys_to_delete.push_back(kv.first);
        }
    }

    for(auto& key: keys_to_delete) {
        user_prefix_queries.erase(key);
    }
}

std::unordered_map<std::string, std::vector<QueryAnalytics::QWithTimestampFilter>> QueryAnalytics::get_user_prefix_queries() {
    std::unique_lock lk(umutex);
    return user_prefix_queries;
}

tsl::htrie_map<char, uint32_t> QueryAnalytics::get_local_counts() {
    std::unique_lock lk(lmutex);
    return local_counts;
}

void QueryAnalytics::set_expand_query(bool expand_query) {
    this->expand_query = expand_query;
}

bool QueryAnalytics::is_auto_aggregation_enabled() const {
    return auto_aggregation_enabled;
}