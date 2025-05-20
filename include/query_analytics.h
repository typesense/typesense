#pragma once

#include <string>
#include <vector>
#include <tsl/htrie_map.h>
#include <json.hpp>
#include <atomic>
#include <shared_mutex>
#include <set>

struct query_event_t {
    std::string query;
    std::string user_id;
    std::string collection;
    std::string event_name;
    uint64_t timestamp;
    std::string event_type;

    void to_json(nlohmann::json& obj) const;
};

class QueryAnalytics {
public:
    struct analytics_meta_t {
        std::string query;
        uint64_t timestamp;
        std::string filter_str;
        std::string tag_str;

        analytics_meta_t(const std::string& query, uint64_t timestamp = 0, const std::string& filter = "", const std::string& tag = "")
                : query(query), timestamp(timestamp), filter_str(filter),  tag_str(tag){}

        bool operator==(const analytics_meta_t& other) const {
            return query == other.query && filter_str == other.filter_str && tag_str == other.tag_str;
        }

        // Hash function for the struct
        struct Hash {
            std::size_t operator()(const analytics_meta_t& meta) const {
                return std::hash<std::string>{}(meta.query + meta.filter_str + meta.tag_str);
            }
        };
    };

    static const size_t QUERY_FINALIZATION_INTERVAL_MICROS = 4 * 1000 * 1000;

private:

    size_t k;
    const size_t max_size;
    const size_t max_query_length = 1024;

    bool expand_query = false;
    bool auto_aggregation_enabled;
    bool log_to_analytics = false;
    std::string event_name;
    std::string collection_name;

    // counts aggregated within the current node
    std::unordered_map<analytics_meta_t, uint32_t, analytics_meta_t::Hash> local_counts;
    std::shared_mutex lmutex;

    std::unordered_map<std::string, std::vector<analytics_meta_t>> user_prefix_queries;
    std::unordered_map<std::string, std::vector<query_event_t>> query_events;
    std::shared_mutex umutex;
    std::set<std::string> meta_fields;

public:

    QueryAnalytics(size_t k, bool enable_auto_aggregation = true, const std::set<std::string>& meta_fields = {}, bool log_to_analytics = false, const std::string& event_name = "", const std::string& collection_name = "");

    void add(const std::string& value, const std::string& expanded_key,
             const bool live_query, const std::string& user_id, uint64_t now_ts_us = 0, const std::string& filter = "",
             const std::string& tag = "");

    void compact_user_queries(uint64_t now_ts_us);

    void serialize_as_docs(std::string& docs);
    void serialize_as_events(std::string& events);

    void reset_local_counts();

    size_t get_k();

    std::unordered_map<std::string, std::vector<analytics_meta_t>> get_user_prefix_queries();

    std::unordered_map<analytics_meta_t, uint32_t, analytics_meta_t::Hash> get_local_counts();

    void set_expand_query(bool expand_query);

    bool is_auto_aggregation_enabled() const;
};
