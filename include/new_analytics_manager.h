#pragma once

#include "doc_analytics.h"
#include "query_analytic.h"
#include <vector>
#include <string>
#include <shared_mutex>
#include "lru/lru.hpp"
#include "raft_server.h"

struct external_event_cache_t {
    uint64_t last_update_time;
    uint64_t count;

    bool operator == (const external_event_cache_t& res) const {
        return last_update_time == res.last_update_time;
    }

    bool operator != (const external_event_cache_t& res) const {
        return last_update_time != res.last_update_time;
    }
};

class NewAnalyticsManager {
private:
    mutable std::shared_mutex mutex;
    std::condition_variable_any cv;
    const size_t QUERY_COMPACTION_INTERVAL_S = 30;
    bool isRateLimitEnabled = true;
    uint32_t analytics_minute_rate_limit = 5;

    std::atomic<bool> quit = false;

    DocAnalytics& doc_analytics = DocAnalytics::get_instance();
    QueryAnalytic& query_analytic = QueryAnalytic::get_instance();
    LRU::Cache<std::string, external_event_cache_t> external_events_cache;
    std::unordered_map<std::string, std::string> rules_map;

    Store* store = nullptr;
    Store* analytics_store = nullptr;

    NewAnalyticsManager() {}
    ~NewAnalyticsManager();

public:
    static constexpr const char* ANALYTICS_RULE_PREFIX = "$NAR";
    static NewAnalyticsManager& get_instance() {
        static NewAnalyticsManager instance;
        return instance;
    }

    NewAnalyticsManager(NewAnalyticsManager const&) = delete;
    void operator=(NewAnalyticsManager const&) = delete;

    void persist_db_events(ReplicationState *raft_server, uint64_t prev_persistence_s);
    void persist_analytics_db_events(ReplicationState *raft_server, uint64_t prev_persistence_s);
    
    Option<bool> add_external_event(const std::string& client_ip, const nlohmann::json& event_data);
    Option<bool> add_internal_event(const query_internal_event_t& event_data);

    Option<nlohmann::json> get_events(const std::string& userid, const std::string& event_name, uint32_t N);
    Option<nlohmann::json> list_rules(const std::string& rule_tag = "");
    Option<nlohmann::json> get_rule(const std::string& name);
    Option<bool> create_rule(nlohmann::json& payload, bool update, bool write_to_disk);
    Option<bool> remove_rule(const std::string& name);
    void remove_all_rules();

    void resetToggleRateLimit(bool toggle);
    bool write_to_db(const nlohmann::json& payload);

    void run(ReplicationState* raft_server);
    void init(Store* store, Store* analytics_store, uint32_t analytics_minute_rate_limit);

    void stop();
    void dispose();
};