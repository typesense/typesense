#pragma once
#include "query_analytics.h"
#include "option.h"
#include "raft_server.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>

struct click_event_t {
    std::string query;
    uint64_t timestamp;
    std::string user_id;
    std::string doc_id;
    uint64_t position;

    click_event_t() = delete;

    ~click_event_t() = default;

    click_event_t(std::string q, uint64_t ts, std::string uid, std::string id, uint64_t pos) {
        query = q;
        timestamp = ts;
        user_id = uid;
        doc_id = id;
        position = pos;
    }

    click_event_t& operator=(click_event_t& other) {
        if (this != &other) {
            query = other.query;
            timestamp = other.timestamp;
            user_id = other.user_id;
            doc_id = other.doc_id;
            position = other.position;
            return *this;
        }
    }

    void to_json(nlohmann::json& obj) const {
        obj["query"] = query;
        obj["timestamp"] = timestamp;
        obj["user_id"] = user_id;
        obj["doc_id"] = doc_id;
        obj["position"] = position;
    }
};

struct query_hits_count_t {
    std::string query;
    uint64_t timestamp;
    std::string user_id;
    uint64_t hits_count;

    query_hits_count_t() = delete;

    ~query_hits_count_t() = default;

    query_hits_count_t(std::string q, uint64_t ts, std::string uid, uint64_t count) {
        query = q;
        timestamp = ts;
        user_id = uid;
        hits_count = count;
    }

    query_hits_count_t &operator=(query_hits_count_t &other) {
        if (this != &other) {
            query = other.query;
            timestamp = other.timestamp;
            user_id = other.user_id;
            hits_count = other.hits_count;
            return *this;
        }
    }

    void to_json(nlohmann::json &obj) const {
        obj["query"] = query;
        obj["timestamp"] = timestamp;
        obj["user_id"] = user_id;
        obj["hits_count"] = hits_count;
    }
};

struct query_hits_count_comp {
    bool operator()(const query_hits_count_t& a, const query_hits_count_t& b) const {
        return a.query < b.query;
    }
};

struct event_cache_t {
    uint64_t last_update_time;
    uint64_t count;

    bool operator == (const event_cache_t& res) const {
        return last_update_time == res.last_update_time;
    }

    bool operator != (const event_cache_t& res) const {
        return last_update_time != res.last_update_time;
    }
};

class AnalyticsManager {
private:
    mutable std::mutex mutex;
    std::condition_variable cv;

    std::atomic<bool> quit = false;

    const size_t QUERY_COMPACTION_INTERVAL_S = 30;

    struct suggestion_config_t {
        std::string name;
        std::string suggestion_collection;
        std::vector<std::string> query_collections;
        size_t limit;

        void to_json(nlohmann::json& obj) const {
            obj["name"] = name;
            obj["type"] = POPULAR_QUERIES_TYPE;
            obj["params"] = nlohmann::json::object();
            obj["params"]["limit"] = limit;
            obj["params"]["source"]["collections"] = query_collections;
            obj["params"]["destination"]["collection"] = suggestion_collection;
        }
    };

    // config name => config
    std::unordered_map<std::string, suggestion_config_t> suggestion_configs;

    // query collection => suggestion collections
    std::unordered_map<std::string, std::vector<std::string>> query_collection_mapping;

    // suggestion collection => popular queries
    std::unordered_map<std::string, QueryAnalytics*> popular_queries;

    // suggestion collection => nohits queries
    std::unordered_map<std::string, QueryAnalytics*> nohits_queries;

    //query collection => click events
    std::unordered_map<std::string, std::vector<click_event_t>> query_collection_click_events;

    //query collection => query hits count
    std::unordered_map<std::string, std::set<query_hits_count_t, query_hits_count_comp>> query_collection_hits_count;

    Store* store = nullptr;
    Store* analytics_store = nullptr;

    AnalyticsManager() {}

    ~AnalyticsManager();

    Option<bool> remove_queries_index(const std::string& name);

    Option<bool> create_queries_index(nlohmann::json &payload,
                                              bool upsert,
                                              bool write_to_disk);

public:

    static constexpr const char* ANALYTICS_RULE_PREFIX = "$AR";
    static constexpr const char* CLICK_EVENT = "$CE";
    static constexpr const char* QUERY_HITS_COUNT = "$QH";
    static constexpr const char* POPULAR_QUERIES_TYPE = "popular_queries";
    static constexpr const char* NOHITS_QUERIES_TYPE = "nohits_queries";

    static AnalyticsManager& get_instance() {
        static AnalyticsManager instance;
        return instance;
    }

    AnalyticsManager(AnalyticsManager const&) = delete;
    void operator=(AnalyticsManager const&) = delete;

    void init(Store* store, Store* analytics_store);

    void run(ReplicationState* raft_server);

    Option<nlohmann::json> list_rules();

    Option<nlohmann::json> get_rule(const std::string& name);

    Option<bool> create_rule(nlohmann::json& payload, bool upsert, bool write_to_disk);

    Option<bool> remove_rule(const std::string& name);

    void add_suggestion(const std::string& query_collection,
                        const std::string& query, bool live_query, const std::string& user_id);

    void stop();

    void dispose();

    void persist_query_events(ReplicationState *raft_server, uint64_t prev_persistence_s);

    std::unordered_map<std::string, QueryAnalytics*> get_popular_queries();

    Option<bool> add_click_event(const std::string& query_collection, const std::string& query, const std::string& user_id,
                            std::string doc_id, uint64_t position, const std::string& client_ip);

    void persist_query_hits_click_events(ReplicationState *raft_server, uint64_t prev_persistence_s);

    nlohmann::json get_click_events();

    Option<bool> write_events_to_store(nlohmann::json& event_jsons);

    void add_nohits_query(const std::string& query_collection,
                          const std::string& query, bool live_query, const std::string& user_id);

    std::unordered_map<std::string, QueryAnalytics*> get_nohits_queries();

    void resetRateLimit();

    void add_query_hits_count(const std::string& query_collection, const std::string& query, const std::string& user_id,
                                            uint64_t hits_count);

    nlohmann::json get_query_hits_counts();

    void checkEventsExpiry(uint64_t events_ttl_interval=2592000000000); //30days default
};
