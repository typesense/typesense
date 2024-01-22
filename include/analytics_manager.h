#pragma once
#include "query_analytics.h"
#include "option.h"
#include "raft_server.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>

struct event_t {
    std::string query;
    std::string event_type;
    uint64_t timestamp;
    std::string user_id;
    std::string doc_id;
    uint64_t position;

    event_t() = delete;

    ~event_t() = default;

    event_t(std::string q, std::string type, uint64_t ts, std::string uid, std::string id, uint64_t pos) {
        query = q;
        event_type = type;
        timestamp = ts;
        user_id = uid;
        doc_id = id;
        position = pos;
    }

    event_t& operator=(event_t& other) {
        if (this != &other) {
            query = other.query;
            event_type = other.event_type;
            timestamp = other.timestamp;
            user_id = other.user_id;
            doc_id = other.doc_id;
            position = other.position;
            return *this;
        }
    }

    void to_json(nlohmann::json& obj) const {
        obj["query"] = query;
        obj["event_type"] = event_type;
        obj["timestamp"] = timestamp;
        obj["user_id"] = user_id;
        obj["doc_id"] = doc_id;
        obj["position"] = position;
    }
};

struct counter_event_t {
    std::string counter_field;
    std::map<std::string, uint64_t> docid_counts;
    std::map<std::string, uint16_t> event_weight_map;
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
        std::string rule_type;
        bool expand_query = false;

        void to_json(nlohmann::json& obj) const {
            obj["name"] = name;
            obj["type"] = rule_type;
            obj["params"] = nlohmann::json::object();
            obj["params"]["limit"] = limit;
            obj["params"]["source"]["collections"] = query_collections;
            obj["params"]["destination"]["collection"] = suggestion_collection;

            if(rule_type == POPULAR_QUERIES_TYPE) {
                obj["params"]["expand_query"] = expand_query;
            }
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

    // collection => popular clicks
    std::unordered_map<std::string, counter_event_t> counter_events;

    //query collection => events
    std::unordered_map<std::string, std::vector<event_t>> query_collection_events;

    Store* store = nullptr;
    std::ofstream  analytics_logs;

    bool isRateLimitTestEnabled = false;
    uint64_t analytics_logs_count = 0;

    AnalyticsManager() {}

    ~AnalyticsManager();

    Option<bool> remove_index(const std::string& name);

    Option<bool> create_index(nlohmann::json &payload,
                              bool upsert,
                              bool write_to_disk);

public:

    static constexpr const char* ANALYTICS_RULE_PREFIX = "$AR";
    static constexpr const char* POPULAR_QUERIES_TYPE = "popular_queries";
    static constexpr const char* NOHITS_QUERIES_TYPE = "nohits_queries";
    static constexpr const char* COUNTER_TYPE = "counter";
    static constexpr const char* QUERY_CLICK = "query_click";
    static constexpr const char* QUERY_PURCHASE = "query_purchase";

    static AnalyticsManager& get_instance() {
        static AnalyticsManager instance;
        return instance;
    }

    AnalyticsManager(AnalyticsManager const&) = delete;
    void operator=(AnalyticsManager const&) = delete;

    void init(Store* store, const std::string& analytics_dir="");

    void run(ReplicationState* raft_server);

    Option<nlohmann::json> list_rules();

    Option<nlohmann::json> get_rule(const std::string& name);

    Option<bool> create_rule(nlohmann::json& payload, bool upsert, bool write_to_disk);

    Option<bool> remove_rule(const std::string& name);

    void add_suggestion(const std::string& query_collection,
                        const std::string& query, const std::string& expanded_query,
                        bool live_query, const std::string& user_id);

    void stop();

    void dispose();

    void persist_query_events(ReplicationState *raft_server, uint64_t prev_persistence_s);

    std::unordered_map<std::string, QueryAnalytics*> get_popular_queries();

    Option<bool> add_event(const std::string& event_type, const std::string& query_collection, const std::string& query, const std::string& user_id,
                            std::string doc_id, uint64_t position, const std::string& client_ip);

    void persist_events(ReplicationState *raft_server, uint64_t prev_persistence_s);

    void persist_popular_events(ReplicationState *raft_server, uint64_t prev_persistence_s);

    nlohmann::json get_events(const std::string& coll, const std::string& event_type);

    std::unordered_map<std::string, counter_event_t> get_popular_clicks();

    void add_nohits_query(const std::string& query_collection,
                          const std::string& query, bool live_query, const std::string& user_id);

    std::unordered_map<std::string, QueryAnalytics*> get_nohits_queries();

    void resetToggleRateLimit(bool toggle);
};
