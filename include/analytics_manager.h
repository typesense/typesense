#pragma once
#include "popular_queries.h"
#include "option.h"
#include "raft_server.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>

struct ClickEvent {
    std::string query;
    uint64_t timestamp;
    std::string doc_id;
    uint64_t position;

    ClickEvent() = delete;

    ~ClickEvent() = default;

    ClickEvent(std::string q, uint64_t ts, std::string id, uint64_t pos) {
        query = q;
        timestamp = ts;
        doc_id = id;
        position = pos;
    }

    ClickEvent& operator=(ClickEvent& other) {
        if (this != &other) {
            query = other.query;
            timestamp = other.timestamp;
            doc_id = other.doc_id;
            position = other.position;
            return *this;
        }
    }

    void to_json(nlohmann::json& obj) const {
        obj["query"] = query;
        obj["timestamp"] = timestamp;
        obj["doc_id"] = doc_id;
        obj["position"] = position;
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
    std::unordered_map<std::string, PopularQueries*> popular_queries;

    //query collection => click events
    std::unordered_map<std::string, std::vector<ClickEvent>> query_collection_click_events;

    Store* store = nullptr;

    AnalyticsManager() {}

    ~AnalyticsManager();

    Option<bool> remove_popular_queries_index(const std::string& name);

    Option<bool> create_popular_queries_index(nlohmann::json &payload,
                                              bool upsert,
                                              bool write_to_disk);

public:

    static constexpr const char* ANALYTICS_RULE_PREFIX = "$AR";
    static constexpr const char* POPULAR_QUERIES_TYPE = "popular_queries";
    static constexpr const char* CLICK_EVENT = "$CE";

    static AnalyticsManager& get_instance() {
        static AnalyticsManager instance;
        return instance;
    }

    AnalyticsManager(AnalyticsManager const&) = delete;
    void operator=(AnalyticsManager const&) = delete;

    void init(Store* store);

    void run(ReplicationState* raft_server);

    Option<nlohmann::json> list_rules();

    Option<nlohmann::json> get_rule(const std::string& name);

    Option<bool> create_rule(nlohmann::json& payload, bool upsert, bool write_to_disk);

    Option<bool> remove_rule(const std::string& name);

    void add_suggestion(const std::string& query_collection,
                        const std::string& query, bool live_query, const std::string& user_id);

    void stop();

    void dispose();

    void persist_suggestions(ReplicationState *raft_server, uint64_t prev_persistence_s);

    std::unordered_map<std::string, PopularQueries*> get_popular_queries();

    void add_click_event(const std::string& query_collection, const std::string& query,
                            std::string doc_id, uint64_t position);

    void persist_click_event(ReplicationState *raft_server, uint64_t prev_persistence_s);

    nlohmann::json get_click_events();

    Option<bool> write_click_event_to_store(nlohmann::json& click_event_json);
};
