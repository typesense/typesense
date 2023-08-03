#pragma once
#include "popular_queries.h"
#include "option.h"
#include "raft_server.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>

class AnalyticsManager {
private:
    mutable std::mutex mutex;
    std::condition_variable cv;

    std::atomic<bool> quit = false;

    const size_t QUERY_COMPACTION_INTERVAL_S = 60;

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
                        std::string& query, bool live_query, const std::string& user_id);

    void stop();

    void dispose();

    void persist_suggestions(ReplicationState *raft_server, uint64_t prev_persistence_s);

    std::unordered_map<std::string, PopularQueries*> get_popular_queries();
};
