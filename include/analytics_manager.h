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
        size_t max_suggestions;
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

public:

    static constexpr const char* ANALYTICS_CONFIG_PREFIX = "$AC";
    static constexpr const char* RESOURCE_TYPE = "popular_queries";

    static AnalyticsManager& get_instance() {
        static AnalyticsManager instance;
        return instance;
    }

    AnalyticsManager(AnalyticsManager const&) = delete;
    void operator=(AnalyticsManager const&) = delete;

    void init(Store* store);

    void run(ReplicationState* raft_server);

    Option<bool> create_index(nlohmann::json& payload, bool write_to_disk = true);

    Option<bool> remove_suggestion_index(const std::string& name);

    void add_suggestion(const std::string& query_collection,
                        std::string& query, const bool live_query, const std::string& user_id);

    void stop();

    void dispose();
};
