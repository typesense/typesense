#pragma once
#include "popular_queries.h"
#include "option.h"
#include "raft_server.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>

class QuerySuggestions {
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

    QuerySuggestions() {}

    ~QuerySuggestions();

public:

    static constexpr const char* EVENT_SINK_CONFIG_PREFIX = "$ES";

    static constexpr const char* SINK_TYPE = "query_suggestions";

    static QuerySuggestions& get_instance() {
        static QuerySuggestions instance;
        return instance;
    }

    QuerySuggestions(QuerySuggestions const&) = delete;
    void operator=(QuerySuggestions const&) = delete;

    void init(Store* store);

    void run(ReplicationState* raft_server);

    Option<nlohmann::json> create_index(const nlohmann::json& payload, bool write_to_disk = true);

    Option<bool> remove_suggestion_index(const std::string& name);

    void add_suggestion(const std::string& query_collection,
                        std::string& query, const bool live_query, const std::string& user_id);

    void stop();

    void dispose();
};
