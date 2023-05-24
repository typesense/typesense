#include <mutex>
#include <thread>
#include "analytics_manager.h"
#include "tokenizer.h"
#include "http_client.h"
#include "collection_manager.h"

Option<bool> AnalyticsManager::create_index(nlohmann::json& payload, bool write_to_disk) {
    /*
        Sample payload:

        {
            "name": "top_queries",
            "limit": 1000,
            "source": {
                "collections": ["brands", "products"]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    */

    if(!payload.contains("name") || !payload["name"].is_string()) {
        return Option<bool>(400, "Bad or missing name.");
    }

    if(!payload.contains("source") || !payload["source"].is_object()) {
        return Option<bool>(400, "Bad or missing source.");
    }

    if(!payload.contains("destination") || !payload["destination"].is_object()) {
        return Option<bool>(400, "Bad or missing destination.");
    }

    const std::string& suggestion_config_name = payload["name"].get<std::string>();

    size_t max_suggestions = 1000;

    if(payload.contains("limit") && payload["limit"].is_number_integer()) {
        max_suggestions = payload["limit"].get<size_t>();
    }

    if(suggestion_configs.find(suggestion_config_name) != suggestion_configs.end()) {
        return Option<bool>(400, "There's already another configuration with the name `" +
                                            suggestion_config_name + "`.");
    }

    if(!payload["source"].contains("collections") || !payload["source"]["collections"].is_array()) {
        return Option<bool>(400, "Must contain a valid list of source collections.");
    }

    if(!payload["destination"].contains("collection") || !payload["destination"]["collection"].is_string()) {
        return Option<bool>(400, "Must contain a valid destination collection.");
    }

    const std::string& suggestion_collection = payload["destination"]["collection"].get<std::string>();
    suggestion_config_t suggestion_config;
    suggestion_config.name = suggestion_config_name;
    suggestion_config.suggestion_collection = suggestion_collection;
    suggestion_config.max_suggestions = max_suggestions;

    for(const auto& coll: payload["source"]["collections"]) {
        if(!coll.is_string()) {
            return Option<bool>(400, "Must contain a valid list of source collection names.");
        }

        const std::string& src_collection = coll.get<std::string>();
        suggestion_config.query_collections.push_back(src_collection);
    }

    std::unique_lock lock(mutex);

    suggestion_configs.emplace(suggestion_config_name, suggestion_config);

    for(const auto& query_coll: suggestion_config.query_collections) {
        query_collection_mapping[query_coll].push_back(suggestion_collection);
    }

    PopularQueries* popularQueries = new PopularQueries(max_suggestions);
    popular_queries.emplace(suggestion_collection, popularQueries);

    if(write_to_disk) {
        payload["type"] = RESOURCE_TYPE;
        auto suggestion_key = std::string(ANALYTICS_CONFIG_PREFIX) + "_" + suggestion_config_name;
        bool inserted = store->insert(suggestion_key, payload.dump());
        if(!inserted) {
            return Option<bool>(500, "Error while storing the config to disk.");
        }
    }

    return Option<bool>(true);
}

AnalyticsManager::~AnalyticsManager() {
    std::unique_lock lock(mutex);

    for(auto& kv: popular_queries) {
        delete kv.second;
    }
}

Option<bool> AnalyticsManager::remove_suggestion_index(const std::string &name) {
    std::unique_lock lock(mutex);

    auto suggestion_configs_it = suggestion_configs.find(name);

    if(suggestion_configs_it == suggestion_configs.end()) {
        return Option<bool>(404, "Index not found.");
    }

    const auto& suggestion_collection = suggestion_configs_it->second.suggestion_collection;

    for(const auto& query_collection: suggestion_configs_it->second.query_collections) {
        query_collection_mapping.erase(query_collection);
    }

    if(popular_queries.count(suggestion_collection) != 0) {
        delete popular_queries[suggestion_collection];
        popular_queries.erase(suggestion_collection);
    }

    suggestion_configs.erase(name);

    auto suggestion_key = std::string(ANALYTICS_CONFIG_PREFIX) + "_" + name;
    bool erased = store->remove(suggestion_key);
    if(!erased) {
        return Option<bool>(500, "Error while deleting from disk.");
    }

    return Option<bool>(true);
}

void AnalyticsManager::add_suggestion(const std::string &query_collection, std::string &query,
                                      const bool live_query, const std::string& user_id) {
    // look up suggestion collections for the query collection
    std::unique_lock lock(mutex);
    const auto& suggestion_collections_it = query_collection_mapping.find(query_collection);
    if(suggestion_collections_it != query_collection_mapping.end()) {
        for(const auto& suggestion_collection: suggestion_collections_it->second) {
            const auto& popular_queries_it = popular_queries.find(suggestion_collection);
            if(popular_queries_it != popular_queries.end()) {
                Tokenizer::normalize_ascii(query);
                popular_queries_it->second->add(query, live_query, user_id);
            }
        }
    }
}

void AnalyticsManager::run(ReplicationState* raft_server) {
    uint64_t prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count();

    while(!quit) {
        std::unique_lock lk(mutex);
        cv.wait_for(lk, std::chrono::seconds(QUERY_COMPACTION_INTERVAL_S), [&] { return quit.load(); });

        //LOG(INFO) << "QuerySuggestions::run";

        if(quit) {
            lk.unlock();
            break;
        }

        for(const auto& suggestion_config: suggestion_configs) {
            const std::string& sink_name = suggestion_config.first;
            const std::string& suggestion_coll = suggestion_config.second.suggestion_collection;

            auto popular_queries_it = popular_queries.find(suggestion_coll);
            if(popular_queries_it == popular_queries.end()) {
                continue;
            }

            // need to prepare the counts as JSON docs for import into the suggestion collection
            // {"id": "432432", "q": "foo", "$operations": {"increment": {"count": 100}}}

            PopularQueries* popularQueries = popular_queries_it->second;

            // aggregate prefix queries to their final form
            auto now = std::chrono::system_clock::now().time_since_epoch();
            auto now_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
            popularQueries->compact_user_queries(now_ts_us);

            auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(now).count();

            if(now_ts_seconds - prev_persistence_s < Config::get_instance().get_analytics_flush_interval()) {
                // we will persist aggregation every hour
                continue;
            }

            prev_persistence_s = now_ts_seconds;

            std::string import_payload;
            popularQueries->serialize_as_docs(import_payload);

            if(import_payload.empty()) {
                continue;
            }

            // send http request
            std::string leader_url = raft_server->get_leader_url();
            if(!leader_url.empty()) {
                const std::string& resource_url = leader_url + "collections/" + suggestion_coll +
                                                    "/documents/import?action=emplace";
                std::string res;
                std::map<std::string, std::string> res_headers;
                std::unordered_map<std::string, std::string> headers;
                long status_code = HttpClient::post_response(resource_url, import_payload,
                                                             res, res_headers, headers, 10*1000);

                if(status_code != 200) {
                    LOG(ERROR) << "Error while sending query suggestions events to leader. "
                               << "Status code: " << status_code << ", response: " << res;
                } else {
                    LOG(INFO) << "Sent query suggestions to leader for aggregation.";
                    popularQueries->reset_local_counts();

                    if(raft_server->is_leader()) {
                        // try to run top-K compaction of suggestion collection
                        auto coll = CollectionManager::get_instance().get_collection(suggestion_coll);
                        if (coll == nullptr) {
                            LOG(ERROR) << "No collection found for suggestions aggregation: " + suggestion_coll;
                            continue;
                        }

                        coll->truncate_after_top_k("count", popularQueries->get_k());
                    }
                }
            }
        }

        lk.unlock();
    }

    dispose();
}

void AnalyticsManager::stop() {
    quit = true;
    cv.notify_all();
}

void AnalyticsManager::dispose() {
    for(auto& kv: popular_queries) {
        delete kv.second;
    }

    popular_queries.clear();
}

void AnalyticsManager::init(Store* store) {
    this->store = store;
}
