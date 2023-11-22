#include <mutex>
#include <thread>
#include "analytics_manager.h"
#include "tokenizer.h"
#include "http_client.h"
#include "collection_manager.h"
#include "lru/lru.hpp"

LRU::Cache<std::string, event_cache_t> events_cache;
#define CLICK_EVENTS_RATE_LIMIT_SEC 60
#define CLICK_EVENTS_RATE_LIMIT_COUNT 5

Option<bool> AnalyticsManager::create_rule(nlohmann::json& payload, bool upsert, bool write_to_disk) {
    /*
        Sample payload:

        {
            "name": "top_search_queries",
            "type": "popular_queries",
            "params": {
                "limit": 1000,
                "source": {
                    "collections": ["brands", "products"]
                },
                "destination": {
                    "collection": "top_search_queries"
                }
            }
        }
    */

    if(!payload.contains("type") || !payload["type"].is_string()) {
        return Option<bool>(400, "Request payload contains invalid type.");
    }

    if(!payload.contains("name") || !payload["name"].is_string()) {
        return Option<bool>(400, "Bad or missing name.");
    }

    if(!payload.contains("params") || !payload["params"].is_object()) {
        return Option<bool>(400, "Bad or missing params.");
    }

    if(payload["type"] == POPULAR_QUERIES_TYPE) {
        return create_popular_queries_index(payload, upsert, write_to_disk);
    }

    return Option<bool>(400, "Invalid type.");
}

Option<bool> AnalyticsManager::create_popular_queries_index(nlohmann::json &payload, bool upsert, bool write_to_disk) {
    // params and name are validated upstream
    const std::string& suggestion_config_name = payload["name"].get<std::string>();
    bool already_exists = suggestion_configs.find(suggestion_config_name) != suggestion_configs.end();

    if(!upsert && already_exists) {
        return Option<bool>(400, "There's already another configuration with the name `" +
                                 suggestion_config_name + "`.");
    }

    const auto& params = payload["params"];

    if(!params.contains("source") || !params["source"].is_object()) {
        return Option<bool>(400, "Bad or missing source.");
    }

    if(!params.contains("destination") || !params["destination"].is_object()) {
        return Option<bool>(400, "Bad or missing destination.");
    }

    size_t limit = 1000;

    if(params.contains("limit") && params["limit"].is_number_integer()) {
        limit = params["limit"].get<size_t>();
    }

    if(!params["source"].contains("collections") || !params["source"]["collections"].is_array()) {
        return Option<bool>(400, "Must contain a valid list of source collections.");
    }

    if(!params["destination"].contains("collection") || !params["destination"]["collection"].is_string()) {
        return Option<bool>(400, "Must contain a valid destination collection.");
    }

    const std::string& suggestion_collection = params["destination"]["collection"].get<std::string>();
    suggestion_config_t suggestion_config;
    suggestion_config.name = suggestion_config_name;
    suggestion_config.suggestion_collection = suggestion_collection;
    suggestion_config.limit = limit;

    if(!upsert && popular_queries.count(suggestion_collection) != 0) {
        return Option<bool>(400, "There's already another configuration for this destination collection.");
    }

    for(const auto& coll: params["source"]["collections"]) {
        if(!coll.is_string()) {
            return Option<bool>(400, "Must contain a valid list of source collection names.");
        }

        const std::string& src_collection = coll.get<std::string>();
        suggestion_config.query_collections.push_back(src_collection);
    }

    std::unique_lock lock(mutex);

    if(already_exists) {
        // remove the previous configuration with same name (upsert)
        Option<bool> remove_op = remove_popular_queries_index(suggestion_config_name);
        if(!remove_op.ok()) {
            return Option<bool>(500, "Error erasing the existing configuration.");;
        }
    }

    suggestion_configs.emplace(suggestion_config_name, suggestion_config);

    for(const auto& query_coll: suggestion_config.query_collections) {
        query_collection_mapping[query_coll].push_back(suggestion_collection);
    }

    PopularQueries* popularQueries = new PopularQueries(limit);
    popular_queries.emplace(suggestion_collection, popularQueries);

    if(write_to_disk) {
        auto suggestion_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + suggestion_config_name;
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

Option<nlohmann::json> AnalyticsManager::list_rules() {
    std::unique_lock lock(mutex);

    nlohmann::json rules = nlohmann::json::object();
    rules["rules"]= nlohmann::json::array();

    for(const auto& suggestion_config: suggestion_configs) {
        nlohmann::json rule;
        suggestion_config.second.to_json(rule);
        rules["rules"].push_back(rule);
    }

    return Option<nlohmann::json>(rules);
}

Option<nlohmann::json> AnalyticsManager::get_rule(const string& name) {
    nlohmann::json rule;
    std::unique_lock lock(mutex);

    auto suggestion_config_it = suggestion_configs.find(name);
    if(suggestion_config_it == suggestion_configs.end()) {
        return Option<nlohmann::json>(404, "Rule not found.");
    }

    suggestion_config_it->second.to_json(rule);
    return Option<nlohmann::json>(rule);
}

Option<bool> AnalyticsManager::remove_rule(const string &name) {
    std::unique_lock lock(mutex);

    auto suggestion_configs_it = suggestion_configs.find(name);
    if(suggestion_configs_it != suggestion_configs.end()) {
        return remove_popular_queries_index(name);
    }

    return Option<bool>(404, "Rule not found.");
}

Option<bool> AnalyticsManager::remove_popular_queries_index(const std::string &name) {
    // lock is held by caller
    auto suggestion_configs_it = suggestion_configs.find(name);

    if(suggestion_configs_it == suggestion_configs.end()) {
        return Option<bool>(404, "Rule not found.");
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

    auto suggestion_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + name;
    bool erased = store->remove(suggestion_key);
    if(!erased) {
        return Option<bool>(500, "Error while deleting from disk.");
    }

    return Option<bool>(true);
}

void AnalyticsManager::add_suggestion(const std::string &query_collection, const std::string& query,
                                      const bool live_query, const std::string& user_id) {
    // look up suggestion collections for the query collection
    std::unique_lock lock(mutex);
    const auto& suggestion_collections_it = query_collection_mapping.find(query_collection);
    if(suggestion_collections_it != query_collection_mapping.end()) {
        for(const auto& suggestion_collection: suggestion_collections_it->second) {
            const auto& popular_queries_it = popular_queries.find(suggestion_collection);
            if(popular_queries_it != popular_queries.end()) {
                popular_queries_it->second->add(query, live_query, user_id);
            }
        }
    }
}

Option<bool> AnalyticsManager::add_click_event(const std::string &query_collection, const std::string &query, const std::string &user_id,
                                       std::string doc_id, uint64_t position, const std::string& client_ip) {
    std::unique_lock lock(mutex);
    if(analytics_store) {
        auto &click_events_vec = query_collection_click_events[query_collection];

        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        auto events_cache_it = events_cache.find(client_ip);

        if (events_cache_it != events_cache.end()) {
            //event found in events cache
            if ((now_ts_seconds - events_cache_it->second.last_update_time) < CLICK_EVENTS_RATE_LIMIT_SEC) {
                if (events_cache_it->second.count >= CLICK_EVENTS_RATE_LIMIT_COUNT) {
                    return Option<bool>(500, "click event rate limit reached.");
                } else {
                    events_cache_it->second.count++;
                }
            } else {
                events_cache_it->second.last_update_time = now_ts_seconds;
                events_cache_it->second.count = 1;
            }
        } else {
            event_cache_t eventCache{(uint64_t) now_ts_seconds, 1};
            events_cache.insert(client_ip, eventCache);
        }

        auto now_ts_useconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        ClickEvent click_event(query, now_ts_useconds, user_id, doc_id, position);
        click_events_vec.emplace_back(click_event);

        return Option<bool>(true);
    }

    LOG(ERROR) << "Analytics Directory not provided.";
    return Option<bool>(true);
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

        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        if(now_ts_seconds - prev_persistence_s < Config::get_instance().get_analytics_flush_interval()) {
            // we will persist aggregation every hour
            // LOG(INFO) << "QuerySuggestions::run interval is less, continuing";
            continue;
        }

        persist_suggestions(raft_server, prev_persistence_s);
        persist_click_events(raft_server, prev_persistence_s);
        prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        lk.unlock();
    }

    dispose();
}

void AnalyticsManager::persist_suggestions(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    // lock is held by caller
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

        std::string import_payload;
        popularQueries->serialize_as_docs(import_payload);

        if(import_payload.empty()) {
            continue;
        }

        // send http request
        std::string leader_url = raft_server->get_leader_url();
        if(!leader_url.empty()) {
            const std::string& base_url = leader_url + "collections/" + suggestion_coll;
            std::string res;

            const std::string& update_url = base_url + "/documents/import?action=emplace";
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::post_response(update_url, import_payload,
                                                         res, res_headers, {}, 10*1000, true);

            if(status_code != 200) {
                LOG(ERROR) << "Error while sending query suggestions events to leader. "
                           << "Status code: " << status_code << ", response: " << res;
            } else {
                LOG(INFO) << "Query aggregation for collection: " + suggestion_coll;
                popularQueries->reset_local_counts();

                if(raft_server->is_leader()) {
                    // try to run top-K compaction of suggestion collection
                    const std::string top_k_param = "count:" + std::to_string(popularQueries->get_k());
                    const std::string& truncate_topk_url = base_url + "/documents?top_k_by=" + top_k_param;
                    res.clear();
                    res_headers.clear();
                    status_code = HttpClient::delete_response(truncate_topk_url, res, res_headers, 10*1000, true);
                    if(status_code != 200) {
                        LOG(ERROR) << "Error while running top K for query suggestions collection. "
                                   << "Status code: " << status_code << ", response: " << res;
                    } else {
                        LOG(INFO) << "Top K aggregation for collection: " + suggestion_coll;
                    }
                }
            }
        }
    }
}

void AnalyticsManager::persist_click_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    // lock is held by caller
    nlohmann::json payload_json = nlohmann::json::array();

    for (const auto &click_events_collection_it: query_collection_click_events) {
        auto collection_id = CollectionManager::get_instance().get_collection(
                click_events_collection_it.first)->get_collection_id();
        for (const auto &click_event: click_events_collection_it.second) {
            // send http request
            nlohmann::json click_event_json;
            click_event.to_json(click_event_json);
            click_event_json["collection_id"] = std::to_string(collection_id);
            payload_json.push_back(click_event_json);
        }
    }

    if(payload_json.empty()) {
        return;
    }

    const std::string import_payload = payload_json.dump();

    std::string leader_url = raft_server->get_leader_url();
    if (!leader_url.empty()) {
        const std::string &base_url = leader_url + "analytics";
        std::string res;

        const std::string &update_url = base_url + "/click_events/replicate";
        std::map<std::string, std::string> res_headers;
        long status_code = HttpClient::post_response(update_url, import_payload,
                                                     res, res_headers, {}, 10 * 1000, true);

        if (status_code != 200) {
            LOG(ERROR) << "Error while sending click events to leader. "
                       << "Status code: " << status_code << ", response: " << res;
        } else {
            query_collection_click_events.clear();
        }
    }
}

void AnalyticsManager::stop() {
    quit = true;
    cv.notify_all();
}

void AnalyticsManager::dispose() {
    std::unique_lock lk(mutex);

    for(auto& kv: popular_queries) {
        delete kv.second;
    }

    popular_queries.clear();
}

void AnalyticsManager::init(Store* store, Store* analytics_store) {
    this->store = store;
    this->analytics_store = analytics_store;
}

std::unordered_map<std::string, PopularQueries*> AnalyticsManager::get_popular_queries() {
    std::unique_lock lk(mutex);
    return popular_queries;
}

nlohmann::json AnalyticsManager::get_click_events() {
    std::unique_lock lk(mutex);
    std::vector<std::string> click_event_jsons;
    nlohmann::json result_json = nlohmann::json::array();

    if (analytics_store) {
        analytics_store->scan_fill(std::string(CLICK_EVENT) + "_", std::string(CLICK_EVENT) + "`",
                                   click_event_jsons);

        for (const auto &click_event_json: click_event_jsons) {
            nlohmann::json click_event = nlohmann::json::parse(click_event_json);
            result_json.push_back(click_event);
        }
    }

    return result_json;
}

Option<bool> AnalyticsManager::write_click_event_to_store(nlohmann::json &click_event_jsons) {
    for(const auto& click_event_json : click_event_jsons) {
        auto collection_id = click_event_json["collection_id"].get<std::string>();
        auto timestamp = click_event_json["timestamp"].get<uint64_t>();
        const std::string key = std::string(CLICK_EVENT) + "_" + collection_id + "_" +
                                std::to_string(timestamp);
        if(analytics_store) {
            bool inserted = analytics_store->insert(key, click_event_json.dump());
            if (!inserted) {
                return Option<bool>(500, "Unable to insert clickevent into store.");
            }
        } else {
            return Option<bool>(500, "Analytics DB not initialized.");
        }
    }
    return Option<bool>(true);
}

void AnalyticsManager::resetRateLimit() {
    events_cache.clear();
}