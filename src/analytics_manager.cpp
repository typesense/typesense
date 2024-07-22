#include <mutex>
#include <thread>
#include "analytics_manager.h"
#include "tokenizer.h"
#include "http_client.h"
#include "collection_manager.h"
#include "string_utils.h"

#define EVENTS_RATE_LIMIT_SEC 60
#define EVENTS_RATE_LIMIT_COUNT 5

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

    if(payload["type"] == POPULAR_QUERIES_TYPE || payload["type"] == NOHITS_QUERIES_TYPE
        || payload["type"] == COUNTER_TYPE || payload["type"] == LOG_TYPE) {
        return create_index(payload, upsert, write_to_disk);
    }

    return Option<bool>(400, "Invalid type.");
}

Option<bool> AnalyticsManager::create_index(nlohmann::json &payload, bool upsert, bool write_to_disk) {
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

    size_t limit = 1000;
    bool expand_query = false;

    if(params.contains("limit") && params["limit"].is_number_integer()) {
        limit = params["limit"].get<size_t>();
    }

    if(params.contains("expand_query") && params["expand_query"].is_boolean()) {
        expand_query = params["expand_query"].get<bool>();
    }

    std::string counter_field;
    std::string suggestion_collection;

    suggestion_config_t suggestion_config;
    suggestion_config.name = suggestion_config_name;
    suggestion_config.limit = limit;
    suggestion_config.expand_query = expand_query;
    suggestion_config.rule_type = payload["type"];

    if(payload["type"] == LOG_TYPE) {
        if(!params["source"].contains("collections") || !params["source"]["collections"].is_array()) {
            return Option<bool>(400, "Must contain a valid list of source collections.");
        }

        if(!params["source"].contains("events") || (params["source"].contains("events") &&
                                                    (params["source"]["events"].empty()
                                                     || !params["source"]["events"].is_array()
                                                     || !params["source"]["events"][0].is_object()))) {
            return Option<bool>(400, "Bad or missing events.");
        }

        suggestion_config.events = params["source"]["events"];

        suggestion_collection = params["source"]["collections"][0].get<std::string>();
        suggestion_config.suggestion_collection = suggestion_collection;
    } else {
        if(payload["type"] == COUNTER_TYPE) {
            if(!params["source"].contains("events") || (params["source"].contains("events") &&
                                                        (params["source"]["events"].empty()
                                                         || !params["source"]["events"].is_array()
                                                         || !params["source"]["events"][0].is_object()))) {
                return Option<bool>(400, "Bad or missing events.");
            }

            suggestion_config.events = params["source"]["events"];
        }

        if(!params.contains("destination") || !params["destination"].is_object()) {
            return Option<bool>(400, "Bad or missing destination.");
        }

        if(!params["source"].contains("collections") || !params["source"]["collections"].is_array()) {
            return Option<bool>(400, "Must contain a valid list of source collections.");
        }

        if(!params["destination"].contains("collection") || !params["destination"]["collection"].is_string()) {
            return Option<bool>(400, "Must contain a valid destination collection.");
        }


        if(params["destination"].contains("counter_field")) {
            if(!params["destination"]["counter_field"].is_string()) {
                return Option<bool>(400, "Must contain a valid counter_field.");
            }
            counter_field = params["destination"]["counter_field"].get<std::string>();
            suggestion_config.counter_field = counter_field;
        }

        suggestion_collection = params["destination"]["collection"].get<std::string>();
        suggestion_config.suggestion_collection = suggestion_collection;
    }

    for(const auto& coll: params["source"]["collections"]) {
        if(!coll.is_string()) {
            return Option<bool>(400, "Must contain a valid list of source collection names.");
        }

        const std::string& src_collection = coll.get<std::string>();
        suggestion_config.query_collections.push_back(src_collection);
    }
    if(payload["type"] == POPULAR_QUERIES_TYPE) {
        if(!upsert && popular_queries.count(suggestion_collection) != 0) {
            return Option<bool>(400, "There's already another configuration for this destination collection.");
        }
    } else if(payload["type"] == NOHITS_QUERIES_TYPE) {
        if(!upsert && nohits_queries.count(suggestion_collection) != 0) {
            return Option<bool>(400, "There's already another configuration for this destination collection.");
        }
    } else if(payload["type"] == COUNTER_TYPE) {
        if(!upsert && counter_events.count(suggestion_collection) != 0) {
            return Option<bool>(400, "There's already another configuration for this destination collection.");
        }

        auto coll = CollectionManager::get_instance().get_collection(suggestion_collection).get();
        if(coll != nullptr) {
            if(!coll->contains_field(counter_field)) {
                return Option<bool>(404,
                                    "counter_field `" + counter_field + "` not found in destination collection.");
            }
        } else {
            return Option<bool>(404, "Collection `" + suggestion_collection + "` not found.");
        }
    }

    std::unique_lock lock(mutex);

    if(already_exists) {
        // remove the previous configuration with same name (upsert)
        Option<bool> remove_op = remove_index(suggestion_config_name);
        if(!remove_op.ok()) {
            return Option<bool>(500, "Error erasing the existing configuration.");;
        }
    }

    suggestion_configs.emplace(suggestion_config_name, suggestion_config);

    for(const auto& query_coll: suggestion_config.query_collections) {
        query_collection_mapping[query_coll].push_back(suggestion_collection);
    }

    if(payload["type"] == POPULAR_QUERIES_TYPE) {
        QueryAnalytics* popularQueries = new QueryAnalytics(limit);
        popularQueries->set_expand_query(suggestion_config.expand_query);
        popular_queries.emplace(suggestion_collection, popularQueries);
    } else if(payload["type"] == NOHITS_QUERIES_TYPE) {
        QueryAnalytics* noresultsQueries = new QueryAnalytics(limit);
        nohits_queries.emplace(suggestion_collection, noresultsQueries);
    } else if(payload["type"] == COUNTER_TYPE) {
        if(query_collection_events.count(suggestion_collection) == 0) {
            std::vector<event_t> vec;
            query_collection_events.emplace(suggestion_collection, vec);
        }

        std::map<std::string, uint16_t> event_weight_map;
        for(const auto& event: params["source"]["events"]) {
            if(!event.contains("name") || event_collection_map.count(event["name"]) != 0) {
                remove_index(suggestion_config_name);
                return Option<bool>(400, "Events must contain a unique name.");
            }
            if(!event.contains("weight") || !event["weight"].is_number()) {
                remove_index(suggestion_config_name);
                return Option<bool>(400, "Counter events must contain a weight value.");
            }

            //store event name to their weights
            //which can be used to keep counter events separate from non counter events
            bool log_to_store = false;
            if(event.contains("log_to_store")) {
                log_to_store = event["log_to_store"].get<bool>();

                if(log_to_store && !analytics_store) {
                    remove_index(suggestion_config_name);
                    return Option<bool>(400, "Event can't be logged when analytics-db is not defined.");
                }
            }
            event_weight_map[event["name"]] = event["weight"];
            event_type_collection ec{event["type"], suggestion_collection, log_to_store, suggestion_config_name};
            event_collection_map.emplace(event["name"], ec);
        }
        counter_events.emplace(suggestion_collection, counter_event_t{counter_field, {}, event_weight_map});
    } else if(payload["type"] == LOG_TYPE) {
        //multiple event types can be used with one collection
        //hence if no events are created for collection then only initialize it
        if(query_collection_events.count(suggestion_collection) == 0) {
            std::vector<event_t> vec;
            query_collection_events.emplace(suggestion_collection, vec);
        }

        for(const auto& event: params["source"]["events"]) {
            if(!event.contains("name") || event_collection_map.count(event["name"]) != 0) {
                return Option<bool>(400, "Events must contain a unique name.");
            }
            event_type_collection ec{event["type"], suggestion_collection, true, suggestion_config_name};
            event_collection_map.emplace(event["name"], ec);
        }
    }

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

    for(auto& kv: nohits_queries) {
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

Option<nlohmann::json> AnalyticsManager::get_rule(const std::string& name) {
    nlohmann::json rule;
    std::unique_lock lock(mutex);

    auto suggestion_config_it = suggestion_configs.find(name);
    if(suggestion_config_it == suggestion_configs.end()) {
        return Option<nlohmann::json>(404, "Rule not found.");
    }

    suggestion_config_it->second.to_json(rule);
    return Option<nlohmann::json>(rule);
}

Option<bool> AnalyticsManager::remove_rule(const std::string &name) {
    std::unique_lock lock(mutex);

    auto suggestion_configs_it = suggestion_configs.find(name);
    if(suggestion_configs_it != suggestion_configs.end()) {
        return remove_index(name);
    }

    return Option<bool>(404, "Rule not found.");
}

Option<bool> AnalyticsManager::remove_all_rules() {
    std::unique_lock lock(mutex);

    std::vector<std::string> rules_list;
    //populate rules to delete later
    for(const auto& suggestion_config_it : suggestion_configs) {
        rules_list.emplace_back(suggestion_config_it.first);
    }

    for(const auto& rule : rules_list) {
        remove_index(rule);
    }

    return Option<bool>(true);
}

Option<bool> AnalyticsManager::remove_index(const std::string &name) {
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

    if(nohits_queries.count(suggestion_collection) != 0) {
        delete nohits_queries[suggestion_collection];
        nohits_queries.erase(suggestion_collection);
    }

    if(counter_events.count(suggestion_collection) != 0) {
        counter_events.erase(suggestion_collection);
    }

    if(query_collection_events.count(suggestion_collection) != 0) {
        query_collection_events.erase(suggestion_collection);
    }

    suggestion_configs.erase(name);

    //remove corresponding events with rule
    auto it = event_collection_map.begin();
    for(it; it != event_collection_map.end(); ++it) {
        if(it->second.analytic_rule == name) {
            break;
        }
    }
    if(it != event_collection_map.end()) {
        event_collection_map.erase(it);
    }

    auto suggestion_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + name;
    bool erased = store->remove(suggestion_key);
    if(!erased) {
        return Option<bool>(500, "Error while deleting from disk.");
    }

    return Option<bool>(true);
}

void AnalyticsManager::add_suggestion(const std::string &query_collection,
                                      const std::string& query, const std::string& expanded_query,
                                      const bool live_query, const std::string& user_id) {
    // look up suggestion collections for the query collection
    std::unique_lock lock(mutex);
    const auto& suggestion_collections_it = query_collection_mapping.find(query_collection);
    if(suggestion_collections_it != query_collection_mapping.end()) {
        for(const auto& suggestion_collection: suggestion_collections_it->second) {
            const auto& popular_queries_it = popular_queries.find(suggestion_collection);
            if(popular_queries_it != popular_queries.end()) {
                popular_queries_it->second->add(query, expanded_query, live_query, user_id);
            }
        }
    }
}

Option<bool> AnalyticsManager::add_event(const std::string& client_ip, const std::string& event_type,
                                         const std::string& event_name, const nlohmann::json& event_json) {
    std::unique_lock lock(mutex);

    const auto event_collection_map_it = event_collection_map.find(event_name);
    if(event_collection_map_it == event_collection_map.end()) {
        return Option<bool>(404, "No analytics rule defined for event name " + event_name);
    }

    if(event_collection_map_it->second.event_type != event_type) {
        return Option<bool>(400, "event_type mismatch in analytic rules.");
    }

    const auto& query_collection = event_collection_map_it->second.collection;

    const auto& query_collection_events_it = query_collection_events.find(query_collection);
    if(query_collection_events_it != query_collection_events.end()) {
        auto &events_vec = query_collection_events_it->second;
#ifdef TEST_BUILD
        if (isRateLimitEnabled) {
#endif
        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        auto events_cache_it = events_cache.find(client_ip);

        if (events_cache_it != events_cache.end()) {
            // event found in events cache
            if ((now_ts_seconds - events_cache_it->second.last_update_time) < EVENTS_RATE_LIMIT_SEC) {
                if (events_cache_it->second.count >= EVENTS_RATE_LIMIT_COUNT) {
                    return Option<bool>(500, "event rate limit reached.");
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
#ifdef TEST_BUILD
        }
#endif
        auto now_ts_useconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        std::string query;
        std::string user_id;
        std::string doc_id;
        std::vector<std::pair<std::string, std::string>> custom_data;

        if (event_type == CUSTOM_EVENT) {
            for(auto itr = event_json.begin(); itr != event_json.end(); ++itr) {
                if (itr.key() == "query") {
                    query = itr.value().get<std::string>();
                } else if (itr.key()== "user_id") {
                    user_id = itr.value().get<std::string>();
                } else if (itr.key() == "doc_id") {
                    doc_id = itr.value().get<std::string>();
                } else {
                    auto kv = std::make_pair(itr.key(), itr.value().get<std::string>());
                    custom_data.push_back(kv);
                }
            }
        } else {
            query = event_json.contains("q") ? event_json["q"].get<std::string>() : "";
            user_id = event_json["user_id"].get<std::string>();
            doc_id = event_json["doc_id"].get<std::string>();
        }

        event_t event(query, event_type, now_ts_useconds, user_id, doc_id,
                      event_name, event_collection_map[event_name].log_to_store, custom_data);
        events_vec.emplace_back(event);

        if (!counter_events.empty()) {
            auto counter_events_it = counter_events.find(query_collection);
            if (counter_events_it != counter_events.end()) {
                auto event_weight_map_it = counter_events_it->second.event_weight_map.find(event_name);
                if (event_weight_map_it != counter_events_it->second.event_weight_map.end()) {
                    auto inc_val = event_weight_map_it->second;
                    counter_events_it->second.docid_counts[doc_id] += inc_val;
                } else {
                    LOG(ERROR) << "event_name " << event_name
                               << " not defined in analytic rule for counter events.";
                }
            } else {
                LOG(ERROR) << "collection " << query_collection << " not found in analytics rule.";
            }
        }
    }
    return Option<bool>(true);
}

void AnalyticsManager::add_nohits_query(const std::string &query_collection, const std::string &query,
                                        bool live_query, const std::string &user_id) {
    // look up suggestion collections for the query collection
    std::unique_lock lock(mutex);
    const auto& suggestion_collections_it = query_collection_mapping.find(query_collection);
    if(suggestion_collections_it != query_collection_mapping.end()) {
        for(const auto& suggestion_collection: suggestion_collections_it->second) {
            const auto& noresults_queries_it = nohits_queries.find(suggestion_collection);
            if(noresults_queries_it != nohits_queries.end()) {
                noresults_queries_it->second->add(query, query, live_query, user_id);
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

        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        if(now_ts_seconds - prev_persistence_s < Config::get_instance().get_analytics_flush_interval()) {
            // we will persist aggregation every hour
            // LOG(INFO) << "QuerySuggestions::run interval is less, continuing";
            continue;
        }

        persist_query_events(raft_server, prev_persistence_s);
        persist_events(raft_server, prev_persistence_s);
        persist_popular_events(raft_server, prev_persistence_s);

        prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        lk.unlock();
    }

    dispose();
}

void AnalyticsManager::persist_query_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    // lock is held by caller

    auto send_http_response = [&](QueryAnalytics* queryAnalyticsPtr,
            const std::string& import_payload, const std::string& suggestion_coll, const std::string& query_type) {
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
                LOG(ERROR) << "Error while sending "<< query_type <<" events to leader. "
                           << "Status code: " << status_code << ", response: " << res;
            } else {
                LOG(INFO) << "Query aggregation for collection: " + suggestion_coll;
                queryAnalyticsPtr->reset_local_counts();

                if(raft_server->is_leader()) {
                    // try to run top-K compaction of suggestion collection
                    const std::string top_k_param = "count:" + std::to_string(queryAnalyticsPtr->get_k());
                    const std::string& truncate_topk_url = base_url + "/documents?top_k_by=" + top_k_param;
                    res.clear();
                    res_headers.clear();
                    status_code = HttpClient::delete_response(truncate_topk_url, res, res_headers, 10*1000, true);
                    if(status_code != 200) {
                        LOG(ERROR) << "Error while running top K for " << query_type <<" suggestions collection. "
                                   << "Status code: " << status_code << ", response: " << res;
                    } else {
                        LOG(INFO) << "Top K aggregation for collection: " + suggestion_coll;
                    }
                }
            }
        }
    };


    for(const auto& suggestion_config: suggestion_configs) {
        const std::string& sink_name = suggestion_config.first;
        const std::string& suggestion_coll = suggestion_config.second.suggestion_collection;

        auto popular_queries_it = popular_queries.find(suggestion_coll);
        auto nohits_queries_it = nohits_queries.find(suggestion_coll);

        // need to prepare the counts as JSON docs for import into the suggestion collection
        // {"id": "432432", "q": "foo", "$operations": {"increment": {"count": 100}}}
        std::string import_payload;

        if(popular_queries_it != popular_queries.end()) {
            import_payload.clear();
            QueryAnalytics *popularQueries = popular_queries_it->second;

            // aggregate prefix queries to their final form
            auto now = std::chrono::system_clock::now().time_since_epoch();
            auto now_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
            popularQueries->compact_user_queries(now_ts_us);

            popularQueries->serialize_as_docs(import_payload);
            send_http_response(popularQueries, import_payload, suggestion_coll, "popular queries");
        }

        if(nohits_queries_it != nohits_queries.end()) {
            import_payload.clear();
            QueryAnalytics *nohitsQueries = nohits_queries_it->second;
            // aggregate prefix queries to their final form
            auto now = std::chrono::system_clock::now().time_since_epoch();
            auto now_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
            nohitsQueries->compact_user_queries(now_ts_us);

            nohitsQueries->serialize_as_docs(import_payload);
            send_http_response(nohitsQueries, import_payload, suggestion_coll, "nohits queries");
        }

        if(import_payload.empty()) {
            continue;
        }
    }
}

void AnalyticsManager::persist_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    // lock is held by caller

    auto send_http_response = [&](const std::string& import_payload) {
        // send http request
        if(raft_server == nullptr) {
            return;
        }

        std::string leader_url = raft_server->get_leader_url();
        if(!leader_url.empty()) {
            const std::string& base_url = leader_url + "analytics/";
            std::string res;

            const std::string& update_url = base_url + "aggregate_events";
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::post_response(update_url, import_payload,
                                                         res, res_headers, {}, 10*1000, true);

            if(status_code != 200) {
                LOG(ERROR) << "Error while sending "<<" log events to leader. "
                           << "Status code: " << status_code << ", response: " << res;
            }
        }
    };

    nlohmann::json payload = nlohmann::json::array();
    for (auto &events_collection_it: query_collection_events) {
        const auto& collection = events_collection_it.first;
        for (const auto &event: events_collection_it.second) {
            if (event.log_to_store) {
                nlohmann::json event_data;
                event.to_json(event_data, collection);
                payload.push_back(event_data);
            }
        }
        if(!payload.empty()) {
            send_http_response(payload.dump());
            events_collection_it.second.clear();
        }
    }
}

void AnalyticsManager::persist_popular_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    auto send_http_response = [&](const std::string& import_payload, const std::string& collection) {
        if (raft_server == nullptr) {
            return;
        }

        std::string leader_url = raft_server->get_leader_url();
        if (!leader_url.empty()) {
            const std::string &base_url = leader_url + "collections/" + collection;
            std::string res;

            const std::string &update_url = base_url + "/documents/import?action=update";
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::post_response(update_url, import_payload,
                                                         res, res_headers, {}, 10 * 1000, true);

            if (status_code != 200) {
                LOG(ERROR) << "Error while sending popular_clicks events to leader. "
                           << "Status code: " << status_code << ", response: " << res;
            }
        }
    };

    for(auto& counter_event_it : counter_events) {
        auto coll = counter_event_it.first;
        std::string docs;
        counter_event_it.second.serialize_as_docs(docs);
        send_http_response(docs, coll);
        counter_event_it.second.docid_counts.clear();
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

    for(auto& kv: nohits_queries) {
        delete kv.second;
    }

    nohits_queries.clear();

    suggestion_configs.clear();

    query_collection_mapping.clear();

    counter_events.clear();

    query_collection_events.clear();

    event_collection_map.clear();

    events_cache.clear();
}

void AnalyticsManager::init(Store* store, Store* analytics_store) {
    this->store = store;
    this->analytics_store = analytics_store;

    if(analytics_store) {
        events_cache.capacity(1024);
    }
}

std::unordered_map<std::string, QueryAnalytics*> AnalyticsManager::get_popular_queries() {
    std::unique_lock lk(mutex);
    return popular_queries;
}

std::unordered_map<std::string, QueryAnalytics*> AnalyticsManager::get_nohits_queries() {
    std::unique_lock lk(mutex);
    return nohits_queries;
}

std::unordered_map<std::string, counter_event_t> AnalyticsManager::get_popular_clicks() {
    std::unique_lock lk(mutex);
    return counter_events;
}

void AnalyticsManager::resetToggleRateLimit(bool toggle) {
    std::unique_lock lk(mutex);
    events_cache.clear();
    isRateLimitEnabled = toggle;
}

void counter_event_t::serialize_as_docs(std::string &docs) {
    for (auto kv: docid_counts) {
        nlohmann::json doc;
        doc["id"] = kv.first;
        doc["$operations"]["increment"][counter_field] = kv.second;
        docs += doc.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore) + "\n";
    }

    if (!docs.empty()) {
        docs.pop_back();
    }
}

bool AnalyticsManager::write_to_db(const nlohmann::json& payload) {
    if(analytics_store) {
        for(const auto& event: payload) {
            std::string key = event["user_id"].get<std::string>() + "_" + event["type"].get<std::string>()
                    + "_" + StringUtils::serialize_uint64_t(event["timestamp"].get<uint64_t>());

            bool inserted = analytics_store->insert(key, event.dump());
            if(!inserted) {
                LOG(ERROR) << "Error while dumping events to analytics db.";
                return false;
            }
        }
    } else {
        LOG(ERROR) << "Analytics DB not initialized!!";
        return false;
    }

    return true;
}

void AnalyticsManager::get_last_N_events(const std::string& prefix, uint32_t N, std::vector<std::string>& values) {
    const std::string userid_prefix = prefix + "_";
    analytics_store->get_last_N_values(userid_prefix, N, values);
}

void event_t::to_json(nlohmann::json& obj, const std::string& coll) const {
    obj["query"] = query;
    obj["type"] = event_type;
    obj["timestamp"] = timestamp;
    obj["user_id"] = user_id;
    obj["doc_id"] = doc_id;
    obj["name"] = name;
    obj["collection"] = coll;

    if(event_type == "custom") {
        for(const auto& kv : data) {
            obj[kv.first] = kv.second;
        }
    }
}