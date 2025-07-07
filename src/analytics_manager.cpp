#include <mutex>
#include <thread>
#include "analytics_manager.h"
#include "tokenizer.h"
#include "http_client.h"
#include "collection_manager.h"
#include "string_utils.h"

#define EVENTS_RATE_LIMIT_SEC 60

void AnalyticsManager::persist_db_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    LOG(INFO) << "AnalyticsManager::persist_db_events";
    const uint64_t now_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count();

    auto update_counter_events = [&](const std::string& import_payload, const std::string& collection, const std::string& operation) {
        if (raft_server == nullptr) {
            return;
        }

        std::string leader_url = raft_server->get_leader_url();
        if (!leader_url.empty()) {
            const std::string &base_url = leader_url + "collections/" + collection;
            std::string res;

            const std::string &update_url = base_url + "/documents/import?action=" + operation;
            std::map<std::string, std::string> res_headers;
            long status_code = HttpClient::post_response(update_url, import_payload,
                                                         res, res_headers, {}, 10 * 1000, true);

            if (status_code != 200) {
                LOG(ERROR) << "Error while sending update_counter_events to leader. " 
                           << "Collection: " << collection << ", operation: " << operation
                           << "Status code: " << status_code << ", response: " << res;
            }
        }
    };

    auto limit_to_top_k = [&](const std::string& collection, const uint32_t limit) {
      if (raft_server == nullptr) {
          return;
      }

      std::string leader_url = raft_server->get_leader_url();
      if (!leader_url.empty()) {
        const std::string& base_url = leader_url + "collections/" + collection;
        const std::string top_k_param = "count:" + std::to_string(limit);
        const std::string& truncate_topk_url = base_url + "/documents?top_k_by=" + top_k_param;
        std::string res;
        std::map<std::string, std::string> res_headers;
        long status_code = HttpClient::delete_response(truncate_topk_url, res, res_headers, 10*1000, true);
        if (status_code != 200) {
          LOG(ERROR) << "Error while limit_to_top_k for collection: " << collection
                     << "Status code: " << status_code << ", response: " << res;
        }
      }
    };

    std::unique_lock lk(mutex);
    std::vector<std::vector<std::string>> doc_analytics_queue;
    for(auto& counter_event_it : doc_analytics.get_doc_counter_events()) {
      const auto& collection = counter_event_it.second.destination_collection;
      std::string docs;
      counter_event_it.second.serialize_as_docs(docs);
      doc_analytics_queue.push_back({docs, collection});
      doc_analytics.reset_local_counter(counter_event_it.first);
    }
    
    std::vector<std::vector<std::string>> query_analytics_queue;
    query_analytics.compact_all_user_queries(now_ts_us);
    for(auto& counter_event_it : query_analytics.get_query_counter_events()) {
      const auto& collection = counter_event_it.second.destination_collection;
      std::string docs;
      counter_event_it.second.serialize_as_docs(docs);
      query_analytics_queue.push_back({docs, collection, std::to_string(counter_event_it.second.limit)});
      query_analytics.reset_local_counter(counter_event_it.first);
    }

    const size_t rules_size = rules_map.size();
    lk.unlock();

    const size_t delay_interval = Config::get_instance().get_analytics_flush_interval() / (doc_analytics_queue.size() + query_analytics_queue.size() + 1);
    for(const auto& params : doc_analytics_queue) {
      update_counter_events(params[0], params[1], "update");
      if (rules_size > DELAY_WRITE_RULE_SIZE) {
        std::this_thread::sleep_for(std::chrono::seconds(delay_interval));
      }
    }

    for(const auto& params : query_analytics_queue) {
      update_counter_events(params[0], params[1], "emplace");
      limit_to_top_k(params[1], std::stoi(params[2]));
      if (rules_size > DELAY_WRITE_RULE_SIZE) {
        std::this_thread::sleep_for(std::chrono::seconds(delay_interval));
      }
    }
}

void AnalyticsManager::persist_analytics_db_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    std::unique_lock lk(mutex);
    LOG(INFO) << "AnalyticsManager::persist_analytics_db_events";
    auto send_http_response = [&](const std::string& import_payload) {
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

    /* Get the log events from doc_analytics and query_analytics and send them to the leader */

    nlohmann::json payload = nlohmann::json::array();
    for(const auto& log_rule : doc_analytics.get_doc_log_events()) {
      const auto& collection = doc_analytics.get_doc_rule(log_rule.first).collection;
      for(const auto& event_data : log_rule.second) {
        nlohmann::json event_json;
        event_data.to_json(event_json, collection);
        payload.push_back(event_json);
      }
      if(!payload.empty()) {
        send_http_response(payload.dump());
        payload.clear();
        doc_analytics.reset_local_log_events(log_rule.first);
      }
    }

    for(const auto& log_rule : query_analytics.get_query_log_events()) {
      const auto& collection = query_analytics.get_query_rule(log_rule.first).collection;
      for(const auto& event_data : log_rule.second) {
        nlohmann::json event_json;
        event_data.to_json(event_json, collection, log_rule.first);
        payload.push_back(event_json);
      }
      if(!payload.empty()) {
        send_http_response(payload.dump());
        payload.clear();
        query_analytics.reset_local_log_events(log_rule.first);
      }
    }
}

Option<bool> AnalyticsManager::add_external_event(const std::string& client_ip, const nlohmann::json& event_data) {
    std::unique_lock lock(mutex);
#ifdef TEST_BUILD
    if (isRateLimitEnabled) {
#endif
    auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

    auto events_cache_it = external_events_cache.find(client_ip);
    
    if (events_cache_it != external_events_cache.end()) {
          // event found in events cache
          if ((now_ts_seconds - events_cache_it->second.last_update_time) < EVENTS_RATE_LIMIT_SEC) {
              if (events_cache_it->second.count >= analytics_minute_rate_limit) {
                  return Option<bool>(500, "event rate limit reached.");
              } else {
                  events_cache_it->second.count++;
              }
          } else {
              events_cache_it->second.last_update_time = now_ts_seconds;
              events_cache_it->second.count = 1;
          }
      } else {
          external_event_cache_t eventCache{(uint64_t) now_ts_seconds, 1};
          external_events_cache.insert(client_ip, eventCache);
      }
#ifdef TEST_BUILD
    }
#endif
    
    if (!event_data.contains("event_type") || !event_data["event_type"].is_string()) {
      return Option<bool>(400, "Event type is required");
    }

    if (!event_data.contains("name") || !event_data["name"].is_string()) {
      return Option<bool>(400, "name is required");
    }

    if (!event_data.contains("data") || !event_data["data"].is_object()) {
      return Option<bool>(400, "data is required");
    }
    const auto& data = event_data["data"];

    if (data.contains("user_id") && !data["user_id"].is_string()) {
      return Option<bool>(400, "user_id should be a string");
    }

    bool is_doc_event = rules_map.find(event_data["name"].get<std::string>()) != rules_map.end() && rules_map.find(event_data["name"].get<std::string>())->second == "doc";
    bool is_query_event = rules_map.find(event_data["name"].get<std::string>()) != rules_map.end() && rules_map.find(event_data["name"].get<std::string>())->second == "query";

    if(!is_doc_event && !is_query_event) {
      return Option<bool>(400, "Rule not found");
    }

    if(is_doc_event) {
      auto add_event_op = doc_analytics.add_event(client_ip, event_data);
      if(!add_event_op.ok()) {
        return Option<bool>(400, add_event_op.error());
      }
    } else if(is_query_event) {
      auto add_event_op = query_analytics.add_event(client_ip, event_data);
      if(!add_event_op.ok()) {
        return Option<bool>(400, add_event_op.error());
      }
    }
    return Option<bool>(true);
}

Option<bool> AnalyticsManager::add_internal_event(const query_internal_event_t& event_data) {
    return query_analytics.add_internal_event(event_data);
}

Option<nlohmann::json> AnalyticsManager::get_events(const std::string& userid, const std::string& event_name, uint32_t N) {
    std::shared_lock lock(mutex);
    std::vector<std::string> in_memory_values;
    std::vector<std::string> db_values;
    if (N > 1000) {
        return Option<nlohmann::json>(400, "N cannot be greater than 1000");
    }

    bool is_doc_rule = rules_map.find(event_name) != rules_map.end() && rules_map.find(event_name)->second == "doc";
    bool is_query_rule = rules_map.find(event_name) != rules_map.end() && rules_map.find(event_name)->second == "query";
    if(is_doc_rule) {
      doc_analytics.get_events(userid, event_name, N, in_memory_values);
    } else if(is_query_rule) {
      query_analytics.get_events(userid, event_name, N, in_memory_values);
    } else {
      return Option<nlohmann::json>(400, "Rule not found");
    }

    // If we already have enough events from memory, return them
    if (in_memory_values.size() >= N) {
        if (in_memory_values.size() > N) {
            in_memory_values.resize(N);
        }
        nlohmann::json response;
        response["events"] = nlohmann::json::array();
        for(const auto& event: in_memory_values) {
            response["events"].push_back(nlohmann::json::parse(event));
        }
        return Option<nlohmann::json>(response);
    }

    // Get remaining events from database
    std::string user_id = userid;
    user_id.erase(std::remove(user_id.begin(), user_id.end(), '%'), user_id.end());
    auto userid_prefix = user_id + "%" + event_name;
    uint32_t remaining_needed = N - in_memory_values.size();
    analytics_store->get_last_N_values(userid_prefix, remaining_needed, db_values);

    if (!db_values.empty()) {
        std::vector<std::pair<uint64_t, std::string>> all_events;
        
        // Add in-memory events with timestamps
        for (const auto& event_str : in_memory_values) {
            auto event_json = nlohmann::json::parse(event_str);
            uint64_t timestamp = event_json["timestamp"];
            all_events.emplace_back(timestamp, event_str);
        }
        
        // Add database events with timestamps
        for (const auto& event_str : db_values) {
            try {
                auto event_json = nlohmann::json::parse(event_str);
                uint64_t timestamp = event_json["timestamp"];
                all_events.emplace_back(timestamp, event_str);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Error parsing event JSON: " << e.what();
                // Skip invalid events
            }
        }
        
        // Sort all events by timestamp in descending order
        std::sort(all_events.begin(), all_events.end(), 
                  [](const auto& a, const auto& b) { return a.first > b.first; });
        
        // Take top N events
        std::vector<std::string> values;
        values.reserve(std::min(all_events.size(), static_cast<size_t>(N)));
        
        for (size_t i = 0; i < all_events.size() && i < N; i++) {
            values.push_back(all_events[i].second);
        }

        // Deduplicate events based on timestamp and user_id
        std::unordered_set<std::string> seen_events;
        std::vector<std::string> deduped_values;
        deduped_values.reserve(values.size());

        for (const auto& event_str : values) {
            auto event_json = nlohmann::json::parse(event_str);
            std::string dedup_key = std::to_string(event_json["timestamp"].get<uint64_t>()) + 
                                    event_json["user_id"].get<std::string>();
            
            if (seen_events.insert(dedup_key).second) {
                deduped_values.push_back(event_str);
            }
        }

        // Convert to JSON response
        nlohmann::json response;
        response["events"] = nlohmann::json::array();
        for(const auto& event: deduped_values) {
            response["events"].push_back(nlohmann::json::parse(event));
        }
        return Option<nlohmann::json>(response);
    }

    // If no database events, just return in-memory events
    nlohmann::json response;
    response["events"] = nlohmann::json::array();
    for(const auto& event: in_memory_values) {
        response["events"].push_back(nlohmann::json::parse(event));
    }
    return Option<nlohmann::json>(response);
}

Option<nlohmann::json> AnalyticsManager::list_rules(const std::string& rule_tag) {
    std::shared_lock lock(mutex);
    auto doc_list_rules_op = doc_analytics.list_rules(rule_tag);
    auto query_list_rules_op = query_analytics.list_rules(rule_tag);
    if (doc_list_rules_op.ok() && query_list_rules_op.ok()) {
      nlohmann::json response = nlohmann::json::array();
      for(const auto& rule: doc_list_rules_op.get()) {
        response.push_back(rule);
      }
      for(const auto& rule: query_list_rules_op.get()) {
        response.push_back(rule);
      }
      return Option<nlohmann::json>(response);
    } else {
      if(!doc_list_rules_op.ok()) {
        return Option<nlohmann::json>(400, doc_list_rules_op.error());
      } else {
        return Option<nlohmann::json>(400, query_list_rules_op.error());
      }
    }
}


Option<nlohmann::json> AnalyticsManager::get_rule(const std::string& name) {
    std::shared_lock lock(mutex);
    auto rule_type = rules_map.find(name);
    if(rule_type == rules_map.end()) {
      return Option<nlohmann::json>(400, "Rule not found");
    }
    bool is_doc_rule = rule_type->second == "doc";
    bool is_query_rule = rule_type->second == "query";
    Option<nlohmann::json> get_rule_op(500, "Internal server error");
    if(is_doc_rule) {
      get_rule_op = doc_analytics.get_rule(name);
    }
    if(is_query_rule) {
      get_rule_op = query_analytics.get_rule(name);
    }
    if (get_rule_op.ok()) {
        return Option<nlohmann::json>(get_rule_op.get());
    }
    return Option<nlohmann::json>(400, get_rule_op.error());
}

Option<nlohmann::json> AnalyticsManager::create_rule(nlohmann::json& payload, bool update, bool write_to_disk, bool is_live_req) {
    std::unique_lock lock(mutex);
    std::string name;
    Option<nlohmann::json> op_response(500, "Internal server error");

    if (!update) {
        // Validations for creating a new rule
        if (!payload.contains("name") || !payload["name"].is_string() || payload["name"].get<std::string>().empty()) {
            return Option<nlohmann::json>(400, "Name is required when creating an analytics rule");
        }
        name = payload["name"].get<std::string>();

        if (rules_map.find(name) != rules_map.end()) {
            return Option<nlohmann::json>(400, "Rule already exists");
        }

        if (!payload.contains("event_type") || !payload["event_type"].is_string()) {
            return Option<nlohmann::json>(400, "Event type is required when creating a new analytics rule");
        }
        const std::string& event_type = payload["event_type"].get<std::string>();

        if (!payload.contains("type") || !payload["type"].is_string()) {
            return Option<nlohmann::json>(400, "Type is required when creating a new analytics rule");
        }
        const std::string& type = payload["type"].get<std::string>();

        if (!payload.contains("collection") || !payload["collection"].is_string()) {
            return Option<nlohmann::json>(400, "Collection is required when creating a new analytics rule");
        }
        const std::string& collection = payload["collection"].get<std::string>();

        if (is_live_req) {
            auto collection_ptr = CollectionManager::get_instance().get_collection(collection);
            if (collection_ptr == nullptr) {
                return Option<nlohmann::json>(400, "Collection " + collection + " does not exist");
            }
        }

        if (payload.contains("rule_tag") && !payload["rule_tag"].is_string()) {
            return Option<nlohmann::json>(400, "Rule tag should be a string");
        }

        bool is_doc_rule = doc_analytics.check_rule_type(event_type, type);
        bool is_query_rule = query_analytics.check_rule_type(event_type, type);
        if (!is_doc_rule && !is_query_rule) {
            return Option<nlohmann::json>(400, "Event type or type is invalid (or) combination of both is invalid");
        }

        if (is_doc_rule) {
            op_response = doc_analytics.create_rule(payload, update, is_live_req);
        } else {
            op_response = query_analytics.create_rule(payload, update, is_live_req);
        }

        if (!op_response.ok()) {
            return Option<nlohmann::json>(400, op_response.error());
        }

        // Persist the created rule to disk
        if (write_to_disk) {
            auto rule_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + name;
            bool store_op = store->insert(rule_key, op_response.get().dump());
            if (!store_op) {
                return Option<nlohmann::json>(500, "Error while storing the config to disk.");
            }
        }

        rules_map.emplace(name, is_doc_rule ? "doc" : "query");
    } else {
        // Validations for updating an existing rule
        if (payload.contains("type")) {
            return Option<nlohmann::json>(400, "Rule type cannot be changed");
        }
        if (payload.contains("collection")) {
            return Option<nlohmann::json>(400, "Rule collection cannot be changed");
        }
        if (payload.contains("event_type")) {
            return Option<nlohmann::json>(400, "Rule event type cannot be changed");
        }
        if (payload.contains("rule_tag") && !payload["rule_tag"].is_string()) {
            return Option<nlohmann::json>(400, "Rule tag should be a string");
        }
        name = payload["name"].get<std::string>();
        bool is_doc_rule = (rules_map.find(name) != rules_map.end() && rules_map[name] == "doc");
        bool is_query_rule = (rules_map.find(name) != rules_map.end() && rules_map[name] == "query");
        if (!is_doc_rule && !is_query_rule) {
            return Option<nlohmann::json>(400, "Rule not found");
        }

        if (is_doc_rule) {
            op_response = doc_analytics.create_rule(payload, update, is_live_req);
        } else {
            op_response = query_analytics.create_rule(payload, update, is_live_req);
        }

        if (!op_response.ok()) {
            return Option<nlohmann::json>(400, op_response.error());
        }

        // Persist the updated rule to disk
        if (write_to_disk) {
            auto rule_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + name;
            bool store_op = store->insert(rule_key, op_response.get().dump());
            if (!store_op) {
                return Option<nlohmann::json>(500, "Error while storing the config to disk.");
            }
        }
    }

    return Option<nlohmann::json>(op_response.get());
}

Option<bool> AnalyticsManager::create_old_rule(nlohmann::json& payload) {
    if (!payload.contains("name") || !payload["name"].is_string()) {
      return Option<bool>(400, "Rule Migration failed. name is required");
    }
    const std::string& name = payload["name"].get<std::string>();
    if(rules_map.find(name) != rules_map.end()) {
      return Option<bool>(400, "Rule Migration failed. Rule already exists");
    }
    if(!payload.contains("type") || !payload["type"].is_string()) {
      return Option<bool>(400, "Rule Migration failed. type is required");
    }
    const std::string& type = payload["type"].get<std::string>();
    if(type != DocAnalytics::COUNTER_TYPE && type != QueryAnalytics::POPULAR_QUERIES_TYPE && type != QueryAnalytics::NO_HIT_QUERIES_TYPE) {
      return Option<bool>(400, "Rule Migration failed. " + type + " is invalid");
    }

    if(!payload.contains("params") || !payload["params"].is_object()) {
      return Option<bool>(400, "Rule Migration failed. params is required");
    }
    const auto& params = payload["params"];

    const auto& destination = params["destination"];
    if(destination.empty()) {
      return Option<bool>(400, "Rule Migration failed. params.destination is empty");
    }

    if(!destination.contains("collection") || !destination["collection"].is_string()) {
      return Option<bool>(400, "Rule Migration failed. params.destination.collection is required");
    }
    const std::string& destination_collection = destination["collection"].get<std::string>();
    if(destination_collection.empty()) {
      return Option<bool>(400, "Rule Migration failed. params.destination.collection is empty");
    }

    const auto source = params["source"];
    if(source.empty()) {
      return Option<bool>(400, "Rule Migration failed. params.source is empty");
    }

    const auto& collections_json = source["collections"];
    if(collections_json.empty() || !collections_json.is_array()) {
      return Option<bool>(400, "Rule Migration failed. params.collections is required");
    }

    std::vector<std::string> collections;
    for(const auto& collection: collections_json) {
      collections.push_back(collection.get<std::string>());
    }

    bool duplicate_name = collections.size() != 1;

    if (type == QueryAnalytics::POPULAR_QUERIES_TYPE || type == QueryAnalytics::NO_HIT_QUERIES_TYPE) {
      const std::string& event_type = "query";
      size_t limit = 0;
      if(params.contains("limit") && params["limit"].is_number_integer()) {
        limit = params["limit"].get<size_t>();
      }
      
      bool enable_auto_aggregation = true;
      if(source.contains("enable_auto_aggregation") && source["enable_auto_aggregation"].is_boolean()) {
        enable_auto_aggregation = source["enable_auto_aggregation"].get<bool>();
      }

      std::vector<std::string> meta_fields;
      if(params.contains("meta_fields") && params["meta_fields"].is_array()) {
        for(const auto& meta_field: params["meta_fields"]) {
          meta_fields.push_back(meta_field.get<std::string>());
        }
      }
      
      std::string event_name = name;
      std::vector<nlohmann::json> events;
      if(source.contains("events") && source["events"].is_array()) {
        for(const auto& event: source["events"]) {
          events.push_back(event);
        }
      }

      if(events.size() > 1) {
        return Option<bool>(400, "Rule Migration failed. params.events should have only one event");
      }

      if(events.size() == 1) {
        event_name = events[0]["name"].get<std::string>();
        if(event_name.empty()) {
          return Option<bool>(400, "Rule Migration failed. params.events.name is empty");
        }
      }

      bool expand_query = false;
      if(params.contains("expand_query") && params["expand_query"].is_boolean()) {
        expand_query = params["expand_query"].get<bool>();
      }

      for(const auto& collection: collections) {
        nlohmann::json payload = {
          {"name", duplicate_name ? event_name + "_" + collection : event_name},
          {"type", type},
          {"collection", collection},
          {"event_type", event_type},
          {"rule_tag", name},
          {"params", {
            {"destination_collection", destination_collection},
            {"source", source},
            {"limit", limit},
            {"capture_search_requests", enable_auto_aggregation},
            {"expand_query", expand_query},
            {"meta_fields", meta_fields},
          }
        }};
        auto create_op = create_rule(payload, false, true, false);
        if(!create_op.ok()) {
          return Option<bool>(400, create_op.error());
        }
      }
    } else if (type == DocAnalytics::COUNTER_TYPE) {
      if (!destination.contains("counter_field") || !destination["counter_field"].is_string()) {
        return Option<bool>(400, "Rule Migration failed. params.destination.counter_field is required");
      }
      const std::string& counter_field = destination["counter_field"].get<std::string>();
      if(counter_field.empty()) {
        return Option<bool>(400, "Rule Migration failed. params.destination.counter_field is empty");
      }

      if(!source.contains("events") || !source["events"].is_array()) {
        return Option<bool>(400, "Rule Migration failed. params.events is required");
      }

      const auto& events = source["events"];
      if(events.empty()) {
        return Option<bool>(400, "Rule Migration failed. params.events is empty");
      }

      for(const auto& event: events) {
        if(!event.contains("name") || !event["name"].is_string()) {
          return Option<bool>(400, "Rule Migration failed. params.events.name is required");
        }
        const std::string& event_name = event["name"].get<std::string>();
        if(event_name.empty()) {
          return Option<bool>(400, "Rule Migration failed. params.events.name is empty");
        }

        size_t weight = 1;
        if(event.contains("weight") && event["weight"].is_number_integer()) {
          weight = event["weight"].get<size_t>();
        }

        if(!event.contains("type") || !event["type"].is_string()) {
          return Option<bool>(400, "Rule Migration failed. params.events.type is required");
        }
        const std::string& event_type = event["type"].get<std::string>();
        if(event_type.empty()) {
          return Option<bool>(400, "Rule Migration failed. params.events.type is empty");
        }

        for(const auto& collection: collections) {
          nlohmann::json payload = {
            {"name", duplicate_name ? event_name + "_" + collection : event_name},
            {"type", type},
            {"collection", collection},
            {"event_type", event_type},
            {"rule_tag", name},
            {"params", {
              {"destination_collection", destination_collection},
              {"counter_field", counter_field},
              {"weight", weight},
            }}
          };
          auto create_op = create_rule(payload, false, true, false);
          if(!create_op.ok()) {
            return Option<bool>(400, create_op.error());
          }
        }
      }
    } else {
      return Option<bool>(400, "Rule Migration failed. " + type + " is invalid");
    }
    store->remove(std::string(OLD_ANALYTICS_RULE_PREFIX) + "_" + name);
    return Option<bool>(true);
}

Option<bool> AnalyticsManager::remove_rule(const std::string& name) {
    std::unique_lock lock(mutex);
    const bool is_doc_rule = rules_map.find(name) != rules_map.end() && rules_map.find(name)->second == "doc";
    const bool is_query_rule = rules_map.find(name) != rules_map.end() && rules_map.find(name)->second == "query";
    if(!is_doc_rule && !is_query_rule) {
      return Option<bool>(400, "Rule not found");
    }
    if(is_doc_rule) {
      auto remove_rule_op = doc_analytics.remove_rule(name);
      if(!remove_rule_op.ok()) {
        return Option<bool>(400, remove_rule_op.error());
      }
    } else if(is_query_rule) {
      auto remove_rule_op = query_analytics.remove_rule(name);
      if(!remove_rule_op.ok()) {
        return Option<bool>(400, remove_rule_op.error());
      }
    }
    auto rule_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + name;
    bool store_op = store->remove(rule_key);
    if(!store_op) {
      return Option<bool>(500, "Error while removing the config from disk.");
    }
    rules_map.erase(name);
    
    return Option<bool>(true);
}

void AnalyticsManager::remove_all_rules() {
    std::unique_lock lock(mutex);
    doc_analytics.remove_all_rules();
    query_analytics.remove_all_rules();
    rules_map.clear();
}

void AnalyticsManager::resetToggleRateLimit(bool toggle) {
    std::unique_lock lock(mutex);
    external_events_cache.clear();
    isRateLimitEnabled = toggle;
}

bool AnalyticsManager::write_to_db(const nlohmann::json& payload) {
    if(analytics_store) {
        for(const auto& event: payload) {
            std::string userid = event["user_id"].get<std::string>();
            userid.erase(std::remove(userid.begin(), userid.end(), '%'), userid.end());
            std::string event_name = event["name"].get<std::string>();
            std::string ts = StringUtils::serialize_uint64_t(event["timestamp"].get<uint64_t>());

            std::string key =  userid + "%" + event_name + "%" + ts;

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

void AnalyticsManager::run(ReplicationState* raft_server) {
    uint64_t prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count();

    while(!quit) {
        std::unique_lock lk(quit_mutex);
        cv.wait_for(lk, std::chrono::seconds(QUERY_COMPACTION_INTERVAL_S), [&] { return quit.load(); });
        
        if(quit) {
            lk.unlock();
            break;
        }

        LOG(INFO) << "AnalyticsManager::run";

        auto now_ts_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        if(now_ts_seconds - prev_persistence_s < Config::get_instance().get_analytics_flush_interval()) {
            // we will persist aggregation every hour
            // LOG(INFO) << "QuerySuggestions::run interval is less, continuing";
            continue;
        }

        prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        
        persist_analytics_db_events(raft_server, prev_persistence_s);
        persist_db_events(raft_server, prev_persistence_s);
    }

    dispose();
}

Option<nlohmann::json> AnalyticsManager::process_create_rule_request(nlohmann::json& payload, bool is_live_req) {
  if (payload.is_array()) {
    std::vector<nlohmann::json> responses;
    for (auto& rule : payload) {
      auto create_op = create_rule(rule, false, true, is_live_req);
      if (!create_op.ok()) {
        responses.push_back(nlohmann::json({{"error", create_op.error()}}));
      } else {
        responses.push_back(create_op.get());
      }
    }
    return Option<nlohmann::json>(responses);
  } else {
    auto create_op = create_rule(payload, false, true, is_live_req);
    if (!create_op.ok()) {
      return Option<nlohmann::json>(400, create_op.error());
    }
    return Option<nlohmann::json>(create_op.get());
  }
}

void AnalyticsManager::init(Store* store, Store* analytics_store, uint32_t analytics_minute_rate_limit) {
    this->store = store;
    this->analytics_store = analytics_store;
    this->analytics_minute_rate_limit = analytics_minute_rate_limit;

    if(analytics_store) {
        external_events_cache.capacity(1024);
    }
}

void AnalyticsManager::stop() {
    std::unique_lock lk(quit_mutex);
    quit = true;
    lk.unlock();
    dispose();
    cv.notify_all();
}

void AnalyticsManager::dispose() {
    std::unique_lock lk(mutex);
    external_events_cache.clear();
    rules_map.clear();
    doc_analytics.dispose();
    query_analytics.dispose();
}

AnalyticsManager::~AnalyticsManager() {}