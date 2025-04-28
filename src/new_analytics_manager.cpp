#include <mutex>
#include <thread>
#include "new_analytics_manager.h"
#include "tokenizer.h"
#include "http_client.h"
#include "collection_manager.h"
#include "string_utils.h"

#define EVENTS_RATE_LIMIT_SEC 60

void NewAnalyticsManager::persist_db_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    // Lock held by caller NewAnalyticsManager::run
    LOG(INFO) << "NewAnalyticsManager::persist_db_events";

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

    for(auto& counter_event_it : doc_analytics.get_doc_counter_events()) {
      const auto& collection = counter_event_it.second.destination_collection;
      std::string docs;
      counter_event_it.second.serialize_as_docs(docs);
      update_counter_events(docs, collection, "update");
      doc_analytics.reset_local_counter(counter_event_it.first);
    }

    for(auto& counter_event_it : query_analytic.get_query_counter_events()) {
      const auto& collection = counter_event_it.second.destination_collection;
      std::string docs;
      counter_event_it.second.serialize_as_docs(docs);
      update_counter_events(docs, collection, "emplace");
      query_analytic.reset_local_counter(counter_event_it.first);
      limit_to_top_k(collection, counter_event_it.second.limit);
    }
}

void NewAnalyticsManager::persist_analytics_db_events(ReplicationState *raft_server, uint64_t prev_persistence_s) {
    // Lock held by caller NewAnalyticsManager::run
    LOG(INFO) << "NewAnalyticsManager::persist_analytics_db_events";
    auto send_http_response = [&](const std::string& import_payload) {
        if(raft_server == nullptr) {
            return;
        }
        
        std::string leader_url = raft_server->get_leader_url();
        if(!leader_url.empty()) {
            const std::string& base_url = leader_url + "new_analytics/";
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

    for(const auto& log_rule : query_analytic.get_query_log_events()) {
      const auto& collection = query_analytic.get_query_rule(log_rule.first).collection;
      for(const auto& event_data : log_rule.second) {
        nlohmann::json event_json;
        event_data.to_json(event_json, collection, log_rule.first);
        payload.push_back(event_json);
      }
      if(!payload.empty()) {
        send_http_response(payload.dump());
        payload.clear();
        query_analytic.reset_local_log_events(log_rule.first);
      }
    }
}

Option<bool> NewAnalyticsManager::add_external_event(const std::string& client_ip, const nlohmann::json& event_data) {
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
      auto add_event_op = query_analytic.add_event(client_ip, event_data);
      if(!add_event_op.ok()) {
        return Option<bool>(400, add_event_op.error());
      }
    }
    return Option<bool>(true);
}

Option<bool> NewAnalyticsManager::add_internal_event(const nlohmann::json& event_data) {
    // TODO: Implement
    return Option<bool>(true);
}

Option<nlohmann::json> NewAnalyticsManager::get_events(const std::string& userid, const std::string& event_name, uint32_t N) {
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
      query_analytic.get_events(userid, event_name, N, in_memory_values);
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

Option<nlohmann::json> NewAnalyticsManager::list_rules(const std::string& rule_tag) {
    std::shared_lock lock(mutex);
    auto doc_list_rules_op = doc_analytics.list_rules(rule_tag);
    auto query_list_rules_op = query_analytic.list_rules(rule_tag);
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


Option<nlohmann::json> NewAnalyticsManager::get_rule(const std::string& name) {
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
      get_rule_op = query_analytic.get_rule(name);
    }
    if (get_rule_op.ok()) {
        return Option<nlohmann::json>(get_rule_op.get());
    }
    return Option<nlohmann::json>(400, get_rule_op.error());
}

Option<bool> NewAnalyticsManager::create_rule(nlohmann::json& payload, bool update, bool write_to_disk) {
    std::unique_lock lock(mutex);
    std::string name;
    if (!update) {
      if (!payload.contains("name") || !payload["name"].is_string()) {
        return Option<bool>(400, "Name is required when creating a new analytics rule");
      }
      name = payload["name"].get<std::string>();

      if(rules_map.find(name) != rules_map.end()) {
        return Option<bool>(400, "Rule already exists");
      }

      if(!payload.contains("event_type") || !payload["event_type"].is_string()) {
        return Option<bool>(400, "Event type is required when creating a new analytics rule");
      }
      const std::string& event_type = payload["event_type"].get<std::string>();

      if(!payload.contains("type") || !payload["type"].is_string()) {
        return Option<bool>(400, "Type is required when creating a new analytics rule");
      }
      const std::string& type = payload["type"].get<std::string>();

      if (!payload.contains("collection") || !payload["collection"].is_string()) {
        return Option<bool>(400, "Collection is required when creating a new analytics rule");
      }
      const std::string& collection = payload["collection"].get<std::string>();

      auto collection_ptr = CollectionManager::get_instance().get_collection(collection);
      if (collection_ptr == nullptr) {
        return Option<bool>(400, "Collection " + collection + " does not exist");
      }

      if(payload.contains("rule_tag") && !payload["rule_tag"].is_string()) {
        return Option<bool>(400, "Rule tag should be a string");
      }

      bool is_doc_rule = doc_analytics.check_rule_type(event_type, type);
      bool is_query_rule = query_analytic.check_rule_type(event_type, type);
      if(!is_doc_rule && !is_query_rule) {
        return Option<bool>(400, "Event type or type is invalid (or) combination of both is invalid");
      }

      Option<nlohmann::json> create_rule_op(500, "Internal server error");
      if(is_doc_rule) {
        create_rule_op = doc_analytics.create_rule(payload, update);
      } else if(is_query_rule) {
        create_rule_op = query_analytic.create_rule(payload, update);
      }

      if(create_rule_op.ok()) {
        std::string rule_json = create_rule_op.get().dump();
        if(write_to_disk) {
          auto rule_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + name;
          bool store_op = store->insert(rule_key, rule_json);
          if(!store_op) {
              return Option<bool>(500, "Error while storing the config to disk.");
          }
        }
        rules_map.emplace(name, (is_doc_rule) ? "doc" : "query");
      }

      if(!create_rule_op.ok()) {
        return Option<bool>(400, create_rule_op.error());
      }

    } else {
      if(payload.contains("type")) {
        return Option<bool>(400, "Rule type cannot be changed");
      } else if (payload.contains("collection")) {
        return Option<bool>(400, "Rule collection cannot be changed");
      } else if (payload.contains("event_type")) {
        return Option<bool
          >(400, "Rule event type cannot be changed");
      }
      if(payload.contains("rule_tag") && !payload["rule_tag"].is_string()) {
        return Option<bool>(400, "Rule tag should be a string");
      }
      name = payload["name"].get<std::string>();
      bool is_doc_rule = rules_map.find(name) != rules_map.end() && rules_map.find(name)->second == "doc";
      bool is_query_rule = rules_map.find(name) != rules_map.end() && rules_map.find(name)->second == "query";
      if (!is_doc_rule && !is_query_rule) {
        return Option<bool>(400, "Rule not found");
      }

      Option<nlohmann::json> update_rule_op(500, "Internal server error");
      if(is_doc_rule) {
        update_rule_op = doc_analytics.create_rule(payload, update);
      } else if(is_query_rule) {
        update_rule_op = query_analytic.create_rule(payload, update);
      }

      if(update_rule_op.ok()) {
        std::string rule_json = update_rule_op.get().dump();
        if(write_to_disk) {
          auto rule_key = std::string(ANALYTICS_RULE_PREFIX) + "_" + name;
          bool store_op = store->insert(rule_key, rule_json);
          if(!store_op) {
              return Option<bool>(500, "Error while storing the config to disk.");
          }
        }
      }

      if(!update_rule_op.ok()) {
        return Option<bool>(400, update_rule_op.error());
      }
    }

    return Option<bool>(true);
}

Option<bool> NewAnalyticsManager::remove_rule(const std::string& name) {
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
      auto remove_rule_op = query_analytic.remove_rule(name);
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

void NewAnalyticsManager::remove_all_rules() {
    std::unique_lock lock(mutex);
    doc_analytics.remove_all_rules();
    query_analytic.remove_all_rules();
    rules_map.clear();
}

void NewAnalyticsManager::resetToggleRateLimit(bool toggle) {
    std::unique_lock lock(mutex);
    external_events_cache.clear();
    isRateLimitEnabled = toggle;
}

bool NewAnalyticsManager::write_to_db(const nlohmann::json& payload) {
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

void NewAnalyticsManager::run(ReplicationState* raft_server) {
    uint64_t prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count();

    while(!quit) {
        std::unique_lock lk(mutex);
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

        persist_db_events(raft_server, prev_persistence_s);
        persist_analytics_db_events(raft_server, prev_persistence_s);

        prev_persistence_s = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        lk.unlock();
    }

    dispose();
}

void NewAnalyticsManager::init(Store* store, Store* analytics_store, uint32_t analytics_minute_rate_limit) {
    this->store = store;
    this->analytics_store = analytics_store;
    this->analytics_minute_rate_limit = analytics_minute_rate_limit;

    if(analytics_store) {
        external_events_cache.capacity(1024);
    }
}

void NewAnalyticsManager::stop() {
    quit = true;
    dispose();
    cv.notify_all();
}

void NewAnalyticsManager::dispose() {
    std::unique_lock lk(mutex);
    external_events_cache.clear();
    rules_map.clear();
    doc_analytics.dispose();
}

NewAnalyticsManager::~NewAnalyticsManager() {}