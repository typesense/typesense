#include <mutex>
#include "doc_analytics.h"
#include "tokenizer.h"
#include "collection_manager.h"

void doc_event_t::to_json(nlohmann::json& obj, const std::string& coll) const {
    obj["query"] = query;
    obj["event_type"] = event_type;
    obj["timestamp"] = timestamp;
    obj["user_id"] = user_id;
    
    if(!doc_ids.empty()) {
      obj["doc_ids"] = doc_ids;
    } else if(!doc_id.empty()) {
      obj["doc_id"] = doc_id;
    }
    obj["name"] = name;
    obj["collection"] = coll;

    if(event_type == "custom") {
      for(const auto& kv : data) {
        obj[kv.first] = kv.second;
      }
    }
}

void doc_counter_event_t::serialize_as_docs(std::string& docs) {
  for(const auto& kv : docid_counts) {
    nlohmann::json doc;
    doc["id"] = kv.first;
    doc["$operations"]["increment"][counter_field] = kv.second;
    docs += doc.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore) + "\n";
  }

  if(!docs.empty()) {
    docs.pop_back();
  }
}

bool DocAnalytics::check_rule_type(const std::string& event_type, const std::string& type) {
    if ( event_type == CLICK_EVENT || event_type == CONVERSION_EVENT || 
         event_type == VISIT_EVENT || event_type == CUSTOM_EVENT) {
        
        if(type == COUNTER_TYPE || type == LOG_TYPE) {
            return true;
        }
    }
    return false;
}

Option<bool> DocAnalytics::add_event(const std::string& client_ip, const nlohmann::json& event_data) {
    std::unique_lock lock(mutex);
    auto now_ts_useconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
    const auto& event_name = event_data["name"].get<std::string>();
    const auto& data = event_data["data"].get<nlohmann::json>();
    if (data.contains("doc_ids") && data.contains("doc_id")) {
      return Option<bool>(400, "doc_ids and doc_id cannot both be present");
    }
    
    if (!data.contains("doc_ids") && !data.contains("doc_id")) {
      return Option<bool>(400, "doc_ids or doc_id is required");
    }

    if (data.contains("doc_ids") && !data["doc_ids"].is_array()) {
      return Option<bool>(400, "doc_ids should be an array");
    }

    if (data.contains("doc_id") && !data["doc_id"].is_string()) {
      return Option<bool>(400, "doc_id should be a string");
    }

    const auto& type = doc_rules.find(event_data["name"].get<std::string>())->second.type;
    const auto& event_type = doc_rules.find(event_data["name"].get<std::string>())->second.event_type;
    if(type == COUNTER_TYPE) {
      const auto& counter_event_it = doc_counter_events.find(event_name);
      if (counter_event_it == doc_counter_events.end()) {
        return Option<bool>(400, "Rule does not exist");
      }
      const auto& inc_val = counter_event_it->second.weight;
      if(data.contains("doc_ids")) {
        for(const auto& doc_id : data["doc_ids"].get<std::vector<std::string>>()) {
          counter_event_it->second.docid_counts[doc_id] += inc_val;
        }
      } else {
        counter_event_it->second.docid_counts[data["doc_id"].get<std::string>()] += inc_val;
      }
    } else if(type == LOG_TYPE) {
      const auto& log_event_it = doc_log_events.find(event_name);
      if (log_event_it == doc_log_events.end()) {
        return Option<bool>(400, "Rule does not exist");
      }
      std::vector<std::pair<std::string, std::string>> data_vec;
      if(event_type == CUSTOM_EVENT) {
        for(const auto& kv : data.items()) {
          data_vec.push_back(std::make_pair(kv.key(), kv.value().get<std::string>()));
        }
      }
      if (data.contains("doc_ids")) {
        log_event_it->second.push_back(doc_event_t{
          data.contains("query") ? data["query"].get<std::string>() : "",
          event_type,
          uint64_t(now_ts_useconds),
          data["user_id"].get<std::string>(),
          "",
          data["doc_ids"].get<std::vector<std::string>>(),
          event_name,
          data_vec
        });
      } else if (data.contains("doc_id")) {
        log_event_it->second.push_back(doc_event_t{
          data.contains("query") ? data["query"].get<std::string>() : "",
          event_type,
          uint64_t(now_ts_useconds),
          data["user_id"].get<std::string>(),
          data["doc_id"].get<std::string>(),
          std::vector<std::string>(),
          event_name,
          data_vec
        });
      }
    }
    return Option<bool>(true);
}

Option<nlohmann::json> DocAnalytics::create_rule(nlohmann::json& payload, bool update, bool is_live_req) {
    std::unique_lock lock(mutex);
    if(update) {
      if(doc_rules.find(payload["name"].get<std::string>()) == doc_rules.end()) {
        return Option<nlohmann::json>(400, "Rule does not exist");
      }
      nlohmann::json existing_rule;
      doc_rules.find(payload["name"].get<std::string>())->second.to_json(existing_rule);
      if (payload.contains("rule_tag")) {
        existing_rule["rule_tag"] = payload["rule_tag"].get<std::string>();
      }
      if(payload.contains("params")) {
        if(payload["params"].contains("counter_field")) {
          existing_rule["params"]["counter_field"] = payload["params"]["counter_field"].get<std::string>();
        }
        if(payload["params"].contains("destination_collection")) {
          existing_rule["params"]["destination_collection"] = payload["params"]["destination_collection"].get<std::string>();
        }
        if(payload["params"].contains("weight")) {
          existing_rule["params"]["weight"] = payload["params"]["weight"].get<uint32_t>();
        }
      }
      payload = existing_rule;
    }

    if(payload["type"] == COUNTER_TYPE) {
        if(
            !payload.contains("params") || 
            !payload["params"].contains("counter_field") || 
            !payload["params"]["counter_field"].is_string() || 
            payload["params"]["counter_field"].get<std::string>().empty()
          ) {
            return Option<nlohmann::json>(400, "Counter field is required when creating a counter type rule");
        }
        if (
            payload["params"].contains("destination_collection") &&
            !payload["params"]["destination_collection"].is_string() &&
            payload["params"]["destination_collection"].get<std::string>().empty()
          ) {
            return Option<nlohmann::json>(400, "Destination collection should be a string");
        }

        if (
            !payload["params"].contains("weight") ||
            !payload["params"]["weight"].is_number_unsigned() ||
            payload["params"]["weight"].get<uint32_t>() == 0
          ) {
            return Option<nlohmann::json>(400, "Weight should be a number greater than 0");
          }

        if (is_live_req) {
          if(payload.contains("params") && payload["params"].contains("destination_collection")) {
            auto collection_ptr = CollectionManager::get_instance().get_collection(payload["params"]["destination_collection"].get<std::string>());
            if(collection_ptr == nullptr) {
              return Option<nlohmann::json>(400, "Destination collection does not exist");
            }
          }
        }
        doc_counter_event_t counter_event;
        counter_event.counter_field = payload["params"]["counter_field"].get<std::string>();
        counter_event.weight = payload["params"]["weight"].get<uint32_t>();
        if(payload["params"].contains("destination_collection")) {
          counter_event.destination_collection = payload["params"]["destination_collection"].get<std::string>();
        } else {
          counter_event.destination_collection = payload["collection"].get<std::string>();
        }
        if(update) {
          auto existing_doc_counter_event_it = doc_counter_events.find(payload["name"].get<std::string>());
          if(existing_doc_counter_event_it != doc_counter_events.end()) {
            counter_event.docid_counts = existing_doc_counter_event_it->second.docid_counts;
            doc_counter_events.erase(existing_doc_counter_event_it);
          }
          doc_rules.erase(payload["name"].get<std::string>());
        }
        doc_counter_events[payload["name"].get<std::string>()] = counter_event;
        doc_rules[payload["name"].get<std::string>()] = doc_rule_config_t{
          payload["name"].get<std::string>(),
          payload["type"].get<std::string>(),
          payload["collection"].get<std::string>(),
          payload["event_type"].get<std::string>(),
          payload["params"]["counter_field"].get<std::string>(),
          payload.contains("rule_tag") ? payload["rule_tag"].get<std::string>() : "",
          payload["params"]["weight"].get<uint32_t>(),
          payload["params"].contains("destination_collection") ? payload["params"]["destination_collection"].get<std::string>() : payload["collection"].get<std::string>(),
        };
    }

    if(payload["type"] == LOG_TYPE) {
        if(!update) {
          doc_log_events[payload["name"].get<std::string>()] = std::vector<doc_event_t>();
        }
        if (update) {
          auto it = doc_rules.find(payload["name"].get<std::string>());
          doc_rules.erase(it);
        }
        doc_rules[payload["name"].get<std::string>()] = doc_rule_config_t{
          payload["name"].get<std::string>(),
          payload["type"].get<std::string>(),
          payload["collection"].get<std::string>(),
          payload["event_type"].get<std::string>(),
          "",
          payload.contains("rule_tag") ? payload["rule_tag"].get<std::string>() : "",
          0,
          "",
        };
    }
    return Option<nlohmann::json>(payload);

}

Option<bool> DocAnalytics::remove_rule(const std::string& name) {
    std::unique_lock lock(mutex);
    auto it = doc_rules.find(name);
    auto doc_counter_event_it = doc_counter_events.find(name);
    auto doc_log_event_it = doc_log_events.find(name);
    if(it == doc_rules.end()) {
      return Option<bool>(400, "Rule does not exist");
    }
    doc_rules.erase(it);
    if(doc_counter_event_it != doc_counter_events.end()) {
      doc_counter_events.erase(doc_counter_event_it);
    }
    if(doc_log_event_it != doc_log_events.end()) {
      doc_log_events.erase(doc_log_event_it);
    }
    return Option<bool>(true);
}

void DocAnalytics::get_events(const std::string& userid, const std::string& event_name, uint32_t N, std::vector<std::string>& values) {
    std::shared_lock lock(mutex);
    auto it = doc_log_events.find(event_name);
    if(it == doc_log_events.end()) {
      return;
    }
    const auto& collection = doc_rules.find(event_name)->second.collection;
    for(const auto& event : it->second) {
      if(event.user_id == userid) {
        nlohmann::json obj;
        event.to_json(obj, collection);
        values.push_back(obj.dump());
      }
    }
    std::reverse(values.begin(), values.end());
    if(values.size() > N) {
      values.resize(N);
    }
}
Option<nlohmann::json> DocAnalytics::list_rules(const std::string& rule_tag) {
    std::shared_lock lock(mutex);
    nlohmann::json rules = nlohmann::json::array();
    for (const auto& [key, value] : doc_rules) {
        if (rule_tag.empty() || value.rule_tag == rule_tag) {
            nlohmann::json rule;
            value.to_json(rule);
            rules.emplace_back(rule);
        }
    }
    return Option<nlohmann::json>(rules);
}

Option<nlohmann::json> DocAnalytics::get_rule(const std::string& name) {
    std::shared_lock lock(mutex);
    auto it = doc_rules.find(name);
    if(it == doc_rules.end()) {
      return Option<nlohmann::json>(400, "Rule does not exist");
    }

    nlohmann::json obj;
    it->second.to_json(obj);
    return Option(obj);
}

void DocAnalytics::reset_local_counter(const std::string& event_name) {
  std::unique_lock lock(mutex);
  auto it = doc_counter_events.find(event_name);
  if(it == doc_counter_events.end()) {
    return;
  }
  it->second.docid_counts.clear();
}

void DocAnalytics::reset_local_log_events(const std::string& event_name) {
  std::unique_lock lock(mutex);
  auto it = doc_log_events.find(event_name);
  if(it == doc_log_events.end()) {
    return;
  }
  it->second.clear();
}

std::unordered_map<std::string, doc_counter_event_t> DocAnalytics::get_doc_counter_events() {
  std::unique_lock lock(mutex);
  return doc_counter_events;
}

std::unordered_map<std::string, std::vector<doc_event_t>> DocAnalytics::get_doc_log_events() {
  std::unique_lock lock(mutex);
  return doc_log_events;
}

doc_rule_config_t DocAnalytics::get_doc_rule(const std::string& name) {
  std::shared_lock lock(mutex);
  return doc_rules.find(name)->second;
}

void DocAnalytics::remove_all_rules() {
    std::unique_lock lock(mutex);
    doc_rules.clear();
    doc_counter_events.clear();
    doc_log_events.clear();
}

void DocAnalytics::dispose() {
    remove_all_rules();
} 