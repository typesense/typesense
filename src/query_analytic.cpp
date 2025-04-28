#include <mutex>
#include "query_analytic.h"
#include "tokenizer.h"
#include "collection_manager.h"

void query_event_t::to_json(nlohmann::json& obj, const std::string& coll, const std::string& name) const {
  obj["query"] = query;
  obj["type"] = event_type;
  obj["timestamp"] = timestamp;
  obj["user_id"] = user_id;

  if(!filter_str.empty()) {
    obj["filter_by"] = filter_str;
  }

  if(!tag_str.empty()) {
    obj["analytics_tag"] = tag_str;
  }

  obj["collection"] = coll;
  obj["name"] = name;
}

void query_counter_event_t::serialize_as_docs(std::string& docs) {
  for(const auto& kv : query_counts) {
    nlohmann::json doc;
    doc["id"] = std::to_string(StringUtils::hash_wy(kv.first.query.c_str(), kv.first.query.size()));
    doc["q"] = kv.first.query;
    if (meta_fields.find("filter_by") != meta_fields.end() && kv.first.filter_str.empty()) {
      doc["filter_by"] = kv.first.filter_str;
    }

    if (meta_fields.find("analytics_tag") != meta_fields.end() && !kv.first.tag_str.empty()) {
      doc["analytics_tag"] = kv.first.tag_str;
    }
    doc["$operations"]["increment"]["count"] = kv.second;
    docs += doc.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore) + "\n";
  }

  if(!docs.empty()) {
    docs.pop_back();
  }
}

bool QueryAnalytic::check_rule_type(const std::string& event_type, const std::string& type) {
  if(event_type == QUERY_EVENT) {
    return type == LOG_TYPE || type == NO_HIT_QUERIES_TYPE || type == POPULAR_QUERIES_TYPE;
  }

  return false;
}

bool QueryAnalytic::check_rule_type_collection(const std::string& collection, const std::string& type) {
  auto collection_map_it = collection_rules_map.find(collection);
  if(collection_map_it == collection_rules_map.end()) {
    return false;
  }

  for(const auto& rule_name : collection_map_it->second) {
    if(query_rules.find(rule_name)->second.type == type) {
      return true;
    }
  }
  return false;
}

Option<bool> QueryAnalytic::add_event(const std::string& client_ip, const nlohmann::json& event_data) {
  std::unique_lock lock(mutex);
  const auto& event_type = event_data["event_type"].get<std::string>();
  auto now_ts_useconds = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count();
  const auto& event_name = event_data["name"].get<std::string>();
  const auto& data = event_data["data"].get<nlohmann::json>();

  if(!data.contains("q") || !data["q"].is_string()) {
    return Option<bool>(400, "'q' should be a string and is required");
  }

  if(data.contains("filter_by") && !data["filter_by"].is_string()) {
    return Option<bool>(400, "'filter_by' should be a string");
  }

  if(data.contains("analytics_tag") && !data["analytics_tag"].is_string()) {
    return Option<bool>(400, "'analytics_tag' should be a string");
  }
  const auto& type = query_rules.find(event_name)->second.type;

  if(type == POPULAR_QUERIES_TYPE || type == NO_HIT_QUERIES_TYPE) {
    const auto& counter_event_it = query_counter_events.find(event_name);
    if(counter_event_it == query_counter_events.end()) {
      return Option<bool>(400, "Rule does not exist");
    }
    query_event_t query_event{
      data["q"].get<std::string>(),
      event_type,
      uint64_t(now_ts_useconds),
      data["user_id"].get<std::string>(),
      data.contains("filter_by") ? data["filter_by"].get<std::string>() : "",
      data.contains("analytics_tag") ? data["analytics_tag"].get<std::string>() : ""
    };
    auto& query_counts = counter_event_it->second.query_counts;
    auto it = query_counts.find(query_event);
    // skip count when map has become too large (to prevent abuse)
    if(it == query_counts.end() && query_counts.size() < counter_event_it->second.limit * 2) {
      query_counts.emplace(query_event, 1);
    } else {
      it->second++;
    }
  } else if (type == LOG_TYPE) {
    const auto& log_event_it = query_log_events.find(event_name);
    if(log_event_it == query_log_events.end()) {
      return Option<bool>(400, "Rule does not exist");
    }
    log_event_it->second.push_back(query_event_t{
      data["q"].get<std::string>(),
      event_type,
      uint64_t(now_ts_useconds),
      data["user_id"].get<std::string>(),
      data.contains("filter_by") ? data["filter_by"].get<std::string>() : "",
      data.contains("analytics_tag") ? data["analytics_tag"].get<std::string>() : ""
    });
  }
  
  return Option<bool>(true);
}

Option<nlohmann::json> QueryAnalytic::create_rule(nlohmann::json& payload, bool update) {
  std::unique_lock lock(mutex);
  if(update) {
    if(query_rules.find(payload["name"].get<std::string>()) == query_rules.end()) {
      return Option<nlohmann::json>(400, "Rule does not exist");
    }
    nlohmann::json existing_rule;
    query_rules.find(payload["name"].get<std::string>())->second.to_json(existing_rule);
    if(payload.contains("rule_tag")) {
      existing_rule["rule_tag"] = payload["rule_tag"].get<std::string>();
    }
    if(payload.contains("params")) {
      if(payload["params"].contains("limit")) {
        existing_rule["params"]["limit"] = payload["params"]["limit"].get<uint32_t>();
      }
      if(payload["params"].contains("destination_collection")) {
        existing_rule["params"]["destination_collection"] = payload["params"]["destination_collection"].get<std::string>();
      }
      if(payload["params"].contains("expand_query")) {
        existing_rule["params"]["expand_query"] = payload["params"]["expand_query"].get<bool>();
      }
      if(payload["params"].contains("capture_search_requests")) {
        existing_rule["params"]["capture_search_requests"] = payload["params"]["capture_search_requests"].get<bool>();
      }
      if(payload["params"].contains("meta_fields")) {
        existing_rule["params"]["meta_fields"] = payload["params"]["meta_fields"].get<std::set<std::string>>();
      }
    }
    payload = existing_rule;
  }
  
  if(
      payload.contains("params") && 
      payload["params"].contains("expand_query") && 
      !payload["params"]["expand_query"].is_boolean()
    ) {
      return Option<nlohmann::json>(400, "Expand query should be a boolean");
  }

  if(
      payload.contains("params") && 
      payload["params"].contains("capture_search_requests") && 
      !payload["params"]["capture_search_requests"].is_boolean()
    ) {
      return Option<nlohmann::json>(400, "Capture search requests should be a boolean");
  }

  if (
    payload.contains("params") && 
    payload["params"].contains("meta_fields") && 
    !payload["params"]["meta_fields"].is_array()
  ) {
    return Option<nlohmann::json>(400, "Meta fields should be an array of strings");
  }

  if(payload.contains("params") && payload["params"].contains("meta_fields")) {
    auto meta_fields = payload["params"]["meta_fields"];
    for(const auto& meta_field : meta_fields) {
      if(meta_field.is_string() && !meta_field.get<std::string>().empty()) {
        if(meta_field.get<std::string>() != "filter_by" && meta_field.get<std::string>() != "analytics_tag") {
          return Option<nlohmann::json>(400, "Meta field should be either filter_by or analytics_tag");
        }
      } else {
        return Option<nlohmann::json>(400, "Meta field should a non-empty string");
      }
    }
  }

  if(payload["type"] == NO_HIT_QUERIES_TYPE || payload["type"] == POPULAR_QUERIES_TYPE) {
    if(
        !payload.contains("params") || 
        !payload["params"].contains("destination_collection") || 
        !payload["params"]["destination_collection"].is_string() || 
        payload["params"]["destination_collection"].get<std::string>().empty()
      ) {
        return Option<nlohmann::json>(400, "Destination collection is required when creating a no hit queries or popular queries type rule");
    }

    if(payload.contains("params") && payload["params"].contains("destination_collection")) {
      auto collection_ptr = CollectionManager::get_instance().get_collection(payload["params"]["destination_collection"].get<std::string>());
      if(collection_ptr == nullptr) {
        return Option<nlohmann::json>(400, "Destination collection does not exist");
      }
    }

    if (
      !payload["params"].contains("limit") ||
      !payload["params"]["limit"].is_number_unsigned() ||
      payload["params"]["limit"].get<uint32_t>() == 0
    ) {
      return Option<nlohmann::json>(400, "Limit should be a number greater than 0");
    }

    query_counter_event_t counter_event;
    counter_event.destination_collection = payload["params"]["destination_collection"].get<std::string>();
    counter_event.meta_fields = payload["params"].contains("meta_fields") ? payload["params"]["meta_fields"].get<std::set<std::string>>() : std::set<std::string>();
    counter_event.limit = payload["params"]["limit"].get<uint32_t>();
    if(update) {
      auto existing_query_counter_event_it = query_counter_events.find(payload["name"].get<std::string>());
      if(existing_query_counter_event_it != query_counter_events.end()) {
        counter_event.query_counts = existing_query_counter_event_it->second.query_counts;
        query_counter_events.erase(existing_query_counter_event_it);
      }
    }
    query_counter_events.emplace(payload["name"].get<std::string>(), counter_event);
    query_rules.emplace(payload["name"].get<std::string>(), query_rule_config_t{
      payload["name"].get<std::string>(),
      payload["type"].get<std::string>(),
      payload["collection"].get<std::string>(),
      payload["event_type"].get<std::string>(),
      payload.contains("rule_tag") ? payload["rule_tag"].get<std::string>() : "",
      payload["params"]["limit"].get<uint32_t>(),
      payload["params"]["destination_collection"].get<std::string>(),
      payload["params"].contains("expand_query") ? payload["params"]["expand_query"].get<bool>() : false,
      payload["params"].contains("capture_search_requests") ? payload["params"]["capture_search_requests"].get<bool>() : true,
      payload["params"].contains("meta_fields") ? payload["params"]["meta_fields"].get<std::set<std::string>>() : std::set<std::string>()
    });
    if(!update) {
      auto collection_rules_map_it = collection_rules_map.find(payload["collection"].get<std::string>());
      if(collection_rules_map_it == collection_rules_map.end()) {
        collection_rules_map.emplace(payload["collection"].get<std::string>(), std::vector<std::string>());
        collection_rules_map_it = collection_rules_map.find(payload["collection"].get<std::string>());
      }
      collection_rules_map_it->second.push_back(payload["name"].get<std::string>());
    }
  }

  if (payload["type"] == LOG_TYPE) {
    if(!update) {
      query_log_events.emplace(payload["name"].get<std::string>(), std::vector<query_event_t>());
    }
    if (update) {
      query_rules.erase(payload["name"].get<std::string>());
    }
    query_rules.emplace(payload["name"].get<std::string>(), query_rule_config_t{
      payload["name"].get<std::string>(),
      payload["type"].get<std::string>(),
      payload["collection"].get<std::string>(),
      payload["event_type"].get<std::string>(),
      payload.contains("rule_tag") ? payload["rule_tag"].get<std::string>() : "",
      0,
      "",
      payload["params"].contains("expand_query") ? payload["params"]["expand_query"].get<bool>() : false,
      payload["params"].contains("capture_search_requests") ? payload["params"]["capture_search_requests"].get<bool>() : true,
      payload["params"].contains("meta_fields") ? payload["params"]["meta_fields"].get<std::set<std::string>>() : std::set<std::string>()
    });
    if(!update) {
      auto collection_rules_map_it = collection_rules_map.find(payload["collection"].get<std::string>());
      if(collection_rules_map_it == collection_rules_map.end()) {
        collection_rules_map.emplace(payload["collection"].get<std::string>(), std::vector<std::string>());
        collection_rules_map_it = collection_rules_map.find(payload["collection"].get<std::string>());
      }
      collection_rules_map_it->second.push_back(payload["name"].get<std::string>());
    }
  }
  return Option<nlohmann::json>(payload);
}
  

Option<bool> QueryAnalytic::remove_rule(const std::string& name) {
  std::unique_lock lock(mutex);
  auto it = query_rules.find(name);
  auto query_counter_event_it = query_counter_events.find(name);
  auto query_log_event_it = query_log_events.find(name);
  if(it == query_rules.end()) {
    return Option<bool>(400, "Rule does not exist");
  }
  auto collection_rules_map_it = collection_rules_map.find(it->second.collection);
  if(collection_rules_map_it != collection_rules_map.end()) {
    auto rule_name_it = std::find(collection_rules_map_it->second.begin(), collection_rules_map_it->second.end(), name);
    if(rule_name_it != collection_rules_map_it->second.end()) {
      collection_rules_map_it->second.erase(rule_name_it);
    }
  }
  query_rules.erase(it);
  if(query_counter_event_it != query_counter_events.end()) {
    query_counter_events.erase(query_counter_event_it);
  }
  if(query_log_event_it != query_log_events.end()) {
    query_log_events.erase(query_log_event_it);
  }
  return Option<bool>(true);
}

void QueryAnalytic::get_events(const std::string& userid, const std::string& event_name, uint32_t N, std::vector<std::string>& values) {
  std::shared_lock lock(mutex);
  auto it = query_log_events.find(event_name);
  if(it == query_log_events.end()) {
    return;
  }
  const auto& collection = query_rules.find(event_name)->second.collection;
  for(const auto& event : it->second) {
    if(event.user_id == userid) {
      nlohmann::json obj;
      event.to_json(obj, collection, event_name);
      values.push_back(obj.dump());
    }
  }
  std::reverse(values.begin(), values.end());
  if(values.size() > N) {
    values.resize(N);
  }
}

Option<nlohmann::json> QueryAnalytic::list_rules(const std::string& rule_tag) {
  std::shared_lock lock(mutex);
  nlohmann::json rules = nlohmann::json::array();
  for(const auto& [key, value] : query_rules) {
    if(rule_tag.empty() || value.rule_tag == rule_tag) {
      nlohmann::json rule;
      value.to_json(rule);
      rules.emplace_back(rule);
    }
  }
  return Option<nlohmann::json>(rules);
}

Option<nlohmann::json> QueryAnalytic::get_rule(const std::string& name) {
  std::shared_lock lock(mutex);
  auto it = query_rules.find(name);
  if(it == query_rules.end()) {
    return Option<nlohmann::json>(400, "Rule does not exist");
  }
  nlohmann::json rule;
  it->second.to_json(rule);
  return Option<nlohmann::json>(rule);
}

void QueryAnalytic::compact_single_user_queries(uint64_t now_ts_us) {
  // TODO: Implement this
}

void QueryAnalytic::compact_all_user_queries(uint64_t now_ts_us) {
  // TODO: Implement this
}

void QueryAnalytic::reset_local_counter(const std::string& event_name) {
  std::unique_lock lock(mutex);
  auto it = query_counter_events.find(event_name);
  if(it == query_counter_events.end()) {
    return;
  }
  it->second.query_counts.clear();
}

void QueryAnalytic::reset_local_log_events(const std::string& event_name) {
  std::unique_lock lock(mutex);
  auto it = query_log_events.find(event_name);
  if(it == query_log_events.end()) {
    return;
  }
  it->second.clear();
}

std::unordered_map<std::string, query_counter_event_t> QueryAnalytic::get_query_counter_events() {
  std::unique_lock lock(mutex);
  return query_counter_events;
}

std::unordered_map<std::string, std::vector<query_event_t>> QueryAnalytic::get_query_log_events() {
  std::unique_lock lock(mutex);
  return query_log_events;
}

query_rule_config_t QueryAnalytic::get_query_rule(const std::string& name) {
  std::shared_lock lock(mutex);
  return query_rules.find(name)->second;
}

void QueryAnalytic::remove_all_rules() {
  std::unique_lock lock(mutex);
  query_rules.clear();
  query_counter_events.clear();
  query_log_events.clear();
  collection_rules_map.clear();
}

void QueryAnalytic::dispose() {
  remove_all_rules();
}