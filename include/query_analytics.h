#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <set>
#include "json.hpp"
#include "option.h"

struct query_event_t {
  std::string query;
  std::string event_type;
  uint64_t timestamp;
  std::string user_id;
  std::string filter_str;
  std::string tag_str;

  query_event_t(const std::string& q, const std::string& type, uint64_t ts, const std::string& uid, const std::string& filter, const std::string& tag) {
    query = q;
    event_type = type;
    timestamp = ts;
    user_id = uid;
    filter_str = filter;
    tag_str = tag;
  }

  query_event_t& operator=(const query_event_t& other) {
    if (this != &other) {
      query = other.query;
      event_type = other.event_type;
      timestamp = other.timestamp;
      user_id = other.user_id;
      filter_str = other.filter_str;
      tag_str = other.tag_str;
    }
    return *this;
  }

  bool operator==(const query_event_t& other) const {
    return query == other.query && filter_str == other.filter_str && tag_str == other.tag_str;
  }

  struct Hash {
    std::size_t operator()(const query_event_t& event) const {
      return std::hash<std::string>{}(event.query + event.filter_str + event.tag_str);
    }
  };

  void to_json(nlohmann::json& obj, const std::string& coll, const std::string& name) const;
};

struct query_internal_event_t {
  std::string type;
  std::string collection;
  std::string q;
  std::string expanded_q;
  std::string user_id;
  std::string filter_by;
  std::string analytics_tag;
};

struct query_counter_event_t {
  std::unordered_map<query_event_t, uint64_t, query_event_t::Hash> query_counts;
  std::string destination_collection;
  std::set<std::string> meta_fields;
  uint32_t limit;

  void serialize_as_docs(std::string& docs);
};

struct query_rule_config_t {
    std::string name;
    std::string type;
    std::string collection;
    std::string event_type;
    std::string rule_tag;
    uint32_t limit;
    std::string destination_collection;
    bool expand_query;
    bool capture_search_requests;
    std::set<std::string> meta_fields;

    void to_json(nlohmann::json& obj) const {
      obj["name"] = name;
      obj["type"] = type;
      obj["collection"] = collection;
      obj["event_type"] = event_type;
      obj["rule_tag"] = rule_tag;

      if(limit > 0) {
        obj["params"]["limit"] = limit;
      }
      if(!destination_collection.empty()) {
        obj["params"]["destination_collection"] = destination_collection;
      }
      if(!meta_fields.empty()) {
        obj["params"]["meta_fields"] = meta_fields;
      }
      obj["params"]["expand_query"] = expand_query;
      obj["params"]["capture_search_requests"] = capture_search_requests;
    }
};

class QueryAnalytics {
private:
  mutable std::shared_mutex user_compaction_mutex;
  mutable std::shared_mutex mutex;
  std::unordered_map<std::string, query_rule_config_t> query_rules;
  std::unordered_map<std::string, std::vector<std::string>> collection_rules_map;
  std::unordered_map<std::string, std::vector<query_event_t>> query_log_events;
  std::unordered_map<std::string, query_counter_event_t> query_counter_events;
  std::unordered_map<std::string, std::unordered_map<std::string, std::vector<query_event_t>>> popular_user_collection_prefix_queries;
  std::unordered_map<std::string, std::unordered_map<std::string, std::vector<query_event_t>>> nohits_user_collection_prefix_queries;
  std::unordered_map<std::string, std::unordered_map<std::string, std::vector<query_event_t>>> log_user_collection_prefix_queries;

public:
  static constexpr const char* POPULAR_QUERIES_TYPE = "popular_queries";
  static constexpr const char* NO_HIT_QUERIES_TYPE = "nohits_queries";
  static constexpr const char* LOG_TYPE = "log";
  static constexpr const char* QUERY_EVENT = "query";
  static const size_t QUERY_FINALIZATION_INTERVAL_MICROS = 4 * 1000 * 1000;
  static const size_t MAX_QUERY_LENGTH = 1024;

  QueryAnalytics() = default;
  ~QueryAnalytics() = default;

  QueryAnalytics(QueryAnalytics const&) = delete;
  void operator=(QueryAnalytics const&) = delete;

  static QueryAnalytics& get_instance() {
    static QueryAnalytics instance;
    return instance;
  }

  bool check_rule_type(const std::string& event_type, const std::string& type);
  bool check_rule_type_collection(const std::string& collection, const std::string& type);
  Option<bool> add_event(const std::string& client_ip, const nlohmann::json& event_data);
  Option<nlohmann::json> create_rule(nlohmann::json& payload, bool update, bool is_live_req);
  Option<bool> remove_rule(const std::string& name);
  void remove_all_rules();
  void get_events(const std::string& userid, const std::string& event_name, uint32_t N, std::vector<std::string>& values);
  Option<nlohmann::json> list_rules(const std::string& rule_tag = "");
  Option<nlohmann::json> get_rule(const std::string& name);
  Option<bool> add_internal_event(const query_internal_event_t& event_data);
  void compact_single_user_queries(uint64_t now_ts_us, const std::string& user_id, const std::string& type, std::unordered_map<std::string, std::vector<query_event_t>>& user_prefix_queries);
  void compact_all_user_queries(uint64_t now_ts_us);
  void reset_local_counter(const std::string& event_name);
  void reset_local_log_events(const std::string& event_name);
  std::unordered_map<std::string, query_counter_event_t> get_query_counter_events();
  std::unordered_map<std::string, std::vector<query_event_t>> get_query_log_events();
  query_rule_config_t get_query_rule(const std::string& name);
  size_t get_popular_prefix_queries_size();
  size_t get_nohits_prefix_queries_size();
  size_t get_log_prefix_queries_size();
  void dispose();
};