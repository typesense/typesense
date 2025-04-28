#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include "json.hpp"
#include "option.h"

struct doc_rule_config_t {
    std::string name;
    std::string type;
    std::string collection;
    std::string event_type;
    std::string counter_field;
    std::string rule_tag;
    uint32_t weight;
    std::string destination_collection;

    void to_json(nlohmann::json& obj) const {
        obj["name"] = name;
        obj["type"] = type;
        obj["collection"] = collection;
        obj["event_type"] = event_type;
        obj["rule_tag"] = rule_tag;
        if (!counter_field.empty()) {
            obj["params"]["counter_field"] = counter_field;
        }
        if(!destination_collection.empty()) {
            obj["params"]["destination_collection"] = destination_collection;
        }
        if(weight > 0) {
            obj["params"]["weight"] = weight;
        }
    }
};

struct doc_event_t {
    std::string query;
    std::string event_type;
    uint64_t timestamp;
    std::string user_id;
    std::string doc_id;
    std::vector<std::string> doc_ids;
    std::string name;
    std::vector<std::pair<std::string, std::string>> data;

    doc_event_t() = delete;

    ~doc_event_t() = default;

    doc_event_t(const std::string& q, const std::string& type, uint64_t ts, const std::string& uid, 
            const std::string& id, const std::vector<std::string>& ids,
            const std::string& event_name, const std::vector<std::pair<std::string, std::string>> datavec) {
        query = q;
        event_type = type;
        timestamp = ts;
        user_id = uid;
        doc_id = id;
        doc_ids = ids;
        name = event_name;
        data = datavec;
    }

    doc_event_t& operator=(doc_event_t& other) {
        if (this != &other) {
            query = other.query;
            event_type = other.event_type;
            timestamp = other.timestamp;
            user_id = other.user_id;
            doc_id = other.doc_id;
            doc_ids = other.doc_ids;
            name = other.name;
            data = other.data;
            return *this;
        }
    }

    void to_json(nlohmann::json& obj, const std::string& coll) const;
};

struct doc_counter_event_t {
    std::string counter_field;
    std::map<std::string, uint64_t> docid_counts;
    uint64_t weight;
    std::string destination_collection;

    void serialize_as_docs(std::string& docs);
};

class DocAnalytics {
private:
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, doc_rule_config_t> doc_rules;
    std::unordered_map<std::string, std::vector<doc_event_t>> doc_log_events;
    std::unordered_map<std::string, doc_counter_event_t> doc_counter_events;

public:
    DocAnalytics() = default;

    ~DocAnalytics() = default;

    DocAnalytics(DocAnalytics const&) = delete;
    void operator=(DocAnalytics const&) = delete;
    
    static constexpr const char* COUNTER_TYPE = "counter";
    static constexpr const char* LOG_TYPE = "log";
    static constexpr const char* CLICK_EVENT = "click";
    static constexpr const char* CONVERSION_EVENT = "conversion";
    static constexpr const char* VISIT_EVENT = "visit";
    static constexpr const char* CUSTOM_EVENT = "custom";

    static DocAnalytics& get_instance() {
        static DocAnalytics instance;
        return instance;
    }

    bool check_rule_type(const std::string& event_type, const std::string& type);
    Option<bool> add_event(const std::string& client_ip, const nlohmann::json& event_data);
    Option<nlohmann::json> create_rule(nlohmann::json& payload, bool upsert);
    Option<bool> remove_rule(const std::string& name);
    void get_events(const std::string& userid, const std::string& event_name, uint32_t N, std::vector<std::string>& values);
    Option<nlohmann::json> list_rules(const std::string& rule_tag = "");
    Option<nlohmann::json> get_rule(const std::string& name);
    void reset_local_counter(const std::string& event_name);
    void reset_local_log_events(const std::string& event_name);
    std::unordered_map<std::string, doc_counter_event_t> get_doc_counter_events();
    std::unordered_map<std::string, std::vector<doc_event_t>> get_doc_log_events();
    doc_rule_config_t get_doc_rule(const std::string& name);
    void remove_all_rules();

    void dispose();
};