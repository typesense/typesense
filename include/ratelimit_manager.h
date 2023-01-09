#pragma once

#include <string>
#include <vector>
#include <tuple>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <json.hpp>
#include <magic_enum.hpp>
#include "lru/lru.hpp"
#include "option.h"
#include "store.h"




// Action enum for rate limit rules
enum class RateLimitAction {
    allow,
    block,
    throttle
};

// Entity type enum for rate limit rules
enum class RateLimitedEntityType {
    ip,
    api_key
};

// overload operator! to get inverse of RateLimitedEntityType
inline RateLimitedEntityType operator!(const RateLimitedEntityType& entity_type) {
    switch (entity_type) {
        case RateLimitedEntityType::ip:
            return RateLimitedEntityType::api_key;
        case RateLimitedEntityType::api_key:
            return RateLimitedEntityType::ip;
    }
}

// Max requests struct for rate limit rules
struct rate_limit_max_requests_t {
    int64_t minute_threshold = -1;
    int64_t hour_threshold = -1;

};

// Entry struct for rate limit rule pointer hash map as key
struct rate_limit_entity_t {
    RateLimitedEntityType entity_type;
    std::string entity_id;

    // Equality operator for rate_limit_entity_t
    bool operator==(const rate_limit_entity_t& other) const {
        return std::tie(entity_type, entity_id) == std::tie(other.entity_type, other.entity_id);
    }
};

// Struct for rate limit rules
struct rate_limit_rule_t {
    uint32_t id;
    RateLimitAction action;
    std::vector<rate_limit_entity_t> entities;
    rate_limit_max_requests_t max_requests;
    int64_t auto_ban_1m_threshold = -1;
    int64_t auto_ban_1m_duration_hours = -1;
    bool apply_limit_per_entity = false;
    uint32_t priority = 0;

    const nlohmann::json to_json() const;

};

// Request counter struct for ip addresses to keep track of requests for current and previous sampling period
struct request_counter_t {
    int64_t current_requests_count_minute = 0;
    int64_t current_requests_count_hour = 0;
    int64_t previous_requests_count_minute = 0;
    int64_t previous_requests_count_hour = 0;
    int64_t threshold_exceed_count_minute = 0;
    time_t last_reset_time_minute = 0;
    time_t last_reset_time_hour = 0;

    void reset() {
        current_requests_count_minute = 0;
        current_requests_count_hour = 0;
        threshold_exceed_count_minute = 0;
        previous_requests_count_minute = 0;
        previous_requests_count_hour = 0;
        last_reset_time_minute = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        last_reset_time_hour = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    }


    // not-equal operator overload
    bool operator!=(const request_counter_t& other) const{
        return std::tie(current_requests_count_minute, current_requests_count_hour, previous_requests_count_minute, previous_requests_count_hour, last_reset_time_minute, last_reset_time_hour) !=
               std::tie(other.current_requests_count_minute, other.current_requests_count_hour, other.previous_requests_count_minute, other.previous_requests_count_hour, other.last_reset_time_minute, other.last_reset_time_hour);
    }
};

// Struct to store ban information for ip addresses
struct rate_limit_status_t {
    uint32_t status_id;
    int64_t throttling_from;
    int64_t throttling_to;
    rate_limit_entity_t entity;
    // optional second entity for AND bans
    Option<rate_limit_entity_t> and_entity = Option<rate_limit_entity_t>(404, "Not Found");

    rate_limit_status_t(const uint32_t status_id, const int64_t throttling_from, const int64_t throttling_to, const rate_limit_entity_t &entity, const rate_limit_entity_t* and_entity = nullptr) : status_id(status_id), throttling_from(throttling_from), throttling_to(throttling_to), entity(entity) {
        if (and_entity != nullptr) {
            this->and_entity = Option<rate_limit_entity_t>(*and_entity);
        }
    }

    // default constructor
    rate_limit_status_t() = default;


    const nlohmann::json to_json() const;

    void parse_json(const nlohmann::json& json);
};

// Struct to store how many requests made by exceeded rate limit entities
struct rate_limit_exceed_t {
    uint32_t rule_id;
    std::string entities;
    uint64_t request_count = 0;

    const nlohmann::json to_json() const {
        nlohmann::json json;
        std::string api_key = entities.substr(0, entities.find("_"));
        std::string ip = entities.substr(entities.find("_") + 1);
        json["id"] = rule_id;
        json["api_key"] = api_key;
        json["ip"] = ip;
        json["request_count"] = request_count;
        return json;
    }
};


// Hash function for rate_limit_entity_t
namespace std {
    template <>
    struct hash<rate_limit_entity_t> {
        std::size_t operator()(const rate_limit_entity_t& k) const{
            return ((std::hash<int>()(static_cast<int>(k.entity_type)) ^ (std::hash<std::string>()(k.entity_id) << 1)) >> 1);
        }
    };
}

class RateLimitManager 
{
    public:

        RateLimitManager(const RateLimitManager&) = delete;

        RateLimitManager& operator=(const RateLimitManager&) = delete;

        RateLimitManager(RateLimitManager&&) = delete;

        RateLimitManager& operator=(RateLimitManager&&) = delete;

        static RateLimitManager* getInstance();
        
        // Get vector of banned entities
        const std::vector<rate_limit_status_t> get_banned_entities(const RateLimitedEntityType entity_type);

        // Check if request is rate limited for given entities
        bool is_rate_limited(const rate_limit_entity_t& api_key_entity, const rate_limit_entity_t& ip_entity);

        // Add rule by JSON
        Option<nlohmann::json> add_rule(const nlohmann::json &rule_json);

        // Edit rule by JSON
        Option<nlohmann::json> edit_rule(const uint64_t id, const nlohmann::json &rule_json);

        // Find rule by ID
        Option<nlohmann::json> find_rule_by_id(const uint64_t id);

        // Delete rule by ID
        bool delete_rule_by_id(const uint64_t id);

        // Delete ban by ID
        bool delete_ban_by_id(const uint64_t id);

        // Delete throttle by ID
        bool delete_throttle_by_id(const uint32_t id);

        // Get All rules as vector
        const std::vector<rate_limit_rule_t> get_all_rules();

        // Get all rules as json
        const nlohmann::json get_all_rules_json();

        // Get exceeded entities and request counts as JSON
        const nlohmann::json get_exceeded_entities_json();

        // Get autobanned entities as JSON
        const nlohmann::json get_throttled_entities_json();

        // Clear all rules
        void clear_all();

        // Internal function to set base time
        void _set_base_timestamp(const time_t& base_time);

        // Set store
        Option<bool> init(Store* store);

    private:    

        RateLimitManager() {
            rate_limit_request_counts.capacity(10000);
        }

        // Store for rate limit rules
        Store *store;

        // Using a $ prefix so that these meta keys stay above record entries in a lexicographically ordered KV store
        // Prefix for rate limit rules
        static constexpr const char* RULES_NEXT_ID = "$RLN";
        static constexpr const char* RULES_PREFIX = "$RLRP";

        // Prefix for bans
        static constexpr const char* BANS_NEXT_ID = "$RLBN";
        static constexpr const char* BANS_PREFIX = "$RLBP";

        // Static instance of wildcard entities
        inline static const rate_limit_entity_t WILDCARD_IP = rate_limit_entity_t{RateLimitedEntityType::ip, ".*"};
        inline static const rate_limit_entity_t WILDCARD_API_KEY = rate_limit_entity_t{RateLimitedEntityType::api_key, ".*"};

        // ID of latest added rule 
        inline static uint32_t last_rule_id = 0;

        // ID of latest added ban
        inline static uint32_t last_ban_id = 0;

        // ID of latest added throttle
        inline static uint32_t last_throttle_id = 0;

        // Store for rate_limit_rule_t
        std::unordered_map<uint64_t,rate_limit_rule_t> rule_store;

        // LRU Cache to store rate limit and request counts for entities
        LRU::Cache<std::string, request_counter_t> rate_limit_request_counts;

        // Unordered map to point rules from rule store for entities
        std::unordered_map<rate_limit_entity_t, std::vector<rate_limit_rule_t*>> rate_limit_entities;
        
        // Unordered map to store banned entities
        std::unordered_map<std::string, rate_limit_status_t> throttled_entities;

        // Hash map to store exceeds
        std::unordered_map<std::string, rate_limit_exceed_t> rate_limit_exceeds;

        // Mutex to protect access to ip_rate_limits and api_key_rate_limits
        std::shared_mutex rate_limit_mutex;

        // Helper function to ban an entity temporarily
        void temp_ban_entity(const rate_limit_entity_t& entity, const uint64_t number_of_hours);
        // Helper function to ban an entity temporarily without locking mutex
        void temp_ban_entity_wrapped(const rate_limit_entity_t& entity, const uint64_t number_of_hours, const rate_limit_entity_t* and_entity = nullptr);

        // Helper function to check if JSON rule is valid
        Option<bool> is_valid_rule(const nlohmann::json &rule_json);

        // Parse JSON rule to rate_limit_rule_t
        static Option<rate_limit_rule_t> parse_rule(const nlohmann::json &rule_json);

        // Helper function to insert rule in store
        void insert_rule(const rate_limit_rule_t &rule);

        // Helper function to get rule key for DB store from ID
        std::string get_rule_key(const uint32_t id);


        // Helper function to get ban key for DB store from ID
        std::string get_ban_key(const uint32_t id);

        // Base timestamp
        time_t base_timestamp = 0;

        // Helper function to get current timestamp
        time_t get_current_time();

        // Helper function to get throttle key for entity if exists
        Option<std::string> get_throttle_key(const rate_limit_entity_t& ip_entity, const rate_limit_entity_t& api_key_entity);

        // Helper function to get request counter key according to rule type
        static const std::string get_request_counter_key(const rate_limit_rule_t& rule, const rate_limit_entity_t& ip_entity, const rate_limit_entity_t& api_key_entity);

        // Fill bucket rule for the given entity
        void fill_bucket(const rate_limit_entity_t& target_entity, const rate_limit_entity_t& other_entity, std::vector<rate_limit_rule_t*> &rules_bucket);

        // Singleton instance
        inline static RateLimitManager *instance;

};

