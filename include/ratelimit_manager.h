#pragma once

#include <string>
#include <vector>
#include <tuple>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include "lru/lru.hpp"
#include "option.h"




// Action enum for rate limit rules
enum class RateLimitAction {
    ALLOW,
    BLOCK,
    THROTTLE
};

enum class RateLimitedResourceType {
    IP,
    API_KEY
};

struct rate_limit_throttle_t {
    int64_t minute_rate_limit;
    int64_t hour_rate_limit;

};

struct rate_limit_rule_t {
    uint64_t id;
    RateLimitAction action;
    RateLimitedResourceType resource_type;
    std::vector<std::string> values;
    rate_limit_throttle_t throttle;
    int64_t auto_ban_threshold_num = -1;
    int64_t auto_ban_num_days = -1;
};

// Entry struct for rate limit rule pointer hash map as key
struct rate_limit_rule_entry_t {
    RateLimitedResourceType resource_type;
    std::string value;

    // Equality operator for rate_limit_rule_entry_t
    bool operator==(const rate_limit_rule_entry_t& other) const {
        return std::tie(resource_type, value) == std::tie(other.resource_type, other.value);
    }
};

// Request counter struct for IP addresses to keep track of requests for current and previous sampling period
struct request_counter_t {
    int64_t minute_rate_limit;
    int64_t hour_rate_limit;
    int64_t current_requests_count_minute = 0;
    int64_t current_requests_count_hour = 0;
    int64_t previous_requests_count_minute = 0;
    int64_t previous_requests_count_hour = 0;
    time_t last_reset_time_minute = 0;
    time_t last_reset_time_hour = 0;


    // not-equal operator overload
    bool operator!=(const request_counter_t& other) const{
        return std::tie(minute_rate_limit, hour_rate_limit, current_requests_count_minute, current_requests_count_hour, previous_requests_count_minute, previous_requests_count_hour, last_reset_time_minute, last_reset_time_hour) !=
               std::tie(other.minute_rate_limit, other.hour_rate_limit, other.current_requests_count_minute, other.current_requests_count_hour, other.previous_requests_count_minute, other.previous_requests_count_hour, other.last_reset_time_minute, other.last_reset_time_hour);
    }
};

// Struct to store ban information for IP addresses
struct rate_limit_status_t {
    bool is_banned = false;
    int64_t throttling_from;
    int64_t throttling_to;
    std::string value;
    RateLimitedResourceType resource_type;
};


// Hash function for rate_limit_rule_entry_t
namespace std {
    template <>
    struct hash<rate_limit_rule_entry_t> {
        std::size_t operator()(const rate_limit_rule_entry_t& k) const{
            return ((std::hash<int>()(static_cast<int>(k.resource_type)) ^ (std::hash<std::string>()(k.value) << 1)) >> 1);
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
        
        // Add rate limit for entries, returns false if there is a rule for any of the entries is already set
        bool throttle_entries(const RateLimitedResourceType resource_type, const std::vector<std::string> &entries, const int64_t minute_rate_limit, const int64_t hour_rate_limit, const int64_t auto_ban_threshold_num, const int64_t auto_ban_num_days);
        
        // Remove rate limit for entry
        bool remove_rule_entry(const RateLimitedResourceType resource_type, const std::string &entry);

        // Get vector of banned entries
        const std::vector<rate_limit_status_t> get_banned_entries(const RateLimitedResourceType resource_type);

        // Check if request is rate limited for given entries
        bool is_rate_limited(const std::vector<rate_limit_rule_entry_t> &entries);

        // Permanently ban entries
        bool ban_entries_permanently(const RateLimitedResourceType resource_type, const std::vector<std::string> &entries);

        // Allow entries
        bool allow_entries(RateLimitedResourceType resource_type, const std::vector<std::string> &entries);

        // Find rule by ID
        Option<rate_limit_rule_t> find_rule_by_id(const uint64_t id);

        // Delete rule by ID
        bool delete_rule_by_id(const uint64_t id);

        // Edit rule by ID
        bool edit_rule_by_id(const uint64_t id, const rate_limit_rule_t& rule);

        // Get All rules 
        const std::vector<rate_limit_rule_t> get_all_rules();

        // Clear all rules
        void clear_all();

    private:    

        RateLimitManager() {
            rate_limit_request_counts.capacity(10000);
        }

        // ID of latest added rule 
        inline static uint64_t last_rule_id = 0;

        // Store for rate_limit_rule_t
        std::unordered_map<uint64_t,rate_limit_rule_t> rule_store;

        // LRU Cache to store rate limit and request counts for entries
        LRU::Cache<rate_limit_rule_entry_t, request_counter_t> rate_limit_request_counts;

        // Unordered map to point rules from rule store for entries
        std::unordered_map<rate_limit_rule_entry_t, rate_limit_rule_t*> rate_limit_entries;
        
        // Unordered map to store banned entries
        std::unordered_map<rate_limit_rule_entry_t, rate_limit_status_t> throttled_entries;

        // Mutex to protect access to ip_rate_limits and api_key_rate_limits
        std::shared_mutex rate_limit_mutex;

        // Helper function to ban an entry temporarily
        void temp_ban_entry(const rate_limit_rule_entry_t& entry, const int64_t number_of_days);
        // Helper function to ban an entry temporarily without locking mutex
        void temp_ban_entry_wrapped(const rate_limit_rule_entry_t& entry, const int64_t number_of_days);

        // Singleton instance
        inline static RateLimitManager *instance;

};

