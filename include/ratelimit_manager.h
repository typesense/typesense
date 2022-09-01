#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include "lru/lru.hpp"


// Enum for rate limit ban durations from one minute to six hours
enum class RateLimitBanDuration {
    NO_BAN_BEFORE,
    ONE_MINUTE,
    FIVE_MINUTES,
    TEN_MINUTES,
    THIRTY_MINUTES,
    ONE_HOUR,
    THREE_HOURS,
    SIX_HOURS,
    PERMANENT
};


// Rate limit banned IP  structure with throttling_from and throttling_to fields.
struct ratelimit_banned_ip_t {
    std::string ip;
    std::string api_key;
    int64_t throttling_from;
    int64_t throttling_to;
};




// Rate limit struct which stores if IP or api_keey isTracked and rate limits for one minute and one hour.
struct ratelimit_tracker_t {

    private:

        friend class RateLimitManager;

        inline static int64_t last_id;
    public:

    ratelimit_tracker_t() 
    {
        id =  ratelimit_tracker_t::last_id++;
    }

    ratelimit_tracker_t(bool is_tracked, bool is_allowed, bool is_banned_permanently, int64_t minute_rate_limit, int64_t hour_rate_limit) :
        is_tracked(is_tracked), is_allowed(is_allowed), is_banned_permanently(is_banned_permanently), minute_rate_limit(minute_rate_limit), hour_rate_limit(hour_rate_limit)
    {
        id = ratelimit_tracker_t::last_id++;
    }
    int64_t id;
    bool is_tracked = false;
    bool is_allowed = false;
    bool is_banned_permanently = false;
    int64_t minute_rate_limit;
    int64_t hour_rate_limit;
    std::string ip;
    std::string api_key;
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
    bool operator!=(const request_counter_t& other) const
    {
        return minute_rate_limit != other.minute_rate_limit || hour_rate_limit != other.hour_rate_limit || current_requests_count_minute != other.current_requests_count_minute || current_requests_count_hour != other.current_requests_count_hour || previous_requests_count_minute != other.previous_requests_count_minute || previous_requests_count_hour != other.previous_requests_count_hour || last_reset_time_minute != other.last_reset_time_minute || last_reset_time_hour != other.last_reset_time_hour;
    }
};

// Struct to store ban information for IP addresses
struct ratelimit_ban_t {
    bool is_banned = false;
    int64_t throttling_from;
    int64_t throttling_to;
    std::string api_key;
    std::string ip;
    RateLimitBanDuration banDuration = RateLimitBanDuration::NO_BAN_BEFORE;
};


class RateLimitManager 
{
    public:


        RateLimitManager(const RateLimitManager&) = delete;

        RateLimitManager& operator=(const RateLimitManager&) = delete;

        RateLimitManager(RateLimitManager&&) = delete;

        RateLimitManager& operator=(RateLimitManager&&) = delete;

        static RateLimitManager* getInstance();
        
        // Add rate limit for API key
        void add_rate_limit_api_key(const std::string &api_key, const int64_t minute_rate_limit, const int64_t hour_rate_limit);
        // Add rate limit for IP
        void add_rate_limit_ip(const std::string &ip, const int64_t minute_rate_limit, const int64_t hour_rate_limit);
        // Remove rate limit for API key
        void remove_rate_limit_api_key(const std::string &api_key);
        // Remove rate limit for IP
        void remove_rate_limit_ip(const std::string &ip);

        // Get vector of banned IPs
        const std::vector<ratelimit_ban_t> get_banned_ips();

        // Get vector of banned API keys
        const std::vector<ratelimit_ban_t> get_banned_api_keys();

        // Get vector of tracked IPs
        const std::vector<std::string> get_tracked_ips();

        // Get vector of tracked API keys
        const std::vector<std::string> get_tracked_api_keys();

        // Check if request is rate limited for API key and IP
        bool is_rate_limited(const std::string &api_key, const std::string &ip);

        // Permanently ban IP
        void ban_ip_permanently(const std::string &ip);

        // Permanently ban API key
        void ban_api_key_permanently(const std::string &api_key);

        // Allow IP
        void allow_ip(const std::string &ip);

        // Allow API key
        void allow_api_key(const std::string &api_key);

        // Find rule by ID
        ratelimit_tracker_t find_rule_by_id(const int64_t id);

        // Delete rule by ID
        void delete_rule_by_id(const int64_t id);

        // Edit rule by ID
        void edit_rule_by_id(const int64_t id, const ratelimit_tracker_t& rule);

        // Get All rules for IP addresess
        const std::vector<ratelimit_tracker_t> get_all_rules();

        // Clear all rules, bans and tracked IPs and API keys
        void clear_all();

    private:    

        RateLimitManager() {
            ip_request_counts.capacity(10000);
            api_key_request_counts.capacity(10000);
            ratelimit_tracker_t::last_id = 0;
        }


        // Unordered map to store rate limit and request counts for IP addresses
        LRU::Cache<std::string, request_counter_t> ip_request_counts;
        // Unordered map to store rate limit and request counts for api_key
        LRU::Cache<std::string, request_counter_t> api_key_request_counts;
        // Unordered map to store if api_key is tracked
        std::unordered_map<std::string, ratelimit_tracker_t> api_key_rate_limits;
        // Unordered map to store if IP is tracked
        std::unordered_map<std::string, ratelimit_tracker_t> ip_rate_limits;
        // Unordered map to store banned IPs
        std::unordered_map<std::string, std::vector<ratelimit_ban_t>> banned_ips;
        // Unordered map to store banned API keys
        std::unordered_map<std::string, ratelimit_ban_t> banned_api_keys;

        // Mutex to protect access to ip_rate_limits and api_key_rate_limits
        std::shared_mutex rate_limit_mutex;

        // Helper function to ban API key
        void ban_api_key(const std::string &api_key);
        // Helper function to ban IP
        void ban_ip(const std::string &ip, const std::string& api_key);
        // Helper function to ban API key without mutex lock
        void ban_api_key_wrapped(const std::string &api_key);
        // Helper function to ban IP without mutex lock
        void ban_ip_wrapped(const std::string &ip, const std::string& api_key);



        // Singleton instance
        inline static RateLimitManager *instance;

};

