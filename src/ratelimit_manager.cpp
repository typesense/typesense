#include "ratelimit_manager.h"


RateLimitManager * RateLimitManager::getInstance() {
    if (!instance) {
        instance = new RateLimitManager();
    }

    return instance;
}

void RateLimitManager::add_rate_limit_api_key(const std::string & api_key,
    const int64_t minute_rate_limit,
        const int64_t hour_rate_limit) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Add rate limit for API key
    api_key_rate_limits[api_key] = {
        true,
        false,
        false,
        minute_rate_limit,
        hour_rate_limit
    };

}

void RateLimitManager::add_rate_limit_ip(const std::string & ip,
    const int64_t minute_rate_limit,
        const int64_t hour_rate_limit) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Add rate limit for IP
    ip_rate_limits[ip] = {
        true,
        false,
        false,
        minute_rate_limit,
        hour_rate_limit
    };
}

void RateLimitManager::remove_rate_limit_api_key(const std::string & api_key) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Remove rate limit for API key
    api_key_rate_limits.erase(api_key);
}

void RateLimitManager::remove_rate_limit_ip(const std::string & ip) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Remove rate limit for IP
    ip_rate_limits.erase(ip);
}

const std::vector < ratelimit_ban_t > RateLimitManager::get_banned_ips() {
    // lock mutex
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Get vector of banned IPs
    std::vector < ratelimit_ban_t > banned_ips_local;

    for (auto & ip: banned_ips) {
        for (auto & e: ip.second) {
            //Check if ban time is over
            if (e.throttling_to <= std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) && e.is_banned) {
                e.is_banned = false;
            } else if (e.is_banned) {
                //Add ban to vector
                banned_ips_local.push_back(e);
            }
        }
    }

    // Get permanent bans
    for (const auto & ip: ip_rate_limits) {
        if (ip.second.is_banned_permanently) {
            banned_ips_local.push_back({
                true,
                0,
                0,
                "",
                ip.first,
                RateLimitBanDuration::PERMANENT
            });
        }

    }

    return banned_ips_local;
}

const std::vector < std::string > RateLimitManager::get_tracked_ips() {
    // lock mutex
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Get vector of tracked IPs
    std::vector < std::string > tracked_ips;
    for (auto & ip: ip_rate_limits) {
        if (ip.second.is_tracked) {
            tracked_ips.push_back(ip.first);
        }
    }
    return tracked_ips;
}

const std::vector < std::string > RateLimitManager::get_tracked_api_keys() {
    // lock mutex
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Get vector of tracked API keys
    std::vector < std::string > tracked_api_keys;
    for (auto & api_key: api_key_rate_limits) {
        if (api_key.second.is_tracked) {
            tracked_api_keys.push_back(api_key.first);
        }
    }
    return tracked_api_keys;
}

void RateLimitManager::ban_api_key(const std::string & api_key) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);
    // Ban API key
    ban_api_key_wrapped(api_key);
}

void RateLimitManager::ban_ip(const std::string & ip,
    const std::string & api_key) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);
    ban_ip_wrapped(ip, api_key);
}

void RateLimitManager::ban_ip_permanently(const std::string & ip) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    ip_rate_limits[ip] = ratelimit_tracker_t {
        false,
        false,
        true,
        0,
        0,
    };
}

void RateLimitManager::ban_api_key_permanently(const std::string & api_key) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    api_key_rate_limits[api_key] = ratelimit_tracker_t {
        false,
        false,
        true,
        0,
        0,
    };
}

bool RateLimitManager::is_rate_limited(const std::string & api_key,
    const std::string & ip) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if API Key or IP is allowed
    if (api_key_rate_limits[api_key].is_allowed || ip_rate_limits[ip].is_allowed) {
        return false;
    }

    // Check if API Key or IP is banned
    if (api_key_rate_limits[api_key].is_banned_permanently || ip_rate_limits[ip].is_banned_permanently) {
        return true;
    }

    // Check if API key is tracked
    if (api_key_rate_limits[api_key].is_tracked) {

        // Check if API key is banned
        if (banned_api_keys[api_key].is_banned) {

            // Check if throttling time is over
            if (banned_api_keys[api_key].throttling_to < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
                // Remove ban
                banned_api_keys[api_key].is_banned = false;
                return false;
            } else {
                // API key is banned
                return true;
            }
        }

        if (!api_key_request_counts.contains(api_key)) {
            api_key_request_counts.emplace(api_key, request_counter_t {});
        }

        // Check last reset time for minute rate limit 
        if (api_key_request_counts[api_key].last_reset_time_minute + 60 < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
            // Reset minute rate limit
            api_key_request_counts[api_key].previous_requests_count_minute = api_key_request_counts[api_key].current_requests_count_minute;
            api_key_request_counts[api_key].current_requests_count_minute = 0;
            api_key_request_counts[api_key].last_reset_time_minute = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        }

        // Check last reset time for hour rate limit 
        if (api_key_request_counts[api_key].last_reset_time_hour + 3600 < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
            // Reset hour rate limit
            api_key_request_counts[api_key].previous_requests_count_hour = api_key_request_counts[api_key].current_requests_count_hour;
            api_key_request_counts[api_key].current_requests_count_hour = 0;
            api_key_request_counts[api_key].last_reset_time_hour = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        }

        // Increase current request count for minute rate limit and hour rate limit
        api_key_request_counts[api_key].current_requests_count_minute++;
        api_key_request_counts[api_key].current_requests_count_hour++;

        time_t currentTime;
        struct tm * timeinfo;
        std::time( & currentTime);
        timeinfo = std::localtime( & currentTime);

        // Check if current request count for minute rate limit is over the limit with sliding window
        auto current_rate_minute = (60 - (timeinfo -> tm_sec)) / 60.0 * api_key_request_counts[api_key].previous_requests_count_minute + api_key_request_counts[api_key].current_requests_count_minute;

        if (api_key_rate_limits[api_key].minute_rate_limit >= 0 && current_rate_minute >= api_key_rate_limits[api_key].minute_rate_limit) {
            ban_api_key_wrapped(api_key);
            return true;
        }

        // Check if current request count for hour rate limit is over the limit with sliding window
        auto current_rate_hour = (3600 - (timeinfo -> tm_min * 60 + timeinfo -> tm_sec)) / 3600.0 * api_key_request_counts[api_key].previous_requests_count_hour + api_key_request_counts[api_key].current_requests_count_hour;
        if (api_key_rate_limits[api_key].hour_rate_limit >= 0 && current_rate_hour >= api_key_rate_limits[api_key].hour_rate_limit) {
            ban_api_key_wrapped(api_key);
            return true;
        }
    }

    // Check if IP is tracked
    if (ip_rate_limits[ip].is_tracked) {
        // Check if IP is banned for this API key
        for (auto & element: banned_ips[ip]) {
            if ((element.api_key == api_key || element.api_key == "") && element.is_banned) {

                // Check if throttling time is over
                if (element.throttling_to < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
                    // Remove ban
                    element.is_banned = false;
                    return false;
                } else {
                    // IP is banned for this API key
                    return true;
                }
            }
        }

        if (!ip_request_counts.contains(ip)) {
            ip_request_counts.emplace(ip, request_counter_t {});
        }

        // Check last reset time for minute rate limit
        if (ip_request_counts[ip].last_reset_time_minute + 60 < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
            // Reset minute rate limit
            ip_request_counts[ip].previous_requests_count_minute = ip_request_counts[ip].current_requests_count_minute;
            ip_request_counts[ip].current_requests_count_minute = 0;
            ip_request_counts[ip].last_reset_time_minute = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        }

        // Check last reset time for hour rate limit
        if (ip_request_counts[ip].last_reset_time_hour + 3600 < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
            // Reset hour rate limit
            ip_request_counts[ip].previous_requests_count_hour = ip_request_counts[ip].current_requests_count_hour;
            ip_request_counts[ip].current_requests_count_hour = 0;
            ip_request_counts[ip].last_reset_time_hour = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        }

        // Increase current request count for minute rate limit and hour rate limit
        ip_request_counts[ip].current_requests_count_minute++;
        ip_request_counts[ip].current_requests_count_hour++;

        time_t currentTime;
        struct tm * timeinfo;
        std::time( & currentTime);
        timeinfo = std::localtime( & currentTime);

        // Check if current request count for minute rate limit is over the limit with sliding window
        auto current_rate_minute = (60 - timeinfo -> tm_sec) / 60.0 * ip_request_counts[ip].previous_requests_count_minute + ip_request_counts[ip].current_requests_count_minute;
        if (ip_rate_limits[ip].minute_rate_limit >= 0 && current_rate_minute >= ip_rate_limits[ip].minute_rate_limit) {
            ban_ip_wrapped(ip, api_key);
            return true;
        }

        // Check if current request count for hour rate limit is over the limit with sliding window
        auto current_rate_hour = (3600 - (timeinfo -> tm_min * 60 + timeinfo -> tm_sec)) / 3600.0 * ip_request_counts[ip].previous_requests_count_hour + ip_request_counts[ip].current_requests_count_hour;
        if (ip_rate_limits[ip].hour_rate_limit >= 0 && current_rate_hour >= ip_rate_limits[ip].hour_rate_limit) {
            ban_ip_wrapped(ip, api_key);
            return true;
        }

    }

    return false;

}

void RateLimitManager::allow_ip(const std::string & ip) {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    ip_rate_limits[ip] = ratelimit_tracker_t {
        false,
        true,
        false,
        0,
        0,
    };
}

void RateLimitManager::allow_api_key(const std::string & api_key) {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    api_key_rate_limits[api_key] = ratelimit_tracker_t {
        false,
        true,
        false,
        0,
        0,
    };
}

ratelimit_tracker_t RateLimitManager::find_rule_by_id(const int64_t id) {
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);
    for (auto & element: ip_rate_limits) {
        if (element.second.id == id) {
            return element.second;
        }
    }
    for (auto & element: api_key_rate_limits) {
        if (element.second.id == id) {
            return element.second;
        }
    }

    ratelimit_tracker_t empty;
    empty.id = -1;
    return empty;
}

void RateLimitManager::delete_rule_by_id(const int64_t id) {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);
    for (auto & element: ip_rate_limits) {
        if (element.second.id == id) {
            ip_rate_limits.erase(element.first);
            return;
        }
    }
    for (auto & element: api_key_rate_limits) {
        if (element.second.id == id) {
            api_key_rate_limits.erase(element.first);
            return;
        }
    }
}

void RateLimitManager::edit_rule_by_id(const int64_t id,
    const ratelimit_tracker_t & new_rule) {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    for (auto & element: ip_rate_limits) {
        if (element.second.id == id) {
            element.second = new_rule;
            return;
        }
    }
    for (auto & element: api_key_rate_limits) {
        if (element.second.id == id) {
            element.second = new_rule;
            return;
        }
    }
}

const std::vector < ratelimit_tracker_t > RateLimitManager::get_all_rules() {
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);

    std::vector < ratelimit_tracker_t > all_rules;

    for (auto & element: ip_rate_limits) {
        if (!element.second.is_tracked && !element.second.is_allowed && !element.second.is_banned_permanently) {
            continue;
        }
        ratelimit_tracker_t rule = element.second;
        rule.ip = element.first;
        all_rules.push_back(rule);
    }

    for (auto & element: api_key_rate_limits) {
        if (!element.second.is_tracked && !element.second.is_allowed && !element.second.is_banned_permanently) {
            continue;
        }
        ratelimit_tracker_t rule = element.second;
        rule.api_key = element.first;
        all_rules.push_back(rule);
    }

    return all_rules;
}

const std::vector < ratelimit_ban_t > RateLimitManager::get_banned_api_keys() {
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);

    std::vector < ratelimit_ban_t > banned_api_keys_local;

    for (auto & element: banned_api_keys) {
        // Check if ban time is over
        if (element.second.throttling_to > std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
            banned_api_keys_local.push_back(element.second);
        } else {
            // Remove ban if time is over
            element.second.is_banned = false;
        }
    }

    // Get permanent bans
    for (auto & element: api_key_rate_limits) {
        if (element.second.is_banned_permanently) {
            banned_api_keys_local.push_back({
                true,
                0,
                0,
                element.first,
                "",
                RateLimitBanDuration::PERMANENT
            });
        }
    }

    return banned_api_keys_local;
}

void RateLimitManager::clear_all() {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);
    ip_rate_limits.clear();
    api_key_rate_limits.clear();
    banned_ips.clear();
    banned_api_keys.clear();
    ip_request_counts.clear();
    api_key_request_counts.clear();
}

void RateLimitManager::ban_api_key_wrapped(const std::string & api_key) {
    switch (banned_api_keys[api_key].banDuration) {
    case RateLimitBanDuration::NO_BAN_BEFORE:
        banned_api_keys[api_key].banDuration = RateLimitBanDuration::ONE_MINUTE;
        banned_api_keys[api_key].api_key = api_key;
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 60;
        banned_api_keys[api_key].is_banned = true;
        break;
    case RateLimitBanDuration::ONE_MINUTE:
        banned_api_keys[api_key].banDuration = RateLimitBanDuration::FIVE_MINUTES;
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 300;
        banned_api_keys[api_key].is_banned = true;
        break;
    case RateLimitBanDuration::FIVE_MINUTES:
        banned_api_keys[api_key].banDuration = RateLimitBanDuration::TEN_MINUTES;
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 600;
        banned_api_keys[api_key].is_banned = true;
        break;
    case RateLimitBanDuration::TEN_MINUTES:
        banned_api_keys[api_key].banDuration = RateLimitBanDuration::THIRTY_MINUTES;
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 1800;
        banned_api_keys[api_key].is_banned = true;
        break;
    case RateLimitBanDuration::THIRTY_MINUTES:
        banned_api_keys[api_key].banDuration = RateLimitBanDuration::ONE_HOUR;
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 3600;
        banned_api_keys[api_key].is_banned = true;
        break;
    case RateLimitBanDuration::ONE_HOUR:
        banned_api_keys[api_key].banDuration = RateLimitBanDuration::THREE_HOURS;
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 10800;
        banned_api_keys[api_key].is_banned = true;
        break;
    case RateLimitBanDuration::THREE_HOURS:
        banned_api_keys[api_key].banDuration = RateLimitBanDuration::SIX_HOURS;
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 21600;
        banned_api_keys[api_key].is_banned = true;
    case RateLimitBanDuration::SIX_HOURS:
        banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 21600;
        banned_api_keys[api_key].is_banned = true;
        break;
    }

    api_key_request_counts[api_key].previous_requests_count_minute = 0;

    if (banned_api_keys[api_key].banDuration == RateLimitBanDuration::ONE_HOUR || banned_api_keys[api_key].banDuration == RateLimitBanDuration::THREE_HOURS || banned_api_keys[api_key].banDuration == RateLimitBanDuration::SIX_HOURS)
        api_key_request_counts[api_key].previous_requests_count_hour = 0;
}

void RateLimitManager::ban_ip_wrapped(const std::string & ip,
    const std::string & api_key) {
    bool found = false;

    for (ratelimit_ban_t & element: banned_ips[ip]) {
        if (element.api_key == api_key) {
            switch (element.banDuration) {
            case RateLimitBanDuration::NO_BAN_BEFORE:
                element.banDuration = RateLimitBanDuration::ONE_MINUTE;
                element.ip = ip;
                element.api_key = api_key;
                element.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                element.throttling_to = element.throttling_from + 60;
                element.is_banned = true;
                break;
            case RateLimitBanDuration::ONE_MINUTE:
                element.banDuration = RateLimitBanDuration::FIVE_MINUTES;
                element.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                element.throttling_to = element.throttling_from + 300;
                element.is_banned = true;
                break;
            case RateLimitBanDuration::FIVE_MINUTES:
                element.banDuration = RateLimitBanDuration::TEN_MINUTES;
                element.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                element.throttling_to = element.throttling_from + 600;
                element.is_banned = true;
                break;
            case RateLimitBanDuration::TEN_MINUTES:
                element.banDuration = RateLimitBanDuration::THIRTY_MINUTES;
                element.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                element.throttling_to = element.throttling_from + 1800;
                element.is_banned = true;
                break;
            case RateLimitBanDuration::THIRTY_MINUTES:
                element.banDuration = RateLimitBanDuration::ONE_HOUR;
                element.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                element.throttling_to = element.throttling_from + 3600;
                element.is_banned = true;
                break;
            case RateLimitBanDuration::ONE_HOUR:
                element.banDuration = RateLimitBanDuration::THREE_HOURS;
                element.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                element.throttling_to = element.throttling_from + 10800;
                element.is_banned = true;
                break;
            case RateLimitBanDuration::THREE_HOURS:
                element.banDuration = RateLimitBanDuration::SIX_HOURS;
                element.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                element.throttling_to = element.throttling_from + 21600;
                element.is_banned = true;
                break;
            case RateLimitBanDuration::SIX_HOURS:
                banned_api_keys[api_key].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                banned_api_keys[api_key].throttling_to = banned_api_keys[api_key].throttling_from + 21600;
                banned_api_keys[api_key].is_banned = true;
                break;
            }

            found = true;
            break;
        }
    }

    if (!found) {
        ratelimit_ban_t new_ban;
        new_ban.api_key = api_key;
        new_ban.ip = ip;
        new_ban.throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        new_ban.throttling_to = new_ban.throttling_from + 60;
        new_ban.is_banned = true;
        new_ban.banDuration = RateLimitBanDuration::ONE_MINUTE;
        banned_ips[ip].push_back(new_ban);
    }

    ip_request_counts[ip].previous_requests_count_minute = 0;
    if (banned_ips[ip].back().banDuration == RateLimitBanDuration::ONE_HOUR || banned_ips[ip].back().banDuration == RateLimitBanDuration::THREE_HOURS || banned_ips[ip].back().banDuration == RateLimitBanDuration::SIX_HOURS)
        ip_request_counts[ip].previous_requests_count_hour = 0;
}