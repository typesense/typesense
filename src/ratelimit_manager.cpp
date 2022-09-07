#include "ratelimit_manager.h"
#include <iterator>

RateLimitManager * RateLimitManager::getInstance() {
    if (!instance) {
        instance = new RateLimitManager();
    }

    return instance;
}

bool RateLimitManager::throttle_entries(const RateLimitedResourceType resource_type, const std::vector<std::string> &entries, const int64_t minute_rate_limit, const int64_t hour_rate_limit, const int64_t auto_ban_threshold_num, const int64_t auto_ban_num_days) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);

    for(const auto &entry : entries) {
        if (rate_limit_entries.count(rate_limit_rule_entry_t{resource_type,entry}) > 0) {
            return false;
        }
    }

    // add rate limit for entry
    rule_store.insert({last_rule_id,rate_limit_rule_t{last_rule_id,RateLimitAction::THROTTLE, resource_type,entries, rate_limit_throttle_t{minute_rate_limit, hour_rate_limit}, auto_ban_threshold_num, auto_ban_num_days}});

    // Add rule from rule store to rate limit rule pointer
    for(const auto &entry : entries) {
        rate_limit_entries.insert({rate_limit_rule_entry_t{resource_type,entry}, &rule_store.at(last_rule_id)});
    }

    last_rule_id++;;
    return true;

}

bool RateLimitManager::remove_rule_entry(const RateLimitedResourceType resource_type, const std::string &entry) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);

    // Check if a rule exists for the given IP
    if (rate_limit_entries.count(rate_limit_rule_entry_t{resource_type,entry}) == 0) {
        return false;
    }

    // Remove the rule from the rule store
    rule_store.erase(rate_limit_entries.at(rate_limit_rule_entry_t{resource_type,entry})->id);

    // Remove the rule from the entry rate limits map
    rate_limit_entries.erase(rate_limit_rule_entry_t{resource_type,entry});

    return true;

}

void RateLimitManager::temp_ban_entry(const rate_limit_rule_entry_t& entry, const int64_t number_of_days) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);
    temp_ban_entry_wrapped(entry, number_of_days);
}

bool RateLimitManager::ban_entries_permanently(const RateLimitedResourceType resource_type, const std::vector<std::string> &entries) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if a rule exists for the given IP addresses
    for(const auto& entry : entries) {
        if (rate_limit_entries.count(rate_limit_rule_entry_t{resource_type,entry}) > 0) {
            return false;
        }
    }

    // add rate limit for entry
    rule_store.insert({last_rule_id,rate_limit_rule_t{last_rule_id,RateLimitAction::BLOCK, resource_type,entries, rate_limit_throttle_t{0, 0}, -1, -1}});

    // Add rule from rule store to rate limit rule pointer
    for(const auto &entry : entries) {
        rate_limit_entries.insert({rate_limit_rule_entry_t{resource_type,entry}, &rule_store.at(last_rule_id)});
    }

    last_rule_id++;
    return true;
}



bool RateLimitManager::is_rate_limited(const std::vector<rate_limit_rule_entry_t> &entries) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);
    
    // Check if any of the entries has rule
    for(const auto &entry : entries) {

        if (rate_limit_entries.count(entry) == 0) {
            continue;
        }

        const auto& rule = *(rate_limit_entries.at(entry));

        if (rule.action == RateLimitAction::BLOCK) {
            return true;
        }
        else if(rule.action == RateLimitAction::ALLOW) {
            return false;
        }
        else if(throttled_entries.count(entry) > 0 && throttled_entries.at(entry).is_banned) {
            
            // Check if ban duration is not over
            if (throttled_entries.at(entry).throttling_to > std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
                
                // Reset request count for the entry 
                if(rate_limit_request_counts.contains(entry)) {
                    auto request_counts = rate_limit_request_counts.lookup(entry);

                    request_counts.current_requests_count_hour = 0;
                    request_counts.current_requests_count_minute = 0;
                }

                return true;
            }
 
            // Remove ban
            throttled_entries.at(entry).is_banned = false;

            // Check if throttle rule still exists
            if (rate_limit_entries.count(entry) > 0 && (rate_limit_entries.at(entry)->action == RateLimitAction::ALLOW)) {
                rate_limit_request_counts.lookup(entry).current_requests_count_minute++;
                rate_limit_request_counts.lookup(entry).current_requests_count_hour++;
            }

            continue;

        }
        else {
            
            if(!rate_limit_request_counts.contains(entry)){
                rate_limit_request_counts.insert(entry, request_counter_t{});
            }
            
            auto& request_counts = rate_limit_request_counts.lookup(entry);

            // Check if last reset time was more than 1 minute ago
            if (request_counts.last_reset_time_minute <= std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - 60) {
                request_counts.previous_requests_count_minute = request_counts.current_requests_count_minute;
                request_counts.current_requests_count_minute = 0;

                if(request_counts.last_reset_time_minute < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - 120) {
                    request_counts.previous_requests_count_minute = 0;
                }

                request_counts.last_reset_time_minute = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            }

            // Check if last reset time was more than 1 hour ago
            if (request_counts.last_reset_time_hour < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - 3600) {
                request_counts.previous_requests_count_hour = request_counts.current_requests_count_hour;
                request_counts.current_requests_count_hour = 0;

                if(request_counts.last_reset_time_hour < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - 7200) {
                    request_counts.previous_requests_count_hour = 0;
                }

                request_counts.last_reset_time_hour = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            }

            // Increment request counts
            request_counts.current_requests_count_minute++;
            request_counts.current_requests_count_hour++;

            // Check if request count is over the limit
            auto current_rate_for_minute = (60 - (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - request_counts.last_reset_time_minute)) / 60  * request_counts.previous_requests_count_minute;
            current_rate_for_minute += request_counts.current_requests_count_minute;

            if(rule.throttle.minute_rate_limit >= 0 && current_rate_for_minute > rule.throttle.minute_rate_limit) {
                if(rule.auto_ban_threshold_num >= 0 && rule.auto_ban_num_days >= 0) {
                    temp_ban_entry_wrapped(entry, rule.auto_ban_num_days);
                } 
                return true;
            }

            auto current_rate_for_hour = (3600 - (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - request_counts.last_reset_time_hour)) / 3600  * request_counts.previous_requests_count_hour;
            current_rate_for_hour += request_counts.current_requests_count_hour;

            if(rule.throttle.hour_rate_limit >= 0 && current_rate_for_hour > rule.throttle.hour_rate_limit) {
                return true;
            }
        }
    }

    return false;
}



bool RateLimitManager::allow_entries(RateLimitedResourceType resource_type, const std::vector<std::string> &entries) {
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);

    // Check if a rule exists for the given API keys
    for(const auto& entry : entries) {
        if (rate_limit_entries.count(rate_limit_rule_entry_t{resource_type,entry}) > 0) {
            return false;
        }
    }

    // add rate limit for entry
    rule_store.insert({last_rule_id,rate_limit_rule_t{last_rule_id,RateLimitAction::ALLOW, resource_type,entries, rate_limit_throttle_t{0, 0}}});

    // Add rule from rule store to rate limit rule pointer
    for(const auto &entry : entries) {
        rate_limit_entries.insert({rate_limit_rule_entry_t{resource_type,entry}, &rule_store.at(last_rule_id)});
    }

    last_rule_id++;;
    return true;
}

Option<rate_limit_rule_t> RateLimitManager::find_rule_by_id(const uint64_t id) {
    std::shared_lock<std::shared_mutex> lock(rate_limit_mutex);

    if(rule_store.count(id) > 0) {
        return Option<rate_limit_rule_t>(rule_store.at(id));
    }


    return Option<rate_limit_rule_t>(404, "Not Found");
}

bool RateLimitManager::delete_rule_by_id(const uint64_t id) {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);

    // Check if a rule exists for the given ID
    if(rule_store.count(id) > 0) {
        auto rule = rule_store.at(id);

        // Remove rule from rule store
        rule_store.erase(id);

        // Remove rule from rate limit rule pointer
        for(const auto &entry : rule.values) {
            rate_limit_entries.erase(rate_limit_rule_entry_t{rule.resource_type,entry});
        }

        return true;
    }

    return false;
}

bool RateLimitManager::edit_rule_by_id(const uint64_t id, const rate_limit_rule_t & new_rule) {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);

    // Check if a rule exists for the given ID
    if(rule_store.count(id) > 0) {
        auto rule = rule_store.at(id);

        // Remove rule from rate limit rule pointer
        for(const auto &entry : rule.values) {
            rate_limit_entries.erase(rate_limit_rule_entry_t{rule.resource_type,entry});
        }

        // Update rule in rule store
        rule_store.at(id) = new_rule;

        // Add rule from rule store to rate limit rule pointer
        for(const auto &entry : new_rule.values) {
            rate_limit_entries.insert({rate_limit_rule_entry_t{new_rule.resource_type,entry}, &rule_store.at(id)});
        }

        return true;
    }

    return false;
}

const std::vector<rate_limit_rule_t> RateLimitManager::get_all_rules() {
    std::shared_lock<std::shared_mutex> lock(rate_limit_mutex);

    // Get all rules in a vector
    std::vector<rate_limit_rule_t> rules;
    for(const auto &rule : rule_store) {
        rules.push_back(rule.second);
    }

    return rules;
}

const std::vector<rate_limit_status_t> RateLimitManager::get_banned_entries(const RateLimitedResourceType resource_type) {

    std::shared_lock<std::shared_mutex> lock(rate_limit_mutex);

    std::vector < rate_limit_status_t > banned_entries;

    for (auto & element: throttled_entries) {
        if (element.second.resource_type == resource_type) {
            banned_entries.push_back(element.second);
        }
    }
    

    // Get permanent bans
    for (auto & element: rule_store) {
        if (element.second.action == RateLimitAction::BLOCK && element.second.resource_type == resource_type) {
            for (auto & entry : element.second.values) {
                banned_entries.push_back(rate_limit_status_t{true,0,0,entry, resource_type});
            }
        }
    }

    return banned_entries;
    
}

void RateLimitManager::clear_all() {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);
    rate_limit_request_counts.clear();
    rate_limit_entries.clear();
    throttled_entries.clear();
    rule_store.clear();
    last_rule_id = 0;
}

void RateLimitManager::temp_ban_entry_wrapped(const rate_limit_rule_entry_t& entry, const int64_t number_of_days) {

    // Check if entry is already banned
    if (throttled_entries.count(entry) > 0 && throttled_entries.at(entry).is_banned) {
        return;
    }

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // Add entry to throttled_entries for the given number of days
    throttled_entries.insert({entry, rate_limit_status_t{true, now, now + (number_of_days * 24 * 60 * 60), entry.value, entry.resource_type}});


    if(rate_limit_request_counts.contains(entry)){
        // Reset counters for the given entry
        rate_limit_request_counts.lookup(entry).current_requests_count_minute = 0;
        rate_limit_request_counts.lookup(entry).current_requests_count_hour = 0;
    }

}
