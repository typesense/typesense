#include "ratelimit_manager.h"
#include <iterator>

RateLimitManager * RateLimitManager::getInstance() {
    if (!instance) {
        instance = new RateLimitManager();
    }

    return instance;
}

bool RateLimitManager::throttle_entries(const RateLimitedResourceType resource_type, const std::vector<std::string> &entries, const int64_t minute_rate_limit, const int64_t hour_rate_limit) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    for(const auto &entry : entries) {
        if (rate_limit_rule_pointer.count(rate_limit_rule_entry_t{resource_type,entry}) > 0) {
            return false;
        }
    }

    // add rate limit for entry
    rule_store.push_back(rate_limit_rule_t{last_rule_id++,RateLimitAction::THROTTLE, resource_type,entries, rate_limit_throttle_t{minute_rate_limit, hour_rate_limit}});

    // Add rule from rule store to rate limit rule pointer
    for(const auto &entry : entries) {
        rate_limit_rule_pointer.insert({rate_limit_rule_entry_t{resource_type,entry}, &rule_store.back()});
    }

    return true;

}

bool RateLimitManager::remove_rule_entry(const RateLimitedResourceType resource_type, const std::string &entry) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if a rule exists for the given IP
    if (rate_limit_rule_pointer.count(rate_limit_rule_entry_t{resource_type,entry}) == 0) {
        return false;
    }

    // Remove the rule from the rule store
    rule_store.erase(rule_store.begin() + std::distance(rule_store.data(), rate_limit_rule_pointer.at(rate_limit_rule_entry_t{resource_type,entry})));

    // Remove the rule from the entry rate limits map
    rate_limit_rule_pointer.erase(rate_limit_rule_entry_t{resource_type,entry});

    return true;

}

void RateLimitManager::temp_ban_entry(const rate_limit_rule_entry_t& entry) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);
    temp_ban_entry_wrapped(entry);
}

bool RateLimitManager::ban_entries_permanently(const RateLimitedResourceType resource_type, const std::vector<std::string> &entries) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if a rule exists for the given IP addresses
    for(const auto& entry : entries) {
        if (rate_limit_rule_pointer.count(rate_limit_rule_entry_t{resource_type,entry}) > 0) {
            return false;
        }
    }

    // Add rule to rule store
    rule_store.push_back(rate_limit_rule_t{last_rule_id++,RateLimitAction::BLOCK, resource_type,entries, rate_limit_throttle_t{0, 0}});

    // Add rule from rule store to ip_rate_limits
    for(const auto &entry : entries) {
        rate_limit_rule_pointer.insert({rate_limit_rule_entry_t{resource_type, entry}, &rule_store.back()});
    }

    return true;
}



bool RateLimitManager::is_rate_limited(const std::vector<rate_limit_rule_entry_t> &entries) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);
    
    // Check if any of the entries has rule
    for(const auto &entry : entries) {
        if (rate_limit_rule_pointer.count(entry) > 0) {
            
            const auto& rule = *(rate_limit_rule_pointer.at(entry));
            if (rule.action == RateLimitAction::BLOCK) {
                return true;
            }
            else if(rule.action == RateLimitAction::ALLOW) {
                return false;
            }
            else if(throttled_entries.count(entry) > 0 && throttled_entries.at(entry).is_banned) {
                // Check if ban duration is over
                if (throttled_entries.at(entry).throttling_to <= std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
                    
                    // Remove ban
                    throttled_entries.at(entry).is_banned = false;

                    // Check if throttle rule still exists
                    if (rate_limit_rule_pointer.count(entry) > 0 && (rate_limit_rule_pointer.at(entry)->action == RateLimitAction::ALLOW)) {
                        rate_limit_request_counts.lookup(entry).current_requests_count_minute++;
                        rate_limit_request_counts.lookup(entry).current_requests_count_hour++;
                    }

                    continue;
                }
                else {
                    return true;
                }
            }
            else {
                
                if(!rate_limit_request_counts.contains(entry)){
                    rate_limit_request_counts.insert(entry, request_counter_t{});
                }
                
                auto& request_counts = rate_limit_request_counts.lookup(entry);

                // Check if last reset time was more than 1 minute ago
                if (request_counts.last_reset_time_minute < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - 60) {
                    request_counts.previous_requests_count_minute = request_counts.current_requests_count_minute;
                    request_counts.current_requests_count_minute = 0;
                    request_counts.last_reset_time_minute = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                }

                // Check if last reset time was more than 1 hour ago
                if (request_counts.last_reset_time_hour < std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - 3600) {
                    request_counts.previous_requests_count_hour = request_counts.current_requests_count_hour;
                    request_counts.current_requests_count_hour = 0;
                    request_counts.last_reset_time_hour = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                }

                // Increment request counts
                request_counts.current_requests_count_minute++;
                request_counts.current_requests_count_hour++;

                // Check if request count is over the limit
                auto current_rate_for_minute = (60 - (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - request_counts.last_reset_time_minute)) / 60  * request_counts.previous_requests_count_minute;
                current_rate_for_minute += request_counts.current_requests_count_minute;

                if(rule.throttle.minute_rate_limit >= 0 && current_rate_for_minute > rule.throttle.minute_rate_limit) {
                    temp_ban_entry_wrapped(entry);
                    return true;
                }

                auto current_rate_for_hour = (3600 - (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - request_counts.last_reset_time_hour)) / 3600  * request_counts.previous_requests_count_hour;
                current_rate_for_hour += request_counts.current_requests_count_hour;

                if(rule.throttle.hour_rate_limit >= 0&& current_rate_for_hour > rule.throttle.hour_rate_limit) {
                    temp_ban_entry_wrapped(entry);
                    return true;
                }
            }
        }
    }

    return false;
}



bool RateLimitManager::allow_entries(RateLimitedResourceType resource_type, const std::vector<std::string> &entries) {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if a rule exists for the given API keys
    for(const auto& entry : entries) {
        if (rate_limit_rule_pointer.count(rate_limit_rule_entry_t{resource_type,entry}) > 0) {
            return false;
        }
    }

    // Add rule to rule store
    rule_store.push_back(rate_limit_rule_t{last_rule_id++,RateLimitAction::ALLOW, resource_type,entries, rate_limit_throttle_t{0, 0}});

    // Add rule from rule store to entry_rate_limits
    for(const auto &entry : entries) {
        rate_limit_rule_pointer.insert({rate_limit_rule_entry_t{resource_type,entry}, &rule_store.back()});
    }


    return true;
}

const rate_limit_rule_t* RateLimitManager::find_rule_by_id(const uint64_t id) {
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);

    for(const auto &rule : rule_store) {
        if (rule.id == id) {
            return &rule;
        }
    }

    return nullptr;
}

bool RateLimitManager::delete_rule_by_id(const uint64_t id) {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if a rule exists for the given ID
    for(auto it = rule_store.begin(); it != rule_store.end(); ++it) {
        if (it->id == id) {
            
            // Erase rule pointer from rate_limit_rule_pointer
            for(const auto &value : it->values){
                rate_limit_rule_pointer.erase({it->resource_type,value});
            }
            // Erase rule from rule_store
            rule_store.erase(it);
            return true;
        }
    }

    return false;

}

bool RateLimitManager::edit_rule_by_id(const uint64_t id, const rate_limit_rule_t & new_rule) {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if a rule exists for the given ID
    for(auto it = rule_store.begin(); it != rule_store.end(); ++it) {
        if (it->id == id) {
            // Erase rule pointer from rate_limit_rule_pointer
            for(const auto &value : it->values){
                rate_limit_rule_pointer.erase({it->resource_type,value});
            }
            // Erase rule from rule_store
            rule_store.erase(it);
            // Add rule to rule store
            rule_store.push_back(rate_limit_rule_t{id,new_rule.action, new_rule.resource_type, new_rule.values, new_rule.throttle});
            // Add rule from rule store to rate_limit_rule_pointer
            for(const auto &value : new_rule.values){
                rate_limit_rule_pointer.insert({rate_limit_rule_entry_t{new_rule.resource_type,value}, &rule_store.back()});
            }
            return true;
        }
    }

    return false;
}

const std::vector <rate_limit_rule_t>& RateLimitManager::get_all_rules() {
    std::shared_lock < std::shared_mutex > lock(rate_limit_mutex);
    return rule_store;
}

const std::vector < rate_limit_ban_t > RateLimitManager::get_banned_entries(const RateLimitedResourceType resource_type) {

    std::shared_lock <std::shared_mutex> lock(rate_limit_mutex);

    std::vector < rate_limit_ban_t > banned_entries;

    for (auto & element: throttled_entries) {
        if (element.second.resource_type == resource_type) {
            banned_entries.push_back(element.second);
        }
    }
    

    // Get permanent bans
    for (auto & element: rule_store) {
        if (element.action == RateLimitAction::BLOCK && element.resource_type == resource_type) {
            for (auto & entry : element.values) {
                banned_entries.push_back(rate_limit_ban_t{true,0,0,entry, resource_type, RateLimitBanDuration::PERMANENT});
            }
        }
    }

    return banned_entries;
    
}

void RateLimitManager::clear_all() {
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);
    rate_limit_request_counts.clear();
    rate_limit_rule_pointer.clear();
    throttled_entries.clear();
    rule_store.clear();
    last_rule_id = 0;
}

void RateLimitManager::temp_ban_entry_wrapped(const rate_limit_rule_entry_t& entry) {
    switch (throttled_entries[entry].banDuration) {
    case RateLimitBanDuration::NO_BAN_BEFORE:
        throttled_entries[entry].banDuration = RateLimitBanDuration::ONE_MINUTE;
        throttled_entries[entry].value = entry.value;
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 60;
        throttled_entries[entry].is_banned = true;
        break;
    case RateLimitBanDuration::ONE_MINUTE:
        throttled_entries[entry].banDuration = RateLimitBanDuration::FIVE_MINUTES;
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 300;
        throttled_entries[entry].is_banned = true;
        break;
    case RateLimitBanDuration::FIVE_MINUTES:
        throttled_entries[entry].banDuration = RateLimitBanDuration::TEN_MINUTES;
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 600;
        throttled_entries[entry].is_banned = true;
        break;
    case RateLimitBanDuration::TEN_MINUTES:
        throttled_entries[entry].banDuration = RateLimitBanDuration::THIRTY_MINUTES;
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 1800;
        throttled_entries[entry].is_banned = true;
        break;
    case RateLimitBanDuration::THIRTY_MINUTES:
        throttled_entries[entry].banDuration = RateLimitBanDuration::ONE_HOUR;
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 3600;
        throttled_entries[entry].is_banned = true;
        break;
    case RateLimitBanDuration::ONE_HOUR:
        throttled_entries[entry].banDuration = RateLimitBanDuration::THREE_HOURS;
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 10800;
        throttled_entries[entry].is_banned = true;
        break;
    case RateLimitBanDuration::THREE_HOURS:
        throttled_entries[entry].banDuration = RateLimitBanDuration::SIX_HOURS;
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 21600;
        throttled_entries[entry].is_banned = true;
        break;
    case RateLimitBanDuration::SIX_HOURS:
        throttled_entries[entry].throttling_from = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        throttled_entries[entry].throttling_to = throttled_entries[entry].throttling_from + 21600;
        throttled_entries[entry].is_banned = true;
        break;
    }

    rate_limit_request_counts[entry].current_requests_count_minute = 0;

    if (throttled_entries[entry].banDuration == RateLimitBanDuration::ONE_HOUR || throttled_entries[entry].banDuration == RateLimitBanDuration::THREE_HOURS || throttled_entries[entry].banDuration == RateLimitBanDuration::SIX_HOURS) {
        rate_limit_request_counts[entry].current_requests_count_hour = 0;
    }
}
