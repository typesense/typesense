#include "ratelimit_manager.h"
#include <iterator>

RateLimitManager * RateLimitManager::getInstance() {
    if (!instance) {
        instance = new RateLimitManager();
    }

    return instance;
}

bool RateLimitManager::throttle_entities(const RateLimitedEntityType entity_type, const std::vector<std::string> &entities, const int64_t minute_threshold, const int64_t hour_threshold, const int64_t auto_ban_threshold_num, const int64_t auto_ban_num_days) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);

    for(const auto &entity : entities) {
        if (rate_limit_entities.count(rate_limit_entity_t{entity_type,entity}) > 0) {
            return false;
        }
    }

    // add rate limit for entity
    rule_store.insert({last_rule_id,rate_limit_rule_t{last_rule_id,RateLimitAction::throttle, entity_type,entities, rate_limit_max_requests_t{minute_threshold, hour_threshold}, auto_ban_threshold_num, auto_ban_num_days}});

    // Add rule from rule store to rate limit rule pointer
    for(const auto &entity : entities) {
        rate_limit_entities.insert({rate_limit_entity_t{entity_type,entity}, &rule_store.at(last_rule_id)});
    }

    last_rule_id++;;
    return true;

}

bool RateLimitManager::remove_rule_entity(const RateLimitedEntityType entity_type, const std::string &entity) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);

    // Check if a rule exists for the given IP
    if (rate_limit_entities.count(rate_limit_entity_t{entity_type,entity}) == 0) {
        return false;
    }

    // Remove the rule from the rule store
    rule_store.erase(rate_limit_entities.at(rate_limit_entity_t{entity_type,entity})->id);

    // Remove the rule from the entity rate limits map
    rate_limit_entities.erase(rate_limit_entity_t{entity_type,entity});

    return true;

}

void RateLimitManager::temp_ban_entity(const rate_limit_entity_t& entity, const int64_t number_of_days) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);
    temp_ban_entity_wrapped(entity, number_of_days);
}

bool RateLimitManager::ban_entities_permanently(const RateLimitedEntityType entity_type, const std::vector<std::string> &entities) {
    // lock mutex
    std::unique_lock < std::shared_mutex > lock(rate_limit_mutex);

    // Check if a rule exists for the given IP addresses
    for(const auto& entity : entities) {
        if (rate_limit_entities.count(rate_limit_entity_t{entity_type,entity}) > 0) {
            return false;
        }
    }

    // add rate limit for entity
    rule_store.insert({last_rule_id,rate_limit_rule_t{last_rule_id,RateLimitAction::block, entity_type,entities, rate_limit_max_requests_t{0, 0}, -1, -1}});

    // Add rule from rule store to rate limit rule pointer
    for(const auto &entity : entities) {
        rate_limit_entities.insert({rate_limit_entity_t{entity_type,entity}, &rule_store.at(last_rule_id)});
    }

    last_rule_id++;
    return true;
}



bool RateLimitManager::is_rate_limited(const std::vector<rate_limit_entity_t> &entities) {
    // lock mutex
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);
    
    // Check if any of the entities has rule
    for(const auto &entity : entities) {

        auto entity_rule = rate_limit_entities.find(entity);
        auto wildcard_rule = rate_limit_entities.find(rate_limit_entity_t{entity.entity_type, ".*"});

        if (entity_rule == rate_limit_entities.end() && wildcard_rule == rate_limit_entities.end()) {
            continue;
        }

        const auto& rule =  entity_rule != rate_limit_entities.end() ? *(entity_rule->second) : *(wildcard_rule->second);

        if (rule.action == RateLimitAction::block) {
            return true;
        }
        else if(rule.action == RateLimitAction::allow) {
            return false;
        }
        else if(throttled_entities.count(entity) > 0) {
            
            // Check if ban duration is not over
            if (throttled_entities.at(entity).throttling_to > std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) {
                return true;
            }
 
            // Remove ban
            throttled_entities.erase(entity);


            // Reset request counts
            auto& request_counts = rate_limit_request_counts.lookup(entity);
            request_counts.reset();


            // Check if throttle rule still exists
            if (rule.action == RateLimitAction::throttle) {
                // Increase because of this request
                request_counts.current_requests_count_minute++;
                request_counts.current_requests_count_hour++;
            }

            continue;

        }
        else {
            
            if(!rate_limit_request_counts.contains(entity)){
                rate_limit_request_counts.insert(entity, request_counter_t{});
            }
            
            auto& request_counts = rate_limit_request_counts.lookup(entity);

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



            // Check if request count is over the limit
            auto current_rate_for_minute = (60 - (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - request_counts.last_reset_time_minute)) / 60  * request_counts.previous_requests_count_minute;
            current_rate_for_minute += request_counts.current_requests_count_minute;

            if(rule.max_requests.minute_threshold >= 0 && current_rate_for_minute >= rule.max_requests.minute_threshold) {
                if(rule.auto_ban_threshold_num >= 0 && rule.auto_ban_num_days >= 0) {
                    if(++request_counts.threshold_exceed_count_minute > rule.auto_ban_threshold_num) {
                        temp_ban_entity_wrapped(entity, rule.auto_ban_num_days);
                        return true;
                    }
                } 
                else {
                    return true;
                }
            }


            auto current_rate_for_hour = (3600 - (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - request_counts.last_reset_time_hour)) / 3600  * request_counts.previous_requests_count_hour;
            current_rate_for_hour += request_counts.current_requests_count_hour;

            if(rule.max_requests.hour_threshold >= 0 && current_rate_for_hour >= rule.max_requests.hour_threshold) {
                return true;
            }

            // Increment request counts
            request_counts.current_requests_count_minute++;
            request_counts.current_requests_count_hour++;
        }
    }

    return false;
}



bool RateLimitManager::allow_entities(RateLimitedEntityType entity_type, const std::vector<std::string> &entities) {
    std::unique_lock<std::shared_mutex>lock(rate_limit_mutex);

    // Check if a rule exists for the given API keys
    for(const auto& entity : entities) {
        if (rate_limit_entities.count(rate_limit_entity_t{entity_type,entity}) > 0) {
            return false;
        }
    }

    // add rate limit for entity
    rule_store.insert({last_rule_id,rate_limit_rule_t{last_rule_id,RateLimitAction::allow, entity_type,entities, rate_limit_max_requests_t{0, 0}}});

    // Add rule from rule store to rate limit rule pointer
    for(const auto &entity : entities) {
        rate_limit_entities.insert({rate_limit_entity_t{entity_type,entity}, &rule_store.at(last_rule_id)});
    }

    last_rule_id++;;
    return true;
}

Option<nlohmann::json> RateLimitManager::find_rule_by_id(const uint64_t id) {
    std::shared_lock<std::shared_mutex> lock(rate_limit_mutex);

    if(rule_store.count(id) > 0) {
        return Option<nlohmann::json>(rule_store.at(id).to_json());
    }

    return Option<nlohmann::json>(404, "Not Found");
}

bool RateLimitManager::delete_rule_by_id(const uint64_t id) {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);

    // Check if a rule exists for the given ID
    if(rule_store.count(id) > 0) {
        auto rule = rule_store.at(id);

        // Remove rule from rule store
        rule_store.erase(id);

        // Remove rule from rate limit rule pointer
        for(const auto &entity : rule.entity_ids) {
            rate_limit_entities.erase(rate_limit_entity_t{rule.entity_type,entity});
        }

        return true;
    }

    return false;
}

bool RateLimitManager::edit_rule_by_id(const uint64_t id, const rate_limit_rule_t& new_rule) {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);

    // Check if a rule exists for the given ID
    auto rule = rule_store.find(id);


    if(rule != rule_store.end()) {

        // Remove rule from rate limit rule pointer
        for(const auto &entity : rule->second.entity_ids) {
            rate_limit_entities.erase(rate_limit_entity_t{rule->second.entity_type,entity});
        }

        // Update rule in rule store
        rule_store[id] = new_rule;

        // Add rule from rule store to rate limit rule pointer
        for(const auto &entity : new_rule.entity_ids) {
            rate_limit_entities.insert({rate_limit_entity_t{new_rule.entity_type,entity}, &rule_store.at(id)});
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

const std::vector<rate_limit_status_t> RateLimitManager::get_banned_entities(const RateLimitedEntityType entity_type) {

    std::shared_lock<std::shared_mutex> lock(rate_limit_mutex);

    std::vector < rate_limit_status_t > banned_entities;

    for (auto & element: throttled_entities) {
        if (element.second.entity_type == entity_type) {
            banned_entities.push_back(element.second);
        }
    }
    

    // Get permanent bans
    for (auto & element: rule_store) {
        if (element.second.action == RateLimitAction::block && element.second.entity_type == entity_type) {
            for (auto & entity : element.second.entity_ids) {
                banned_entities.push_back(rate_limit_status_t{0,0,entity, entity_type});
            }
        }
    }

    return banned_entities;
    
}

void RateLimitManager::clear_all() {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);
    rate_limit_request_counts.clear();
    rate_limit_entities.clear();
    throttled_entities.clear();
    rule_store.clear();
    last_rule_id = 0;
}

void RateLimitManager::temp_ban_entity_wrapped(const rate_limit_entity_t& entity, const int64_t number_of_days) {

    // Check if entity is already banned
    if (throttled_entities.count(entity) > 0) {
        return;
    }

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // Add entity to throttled_entities for the given number of days
    throttled_entities.insert({entity, rate_limit_status_t{now, now + (number_of_days * 24 * 60 * 60), entity.entity_id, entity.entity_type}});


    if(rate_limit_request_counts.contains(entity)){
        // Reset counters for the given entity
        rate_limit_request_counts.lookup(entity).current_requests_count_minute = 0;
        rate_limit_request_counts.lookup(entity).current_requests_count_hour = 0;
    }

}

const nlohmann::json RateLimitManager::get_all_rules_json()
{
    std::shared_lock<std::shared_mutex> lock(rate_limit_mutex);

    nlohmann::json rules_json = nlohmann::json::array();

    for(const auto &rule : rule_store) {
        rules_json.push_back(rule.second.to_json());
    }

    return rules_json;
}


const nlohmann::json rate_limit_rule_t::to_json() const {
        nlohmann::json rule;
        rule["id"] = id;
        rule["action"] = magic_enum::enum_name(action);
        rule["entity_type"] = magic_enum::enum_name(entity_type);
        rule["entity_ids"] = entity_ids;
        
        if(max_requests.minute_threshold >= 0) {
        rule["max_requests"]["minute_threshold"] = max_requests.minute_threshold;
        }

        if(max_requests.hour_threshold >= 0) {
        rule["max_requests"]["hour_threshold"] = max_requests.hour_threshold;
        }

        if(auto_ban_threshold_num >= 0) {
        rule["auto_ban_threshold_num"] = auto_ban_threshold_num;
        }

        if(auto_ban_num_days >= 0) {
        rule["auto_ban_num_days"] = auto_ban_num_days;
        }

        return rule;
}