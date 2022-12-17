#include "ratelimit_manager.h"
#include "string_utils.h"
#include "logger.h"
#include <iterator>

RateLimitManager * RateLimitManager::getInstance() {
    if (!instance) {
        instance = new RateLimitManager();
    }

    return instance;
}


bool RateLimitManager::remove_rule_entity(const RateLimitedEntityType entity_type, const std::string &entity) {
    // lock mutex
    std::unique_lock lock(rate_limit_mutex);
    bool found_rule = false;
    // Check if a OR rule exists for the given entity
    if (rate_limit_entities.count(rate_limit_entity_t{entity_type,entity}) != 0) {
        // Remove the rule from the rule store
        rule_store.erase(rate_limit_entities.at(rate_limit_entity_t{entity_type,entity})->id);
        // Remove the rule from the entity rate limits map
        rate_limit_entities.erase(rate_limit_entity_t{entity_type,entity});
        found_rule = true;
    }
    // Check if a AND rule exists for the given entity
    std::unordered_map<std::pair<rate_limit_entity_t, rate_limit_entity_t>, rate_limit_rule_t*>::iterator it;
    while((it = std::find_if(rate_limit_entities_and.begin(), rate_limit_entities_and.end(), 
        [entity_type, entity](const std::pair<std::pair<rate_limit_entity_t, rate_limit_entity_t>, rate_limit_rule_t*> &p) {
            if(entity_type == RateLimitedEntityType::api_key) {
                return p.first.first.entity_id == entity;
            }
            else if(entity_type == RateLimitedEntityType::ip) {
                return p.first.second.entity_id == entity;
            }
            return false;
        })) != rate_limit_entities_and.end()) {
        // Remove the rule from the rule store
        rule_store.erase(it->second->id);
        // Remove the rule from the entity rate limits map
        rate_limit_entities_and.erase(it);
        found_rule = true;
    }
    return found_rule;
}

void RateLimitManager::temp_ban_entity(const rate_limit_entity_t& entity, const int64_t number_of_hours) {
    // lock mutex
    std::shared_lock lock(rate_limit_mutex);
    temp_ban_entity_unsecure(entity, number_of_hours);
}

bool RateLimitManager::is_rate_limited(const std::vector<rate_limit_entity_t> &entities) {
    // lock mutex
    std::shared_lock lock(rate_limit_mutex);

    rate_limit_entity_t api_key_entity, ip_entity;
    // Find the api_key and ip entities 
    for(auto &entity : entities) {
        if(entity.entity_type == RateLimitedEntityType::api_key) {
            api_key_entity = entity;
        }
        else if(entity.entity_type == RateLimitedEntityType::ip) {
            ip_entity = entity;
        }
    }
    // Check if a AND rule exists for the given entities
    auto and_rule = rate_limit_entities_and.find(std::make_pair(api_key_entity, ip_entity));
    // Pointer to the rule with the highest priority
    rate_limit_rule_t *rule = nullptr;
    bool is_and = false;

    if (and_rule != rate_limit_entities_and.end()) {
        if (rule == nullptr || and_rule->second->priority > rule->priority) {
            rule = &(*and_rule->second);
            is_and = true;
        }
    }
    // Get the rule with the highest priority
    for(auto &entity : entities) {
        auto entity_rule = rate_limit_entities.find(entity);
        auto wildcard_rule = rate_limit_entities.find(rate_limit_entity_t{entity.entity_type, ".*"});
        // Pick the rule with the highest priority between entitiy_rule, wildcard_rule and and_rule
        if (entity_rule != rate_limit_entities.end()) {
            if (rule == nullptr || entity_rule->second->priority > rule->priority) {
                rule = &(*entity_rule->second);
                is_and = rule->is_and;
            }
        }
        if (wildcard_rule != rate_limit_entities.end()) {
            if (rule == nullptr || wildcard_rule->second->priority > rule->priority) {
                rule = &(*wildcard_rule->second);
                is_and = rule->is_and;
            }
        }
    }
    // If no rule was found, return false
    if (rule == nullptr) {
        return false;
    }
    
    const auto& throttled_entity_it = (is_and ? throttled_entities[ip_entity].find(rate_limit_entity_t{RateLimitedEntityType::api_key, ".*"}) : throttled_entities[ip_entity].find(api_key_entity));

    if (rule->action == RateLimitAction::block) {
        return true;
    }
    else if(rule->action == RateLimitAction::allow) {
        return false;
    }

    // Check if the entities are already throttled
    if(throttled_entities.count(ip_entity) > 0 && throttled_entity_it != throttled_entities[ip_entity].end()) {
        auto& throttled_entity = throttled_entity_it->second;

        // Check if ban duration is not over                                              
        if (throttled_entity.throttling_to > get_current_time()) {
            return true;
        }
        // Remove ban from DB store
        std::string ban_key = std::string(BANS_PREFIX) + "_" + std::to_string(throttled_entity.status_id);
        store->remove(ban_key);
        // Remove ban
        throttled_entities[ip_entity].erase(throttled_entity_it);
        if(throttled_entities[ip_entity].empty()) {
            throttled_entities.erase(ip_entity);
        }
        // Reset request counts
        auto& request_counts = is_and ? rate_limit_request_counts_and.lookup(std::make_pair(api_key_entity, ip_entity)) : rate_limit_request_counts.lookup(ip_entity);
        request_counts.reset();

        // Increase because of this request
        request_counts.current_requests_count_minute++;
        request_counts.current_requests_count_hour++;
        return false;
    }

    if(!is_and && !rate_limit_request_counts.contains(ip_entity)){
        rate_limit_request_counts.insert(ip_entity, request_counter_t{});
    } else if(is_and && !rate_limit_request_counts_and.contains(std::make_pair(api_key_entity, ip_entity))){
        rate_limit_request_counts_and.insert(std::make_pair(api_key_entity, ip_entity), request_counter_t{});
    }
    
    auto& request_counts = is_and ? rate_limit_request_counts_and.lookup(std::make_pair(api_key_entity, ip_entity)) : rate_limit_request_counts.lookup(ip_entity);
    // Check if last reset time was more than 1 minute ago
    if (request_counts.last_reset_time_minute <= get_current_time() - 60) {
        request_counts.previous_requests_count_minute = request_counts.current_requests_count_minute;
        request_counts.current_requests_count_minute = 0;
        if(request_counts.last_reset_time_minute <= get_current_time() - 120) {
            request_counts.previous_requests_count_minute = 0;
        }
        request_counts.last_reset_time_minute = get_current_time();
    }
    // Check if last reset time was more than 1 hour ago
    if (request_counts.last_reset_time_hour <= get_current_time() - 3600) {
        request_counts.previous_requests_count_hour = request_counts.current_requests_count_hour;
        request_counts.current_requests_count_hour = 0;
        if(request_counts.last_reset_time_hour <= get_current_time() - 7200) {
            request_counts.previous_requests_count_hour = 0;
        }
        request_counts.last_reset_time_hour = get_current_time();
    }
    // Check if request count is over the limit
    auto current_rate_for_minute = (60 - (get_current_time() - request_counts.last_reset_time_minute)) / 60  * request_counts.previous_requests_count_minute;
    current_rate_for_minute += request_counts.current_requests_count_minute;
    if(rule->max_requests.minute_threshold >= 0 && current_rate_for_minute >= rule->max_requests.minute_threshold) {
        bool auto_ban_is_enabled = (rule->auto_ban_threshold_num > 0 && rule->auto_ban_num_hours > 0);
        if(auto_ban_is_enabled) {
            if(get_current_time() - request_counts.last_threshold_exceed_time >= 60) {
                request_counts.threshold_exceed_count_minute++;
                request_counts.last_threshold_exceed_time = get_current_time();
            }
            if(request_counts.threshold_exceed_count_minute > rule->auto_ban_threshold_num) {
                temp_ban_entity_unsecure(ip_entity, rule->auto_ban_num_hours, is_and ? &api_key_entity : nullptr);
            }
        } 
        return true;
    }
    auto current_rate_for_hour = (3600 - (get_current_time() - request_counts.last_reset_time_hour)) / 3600  * request_counts.previous_requests_count_hour;
    current_rate_for_hour += request_counts.current_requests_count_hour;
    if(rule->max_requests.hour_threshold >= 0 && current_rate_for_hour >= rule->max_requests.hour_threshold) {
        return true;
    }
    // Increment request counts
    request_counts.current_requests_count_minute++;
    request_counts.current_requests_count_hour++;

    return false;
}

Option<nlohmann::json> RateLimitManager::find_rule_by_id(const uint64_t id) {
    std::shared_lock lock(rate_limit_mutex);

    if(rule_store.count(id) > 0) {
        return Option<nlohmann::json>(rule_store.at(id).to_json());
    }
    return Option<nlohmann::json>(404, "Not Found");
}

bool RateLimitManager::delete_rule_by_id(const uint64_t id) {
    std::unique_lock lock(rate_limit_mutex);
    const std::string rule_store_key = get_rule_key(id);
    bool deleted = store->remove(rule_store_key);
    if(!deleted) {
        return false;
    }
    // Check if a rule exists for the given ID
    if(rule_store.count(id) > 0) {
        auto rule = rule_store.at(id);
        // Remove rule from rule store
        rule_store.erase(id);
        // Remove rule from rate limit rule pointer
        if(!rule.is_and) {
            for(const auto& entity : rule.entities) {
                rate_limit_entities.erase(entity);
            }
        } else {
            std::vector<rate_limit_entity_t> ip_entities, api_key_entities;
            for(const auto& entity : rule.entities) {
                if(entity.entity_type == RateLimitedEntityType::ip) {
                    ip_entities.push_back(entity);
                } else {
                    api_key_entities.push_back(entity);
                }
            }

            // Delete all combinations of ip and api key entities from AND store
            for(const auto& ip_entity : ip_entities) {
                for(const auto& api_key_entity : api_key_entities) {
                    rate_limit_entities_and.erase(std::make_pair(api_key_entity, ip_entity));
                }
            }
        }
        return true;
    }
    return false;
}


const std::vector<rate_limit_rule_t> RateLimitManager::get_all_rules() {
    std::shared_lock lock(rate_limit_mutex);

    // Get all rules in a vector
    std::vector<rate_limit_rule_t> rules;
    for(const auto &rule : rule_store) {
        rules.push_back(rule.second);
    }
    return rules;
}

const std::vector<rate_limit_status_t> RateLimitManager::get_banned_entities() {
    std::shared_lock lock(rate_limit_mutex);

    std::vector <rate_limit_status_t> banned_entities;
    for (auto& element: throttled_entities) {
        for (auto& entity: element.second) {
            banned_entities.push_back(entity.second);
        }
    }
    // Get permanent bans
    for (auto& element: rule_store) {
        if (element.second.action == RateLimitAction::block) {
            for (auto& entity: element.second.entities) {
                    banned_entities.push_back(rate_limit_status_t{last_ban_id, 0, 0, entity});
            }
        }
    }
    return banned_entities;
}

void RateLimitManager::clear_all() {
    std::shared_lock lock(rate_limit_mutex);
    rate_limit_request_counts.clear();
    rate_limit_entities.clear();
    rate_limit_entities_and.clear();
    rate_limit_request_counts_and.clear();
    throttled_entities.clear();
    rule_store.clear();
    last_rule_id = 0;
    last_ban_id = 0;
    base_timestamp = 0;
}

void RateLimitManager::temp_ban_entity_unsecure(const rate_limit_entity_t& entity, const int64_t number_of_hours, const rate_limit_entity_t* and_entity) {
    // Check if entity is already banned
    if (throttled_entities.find(entity) != throttled_entities.end() && throttled_entities[entity].find(and_entity != nullptr ? *and_entity : rate_limit_entity_t{RateLimitedEntityType::api_key, ".*"}) != throttled_entities[entity].end()) {
        return;
    }
    auto now = get_current_time();
    // Add entity to throttled_entities for the given number of days
    rate_limit_status_t status(last_ban_id, now, now + (number_of_hours * 60 * 60), entity, and_entity);
    std::string ban_key = get_ban_key(last_ban_id);
    bool inserted = store->insert(ban_key, status.to_json().dump());
    if(!inserted) {
        LOG(INFO) << "Failed to insert ban for entity " << entity.entity_id;
    }
    if(and_entity != nullptr) {
        throttled_entities[entity][*and_entity] = status;
    } else {
        throttled_entities[entity][rate_limit_entity_t{RateLimitedEntityType::api_key, ".*"}] = status;
    }
    last_ban_id++;
    store->increment(std::string(BANS_NEXT_ID), 1);
    if(rate_limit_request_counts.contains(entity)){
        // Reset counters for the given entity
        rate_limit_request_counts.lookup(entity).current_requests_count_minute = 0;
        rate_limit_request_counts.lookup(entity).current_requests_count_hour = 0;
    }
}

const nlohmann::json RateLimitManager::get_all_rules_json() {
    std::shared_lock lock(rate_limit_mutex);

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
        rule["priority"] = priority;
        for(const auto& entity : entities) {
            if(entity.entity_type == RateLimitedEntityType::ip) {
                if(!rule.contains("ip_addresses")) {
                    rule["ip_addresses"] = nlohmann::json::array();
                }
                rule["ip_addresses"].push_back(entity.entity_id);
            } else  {
                if(!rule.contains("api_keys")) {
                    rule["api_keys"] = nlohmann::json::array();
                }
                rule["api_keys"].push_back(entity.entity_id);
            }
        }
        if(max_requests.minute_threshold >= 0) {
            rule["max_requests_1m"] = max_requests.minute_threshold;
        }
        if(max_requests.hour_threshold >= 0) {
            rule["max_requests_1h"] = max_requests.hour_threshold;
        }
        if(auto_ban_threshold_num >= 0) {
            rule["auto_ban_threshold_num"] = auto_ban_threshold_num;
        }
        if(auto_ban_num_hours >= 0) {
            rule["auto_ban_num_hours"] = auto_ban_num_hours;
        }
        return rule;
}

const nlohmann::json rate_limit_status_t::to_json() const {
    nlohmann::json status;
    status["throttling_from"] = throttling_from;
    status["throttling_to"] = throttling_to;
    status["value"] = entity.entity_id;
    status["entity_type"] = magic_enum::enum_name(entity.entity_type);
    if(is_and) {
        status["and_entity"] = nlohmann::json::object();
        status["and_entity"]["value"] = and_entity.entity_id;
        status["and_entity"]["entity_type"] = magic_enum::enum_name(and_entity.entity_type);
    }
    status["id"] = status_id;
    return status;
}

void rate_limit_status_t::parse_json(const nlohmann::json &json) {

    throttling_from = json["throttling_from"];
    throttling_to = json["throttling_to"];
    entity.entity_id = json["value"];
    entity.entity_type = magic_enum::enum_cast<RateLimitedEntityType>(json["entity_type"].get<std::string>()).value();
    if(json.contains("and_entity")) {
        is_and = true;
        and_entity.entity_id = json["and_entity"]["value"];
        and_entity.entity_type = magic_enum::enum_cast<RateLimitedEntityType>(json["and_entity"]["entity_type"].get<std::string>()).value();
    }
    status_id = json["id"];
}

Option<nlohmann::json> RateLimitManager::add_rule(const nlohmann::json &rule_json) {
    auto rule_validation_result = is_valid_rule(rule_json);
    if(!rule_validation_result.ok()) {
        return Option<nlohmann::json>(rule_validation_result.code(), rule_validation_result.error());
    }

    std::shared_lock lock(rate_limit_mutex);
    if(rule_json["ip_addresses"].is_array()) {
        for(const auto& ip: rule_json["ip_addresses"]) {
            // Check if a rule exists for the entity
            if (rate_limit_entities.count(rate_limit_entity_t{RateLimitedEntityType::ip, ip}) > 0) {
                return Option<nlohmann::json>(400, "A rule already exists for one of the entities");
            }
        }
    }

    if(rule_json["api_keys"].is_array()) {
        for(const auto& api_key: rule_json["api_keys"]) {
            // Check if a rule exists for the entity
            if (rate_limit_entities.count(rate_limit_entity_t{RateLimitedEntityType::api_key, api_key}) > 0) {
                return Option<nlohmann::json>(400, "A rule already exists for one of the entities");
            }
        }
    }

    rate_limit_rule_t parsed_rule = parse_rule(rule_json);
    parsed_rule.id = last_rule_id++;
    const std::string rule_store_key = get_rule_key(parsed_rule.id);
    bool inserted = store->insert(rule_store_key, parsed_rule.to_json().dump());
    if(!inserted) {
        return Option<nlohmann::json>(500, "Failed to insert rule into the DB store");
    }
    store->increment(std::string(RULES_NEXT_ID), 1);
    // unlock mutex before inserting rule to rule store
    lock.unlock();
    // Insert rule to rule store
    insert_rule(parsed_rule);
    nlohmann::json response;
    response["message"] = "Rule added successfully.";
    response["rule"] = parsed_rule.to_json();
    return Option<nlohmann::json>(response);
}

Option<nlohmann::json> RateLimitManager::edit_rule(const uint64_t id, const nlohmann::json &rule_json) {
    std::shared_lock lock(rate_limit_mutex);

    const auto& rule_option = find_rule_by_id(id);
    if(!rule_option.ok()) {
        return Option<nlohmann::json>(rule_option.code(), rule_option.error());
    }
    auto rule_validation_result = is_valid_rule(rule_json);
    if(!rule_validation_result.ok()) {
        return Option<nlohmann::json>(rule_validation_result.code(), rule_validation_result.error());
    }

    rate_limit_rule_t parsed_rule = parse_rule(rule_json);
    parsed_rule.id = id;
    lock.unlock();
    // Delete rule from rate limit rule pointer
    delete_rule_by_id(id);
    lock.lock();
    const std::string rule_store_key = get_rule_key(parsed_rule.id);
    bool inserted = store->insert(rule_store_key, parsed_rule.to_json().dump());
    if(!inserted) {
        return Option<nlohmann::json>(500, "Failed to update rule in the DB store");
    }
    // unlock mutex before inserting rule to rule store
    lock.unlock();
    // Insert new rule to rule store
    insert_rule(parsed_rule);
    nlohmann::json response;
    response["message"] = "Rule updated successfully.";
    response["rule"] = parsed_rule.to_json();
    return Option<nlohmann::json>(response);
}

Option<bool> RateLimitManager::is_valid_rule(const nlohmann::json &rule_json) {
    if (rule_json.count("action") == 0) {
        return Option<bool>(400, "Parameter `action` is required.");
    }
    if (rule_json["action"] == "allow") {
        if ((rule_json.count("ip_addresses") == 0 && rule_json.count("api_keys") == 0)) {
            return Option<bool>(400, "`ip_addresses` and/or `api_keys` is required.");
        }
        return Option<bool>(true);
    } else if (rule_json["action"] == "block") {
        if ((rule_json.count("ip_addresses") == 0 && rule_json.count("api_keys") == 0)) {
            return Option<bool>(400, "`ip_addresses` and/or `api_keys` is required.");
        }
        return Option<bool>(true);
    } else if (rule_json["action"] == "throttle") {
        if (rule_json.count("max_requests_1m") == 0 && rule_json.count("max_requests_1h") == 0) {
            return Option<bool>(400, "At least  one of `max_requests_1m` or `max_requests_1h` is required.");
        }
        if (rule_json.count("max_requests_1m") > 0 && rule_json["max_requests_1m"].is_number_integer() == false) {
            return Option<bool>(400, "Parameter `max_requests_1m` must be an integer.");
        }
        if (rule_json.count("max_requests_1h") > 0 && rule_json["max_requests_1h"].is_number_integer() == false) {
            return Option<bool>(400, "Parameter `max_requests_1h` must be an integer.");
        }
        if ((rule_json.count("ip_addresses") == 0 && rule_json.count("api_keys") == 0)) {
            return Option<bool>(400, "`ip_addresses` and/or `api_keys` is required.");
        }
        if(rule_json.count("ip_addresses") > 0 && !rule_json["ip_addresses"].is_array() && !rule_json["ip_addresses"][0].is_string()) {
            return Option<bool>(400, "Parameter `ip_addresses` must be an array of strings.");
        }
        if(rule_json.count("api_keys") > 0 && !rule_json["api_keys"].is_array() && !rule_json["api_keys"][0].is_string()) {
            return Option<bool>(400, "Parameter `api_keys` must be an array of strings.");
        }
        if((rule_json.count("auto_ban_threshold_num") > 0 && rule_json.count("auto_ban_num_hours") == 0) || (rule_json.count("auto_ban_threshold_num") == 0 && rule_json.count("auto_ban_num_hours") > 0)) {
            return Option<bool>(400, "Both `auto_ban_threshold_num` and `auto_ban_num_hours` are required if either is specified.");

        }
        if(rule_json.count("auto_ban_threshold_num") > 0 && rule_json.count("auto_ban_num_hours") > 0) {
            if(!rule_json["auto_ban_threshold_num"].is_number_integer() || !rule_json["auto_ban_num_hours"].is_number_integer()) {
                return Option<bool>(400, "Parameters `auto_ban_threshold_num` and `auto_ban_num_hours` must be integers.");
            }
            if(rule_json["auto_ban_threshold_num"].get<int>() < 0 || rule_json["auto_ban_num_hours"].get<int>() < 0) {
                return Option<bool>(400, "Both `auto_ban_threshold_num` and `auto_ban_num_hours` must be greater than 0.");
            }
        }
    } else {
        return Option<bool>(400, "Invalid action.");
    }
    return Option<bool>(true);
}


rate_limit_rule_t RateLimitManager::parse_rule(const nlohmann::json &rule_json)
{
    rate_limit_rule_t new_rule;
    new_rule.action = magic_enum::enum_cast<RateLimitAction>(rule_json["action"].get<std::string>()).value();
    if(rule_json.count("ip_addresses") > 0) {
        for(const auto& ip_address: rule_json["ip_addresses"]) {
            new_rule.entities.push_back(rate_limit_entity_t{RateLimitedEntityType::ip, ip_address});
        }
    }
    if(rule_json.count("api_keys") > 0) {
        for(const auto& api_key: rule_json["api_keys"]) {
            new_rule.entities.push_back(rate_limit_entity_t{RateLimitedEntityType::api_key, api_key});
        }
    }
    if(rule_json.count("max_requests_1m") > 0) {
        new_rule.max_requests.minute_threshold = rule_json["max_requests_1m"];
    }
    if(rule_json.count("max_requests_1h") > 0) {
        new_rule.max_requests.hour_threshold = rule_json["max_requests_1h"];
    }
    if(rule_json.count("auto_ban_threshold_num") > 0 && rule_json.count("auto_ban_num_hours") > 0) {
        new_rule.auto_ban_threshold_num = rule_json["auto_ban_threshold_num"];
        new_rule.auto_ban_num_hours = rule_json["auto_ban_num_hours"];
    }
    if(rule_json.count("priority") > 0) {
        new_rule.priority = rule_json["priority"];
    }
    new_rule.is_and = rule_json.count("ip_addresses") > 0 && rule_json.count("api_keys") > 0 && (std::find_if(new_rule.entities.begin(), new_rule.entities.end(), [](const rate_limit_entity_t &entity) {
        return entity.entity_id == ".*";
    }) == new_rule.entities.end());
    return new_rule;
}


void RateLimitManager::insert_rule(rate_limit_rule_t &rule) {
    std::unique_lock lock(rate_limit_mutex);

    rule_store[rule.id] = rule;
    if(!rule.is_and) {
        // Check if the rule has both IP and API key entities
        bool has_both = std::find_if(rule.entities.begin(), rule.entities.end(), [](const rate_limit_entity_t &entity) {
            return entity.entity_type == RateLimitedEntityType::ip;
        }) != rule.entities.end() && std::find_if(rule.entities.begin(), rule.entities.end(), [](const rate_limit_entity_t &entity) {
            return entity.entity_type == RateLimitedEntityType::api_key;
        }) != rule.entities.end();
        if(has_both) {
            // Edge case: If the rule has any wildcard entity, we need to add it by an IP wildcard entity
            auto is_wildcard_ip = std::find_if(rule.entities.begin(), rule.entities.end(), [](const rate_limit_entity_t &entity) {
                return entity.entity_type == RateLimitedEntityType::ip && entity.entity_id == ".*";
            }) != rule.entities.end();

            auto is_wildcard_api_key = std::find_if(rule.entities.begin(), rule.entities.end(), [](const rate_limit_entity_t &entity) {
                return entity.entity_type == RateLimitedEntityType::api_key && entity.entity_id == ".*";
            }) != rule.entities.end();

            // Edge Case: If the rule has both wildcard entities, we count it as an IP wildcard entity
            if(is_wildcard_ip && is_wildcard_api_key) {
                rate_limit_entities.insert({rate_limit_entity_t{RateLimitedEntityType::ip, ".*"}, &rule_store.at(rule.id)});
            }
            // Edge Case: If the rule has an IP wildcard entity, but not an API key wildcard entity, we need to remove all IP entities from the rule
            else if(is_wildcard_ip) {
                for(const auto &entity : rule.entities) {
                    if(entity.entity_type == RateLimitedEntityType::api_key) {
                        rate_limit_entities.insert({entity, &rule_store.at(rule.id)});
                    }
                }
            }
            // Edge Case: If the rule has an API key wildcard entity, but not an IP wildcard entity, we need to remove all API key entities from the rule
            else if(is_wildcard_api_key) {
                for(const auto &entity : rule.entities) {
                    if(entity.entity_type == RateLimitedEntityType::ip) {
                        rate_limit_entities.insert({entity, &rule_store.at(rule.id)});
                    }
                }
            }
        }
        else {
            if(std::find_if(rule.entities.begin(), rule.entities.end(), [](const rate_limit_entity_t &entity) {
                return entity.entity_type == RateLimitedEntityType::ip;
            }) == rule.entities.end()) {
                rule_store[rule.id].is_and = true;
            }
            for(const auto &entity : rule.entities) {
                rate_limit_entities.insert({entity, &rule_store.at(rule.id)});
            }
        }
    } else {
        std::vector<rate_limit_entity_t> ip_entities, api_key_entities;
        for(const auto &entity : rule.entities) {
            if(entity.entity_type == RateLimitedEntityType::ip) {
                ip_entities.push_back(entity);
            } else {
                api_key_entities.push_back(entity);
            }
        }
        // Add all combinations of ip and api key entities to AND store
        for(const auto &ip_entity : ip_entities) {
            for(const auto &api_key_entity : api_key_entities) {
                rate_limit_entities_and.insert({std::make_pair(api_key_entity, ip_entity), &rule_store.at(rule.id)});
            }
        }
    }
}


Option<bool> RateLimitManager::init(Store* store) {
    std::shared_lock lock(rate_limit_mutex);

    if (store == nullptr) {
        return Option<bool>(500, "Store is null");
    }

    // Set store
    this->store = store;
    // Load rules from database
    std::string last_rule_id_str;
    StoreStatus last_rule_id_status = store->get(std::string(RULES_NEXT_ID), last_rule_id_str);
    if(last_rule_id_status == StoreStatus::ERROR) {
        return Option<bool>(500, "Error while fetching rule next id from database.");
    }
    else if(last_rule_id_status == StoreStatus::FOUND) {
        last_rule_id = StringUtils::deserialize_uint32_t(last_rule_id_str);
    }
    else {
        last_rule_id = 0;
    }

    std::vector<std::string> rule_json_strs;
    store->scan_fill(std::string(RULES_PREFIX) + "_", std::string(RULES_PREFIX) + "`", rule_json_strs);

    for(const auto& rule_json_str: rule_json_strs) {
        nlohmann::json rule_json = nlohmann::json::parse(rule_json_str);
        for(const auto& ip: rule_json["ip_addresses"]) {
        // Check if a rule exists for the entity
        if (rate_limit_entities.count(rate_limit_entity_t{RateLimitedEntityType::ip, ip}) > 0) {
            return Option<bool>(400, "A rule already exists for one of the entities");
        }
        }
        for(const auto& api_key: rule_json["api_keys"]) {
            // Check if a rule exists for the entity
            if (rate_limit_entities.count(rate_limit_entity_t{RateLimitedEntityType::api_key, api_key}) > 0) {
                return Option<bool>(400, "A rule already exists for one of the entities");
            }
        }

        auto rule = parse_rule(rule_json);
        rule.id = rule_json["id"];

        // unlock mutex before inserting rule
        lock.unlock();
        insert_rule(rule);
        lock.lock();
    }
    // Load bans from database
    std::string last_ban_id_str;
    StoreStatus last_ban_id_status = store->get(BANS_NEXT_ID, last_ban_id_str);
    if(last_ban_id_status == StoreStatus::ERROR) {
        LOG(INFO) << "Error while fetching ban next id from database.";
        return Option<bool>(500, "Error while fetching ban next id from database.");
    }
    else if(last_ban_id_status == StoreStatus::FOUND) {
        last_ban_id = StringUtils::deserialize_uint32_t(last_ban_id_str);
    }
    else {
        LOG(INFO) << "No bans found in database.";
        last_ban_id = 0;
    }

    std::vector<std::string> ban_json_strs;
    store->scan_fill(std::string(BANS_PREFIX) + "_", std::string(BANS_PREFIX) + "`", ban_json_strs);

    for(const auto& ban_json_str: ban_json_strs) {
        nlohmann::json ban_json = nlohmann::json::parse(ban_json_str);
        rate_limit_status_t ban_status;
        ban_status.parse_json(ban_json);
        if(ban_status.is_and) {
            throttled_entities[ban_status.entity][ban_status.and_entity] = ban_status;
        } else {
            throttled_entities[ban_status.entity][rate_limit_entity_t{RateLimitedEntityType::api_key, ".*"}] = ban_status;
        }
    }
    LOG(INFO) << "Loaded " << rule_store.size() << " rate limit rules.";
    LOG(INFO) << "Loaded " << throttled_entities.size() << " rate limit bans.";
    return Option<bool>(true);
}

std::string RateLimitManager::get_rule_key(const uint32_t id) {
    return std::string(RULES_PREFIX) + "_" + std::to_string(id);
}

std::string RateLimitManager::get_ban_key(const uint32_t id) {
    return std::string(BANS_PREFIX) + "_" + std::to_string(id);
}

time_t RateLimitManager::get_current_time() {
    return  base_timestamp + std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());;
}

void RateLimitManager::_set_base_timestamp(const time_t& timestamp) {
    base_timestamp = timestamp;
}


const nlohmann::json RateLimitManager::get_all_throttled_entities_json() {
    std::shared_lock lock(rate_limit_mutex);

    nlohmann::json throttled_entities_array = nlohmann::json::object();
    for(const auto& entity_map : throttled_entities) {
        for(const auto& entity : entity_map.second) {
            if(entity.second.throttling_to <= get_current_time()) {
                store->remove(get_ban_key(entity.second.status_id));
                throttled_entities.erase(entity.first);
                if(throttled_entities[entity.first].empty()) {
                    throttled_entities.erase(entity.first);
                }
            }
            auto entity_json = entity.second.to_json();

            entity_json["ip_address"] = entity_json["value"];

            if(entity.second.is_and) {
                entity_json["api_key"] = entity.second.and_entity.entity_id;
            }

            entity_json.erase("value");
            entity_json.erase("entity_type");
            entity_json.erase("and_entity");
            throttled_entities_array["active"].push_back(entity_json);
        }
    }
    return throttled_entities_array;
}

const Option<nlohmann::json> RateLimitManager::delete_throttle_by_id(const uint64_t id) {
    std::unique_lock lock(rate_limit_mutex);
    std::string ban_json_str;

    const auto found = store->get(get_ban_key(id), ban_json_str);

    if(found == StoreStatus::NOT_FOUND) {
        return Option<nlohmann::json>(400, "Could not find any ban with the given ID");
    }

    nlohmann::json ban_json = nlohmann::json::parse(ban_json_str);
    rate_limit_status_t ban_status;
    ban_status.parse_json(ban_json);
    if(ban_status.is_and) {
        throttled_entities[ban_status.entity].erase(ban_status.and_entity);
    } else {
        throttled_entities[ban_status.entity].erase(rate_limit_entity_t{RateLimitedEntityType::api_key, ".*"});
    }
    if(throttled_entities[ban_status.entity].empty()) {
        throttled_entities.erase(ban_status.entity);
    }
    const auto removed = store->remove(get_ban_key(id));

    if(!removed) {
        return Option<nlohmann::json>(400, "Error while removing the ban from the store");
    }

    nlohmann::json res;
    res["id"] = id;
    return Option<nlohmann::json>(res);
}