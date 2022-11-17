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
            if (throttled_entities.at(entity).throttling_to > get_current_time()) {
                return true;
            }
            // Remove ban from DB store
            std::string ban_key = std::string(BANS_PREFIX) + "_" + std::to_string(throttled_entities.at(entity).status_id);
            store->remove(ban_key);
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
            if(rule.max_requests.minute_threshold >= 0 && current_rate_for_minute >= rule.max_requests.minute_threshold) {
                bool auto_ban_is_enabled = (rule.auto_ban_threshold_num > 0 && rule.auto_ban_num_days > 0);
                if(auto_ban_is_enabled) {
                    request_counts.threshold_exceed_count_minute++;
                    if(request_counts.threshold_exceed_count_minute > rule.auto_ban_threshold_num) {
                        temp_ban_entity_wrapped(entity, rule.auto_ban_num_days);
                    }
                } 
                return true;
            }
            auto current_rate_for_hour = (3600 - (get_current_time() - request_counts.last_reset_time_hour)) / 3600  * request_counts.previous_requests_count_hour;
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

Option<nlohmann::json> RateLimitManager::find_rule_by_id(const uint64_t id) {
    std::shared_lock<std::shared_mutex> lock(rate_limit_mutex);
    if(rule_store.count(id) > 0) {
        return Option<nlohmann::json>(rule_store.at(id).to_json());
    }
    return Option<nlohmann::json>(404, "Not Found");
}

bool RateLimitManager::delete_rule_by_id(const uint64_t id) {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);
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
        for(const auto &entity : rule.entity_ids) {
            rate_limit_entities.erase(rate_limit_entity_t{rule.entity_type,entity});
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
                banned_entities.push_back(rate_limit_status_t{last_ban_id, 0, 0, entity, entity_type});
                last_ban_id++;
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
    last_ban_id = 0;
    base_timestamp = 0;
}

void RateLimitManager::temp_ban_entity_wrapped(const rate_limit_entity_t& entity, const int64_t number_of_days) {
    // Check if entity is already banned
    if (throttled_entities.count(entity) > 0) {
        return;
    }
    auto now = get_current_time();
    // Add entity to throttled_entities for the given number of days
    rate_limit_status_t status{last_ban_id, now, now + (number_of_days * 24 * 60 * 60), entity.entity_id, entity.entity_type};
    std::string ban_key = get_ban_key(last_ban_id);
    store->insert(ban_key, status.to_json().dump());
    throttled_entities.insert({entity, rate_limit_status_t{last_ban_id, now, now + (number_of_days * 24 * 60 * 60), entity.entity_id, entity.entity_type}});
    last_ban_id++;
    if(rate_limit_request_counts.contains(entity)){
        // Reset counters for the given entity
        rate_limit_request_counts.lookup(entity).current_requests_count_minute = 0;
        rate_limit_request_counts.lookup(entity).current_requests_count_hour = 0;
    }
}

const nlohmann::json RateLimitManager::get_all_rules_json() {
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
        if(entity_type == RateLimitedEntityType::ip) {
            rule["ip_addresses"] = entity_ids;
        } else {
            rule["api_keys"] = entity_ids;
        }
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

const nlohmann::json rate_limit_status_t::to_json() const {
    nlohmann::json status;
    status["throttling_from"] = throttling_from;
    status["throttling_to"] = throttling_to;
    status["value"] = value;
    status["entity_type"] = magic_enum::enum_name(entity_type);
    return status;
}

void rate_limit_status_t::parse_json(const nlohmann::json &json) {
    throttling_from = json["throttling_from"];
    throttling_to = json["throttling_to"];
    value = json["value"];
    entity_type = magic_enum::enum_cast<RateLimitedEntityType>(json["entity_type"].get<std::string>()).value();
}


Option<nlohmann::json> RateLimitManager::add_rule(const nlohmann::json &rule_json) {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);
    auto rule_validation_result = is_valid_rule(rule_json);
    if(!rule_validation_result.ok()) {
        return Option<nlohmann::json>(rule_validation_result.code(), rule_validation_result.error());
    }
    auto parsed_rule_option = parse_rule(rule_json);
    if(!parsed_rule_option.ok()) {
        return Option<nlohmann::json>(parsed_rule_option.code(), parsed_rule_option.error());
    }
    rate_limit_rule_t parsed_rule = parsed_rule_option.get();
    parsed_rule.id = last_rule_id++;
    const std::string rule_store_key = get_rule_key(parsed_rule.id);
    bool inserted = store->insert(rule_store_key, parsed_rule.to_json().dump());
    if(!inserted) {
        return Option<nlohmann::json>(500, "Failed to insert rule into the DB store");
    }
    store->increment(std::string(RULES_NEXT_ID), 1);
    // Insert rule to rule store
    insert_rule(parsed_rule);
    nlohmann::json response;
    response["message"] = "Rule added successfully.";
    response["rule"] = parsed_rule.to_json();
    return Option<nlohmann::json>(response);
}

Option<nlohmann::json> RateLimitManager::edit_rule(const uint64_t id, const nlohmann::json &rule_json) {
    const auto& rule_option = find_rule_by_id(id);
    if(!rule_option.ok()) {
        return Option<nlohmann::json>(rule_option.code(), rule_option.error());
    }
    auto rule_validation_result = is_valid_rule(rule_json);
    if(!rule_validation_result.ok()) {
        return Option<nlohmann::json>(rule_validation_result.code(), rule_validation_result.error());
    }
    auto parsed_rule_option = parse_rule(rule_json, false);
    if(!parsed_rule_option.ok()) {
        return Option<nlohmann::json>(parsed_rule_option.code(), parsed_rule_option.error());
    }
    rate_limit_rule_t parsed_rule = parsed_rule_option.get();
    parsed_rule.id = id;
    const std::string rule_store_key = get_rule_key(parsed_rule.id);
    bool inserted = store->insert(rule_store_key, parsed_rule.to_json().dump());
    if(!inserted) {
        return Option<nlohmann::json>(500, "Failed to update rule in the DB store");
    }
    auto old_rule = rule_store.at(id);
    // Remove rule from rate limit rule pointer
    for(const auto &entity : old_rule.entity_ids) {
        rate_limit_entities.erase(rate_limit_entity_t{old_rule.entity_type,entity});
    }
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
        if ((rule_json.count("ip_addresses") == 0 && rule_json.count("api_keys") == 0) || (rule_json.count("ip_addresses") > 0 && rule_json.count("api_keys") > 0)) {
            return Option<bool>(400, "Either `ip_addresses` or `api_keys` is required.");
        }
        return Option<bool>(true);
    } else if (rule_json["action"] == "block") {
        if ((rule_json.count("ip_addresses") == 0 && rule_json.count("api_keys") == 0) || (rule_json.count("ip_addresses") > 0 && rule_json.count("api_keys") > 0)) {
            return Option<bool>(400, "Either `ip_addresses` or `api_keys` is required.");
        }
        return Option<bool>(true);
    } else if (rule_json["action"] == "throttle") {
        if (rule_json.count("max_requests_60s") == 0 && rule_json.count("max_requests_1h") == 0) {
            return Option<bool>(400, "At least  one of `max_requests_60s` or `max_requests_1h` is required.");
        }
        if (rule_json.count("max_requests_60s") > 0 && rule_json["max_requests_60s"].is_number_integer() == false) {
            return Option<bool>(400, "Parameter `max_requests_60s` must be an integer.");
        }
        if (rule_json.count("max_requests_1h") > 0 && rule_json["max_requests_1h"].is_number_integer() == false) {
            return Option<bool>(400, "Parameter `max_requests_1h` must be an integer.");
        }
        if ((rule_json.count("ip_addresses") == 0 && rule_json.count("api_keys") == 0) || (rule_json.count("ip_addresses") > 0 && rule_json.count("api_keys") > 0)) {
            return Option<bool>(400, "Either `ip_addresses` or `api_keys` is required.");
        }
        if(rule_json.count("ip_addresses") > 0 && !rule_json["ip_addresses"].is_array() && !rule_json["ip_addresses"][0].is_string()) {
            return Option<bool>(400, "Parameter `ip_addresses` must be an array of strings.");
        }
        if(rule_json.count("api_keys") > 0 && !rule_json["api_keys"].is_array() && !rule_json["api_keys"][0].is_string()) {
            return Option<bool>(400, "Parameter `api_keys` must be an array of strings.");
        }
        if((rule_json.count("auto_ban_threshold_num") > 0 && rule_json.count("auto_ban_num_days") == 0) || (rule_json.count("auto_ban_threshold_num") == 0 && rule_json.count("auto_ban_num_days") > 0)) {
            return Option<bool>(400, "Both `auto_ban_threshold_num` and `auto_ban_num_days` are required if either is specified.");

        }
        if(rule_json.count("auto_ban_threshold_num") > 0 && rule_json.count("auto_ban_num_days") > 0) {
            if(!rule_json["auto_ban_threshold_num"].is_number_integer() || !rule_json["auto_ban_num_days"].is_number_integer()) {
                return Option<bool>(400, "Parameters `auto_ban_threshold_num` and `auto_ban_num_days` must be integers.");
            }
            if(rule_json["auto_ban_threshold_num"].get<int>() < 0 || rule_json["auto_ban_num_days"].get<int>() < 0) {
                return Option<bool>(400, "Both `auto_ban_threshold_num` and `auto_ban_num_days` must be greater than 0.");
            }
        }
    } else {
        return Option<bool>(400, "Invalid action.");
    }
    return Option<bool>(true);
}


Option<rate_limit_rule_t> RateLimitManager::parse_rule(const nlohmann::json &rule_json, bool alert_if_exists)
{
    rate_limit_rule_t new_rule;
    new_rule.action = magic_enum::enum_cast<RateLimitAction>(rule_json["action"].get<std::string>()).value();
    if(rule_json.count("ip_addresses") > 0) {
        new_rule.entity_type = RateLimitedEntityType::ip;
        for(const auto& ip: rule_json["ip_addresses"]) {
            // Check if a rule exists for the entity
            if (rate_limit_entities.count(rate_limit_entity_t{new_rule.entity_type, ip}) > 0 && alert_if_exists) {
                return Option<rate_limit_rule_t>(400, "A rule already exists for one of the entities");
            }
            new_rule.entity_ids.push_back(ip);
        }
    }
    if(rule_json.count("api_keys") > 0) {
        new_rule.entity_type = RateLimitedEntityType::api_key;
        for(const auto& api_key: rule_json["api_keys"]) {
            // Check if a rule exists for the entity
            if (rate_limit_entities.count(rate_limit_entity_t{new_rule.entity_type, api_key}) > 0 && alert_if_exists) {
                return Option<rate_limit_rule_t>(400, "A rule already exists for one of the entities");
            }
            new_rule.entity_ids.push_back(api_key);
        }
    }
    if(rule_json.count("max_requests_60s") > 0) {
        new_rule.max_requests.minute_threshold = rule_json["max_requests_60s"];
    }
    if(rule_json.count("max_requests_1h") > 0) {
        new_rule.max_requests.hour_threshold = rule_json["max_requests_1h"];
    }
    if(rule_json.count("auto_ban_threshold_num") > 0 && rule_json.count("auto_ban_num_days") > 0) {
        new_rule.auto_ban_threshold_num = rule_json["auto_ban_threshold_num"];
        new_rule.auto_ban_num_days = rule_json["auto_ban_num_days"];
    }
    return Option<rate_limit_rule_t>(new_rule);
}


void RateLimitManager::insert_rule(const rate_limit_rule_t &rule) {
    rule_store[rule.id] = rule;
    for(const auto &entity : rule.entity_ids) {
        rate_limit_entities.insert({rate_limit_entity_t{rule.entity_type,entity}, &rule_store.at(rule.id)});
    }
}


Option<bool> RateLimitManager::init(Store *store) {
    std::unique_lock<std::shared_mutex> lock(rate_limit_mutex);
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
        Option<rate_limit_rule_t> rule_option = parse_rule(rule_json);
        if(!rule_option.ok()) {
            return Option<bool>(rule_option.code(), rule_option.error());
        }
        auto rule = rule_option.get();
        rule.id = rule_json["id"];
        insert_rule(rule);
    }
    // Load bans from database
    std::string last_ban_id_str;
    StoreStatus last_ban_id_status = store->get(BANS_NEXT_ID, last_ban_id_str);
    if(last_ban_id_status == StoreStatus::ERROR) {
        return Option<bool>(500, "Error while fetching ban next id from database.");
    }
    else if(last_ban_id_status == StoreStatus::FOUND) {
        last_ban_id = StringUtils::deserialize_uint32_t(last_ban_id_str);
    }
    else {
        last_ban_id = 0;
    }

    std::vector<std::string> ban_json_strs;
    store->scan_fill(std::string(BANS_PREFIX) + "_", std::string(BANS_PREFIX) + "`", ban_json_strs);

    for(const auto& ban_json_str: ban_json_strs) {
        nlohmann::json ban_json = nlohmann::json::parse(ban_json_str);
        rate_limit_status_t ban_status;
        ban_status.parse_json(ban_json);
        throttled_entities.insert({rate_limit_entity_t{ban_status.entity_type, ban_status.value}, ban_status});
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