#include "auth_manager.h"

Option<std::vector<api_key_t>> AuthManager::list_keys() {
    std::vector<std::string> api_key_json_strs;
    store->scan_fill(API_KEYS_PREFIX, api_key_json_strs);

    std::vector<api_key_t> stored_api_keys;

    for(const auto& api_key_json_str: api_key_json_strs) {
        api_key_t api_key;
        Option<bool> load_op = api_key.load(api_key_json_str);
        if(!load_op.ok()) {
            return Option<std::vector<api_key_t>>(load_op.code(), load_op.error());
        }

        stored_api_keys.push_back(api_key.truncate_value());
    }

    return Option<std::vector<api_key_t>>(stored_api_keys);
}

Option<api_key_t> AuthManager::get_key(uint32_t id, bool truncate_value) {
    std::string api_key_store_key = std::string(API_KEYS_PREFIX) + "_" + std::to_string(id);
    std::string api_key_json_str;
    StoreStatus status = store->get(api_key_store_key, api_key_json_str);

    if(status == StoreStatus::FOUND) {
        api_key_t api_key;
        const Option<bool> & load_op = api_key.load(api_key_json_str);
        if(!load_op.ok()) {
            return Option<api_key_t>(load_op.code(), load_op.error());
        }

        if(truncate_value) {
            api_key.truncate_value();
        }
        return Option<api_key_t>(api_key);
    }

    if(status == StoreStatus::NOT_FOUND) {
        return Option<api_key_t>(404, "Not found.");
    }

    return Option<api_key_t>(500, "Error while fetching key from store.");
}

Option<api_key_t> AuthManager::create_key(api_key_t& api_key) {
    if(api_keys.count(api_key.value) != 0) {
        return Option<api_key_t>(409, "API key generation conflict.");
    }

    api_key.id = get_next_api_key_id();

    std::string api_key_store_key = std::string(API_KEYS_PREFIX) + "_" + std::to_string(api_key.id);
    const nlohmann::json & api_key_obj = api_key.to_json();

    bool inserted = store->insert(api_key_store_key, api_key_obj.dump());
    if(!inserted) {
        return Option<api_key_t>(500, "Could not store generated API key.");
    }

    api_keys.emplace(api_key.value, api_key);
    return Option<api_key_t>(api_key);
}

Option<api_key_t> AuthManager::remove_key(uint32_t id) {
    Option<api_key_t> key_op = get_key(id, false);
    
    if(!key_op.ok()) {
        return Option<api_key_t>(500, key_op.error());
    }

    std::string api_key_store_key = std::string(API_KEYS_PREFIX) + "_" + std::to_string(id);
    if(!store->remove(api_key_store_key)) {
        return Option<api_key_t>(500, "Could not delete API key.");
    }

    api_key_t&& key = key_op.get();
    api_keys.erase(key.value);

    return Option<api_key_t>(key.truncate_value());
}

uint32_t AuthManager::get_next_api_key_id() {
    store->increment(std::string(API_KEY_NEXT_ID_KEY), 1);
    return next_api_key_id++;
}

Option<bool> AuthManager::init(Store *store) {
    this->store = store;

    std::string next_api_key_id_str;
    StoreStatus next_api_key_id_status = store->get(API_KEY_NEXT_ID_KEY, next_api_key_id_str);

    if(next_api_key_id_status == StoreStatus::ERROR) {
        return Option<bool>(500, "Error while fetching the next API key id from the store.");
    }

    if(next_api_key_id_status == StoreStatus::FOUND) {
        next_api_key_id = (uint32_t) StringUtils::deserialize_uint32_t(next_api_key_id_str);
    } else {
        next_api_key_id = 0;
    }

    std::vector<std::string> api_key_json_strs;
    store->scan_fill(API_KEYS_PREFIX, api_key_json_strs);

    for(auto & api_key_json_str: api_key_json_strs) {
        api_key_t api_key;
        Option<bool> load_op = api_key.load(api_key_json_str);
        if(!load_op.ok()) {
            return Option<bool>(load_op.code(), load_op.error());
        }

        api_keys.emplace(api_key.value, api_key);
    }

    return Option<bool>(true);
}

bool AuthManager::authenticate(const std::string& req_api_key, const std::string& action,
                               const std::string& collection) {
    if(api_keys.count(req_api_key) == 0) {
        return false;
    }

    const api_key_t& api_key = api_keys[req_api_key];

    // check if action is allowed

    bool action_is_allowed = false;
    for(const std::string& allowed_action: api_key.actions) {
        if(allowed_action == "*" || (action != "*" && allowed_action == action)) {
            action_is_allowed = true;
            break;
        }
    }

    if(!action_is_allowed) {
        return false;
    }

    // check if action is allowed against a specific collection

    for(const std::string& allowed_collection: api_key.collections) {
        if(allowed_collection == "*" || (collection != "*" && allowed_collection == collection) || collection.empty()) {
            return true;
        }
    }

    return false;
}

