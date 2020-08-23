#include "auth_manager.h"
#include <openssl/evp.h>

constexpr const char* AuthManager::DOCUMENTS_SEARCH_ACTION;

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

bool AuthManager::authenticate(const std::string& req_api_key, const std::string& action,
                               const std::string& collection, std::map<std::string, std::string>& params) {

    if(req_api_key.size() > KEY_LEN) {
        // scoped API key: validate and if valid, extract params
        Option<std::string> params_op = params_from_scoped_key(req_api_key, action, collection);
        if(!params_op.ok()) {
            // authentication failed
            return false;
        }

        const std::string& embedded_params_str = params_op.get();
        nlohmann::json embedded_params;

        try {
            embedded_params = nlohmann::json::parse(embedded_params_str);
        } catch(const std::exception& e) {
            LOG(ERROR) << "JSON error: " << e.what();
            return false;
        }

        if(!embedded_params.is_object()) {
            LOG(ERROR) << "Scoped API key contains invalid search parameters.";
            return false;
        }

        // enrich params with values from embedded_params
        for (const auto& it: embedded_params.items()){
            if(params.count(it.key()) == 0) {
                params[it.key()] = it.value();
            } else if(it.key() == "filter_by") {
                params[it.key()] = params[it.key()] + "&&" + it.value().get<std::string>();
            } else {
                params[it.key()] = it.value();
            }
        }

        return true;
    }

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

Option<std::string> AuthManager::params_from_scoped_key(const std::string &scoped_api_key, const std::string& action,
                                                        const std::string& collection) {
    // allow only searches from scoped keys
    if(action != DOCUMENTS_SEARCH_ACTION) {
        LOG(ERROR) << "Scoped API keys can only be used for searches.";
        return Option<std::string>(403, "Forbidden.");
    }

    const std::string& key_payload = StringUtils::base64_decode(scoped_api_key);

    // FORMAT:
    // <DIGEST><PARENT_KEY_PREFIX><PARAMS>
    const std::string& hmacSHA256 = key_payload.substr(0, HMAC_BASE64_LEN);
    const std::string& key_prefix = key_payload.substr(HMAC_BASE64_LEN, api_key_t::PREFIX_LEN);
    const std::string& custom_params = key_payload.substr(HMAC_BASE64_LEN + api_key_t::PREFIX_LEN);

    // calculate and verify hmac against matching api key
    for (const auto &kv : api_keys) {
        if(kv.first.substr(0, api_key_t::PREFIX_LEN) == key_prefix) {
            const api_key_t& api_key = kv.second;

            // ensure that parent key has only search scope
            if(api_key.actions.size() != 1 || api_key.actions[0] != DOCUMENTS_SEARCH_ACTION) {
                LOG(ERROR) << "Parent API key must allow only `" << DOCUMENTS_SEARCH_ACTION << "` action.";
                return Option<std::string>(403, "Forbidden.");
            }

            // ensure that parent key collection filter matches queried collection
            bool collection_allowed = false;
            for(const std::string& allowed_collection: api_key.collections) {
                if(allowed_collection == "*" || (collection != "*" && allowed_collection == collection)) {
                    collection_allowed = true;
                    break;
                }
            }

            if(!collection_allowed) {
                LOG(ERROR) << "Parent key does not allow queries against queried collection.";
                return Option<std::string>(403, "Forbidden.");
            }

            // finally verify hmac
            std::string digest = StringUtils::hmac(kv.first, custom_params);

            if(digest == hmacSHA256) {
                return Option<std::string>(custom_params);
            }
        }
    }

    return Option<std::string>(403, "Forbidden.");
}
