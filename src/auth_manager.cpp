#include "auth_manager.h"
#include <openssl/evp.h>
#include <regex>
#include <join.h>
#include "collection_manager.h"

constexpr const char* AuthManager::DOCUMENTS_SEARCH_ACTION;
constexpr const uint64_t api_key_t::FAR_FUTURE_TIMESTAMP;

std::vector<std::string> parse_requested_search_set_value(const nlohmann::json& value) {
    if(value.is_string()) {
        std::vector<std::string> parsed_values;
        StringUtils::split(value.get<std::string>(), parsed_values, ",");
        return parsed_values;
    }

    if(!value.is_array()) {
        return {};
    }

    std::vector<std::string> parsed_values;
    for(const auto& item: value) {
        if(item.is_string() && !item.get<std::string>().empty()) {
            parsed_values.push_back(item.get<std::string>());
        }
    }

    return parsed_values;
}

std::vector<std::string> resolve_requested_search_sets(const std::map<std::string, std::string>& params,
                                                                    const nlohmann::json& auth_context,
                                                                    const char* request_param,
                                                                    const char* requested_auth_param,
                                                                    const char* preset_auth_param) {
    const auto params_it = params.find(request_param);
    if(params_it != params.end()) {
        std::vector<std::string> parsed_values;
        StringUtils::split(params_it->second, parsed_values, ",");
        return parsed_values;
    }

    const auto auth_context_it = auth_context.find(requested_auth_param);
    if(auth_context_it != auth_context.end()) {
        return parse_requested_search_set_value(*auth_context_it);
    }

    const auto embedded_param_it = auth_context.find(request_param);
    if(embedded_param_it != auth_context.end()) {
        return parse_requested_search_set_value(*embedded_param_it);
    }

    const auto preset_auth_context_it = auth_context.find(preset_auth_param);
    if(preset_auth_context_it != auth_context.end()) {
        return parse_requested_search_set_value(*preset_auth_context_it);
    }

    return {};
}

std::vector<std::string> merge_requested_sets(std::vector<std::string> effective_sets,
                                                           const std::vector<std::string>& requested_sets) {
    std::unordered_set<std::string> effective_set_set(effective_sets.begin(), effective_sets.end());
    for(const auto& requested_set: requested_sets) {
        if(effective_set_set.find(requested_set) == effective_set_set.end()) {
            effective_sets.push_back(requested_set);
            effective_set_set.insert(requested_set);
        }
    }

    return effective_sets;
}

bool has_non_wildcard_constraint(const std::vector<std::string>& allowed_sets) {
    return !allowed_sets.empty() &&
           std::find(allowed_sets.begin(), allowed_sets.end(), "*") == allowed_sets.end();
}

nlohmann::json merge_auth_context(const nlohmann::json& auth_context,
                                               const nlohmann::json& embedded_params) {
    nlohmann::json merged_auth_context = auth_context;
    for(const auto& item: embedded_params.items()) {
        if(merged_auth_context.count(item.key()) == 0) {
            merged_auth_context[item.key()] = item.value();
        }
    }

    return merged_auth_context;
}

Option<bool> AuthManager::init(Store* store, const std::string& bootstrap_auth_key) {
    // This function must be idempotent, i.e. when called multiple times, must produce the same state without leaks
    //LOG(INFO) << "AuthManager::init()";
    std::unique_lock lock(mutex);

    this->store = store;
    this->bootstrap_auth_key = bootstrap_auth_key;

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
    store->scan_fill(std::string(API_KEYS_PREFIX) + "_",
                     std::string(API_KEYS_PREFIX) + "`",
                     api_key_json_strs);

    LOG(INFO) << "Indexing " << api_key_json_strs.size() << " API key(s) found on disk.";

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

Option<std::vector<api_key_t>> AuthManager::list_keys() const {
    std::shared_lock lock(mutex);

    std::vector<std::string> api_key_json_strs;
    store->scan_fill(std::string(API_KEYS_PREFIX) + "_",
                     std::string(API_KEYS_PREFIX) + "`", api_key_json_strs);

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

Option<api_key_t> AuthManager::get_key(uint32_t id, bool truncate_value) const {
    std::shared_lock lock(mutex);

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
    //LOG(INFO) << "AuthManager::create_key()";
    std::unique_lock lock(mutex);

    if(api_keys.count(api_key.value) != 0 || api_key.value == bootstrap_auth_key) {
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
        return Option<api_key_t>(key_op.code(), key_op.error());
    }

    std::string api_key_store_key = std::string(API_KEYS_PREFIX) + "_" + std::to_string(id);
    if(!store->remove(api_key_store_key)) {
        return Option<api_key_t>(500, "Could not delete API key.");
    }

    std::unique_lock lock(mutex);

    api_key_t&& key = key_op.get();
    api_keys.erase(key.value);

    return Option<api_key_t>(key.truncate_value());
}

Option<api_key_t> AuthManager::update_key(uint32_t id, api_key_t&& api_key) {
    Option<api_key_t> existing_key_op = get_key(id, false);
    
    if(!existing_key_op.ok()) {
        return Option<api_key_t>(existing_key_op.code(), existing_key_op.error());
    }

    api_key_t existing_key = existing_key_op.get();

    if(existing_key.value != api_key.value) {
        return Option<api_key_t>(400, "API key value cannot be updated.");
    }

    std::string api_key_store_key = std::string(API_KEYS_PREFIX) + "_" + std::to_string(id);
    const nlohmann::json & api_key_obj = api_key.to_json();

    bool updated = store->insert(api_key_store_key, api_key_obj.dump());
    if(!updated) {
        return Option<api_key_t>(500, "Could not update API key.");
    }

    std::unique_lock lock(mutex);

    api_keys[api_key.value] = api_key;

    return Option<api_key_t>(api_key.truncate_value());
}

uint32_t AuthManager::get_next_api_key_id() {
    store->increment(std::string(API_KEY_NEXT_ID_KEY), 1);
    return next_api_key_id++;
}

bool AuthManager::authenticate(const std::string& action,
                               const std::vector<collection_key_t>& collection_keys,
                               std::map<std::string, std::string>& params,
                               std::vector<nlohmann::json>& embedded_params_vec) const {

    std::shared_lock lock(mutex);
    //LOG(INFO) << "AuthManager::authenticate()";

    size_t num_keys_matched = 0;
    for(size_t i = 0; i < collection_keys.size(); i++) {
        const auto& coll_key = collection_keys[i];
        if(coll_key.api_key.empty()) {
            return false;
        }

        if(coll_key.api_key == bootstrap_auth_key) {
            return true;
        }

        const auto& key_it = api_keys.find(coll_key.api_key);
        const auto& auth_context = embedded_params_vec[i];
        nlohmann::json embedded_params;

        if(key_it != api_keys.end()) {
            const api_key_t& api_key = key_it.value();
            if(!auth_against_key(coll_key.collection, action, api_key, false, params, auth_context)) {
                return false;
            }
        } else {
            // could be a scoped API key
            Option<bool> auth_op = authenticate_parse_params(coll_key, action, params, auth_context, embedded_params);
            if(!auth_op.ok()) {
                return false;
            }
        }

        num_keys_matched++;

        // lengths of embedded_params_vec and collection_keys are guaranteed by upstream to be the same
        embedded_params_vec[i] = embedded_params;
    }

    //LOG(INFO) << "api_keys.size() = " << api_keys.size();
    return (num_keys_matched == collection_keys.size());
}

Option<std::string> resolve_scoped_search_collection(const std::string& request_collection,
                                                                  const nlohmann::json& embedded_params) {
    const auto collection_it = embedded_params.find("collection");
    if(collection_it == embedded_params.end()) {
        return Option<std::string>(request_collection);
    }

    if(!collection_it->is_string()) {
        return Option<std::string>(400, "`collection` inside Scoped Search API key must be a string.");
    }

    const auto embedded_collection = collection_it->get<std::string>();
    if(embedded_collection.empty()) {
        return Option<std::string>(request_collection);
    }

    if(request_collection.empty()) {
        return Option<std::string>(embedded_collection);
    }

    if(request_collection != embedded_collection) {
        return Option<std::string>(400, "`collection` inside Scoped Search API key conflicts with the request collection.");
    }

    return Option<std::string>(request_collection);
}

bool AuthManager::sets_allowed(const std::vector<std::string>& allowed_set_patterns,
                                          const std::vector<std::string>& collection_sets) {
    return std::all_of(collection_sets.begin(), collection_sets.end(),
                       [&](const std::string& collection_set) {
                           return std::any_of(allowed_set_patterns.begin(), allowed_set_patterns.end(),
                                              [&](const std::string& allowed_set_pattern) {
                                                  return allowed_set_pattern == collection_set ||
                                                         regexp_match(collection_set, allowed_set_pattern);
                                              });
                       });
}

bool AuthManager::regexp_match(const std::string& value, const std::string& regexp) {
    try {
        return std::regex_match (value, std::regex(regexp));
    } catch(const std::exception& e) {
        LOG(ERROR) << "Error while matching regexp " << regexp << " against value " << value;
        return false;
    }
}

bool AuthManager::auth_against_key(const std::string& req_collection, const std::string& action,
                                   const api_key_t& api_key, const bool search_only,
                                   const std::map<std::string, std::string>& params,
                                   const nlohmann::json& auth_context) const {

    if(uint64_t(std::time(0)) > api_key.expires_at) {
        LOG(ERROR) << fmt_error("Rejecting expired API key.", api_key.value);
        return false;
    }

    if(search_only) {
        // ensure that parent key has only search scope
        if(api_key.actions.size() != 1 || api_key.actions[0] != DOCUMENTS_SEARCH_ACTION) {
            LOG(ERROR) << fmt_error(std::string("Parent API key must allow only `") + DOCUMENTS_SEARCH_ACTION + "` action.",
                                    api_key.value);
            return false;
        }
    } else {
        bool action_is_allowed = false;
        for(const std::string& allowed_action: api_key.actions) {
            if(allowed_action == "*" || (action != "*" && allowed_action == action)) {
                action_is_allowed = true;
                break;
            }

            // e.g. collections:create or documents:create
            if (allowed_action.size() >= 2 && allowed_action[allowed_action.size() - 2] == ':' &&
                allowed_action.back() == '*') {
                std::string allowed_resource = allowed_action.substr(0, allowed_action.size() - 2);
                std::vector<std::string> actual_action_parts;
                StringUtils::split(action, actual_action_parts, ":");
                if(actual_action_parts[0] == allowed_resource) {
                    action_is_allowed = true;
                    break;
                }
            }
        }

        if(!action_is_allowed) {
            return false;
        }
    }

    bool coll_allowed = false;

    for(const std::string& allowed_collection: api_key.collections) {
        if(allowed_collection == "*" || (allowed_collection == req_collection) || req_collection.empty() ||
            regexp_match(req_collection, allowed_collection)) {
            coll_allowed = true;
            break;
        }
    }

    if(!coll_allowed) {
        // even if one collection is not allowed, we reject the entire request
        return false;
    }

    const bool check_synonym_sets = has_non_wildcard_constraint(api_key.synonym_sets);
    const bool check_curation_sets = has_non_wildcard_constraint(api_key.curation_sets);

    if(!check_synonym_sets && !check_curation_sets) {
        return true;
    }

    auto coll = CollectionManager::get_instance().get_collection(req_collection);
    if(!coll) {
        return true;
    }


    if(check_synonym_sets) {
        auto search_synonym_sets = resolve_requested_search_sets(params, auth_context,
                                      "synonym_sets",
                                      AUTH_REQUESTED_SYNONYM_SETS_PARAM,
                                      AUTH_PRESET_SYNONYM_SETS_PARAM);

        auto effective_synonym_sets = merge_requested_sets(coll->get_synonym_sets(),
                                                           search_synonym_sets);
        if(!sets_allowed(api_key.synonym_sets, effective_synonym_sets)) {
            return false;
        }
    }

    if(check_curation_sets) {
        auto search_curation_sets = resolve_requested_search_sets(params, auth_context,
                                      "curation_sets",
                                        AUTH_REQUESTED_CURATION_SETS_PARAM,
                                        AUTH_PRESET_CURATION_SETS_PARAM);

        auto effective_curation_sets = merge_requested_sets(coll->get_curation_sets(),
                                                            search_curation_sets);
        if(!sets_allowed(api_key.curation_sets, effective_curation_sets)) {
            return false;
        }
    }

    return true;
}

Option<bool> AuthManager::authenticate_parse_params(const collection_key_t& scoped_api_key, const std::string& action,
                                                    const std::map<std::string, std::string>& params,
                                                    const nlohmann::json& auth_context,
                                                    nlohmann::json& embedded_params) const {

    // allow only searches from scoped keys
    if(action != DOCUMENTS_SEARCH_ACTION) {
        LOG(ERROR) << "Scoped API keys can only be used for searches.";
        return Option<bool>(403, "Forbidden.");
    }

    const std::string& key_payload = StringUtils::base64_decode(scoped_api_key.api_key);

    if(key_payload.size() < HMAC_BASE64_LEN + api_key_t::PREFIX_LEN) {
        LOG(ERROR) << "Malformed scoped API key.";
        return Option<bool>(403, "Forbidden.");
    }

    // FORMAT:
    // <DIGEST><PARENT_KEY_PREFIX><PARAMS>
    const std::string& hmacSHA256 = key_payload.substr(0, HMAC_BASE64_LEN);
    const std::string& key_prefix = key_payload.substr(HMAC_BASE64_LEN, api_key_t::PREFIX_LEN);
    const std::string& custom_params = key_payload.substr(HMAC_BASE64_LEN + api_key_t::PREFIX_LEN);

    // Calculate and verify hmac against matching api key.
    // There could be several matching keys since we look up only on a 4-char prefix.
    auto prefix_range = api_keys.equal_prefix_range(key_prefix);

    for(auto it = prefix_range.first; it != prefix_range.second; ++it) {
        const api_key_t& root_api_key = it.value();

        // finally verify hmac
        std::string digest = StringUtils::hmac(root_api_key.value, custom_params);

        if(digest == hmacSHA256) {
            try {
                embedded_params = nlohmann::json::parse(custom_params);
            } catch(const std::exception& e) {
                return Option<bool>(403, "Forbidden.");
            }

            if(!embedded_params.is_object()) {
                return Option<bool>(403, "Forbidden.");
            }

            auto effective_collection_op = resolve_scoped_search_collection(scoped_api_key.collection, embedded_params);
            if(!effective_collection_op.ok()) {
                LOG(ERROR) << effective_collection_op.error();
                return Option<bool>(403, "Forbidden.");
            }
            const auto effective_collection = effective_collection_op.get();

            // ensure that parent key collection filter matches the final collection that will be executed
            bool auth_success = auth_against_key(effective_collection, action, root_api_key, true,
                                                 params, merge_auth_context(auth_context, embedded_params));
            if(!auth_success) {
                return Option<bool>(403, "Forbidden.");
            }

            if(embedded_params.count("expires_at") != 0) {
                if(!embedded_params["expires_at"].is_number_integer() ||
                   embedded_params["expires_at"].get<int64_t>() < 0) {
                    return Option<bool>(403, "Forbidden.");
                }

                // if parent key's expiry timestamp is smaller, it takes precedence
                uint64_t expiry_ts = std::min(root_api_key.expires_at, embedded_params["expires_at"].get<uint64_t>());

                if(uint64_t(std::time(0)) > expiry_ts) {
                    return Option<bool>(403, "Forbidden.");
                }
            }

            embedded_params[AUTH_RESOLVED_COLLECTION_PARAM] = effective_collection;
            return Option<bool>(true);
        }
    }

    return Option<bool>(403, "Forbidden.");
}

std::string AuthManager::fmt_error(std::string&& error, const std::string& key) {
    std::stringstream ss;
    ss << error << " Key prefix: " << key.substr(0, api_key_t::PREFIX_LEN) << ", SHA256: "
       << StringUtils::hash_sha256(key);
    return ss.str();
}

Option<uint32_t> api_key_t::validate(const nlohmann::json &key_obj) {
    auto mandatory_keys = {
        "description", "actions", "collections"
    };

    for(auto key: mandatory_keys) {
        if(key_obj.count(key) == 0) {
            return Option<uint32_t>(400, std::string("Could not find a `") + key + "` key.");
        }
    }

    if(key_obj.count("value") != 0 && !key_obj["value"].is_string()) {
        return Option<uint32_t>(400, std::string("Key value must be a string."));
    }

    if(key_obj.count("description") != 0 && !key_obj["description"].is_string()) {
        return Option<uint32_t>(400, std::string("Key description must be a string."));
    }

    if(!key_obj["actions"].is_array() || key_obj["actions"].empty()) {
        return Option<uint32_t>(400,"Wrong format for `actions`. It should be an array of string.");
    }

    if(!key_obj["collections"].is_array() || key_obj["collections"].empty()) {
        return Option<uint32_t>(400,"Wrong format for `collections`. It should be an array of string.");
    }

    for(const nlohmann::json & item: key_obj["actions"]) {
        if(!item.is_string()) {
            return Option<uint32_t>(400,"Wrong format for `actions`. It should be an array of string.");
        }
    }

    for(const nlohmann::json & item: key_obj["collections"]) {
        if(!item.is_string()) {
            return Option<uint32_t>(400,"Wrong format for `collections`. It should be an array of string.");
        }
    }

    if(key_obj.count("expires_at") != 0) {
        if(!key_obj["expires_at"].is_number_integer() || key_obj["expires_at"].get<int64_t>() < 0) {
            return Option<uint32_t>(400,"Wrong format for `expires_at`. It should be an unsigned integer.");
        }
    }

    if(key_obj.count("synonym_sets") != 0) {
        if(!key_obj["synonym_sets"].is_array()) {
            return Option<uint32_t>(400,"Wrong format for `synonym_sets`. It should be an array of string.");
        }

        for(const nlohmann::json & item: key_obj["synonym_sets"]) {
            if(!item.is_string()) {
                return Option<uint32_t>(400,"Wrong format for `synonym_sets`. It should be an array of string.");
            }
        }
    }

    if(key_obj.count("curation_sets") != 0) {
        if(!key_obj["curation_sets"].is_array()) {
            return Option<uint32_t>(400,"Wrong format for `curation_sets`. It should be an array of string.");
        }

        for(const nlohmann::json & item: key_obj["curation_sets"]) {
            if(!item.is_string()) {
                return Option<uint32_t>(400,"Wrong format for `curation_sets`. It should be an array of string.");
            }
        }
    }
    
    return Option<uint32_t>(200);
}


bool AuthManager::add_item_to_params(std::map<std::string, std::string>& req_params,
                                     const nlohmann::detail::iteration_proxy_value<nlohmann::json::iterator>& item,
                                     bool overwrite) {

    std::string str_value;

    if(item.value().is_string()) {
        str_value = item.value().get<std::string>();
    } else if(item.value().is_number_integer()) {
        str_value = std::to_string(item.value().get<int64_t>());
    } else if(item.value().is_number_float()) {
        str_value = std::to_string(item.value().get<float>());
    } else if(item.value().is_boolean()) {
        str_value = item.value().get<bool>() ? "true" : "false";
    } else {
        return false;
    }

    if(req_params.count(item.key()) == 0) {
        req_params[item.key()] = str_value;
    } else if(item.key() == "filter_by") {
        auto& embedded_param = str_value;
        auto& query_param = req_params[item.key()];

        // Join follows $collection_name(<join_condition>) pattern. There might be false-positive matches with this
        // regular expression like, "(field: foo$) || (field: bar)" but that is acceptable.
        const std::regex join_pattern(R"(\$.+\(.+\))");
        if (std::regex_search(embedded_param, join_pattern) && std::regex_search(query_param, join_pattern) &&
            !Join::merge_join_conditions(embedded_param, query_param)) {
            return false;
        }

        if(!req_params[item.key()].empty() && !str_value.empty()) {
            req_params[item.key()] = "(" + req_params[item.key()] + ") && (" + str_value + ")";
        } else if(req_params[item.key()].empty() && !str_value.empty()) {
            req_params[item.key()] = "(" + str_value + ")";
        } else if(!req_params[item.key()].empty() && str_value.empty()) {
            req_params[item.key()] = "(" + req_params[item.key()] + ")";
        }
    } else if(overwrite) {
        req_params[item.key()] = str_value;
    }

    return true;
}

void AuthManager::remove_expired_keys() {
    const Option<std::vector<api_key_t>>& keys_op = list_keys();
    if(!keys_op.ok()) {
        LOG(ERROR) << keys_op.error();
        return;
    }

    const std::vector<api_key_t>& keys = keys_op.get();
    for(const auto& key : keys) {
        if(key.autodelete &&  (uint64_t(std::time(0)) > key.expires_at)) {
            LOG(INFO) << "Deleting expired key " << key.value;
            auto delete_op = remove_key(key.id);
            if(!delete_op.ok()) {
                LOG(ERROR) << delete_op.error();
            }
        }
    }
}

void AuthManager::do_housekeeping() {
    remove_expired_keys();
}

std::vector<std::string> AuthManager::get_api_key_collections(const std::string& value) {
    if(api_keys.find(value) != api_keys.end()) {
        return api_keys.at(value).collections;
    }

    return std::vector<std::string>();
}
