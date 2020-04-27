#pragma once

#include <string>
#include <vector>
#include <map>
#include "json.hpp"
#include "option.h"
#include "store.h"

struct api_key_t {
    uint32_t id;
    std::string value;
    std::string description;
    std::vector<std::string> actions;
    std::vector<std::string> collections;

    api_key_t() {

    }

    api_key_t(const std::string& value, const std::string& description, const std::vector<std::string>& actions,
              const std::vector<std::string>& collections):
              value(value), description(description), actions(actions), collections(collections) {

    }

    Option<bool> load(const std::string & json_str) {
        nlohmann::json key_obj;

        try {
            key_obj = nlohmann::json::parse(json_str);
        } catch(...) {
            return Option<bool>(500, "Error while parsing JSON string.");
        }

        id = key_obj["id"];
        value = key_obj["value"];
        description = key_obj["description"].get<std::string>();
        actions = key_obj["actions"].get<std::vector<std::string>>();
        collections = key_obj["collections"].get<std::vector<std::string>>();

        return Option<bool>(true);
    }

    static Option<uint32_t> validate(const nlohmann::json & key_obj) {
        auto mandatory_keys = {
            "description", "actions", "collections"
        };

        for(auto key: mandatory_keys) {
            if(key_obj.count(key) == 0) {
                return Option<uint32_t>(400, std::string("Could not find a `") + key + "` key.");
            }
        }

        return Option<uint32_t>(200);
    }

    nlohmann::json to_json() const {
        nlohmann::json obj;
        obj["id"] = id;
        obj["value"] = value;
        obj["description"] = description;
        obj["actions"] = actions;
        obj["collections"] = collections;

        return obj;
    }

    api_key_t& truncate_value() {
        value = value.substr(0, 4);  // return only first 4 chars
        return (*this);
    }
};

class AuthManager {

private:

    std::map<std::string, api_key_t> api_keys;  // stores key_value => key mapping
    Store *store;

    // Auto incrementing API KEY ID
    uint32_t next_api_key_id;

    // Using a $ prefix so that these meta keys stay above record entries in a lexicographically ordered KV store
    static constexpr const char* API_KEY_NEXT_ID_KEY = "$KN";
    static constexpr const char* API_KEYS_PREFIX = "$KP";

    uint32_t get_next_api_key_id();

public:

    AuthManager() = default;

    Option<bool> init(Store *store);

    Option<std::vector<api_key_t>> list_keys();

    Option<api_key_t> get_key(uint32_t id, bool truncate_value = true);

    Option<api_key_t> create_key(api_key_t& api_key);

    Option<api_key_t> remove_key(uint32_t id);

    bool authenticate(
        const std::string& req_api_key,
        const std::string& action,
        const std::string& collection
    );
};