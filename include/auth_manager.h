#pragma once

#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <tsl/htrie_map.h>
#include "json.hpp"
#include "option.h"
#include "store.h"

struct api_key_t {
    uint32_t id;
    std::string value;
    std::string description;
    std::vector<std::string> actions;
    std::vector<std::string> collections;
    uint64_t expires_at;
    bool autodelete;

    static constexpr const size_t PREFIX_LEN = 4;
    static constexpr const uint64_t FAR_FUTURE_TIMESTAMP = 64723363199;  // year 4020

    api_key_t() {

    }

    api_key_t(const std::string &value, const std::string &description, const std::vector<std::string> &actions,
              const std::vector<std::string> &collections, uint64_t expires_at, bool autodel=false) :
            value(value), description(description), actions(actions), collections(collections), expires_at(expires_at),
            autodelete(autodel) {

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

        // handle optional fields

        if(key_obj.count("expires_at") != 0) {
            expires_at = key_obj["expires_at"];
        } else {
            expires_at = FAR_FUTURE_TIMESTAMP;
        }

        if(key_obj.count("autodelete") != 0) {
            autodelete = key_obj["autodelete"].get<bool>();
        } else {
            autodelete = false;
        }

        return Option<bool>(true);
    }

    static Option<uint32_t> validate(const nlohmann::json & key_obj);

    nlohmann::json to_json() const {
        nlohmann::json obj;
        obj["id"] = id;
        obj["value"] = value;
        obj["description"] = description;
        obj["actions"] = actions;
        obj["collections"] = collections;
        obj["expires_at"] = expires_at;
        obj["autodelete"] = autodelete;

        return obj;
    }

    api_key_t& truncate_value() {
        value = value.substr(0, PREFIX_LEN);  // return only first 4 chars
        return (*this);
    }
};

struct collection_key_t {
    std::string collection;
    std::string api_key;

    explicit collection_key_t(const std::string& collection, const std::string& api_key):
            collection(collection), api_key(api_key) {

    }
};

class AuthManager {

private:

    mutable std::shared_mutex mutex;

    tsl::htrie_map<char, api_key_t> api_keys;  // stores key_value => key mapping
    Store *store;

    std::string bootstrap_auth_key;

    // Auto incrementing API KEY ID
    std::atomic<uint32_t> next_api_key_id;

    // Using a $ prefix so that these meta keys stay above record entries in a lexicographically ordered KV store
    static constexpr const char* API_KEY_NEXT_ID_KEY = "$KN";
    static constexpr const char* API_KEYS_PREFIX = "$KP";

    uint32_t get_next_api_key_id();

    static constexpr const char* DOCUMENTS_SEARCH_ACTION = "documents:search";

    static std::string fmt_error(std::string&& error, const std::string& key);

    Option<bool> authenticate_parse_params(const collection_key_t& scoped_api_key, const std::string& action,
                                           nlohmann::json& embedded_params) const ;

    bool auth_against_key(const std::string& req_collection, const std::string& action,
                          const api_key_t &api_key, const bool search_only) const;

    static bool regexp_match(const std::string& value, const std::string& regexp);

    void remove_expired_keys();
public:

    static const size_t GENERATED_KEY_LEN = 32;
    static const size_t HMAC_BASE64_LEN = 44;

    AuthManager() = default;

    Option<bool> init(Store* store, const std::string& bootstrap_auth_key);

    Option<std::vector<api_key_t>> list_keys() const;

    Option<api_key_t> get_key(uint32_t id, bool truncate_value = true) const;

    Option<api_key_t> create_key(api_key_t& api_key);

    Option<api_key_t> remove_key(uint32_t id);

    bool authenticate(const std::string& action,
                      const std::vector<collection_key_t>& collection_keys,
                      std::map<std::string, std::string>& params,
                      std::vector<nlohmann::json>& embedded_params_vec) const;

    static bool add_item_to_params(std::map<std::string, std::string> &req_params,
                                   const nlohmann::detail::iteration_proxy_value<nlohmann::json::iterator>& item,
                                   bool overwrite);

    void do_housekeeping();

    std::vector<std::string> get_api_key_collections(const std::string& value);
};