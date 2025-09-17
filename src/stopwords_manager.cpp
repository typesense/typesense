#include "include/stopwords_manager.h"
#include "include/tokenizer.h"
#include "include/string_utils.h"

void StopwordsManager::init(Store* _store) {
    store = _store;
}

spp::sparse_hash_map<std::string, stopword_struct_t> StopwordsManager::get_stopwords() const {
    std::shared_lock lock(mutex);
    return stopword_configs;
}

Option<bool> StopwordsManager::get_stopword(const std::string& stopword_name, stopword_struct_t& stopwords_struct) const {
    std::shared_lock lock(mutex);

    const auto& it = stopword_configs.find(stopword_name);
    if(it != stopword_configs.end()) {
        stopwords_struct = it->second;
        return Option<bool>(true);
    }

    return Option<bool>(404, "Stopword `" + stopword_name +"` not found.");
}

Option<bool> StopwordsManager::get_combined_stopwords(const std::string& stopwords_set, stopword_struct_t& combined_stopwords) const {
    std::shared_lock lock(mutex);
    
    // Parse stopword set names from comma-separated string
    auto stopword_set_names = StringUtils::parse_stopword_set_names(stopwords_set);
    
    if(stopword_set_names.empty()) {
        return Option<bool>(400, "No stopword sets specified.");
    }
    
    if(stopword_set_names.size() == 1) {
        return get_stopword(stopword_set_names[0], combined_stopwords);
    }
    
    // Multiple stopword sets - create combined stopword struct
    combined_stopwords.id = "combined";
    combined_stopwords.stopwords.clear();
    combined_stopwords.locale = ""; // Combined sets may have different locales
    
    // Merge all stopword sets
    for(const auto& name : stopword_set_names) {
        stopword_struct_t individual_set;
        const auto& it = stopword_configs.find(name);
        if(it != stopword_configs.end()) {
            individual_set = it->second;
            combined_stopwords.stopwords.insert(individual_set.stopwords.begin(), individual_set.stopwords.end());
        } else {
            return Option<bool>(404, "Could not find the stopword set named `" + name + "`.");
        }
    }
    
    return Option<bool>(true);
}

Option<bool> StopwordsManager::upsert_stopword(const std::string& stopword_name, const nlohmann::json& stopwords_json,
                                               bool write_to_store) {
    std::unique_lock lock(mutex);

    const char* STOPWORD_VALUES = "stopwords";
    const char* STOPWORD_LOCALE = "locale";
    std::string locale  = "";

    if(stopwords_json.count(STOPWORD_VALUES) == 0){
        return Option<bool>(400, (std::string("Parameter `") + STOPWORD_VALUES + "` is required"));
    }

    if(stopwords_json[STOPWORD_VALUES].empty()) {
        return Option<bool>(400, (std::string("Parameter `") + STOPWORD_VALUES + "` is empty"));
    }

    if((!stopwords_json[STOPWORD_VALUES].is_array()) || (!stopwords_json[STOPWORD_VALUES][0].is_string())) {
        return Option<bool>(400, (std::string("Parameter `") + STOPWORD_VALUES + "` is required as string array value"));
    }

    if(stopwords_json.count(STOPWORD_LOCALE) != 0) {
        if (!stopwords_json[STOPWORD_LOCALE].is_string()) {
            return Option<bool>(400, (std::string("Parameter `") + STOPWORD_LOCALE + "` is required as string value"));
        }
        locale = stopwords_json[STOPWORD_LOCALE];
    }

    if(write_to_store) {
        bool inserted = store->insert(get_stopword_key(stopword_name), stopwords_json.dump());
        if (!inserted) {
            return Option<bool>(500, "Unable to insert into store.");
        }
    }

    std::vector<std::string> tokens;
    spp::sparse_hash_set<std::string> stopwords_set;
    const auto& stopwords = stopwords_json[STOPWORD_VALUES];

    for (const auto &stopword: stopwords.items()) {
        const auto& val = stopword.value().get<std::string>();
        Tokenizer(val, true, false, locale, {}, {}).tokenize(tokens);

        for(const auto& tok : tokens) {
            stopwords_set.emplace(tok);
        }
        tokens.clear();
    }
    stopword_configs[stopword_name] = stopword_struct_t{stopword_name, stopwords_set, locale};
    return Option<bool>(true);
}

std::string StopwordsManager::get_stopword_key(const std::string& stopword_name) {
    return std::string(STOPWORD_PREFIX) + "_" + stopword_name;
}

Option<bool> StopwordsManager::delete_stopword(const std::string& stopword_name) {
    std::unique_lock lock(mutex);

    if(stopword_configs.find(stopword_name) == stopword_configs.end()) {
        return Option<bool>(404, "Stopword `" + stopword_name + "` not found.");
    }

    stopword_configs.erase(stopword_name);

    bool removed = store->remove(get_stopword_key(stopword_name));
    if(!removed) {
        return Option<bool>(500, "Unable to delete from store.");
    }

    return Option<bool>(true);
}

void StopwordsManager::dispose() {
    std::unique_lock lock(mutex);
    stopword_configs.clear();
}

bool StopwordsManager::stopword_exists(const std::string &stopword) {
    std::shared_lock lock(mutex);

    return stopword_configs.find(stopword) != stopword_configs.end();
}