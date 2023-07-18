#include "include/stopwords_manager.h"
#include "include/tokenizer.h"

void StopwordsManager::init(Store* _store) {
    store = _store;
}

spp::sparse_hash_map<std::string, spp::sparse_hash_set<std::string>> StopwordsManager::get_stopwords() const {
    std::shared_lock lock(mutex);
    return stopword_configs;
}

Option<bool> StopwordsManager::get_stopword(const std::string& stopword_name, spp::sparse_hash_set<std::string>& stopwords) const {
    std::shared_lock lock(mutex);

    const auto& it = stopword_configs.find(stopword_name);
    if(it != stopword_configs.end()) {
        stopwords = it->second;
        return Option<bool>(true);
    }

    return Option<bool>(404, "Stopword `" + stopword_name +"` not found.");
}

Option<bool> StopwordsManager::upsert_stopword(const std::string& stopword_name, const nlohmann::json& stopwords_json) {
    std::unique_lock lock(mutex);

    bool inserted = store->insert(get_stopword_key(stopword_name), stopwords_json.dump());
    if(!inserted) {
        return Option<bool>(500, "Unable to insert into store.");
    }

    std::vector<std::string> tokens;
    spp::sparse_hash_set<std::string> stopwords_set;
    const auto& stopwords = stopwords_json["stopwords"];
    const auto& locale = stopwords_json["locale"];

    for (const auto &stopword: stopwords.items()) {
        const auto& val = stopword.value().get<std::string>();
        Tokenizer(val, true, false, locale, {}, {}).tokenize(tokens);

        for(const auto& tok : tokens) {
            stopwords_set.emplace(tok);
        }
        tokens.clear();
    }
    stopword_configs[stopword_name] = stopwords_set;
    return Option<bool>(true);
}

std::string StopwordsManager::get_stopword_key(const std::string& stopword_name) {
    return std::string(STOPWORD_PREFIX) + "_" + stopword_name;
}

Option<bool> StopwordsManager::delete_stopword(const std::string& stopword_name) {
    std::unique_lock lock(mutex);

    bool removed = store->remove(get_stopword_key(stopword_name));
    if(!removed) {
        return Option<bool>(500, "Unable to delete from store.");
    }

    if(stopword_configs.find(stopword_name) == stopword_configs.end()) {
        return Option<bool>(404, "Stopword `" + stopword_name + "` not found.");
    }

    stopword_configs.erase(stopword_name);
    return Option<bool>(true);
}

void StopwordsManager::load_stopword_config(const std::string& stopword_name, const nlohmann::json& stopwords_json) {
    std::unique_lock lock(mutex);

    std::vector<std::string> tokens;
    spp::sparse_hash_set<std::string> stopwords_set;
    const auto& stopwords = stopwords_json["stopwords"];
    const auto& locale = stopwords_json["locale"];

    for (const auto &stopword: stopwords.items()) {
        const auto& val = stopword.value().get<std::string>();
        Tokenizer(val, true, false, locale, {}, {}).tokenize(tokens);

        for(const auto& tok : tokens) {
            stopwords_set.emplace(tok);
        }
        tokens.clear();
    }
    stopword_configs[stopword_name] = stopwords_set;
}