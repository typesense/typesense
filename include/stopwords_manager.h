#pragma once

#include "sparsepp.h"
#include "option.h"
#include "json.hpp"
#include "shared_mutex"
#include "mutex"
#include "store.h"

class StopwordsManager{
private:
    spp::sparse_hash_map<std::string, spp::sparse_hash_set<std::string>> stopword_configs;

    static std::string get_stopword_key(const std::string & stopword_name);

    mutable std::shared_mutex mutex;

    Store* store = nullptr;
public:
    StopwordsManager() = default;

    ~StopwordsManager() {
        stopword_configs.clear();
    }

    static constexpr const char* STOPWORD_PREFIX = "$SW";

    static StopwordsManager& get_instance() {
        static StopwordsManager instance;
        return instance;
    }

    void init(Store* store);

    spp::sparse_hash_map<std::string, spp::sparse_hash_set<std::string>> get_stopwords() const;

    Option<bool> get_stopword(const std::string&, spp::sparse_hash_set<std::string>&) const;

    Option<bool> upsert_stopword(const std::string&, const nlohmann::json&, bool write_to_store=false);

    Option<bool> delete_stopword(const std::string&);

    void dispose();
};