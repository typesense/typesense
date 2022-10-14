#pragma once

#include <set>
#include "sparsepp.h"
#include "json.hpp"
#include "string_utils.h"
#include "option.h"
#include "tokenizer.h"
#include "store.h"

struct stop_word_t {
    std::string id;
    std::vector<std::string> word;
    std::string locale;
    std::vector<char> symbols;

    stop_word_t() = default;

    stop_word_t(const std::string& id, const std::vector<std::string>& word):
            id(id), word(word) {

    }

    nlohmann::json to_view_json() const;

    static Option<bool> parse(const nlohmann::json& stop_word_json, stop_word_t& stpwrd);

    static uint64_t get_hash(const std::vector<std::string>& tokens) {
        uint64_t hash = 1;
        for(size_t i=0; i < tokens.size(); i++) {
            auto& token = tokens[i];
            uint64_t token_hash = StringUtils::hash_wy(token.c_str(), token.size());
            if(i == 0) {
                hash = token_hash;
            } else {
                hash = StringUtils::hash_combine(hash, token_hash);
            }
        }

        return hash;
    }
};

class StopWordIndex {
private:

    mutable std::shared_mutex mutex;
    Store* store;
    spp::sparse_hash_map<std::string, stop_word_t> stop_word_definitions;
    spp::sparse_hash_map<uint64_t, std::vector<std::string>> stop_word_index;

public:

    static constexpr const char* COLLECTION_stop_word_PREFIX = "$CY";

    StopWordIndex(Store* store): store(store) { }

    static std::string get_stop_word_key(const std::string & collection_name, const std::string & stop_word_id);

    bool is_stop_word(const std::string& token) const;

    spp::sparse_hash_map<std::string, stop_word_t> get_stop_words();

    bool get_stop_word(const std::string& id, stop_word_t& stop_word);

    Option<bool> add_stop_word(const std::string & collection_name, const stop_word_t& stop_word);

    Option<bool> remove_stop_word(const std::string & collection_name, const std::string & id);
};