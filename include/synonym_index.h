#pragma once

#include <set>
#include "sparsepp.h"
#include "json.hpp"
#include "string_utils.h"
#include "option.h"
#include "tokenizer.h"
#include "store.h"

struct synonym_t {
    std::string id;
    std::string set_name;
    std::string raw_root;
    // used in code and differs from API + storage format
    std::vector<std::string> root;

    std::vector<std::string> raw_synonyms;
    // used in code and differs from API + storage format
    std::vector<std::vector<std::string>> synonyms;

    std::string locale;
    std::vector<char> symbols;

    synonym_t() = default;

    nlohmann::json to_view_json() const;

    static Option<bool> parse(const nlohmann::json& synonym_json, synonym_t& syn);

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

class SynonymIndex {
private:
    mutable std::shared_mutex mutex;
    Store* store;
    spp::sparse_hash_map<std::string, synonym_t> synonym_definitions;
    spp::sparse_hash_map<uint64_t, std::vector<std::string>> synonym_index;
    spp::sparse_hash_map<std::string, spp::sparse_hash_set<std::string>> synonym_sets_ids_map;

    void synonym_reduction_internal(const std::vector<std::string>& tokens,
                                    size_t start_window_size,
                                    size_t start_index_pos,
                                    std::set<uint64_t>& processed_syn_hashes,
                                    std::vector<std::vector<std::string>>& results,
                                    const std::vector<std::string>& orig_tokens,
                                    const spp::sparse_hash_set<std::string>& synonym_sets) const;

public:
    SynonymIndex() {}

    static SynonymIndex& get_instance() {
        static SynonymIndex instance;
        return instance;
    }

    static constexpr const char* COLLECTION_SYNONYM_PREFIX = "$CY";
    static constexpr const char* SET_SYNONYM_PREFIX = "$SY";

    void init(Store* store);

    void reset();

    static std::string get_synonym_key(const std::string & collection_name, const std::string & synonym_id);

    static std::string get_synonym_set_key(const std::string & synonym_id);

    void synonym_reduction(const std::vector<std::string>& tokens,
                           std::vector<std::vector<std::string>>& results,
                           const spp::sparse_hash_set<std::string>& synonym_sets = spp::sparse_hash_set<std::string>{}) const;

    spp::sparse_hash_map<std::string, synonym_t> get_synonyms();

    bool get_synonym(const std::string& id, synonym_t& synonym);

    Option<bool> add_synonym(const std::string &key, const synonym_t& synonym, bool write_to_store = true);

    Option<bool> remove_synonym(const std::string &key, const std::string & id);

    //set operations

    Option<bool> add_synonym_to_set(const std::string& set_name, const synonym_t& synonym, bool write_to_store = true);

    Option<bool> remove_synonym_from_set(const std::string& set_name, const std::string & id);

    Option<bool> remove_synonym_set(const std::string& set_name);

    bool get_synonyms_sets(std::vector<std::string>& sets);

    Option<bool> get_synonym_set(const std::string& set_name, std::vector<synonym_t>& synonym_set);
};