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
    std::vector<std::string> root;
    std::vector<std::vector<std::string>> synonyms;

    synonym_t() = default;

    synonym_t(const std::string& id, const std::vector<std::string>& root,
              const std::vector<std::vector<std::string>>& synonyms):
            id(id), root(root), synonyms(synonyms) {

    }

    explicit synonym_t(const nlohmann::json& synonym) {
        id = synonym["id"].get<std::string>();
        if(synonym.count("root") != 0) {
            root = synonym["root"].get<std::vector<std::string>>();
        }
        synonyms = synonym["synonyms"].get<std::vector<std::vector<std::string>>>();
    }

    nlohmann::json to_json() const {
        nlohmann::json obj;
        obj["id"] = id;
        obj["root"] = root;
        obj["synonyms"] = synonyms;
        return obj;
    }

    nlohmann::json to_view_json() const {
        nlohmann::json obj;
        obj["id"] = id;
        obj["root"] = StringUtils::join(root, " ");

        obj["synonyms"] = nlohmann::json::array();

        for(const auto& synonym: synonyms) {
            obj["synonyms"].push_back(StringUtils::join(synonym, " "));
        }

        return obj;
    }

    static Option<bool> parse(const nlohmann::json& synonym_json, synonym_t& syn) {
        if(synonym_json.count("id") == 0) {
            return Option<bool>(400, "Missing `id` field.");
        }

        if(synonym_json.count("synonyms") == 0) {
            return Option<bool>(400, "Could not find an array of `synonyms`");
        }

        if(synonym_json.count("root") != 0 && !synonym_json["root"].is_string()) {
            return Option<bool>(400, "Key `root` should be a string.");
        }

        if (!synonym_json["synonyms"].is_array() || synonym_json["synonyms"].empty()) {
            return Option<bool>(400, "Could not find an array of `synonyms`");
        }

        for(const auto& synonym: synonym_json["synonyms"]) {
            if(!synonym.is_string() || synonym == "") {
                return Option<bool>(400, "Could not find a valid string array of `synonyms`");
            }

            std::vector<std::string> tokens;
            Tokenizer(synonym, true).tokenize(tokens);
            syn.synonyms.push_back(tokens);
        }

        if(synonym_json.count("root") != 0) {
            std::vector<std::string> tokens;
            Tokenizer(synonym_json["root"], true).tokenize(tokens);
            syn.root = tokens;
        }

        syn.id = synonym_json["id"];
        return Option<bool>(true);
    }

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

    void synonym_reduction_internal(const std::vector<std::string>& tokens,
                                    size_t start_window_size,
                                    size_t start_index_pos,
                                    std::set<uint64_t>& processed_syn_hashes,
                                    std::vector<std::vector<std::string>>& results) const;

public:

    static constexpr const char* COLLECTION_SYNONYM_PREFIX = "$CY";

    SynonymIndex(Store* store): store(store) { }

    static std::string get_synonym_key(const std::string & collection_name, const std::string & synonym_id);

    void synonym_reduction(const std::vector<std::string>& tokens,
                           std::vector<std::vector<std::string>>& results) const;

    spp::sparse_hash_map<std::string, synonym_t> get_synonyms();

    bool get_synonym(const std::string& id, synonym_t& synonym);

    Option<bool> add_synonym(const std::string & collection_name, const synonym_t& synonym);

    Option<bool> remove_synonym(const std::string & collection_name, const std::string & id);
};