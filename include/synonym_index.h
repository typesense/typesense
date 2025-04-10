#pragma once

#include <set>
#include "sparsepp.h"
#include "json.hpp"
#include "string_utils.h"
#include "option.h"
#include "tokenizer.h"
#include "store.h"
#include "art.h"

struct synonym_t {
    std::string id;

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

struct synonym_match_t {
    std::string synonym_id;
    size_t start_index;
    size_t end_index;
};

struct synonym_node_t {
    std::unordered_map<std::string, synonym_node_t*> children;
    art_tree* children_tree;
    size_t children_tree_index = 0;
    std::vector<std::string> terminal_synonym_ids;
    std::string token;

    ~synonym_node_t() {
        for(auto& child : children) {
            delete child.second;
        }
        if (children_tree != nullptr) {
            art_tree_destroy(children_tree);
            delete children_tree;
        }
        children.clear();
    }
    
    // avoid copying art tree
    synonym_node_t& operator=(const synonym_node_t& other) = delete;

    // avoid copying art tree
    synonym_node_t(const synonym_node_t& other) = delete;

    synonym_node_t(synonym_node_t&& other) noexcept {
        children = std::move(other.children);
        other.children.clear();
        terminal_synonym_ids = std::move(other.terminal_synonym_ids);
        children_tree = other.children_tree;
        other.children_tree = nullptr;
        children_tree_index = other.children_tree_index;
        token = std::move(other.token);
    }

    synonym_node_t& operator=(synonym_node_t&& other) noexcept {
        if (this != &other) {
            children = std::move(other.children);
            other.children.clear();
            terminal_synonym_ids = std::move(other.terminal_synonym_ids);
            children_tree = other.children_tree;
            other.children_tree = nullptr;
            children_tree_index = other.children_tree_index;
            token = std::move(other.token);
        }
        return *this;
    }

    synonym_node_t() {
        children_tree = new art_tree();
        art_tree_init(children_tree);
    }

    Option<bool> add(const synonym_t& synonym);

    Option<bool> remove(const synonym_t& synonym);

    Option<bool> get_synonyms(const std::vector<std::string>& tokens, std::vector<synonym_match_t>& synonyms, uint32_t num_typos, size_t start_index = 0, bool synonym_prefix = false) const;

    Option<bool> get_synonyms(const std::vector<std::string>& tokens, std::vector<synonym_match_t>& synonyms, uint32_t num_typos, bool synonym_prefix, size_t start_index, size_t current_index) const;

    std::vector<synonym_node_t*> get_matching_children(const std::string& token, uint32_t num_typos, bool synonym_prefix) const;

    void cleanup();

    static bool cleanup(synonym_node_t* current, synonym_node_t* parent);

};

class SynonymIndex {
private:

    mutable std::shared_mutex mutex;
    Store* store;
    spp::sparse_hash_map<std::string, uint32_t> synonym_ids_index_map;
    uint32_t synonym_index = 0;
    std::map<uint32_t, synonym_t> synonym_definitions;
    synonym_node_t synonym_trie_root;
public:

    static constexpr const char* COLLECTION_SYNONYM_PREFIX = "$CY";

    SynonymIndex(Store* store): store(store) {

    }

    ~SynonymIndex() {
    }

    static std::string get_synonym_key(const std::string & collection_name, const std::string & synonym_id);

    void synonym_reduction(const std::vector<std::string>& tokens,
                           const std::string& locale,
                           std::vector<std::vector<std::string>>& results,
                           bool synonym_prefix, uint32_t synonym_num_typos) const;

    Option<std::map<uint32_t, synonym_t*>> get_synonyms(uint32_t limit=0, uint32_t offset=0);

    bool get_synonym(const std::string& id, synonym_t& synonym);

    Option<bool> add_synonym(const std::string & collection_name, const synonym_t& synonym,
                             bool write_to_store = true);

    Option<bool> remove_synonym(const std::string & collection_name, const std::string & id);
};