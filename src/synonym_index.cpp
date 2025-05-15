#include "synonym_index.h"
#include "posting.h"


void SynonymIndex::synonym_reduction(const std::vector<std::string>& tokens,
                                     const std::string& locale,
                                     std::vector<std::vector<std::string>>& results,
                                     bool synonym_prefix,
                                     uint32_t synonym_num_typos) const {
    std::shared_lock lock(mutex);
    if (synonym_definitions.empty()) {
        return;
    }

    // hard cap to prevent run-away memory usage
    constexpr std::size_t kMaxExpansionsPerCell = 200;
    auto capped_insert = [&](std::set<std::vector<std::string>>& cell,
                             std::vector<std::string>&& candidate) {
        if (cell.size() < kMaxExpansionsPerCell) {
            cell.insert(std::move(candidate));
        }
    };

    std::vector<std::set<std::vector<std::string>>> dp(tokens.size() + 1);
    std::vector<std::vector<synonym_match_t>> synonym_matches(tokens.size());

    for (std::size_t i = 0; i < tokens.size(); ++i) {
        synonym_trie_root.get_synonyms(tokens, synonym_matches[i], synonym_num_typos, i, synonym_prefix);
    }

    dp[tokens.size()] = {{}};

    for (int i = static_cast<int>(tokens.size()) - 1; i >= 0; --i) {
        for (const auto& suffixExp: dp[i + 1]) {
            std::vector<std::string> newExp;
            newExp.reserve(1 + suffixExp.size());
            newExp.push_back(tokens[i]);
            newExp.insert(newExp.end(), suffixExp.begin(), suffixExp.end());
            capped_insert(dp[i], std::move(newExp));
        }

        for (const auto& sm: synonym_matches[i]) {
            auto idxIt = synonym_ids_index_map.find(sm.synonym_id);
            if (idxIt == synonym_ids_index_map.end()) {
                continue;
            }

            auto defIt = synonym_definitions.find(idxIt->second);
            if (defIt == synonym_definitions.end()) {
                continue;
            }

            const auto& def = defIt->second;
            if (def.locale != locale) {
                continue;
            }

            if (!def.root.empty()) {
                std::vector<std::string> orig_tokens(tokens.begin() + sm.start_index,
                                                     tokens.begin() + sm.end_index);
                if (def.root != orig_tokens) {
                    continue;
                }
            }

            std::size_t end_idx = sm.end_index;
            for (const auto& suffixExp: dp[end_idx]) {
                for (const auto& synTokens: def.synonyms) {
                    std::vector<std::string> newExp;
                    newExp.reserve(synTokens.size() + suffixExp.size());
                    newExp.insert(newExp.end(), synTokens.begin(), synTokens.end());
                    newExp.insert(newExp.end(), suffixExp.begin(), suffixExp.end());
                    capped_insert(dp[i], std::move(newExp));
                }
            }
        }
    }

    for (const auto& exp: dp[0]) {
        // exclude original tokens
        if (exp != tokens) {
            results.push_back(exp);
        }
    }
}

Option<bool> SynonymIndex::add_synonym(const std::string & collection_name, const synonym_t& synonym,
                                       bool write_to_store) {
    std::unique_lock write_lock(mutex);
    if(synonym_ids_index_map.count(synonym.id) != 0) {
        write_lock.unlock();
        // first we have to delete existing entries so we can upsert
        Option<bool> rem_op = remove_synonym(collection_name, synonym.id);
        if (!rem_op.ok()) {
            return rem_op;
        }
        write_lock.lock();
    }

    synonym_definitions[synonym_index] = synonym;
    synonym_ids_index_map[synonym.id] = synonym_index;

    std::vector<std::string> keys;

    if(!synonym.root.empty()) {
        auto root_tokens_str = StringUtils::join(synonym.root, " ");
        keys.push_back(root_tokens_str);
    } else {
        for(const auto & syn_tokens : synonym.synonyms) {
            auto synonyms_str = StringUtils::join(syn_tokens, " ");
            keys.push_back(synonyms_str);
        }
    }


    ++synonym_index;

    write_lock.unlock();

    if(write_to_store) {
        bool inserted = store->insert(get_synonym_key(collection_name, synonym.id), synonym.to_view_json().dump());
        if(!inserted) {
            return Option<bool>(500, "Error while storing the synonym on disk.");
        }
    }

    synonym_trie_root.add(synonym);

    return Option<bool>(true);
}

bool SynonymIndex::get_synonym(const std::string& id, synonym_t& synonym) {
    std::shared_lock lock(mutex);

    if(synonym_ids_index_map.count(id) != 0) {
        auto index = synonym_ids_index_map.at(id);
        synonym = synonym_definitions.at(index);
        return true;
    }

    return false;
}

Option<bool> SynonymIndex::remove_synonym(const std::string & collection_name, const std::string &id) {
    std::unique_lock lock(mutex);
    const auto& syn_iter = synonym_ids_index_map.find(id);

    if(syn_iter != synonym_ids_index_map.end()) {
        bool removed = store->remove(get_synonym_key(collection_name, id));
        if(!removed) {
            return Option<bool>(500, "Error while deleting the synonym from disk.");
        }

        const auto& synonym = synonym_definitions.at(syn_iter->second);
        std::vector<std::string> keys;

        auto root_str = StringUtils::join(synonym.root, " ");
        keys.push_back(root_str);

        for(const auto & syn_tokens : synonym.synonyms) {
            auto synonyms_str = StringUtils::join(syn_tokens, " ");
            keys.push_back(synonyms_str);
        }

        auto index = synonym_ids_index_map.at(id);
        synonym_ids_index_map.erase(id);
        synonym_trie_root.remove(synonym);
        synonym_trie_root.cleanup();
        synonym_definitions.erase(index);

        return Option<bool>(true);
    }

    return Option<bool>(404, "Could not find that `id`.");
}

Option<std::map<uint32_t, synonym_t*>> SynonymIndex::get_synonyms(uint32_t limit, uint32_t offset) {
    std::shared_lock lock(mutex);
    std::map<uint32_t, synonym_t*> synonyms_map;

    auto synonym_it = synonym_definitions.begin();

    if(offset > 0) {
        if(offset >= synonym_definitions.size()) {
            return Option<std::map<uint32_t, synonym_t*>>(400, "Invalid offset param.");
        }

        std::advance(synonym_it, offset);
    }

    auto synonym_end = synonym_definitions.end();

    if(limit > 0 && (offset + limit < synonym_definitions.size())) {
        synonym_end = synonym_it;
        std::advance(synonym_end, limit);
    }

    while (synonym_it != synonym_end) {
        synonyms_map[synonym_it->first] = &synonym_it->second;
        synonym_it++;
    }

    return Option<std::map<uint32_t, synonym_t*>>(synonyms_map);
}

std::string SynonymIndex::get_synonym_key(const std::string & collection_name, const std::string & synonym_id) {
    return std::string(COLLECTION_SYNONYM_PREFIX) + "_" + collection_name + "_" + synonym_id;
}

Option<bool> synonym_t::parse(const nlohmann::json& synonym_json, synonym_t& syn) {
    if(synonym_json.count("id") == 0) {
        return Option<bool>(400, "Missing `id` field.");
    }

    if(synonym_json.count("synonyms") == 0) {
        return Option<bool>(400, "Could not find an array of `synonyms`");
    }

    if (!synonym_json["synonyms"].is_array() || synonym_json["synonyms"].empty()) {
        return Option<bool>(400, "Could not find an array of `synonyms`");
    }

    if(synonym_json.count("locale") != 0) {
        if(!synonym_json["locale"].is_string()) {
            return Option<bool>(400, "Synonym `locale` should be a string.`");
        }

        syn.locale = synonym_json["locale"].get<std::string>();
    }

    if(synonym_json.count("symbols_to_index") != 0) {
        if(!synonym_json["symbols_to_index"].is_array() || synonym_json["symbols_to_index"].empty() ||
            !synonym_json["symbols_to_index"][0].is_string()) {
            return Option<bool>(400, "Synonym `symbols_to_index` should be an array of strings.");
        }

        auto symbols = synonym_json["symbols_to_index"].get<std::vector<std::string>>();
        for(auto symbol: symbols) {
            if(symbol.size() != 1) {
                return Option<bool>(400, "Synonym `symbols_to_index` should be an array of single character symbols.");
            }

            syn.symbols.push_back(symbol[0]);
        }
    }

    if(synonym_json.count("root") != 0) {
        std::vector<std::string> tokens;

        if(synonym_json["root"].is_string()) {
            Tokenizer(synonym_json["root"].get<std::string>(), true, false, syn.locale, syn.symbols).tokenize(tokens);
            syn.raw_root = synonym_json["root"].get<std::string>();
        } else if(synonym_json["root"].is_array()) {
            // Typesense 0.23.1 and below incorrectly stored root as array
            for(const auto& root_ele: synonym_json["root"]) {
                if(!root_ele.is_string()) {
                    return Option<bool>(400, "Synonym root is not valid.");
                }

                tokens.push_back(root_ele.get<std::string>());
            }

            syn.raw_root = StringUtils::join(tokens, " ");
        } else {
            return Option<bool>(400, "Key `root` should be a string.");
        }

        syn.root = tokens;
    }

    for(const auto& synonym: synonym_json["synonyms"]) {
        std::vector<std::string> tokens;
        if(synonym.is_string()) {
            Tokenizer(synonym.get<std::string>(), true, false, syn.locale, syn.symbols).tokenize(tokens);
            syn.raw_synonyms.push_back(synonym.get<std::string>());
        } else if(synonym.is_array()) {
            // Typesense 0.23.1 and below incorrectly stored synonym as array
            if(synonym.empty()) {
                return Option<bool>(400, "Could not find a valid string array of `synonyms`");
            }

            for(const auto& ele: synonym) {
                if(!ele.is_string() || ele.get<std::string>().empty()) {
                    return Option<bool>(400, "Could not find a valid string array of `synonyms`");
                }
                tokens.push_back(ele.get<std::string>());
            }

            syn.raw_synonyms.push_back(StringUtils::join(tokens, " "));
        } else {
            return Option<bool>(400, "Could not find a valid string array of `synonyms`");
        }

        syn.synonyms.push_back(tokens);
    }

    syn.id = synonym_json["id"];
    return Option<bool>(true);
}

nlohmann::json synonym_t::to_view_json() const {
    nlohmann::json obj;
    obj["id"] = id;
    obj["root"] = raw_root;

    obj["synonyms"] = nlohmann::json::array();

    for(const auto& synonym: raw_synonyms) {
        obj["synonyms"].push_back(synonym);
    }

    if(!locale.empty()) {
        obj["locale"] = locale;
    }

    if(!symbols.empty()) {
        obj["symbols_to_index"] = nlohmann::json::array();
        for(char c: symbols) {
            obj["symbols_to_index"].push_back(std::string(1, c));
        }
    }

    return obj;
}

Option<bool> synonym_node_t::add(const synonym_t& syn) {
    if(!syn.raw_root.empty()) {
        auto& tokens = syn.root;
        auto current_node = this;
        for(size_t i = 0; i < tokens.size(); i++) {
            auto& token = tokens[i];
            synonym_node_t* child_node = current_node->children[token];

            if(child_node == nullptr) {
                child_node = new synonym_node_t();
                child_node->token = token;
                current_node->children[token] = child_node;
                art_document document(current_node->children_tree_index, current_node->children_tree_index, {0});
                art_insert(current_node->children_tree, (unsigned char *) token.c_str(), token.size() + 1, &document);
                current_node->children_tree_index++;
            }

            if(i == tokens.size() - 1) {
                child_node->terminal_synonym_ids.push_back(syn.id);
            }

            current_node = child_node;
        }
    }

    for(const auto& synonym: syn.synonyms) {
        auto& tokens = synonym;
        auto current_node = this;

        for(size_t i = 0; i < tokens.size(); i++) {
            auto& token = tokens[i];
            synonym_node_t* child_node = current_node->children[token];

            if(child_node == nullptr) {
                child_node = new synonym_node_t();
                child_node->token = token;
                current_node->children[token] = child_node;
                art_document document(current_node->children_tree_index, current_node->children_tree_index, {0});
                art_insert(current_node->children_tree, (unsigned char *) token.c_str(), token.size() + 1, &document);
                current_node->children_tree_index++;
            }

            if(i == tokens.size() - 1) {
                child_node->terminal_synonym_ids.push_back(syn.id);
            }

            current_node = child_node;
        }
    }

    return Option<bool>(true);
}

Option<bool> synonym_node_t::remove(const synonym_t& syn) {
    if(!syn.raw_root.empty()) {
        auto& tokens = syn.root;
        auto current_node = this;
        for(size_t i = 0; i < tokens.size(); i++) {
            auto& token = tokens[i];
            synonym_node_t* child_node = current_node->children[token];

            if(child_node == nullptr) {
                return Option<bool>(404, "Could not find the synonym.");
            }

            if(i == tokens.size() - 1) {
                auto it = std::find(child_node->terminal_synonym_ids.begin(), child_node->terminal_synonym_ids.end(), syn.id);
                if(it != child_node->terminal_synonym_ids.end()) {
                    child_node->terminal_synonym_ids.erase(it);
                }
            }
            

            current_node = child_node;
        }
    }

    for(const auto& synonym: syn.synonyms) {
        auto& tokens = synonym;
        auto current_node = this;

        for(size_t i = 0; i < tokens.size(); i++) {
            auto& token = tokens[i];
            synonym_node_t* child_node = current_node->children[token];

            if(child_node == nullptr) {
                return Option<bool>(404, "Could not find the synonym.");
            }

            if(i == tokens.size() - 1) {
                auto it = std::find(child_node->terminal_synonym_ids.begin(), child_node->terminal_synonym_ids.end(), syn.id);
                if(it != child_node->terminal_synonym_ids.end()) {
                    child_node->terminal_synonym_ids.erase(it);
                }
            }

            current_node = child_node;
        }
    }

    return Option<bool>(true);
}


Option<bool> synonym_node_t::get_synonyms(const std::vector<std::string>& tokens, std::vector<synonym_match_t>& synonyms, uint32_t num_typos, size_t start_index, bool synonym_prefix) const {
    return get_synonyms(tokens, synonyms, num_typos, synonym_prefix, start_index, start_index);
}


Option<bool> synonym_node_t::get_synonyms(const std::vector<std::string>& tokens, std::vector<synonym_match_t>& synonyms, uint32_t num_typos, bool synonym_prefix, size_t start_index, size_t current_index) const {
    if(current_index > tokens.size()) {
        return Option<bool>(true);
    }

    for (const auto& terminal_synonym_id : terminal_synonym_ids) {
        auto synonym = synonym_match_t();
        synonym.synonym_id = terminal_synonym_id;
        synonym.start_index = start_index;
        synonym.end_index = current_index;
        synonyms.push_back(synonym);
    }

    if(current_index == tokens.size()) {
        return Option<bool>(true);
    }

    auto matching_children = get_matching_children(tokens[current_index], num_typos, synonym_prefix);
    for (const auto& child_node : matching_children) {
        child_node->get_synonyms(tokens, synonyms, num_typos, synonym_prefix, start_index, current_index + 1);
    }

    return Option<bool>(true);
}

std::vector<synonym_node_t*> synonym_node_t::get_matching_children(const std::string& token, uint32_t num_typos, bool synonym_prefix) const {
    auto it = children.find(token);
    if(it != children.end()) {
        return {it->second};
    }


    // do fuzzy search if the token is not found
    std::vector<art_leaf*> leaves;
    std::set<std::string> exclude_leaves;
    art_fuzzy_search((art_tree*) children_tree, (unsigned char*)token.c_str(), token.size(), 0, num_typos,
                     10, FREQUENCY, synonym_prefix, false, "", nullptr, 0, leaves, exclude_leaves);
    
    std::vector<synonym_node_t*> matching_children;
    for (const auto &leaf: leaves) {
        auto child_node = children.find((char*)leaf->key);
        if (child_node != children.end()) {
            matching_children.push_back(child_node->second);
        }
    }

    return matching_children;
}

void synonym_node_t::cleanup() {
    cleanup(this, nullptr);
}

bool synonym_node_t::cleanup(synonym_node_t* node, synonym_node_t* parent) {
    if(node == nullptr) {
        return false;
    }

    std::vector<std::string> keys_to_delete;

    for(auto& child: node->children) {
        auto to_delete = cleanup(child.second, node);
        if(to_delete) {
            keys_to_delete.push_back(child.first);
        }
    }

    for(auto& key: keys_to_delete) {
        auto child = node->children.find(key);
        if(child != node->children.end()) {
            delete child->second;
            node->children.erase(child);
        }
    }

    if(node->terminal_synonym_ids.empty() && node->children.empty() && parent != nullptr) {
        return true;
    }

    return false;
}