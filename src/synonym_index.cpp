#include "synonym_index.h"
#include "posting.h"

void SynonymIndex::synonym_reduction_internal(const std::vector<std::string>& tokens,
                                            size_t start_window_size, size_t start_index_pos,
                                            std::set<std::string>& processed_tokens,
                                            std::vector<std::vector<std::string>>& results,
                                            const std::vector<std::string>& orig_tokens,
                                            bool synonym_prefix, uint32_t synonym_num_typos) const {

    bool recursed = false;

    for(size_t window_len = start_window_size; window_len > 0; window_len--) {
        for(size_t start_index = start_index_pos; start_index+window_len-1 < tokens.size(); start_index++) {
            std::string merged_tokens_str="";
            for(size_t i = start_index; i < start_index+window_len; i++) {
                merged_tokens_str += tokens[i];
                merged_tokens_str += " ";
            }
            StringUtils::trim(merged_tokens_str);

            std::vector<art_leaf*> leaves;
            std::set<std::string> exclude_leaves;
            auto merged_tokens_len = strlen(merged_tokens_str.c_str());
            merged_tokens_len = synonym_prefix ? merged_tokens_len : merged_tokens_len + 1;

            art_fuzzy_search(synonym_index_tree, (unsigned char*)merged_tokens_str.c_str(), merged_tokens_len, 0, synonym_num_typos,
                             10, FREQUENCY, synonym_prefix, false, "", nullptr, 0, leaves, exclude_leaves);

            if(processed_tokens.count(merged_tokens_str) == 0) {
                // tokens in this window match a synonym: reconstruct tokens and rerun synonym mapping against matches
                for (const auto &leaf: leaves) {
                    std::vector<posting_list_t*> expanded_plists;
                    posting_list_t::iterator_t it(nullptr, nullptr, nullptr);

                    if(IS_COMPACT_POSTING(leaf->values)) {
                        auto compact_posting_list = COMPACT_POSTING_PTR(leaf->values);
                        posting_list_t* full_posting_list = compact_posting_list->to_full_posting_list();
                        expanded_plists.push_back(full_posting_list);
                        it = full_posting_list->new_iterator(nullptr, nullptr, 0);
                    } else {
                        posting_list_t* full_posting_list = (posting_list_t*)(leaf->values);
                        it = full_posting_list->new_iterator(nullptr, nullptr, 0);
                    }

                    while(it.valid()) {
                        auto syn_index = it.id();
                        const auto &syn_def = synonym_definitions.at(syn_index);

                        for (const auto &syn_def_tokens: syn_def.synonyms) {
                            std::vector<std::string> new_tokens;

                            for (size_t i = 0; i < start_index; i++) {
                                new_tokens.push_back(tokens[i]);
                            }

                            for (size_t i = 0; i < syn_def_tokens.size(); i++) {
                                const auto &syn_def_token = syn_def_tokens[i];
                                new_tokens.push_back(syn_def_token);
                                processed_tokens.emplace(syn_def_token);
                            }

                            for (size_t i = start_index + window_len; i < tokens.size(); i++) {
                                new_tokens.push_back(tokens[i]);
                            }

                            processed_tokens.emplace(merged_tokens_str);
                            auto syn_def_tokens_str = StringUtils::join(syn_def_tokens, " ");
                            processed_tokens.emplace(syn_def_tokens_str);

                            recursed = true;
                            synonym_reduction_internal(new_tokens, window_len,
                                                       start_index, processed_tokens, results, orig_tokens,
                                                       synonym_prefix, synonym_num_typos);
                        }

                        it.next();
                    }

                    for(posting_list_t* plist: expanded_plists) {
                        delete plist;
                    }
                }
            }
        }

        // reset it because for the next window we have to start from scratch
        start_index_pos = 0;
    }

    if(!recursed && !processed_tokens.empty() && tokens != orig_tokens) {
        results.emplace_back(tokens);
    }
}

void SynonymIndex::synonym_reduction(const std::vector<std::string>& tokens,
                                     std::vector<std::vector<std::string>>& results,
                                     bool synonym_prefix, uint32_t synonym_num_typos) const {
    std::shared_lock lock(mutex);
    if(synonym_definitions.empty()) {
        return;
    }

    std::set<std::string> processed_tokens;
    synonym_reduction_internal(tokens, tokens.size(), 0, processed_tokens, results, tokens,
                               synonym_prefix, synonym_num_typos);
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

    for(const auto& key : keys) {
        art_leaf* exact_leaf = (art_leaf *) art_search(synonym_index_tree, (unsigned char *) key.c_str(), key.size() + 1);
        if(exact_leaf) {
            auto offset = posting_t::num_ids(exact_leaf->values);
            posting_t::upsert(exact_leaf->values, synonym_index, {offset});
        } else {
            art_document document(synonym_index, synonym_index, {0});
            art_insert(synonym_index_tree, (unsigned char *) key.c_str(), key.size() + 1, &document);
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
        keys.insert(keys.end(), synonym.root.begin(), synonym.root.end());
        keys.insert(keys.end(), synonym.raw_synonyms.begin(), synonym.raw_synonyms.end());

        for(const auto& key : keys) {
            art_leaf* found_leaf = (art_leaf *) art_search(synonym_index_tree, (unsigned char *) key.c_str(), key.size() + 1);
            if(found_leaf) {
                auto index = syn_iter->second;
                posting_t::erase(found_leaf->values, index);
                if(posting_t::num_ids(found_leaf->values) == 0) {
                    art_delete(synonym_index_tree, (unsigned char*)key.c_str(), key.size() + 1);
                }
            }
        }

        auto index = synonym_ids_index_map.at(id);
        synonym_ids_index_map.erase(id);
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

    for (synonym_it; synonym_it != synonym_end; ++synonym_it) {
        synonyms_map[synonym_it->first] = &synonym_it->second;
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

