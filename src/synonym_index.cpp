#include "synonym_index.h"


void SynonymIndex::synonym_reduction_internal(const std::vector<std::string>& tokens,
                                            size_t start_window_size, size_t start_index_pos,
                                            std::set<uint64_t>& processed_syn_hashes,
                                            std::vector<std::vector<std::string>>& results) const {

    bool recursed = false;

    for(size_t window_len = start_window_size; window_len > 0; window_len--) {
        for(size_t start_index = start_index_pos; start_index+window_len-1 < tokens.size(); start_index++) {
            std::vector<uint64_t> syn_hashes;
            uint64_t syn_hash = 1;

            for(size_t i = start_index; i < start_index+window_len; i++) {
                uint64_t token_hash = StringUtils::hash_wy(tokens[i].c_str(), tokens[i].size());

                if(i == start_index) {
                    syn_hash = token_hash;
                } else {
                    syn_hash = StringUtils::hash_combine(syn_hash, token_hash);
                }

                syn_hashes.push_back(token_hash);
            }

            const auto& syn_itr = synonym_index.find(syn_hash);

            if(syn_itr != synonym_index.end() && processed_syn_hashes.count(syn_hash) == 0) {
                // tokens in this window match a synonym: reconstruct tokens and rerun synonym mapping against matches
                const auto& syn_ids = syn_itr->second;

                for(const auto& syn_id: syn_ids) {
                    const auto &syn_def = synonym_definitions.at(syn_id);

                    for (const auto &syn_def_tokens: syn_def.synonyms) {
                        std::vector<std::string> new_tokens;

                        for (size_t i = 0; i < start_index; i++) {
                            new_tokens.push_back(tokens[i]);
                        }

                        std::vector<uint64_t> syn_def_hashes;
                        uint64_t syn_def_hash = 1;

                        for (size_t i = 0; i < syn_def_tokens.size(); i++) {
                            const auto &syn_def_token = syn_def_tokens[i];
                            new_tokens.push_back(syn_def_token);
                            uint64_t token_hash = StringUtils::hash_wy(syn_def_token.c_str(),
                                                                       syn_def_token.size());

                            if (i == 0) {
                                syn_def_hash = token_hash;
                            } else {
                                syn_def_hash = StringUtils::hash_combine(syn_def_hash, token_hash);
                            }

                            syn_def_hashes.push_back(token_hash);
                        }

                        if (syn_def_hash == syn_hash) {
                            // skip over token matching itself in the group
                            continue;
                        }

                        for (size_t i = start_index + window_len; i < tokens.size(); i++) {
                            new_tokens.push_back(tokens[i]);
                        }

                        processed_syn_hashes.emplace(syn_def_hash);
                        processed_syn_hashes.emplace(syn_hash);

                        for (uint64_t h: syn_def_hashes) {
                            processed_syn_hashes.emplace(h);
                        }

                        for (uint64_t h: syn_hashes) {
                            processed_syn_hashes.emplace(h);
                        }

                        recursed = true;
                        synonym_reduction_internal(new_tokens, window_len, start_index, processed_syn_hashes, results);
                    }
                }
            }
        }

        // reset it because for the next window we have to start from scratch
        start_index_pos = 0;
    }

    if(!recursed && !processed_syn_hashes.empty()) {
        results.emplace_back(tokens);
    }
}

void SynonymIndex::synonym_reduction(const std::vector<std::string>& tokens,
                                   std::vector<std::vector<std::string>>& results) const {
    if(synonym_definitions.empty()) {
        return;
    }

    std::set<uint64_t> processed_syn_hashes;
    synonym_reduction_internal(tokens, tokens.size(), 0, processed_syn_hashes, results);
}

Option<bool> SynonymIndex::add_synonym(const std::string & collection_name, const synonym_t& synonym) {
    if(synonym_definitions.count(synonym.id) != 0) {
        // first we have to delete existing entries so we can upsert
        Option<bool> rem_op = remove_synonym(collection_name, synonym.id);
        if(!rem_op.ok()) {
            return rem_op;
        }
    }

    std::unique_lock write_lock(mutex);
    synonym_definitions[synonym.id] = synonym;

    if(!synonym.root.empty()) {
        uint64_t root_hash = synonym_t::get_hash(synonym.root);
        synonym_index[root_hash].emplace_back(synonym.id);
    } else {
        for(const auto & syn_tokens : synonym.synonyms) {
            uint64_t syn_hash = synonym_t::get_hash(syn_tokens);
            synonym_index[syn_hash].emplace_back(synonym.id);
        }
    }

    write_lock.unlock();

    bool inserted = store->insert(get_synonym_key(collection_name, synonym.id), synonym.to_view_json().dump());
    if(!inserted) {
        return Option<bool>(500, "Error while storing the synonym on disk.");
    }

    return Option<bool>(true);
}

bool SynonymIndex::get_synonym(const std::string& id, synonym_t& synonym) {
    std::shared_lock lock(mutex);

    if(synonym_definitions.count(id) != 0) {
        synonym = synonym_definitions[id];
        return true;
    }

    return false;
}

Option<bool> SynonymIndex::remove_synonym(const std::string & collection_name, const std::string &id) {
    std::unique_lock lock(mutex);
    const auto& syn_iter = synonym_definitions.find(id);

    if(syn_iter != synonym_definitions.end()) {
        bool removed = store->remove(get_synonym_key(collection_name, id));
        if(!removed) {
            return Option<bool>(500, "Error while deleting the synonym from disk.");
        }

        const auto& synonym = syn_iter->second;
        if(!synonym.root.empty()) {
            uint64_t root_hash = synonym_t::get_hash(synonym.root);
            synonym_index.erase(root_hash);
        } else {
            for(const auto & syn_tokens : synonym.synonyms) {
                uint64_t syn_hash = synonym_t::get_hash(syn_tokens);
                synonym_index.erase(syn_hash);
            }
        }

        synonym_definitions.erase(id);
        return Option<bool>(true);
    }

    return Option<bool>(404, "Could not find that `id`.");
}

spp::sparse_hash_map<std::string, synonym_t> SynonymIndex::get_synonyms() {
    std::shared_lock lock(mutex);
    return synonym_definitions;
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

