#include "stop_word_index.h"


//void StopWordIndex::stop_word_reduction_internal(const std::vector<std::string>& tokens,
//                                            size_t start_window_size, size_t start_index_pos,
//                                            std::set<uint64_t>& processed_stpwrd_hashes,
//                                            std::vector<std::vector<std::string>>& results) const {
//
//    bool recursed = false;
//
//    for(size_t window_len = start_window_size; window_len > 0; window_len--) {
//        for(size_t start_index = start_index_pos; start_index+window_len-1 < tokens.size(); start_index++) {
//            std::vector<uint64_t> stpwrd_hashes;
//            uint64_t stpwrd_hash = 1;
//
//            for(size_t i = start_index; i < start_index+window_len; i++) {
//                uint64_t token_hash = StringUtils::hash_wy(tokens[i].c_str(), tokens[i].size());
//
//                if(i == start_index) {
//                    stpwrd_hash = token_hash;
//                } else {
//                    stpwrd_hash = StringUtils::hash_combine(stpwrd_hash, token_hash);
//                }
//
//                stpwrd_hashes.push_back(token_hash);
//            }
//
//            const auto& stpwrd_itr = stop_word_index.find(stpwrd_hash);
//
//            if(stpwrd_itr != stop_word_index.end() && processed_stpwrd_hashes.count(stpwrd_hash) == 0) {
//                // tokens in this window match a stop_word: reconstruct tokens and rerun stop_word mapping against matches
//                const auto& stpwrd_ids = stpwrd_itr->second;
//
//                for(const auto& stpwrd_id: stpwrd_ids) {
//                    const auto &stpwrd_def = stop_word_definitions.at(stpwrd_id);
//
//                    for (const auto &stpwrd_def_tokens: stpwrd_def.stop_words) {
//                        std::vector<std::string> new_tokens;
//
//                        for (size_t i = 0; i < start_index; i++) {
//                            new_tokens.push_back(tokens[i]);
//                        }
//
//                        std::vector<uint64_t> stpwrd_def_hashes;
//                        uint64_t stpwrd_def_hash = 1;
//
//                        for (size_t i = 0; i < stpwrd_def_tokens.size(); i++) {
//                            const auto &stpwrd_def_token = stpwrd_def_tokens[i];
//                            new_tokens.push_back(stpwrd_def_token);
//                            uint64_t token_hash = StringUtils::hash_wy(stpwrd_def_token.c_str(),
//                                                                       stpwrd_def_token.size());
//
//                            if (i == 0) {
//                                stpwrd_def_hash = token_hash;
//                            } else {
//                                stpwrd_def_hash = StringUtils::hash_combine(stpwrd_def_hash, token_hash);
//                            }
//
//                            stpwrd_def_hashes.push_back(token_hash);
//                        }
//
//                        if (stpwrd_def_hash == stpwrd_hash) {
//                            // skip over token matching itself in the group
//                            continue;
//                        }
//
//                        for (size_t i = start_index + window_len; i < tokens.size(); i++) {
//                            new_tokens.push_back(tokens[i]);
//                        }
//
//                        processed_stpwrd_hashes.emplace(stpwrd_def_hash);
//                        processed_stpwrd_hashes.emplace(stpwrd_hash);
//
//                        for (uint64_t h: stpwrd_def_hashes) {
//                            processed_stpwrd_hashes.emplace(h);
//                        }
//
//                        for (uint64_t h: stpwrd_hashes) {
//                            processed_stpwrd_hashes.emplace(h);
//                        }
//
//                        recursed = true;
//                        stop_word_reduction_internal(new_tokens, window_len, start_index, processed_stpwrd_hashes, results);
//                    }
//                }
//            }
//        }
//
//        // reset it because for the next window we have to start from scratch
//        start_index_pos = 0;
//    }
//
//    if(!recursed && !processed_stpwrd_hashes.empty()) {
//        results.emplace_back(tokens);
//    }
//}

void StopWordIndex::stop_word_reduction(const std::vector<std::string>& tokens,
                                   std::vector<std::vector<std::string>>& results) const {
    return;
//    if(stop_word_definitions.empty()) {
//        return;
//    }
//
//    std::set<uint64_t> processed_stpwrd_hashes;
//    stop_word_reduction_internal(tokens, tokens.size(), 0, processed_stpwrd_hashes, results);
}

Option<bool> StopWordIndex::add_stop_word(const std::string & collection_name, const stop_word_t& stop_word) {
    if(stop_word_definitions.count(stop_word.id) != 0) {
        // first we have to delete existing entries so we can upsert
        Option<bool> rem_op = remove_stop_word(collection_name, stop_word.id);
        if(!rem_op.ok()) {
            return rem_op;
        }
    }

    std::unique_lock write_lock(mutex);
    stop_word_definitions[stop_word.id] = stop_word;

    uint64_t word_hash = stop_word_t::get_hash(stop_word.word);
    stop_word_index[word_hash].emplace_back(stop_word.id);
    
    write_lock.unlock();

    bool inserted = store->insert(get_stop_word_key(collection_name, stop_word.id), stop_word.to_view_json().dump());
    if(!inserted) {
        return Option<bool>(500, "Error while storing the stop word on disk.");
    }

    return Option<bool>(true);
}

bool StopWordIndex::get_stop_word(const std::string& id, stop_word_t& stop_word) {
    std::shared_lock lock(mutex);

    if(stop_word_definitions.count(id) != 0) {
        stop_word = stop_word_definitions[id];
        return true;
    }

    return false;
}

Option<bool> StopWordIndex::remove_stop_word(const std::string & collection_name, const std::string &id) {
    std::unique_lock lock(mutex);
    const auto& stpwrd_iter = stop_word_definitions.find(id);

    if(stpwrd_iter != stop_word_definitions.end()) {
        bool removed = store->remove(get_stop_word_key(collection_name, id));
        if(!removed) {
            return Option<bool>(500, "Error while deleting the stop word from disk.");
        }

        const auto& stop_word = stpwrd_iter->second;

        uint64_t word_hash = stop_word_t::get_hash(stop_word.word);
        stop_word_index.erase(word_hash);

        stop_word_definitions.erase(id);
        return Option<bool>(true);
    }

    return Option<bool>(404, "Could not find that `id`.");
}

spp::sparse_hash_map<std::string, stop_word_t> StopWordIndex::get_stop_words() {
    std::shared_lock lock(mutex);
    return stop_word_definitions;
}

std::string StopWordIndex::get_stop_word_key(const std::string & collection_name, const std::string & stop_word_id) {
    return std::string(COLLECTION_stop_word_PREFIX) + "_" + collection_name + "_" + stop_word_id;
}

Option<bool> stop_word_t::parse(const nlohmann::json& stop_word_json, stop_word_t& stpwrd) {
    if(stop_word_json.count("id") == 0) {
        return Option<bool>(400, "Missing `id` field.");
    }

    if(stop_word_json.count("word") == 0) {
        return Option<bool>(400, "Could not find key named `word` in the payload.");
    }
    
    if(!stop_word_json["word"].is_string()) {
        return Option<bool>(400, "Could not find key named `word` that is a string.");
    }

    if(stop_word_json.count("locale") != 0) {
        if(!stop_word_json["locale"].is_string()) {
            return Option<bool>(400, "Key `locale` should be a string.`");
        }

        stpwrd.locale = stop_word_json["locale"].get<std::string>();
    }

    if(stop_word_json.count("symbols_to_index") != 0) {
        if(!stop_word_json["symbols_to_index"].is_array() || stop_word_json["symbols_to_index"].empty() ||
            !stop_word_json["symbols_to_index"][0].is_string()) {
            return Option<bool>(400, "Stop word `symbols_to_index` should be an array of strings.");
        }

        auto symbols = stop_word_json["symbols_to_index"].get<std::vector<std::string>>();
        for(auto symbol: symbols) {
            if(symbol.size() != 1) {
                return Option<bool>(400, "Stop word `symbols_to_index` should be an array of single character symbols.");
            }

            stpwrd.symbols.push_back(symbol[0]);
        }
    }

    if(stop_word_json.count("word") != 0) {
        std::vector<std::string> tokens;
        Tokenizer(stop_word_json["word"], true, false, stpwrd.locale, stpwrd.symbols).tokenize(tokens);
        stpwrd.word = tokens;
    }

    stpwrd.id = stop_word_json["id"];
    return Option<bool>(true);
}

nlohmann::json stop_word_t::to_view_json() const {
    nlohmann::json obj;
    obj["id"] = id;
    obj["word"] = StringUtils::join(word, " ");

    if(!locale.empty()) {
        obj["locale"] = locale;
    }

    if(!symbols.empty()) {
        obj["symbols_to_index"] = symbols;
    }

    return obj;
}

