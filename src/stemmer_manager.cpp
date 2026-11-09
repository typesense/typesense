#include "stemmer_manager.h"
#include "tokenizer.h"
#include <algorithm>


Stemmer::Stemmer(const char * language, const std::string& dictionary_name) {
    if(dictionary_name.empty()) {
        this->stemmer = sb_stemmer_new(language, nullptr);
    } else {
        this->dictionary_name = dictionary_name;
    }

    this->cache = LRU::Cache<std::string, std::string>(20);
    this->normalized_cache = LRU::Cache<std::string, std::string>(20);
}

Stemmer::~Stemmer() {
    if(stemmer) {
        sb_stemmer_delete(stemmer);
    }
}

std::string Stemmer::stem(const std::string & word, bool normalize_dictionary) {
    std::unique_lock<std::mutex> lock(mutex);
    normalize_dictionary = normalize_dictionary && !dictionary_name.empty();
    if (!dictionary_name.empty()) {
        auto generation = StemmerManager::get_instance().get_dictionary_generation();
        if (generation != dictionary_generation) {
            cache.clear();
            normalized_cache.clear();
            dictionary_generation = generation;
        }
    }
    auto& word_cache = normalize_dictionary ? normalized_cache : cache;
    if (word_cache.contains(word)) {
        return word_cache.lookup(word);
    }
    std::string stemmed_word;
    if (dictionary_name.empty()) {
        auto stemmed = sb_stemmer_stem(stemmer, reinterpret_cast<const sb_symbol *>(word.c_str()), word.length());
        stemmed_word = std::string(reinterpret_cast<const char *>(stemmed));
    } else {
        auto root = StemmerManager::get_instance().get_normalized_word(dictionary_name, word, normalize_dictionary);
        stemmed_word = root.empty() ? word : root;
    }
    word_cache.insert(word, stemmed_word);
    return stemmed_word;
}

void StemmerManager::init(Store* _store) {
    store = _store;
}

StemmerManager::~StemmerManager() {
    delete_all_stemmers();
    stem_dictionaries.clear();
    normalized_dictionaries.clear();
}

void StemmerManager::dispose() {
    delete_all_stemmers();
    std::lock_guard<std::mutex> lock(mutex);
    stem_dictionaries.clear();
    normalized_dictionaries.clear();
    dictionary_generation.fetch_add(1, std::memory_order_release);
}

std::shared_ptr<Stemmer> StemmerManager::get_stemmer(const std::string& language, const std::string& dictionary_name) {
    std::unique_lock<std::mutex> lock(mutex);
    // use english as default language
    std::string language_ = language.empty() ? "english" : language;
    
    // Treat de_en the same as en
    if (language_ == "de_en") {
        language_ = "english";
    }
    
    if (stemmers.find(language_) == stemmers.end()) {
        stemmers[language] = std::make_shared<Stemmer>(language_.c_str(), dictionary_name);
    }
    return stemmers[language];
}

void StemmerManager::delete_stemmer(const std::string& language) {
    std::unique_lock<std::mutex> lock(mutex);
    if (stemmers.find(language) != stemmers.end()) {
        stemmers.erase(language);
    }
}

void StemmerManager::delete_all_stemmers() {
    std::unique_lock<std::mutex> lock(mutex);
    stemmers.clear();
}

const bool StemmerManager::validate_language(const std::string& language) {
    std::string language_ = language.empty() ? "english" : language;

    // Treat de_en the same as en
    if (language_ == "de_en") {
        language_ = "english";
    }

    auto stemmer = sb_stemmer_new(language_.c_str(), nullptr);
    if (stemmer == nullptr) {
        return false;
    }
    sb_stemmer_delete(stemmer);
    return true;
}

Option<bool> StemmerManager::upsert_stemming_dictionary(const std::string& dictionary_name, const std::vector<std::string> &json_lines,
                                                bool write_to_store) {
    if(json_lines.empty()) {
        return Option<bool>(400, "Invalid dictionary format.");
    }

    std::lock_guard<std::mutex> lock(mutex);

    nlohmann::json json_line;
    Tokenizer normalizer("");
    decltype(stem_dictionaries)::mapped_type* original_dictionary = nullptr;
    decltype(normalized_dictionaries)::mapped_type* derived_dictionary = nullptr;
    
    for(const auto& line_str : json_lines) {
        try {
            json_line = nlohmann::json::parse(line_str);
        } catch(...) {
            return Option<bool>(400, "Invalid dictionary format.");
        }

        if(!json_line.contains("word") || !json_line.contains("root")) {
            return Option<bool>(400, "dictionary lines should contain `word` and `root` values.");
        }
        if (!original_dictionary) {
            original_dictionary = &stem_dictionaries[dictionary_name];
            if (original_dictionary->empty()) {
                original_dictionary->reserve(json_lines.size());
            }
        }
        auto& dictionary = *original_dictionary;
        auto inserted = dictionary.emplace(json_line["word"], json_line["root"]);
        if (inserted.second) {
            // Publish invalidation while holding the manager mutex. A cache miss
            // waits for the complete map update through get_normalized_word().
            dictionary_generation.fetch_add(1, std::memory_order_release);
            const auto& word = inserted.first->first;
            const auto& original_root = inserted.first->second;
            if (!derived_dictionary) {
                derived_dictionary = &normalized_dictionaries[dictionary_name];
                if (derived_dictionary->empty()) {
                    derived_dictionary->reserve(json_lines.size());
                }
            }
            auto& normalized_dictionary = *derived_dictionary;
            const auto unchanged_ascii = [](const std::string& value) {
                const auto* p = reinterpret_cast<const unsigned char*>(value.data());
                const auto* end = p + value.size();
                for (; p != end; ++p) {
                    if (*p >= 128 || (*p >= 'A' && *p <= 'Z')) return false;
                }
                return true;
            };
            if (unchanged_ascii(word) && unchanged_ascii(original_root)) {
                if (!word.empty()) {
                    // Skip normalization allocations while retaining a single-map
                    // normalized lookup and exact-spelling collision precedence.
                    normalized_dictionary[word] = normalized_entry_t{original_root, true, false};
                }
                continue;
            }
            auto key = normalizer.normalize_stemming_dictionary_word(word);
            auto root = normalizer.normalize_stemming_dictionary_word(original_root);
            if (!key.empty()) {
                const bool exact = key == word;
                if (!exact && dictionary.count(key) != 0) {
                    // An original exact spelling wins over every folded alias.
                    continue;
                }
                auto normalized = normalized_dictionary.emplace(
                    key, normalized_entry_t{root, exact, false});
                if (!normalized.second) {
                    auto& entry = normalized.first->second;
                    if (exact) {
                        // Preserve an existing exact spelling over folded aliases.
                        entry = normalized_entry_t{root, true, false};
                    } else if (!entry.exact && entry.root != root) {
                        entry.ambiguous = true;
                    }
                }
            }
        }
    }

    if(write_to_store) {
        nlohmann::json dictionary_json;
        dictionary_json["id"] = dictionary_name;
        dictionary_json["words"] = nlohmann::json::array();

        for(const auto& kv : stem_dictionaries[dictionary_name]) {
            nlohmann::json line;
            line["word"] = kv.first;
            line["root"] = kv.second;
            dictionary_json["words"].push_back(line);
        }

        bool inserted = store->insert(get_stemming_dictionary_key(dictionary_name), dictionary_json.dump());
        if (!inserted) {
            return Option<bool>(500, "Unable to insert into store.");
        }
    }

    return Option<bool>(true);
}

bool StemmerManager::load_stemming_dictioary(const nlohmann::json &dictionary_json) {
    const auto& dictionary_name = dictionary_json["id"];
    std::vector<std::string> json_lines;

    for(const auto& line : dictionary_json["words"]) {
        json_lines.push_back(line.dump());
    }

    upsert_stemming_dictionary(dictionary_name, json_lines, false);

    return true;
}

std::string StemmerManager::get_normalized_word(const std::string &dictionary_name, const std::string &word,
                                              bool normalize_dictionary) {
    std::lock_guard<std::mutex> lock(mutex);

    if (normalize_dictionary) {
        if (word.empty()) {
            return "";
        }
        auto dictionary = normalized_dictionaries.find(dictionary_name);
        if (dictionary == normalized_dictionaries.end()) {
            return "";
        }
        auto found = dictionary->second.find(word);
        return found == dictionary->second.end() || found->second.ambiguous ? "" : found->second.root;
    }

    std::string normalized_word;

    auto stem_dictionaries_it = stem_dictionaries.find(dictionary_name);
    if(stem_dictionaries_it != stem_dictionaries.end()) {
        const auto& dictionary = stem_dictionaries_it->second;
        auto found = dictionary.find(word);

        if(found != dictionary.end()) {
            normalized_word = found->second;
        }
    }

    return normalized_word;
}

void StemmerManager::get_stemming_dictionaries(nlohmann::json &dictionaries) {
    std::lock_guard<std::mutex> lock(mutex);

    dictionaries["dictionaries"] = nlohmann::json::array();
    for (const auto &kv: stem_dictionaries) {
        dictionaries["dictionaries"].push_back(kv.first);
    }
}

bool StemmerManager::get_stemming_dictionary(const std::string &id, nlohmann::json &dictionary) {
    std::lock_guard<std::mutex> lock(mutex);

    auto found = stem_dictionaries.find(id);
    if(found != stem_dictionaries.end()) {

        dictionary["id"] = id;
        dictionary["words"] = nlohmann::json::array();

        for (const auto &kv: found->second) {
            nlohmann::json line;
            line["word"] = kv.first;
            line["root"] = kv.second;
            dictionary["words"].push_back(line);
        }

        return true;
    }

    return false;
}

Option<bool> StemmerManager::del_stemming_dictionary(const std::string &id) {
    std::lock_guard<std::mutex> lock(mutex);

    auto found = stem_dictionaries.find(id);
    if(found != stem_dictionaries.end()) {
        dictionary_generation.fetch_add(1, std::memory_order_release);
        stem_dictionaries.erase(found);
        normalized_dictionaries.erase(id);

        bool removed = store->remove(get_stemming_dictionary_key(id));
        if(!removed) {
            return Option<bool>(500, "Unable to delete from store.");
        }
    }

    return Option<bool>(true);
}

void StemmerManager::delete_all_stemming_dictionaries() {
    std::lock_guard<std::mutex> lock(mutex);
    for(const auto& kv : stem_dictionaries) {
        store->remove(get_stemming_dictionary_key(kv.first));
    }
    stem_dictionaries.clear();
    normalized_dictionaries.clear();
    dictionary_generation.fetch_add(1, std::memory_order_release);
}

std::string StemmerManager::get_stemming_dictionary_key(const std::string &dictionary_name) {
    return std::string(STEMMING_DICTIONARY_PREFIX) + "_" + dictionary_name;
}
