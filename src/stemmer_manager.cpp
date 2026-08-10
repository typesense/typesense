#include "stemmer_manager.h"


Stemmer::Stemmer(const char * language, const std::string& dictionary_name) {
    if(dictionary_name.empty()) {
        this->stemmer = sb_stemmer_new(language, nullptr);
    } else {
        this->dictionary_name = dictionary_name;
    }

    this->cache = LRU::Cache<std::string, std::string>(20);
}

Stemmer::~Stemmer() {
    if(stemmer) {
        sb_stemmer_delete(stemmer);
    }
}

std::string Stemmer::stem(const std::string & word) {
    std::unique_lock<std::mutex> lock(mutex);
    std::string stemmed_word;
    if (cache.contains(word)) {
        return cache.lookup(word);
    }

    if(dictionary_name.empty()) {
        auto stemmed = sb_stemmer_stem(stemmer, reinterpret_cast<const sb_symbol *>(word.c_str()), word.length());
        stemmed_word = std::string(reinterpret_cast<const char *>(stemmed));
    } else {
        const auto& normalized_word = StemmerManager::get_instance().get_normalized_word(dictionary_name, word);
        if(normalized_word.empty()) {
            stemmed_word = word;
        } else {
            stemmed_word = normalized_word;
        }
    }

    cache.insert(word, stemmed_word);
    return stemmed_word;
}

void StemmerManager::init(Store* _store) {
    store = _store;
}

StemmerManager::~StemmerManager() {
    delete_all_stemmers();
    stem_dictionaries.clear();
}

void StemmerManager::dispose() {
    delete_all_stemmers();
    stem_dictionaries.clear();
}

std::shared_ptr<Stemmer> StemmerManager::get_stemmer(const std::string& language, const std::string& dictionary_name) {
    std::unique_lock<std::mutex> lock(mutex);
    // use english as default language
    std::string language_ = language.empty() ? "english" : language;

    // Treat de_en the same as en
    if (language_ == "de_en") {
        language_ = "english";
    }

    // Cache key must include dictionary_name: two fields on the same locale but with
    // different (or no) stem_dictionary need distinct Stemmer instances, otherwise the
    // first field constructed for a locale silently decides the stemmer every later
    // field on that locale gets, regardless of its own dictionary_name.
    const std::string cache_key = language_ + '\x1f' + dictionary_name;

    auto it = stemmers.find(cache_key);
    if (it == stemmers.end()) {
        auto stemmer = std::make_shared<Stemmer>(language_.c_str(), dictionary_name);
        it = stemmers.emplace(cache_key, stemmer).first;
    }
    return it->second;
}

void StemmerManager::delete_stemmer(const std::string& language) {
    std::unique_lock<std::mutex> lock(mutex);
    std::string language_ = language.empty() ? "english" : language;
    if (language_ == "de_en") {
        language_ = "english";
    }

    // A language can have multiple cached stemmers (one per distinct dictionary_name,
    // see get_stemmer above), so remove every entry for this language, not just one.
    const std::string prefix = language_ + '\x1f';
    for (auto it = stemmers.begin(); it != stemmers.end();) {
        if (it->first.compare(0, prefix.size(), prefix) == 0) {
            it = stemmers.erase(it);
        } else {
            ++it;
        }
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
    
    for(const auto& line_str : json_lines) {
        try {
            json_line = nlohmann::json::parse(line_str);
        } catch(...) {
            return Option<bool>(400, "Invalid dictionary format.");
        }

        if(!json_line.contains("word") || !json_line.contains("root")) {
            return Option<bool>(400, "dictionary lines should contain `word` and `root` values.");
        }
        stem_dictionaries[dictionary_name].emplace(json_line["word"], json_line["root"]);
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

std::string StemmerManager::get_normalized_word(const std::string &dictionary_name, const std::string &word) {
    std::lock_guard<std::mutex> lock(mutex);

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
        stem_dictionaries.erase(found);

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
}

std::string StemmerManager::get_stemming_dictionary_key(const std::string &dictionary_name) {
    return std::string(STEMMING_DICTIONARY_PREFIX) + "_" + dictionary_name;
}
