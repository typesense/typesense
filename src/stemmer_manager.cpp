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

StemmerManager::~StemmerManager() {
    delete_all_stemmers();
    stem_dictionaries.clear();
}

std::shared_ptr<Stemmer> StemmerManager::get_stemmer(const std::string& language, const std::string& dictionary_name) {
    std::unique_lock<std::mutex> lock(mutex);
    // use english as default language
    const std::string language_ = language.empty() ? "english" : language;
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
    const std::string language_ = language.empty() ? "english" : language;
    auto stemmer = sb_stemmer_new(language_.c_str(), nullptr);
    if (stemmer == nullptr) {
        return false;
    }
    sb_stemmer_delete(stemmer);
    return true;
}

bool StemmerManager::save_words(const std::string& dictionary_name, const std::vector<std::string> &json_lines) {
    if(json_lines.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex);

    nlohmann::json json_line;
    for(const auto& line_str : json_lines) {
        try {
            json_line = nlohmann::json::parse(line_str);
        } catch(...) {
            return false;
        }

        if(!json_line.contains("word") || !json_line.contains("root")) {
            return false;
        }
        stem_dictionaries[dictionary_name].emplace(json_line["word"], json_line["root"]);
    }
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

    if(!stem_dictionaries.empty()) {
        dictionaries["dictionaries"] = nlohmann::json::array();
        for (const auto &kv: stem_dictionaries) {
                dictionaries["dictionaries"].push_back(kv.first);
        }
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

void StemmerManager::del_stemming_dictionary(const std::string &id) {
    std::lock_guard<std::mutex> lock(mutex);

    auto found = stem_dictionaries.find(id);
    if(found != stem_dictionaries.end()) {
        stem_dictionaries.erase(found);
    }
}

void StemmerManager::delete_all_stemming_dictionaries() {
    std::lock_guard<std::mutex> lock(mutex);
    stem_dictionaries.clear();
}