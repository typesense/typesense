#include "stemmer_manager.h"


Stemmer::Stemmer(const char * language) {
    this->stemmer = sb_stemmer_new(language, nullptr);
    this->cache = LRU::Cache<std::string, std::string>(20);
}

Stemmer::~Stemmer() {
    sb_stemmer_delete(stemmer);
}

std::string Stemmer::stem(const std::string & word) {
    std::unique_lock<std::mutex> lock(mutex);
    std::string stemmed_word;
    if (cache.contains(word)) {
        return cache.lookup(word);
    }
    auto stemmed = sb_stemmer_stem(stemmer, reinterpret_cast<const sb_symbol*>(word.c_str()), word.length());
    stemmed_word = std::string(reinterpret_cast<const char*>(stemmed));
    cache.insert(word, stemmed_word);
    return stemmed_word;
}

StemmerManager::~StemmerManager() {
    delete_all_stemmers();
}

std::shared_ptr<Stemmer> StemmerManager::get_stemmer(const std::string& language) {
    std::unique_lock<std::mutex> lock(mutex);
    // use english as default language
    const std::string language_ = language.empty() ? "english" : language;
    if (stemmers.find(language_) == stemmers.end()) {
        stemmers[language] = std::make_shared<Stemmer>(language_.c_str());
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