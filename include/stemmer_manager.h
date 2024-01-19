#pragma once

#include <unordered_map>
#include <string> 
#include <vector>
#include <memory>
#include <mutex>
#include <libstemmer.h>
#include "lru/lru.hpp"


class Stemmer {
    private:
        sb_stemmer * stemmer = nullptr;
        LRU::Cache<std::string, std::string> cache;
        std::mutex mutex;
    public:
        Stemmer(const char * language);
        ~Stemmer();
        std::string stem(const std::string & word);
};


class StemmerManager {
    private:
        std::unordered_map<std::string, std::shared_ptr<Stemmer>> stemmers;
        StemmerManager() {}
        std::mutex mutex;
    public:
        static StemmerManager& get_instance() {
            static StemmerManager instance;
            return instance;
        }
        StemmerManager(StemmerManager const&) = delete;
        void operator=(StemmerManager const&) = delete;
        StemmerManager(StemmerManager&&) = delete;
        void operator=(StemmerManager&&) = delete;
        ~StemmerManager();
        std::shared_ptr<Stemmer> get_stemmer(const std::string& language);
        void delete_stemmer(const std::string& language);
        void delete_all_stemmers();
        const bool validate_language(const std::string& language);
};