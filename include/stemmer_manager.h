#pragma once

#include <unordered_map>
#include <string> 
#include <vector>
#include <memory>
#include <mutex>
#include <libstemmer.h>
#include "lru/lru.hpp"
#include "sparsepp.h"
#include "json.hpp"


class Stemmer {
    private:
        sb_stemmer * stemmer = nullptr;
        LRU::Cache<std::string, std::string> cache;
        std::mutex mutex;
        bool use_dictionary = false;
    public:
        Stemmer(const char * language, bool use_dictionary = false);
        ~Stemmer();
        std::string stem(const std::string & word);
};


class StemmerManager {
    private:
        std::unordered_map<std::string, std::shared_ptr<Stemmer>> stemmers;
        StemmerManager() {}
        std::mutex mutex;
        spp::sparse_hash_map<std::string, std::string> stem_dictionary;
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
        std::shared_ptr<Stemmer> get_stemmer(const std::string& language, bool stem_dictionary = false);
        void delete_stemmer(const std::string& language);
        void delete_all_stemmers();
        const bool validate_language(const std::string& language);
        bool dump_dictionary(const std::vector<std::string> &json_lines);
        spp::sparse_hash_map<std::string, std::string> get_dictionary();
};