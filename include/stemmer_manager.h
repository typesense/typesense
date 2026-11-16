#pragma once

#include <unordered_map>
#include <string> 
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <libstemmer.h>
#include "lru/lru.hpp"
#include "sparsepp.h"
#include "json.hpp"
#include "store.h"


class Stemmer {
    private:
        sb_stemmer * stemmer = nullptr;
        LRU::Cache<std::string, std::string> cache;
        LRU::Cache<std::string, std::string> normalized_cache;
        uint64_t dictionary_generation = 0;
        std::mutex mutex;
        std::string dictionary_name;
    public:
        Stemmer(const char * language, const std::string& dictionary_name="");
        ~Stemmer();
        std::string stem(const std::string & word, bool normalize_dictionary = false);
};


class StemmerManager {
    private:
        std::unordered_map<std::string, std::shared_ptr<Stemmer>> stemmers;
        StemmerManager() {}
        std::mutex mutex;
        spp::sparse_hash_map<std::string, spp::sparse_hash_map<std::string, std::string>> stem_dictionaries;
        struct normalized_entry_t {
            std::string root;
            bool exact = false;
            bool ambiguous = false;
        };
        spp::sparse_hash_map<std::string, spp::sparse_hash_map<std::string, normalized_entry_t>> normalized_dictionaries;
        std::atomic<uint64_t> dictionary_generation{0};
        Store* store;

        std::string get_stemming_dictionary_key(const std::string& dictionary_name);

    public:
        static StemmerManager& get_instance() {
            static StemmerManager instance;
            return instance;
        }

        static constexpr const char* STEMMING_DICTIONARY_PREFIX = "$SD";

        StemmerManager(StemmerManager const&) = delete;

        void operator=(StemmerManager const&) = delete;

        StemmerManager(StemmerManager&&) = delete;

        void operator=(StemmerManager&&) = delete;

        ~StemmerManager();

        void init(Store* _store);

        void dispose();

        std::shared_ptr<Stemmer> get_stemmer(const std::string& language, const std::string& dictionary_name="");

        void delete_stemmer(const std::string& language);

        void delete_all_stemmers();

        const bool validate_language(const std::string& language);

        Option<bool> upsert_stemming_dictionary(const std::string& dictionary_name, const std::vector<std::string> &json_lines,
                                        bool write_to_store = true);

        bool load_stemming_dictioary(const nlohmann::json& dictionary);

        std::string get_normalized_word(const std::string& dictionary_name, const std::string& word,
                                        bool normalize_dictionary = false);

        uint64_t get_dictionary_generation() const {
            return dictionary_generation.load(std::memory_order_acquire);
        }

        void get_stemming_dictionaries(nlohmann::json& dictionaries);

        bool get_stemming_dictionary(const std::string& id, nlohmann::json& dictionary);

        Option<bool> del_stemming_dictionary(const std::string& id);

        void delete_all_stemming_dictionaries();
};
