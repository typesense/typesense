#pragma once

#include "logger.h"
#include "synonym_index.h"
#include <unordered_map>
#include <string>


class SynonymIndexManager {
    public:
    
        static SynonymIndexManager& get_instance();

        SynonymIndexManager(const SynonymIndexManager&) = delete;
        SynonymIndexManager& operator=(const SynonymIndexManager&) = delete;
        SynonymIndexManager(SynonymIndexManager&&) = delete;
        SynonymIndexManager& operator=(SynonymIndexManager&&) = delete;

        void init_store(Store* store);

        // Add a synonym index by moving
        Option<SynonymIndex*> add_synonym_index(const std::string& index_name, SynonymIndex&& index);
        // Construct from scratch with a given name
        Option<SynonymIndex*> add_synonym_index(const std::string& index_name);

        Option<SynonymIndex*> get_synonym_index(const std::string& index_name);

        Option<bool> remove_synonym_index(const std::string& index_name);

        static Option<bool> validate_synonym_index(const nlohmann::json& paylaod);

        nlohmann::json get_all_synonym_indices_json();

        static std::string get_synonym_index_key(const std::string& index_name) {
            return SYNONYM_INDEX_KEY + std::string("_") + index_name;
        }

        void load_synonym_indices();
    private:
        SynonymIndexManager();
        ~SynonymIndexManager();

        static constexpr auto SYNONYM_INDEX_KEY = "$SI";



        // The synonym index
        std::list<SynonymIndex> synonym_index_list;
        std::unordered_map<std::string, std::list<SynonymIndex>::iterator> synonym_index_map;

        Store* store;

        nlohmann::json get_synonym_index_json(const std::string& index_name);
};