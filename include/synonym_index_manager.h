#pragma once

#include "logger.h"
#include "synonym_index.h"
#include <unordered_map>
#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>


class SynonymIndexManager {
    public:
        static SynonymIndexManager& get_instance();

        SynonymIndexManager(const SynonymIndexManager&) = delete;
        SynonymIndexManager& operator=(const SynonymIndexManager&) = delete;
        SynonymIndexManager(SynonymIndexManager&&) = delete;
        SynonymIndexManager& operator=(SynonymIndexManager&&) = delete;

        void init_store(Store* store);

        // Add a synonym index by moving
        Option<std::shared_ptr<SynonymIndex>> add_synonym_index(const std::string& index_name, SynonymIndex&& index, bool write_to_store = true);
        // Construct from scratch with a given name
        Option<std::shared_ptr<SynonymIndex>> add_synonym_index(const std::string& index_name, bool write_to_store = true);

        Option<std::shared_ptr<SynonymIndex>> get_synonym_index(const std::string& index_name);

        Option<bool> remove_synonym_index(const std::string& index_name);

        static Option<bool> validate_synonym_index(const nlohmann::json& paylaod);

        nlohmann::json get_all_synonym_indices_json();

        Option<nlohmann::json> upsert_synonym_set(const std::string& name, const nlohmann::json& items_array);
        Option<nlohmann::json> list_synonym_items(const std::string& name, uint32_t limit, uint32_t offset);
        Option<nlohmann::json> get_synonym_item(const std::string& name, const std::string& id);
        Option<bool> upsert_synonym_item(const std::string& name, const nlohmann::json& syn_json);
        Option<bool> delete_synonym_item(const std::string& name, const std::string& id);

        static std::string get_synonym_index_key(const std::string& index_name) {
            return SYNONYM_INDEX_KEY + std::string("_") + index_name;
        }

        void load_synonym_indices();

        void dispose();
    private:
        SynonymIndexManager();
        ~SynonymIndexManager();

        static constexpr auto SYNONYM_INDEX_KEY = "$SI";

        // Guards synonym_index_map. Lookups take it shared; add/remove/dispose take it unique.
        mutable std::shared_mutex mutex;

        // The synonym indices, keyed by index name. Stored as shared_ptr so an index handed out to an
        // in-flight search stays alive even if it is concurrently replaced or removed from the map.
        std::unordered_map<std::string, std::shared_ptr<SynonymIndex>> synonym_index_map;

        Store* store = nullptr;

        nlohmann::json get_synonym_index_json(const std::string& index_name);
};