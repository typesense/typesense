#pragma once

#include <list>
#include <unordered_map>
#include <string>
#include "logger.h"
#include "curation_index.h"

class CurationIndexManager {
    public:
        static CurationIndexManager& get_instance();

        CurationIndexManager(const CurationIndexManager&) = delete;
        CurationIndexManager& operator=(const CurationIndexManager&) = delete;
        CurationIndexManager(CurationIndexManager&&) = delete;
        CurationIndexManager& operator=(CurationIndexManager&&) = delete;

        void init_store(Store* store);

        Option<CurationIndex*> add_curation_index(const std::string& index_name, CurationIndex&& index, bool write_to_store = true);
        Option<CurationIndex*> add_curation_index(const std::string& index_name, bool write_to_store = true);

        Option<CurationIndex*> get_curation_index(const std::string& index_name);
        Option<bool> remove_curation_index(const std::string& index_name);

        static Option<bool> validate_curation_index(const nlohmann::json& payload);

        nlohmann::json get_all_curation_indices_json();

        Option<nlohmann::json> upsert_curation_set(const std::string& name, const nlohmann::json& items_array);
        Option<nlohmann::json> list_curation_items(const std::string& name, uint32_t limit, uint32_t offset);
        Option<nlohmann::json> get_curation_item(const std::string& name, const std::string& id);
        Option<bool> upsert_curation_item(const std::string& name, const nlohmann::json& ov_json);
        Option<bool> delete_curation_item(const std::string& name, const std::string& id);

        static std::string get_curation_index_key(const std::string& index_name) {
            return OVERRIDE_INDEX_KEY + std::string("_") + index_name;
        }

        void load_curation_indices();
        void dispose();
    private:
        CurationIndexManager();
        ~CurationIndexManager();

        static constexpr auto OVERRIDE_INDEX_KEY = "$OISET";

        std::list<CurationIndex> curation_index_list;
        std::unordered_map<std::string, std::list<CurationIndex>::iterator> curation_index_map;
        Store* store = nullptr;

        nlohmann::json get_curation_index_json(const std::string& index_name);
};


