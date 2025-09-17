#pragma once

#include <list>
#include <unordered_map>
#include <string>
#include "logger.h"
#include "override_index.h"

class OverrideIndexManager {
    public:
        static OverrideIndexManager& get_instance();

        OverrideIndexManager(const OverrideIndexManager&) = delete;
        OverrideIndexManager& operator=(const OverrideIndexManager&) = delete;
        OverrideIndexManager(OverrideIndexManager&&) = delete;
        OverrideIndexManager& operator=(OverrideIndexManager&&) = delete;

        void init_store(Store* store);

        Option<OverrideIndex*> add_override_index(const std::string& index_name, OverrideIndex&& index);
        Option<OverrideIndex*> add_override_index(const std::string& index_name);

        Option<OverrideIndex*> get_override_index(const std::string& index_name);
        Option<bool> remove_override_index(const std::string& index_name);

        static Option<bool> validate_override_index(const nlohmann::json& payload);

        nlohmann::json get_all_override_indices_json();

        Option<nlohmann::json> upsert_override_set(const std::string& name, const nlohmann::json& items_array);
        Option<nlohmann::json> list_override_items(const std::string& name, uint32_t limit, uint32_t offset);
        Option<nlohmann::json> get_override_item(const std::string& name, const std::string& id);
        Option<bool> upsert_override_item(const std::string& name, const nlohmann::json& ov_json);
        Option<bool> delete_override_item(const std::string& name, const std::string& id);

        static std::string get_override_index_key(const std::string& index_name) {
            return OVERRIDE_INDEX_KEY + std::string("_") + index_name;
        }

        void load_override_indices();
        void dispose();
    private:
        OverrideIndexManager();
        ~OverrideIndexManager();

        static constexpr auto OVERRIDE_INDEX_KEY = "$OISET";

        std::list<OverrideIndex> override_index_list;
        std::unordered_map<std::string, std::list<OverrideIndex>::iterator> override_index_map;
        Store* store = nullptr;

        nlohmann::json get_override_index_json(const std::string& index_name);
};


