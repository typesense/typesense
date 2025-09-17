#include "override_index_manager.h"

OverrideIndexManager& OverrideIndexManager::get_instance() {
    static OverrideIndexManager instance{};
    return instance;
}

OverrideIndexManager::OverrideIndexManager() = default;
OverrideIndexManager::~OverrideIndexManager() = default;

void OverrideIndexManager::init_store(Store* store) {
    this->store = store;
    load_override_indices();
}

Option<OverrideIndex*> OverrideIndexManager::add_override_index(const std::string& index_name, OverrideIndex&& index) {
    auto res = override_index_list.insert(override_index_list.end(), std::move(index));
    if(override_index_map.find(index_name) != override_index_map.end()) {
        LOG(INFO) << "Removing existing override index: " << index_name;
        override_index_list.erase(override_index_map[index_name]);
        override_index_map.erase(index_name);
    }
    override_index_map.emplace(index_name, res);
    store->insert(OverrideIndexManager::get_override_index_key(index_name), index_name);
    return Option<OverrideIndex*>(&(*res));
}

Option<OverrideIndex*> OverrideIndexManager::add_override_index(const std::string& index_name) {
    OverrideIndex index(store, index_name);
    auto res = override_index_list.insert(override_index_list.end(), std::move(index));
    if(override_index_map.find(index_name) != override_index_map.end()) {
        override_index_list.erase(override_index_map[index_name]);
        override_index_map.erase(index_name);
        LOG(INFO) << "Removed existing override index: " << index_name;
    }
    override_index_map.emplace(index_name, res);
    store->insert(OverrideIndexManager::get_override_index_key(index_name), index_name);
    return Option<OverrideIndex*>(&(*res));
}

Option<OverrideIndex*> OverrideIndexManager::get_override_index(const std::string& index_name) {
    auto it = override_index_map.find(index_name);
    if(it != override_index_map.end()) {
        return Option<OverrideIndex*>(&(*it->second));
    }
    return Option<OverrideIndex*>(404, "Override index not found");
}

Option<bool> OverrideIndexManager::remove_override_index(const std::string& index_name) {
    auto it = override_index_map.find(index_name);
    if(it != override_index_map.end()) {
        override_index_list.erase(it->second);
        override_index_map.erase(it);
        store->remove(OverrideIndexManager::get_override_index_key(index_name));
        store->delete_range(OverrideIndex::COLLECTION_OVERRIDE_SET_PREFIX + std::string("_") + index_name + "_",
                            OverrideIndex::COLLECTION_OVERRIDE_SET_PREFIX + std::string("_") + index_name + "`");
        return Option<bool>(true);
    }
    return Option<bool>(404, "Override index not found");
}

nlohmann::json OverrideIndexManager::get_all_override_indices_json() {
    nlohmann::json result = nlohmann::json::array();
    for(const auto& pair: override_index_map) {
        result.push_back(get_override_index_json(pair.first));
    }
    return result;
}

nlohmann::json OverrideIndexManager::get_override_index_json(const std::string& index_name) {
    auto it = override_index_map.find(index_name);
    if(it != override_index_map.end()) {
        auto val = it->second->to_view_json();
        val["name"] = index_name;
        return val;
    }
    return nlohmann::json{};
}

Option<bool> OverrideIndexManager::validate_override_index(const nlohmann::json& payload) {
    if(!payload.is_object()) {
        return Option<bool>(400, "Invalid override index format");
    }
    if(!payload.contains("name") || !payload["name"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'name' field");
    }
    if(!payload.contains("items") || !payload["items"].is_array()) {
        return Option<bool>(400, "Missing or invalid 'items' field");
    }
    for(const auto& item: payload["items"]) {
        override_t ov;
        auto op = override_t::parse(item, item.value("id", std::string{}), ov);
        if(!op.ok()) {
            return Option<bool>(op.code(), op.error());
        }
    }
    return Option<bool>(true);
}

void OverrideIndexManager::load_override_indices() {
    std::vector<std::string> index_names;
    store->scan_fill(
        OverrideIndexManager::OVERRIDE_INDEX_KEY + std::string("_"),
        OverrideIndexManager::OVERRIDE_INDEX_KEY + std::string("`"),
        index_names
    );
    for(const auto& name: index_names) {
        auto add_op = add_override_index(name);
        if(!add_op.ok()) {
            LOG(ERROR) << "Failed to add override index: " << name << ", error: " << add_op.error();
            continue;
        }
        auto& index = *add_op.get();
        std::vector<std::string> overrides;
        store->scan_fill(OverrideIndex::COLLECTION_OVERRIDE_SET_PREFIX + std::string("_") + name + "_",
                         OverrideIndex::COLLECTION_OVERRIDE_SET_PREFIX + std::string("_") + name + "`",
                         overrides);
        for(const auto& override_json: overrides) {
            nlohmann::json ov_json;
            try {
                ov_json = nlohmann::json::parse(override_json);
            } catch(const nlohmann::json::parse_error& e) {
                LOG(ERROR) << "Failed to parse override JSON: " << override_json << ", error: " << e.what();
                continue;
            }
            override_t ov;
            auto parse_op = override_t::parse(ov_json, ov_json.value("id", std::string{}), ov);
            if(!parse_op.ok()) {
                LOG(ERROR) << "Failed to parse override: " << parse_op.error();
                continue;
            }
            index.add_override(ov, false);
        }
    }
}

Option<nlohmann::json> OverrideIndexManager::upsert_override_set(const std::string& name, const nlohmann::json& items_array) {
    if(!store) {
        return Option<nlohmann::json>(500, "Store not initialized.");
    }
    OverrideIndex index{store, name};
    if(!items_array.is_array()) {
        return Option<nlohmann::json>(400, "Invalid 'items' field; must be an array");
    }
    for(const auto& item: items_array) {
        override_t ov;
        auto op = override_t::parse(item, item.value("id", std::string{}), ov);
        if(!op.ok()) {
            return Option<nlohmann::json>(op.code(), op.error());
        }
        auto add_op = index.add_override(ov, true);
        if(!add_op.ok()) {
            return Option<nlohmann::json>(add_op.code(), add_op.error());
        }
    }
    auto add_index_op = add_override_index(name, std::move(index));
    if(!add_index_op.ok()) {
        return Option<nlohmann::json>(add_index_op.code(), add_index_op.error());
    }
    return Option<nlohmann::json>(add_index_op.get()->to_view_json());
}

Option<nlohmann::json> OverrideIndexManager::list_override_items(const std::string& name, uint32_t limit, uint32_t offset) {
    auto get_op = get_override_index(name);
    if(!get_op.ok()) {
        return Option<nlohmann::json>(get_op.code(), get_op.error());
    }
    auto* index = get_op.get();
    auto list_op = index->get_overrides(limit, offset);
    if(!list_op.ok()) {
        return Option<nlohmann::json>(list_op.code(), list_op.error());
    }
    nlohmann::json res = nlohmann::json::array();
    for(const auto& kv: list_op.get()) {
        res.push_back(kv.second->to_json());
    }
    return Option<nlohmann::json>(res);
}

Option<nlohmann::json> OverrideIndexManager::get_override_item(const std::string& name, const std::string& id) {
    auto get_op = get_override_index(name);
    if(!get_op.ok()) {
        return Option<nlohmann::json>(get_op.code(), get_op.error());
    }
    override_t ov;
    bool found = get_op.get()->get_override(id, ov);
    if(!found) {
        return Option<nlohmann::json>(404, "Not Found");
    }
    return Option<nlohmann::json>(ov.to_json());
}

Option<bool> OverrideIndexManager::upsert_override_item(const std::string& name, const nlohmann::json& ov_json) {
    auto get_op = get_override_index(name);
    if(!get_op.ok()) {
        return Option<bool>(get_op.code(), get_op.error());
    }
    override_t ov;
    auto parse_op = override_t::parse(ov_json, ov_json.value("id", std::string{}), ov);
    if(!parse_op.ok()) {
        return Option<bool>(parse_op.code(), parse_op.error());
    }
    return get_op.get()->add_override(ov, true);
}

Option<bool> OverrideIndexManager::delete_override_item(const std::string& name, const std::string& id) {
    auto get_op = get_override_index(name);
    if(!get_op.ok()) {
        return Option<bool>(get_op.code(), get_op.error());
    }
    return get_op.get()->remove_override(id);
}

void OverrideIndexManager::dispose() {
    override_index_list.clear();
    override_index_map.clear();
}


