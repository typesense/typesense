#include "curation_index_manager.h"

CurationIndexManager& CurationIndexManager::get_instance() {
    static CurationIndexManager instance{};
    return instance;
}

CurationIndexManager::CurationIndexManager() = default;
CurationIndexManager::~CurationIndexManager() = default;

void CurationIndexManager::init_store(Store* store) {
    this->store = store;
}

Option<CurationIndex*> CurationIndexManager::add_curation_index(const std::string& index_name, CurationIndex&& index, bool write_to_store) {
    auto res = curation_index_list.insert(curation_index_list.end(), std::move(index));
    if(curation_index_map.find(index_name) != curation_index_map.end()) {
        LOG(INFO) << "Removing existing curation index: " << index_name;
        curation_index_list.erase(curation_index_map[index_name]);
        curation_index_map.erase(index_name);
    }
    curation_index_map.emplace(index_name, res);
    if(write_to_store) {
        store->insert(CurationIndexManager::get_curation_index_key(index_name), index_name);
    }
    return Option<CurationIndex*>(&(*res));
}

Option<CurationIndex*> CurationIndexManager::add_curation_index(const std::string& index_name, bool write_to_store) {
    CurationIndex index(store, index_name);
    auto res = curation_index_list.insert(curation_index_list.end(), std::move(index));
    if(curation_index_map.find(index_name) != curation_index_map.end()) {
        curation_index_list.erase(curation_index_map[index_name]);
        curation_index_map.erase(index_name);
        LOG(INFO) << "Removed existing curation index: " << index_name;
    }
    curation_index_map.emplace(index_name, res);
    if(write_to_store) {
        store->insert(CurationIndexManager::get_curation_index_key(index_name), index_name);
    }
    return Option<CurationIndex*>(&(*res));
}

Option<CurationIndex*> CurationIndexManager::get_curation_index(const std::string& index_name) {
    auto it = curation_index_map.find(index_name);
    if(it != curation_index_map.end()) {
        return Option<CurationIndex*>(&(*it->second));
    }
    return Option<CurationIndex*>(404, "Curation index not found");
}

Option<bool> CurationIndexManager::remove_curation_index(const std::string& index_name) {
    auto it = curation_index_map.find(index_name);
    if(it != curation_index_map.end()) {
        curation_index_list.erase(it->second);
        curation_index_map.erase(it);
        store->remove(CurationIndexManager::get_curation_index_key(index_name));
        store->delete_range(CurationIndex::COLLECTION_CURATION_SET_PREFIX + std::string("_") + index_name + "_",
                            CurationIndex::COLLECTION_CURATION_SET_PREFIX + std::string("_") + index_name + "`");
        return Option<bool>(true);
    }
    return Option<bool>(404, "Curation index not found");
}

nlohmann::json CurationIndexManager::get_all_curation_indices_json() {
    nlohmann::json result = nlohmann::json::array();
    for(const auto& pair: curation_index_map) {
        result.push_back(get_curation_index_json(pair.first));
    }
    return result;
}

nlohmann::json CurationIndexManager::get_curation_index_json(const std::string& index_name) {
    auto it = curation_index_map.find(index_name);
    if(it != curation_index_map.end()) {
        auto val = it->second->to_view_json();
        val["name"] = index_name;
        return val;
    }
    return nlohmann::json{};
}

Option<bool> CurationIndexManager::validate_curation_index(const nlohmann::json& payload) {
    if(!payload.is_object()) {
        return Option<bool>(400, "Invalid curation index format");
    }
    if(!payload.contains("name") || !payload["name"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'name' field");
    }
    if(!payload.contains("items") || !payload["items"].is_array()) {
        return Option<bool>(400, "Missing or invalid 'items' field");
    }
    for(const auto& item: payload["items"]) {
        curation_t ov;
        auto op = curation_t::parse(item, item.value("id", std::string{}), ov);
        if(!op.ok()) {
            return Option<bool>(op.code(), op.error());
        }
    }
    return Option<bool>(true);
}

void CurationIndexManager::load_curation_indices() {
    std::vector<std::string> index_names;
    if (!store) {
        LOG(ERROR) << "Store not initialized for loading curation indices.";
        return;
    }
    store->scan_fill(
        CurationIndexManager::OVERRIDE_INDEX_KEY + std::string("_"),
        CurationIndexManager::OVERRIDE_INDEX_KEY + std::string("`"),
        index_names
    );
    for(const auto& name: index_names) {
        auto add_op = add_curation_index(name, false);
        if(!add_op.ok()) {
            LOG(ERROR) << "Failed to add curation index: " << name << ", error: " << add_op.error();
            continue;
        }
        auto& index = *add_op.get();
        std::vector<std::string> curations;
        store->scan_fill(CurationIndex::COLLECTION_CURATION_SET_PREFIX + std::string("_") + name + "_",
                         CurationIndex::COLLECTION_CURATION_SET_PREFIX + std::string("_") + name + "`",
                         curations);
        for(const auto& curation_json: curations) {
            nlohmann::json ov_json;
            try {
                ov_json = nlohmann::json::parse(curation_json);
            } catch(const nlohmann::json::parse_error& e) {
                LOG(ERROR) << "Failed to parse curation JSON: " << curation_json << ", error: " << e.what();
                continue;
            }
            curation_t ov;
            auto parse_op = curation_t::parse(ov_json, ov_json.value("id", std::string{}), ov);
            if(!parse_op.ok()) {
                LOG(ERROR) << "Failed to parse curation: " << parse_op.error();
                continue;
            }
            index.add_curation(ov, false);
        }
    }
}

Option<nlohmann::json> CurationIndexManager::upsert_curation_set(const std::string& name, const nlohmann::json& items_array) {
    if(!store) {
        return Option<nlohmann::json>(500, "Store not initialized.");
    }
    CurationIndex index{store, name};
    if(!items_array.is_array()) {
        return Option<nlohmann::json>(400, "Invalid 'items' field; must be an array");
    }
    for(const auto& item: items_array) {
        curation_t ov;
        auto op = curation_t::parse(item, item.value("id", std::string{}), ov);
        if(!op.ok()) {
            return Option<nlohmann::json>(op.code(), op.error());
        }
        auto add_op = index.add_curation(ov, true);
        if(!add_op.ok()) {
            return Option<nlohmann::json>(add_op.code(), add_op.error());
        }
    }
    auto add_index_op = add_curation_index(name, std::move(index));
    if(!add_index_op.ok()) {
        return Option<nlohmann::json>(add_index_op.code(), add_index_op.error());
    }
    return Option<nlohmann::json>(add_index_op.get()->to_view_json());
}

Option<nlohmann::json> CurationIndexManager::list_curation_items(const std::string& name, uint32_t limit, uint32_t offset) {
    auto get_op = get_curation_index(name);
    if(!get_op.ok()) {
        return Option<nlohmann::json>(get_op.code(), get_op.error());
    }
    auto* index = get_op.get();
    auto list_op = index->get_curations(limit, offset);
    if(!list_op.ok()) {
        return Option<nlohmann::json>(list_op.code(), list_op.error());
    }
    nlohmann::json res = nlohmann::json::array();
    for(const auto& kv: list_op.get()) {
        res.push_back(kv.second->to_json());
    }
    return Option<nlohmann::json>(res);
}

Option<nlohmann::json> CurationIndexManager::get_curation_item(const std::string& name, const std::string& id) {
    auto get_op = get_curation_index(name);
    if(!get_op.ok()) {
        return Option<nlohmann::json>(get_op.code(), get_op.error());
    }
    curation_t ov;
    bool found = get_op.get()->get_curation(id, ov);
    if(!found) {
        return Option<nlohmann::json>(404, "Not Found");
    }
    return Option<nlohmann::json>(ov.to_json());
}

Option<bool> CurationIndexManager::upsert_curation_item(const std::string& name, const nlohmann::json& ov_json) {
    auto get_op = get_curation_index(name);
    if(!get_op.ok()) {
        return Option<bool>(get_op.code(), get_op.error());
    }
    curation_t ov;
    auto parse_op = curation_t::parse(ov_json, ov_json.value("id", std::string{}), ov);
    if(!parse_op.ok()) {
        return Option<bool>(parse_op.code(), parse_op.error());
    }
    return get_op.get()->add_curation(ov, true);
}

Option<bool> CurationIndexManager::delete_curation_item(const std::string& name, const std::string& id) {
    auto get_op = get_curation_index(name);
    if(!get_op.ok()) {
        return Option<bool>(get_op.code(), get_op.error());
    }
    return get_op.get()->remove_curation(id);
}

void CurationIndexManager::dispose() {
    curation_index_list.clear();
    curation_index_map.clear();
    this->store = nullptr;
}


