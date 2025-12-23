#include "synonym_index_manager.h"

// static
SynonymIndexManager& SynonymIndexManager::get_instance() {
    static SynonymIndexManager instance{};
    return instance;
}

SynonymIndexManager::SynonymIndexManager() = default;
SynonymIndexManager::~SynonymIndexManager() = default;

void SynonymIndexManager::init_store(Store* store) {
    this->store = store;
}

Option<SynonymIndex*> SynonymIndexManager::add_synonym_index(const std::string& index_name, SynonymIndex&& index, bool write_to_store) {
    auto res = synonym_index_list.insert(synonym_index_list.end(), std::move(index));
    if(synonym_index_map.find(index_name) != synonym_index_map.end()) {
        LOG(INFO) << "Removing existing synonym index: " << index_name;
        synonym_index_list.erase(synonym_index_map[index_name]);
        synonym_index_map.erase(index_name);
    }
    synonym_index_map.emplace(index_name, res);
    if(write_to_store) {
        store->insert(SynonymIndexManager::get_synonym_index_key(index_name), index_name);
    }
    return Option<SynonymIndex*>(&(*res));
}

Option<SynonymIndex*> SynonymIndexManager::add_synonym_index(const std::string& index_name, bool write_to_store) {
    SynonymIndex index(store, index_name);
    auto res = synonym_index_list.insert(synonym_index_list.end(), std::move(index));
    if(synonym_index_map.find(index_name) != synonym_index_map.end()) {
        synonym_index_list.erase(synonym_index_map[index_name]);
        synonym_index_map.erase(index_name);
        LOG(INFO) << "Removed existing synonym index: " << index_name;
    }
    synonym_index_map.emplace(index_name, res);
    if(write_to_store) {
        store->insert(SynonymIndexManager::get_synonym_index_key(index_name), index_name);
    }
    return Option<SynonymIndex*>(&(*res));
}

Option<SynonymIndex*> SynonymIndexManager::get_synonym_index(const std::string& index_name) {
    auto it = synonym_index_map.find(index_name);
    if (it != synonym_index_map.end()) {
        return Option<SynonymIndex*>(&(*it->second));
    }
    return Option<SynonymIndex*>(404, "Synonym index not found");
}

Option<bool> SynonymIndexManager::remove_synonym_index(const std::string& index_name) {
    auto it = synonym_index_map.find(index_name);
    if (it != synonym_index_map.end()) {
        synonym_index_list.erase(it->second);
        synonym_index_map.erase(it);
        store->remove(SynonymIndexManager::get_synonym_index_key(index_name));
        // remove all synonyms associated with this index
        store->delete_range(SynonymIndex::COLLECTION_SYNONYM_PREFIX + std::string("_") + index_name + "_",
                         SynonymIndex::COLLECTION_SYNONYM_PREFIX + std::string("_") + index_name + "`");
        return Option<bool>(true);
    }
    return Option<bool>(404, "Synonym index not found");
}

nlohmann::json SynonymIndexManager::get_all_synonym_indices_json() {
    nlohmann::json result = nlohmann::json::array();
    for (const auto& pair : synonym_index_map) {
        LOG(INFO) << "Adding synonym index: " << pair.first;
        result.push_back(get_synonym_index_json(pair.first));
    }
    return result;
}

nlohmann::json SynonymIndexManager::get_synonym_index_json(const std::string& index_name) {
    auto it = synonym_index_map.find(index_name);
    if (it != synonym_index_map.end()) {
        auto val =  it->second->to_view_json();
        val["name"] = index_name;
        return val;
    }
    return nlohmann::json{};
}

Option<bool> SynonymIndexManager::validate_synonym_index(const nlohmann::json& paylaod) {
    if (!paylaod.is_object()) {
        return Option<bool>(400, "Invalid synonym index format");
    }

    if (!paylaod.contains("name") || !paylaod["name"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'name' field");
    }

    if (!paylaod.contains("items") || !paylaod["items"].is_array()) {
        return Option<bool>(400, "Missing or invalid 'items' field");
    }

    for (const auto& synonym : paylaod["items"]) {
        synonym_t syn;
        auto syn_op = synonym_t::parse(synonym, syn);
        if (!syn_op.ok()) {
            return  syn_op;
        }
    }

    return Option<bool>(true);
}

void SynonymIndexManager::load_synonym_indices() {
    std::vector<std::string> synonym_index_names;
    if (!store) {
        LOG(ERROR) << "Store not initialized for loading synonym indices.";
        return;
    }
    store->scan_fill(
        SynonymIndexManager::SYNONYM_INDEX_KEY + std::string("_"),
        SynonymIndexManager::SYNONYM_INDEX_KEY + std::string("`"),
        synonym_index_names
    );

    for (const auto& synonym_index_name : synonym_index_names) {
        // create SynonymIndex object
        auto add_op = add_synonym_index(synonym_index_name, false);
        if (!add_op.ok()) {
            LOG(ERROR) << "Failed to add synonym index: " << synonym_index_name << ", error: " << add_op.error();
            continue;
        }
        auto& index = *add_op.get();
        // get all synonyms for this index
        std::vector<std::string> synonyms;
        store->scan_fill(SynonymIndex::COLLECTION_SYNONYM_PREFIX + std::string("_") + synonym_index_name + "_",
                         SynonymIndex::COLLECTION_SYNONYM_PREFIX + std::string("_") + synonym_index_name + "`",
                        synonyms);
        LOG(INFO) << "Loading synonyms for index: " << synonym_index_name << ", count: " << synonyms.size();
        for (const auto& synonym_json : synonyms) {
            nlohmann::json syn_json;
            try {
                syn_json = nlohmann::json::parse(synonym_json);
            } catch (const nlohmann::json::parse_error& e) {
                LOG(ERROR) << "Failed to parse synonym JSON: " << synonym_json << ", error: " << e.what();
                continue;
            }
            // Add the synonym to the index
            auto synonym = synonym_t();
            auto syn_op = synonym_t::parse(syn_json, synonym);
            if (!syn_op.ok()) {
                LOG(ERROR) << "Failed to parse synonym: " << syn_json.dump() << ", error: " << syn_op.error();
                continue;
            }
            index.add_synonym(synonym, false);
        }
    }
}

Option<nlohmann::json> SynonymIndexManager::upsert_synonym_set(const std::string& name, const nlohmann::json& items_array) {
    if(!store) {
        return Option<nlohmann::json>(500, "Store not initialized.");
    }

    SynonymIndex index{store, name};
    if(!items_array.is_array()) {
        return Option<nlohmann::json>(400, "Invalid 'items' field; must be an array");
    }

    for(const auto& synonym : items_array) {
        synonym_t synonym_entry;
        auto parse_op = synonym_t::parse(synonym, synonym_entry);
        if(!parse_op.ok()) {
            return Option<nlohmann::json>(parse_op.code(), parse_op.error());
        }
        auto add_op = index.add_synonym(synonym_entry, true);
        if(!add_op.ok()) {
            return Option<nlohmann::json>(add_op.code(), add_op.error());
        }
    }

    auto add_index_op = add_synonym_index(name, std::move(index));
    if(!add_index_op.ok()) {
        return Option<nlohmann::json>(add_index_op.code(), add_index_op.error());
    }

    return Option<nlohmann::json>(add_index_op.get()->to_view_json());
}

Option<nlohmann::json> SynonymIndexManager::list_synonym_items(const std::string& name, uint32_t limit, uint32_t offset) {
    auto get_index_op = get_synonym_index(name);
    if(!get_index_op.ok()) {
        return Option<nlohmann::json>(get_index_op.code(), get_index_op.error());
    }
    auto* index = get_index_op.get();
    auto synonyms_op = index->get_synonyms(limit, offset);
    if(!synonyms_op.ok()) {
        return Option<nlohmann::json>(synonyms_op.code(), synonyms_op.error());
    }
    nlohmann::json res_json = nlohmann::json::array();
    const auto& synonyms = synonyms_op.get();
    for(const auto & kv: synonyms) {
        res_json.push_back(kv.second->to_view_json());
    }
    return Option<nlohmann::json>(res_json);
}

Option<nlohmann::json> SynonymIndexManager::get_synonym_item(const std::string& name, const std::string& id) {
    auto get_index_op = get_synonym_index(name);
    if(!get_index_op.ok()) {
        return Option<nlohmann::json>(get_index_op.code(), get_index_op.error());
    }
    synonym_t synonym;
    bool found = get_index_op.get()->get_synonym(id, synonym);
    if(!found) {
        return Option<nlohmann::json>(404, "Not Found");
    }
    return Option<nlohmann::json>(synonym.to_view_json());
}

Option<bool> SynonymIndexManager::upsert_synonym_item(const std::string& name, const nlohmann::json& syn_json) {
    auto get_index_op = get_synonym_index(name);
    if(!get_index_op.ok()) {
        return Option<bool>(get_index_op.code(), get_index_op.error());
    }
    synonym_t synonym_entry;
    auto parse_op = synonym_t::parse(syn_json, synonym_entry);
    if(!parse_op.ok()) {
        return Option<bool>(parse_op.code(), parse_op.error());
    }
    return get_index_op.get()->add_synonym(synonym_entry, true);
}

Option<bool> SynonymIndexManager::delete_synonym_item(const std::string& name, const std::string& id) {
    auto get_index_op = get_synonym_index(name);
    if(!get_index_op.ok()) {
        return Option<bool>(get_index_op.code(), get_index_op.error());
    }
    return get_index_op.get()->remove_synonym(id);
}

void SynonymIndexManager::dispose() {
    synonym_index_list.clear();
    synonym_index_map.clear();
    this->store = nullptr;
}
