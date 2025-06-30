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
    load_synonym_indices();
}

Option<SynonymIndex*> SynonymIndexManager::add_synonym_index(const std::string& index_name, SynonymIndex&& index) {
    if(synonym_index_map.find(index_name) != synonym_index_map.end()) {
        auto remove_op = remove_synonym_index(index_name);
        if (!remove_op.ok()) {
            return Option<SynonymIndex*>(remove_op.code(), "Failed to remove existing synonym index"
            + index_name + ": " + remove_op.error());
        }
        LOG(INFO) << "Removed existing synonym index: " << index_name;
    }
    auto res = synonym_index_list.insert(synonym_index_list.end(), std::move(index));
    synonym_index_map.emplace(index_name, res);
    store->insert(SynonymIndexManager::get_synonym_index_key(index_name), index_name);
    return Option<SynonymIndex*>(&(*res));
}

Option<SynonymIndex*> SynonymIndexManager::add_synonym_index(const std::string& index_name) {
    if(synonym_index_map.find(index_name) != synonym_index_map.end()) {
        auto remove_op = remove_synonym_index(index_name);
        if (!remove_op.ok()) {
            return Option<SynonymIndex*>(remove_op.code(), "Failed to remove existing synonym index"
            + index_name + ": " + remove_op.error());
        }
        LOG(INFO) << "Removed existing synonym index: " << index_name;
    }
    SynonymIndex index(store, index_name);
    auto res = synonym_index_list.insert(synonym_index_list.end(), std::move(index));
    synonym_index_map.emplace(index_name, res);
    store->insert(SynonymIndexManager::get_synonym_index_key(index_name), index_name);
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

    if (!paylaod.contains("synonyms") || !paylaod["synonyms"].is_array()) {
        return Option<bool>(400, "Missing or invalid 'synonyms' field");
    }

    for (const auto& synonym : paylaod["synonyms"]) {
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
    store->scan_fill(
        SynonymIndexManager::SYNONYM_INDEX_KEY + std::string("_"),
        SynonymIndexManager::SYNONYM_INDEX_KEY + std::string("`"),
        synonym_index_names
    );

    for (const auto& synonym_index_name : synonym_index_names) {
        // create SynonymIndex object
        auto add_op = add_synonym_index(synonym_index_name);
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
