#include "curation_index.h"
#include "string_utils.h"

Option<std::map<std::string, curation_t*>> CurationIndex::get_overrides(uint32_t limit, uint32_t offset) {
    std::shared_lock lock(mutex);
    std::map<std::string, curation_t*> res;

    auto it = curation_definitions.begin();
    if(offset > 0) {
        if(offset >= curation_definitions.size()) {
            return Option<std::map<std::string, curation_t*>>(400, "Invalid offset param.");
        }
        std::advance(it, offset);
    }
    auto end = curation_definitions.end();
    if(limit > 0 && (offset + limit < curation_definitions.size())) {
        end = it;
        std::advance(end, limit);
    }
    while(it != end) {
        res[it->first] = &it->second;
        ++it;
    }
    return Option<std::map<std::string, curation_t*>>(res);
}

bool CurationIndex::get_override(const std::string& id, curation_t& ov) {
    std::shared_lock lock(mutex);
    auto defIt = curation_definitions.find(id);
    if(defIt == curation_definitions.end()) return false;
    ov = defIt->second;
    return true;
}

Option<bool> CurationIndex::add_override(const curation_t& ov, bool write_to_store) {
    std::unique_lock lock(mutex);
    // upsert by id (lexicographic order maintained by map key)
    curation_definitions[ov.id] = ov;
    lock.unlock();
    if(write_to_store) {
        bool ok = store->insert(get_curation_key(name, ov.id), ov.to_json().dump());
        if(!ok) {
            return Option<bool>(500, "Error while storing the curation on disk.");
        }
    }
    return Option<bool>(true);
}

Option<bool> CurationIndex::remove_override(const std::string& id) {
    std::unique_lock lock(mutex);
    auto defIt = curation_definitions.find(id);
    if(defIt == curation_definitions.end()) {
        return Option<bool>(404, "Could not find that `id`.");
    }
    bool removed = store->remove(get_curation_key(name, id));
    if(!removed) {
        return Option<bool>(500, "Error while deleting the curation from disk.");
    }
    curation_definitions.erase(defIt);
    return Option<bool>(true);
}

nlohmann::json CurationIndex::to_view_json() const {
    std::shared_lock lock(mutex);
    nlohmann::json obj;
    obj["items"] = nlohmann::json::array();
    for(const auto& kv: curation_definitions) {
        obj["items"].push_back(kv.second.to_json());
    }
    obj["name"] = name;
    return obj;
}


