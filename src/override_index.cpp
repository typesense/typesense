#include "override_index.h"
#include "string_utils.h"

Option<std::map<uint32_t, override_t*>> OverrideIndex::get_overrides(uint32_t limit, uint32_t offset) {
    std::shared_lock lock(mutex);
    std::map<uint32_t, override_t*> res;

    auto it = override_definitions.begin();
    if(offset > 0) {
        if(offset >= override_definitions.size()) {
            return Option<std::map<uint32_t, override_t*>>(400, "Invalid offset param.");
        }
        std::advance(it, offset);
    }
    auto end = override_definitions.end();
    if(limit > 0 && (offset + limit < override_definitions.size())) {
        end = it;
        std::advance(end, limit);
    }
    while(it != end) {
        res[it->first] = &it->second;
        ++it;
    }
    return Option<std::map<uint32_t, override_t*>>(res);
}

bool OverrideIndex::get_override(const std::string& id, override_t& ov) {
    std::shared_lock lock(mutex);
    auto it = override_ids_index_map.find(id);
    if(it == override_ids_index_map.end()) {
        return false;
    }
    auto defIt = override_definitions.find(it->second);
    if(defIt == override_definitions.end()) {
        return false;
    }
    ov = defIt->second;
    return true;
}

Option<bool> OverrideIndex::add_override(const override_t& ov, bool write_to_store) {
    std::unique_lock lock(mutex);
    if(override_ids_index_map.count(ov.id) != 0) {
        // upsert: remove existing first
        lock.unlock();
        auto rem = remove_override(ov.id);
        if(!rem.ok()) {
            return rem;
        }
        lock.lock();
    }
    override_definitions[override_index] = ov;
    override_ids_index_map[ov.id] = override_index;
    ++override_index;
    lock.unlock();
    if(write_to_store) {
        bool ok = store->insert(get_override_key(name, ov.id), ov.to_json().dump());
        if(!ok) {
            return Option<bool>(500, "Error while storing the override on disk.");
        }
    }
    return Option<bool>(true);
}

Option<bool> OverrideIndex::remove_override(const std::string& id) {
    std::unique_lock lock(mutex);
    auto it = override_ids_index_map.find(id);
    if(it == override_ids_index_map.end()) {
        return Option<bool>(404, "Could not find that `id`.");
    }
    bool removed = store->remove(get_override_key(name, id));
    if(!removed) {
        return Option<bool>(500, "Error while deleting the override from disk.");
    }
    auto defIt = override_definitions.find(it->second);
    if(defIt != override_definitions.end()) {
        override_definitions.erase(defIt);
    }
    override_ids_index_map.erase(it);
    return Option<bool>(true);
}

nlohmann::json OverrideIndex::to_view_json() const {
    std::shared_lock lock(mutex);
    nlohmann::json obj;
    obj["items"] = nlohmann::json::array();
    for(const auto& kv: override_definitions) {
        obj["items"].push_back(kv.second.to_json());
    }
    obj["name"] = name;
    return obj;
}


