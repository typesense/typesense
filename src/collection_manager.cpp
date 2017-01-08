
#include <string>
#include <vector>
#include <json.hpp>
#include "collection_manager.h"

CollectionManager::CollectionManager() {

}

void CollectionManager::init(Store *store) {
    this->store = store;

    std::string next_collection_id_str;
    store->get(NEXT_COLLECTION_ID_KEY, next_collection_id_str);
    if(!next_collection_id_str.empty()) {
        next_collection_id = (uint32_t) stoi(next_collection_id_str);
    } else {
        next_collection_id = 0;
        store->insert(NEXT_COLLECTION_ID_KEY, std::to_string(next_collection_id));
    }

    std::vector<std::string> collection_meta_jsons;
    store->scan_fill(COLLECTION_NAME_PREFIX, collection_meta_jsons);

    for(auto collection_meta_json: collection_meta_jsons) {
        nlohmann::json collection_meta = nlohmann::json::parse(collection_meta_json);

        std::vector<field> search_fields;
        nlohmann::json fields_map = collection_meta[COLLECTION_SEARCH_FIELDS_KEY];

        for (nlohmann::json::iterator it = fields_map.begin(); it != fields_map.end(); ++it) {
            search_fields.push_back({it.value()[fields::name], it.value()[fields::type]});
        }

        Collection* collection = new Collection(collection_meta[COLLECTION_NAME_KEY].get<std::string>(),
                                                std::to_string(collection_meta[COLLECTION_ID_KEY].get<uint32_t>()),
                                                collection_meta[COLLECTION_NEXT_SEQ_ID_KEY].get<uint32_t>(),
                                                store,
                                                search_fields,
                                                collection_meta[COLLECTION_RANK_FIELDS_KEY].get<std::vector<std::string>>());
        collections.emplace(get_collection_name_key(collection_meta[COLLECTION_NAME_KEY]), collection);
    }
}

Collection* CollectionManager::create_collection(std::string name, const std::vector<field> & search_fields,
                                          const std::vector<std::string> & rank_fields) {
    if(store->contains(get_collection_name_key(name))) {
        return nullptr;
    }

    nlohmann::json collection_meta;

    nlohmann::json search_fields_json = nlohmann::json::array();;
    for(const field& search_field: search_fields) {
        nlohmann::json field_val;
        field_val[fields::name] = search_field.name;
        field_val[fields::type] = search_field.type;
        search_fields_json.push_back(field_val);
    }

    collection_meta[COLLECTION_NAME_KEY] = name;
    collection_meta[COLLECTION_ID_KEY] = next_collection_id;
    collection_meta[COLLECTION_NEXT_SEQ_ID_KEY] = 0;
    collection_meta[COLLECTION_SEARCH_FIELDS_KEY] = search_fields_json;
    collection_meta[COLLECTION_RANK_FIELDS_KEY] = rank_fields;
    store->insert(get_collection_name_key(name), collection_meta.dump());

    std::string collection_id_str = std::to_string(next_collection_id);
    Collection* new_collection = new Collection(name, collection_id_str, 0, store, search_fields, rank_fields);

    next_collection_id++;
    store->insert(NEXT_COLLECTION_ID_KEY, std::to_string(next_collection_id));

    return new_collection;
}

std::string CollectionManager::get_collection_name_key(std::string collection_name) {
    return COLLECTION_NAME_PREFIX + collection_name;
}

Collection* CollectionManager::get_collection(std::string collection_name) {
    if(collections.count(get_collection_name_key(collection_name)) != 0) {
        return collections.at(get_collection_name_key(collection_name));
    }

    return nullptr;
}

CollectionManager::~CollectionManager() {
    for(auto kv: collections) {
        delete kv.second;
    }
}
