
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
    store->scan_fill(Collection::COLLECTION_META_PREFIX, collection_meta_jsons);

    for(auto collection_meta_json: collection_meta_jsons) {
        nlohmann::json collection_meta = nlohmann::json::parse(collection_meta_json);
        std::string this_collection_name = collection_meta[COLLECTION_NAME_KEY].get<std::string>();

        std::vector<field> search_fields;
        nlohmann::json fields_map = collection_meta[COLLECTION_SEARCH_FIELDS_KEY];

        for (nlohmann::json::iterator it = fields_map.begin(); it != fields_map.end(); ++it) {
            search_fields.push_back({it.value()[fields::name], it.value()[fields::type]});
        }
        
        std::string collection_next_seq_id_str;
        store->get(Collection::get_next_seq_id_key(this_collection_name), collection_next_seq_id_str);

        uint32_t collection_next_seq_id = (const uint32_t) std::stoi(collection_next_seq_id_str);
        std::vector<std::string> collection_rank_fields =
                collection_meta[COLLECTION_RANK_FIELDS_KEY].get<std::vector<std::string>>();

        Collection* collection = new Collection(this_collection_name,
                                                collection_meta[COLLECTION_ID_KEY].get<uint32_t>(),
                                                collection_next_seq_id,
                                                store,
                                                search_fields,
                                                collection_rank_fields);

        // Fetch records from the store and re-create memory index
        std::vector<std::string> documents;
        const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();
        rocksdb::Iterator* iter = store->scan(seq_id_prefix);

        while(iter->Valid() && iter->key().starts_with(seq_id_prefix)) {
            const std::string doc_json_str = iter->value().ToString();
            nlohmann::json document = nlohmann::json::parse(doc_json_str);
            uint32_t seq_id = collection->doc_id_to_seq_id(document["id"]);
            collection->index_in_memory(document, seq_id);
            iter->Next();
        }

        delete iter;

        collections.emplace(Collection::get_meta_key(this_collection_name), collection);
    }

    std::cout << "Finished restoring all collections from disk." << std::endl;
}

Collection* CollectionManager::create_collection(std::string name, const std::vector<field> & search_fields,
                                          const std::vector<std::string> & rank_fields) {
    if(store->contains(Collection::get_meta_key(name))) {
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
    collection_meta[COLLECTION_SEARCH_FIELDS_KEY] = search_fields_json;
    collection_meta[COLLECTION_RANK_FIELDS_KEY] = rank_fields;
    
    Collection* new_collection = new Collection(name, next_collection_id, 0, store, search_fields, rank_fields);

    store->insert(Collection::get_meta_key(name), collection_meta.dump());
    store->insert(Collection::get_next_seq_id_key(name), std::to_string(0));

    next_collection_id++;
    store->insert(NEXT_COLLECTION_ID_KEY, std::to_string(next_collection_id));

    collections.emplace(Collection::get_meta_key(name), new_collection);

    return new_collection;
}

Collection* CollectionManager::get_collection(std::string collection_name) {
    if(collections.count(Collection::get_meta_key(collection_name)) != 0) {
        return collections.at(Collection::get_meta_key(collection_name));
    }

    return nullptr;
}

CollectionManager::~CollectionManager() {
    for(auto kv: collections) {
        drop_collection(kv.first);
    }
}

bool CollectionManager::drop_collection(std::string collection_name) {
    Collection* collection = get_collection(collection_name);
    if(collection == nullptr) {
        return false;
    }

    store->remove(Collection::get_meta_key(collection_name));
    store->remove(Collection::get_next_seq_id_key(collection_name));

    const std::string &collection_id_str = std::to_string(collection->get_collection_id());
    rocksdb::Iterator* iter = store->scan(collection_id_str);
    while(iter->Valid() && iter->key().starts_with(collection_id_str)) {
        store->remove(iter->key().ToString());
        iter->Next();
    }

    delete iter;

    collections.erase(Collection::get_meta_key(collection_name));

    delete collection;
    collection = nullptr;

    return true;
}

uint32_t CollectionManager::get_next_collection_id() {
    return next_collection_id;
}