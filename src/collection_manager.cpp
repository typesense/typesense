
#include <string>
#include <vector>
#include <json.hpp>
#include "collection_manager.h"

CollectionManager::CollectionManager() {

}

Collection* CollectionManager::init_collection(const std::string & collection_meta_json) {
    nlohmann::json collection_meta = nlohmann::json::parse(collection_meta_json);
    std::string this_collection_name = collection_meta[COLLECTION_NAME_KEY].get<std::string>();

    std::vector<field> search_fields;
    nlohmann::json search_fields_map = collection_meta[COLLECTION_SEARCH_FIELDS_KEY];

    for (nlohmann::json::iterator it = search_fields_map.begin(); it != search_fields_map.end(); ++it) {
        search_fields.push_back({it.value()[fields::name], it.value()[fields::type]});
    }

    std::vector<field> facet_fields;
    nlohmann::json facet_fields_map = collection_meta[COLLECTION_FACET_FIELDS_KEY];

    for (nlohmann::json::iterator it = facet_fields_map.begin(); it != facet_fields_map.end(); ++it) {
        facet_fields.push_back({it.value()[fields::name], it.value()[fields::type]});
    }

    std::string collection_next_seq_id_str;
    store->get(Collection::get_next_seq_id_key(this_collection_name), collection_next_seq_id_str);
    uint32_t collection_next_seq_id = collection_next_seq_id_str.size() == 0 ? 0 :
                                      (const uint32_t) std::stoi(collection_next_seq_id_str);

    std::vector<field> collection_sort_fields;
    nlohmann::json sort_fields_map = collection_meta[COLLECTION_SORT_FIELDS_KEY];

    for (nlohmann::json::iterator it = sort_fields_map.begin(); it != sort_fields_map.end(); ++it) {
        collection_sort_fields.push_back({it.value()[fields::name], it.value()[fields::type]});
    }

    std::string token_ranking_field = collection_meta[COLLECTION_TOKEN_ORDERING_FIELD_KEY].get<std::string>();

    Collection* collection = new Collection(this_collection_name,
                                            collection_meta[COLLECTION_ID_KEY].get<uint32_t>(),
                                            collection_next_seq_id,
                                            store,
                                            search_fields,
                                            facet_fields,
                                            collection_sort_fields,
                                            token_ranking_field);

    return collection;
}

void CollectionManager::add_to_collections(Collection* collection) {
    collections.emplace(Collection::get_meta_key(collection->get_name()), collection);
    collection_id_names.emplace(collection->get_collection_id(), collection->get_name());
}

void CollectionManager::init(Store *store, const std::string & auth_key) {
    this->store = store;
    this->auth_key = auth_key;

    std::string next_collection_id_str;
    store->get(NEXT_COLLECTION_ID_KEY, next_collection_id_str);
    if(!next_collection_id_str.empty()) {
        next_collection_id = (uint32_t) stoi(next_collection_id_str);
    } else {
        next_collection_id = 0;
    }

    std::vector<std::string> collection_meta_jsons;
    store->scan_fill(Collection::COLLECTION_META_PREFIX, collection_meta_jsons);

    for(auto & collection_meta_json: collection_meta_jsons) {
        Collection* collection = init_collection(collection_meta_json);

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

        add_to_collections(collection);
    }

    std::cout << "Finished restoring all collections from disk." << std::endl;
}

void CollectionManager::dispose() {
    for(auto & name_collection: collections) {
        delete name_collection.second;
        name_collection.second = nullptr;
    }

    collections.clear();
}

bool CollectionManager::auth_key_matches(std::string auth_key_sent) {
    return (auth_key == auth_key_sent);
}

Collection* CollectionManager::create_collection(std::string name, const std::vector<field> & search_fields,
                                                 const std::vector<field> & facet_fields,
                                                 const std::vector<field> & sort_fields,
                                                 const std::string & token_ranking_field) {
    if(store->contains(Collection::get_meta_key(name))) {
        return nullptr;
    }

    nlohmann::json collection_meta;

    nlohmann::json search_fields_json = nlohmann::json::array();;
    for(const field & search_field: search_fields) {
        nlohmann::json field_val;
        field_val[fields::name] = search_field.name;
        field_val[fields::type] = search_field.type;
        search_fields_json.push_back(field_val);
    }

    nlohmann::json facet_fields_json = nlohmann::json::array();;
    for(const field & facet_field: facet_fields) {
        nlohmann::json field_val;
        field_val[fields::name] = facet_field.name;
        field_val[fields::type] = facet_field.type;
        facet_fields_json.push_back(field_val);
    }

    nlohmann::json sort_fields_json = nlohmann::json::array();;
    for(const field & sort_field: sort_fields) {
        nlohmann::json sort_field_val;
        sort_field_val[fields::name] = sort_field.name;
        sort_field_val[fields::type] = sort_field.type;
        sort_fields_json.push_back(sort_field_val);
    }

    collection_meta[COLLECTION_NAME_KEY] = name;
    collection_meta[COLLECTION_ID_KEY] = next_collection_id;
    collection_meta[COLLECTION_SEARCH_FIELDS_KEY] = search_fields_json;
    collection_meta[COLLECTION_FACET_FIELDS_KEY] = facet_fields_json;
    collection_meta[COLLECTION_SORT_FIELDS_KEY] = sort_fields_json;
    collection_meta[COLLECTION_TOKEN_ORDERING_FIELD_KEY] = token_ranking_field;

    Collection* new_collection = new Collection(name, next_collection_id, 0, store, search_fields, facet_fields,
                                                sort_fields, token_ranking_field);
    next_collection_id++;

    store->insert(Collection::get_next_seq_id_key(name), std::to_string(0));
    store->insert(Collection::get_meta_key(name), collection_meta.dump());
    store->insert(NEXT_COLLECTION_ID_KEY, std::to_string(next_collection_id));

    add_to_collections(new_collection);

    return new_collection;
}

Collection* CollectionManager::get_collection(const std::string & collection_name) {
    if(collections.count(Collection::get_meta_key(collection_name)) != 0) {
        return collections.at(Collection::get_meta_key(collection_name));
    }

    return nullptr;
}

Collection* CollectionManager::get_collection_with_id(uint32_t collection_id) {
    if(collection_id_names.count(collection_id) != 0) {
        return get_collection(collection_id_names.at(collection_id));
    }

    return nullptr;
}

std::vector<Collection*> CollectionManager::get_collections() {
    std::vector<Collection*> collection_vec;
    for(auto kv: collections) {
        collection_vec.push_back(kv.second);
    }

    std::sort(std::begin(collection_vec), std::end(collection_vec),
              [] (Collection* lhs, Collection* rhs) {
                  return lhs->get_collection_id()  > rhs->get_collection_id();
              });

    return collection_vec;
}

Option<bool> CollectionManager::drop_collection(std::string collection_name, const bool remove_from_store) {
    Collection* collection = get_collection(collection_name);
    if(collection == nullptr) {
        return Option<bool>(404, "No collection with name `" + collection_name + "` found.");
    }

    if(remove_from_store) {
        const std::string &collection_id_str = std::to_string(collection->get_collection_id());

        // Note: The following order of dropping documents first before dropping collection meta is important for
        // replication to work properly!
        rocksdb::Iterator* iter = store->scan(collection_id_str);
        while(iter->Valid() && iter->key().starts_with(collection_id_str)) {
            store->remove(iter->key().ToString());
            iter->Next();
        }
        delete iter;

        store->remove(Collection::get_next_seq_id_key(collection_name));
        store->remove(Collection::get_meta_key(collection_name));
    }

    collections.erase(Collection::get_meta_key(collection_name));
    collection_id_names.erase(collection->get_collection_id());

    delete collection;
    collection = nullptr;

    return Option<bool>(true);
}

uint32_t CollectionManager::get_next_collection_id() {
    return next_collection_id;
}

void CollectionManager::set_next_collection_id(uint32_t next_id) {
    next_collection_id = next_id;
}

Store* CollectionManager::get_store() {
    return store;
}