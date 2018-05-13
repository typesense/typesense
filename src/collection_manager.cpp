
#include <string>
#include <vector>
#include <json.hpp>
#include "collection_manager.h"

CollectionManager::CollectionManager() {

}

Collection* CollectionManager::init_collection(const nlohmann::json & collection_meta,
                                               const uint32_t collection_next_seq_id) {
    std::string this_collection_name = collection_meta[COLLECTION_NAME_KEY].get<std::string>();

    std::vector<field> fields;
    nlohmann::json fields_map = collection_meta[COLLECTION_SEARCH_FIELDS_KEY];

    for (nlohmann::json::iterator it = fields_map.begin(); it != fields_map.end(); ++it) {
        fields.push_back({it.value()[fields::name], it.value()[fields::type], it.value()[fields::facet]});
    }

    std::string default_sorting_field = collection_meta[COLLECTION_DEFAULT_SORTING_FIELD_KEY].get<std::string>();

    Collection* collection = new Collection(this_collection_name,
                                            collection_meta[COLLECTION_ID_KEY].get<uint32_t>(),
                                            collection_next_seq_id,
                                            store,
                                            fields,
                                            default_sorting_field);

    return collection;
}

void CollectionManager::add_to_collections(Collection* collection) {
    collections.emplace(collection->get_name(), collection);
    collection_id_names.emplace(collection->get_collection_id(), collection->get_name());
}

Option<bool> CollectionManager::init(Store *store, const std::string & auth_key,
                                     const std::string & search_only_auth_key) {
    this->store = store;
    this->auth_key = auth_key;
    this->search_only_auth_key = search_only_auth_key;

    std::string next_collection_id_str;
    StoreStatus next_coll_id_status = store->get(NEXT_COLLECTION_ID_KEY, next_collection_id_str);

    if(next_coll_id_status == StoreStatus::ERROR) {
        return Option<bool>(500, "Error while fetching the next collection id from the disk.");
    }

    if(next_coll_id_status == StoreStatus::FOUND) {
        next_collection_id = (uint32_t) stoi(next_collection_id_str);
    } else {
        next_collection_id = 0;
    }

    std::vector<std::string> collection_meta_jsons;
    store->scan_fill(Collection::COLLECTION_META_PREFIX, collection_meta_jsons);

    for(auto & collection_meta_json: collection_meta_jsons) {
        nlohmann::json collection_meta;

        try {
            collection_meta = nlohmann::json::parse(collection_meta_json);
        } catch(...) {
            return Option<bool>(500, "Error while parsing collection meta.");
        }

        const std::string & this_collection_name = collection_meta[COLLECTION_NAME_KEY].get<std::string>();
        std::string collection_next_seq_id_str;
        StoreStatus next_seq_id_status = store->get(Collection::get_next_seq_id_key(this_collection_name),
                                                    collection_next_seq_id_str);

        if(next_seq_id_status == StoreStatus::ERROR) {
            return Option<bool>(500, "Error while fetching collection's next sequence ID from the disk for collection "
                                     "`" + this_collection_name + "`");
        }

        if(next_seq_id_status == StoreStatus::NOT_FOUND && next_coll_id_status == StoreStatus::FOUND) {
            return Option<bool>(500, "Next collection id was found, but collection's next sequence ID is missing for "
                                     "`" + this_collection_name + "`");
        }

        uint32_t collection_next_seq_id = next_seq_id_status == StoreStatus::NOT_FOUND ? 0 :
                                          StringUtils::deserialize_uint32_t(collection_next_seq_id_str);

        Collection* collection = init_collection(collection_meta, collection_next_seq_id);

        // Fetch records from the store and re-create memory index
        std::vector<std::string> documents;
        const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();
        rocksdb::Iterator* iter = store->scan(seq_id_prefix);

        while(iter->Valid() && iter->key().starts_with(seq_id_prefix)) {
            const std::string doc_json_str = iter->value().ToString();

            nlohmann::json document;
            try {
                document = nlohmann::json::parse(doc_json_str);
            } catch(...) {
                delete iter;
                return Option<bool>(500, std::string("Error while parsing stored document from collection " +
                                                     collection->get_name() + " with key: ") + iter->key().ToString());
            }

            Option<uint32_t> seq_id_op = collection->doc_id_to_seq_id(document["id"]);
            if(!seq_id_op.ok()) {
               delete iter;
               return Option<bool>(500, std::string("Error while fetching sequence id of document id " +
                                        document["id"].get<std::string>() + " in collection `" +
                                        collection->get_name() + "`"));
            }

            collection->index_in_memory(document, seq_id_op.get());
            iter->Next();
        }

        delete iter;

        add_to_collections(collection);
    }

    return Option<bool>(true);
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

bool CollectionManager::search_only_auth_key_matches(std::string auth_key_sent) {
    return (search_only_auth_key == auth_key_sent);
}

Option<Collection*> CollectionManager::create_collection(const std::string name, const std::vector<field> & fields,
                                                         const std::string & default_sorting_field) {
    if(store->contains(Collection::get_meta_key(name))) {
        return Option<Collection*>(409, std::string("A collection with name `") + name + "` already exists.");
    }

    nlohmann::json collection_meta;

    nlohmann::json fields_json = nlohmann::json::array();;
    for(const field & field: fields) {
        nlohmann::json field_val;
        field_val[fields::name] = field.name;
        field_val[fields::type] = field.type;
        field_val[fields::facet] = field.facet;
        fields_json.push_back(field_val);

        if(field.name == default_sorting_field && !(field.type == field_types::INT32 ||
                                                    field.type == field_types::FLOAT)) {
            return Option<Collection*>(400, "Default sorting field `" + default_sorting_field + "` must be of type int32 "
                                            "or float.");
        }
    }

    collection_meta[COLLECTION_NAME_KEY] = name;
    collection_meta[COLLECTION_ID_KEY] = next_collection_id;
    collection_meta[COLLECTION_SEARCH_FIELDS_KEY] = fields_json;
    collection_meta[COLLECTION_DEFAULT_SORTING_FIELD_KEY] = default_sorting_field;

    Collection* new_collection = new Collection(name, next_collection_id, 0, store, fields, default_sorting_field);
    next_collection_id++;

    rocksdb::WriteBatch batch;
    batch.Put(Collection::get_next_seq_id_key(name), StringUtils::serialize_uint32_t(0));
    batch.Put(Collection::get_meta_key(name), collection_meta.dump());
    batch.Put(NEXT_COLLECTION_ID_KEY, std::to_string(next_collection_id));
    bool write_ok = store->batch_write(batch);

    if(!write_ok) {
        return Option<Collection*>(500, "Could not write to on-disk storage.");
    }

    add_to_collections(new_collection);

    return Option<Collection*>(new_collection);
}

Collection* CollectionManager::get_collection(const std::string & collection_name) {
    if(collections.count(collection_name) != 0) {
        return collections.at(collection_name);
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

    collections.erase(collection_name);
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