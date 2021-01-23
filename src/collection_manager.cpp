#include <string>
#include <vector>
#include <json.hpp>
#include "collection_manager.h"
#include "logger.h"

constexpr const char* CollectionManager::COLLECTION_NUM_MEMORY_SHARDS;
constexpr const size_t CollectionManager::DEFAULT_NUM_MEMORY_SHARDS;

CollectionManager::CollectionManager() {

}

Collection* CollectionManager::init_collection(const nlohmann::json & collection_meta,
                                               const uint32_t collection_next_seq_id) {
    std::string this_collection_name = collection_meta[COLLECTION_NAME_KEY].get<std::string>();

    std::vector<field> fields;
    nlohmann::json fields_map = collection_meta[COLLECTION_SEARCH_FIELDS_KEY];

    for (nlohmann::json::iterator it = fields_map.begin(); it != fields_map.end(); ++it) {
        nlohmann::json & field_obj = it.value();

        // handle older records indexed before optional field introduction
        if(field_obj.count(fields::optional) == 0) {
            field_obj[fields::optional] = false;
        }

        fields.push_back({field_obj[fields::name], field_obj[fields::type],
                          field_obj[fields::facet], field_obj[fields::optional]});
    }

    std::string default_sorting_field = collection_meta[COLLECTION_DEFAULT_SORTING_FIELD_KEY].get<std::string>();
    uint64_t created_at = collection_meta.find((const char*)COLLECTION_CREATED) != collection_meta.end() ?
                       collection_meta[COLLECTION_CREATED].get<uint64_t>() : 0;

    size_t num_memory_shards = collection_meta.count(COLLECTION_NUM_MEMORY_SHARDS) != 0 ?
                               collection_meta[COLLECTION_NUM_MEMORY_SHARDS].get<size_t>() :
                               DEFAULT_NUM_MEMORY_SHARDS;

    LOG(INFO) << "Found collection " << this_collection_name << " with " << num_memory_shards << " memory shards.";

    Collection* collection = new Collection(this_collection_name,
                                            collection_meta[COLLECTION_ID_KEY].get<uint32_t>(),
                                            created_at,
                                            collection_next_seq_id,
                                            store,
                                            fields,
                                            default_sorting_field,
                                            num_memory_shards,
                                            max_memory_ratio);

    return collection;
}

void CollectionManager::add_to_collections(Collection* collection) {
    collections.emplace(collection->get_name(), collection);
    collection_id_names.emplace(collection->get_collection_id(), collection->get_name());
}

void CollectionManager::init(Store *store,
                             const float max_memory_ratio,
                             const std::string & auth_key) {
    this->store = store;
    this->bootstrap_auth_key = auth_key;
    this->max_memory_ratio = max_memory_ratio;
}

Option<bool> CollectionManager::load(const size_t init_batch_size) {
    // This function must be idempotent, i.e. when called multiple times, must produce the same state without leaks
    LOG(INFO) << "CollectionManager::load()";

    Option<bool> auth_init_op = auth_manager.init(store);
    if(!auth_init_op.ok()) {
        LOG(ERROR) << "Auth manager init failed, error=" << auth_init_op.error();
    }

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

    LOG(INFO) << "Found " << collection_meta_jsons.size() << " collection(s) on disk.";

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

        if(get_collection(this_collection_name)) {
            // To maintain idempotency, if the collection already exists in-memory, drop it from memory
            LOG(WARNING) << "Dropping duplicate collection " << this_collection_name << " before loading it again.";
            drop_collection(this_collection_name, false);
        }

        Collection* collection = init_collection(collection_meta, collection_next_seq_id);

        LOG(INFO) << "Loading collection " << collection->get_name();

        // initialize overrides
        std::vector<std::string> collection_override_jsons;
        store->scan_fill(Collection::get_override_key(this_collection_name, ""), collection_override_jsons);

        for(const auto & collection_override_json: collection_override_jsons) {
            nlohmann::json collection_override = nlohmann::json::parse(collection_override_json);
            override_t override;
            auto parse_op = override_t::parse(collection_override, "", override);
            if(parse_op.ok()) {
                collection->add_override(override);
            } else {
                LOG(ERROR) << "Skipping loading of override: " << parse_op.error();
            }
        }

        // initialize synonyms
        std::vector<std::string> collection_synonym_jsons;
        store->scan_fill(Collection::get_synonym_key(this_collection_name, ""), collection_synonym_jsons);

        for(const auto & collection_synonym_json: collection_synonym_jsons) {
            nlohmann::json collection_synonym = nlohmann::json::parse(collection_synonym_json);
            synonym_t synonym(collection_synonym);
            collection->add_synonym(synonym);
        }

        // Fetch records from the store and re-create memory index
        std::vector<std::string> documents;
        const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();

        rocksdb::Iterator* iter = store->scan(seq_id_prefix);
        std::unique_ptr<rocksdb::Iterator> iter_guard(iter);

        std::vector<std::vector<index_record>> iter_batch;

        for(size_t i = 0; i < collection->get_num_memory_shards(); i++) {
            iter_batch.push_back(std::vector<index_record>());
        }

        size_t num_found_docs = 0;
        size_t num_valid_docs = 0;
        size_t num_indexed_docs = 0;

        auto begin = std::chrono::high_resolution_clock::now();

        while(iter->Valid() && iter->key().starts_with(seq_id_prefix)) {
            num_found_docs++;
            const uint32_t seq_id = Collection::get_seq_id_from_key(iter->key().ToString());

            nlohmann::json document;

            try {
                document = nlohmann::json::parse(iter->value().ToString());
            } catch(const std::exception& e) {
                LOG(ERROR) << "JSON error: " << e.what();
                return Option<bool>(false, "Bad JSON.");
            }

            num_valid_docs++;
            iter_batch[seq_id % collection->get_num_memory_shards()].emplace_back(index_record(0, seq_id, document, CREATE));

            // Peek and check for last record right here so that we handle batched indexing correctly
            // Without doing this, the "last batch" would have to be indexed outside the loop.
            iter->Next();
            bool last_record = !(iter->Valid() && iter->key().starts_with(seq_id_prefix));

            // batch must match atleast the number of shards
            const size_t batch_size = std::max(init_batch_size, collection->get_num_memory_shards());

            if((num_valid_docs % batch_size == 0) || last_record) {
                std::vector<size_t> indexed_counts;
                indexed_counts.reserve(iter_batch.size());

                collection->par_index_in_memory(iter_batch, indexed_counts);

                for(size_t i = 0; i < collection->get_num_memory_shards(); i++) {
                    size_t num_records = iter_batch[i].size();
                    size_t num_indexed = indexed_counts[i];

                    if(num_indexed != num_records) {
                        const Option<std::string> & index_error_op = get_first_index_error(iter_batch[i]);
                        if(!index_error_op.ok()) {
                            return Option<bool>(false, index_error_op.get());
                        }
                    }
                    iter_batch[i].clear();
                    num_indexed_docs += num_indexed;
                }
            }

            auto time_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now() - begin).count();

            auto throttle_time_millis = uint64_t((LOAD_THROTTLE_PERCENT/100) * time_millis);

            if(throttle_time_millis != 0) {
                // we throttle only when we have accumulated atleast 1 ms worth of throttling time
                begin = std::chrono::high_resolution_clock::now();
                std::this_thread::sleep_for(std::chrono::milliseconds(throttle_time_millis));
            }
        }

        add_to_collections(collection);
        LOG(INFO) << "Indexed " << num_indexed_docs << "/" << num_found_docs
                  << " documents into collection " << collection->get_name();
    }

    std::string symlink_prefix_key = std::string(SYMLINK_PREFIX) + "_";
    rocksdb::Iterator* iter = store->scan(symlink_prefix_key);
    while(iter->Valid() && iter->key().starts_with(symlink_prefix_key)) {
        std::vector<std::string> parts;
        StringUtils::split(iter->key().ToString(), parts, symlink_prefix_key);
        collection_symlinks[parts[0]] = iter->value().ToString();
        iter->Next();
    }

    delete iter;

    return Option<bool>(true);
}


void CollectionManager::dispose() {
    for(auto & name_collection: collections) {
        delete name_collection.second;
        name_collection.second = nullptr;
    }

    collections.clear();
    store->close();
}

bool CollectionManager::auth_key_matches(const std::string& auth_key_sent,
                                         const std::string& action,
                                         const std::string& collection,
                                         std::map<std::string, std::string>& params) {
    if(auth_key_sent.empty()) {
        return false;
    }

    // check with bootstrap auth key
    if(bootstrap_auth_key == auth_key_sent) {
        return true;
    }

    // finally, check managed auth keys
    return auth_manager.authenticate(auth_key_sent, action, collection, params);
}

Option<Collection*> CollectionManager::create_collection(const std::string& name,
                                                         const size_t num_memory_shards,
                                                         const std::vector<field> & fields,
                                                         const std::string & default_sorting_field,
                                                         const uint64_t created_at) {
    if(store->contains(Collection::get_meta_key(name))) {
        return Option<Collection*>(409, std::string("A collection with name `") + name + "` already exists.");
    }

    bool found_default_sorting_field = false;
    nlohmann::json fields_json = nlohmann::json::array();;

    for(const field & field: fields) {
        nlohmann::json field_val;
        field_val[fields::name] = field.name;
        field_val[fields::type] = field.type;
        field_val[fields::facet] = field.facet;
        field_val[fields::optional] = field.optional;
        fields_json.push_back(field_val);

        if(!field.has_valid_type()) {
            return Option<Collection*>(400, "Field `" + field.name +
                                            "` has an invalid data type `" + field.type +
                                            "`, see docs for supported data types.");
        }

        if(field.name == default_sorting_field && !(field.type == field_types::INT32 ||
                                                    field.type == field_types::INT64 ||
                                                    field.type == field_types::FLOAT)) {
            return Option<Collection*>(400, "Default sorting field `" + default_sorting_field +
                                            "` must be a single valued numerical field.");
        }

        if(field.name == default_sorting_field) {
            if(field.optional) {
                return Option<Collection*>(400, "Default sorting field `" + default_sorting_field +
                                                "` cannot be an optional field.");
            }

            found_default_sorting_field = true;
        }
    }

    if(!found_default_sorting_field) {
        return Option<Collection*>(400, "Default sorting field is defined as `" + default_sorting_field +
                                        "` but is not found in the schema.");
    }

    nlohmann::json collection_meta;
    collection_meta[COLLECTION_NAME_KEY] = name;
    collection_meta[COLLECTION_ID_KEY] = next_collection_id;
    collection_meta[COLLECTION_SEARCH_FIELDS_KEY] = fields_json;
    collection_meta[COLLECTION_DEFAULT_SORTING_FIELD_KEY] = default_sorting_field;
    collection_meta[COLLECTION_CREATED] = created_at;
    collection_meta[COLLECTION_NUM_MEMORY_SHARDS] = num_memory_shards;

    Collection* new_collection = new Collection(name, next_collection_id, created_at, 0, store, fields,
                                                default_sorting_field, num_memory_shards,
                                                this->max_memory_ratio);
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

    // a symlink name takes lesser precedence over a real collection name
    if(collection_symlinks.count(collection_name) != 0) {
        const std::string & symlinked_name = collection_symlinks.at(collection_name);
        if(collections.count(symlinked_name) != 0) {
            return collections.at(symlinked_name);
        }
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

Option<bool> CollectionManager::drop_collection(const std::string& collection_name, const bool remove_from_store) {
    Collection* collection = get_collection(collection_name);
    if(collection == nullptr) {
        return Option<bool>(404, "No collection with name `" + collection_name + "` found.");
    }

    if(remove_from_store) {
        const std::string &collection_id_str = std::to_string(collection->get_collection_id());

        // Note: The order of dropping documents first before dropping collection meta is important for replication
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

    return Option<bool>(true);
}

uint32_t CollectionManager::get_next_collection_id() {
    return next_collection_id;
}

std::string CollectionManager::get_symlink_key(const std::string & symlink_name) {
    return std::string(SYMLINK_PREFIX) + "_" + symlink_name;
}

void CollectionManager::set_next_collection_id(uint32_t next_id) {
    next_collection_id = next_id;
}

spp::sparse_hash_map<std::string, std::string> & CollectionManager::get_symlinks() {
    return collection_symlinks;
}

Option<std::string> CollectionManager::resolve_symlink(const std::string & symlink_name) {
    if(collection_symlinks.count(symlink_name) != 0) {
        return Option<std::string>(collection_symlinks.at(symlink_name));
    }

    return Option<std::string>(404, "Not found.");
}

Option<bool> CollectionManager::upsert_symlink(const std::string & symlink_name, const std::string & collection_name) {
    if(collections.count(symlink_name) != 0) {
        return Option<bool>(500, "Name `" + symlink_name + "` conflicts with an existing collection name.");
    }

    bool inserted = store->insert(get_symlink_key(symlink_name), collection_name);
    if(!inserted) {
        return Option<bool>(500, "Unable to insert into store.");
    }

    collection_symlinks[symlink_name] = collection_name;
    return Option<bool>(true);
}

Option<bool> CollectionManager::delete_symlink(const std::string & symlink_name) {
    bool removed = store->remove(get_symlink_key(symlink_name));
    if(!removed) {
        return Option<bool>(500, "Unable to delete from store.");
    }

    collection_symlinks.erase(symlink_name);
    return Option<bool>(true);
}

Store* CollectionManager::get_store() {
    return store;
}

AuthManager& CollectionManager::getAuthManager() {
    return auth_manager;
}
