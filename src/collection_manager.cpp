#include <string>
#include <vector>
#include <json.hpp>
#include <app_metrics.h>
#include <analytics_manager.h>
#include <event_manager.h>
#include "collection_manager.h"
#include "batched_indexer.h"
#include "logger.h"
#include "magic_enum.hpp"
#include "stopwords_manager.h"
#include "field.h"

constexpr const size_t CollectionManager::DEFAULT_NUM_MEMORY_SHARDS;

CollectionManager::CollectionManager() {

}

Collection* CollectionManager::init_collection(const nlohmann::json & collection_meta,
                                               const uint32_t collection_next_seq_id,
                                               Store* store,
                                               float max_memory_ratio) {
    std::string this_collection_name = collection_meta[Collection::COLLECTION_NAME_KEY].get<std::string>();

    std::vector<field> fields;
    nlohmann::json fields_map = collection_meta[Collection::COLLECTION_SEARCH_FIELDS_KEY];

    for (nlohmann::json::iterator it = fields_map.begin(); it != fields_map.end(); ++it) {
        nlohmann::json & field_obj = it.value();

        // handle older records indexed before optional field introduction
        if(field_obj.count(fields::optional) == 0) {
            field_obj[fields::optional] = false;
        }

        if(field_obj.count(fields::index) == 0) {
            field_obj[fields::index] = true;
        }

        if(field_obj.count(fields::locale) == 0) {
            field_obj[fields::locale] = "";
        }

        if(field_obj.count(fields::infix) == 0) {
            field_obj[fields::infix] = -1;
        }

        if(field_obj.count(fields::nested) == 0) {
            field_obj[fields::nested] = false;
        }

        if(field_obj.count(fields::nested_array) == 0) {
            field_obj[fields::nested_array] = 0;
        }

        if(field_obj.count(fields::num_dim) == 0) {
            field_obj[fields::num_dim] = 0;
        }

        if (field_obj.count(fields::reference) == 0) {
            field_obj[fields::reference] = "";
        }

        if(field_obj.count(fields::embed) == 0) {
            field_obj[fields::embed] = nlohmann::json::object();
        }

        if(field_obj.count(fields::model_config) == 0) {
            field_obj[fields::model_config] = nlohmann::json::object();
        }
        vector_distance_type_t vec_dist_type = vector_distance_type_t::cosine;

        if(field_obj.count(fields::vec_dist) != 0) {
            auto vec_dist_type_op = magic_enum::enum_cast<vector_distance_type_t>(fields::vec_dist);
            if(vec_dist_type_op.has_value()) {
                vec_dist_type = vec_dist_type_op.value();
            }
        }

        if(field_obj.count(fields::embed) != 0 && !field_obj[fields::embed].empty()) {
            size_t num_dim = 0;
            auto& model_config = field_obj[fields::embed][fields::model_config];

            auto res = TextEmbedderManager::get_instance().validate_and_init_model(model_config, num_dim);
            if(!res.ok()) {
                const std::string& model_name = model_config["model_name"].get<std::string>();
                LOG(ERROR) << "Error initializing model: " << model_name << ", error: " << res.error();
                continue;
            }

            field_obj[fields::num_dim] = num_dim;
            LOG(INFO) << "Model init done.";
        }

        field f(field_obj[fields::name], field_obj[fields::type], field_obj[fields::facet],
                field_obj[fields::optional], field_obj[fields::index], field_obj[fields::locale],
                -1, field_obj[fields::infix], field_obj[fields::nested], field_obj[fields::nested_array],
                field_obj[fields::num_dim], vec_dist_type, field_obj[fields::reference], field_obj[fields::embed]);

        // value of `sort` depends on field type
        if(field_obj.count(fields::sort) == 0) {
            f.sort = f.is_num_sort_field();
        } else {
            f.sort = field_obj[fields::sort];
        }

        fields.push_back(f);
    }

    std::string default_sorting_field = collection_meta[Collection::COLLECTION_DEFAULT_SORTING_FIELD_KEY].get<std::string>();

    uint64_t created_at = collection_meta.find((const char*)Collection::COLLECTION_CREATED) != collection_meta.end() ?
                       collection_meta[Collection::COLLECTION_CREATED].get<uint64_t>() : 0;

    size_t num_memory_shards = collection_meta.count(Collection::COLLECTION_NUM_MEMORY_SHARDS) != 0 ?
                               collection_meta[Collection::COLLECTION_NUM_MEMORY_SHARDS].get<size_t>() :
                               DEFAULT_NUM_MEMORY_SHARDS;

    std::string fallback_field_type = collection_meta.count(Collection::COLLECTION_FALLBACK_FIELD_TYPE) != 0 ?
                              collection_meta[Collection::COLLECTION_FALLBACK_FIELD_TYPE].get<std::string>() :
                              "";

    bool enable_nested_fields = collection_meta.count(Collection::COLLECTION_ENABLE_NESTED_FIELDS) != 0 ?
                                 collection_meta[Collection::COLLECTION_ENABLE_NESTED_FIELDS].get<bool>() :
                                 false;

    std::vector<std::string> symbols_to_index;
    std::vector<std::string> token_separators;

    if(collection_meta.count(Collection::COLLECTION_SYMBOLS_TO_INDEX) != 0) {
        symbols_to_index = collection_meta[Collection::COLLECTION_SYMBOLS_TO_INDEX].get<std::vector<std::string>>();
    }

    if(collection_meta.count(Collection::COLLECTION_SEPARATORS) != 0) {
        token_separators = collection_meta[Collection::COLLECTION_SEPARATORS].get<std::vector<std::string>>();
    }

    LOG(INFO) << "Found collection " << this_collection_name << " with " << num_memory_shards << " memory shards.";

    Collection* collection = new Collection(this_collection_name,
                                            collection_meta[Collection::COLLECTION_ID_KEY].get<uint32_t>(),
                                            created_at,
                                            collection_next_seq_id,
                                            store,
                                            fields,
                                            default_sorting_field,
                                            max_memory_ratio,
                                            fallback_field_type,
                                            symbols_to_index,
                                            token_separators,
                                            enable_nested_fields);

    return collection;
}

void CollectionManager::add_to_collections(Collection* collection) {
    const std::string& collection_name = collection->get_name();
    const uint32_t collection_id = collection->get_collection_id();
    std::unique_lock lock(mutex);
    collections.emplace(collection_name, collection);
    collection_id_names.emplace(collection_id, collection_name);
}

void CollectionManager::init(Store *store, ThreadPool* thread_pool,
                             const float max_memory_ratio,
                             const std::string & auth_key,
                             std::atomic<bool>& quit,
                             BatchedIndexer* batch_indexer) {
    std::unique_lock lock(mutex);

    this->store = store;
    this->thread_pool = thread_pool;
    this->bootstrap_auth_key = auth_key;
    this->max_memory_ratio = max_memory_ratio;
    this->quit = &quit;
    this->batch_indexer = batch_indexer;
}

// used only in tests!
void CollectionManager::init(Store *store, const float max_memory_ratio, const std::string & auth_key,
                             std::atomic<bool>& quit) {
    ThreadPool* thread_pool = new ThreadPool(8);
    init(store, thread_pool, max_memory_ratio, auth_key, quit, nullptr);
}

Option<bool> CollectionManager::load(const size_t collection_batch_size, const size_t document_batch_size) {
    // This function must be idempotent, i.e. when called multiple times, must produce the same state without leaks
    LOG(INFO) << "CollectionManager::load()";

    Option<bool> auth_init_op = auth_manager.init(store, bootstrap_auth_key);
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

    LOG(INFO) << "Loading upto " << collection_batch_size << " collections in parallel, "
              << document_batch_size << " documents at a time.";

    std::vector<std::string> collection_meta_jsons;
    store->scan_fill(std::string(Collection::COLLECTION_META_PREFIX) + "_",
                     std::string(Collection::COLLECTION_META_PREFIX) + "`",
                     collection_meta_jsons);

    const size_t num_collections = collection_meta_jsons.size();
    LOG(INFO) << "Found " << num_collections << " collection(s) on disk.";

    ThreadPool loading_pool(collection_batch_size);

    size_t num_processed = 0;
    // Collection name -> Referenced in
    std::map<std::string, std::set<reference_pair>> referenced_ins = {};
    std::mutex m_process;
    std::condition_variable cv_process;

    for(size_t coll_index = 0; coll_index < num_collections; coll_index++) {
        const auto& collection_meta_json = collection_meta_jsons[coll_index];
        nlohmann::json collection_meta = nlohmann::json::parse(collection_meta_json, nullptr, false);
        if(collection_meta.is_discarded()) {
            LOG(ERROR) << "Error while parsing collection meta, json: " << collection_meta_json;
            return Option<bool>(500, "Error while parsing collection meta.");
        }

        auto captured_store = store;
        loading_pool.enqueue([captured_store, num_collections, collection_meta, document_batch_size,
                              &m_process, &cv_process, &num_processed, &next_coll_id_status, quit = quit,
                              &referenced_ins]() {

            //auto begin = std::chrono::high_resolution_clock::now();
            Option<bool> res = load_collection(collection_meta, document_batch_size, next_coll_id_status, *quit);
            /*long long int timeMillis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
            LOG(INFO) << "Time taken for indexing: " << timeMillis << "ms";*/

            if(!res.ok()) {
                LOG(ERROR) << "Error while loading collection. " << res.error();
                LOG(ERROR) << "Typesense is quitting.";
                captured_store->close();
                exit(1);
            }

            std::unique_lock<std::mutex> lock(m_process);
            num_processed++;

            auto& cm = CollectionManager::get_instance();
            auto const& collection_name = collection_meta.at("name");
            auto collection = cm.get_collection(collection_name);
            if (collection != nullptr) {
                for (const auto &item: collection->get_reference_fields()) {
                    auto const& ref_coll_name = item.second.collection;
                    if (referenced_ins.count(ref_coll_name) == 0) {
                        referenced_ins[ref_coll_name] = {};
                    }
                    auto const field_name = item.first + Collection::REFERENCE_HELPER_FIELD_SUFFIX;
                    referenced_ins.at(ref_coll_name).insert(reference_pair{collection_name, field_name});
                }
            }
            cv_process.notify_one();

            size_t progress_modulo = std::max<size_t>(1, (num_collections / 10));  // every 10%
            if(num_processed % progress_modulo == 0) {
                LOG(INFO) << "Loaded " << num_processed << " collection(s) so far";
            }
        });
    }

    // wait for all collections to be loaded
    std::unique_lock<std::mutex> lock_process(m_process);
    cv_process.wait(lock_process, [&](){
        return num_processed == num_collections;
    });

    // Initialize references
    for (const auto &item: referenced_ins) {
        auto& cm = CollectionManager::get_instance();
        auto collection = cm.get_collection(item.first);
        if (collection != nullptr) {
            for (const auto &reference_pair: item.second) {
                collection->add_referenced_in(reference_pair);
            }
        }
    }

    // load aliases

    std::string symlink_prefix_key = std::string(SYMLINK_PREFIX) + "_";
    std::string upper_bound_key = std::string(SYMLINK_PREFIX) + "`";  // cannot inline this
    rocksdb::Slice upper_bound(upper_bound_key);

    rocksdb::Iterator* iter = store->scan(symlink_prefix_key, &upper_bound);
    while(iter->Valid() && iter->key().starts_with(symlink_prefix_key)) {
        std::vector<std::string> parts;
        StringUtils::split(iter->key().ToString(), parts, symlink_prefix_key);
        collection_symlinks[parts[0]] = iter->value().ToString();
        iter->Next();
    }
    delete iter;

    // load presets

    std::string preset_prefix_key = std::string(PRESET_PREFIX) + "_";
    std::string preset_upper_bound_key = std::string(PRESET_PREFIX) + "`"; // cannot inline this
    rocksdb::Slice preset_upper_bound(preset_upper_bound_key);

    iter = store->scan(preset_prefix_key, &preset_upper_bound);
    while(iter->Valid() && iter->key().starts_with(preset_prefix_key)) {
        std::vector<std::string> parts;
        std::string preset_name = iter->key().ToString().substr(preset_prefix_key.size());
        nlohmann::json preset_obj = nlohmann::json::parse(iter->value().ToString(), nullptr, false);

        if(!preset_obj.is_discarded() && preset_obj.is_object()) {
            preset_configs[preset_name] = preset_obj;
        } else {
            LOG(INFO) << "Invalid value for preset " << preset_name;
        }

        iter->Next();
    }
    delete iter;

    //load stopwords
    std::string stopword_prefix_key = std::string(StopwordsManager::STOPWORD_PREFIX) + "_";
    std::string stopword_upper_bound_key = std::string(StopwordsManager::STOPWORD_PREFIX) + "`"; // cannot inline this
    rocksdb::Slice stopword_upper_bound(stopword_upper_bound_key);

    iter = store->scan(stopword_prefix_key, &stopword_upper_bound);
    while(iter->Valid() && iter->key().starts_with(stopword_prefix_key)) {
        std::vector<std::string> parts;
        std::string stopword_name = iter->key().ToString().substr(stopword_prefix_key.size());
        nlohmann::json stopword_obj = nlohmann::json::parse(iter->value().ToString(), nullptr, false);

        if(!stopword_obj.is_discarded() && stopword_obj.is_object()) {
            StopwordsManager::get_instance().upsert_stopword(stopword_name, stopword_obj);
        } else {
            LOG(INFO) << "Invalid object for stopword " << stopword_name;
        }

        iter->Next();
    }
    delete iter;

    // restore query suggestions configs
    std::vector<std::string> analytics_config_jsons;
    store->scan_fill(AnalyticsManager::ANALYTICS_RULE_PREFIX,
                     std::string(AnalyticsManager::ANALYTICS_RULE_PREFIX) + "`",
                     analytics_config_jsons);

    for(const auto& analytics_config_json: analytics_config_jsons) {
        nlohmann::json analytics_config = nlohmann::json::parse(analytics_config_json);
        AnalyticsManager::get_instance().create_rule(analytics_config, false, false);
    }

    LOG(INFO) << "Loaded " << num_collections << " collection(s).";

    loading_pool.shutdown();

    LOG(INFO) << "Initializing batched indexer from snapshot state...";
    if(batch_indexer != nullptr) {
        std::string batched_indexer_state_str;
        StoreStatus s = store->get(BATCHED_INDEXER_STATE_KEY, batched_indexer_state_str);
        if(s == FOUND) {
            nlohmann::json batch_indexer_state = nlohmann::json::parse(batched_indexer_state_str);
            batch_indexer->load_state(batch_indexer_state);
        }
    }

    return Option<bool>(true);
}


void CollectionManager::dispose() {
    std::unique_lock lock(mutex);

    for(auto & name_collection: collections) {
        delete name_collection.second;
        name_collection.second = nullptr;
    }

    collections.clear();
    collection_symlinks.clear();
    preset_configs.clear();
    store->close();
}

bool CollectionManager::auth_key_matches(const string& req_auth_key, const string& action,
                                         const std::vector<collection_key_t>& collection_keys,
                                         std::map<std::string, std::string>& params,
                                         std::vector<nlohmann::json>& embedded_params_vec) const {
    std::shared_lock lock(mutex);

    // check with bootstrap auth key
    if(bootstrap_auth_key == req_auth_key) {
        return true;
    }

    // finally, check managed auth keys
    return auth_manager.authenticate(action, collection_keys, params, embedded_params_vec);
}

Option<Collection*> CollectionManager::create_collection(const std::string& name,
                                                         const size_t num_memory_shards,
                                                         const std::vector<field> & fields,
                                                         const std::string& default_sorting_field,
                                                         const uint64_t created_at,
                                                         const std::string& fallback_field_type,
                                                         const std::vector<std::string>& symbols_to_index,
                                                         const std::vector<std::string>& token_separators,
                                                         const bool enable_nested_fields) {
    std::unique_lock lock(coll_create_mutex);

    if(store->contains(Collection::get_meta_key(name))) {
        return Option<Collection*>(409, std::string("A collection with name `") + name + "` already exists.");
    }

    // validated `fallback_field_type`
    if(!fallback_field_type.empty()) {
        field fallback_field_type_def("temp", fallback_field_type, false);
        if(!fallback_field_type_def.has_valid_type()) {
            return Option<Collection*>(400, std::string("Field `.*` has an invalid type."));
        }
    }

    nlohmann::json fields_json = nlohmann::json::array();;

    Option<bool> fields_json_op = field::fields_to_json_fields(fields, default_sorting_field, fields_json);

    if(!fields_json_op.ok()) {
        return Option<Collection*>(fields_json_op.code(), fields_json_op.error());
    }

    nlohmann::json collection_meta;
    collection_meta[Collection::COLLECTION_NAME_KEY] = name;
    collection_meta[Collection::COLLECTION_ID_KEY] = next_collection_id.load();
    collection_meta[Collection::COLLECTION_SEARCH_FIELDS_KEY] = fields_json;
    collection_meta[Collection::COLLECTION_DEFAULT_SORTING_FIELD_KEY] = default_sorting_field;
    collection_meta[Collection::COLLECTION_CREATED] = created_at;
    collection_meta[Collection::COLLECTION_NUM_MEMORY_SHARDS] = num_memory_shards;
    collection_meta[Collection::COLLECTION_FALLBACK_FIELD_TYPE] = fallback_field_type;
    collection_meta[Collection::COLLECTION_SYMBOLS_TO_INDEX] = symbols_to_index;
    collection_meta[Collection::COLLECTION_SEPARATORS] = token_separators;
    collection_meta[Collection::COLLECTION_ENABLE_NESTED_FIELDS] = enable_nested_fields;

    Collection* new_collection = new Collection(name, next_collection_id, created_at, 0, store, fields,
                                                default_sorting_field,
                                                this->max_memory_ratio, fallback_field_type,
                                                symbols_to_index, token_separators,
                                                enable_nested_fields);
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

    if (referenced_in_backlog.count(name) > 0) {
        new_collection->add_referenced_ins(referenced_in_backlog.at(name));
        referenced_in_backlog.erase(name);
    }

    return Option<Collection*>(new_collection);
}

Collection* CollectionManager::get_collection_unsafe(const std::string & collection_name) const {
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

locked_resource_view_t<Collection> CollectionManager::get_collection(const std::string & collection_name) const {
    std::shared_lock lock(mutex);
    Collection* coll = get_collection_unsafe(collection_name);
    return locked_resource_view_t<Collection>(mutex, coll);
}

locked_resource_view_t<Collection> CollectionManager::get_collection_with_id(uint32_t collection_id) const {
    std::shared_lock lock(mutex);

    if(collection_id_names.count(collection_id) != 0) {
        return get_collection(collection_id_names.at(collection_id));
    }

    return locked_resource_view_t<Collection>(mutex, nullptr);
}

std::vector<Collection*> CollectionManager::get_collections() const {
    std::shared_lock lock(mutex);

    std::vector<Collection*> collection_vec;
    for(const auto& kv: collections) {
        collection_vec.push_back(kv.second);
    }

    std::sort(std::begin(collection_vec), std::end(collection_vec),
              [] (Collection* lhs, Collection* rhs) {
                  return lhs->get_collection_id()  > rhs->get_collection_id();
              });

    return collection_vec;
}

Option<nlohmann::json> CollectionManager::drop_collection(const std::string& collection_name, const bool remove_from_store) {
    std::shared_lock s_lock(mutex);
    auto collection = get_collection_unsafe(collection_name);

    if(collection == nullptr) {
        return Option<nlohmann::json>(404, "No collection with name `" + collection_name + "` found.");
    }

    // to handle alias resolution
    const std::string actual_coll_name = collection->get_name();

    nlohmann::json collection_json = collection->get_summary_json();

    if(remove_from_store) {
        const std::string& del_key_prefix = std::to_string(collection->get_collection_id()) + "_";
        const std::string& del_end_prefix = std::to_string(collection->get_collection_id()) + "`";
        store->delete_range(del_key_prefix, del_end_prefix);
        store->flush();
        store->compact_range(del_key_prefix, del_end_prefix);

        // delete overrides
        const std::string& del_override_prefix =
                std::string(Collection::COLLECTION_OVERRIDE_PREFIX) + "_" + actual_coll_name + "_";
        std::string upper_bound_key = std::string(Collection::COLLECTION_OVERRIDE_PREFIX) + "_" +
                                      actual_coll_name + "`";  // cannot inline this
        rocksdb::Slice upper_bound(upper_bound_key);

        rocksdb::Iterator* iter = store->scan(del_override_prefix, &upper_bound);
        while(iter->Valid() && iter->key().starts_with(del_override_prefix)) {
            store->remove(iter->key().ToString());
            iter->Next();
        }
        delete iter;

        // delete synonyms
        const std::string& del_synonym_prefix =
                std::string(SynonymIndex::COLLECTION_SYNONYM_PREFIX) + "_" + actual_coll_name + "_";

        std::string syn_upper_bound_key = std::string(SynonymIndex::COLLECTION_SYNONYM_PREFIX) + "_" +
                                      actual_coll_name + "`";  // cannot inline this
        rocksdb::Slice syn_upper_bound(syn_upper_bound_key);

        iter = store->scan(del_synonym_prefix, &syn_upper_bound);
        while(iter->Valid() && iter->key().starts_with(del_synonym_prefix)) {
            store->remove(iter->key().ToString());
            iter->Next();
        }
        delete iter;

        store->remove(Collection::get_next_seq_id_key(actual_coll_name));
        store->remove(Collection::get_meta_key(actual_coll_name));
    }

    s_lock.unlock();

    std::unique_lock u_lock(mutex);
    collections.erase(actual_coll_name);
    collection_id_names.erase(collection->get_collection_id());
    u_lock.unlock();

    // don't hold any collection manager locks here, since this can take some time
    delete collection;

    return Option<nlohmann::json>(collection_json);
}

uint32_t CollectionManager::get_next_collection_id() const {
    return next_collection_id;
}

std::string CollectionManager::get_symlink_key(const std::string & symlink_name) {
    return std::string(SYMLINK_PREFIX) + "_" + symlink_name;
}

spp::sparse_hash_map<std::string, std::string> CollectionManager::get_symlinks() const {
    std::shared_lock lock(mutex);
    return collection_symlinks;
}

Option<std::string> CollectionManager::resolve_symlink(const std::string & symlink_name) const {
    std::shared_lock lock(mutex);

    if(collection_symlinks.count(symlink_name) != 0) {
        return Option<std::string>(collection_symlinks.at(symlink_name));
    }

    return Option<std::string>(404, "Not found.");
}

Option<bool> CollectionManager::upsert_symlink(const std::string & symlink_name, const std::string & collection_name) {
    std::unique_lock lock(mutex);

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
    std::unique_lock lock(mutex);

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

bool CollectionManager::parse_sort_by_str(std::string sort_by_str, std::vector<sort_by>& sort_fields) {
    std::string sort_field_expr;
    char prev_non_space_char = 'a';

    for(size_t i=0; i < sort_by_str.size(); i++) {
        if (sort_field_expr.empty()) {
            if (sort_by_str[i] == '$') {
                // Sort by reference field
                auto open_paren_pos = sort_by_str.find('(', i);
                if (open_paren_pos == std::string::npos) {
                    return false;
                }
                sort_field_expr = sort_by_str.substr(i, open_paren_pos - i + 1);

                i = open_paren_pos;
                int paren_count = 1;
                while (++i < sort_by_str.size() && paren_count > 0) {
                    if (sort_by_str[i] == '(') {
                        paren_count++;
                    } else if (sort_by_str[i] == ')') {
                        paren_count--;
                    }
                    sort_field_expr += sort_by_str[i];
                }
                if (paren_count != 0) {
                    return false;
                }

                sort_fields.emplace_back(sort_field_expr, "");
                sort_field_expr = "";
                continue;
            } else if (sort_by_str.substr(i, 5) == sort_field_const::eval) {
                // Optional filtering
                auto open_paren_pos = sort_by_str.find('(', i);
                if (open_paren_pos == std::string::npos) {
                    return false;
                }
                sort_field_expr = sort_field_const::eval + "(";

                i = open_paren_pos;
                int paren_count = 1;
                while (++i < sort_by_str.size() && paren_count > 0) {
                    if (sort_by_str[i] == '(') {
                        paren_count++;
                    } else if (sort_by_str[i] == ')') {
                        paren_count--;
                    }
                    sort_field_expr += sort_by_str[i];
                }
                if (paren_count != 0 || i >= sort_by_str.size()) {
                    return false;
                }

                while (sort_by_str[i] != ':' && ++i < sort_by_str.size());
                if (i >= sort_by_str.size()) {
                    return false;
                }

                std::string order_str;
                while (++i < sort_by_str.size() && sort_by_str[i] != ',') {
                    order_str += sort_by_str[i];
                }
                StringUtils::trim(order_str);
                StringUtils::toupper(order_str);

                sort_fields.emplace_back(sort_field_expr, order_str);
                sort_field_expr = "";
                continue;
            }
        }

        if(i == sort_by_str.size()-1 || (sort_by_str[i] == ',' && !isdigit(prev_non_space_char))) {
            if(i == sort_by_str.size()-1) {
                sort_field_expr += sort_by_str[i];
            }

            int colon_index = sort_field_expr.size()-1;

            while(colon_index >= 0) {
                if(sort_field_expr[colon_index] == ':') {
                    break;
                }

                colon_index--;
            }

            if(colon_index < 0 || colon_index+1 == sort_field_expr.size()) {
                return false;
            }

            std::string order_str = sort_field_expr.substr(colon_index+1, sort_field_expr.size()-colon_index+1);
            StringUtils::trim(order_str);
            StringUtils::toupper(order_str);

            std::string field_name = sort_field_expr.substr(0, colon_index);
            StringUtils::trim(field_name);

            sort_fields.emplace_back(field_name, order_str);
            sort_field_expr = "";
        } else {
            sort_field_expr += sort_by_str[i];
        }

        if(sort_by_str[i] != ' ') {
            prev_non_space_char = sort_by_str[i];
        }
    }

    return true;
}

Option<bool> add_unsigned_int_param(const std::string& param_name, const std::string& str_val, size_t* int_val) {
    if(!StringUtils::is_uint32_t(str_val)) {
        return Option<bool>(400, "Parameter `" + std::string(param_name) + "` must be an unsigned integer.");
    }

    *int_val = std::stoi(str_val);
    return Option<bool>(true);
}

Option<bool> add_unsigned_int_list_param(const std::string& param_name, const std::string& str_val,
                                         std::vector<uint32_t>* int_vals) {
    std::vector<std::string> str_vals;
    StringUtils::split(str_val, str_vals, ",");
    int_vals->clear();

    for(auto& str : str_vals) {
        if(StringUtils::is_uint32_t(str)) {
            int_vals->push_back((uint32_t)std::stoi(str));
        } else {
            return Option<bool>(400, "Parameter `" + param_name + "` is malformed.");
        }
    }

    return Option<bool>(true);
}

void CollectionManager::_get_reference_collection_names(const std::string& filter_query,
                                                        std::set<std::string>& reference_collection_names) {
    auto size = filter_query.size();
    for (uint32_t i = 0; i < size;) {
        auto c = filter_query[i];
        if (c == ' ' || c == '(' || c == ')') {
            i++;
        } else if (c == '&' || c == '|') {
            i += 2;
        } else {
            // Reference filter would start with $ symbol.
            if (c == '$') {
                auto open_paren_pos = filter_query.find('(', ++i);
                if (open_paren_pos == std::string::npos) {
                    return;
                }

                auto reference_collection_name = filter_query.substr(i, open_paren_pos - i);
                StringUtils::trim(reference_collection_name);
                if (!reference_collection_name.empty()) {
                    reference_collection_names.insert(reference_collection_name);
                }

                i = open_paren_pos;
                int parenthesis_count = 1;
                while (++i < size && parenthesis_count > 0) {
                    if (filter_query[i] == '(') {
                        parenthesis_count++;
                    } else if (filter_query[i] == ')') {
                        parenthesis_count--;
                    }
                }
            } else {
                while (filter_query[++i] != ':');
                bool in_backtick = false;
                do {
                    c = filter_query[++i];
                    if (c == '`') {
                        in_backtick = !in_backtick;
                    }
                } while (i < size && (in_backtick || (c != '(' && c != ')' &&
                                                      !(c == '&' && filter_query[i + 1] == '&') &&
                                                      !(c == '|' && filter_query[i + 1] == '|'))));
            }
        }
    }
}

// Separate out the reference includes into `ref_include_fields_vec`.
void initialize_ref_include_fields_vec(const std::string& filter_query, std::vector<std::string>& include_fields_vec,
                                       std::vector<ref_include_fields>& ref_include_fields_vec) {
    std::set<std::string> reference_collection_names;
    CollectionManager::_get_reference_collection_names(filter_query, reference_collection_names);

    std::vector<std::string> result_include_fields_vec;
    auto wildcard_include_all = true;
    for (auto include_field_exp: include_fields_vec) {
        if (include_field_exp[0] != '$') {
            if (include_field_exp == "*") {
                continue;
            }

            wildcard_include_all = false;
            result_include_fields_vec.emplace_back(include_field_exp);
            continue;
        }

        auto as_pos = include_field_exp.find(" as ");
        auto ref_include = include_field_exp.substr(0, as_pos),
                alias = (as_pos == std::string::npos) ? "" :
                        include_field_exp.substr(as_pos + 4, include_field_exp.size() - (as_pos + 4));

        // For an alias `foo`, we need append `foo.` to all the top level keys of reference doc.
        ref_include_fields_vec.emplace_back(ref_include_fields{ref_include, alias.empty() ? alias :
                                                                                StringUtils::trim(alias) + "."});

        auto open_paren_pos = include_field_exp.find('(');
        if (open_paren_pos == std::string::npos) {
            continue;
        }

        auto reference_collection_name = include_field_exp.substr(1, open_paren_pos - 1);
        StringUtils::trim(reference_collection_name);
        if (reference_collection_name.empty()) {
            continue;
        }

        // Referenced collection in filter_query is already mentioned in ref_include_fields.
        reference_collection_names.erase(reference_collection_name);
    }

    // Get all the fields of the referenced collection in the filter but not mentioned in include_fields.
    for (const auto &reference_collection_name: reference_collection_names) {
        ref_include_fields_vec.emplace_back(ref_include_fields{"$" + reference_collection_name + "(*)", ""});
    }

    // Since no field of the collection is mentioned in include_fields, get all the fields.
    if (wildcard_include_all) {
        result_include_fields_vec.clear();
    }

    include_fields_vec = std::move(result_include_fields_vec);
}

Option<bool> CollectionManager::do_search(std::map<std::string, std::string>& req_params,
                                          nlohmann::json& embedded_params,
                                          std::string& results_json_str,
                                          uint64_t start_ts) {

    auto begin = std::chrono::high_resolution_clock::now();

    const char *NUM_TYPOS = "num_typos";
    const char *MIN_LEN_1TYPO = "min_len_1typo";
    const char *MIN_LEN_2TYPO = "min_len_2typo";

    const char *PREFIX = "prefix";
    const char *DROP_TOKENS_THRESHOLD = "drop_tokens_threshold";
    const char *TYPO_TOKENS_THRESHOLD = "typo_tokens_threshold";
    const char *FILTER = "filter_by";
    const char *QUERY = "q";
    const char *QUERY_BY = "query_by";
    const char *QUERY_BY_WEIGHTS = "query_by_weights";
    const char *SORT_BY = "sort_by";

    const char *FACET_BY = "facet_by";
    const char *FACET_QUERY = "facet_query";
    const char *FACET_QUERY_NUM_TYPOS = "facet_query_num_typos";
    const char *MAX_FACET_VALUES = "max_facet_values";

    const char *FACET_RETURN_PARENT = "facet_return_parent";

    const char *VECTOR_QUERY = "vector_query";

    const char* REMOTE_EMBEDDING_TIMEOUT_MS = "remote_embedding_timeout_ms";
    const char* REMOTE_EMBEDDING_NUM_TRIES = "remote_embedding_num_tries";

    const char *GROUP_BY = "group_by";
    const char *GROUP_LIMIT = "group_limit";

    const char *LIMIT_HITS = "limit_hits";
    const char *PER_PAGE = "per_page";
    const char *PAGE = "page";
    const char *OFFSET = "offset";
    const char *LIMIT = "limit";
    const char *RANK_TOKENS_BY = "rank_tokens_by";
    const char *INCLUDE_FIELDS = "include_fields";
    const char *EXCLUDE_FIELDS = "exclude_fields";

    const char *PINNED_HITS = "pinned_hits";
    const char *HIDDEN_HITS = "hidden_hits";
    const char *ENABLE_OVERRIDES = "enable_overrides";
    const char *FILTER_CURATED_HITS = "filter_curated_hits";

    const char *MAX_CANDIDATES = "max_candidates";

    const char *INFIX = "infix";
    const char *MAX_EXTRA_PREFIX = "max_extra_prefix";
    const char *MAX_EXTRA_SUFFIX = "max_extra_suffix";

    // strings under this length will be fully highlighted, instead of showing a snippet of relevant portion
    const char *SNIPPET_THRESHOLD = "snippet_threshold";

    // the number of tokens that should surround the highlighted text
    const char *HIGHLIGHT_AFFIX_NUM_TOKENS = "highlight_affix_num_tokens";

    // list of fields which will be highlighted fully without snippeting
    const char *HIGHLIGHT_FULL_FIELDS = "highlight_full_fields";
    const char *HIGHLIGHT_FIELDS = "highlight_fields";

    const char *HIGHLIGHT_START_TAG = "highlight_start_tag";
    const char *HIGHLIGHT_END_TAG = "highlight_end_tag";

    const char *PRIORITIZE_EXACT_MATCH = "prioritize_exact_match";
    const char *PRIORITIZE_TOKEN_POSITION = "prioritize_token_position";
    const char *PRE_SEGMENTED_QUERY = "pre_segmented_query";

    const char *SEARCH_CUTOFF_MS = "search_cutoff_ms";
    const char *EXHAUSTIVE_SEARCH = "exhaustive_search";
    const char *SPLIT_JOIN_TOKENS = "split_join_tokens";

    const char *TEXT_MATCH_TYPE = "text_match_type";

    const char *ENABLE_HIGHLIGHT_V1 = "enable_highlight_v1";

    const char *FACET_SAMPLE_PERCENT = "facet_sample_percent";
    const char *FACET_SAMPLE_THRESHOLD = "facet_sample_threshold";

    // enrich params with values from embedded params
    for(auto& item: embedded_params.items()) {
        if(item.key() == "expires_at") {
            continue;
        }

        // overwrite = true as embedded params have higher priority
        AuthManager::add_item_to_params(req_params, item, true);
    }

    const auto preset_it = req_params.find("preset");

    if(preset_it != req_params.end()) {
        nlohmann::json preset;
        const auto& preset_op = CollectionManager::get_instance().get_preset(preset_it->second, preset);

        // NOTE: we merge only single preset configuration because multi ("searches") preset value replaces
        // the request body directly before we reach this single search request function.
        if(preset_op.ok() && !preset.contains("searches")) {
            if(!preset.is_object()) {
                return Option<bool>(400, "Search preset is not an object.");
            }

            for(const auto& search_item: preset.items()) {
                // overwrite = false since req params will contain embedded params and so has higher priority
                bool populated = AuthManager::add_item_to_params(req_params, search_item, false);
                if(!populated) {
                    return Option<bool>(400, "One or more search parameters are malformed.");
                }
            }
        }
    }

    //check if stopword set is supplied
    const auto stopword_it = req_params.find("stopwords");
    std::string stopwords_set="";

    if(stopword_it != req_params.end()) {
        stopwords_set = stopword_it->second;
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();
    const std::string& orig_coll_name = req_params["collection"];
    auto collection = collectionManager.get_collection(orig_coll_name);

    if(collection == nullptr) {
        return Option<bool>(404, "Not found.");
    }

    // check presence of mandatory params here

    if(req_params.count(QUERY) == 0) {
        return Option<bool>(400, std::string("Parameter `") + QUERY + "` is required.");
    }

    // end check for mandatory params

    const std::string& raw_query = req_params[QUERY];
    std::vector<uint32_t> num_typos = {2};
    size_t min_len_1typo = 4;
    size_t min_len_2typo = 7;
    std::vector<bool> prefixes = {true};
    size_t drop_tokens_threshold = Index::DROP_TOKENS_THRESHOLD;
    size_t typo_tokens_threshold = Index::TYPO_TOKENS_THRESHOLD;

    std::vector<std::string> search_fields;
    std::string filter_query;
    std::vector<std::string> facet_fields;
    std::vector<sort_by> sort_fields;
    size_t per_page = 10;
    size_t page = 0;
    size_t offset = 0;
    token_ordering token_order = NOT_SET;

    std::vector<std::string> facet_return_parent;

    std::string vector_query;

    std::vector<std::string> include_fields_vec;
    std::vector<std::string> exclude_fields_vec;
    std::vector<ref_include_fields> ref_include_fields_vec;
    spp::sparse_hash_set<std::string> include_fields;
    spp::sparse_hash_set<std::string> exclude_fields;

    size_t max_facet_values = 10;
    std::string simple_facet_query;
    size_t facet_query_num_typos = 2;
    size_t snippet_threshold = 30;
    size_t highlight_affix_num_tokens = 4;
    std::string highlight_full_fields;
    std::string pinned_hits_str;
    std::string hidden_hits_str;
    std::vector<std::string> group_by_fields;
    size_t group_limit = 3;
    std::string highlight_start_tag = "<mark>";
    std::string highlight_end_tag = "</mark>";
    std::vector<uint32_t> query_by_weights;
    size_t limit_hits = UINT32_MAX;
    bool prioritize_exact_match = true;
    bool prioritize_token_position = false;
    bool pre_segmented_query = false;
    bool enable_overrides = true;
    size_t filter_curated_hits_option = 2;
    std::string highlight_fields;
    bool exhaustive_search = false;
    size_t search_cutoff_ms = 30 * 1000;
    enable_t split_join_tokens = fallback;
    size_t max_candidates = 0;
    std::vector<enable_t> infixes;
    size_t max_extra_prefix = INT16_MAX;
    size_t max_extra_suffix = INT16_MAX;
    bool enable_highlight_v1 = true;
    text_match_type_t match_type = max_score;

    size_t remote_embedding_timeout_ms = 5000;
    size_t remote_embedding_num_tries = 2;

    size_t facet_sample_percent = 100;
    size_t facet_sample_threshold = 0;

    std::unordered_map<std::string, size_t*> unsigned_int_values = {
        {MIN_LEN_1TYPO, &min_len_1typo},
        {MIN_LEN_2TYPO, &min_len_2typo},
        {DROP_TOKENS_THRESHOLD, &drop_tokens_threshold},
        {TYPO_TOKENS_THRESHOLD, &typo_tokens_threshold},
        {MAX_FACET_VALUES, &max_facet_values},
        {LIMIT_HITS, &limit_hits},
        {SNIPPET_THRESHOLD, &snippet_threshold},
        {HIGHLIGHT_AFFIX_NUM_TOKENS, &highlight_affix_num_tokens},
        {PAGE, &page},
        {OFFSET, &offset},
        {PER_PAGE, &per_page},
        {LIMIT, &per_page},
        {GROUP_LIMIT, &group_limit},
        {SEARCH_CUTOFF_MS, &search_cutoff_ms},
        {MAX_EXTRA_PREFIX, &max_extra_prefix},
        {MAX_EXTRA_SUFFIX, &max_extra_suffix},
        {MAX_CANDIDATES, &max_candidates},
        {FACET_QUERY_NUM_TYPOS, &facet_query_num_typos},
        {FILTER_CURATED_HITS, &filter_curated_hits_option},
        {FACET_SAMPLE_PERCENT, &facet_sample_percent},
        {FACET_SAMPLE_THRESHOLD, &facet_sample_threshold},
        {REMOTE_EMBEDDING_TIMEOUT_MS, &remote_embedding_timeout_ms},
        {REMOTE_EMBEDDING_NUM_TRIES, &remote_embedding_num_tries},
    };

    std::unordered_map<std::string, std::string*> str_values = {
        {FILTER, &filter_query},
        {VECTOR_QUERY, &vector_query},
        {FACET_QUERY, &simple_facet_query},
        {HIGHLIGHT_FIELDS, &highlight_fields},
        {HIGHLIGHT_FULL_FIELDS, &highlight_full_fields},
        {HIGHLIGHT_START_TAG, &highlight_start_tag},
        {HIGHLIGHT_END_TAG, &highlight_end_tag},
        {PINNED_HITS, &pinned_hits_str},
        {HIDDEN_HITS, &hidden_hits_str},
    };

    std::unordered_map<std::string, bool*> bool_values = {
        {PRIORITIZE_EXACT_MATCH, &prioritize_exact_match},
        {PRIORITIZE_TOKEN_POSITION, &prioritize_token_position},
        {PRE_SEGMENTED_QUERY, &pre_segmented_query},
        {EXHAUSTIVE_SEARCH, &exhaustive_search},
        {ENABLE_OVERRIDES, &enable_overrides},
        {ENABLE_HIGHLIGHT_V1, &enable_highlight_v1},
    };

    std::unordered_map<std::string, std::vector<std::string>*> str_list_values = {
        {QUERY_BY, &search_fields},
        {FACET_BY, &facet_fields},
        {GROUP_BY, &group_by_fields},
        {INCLUDE_FIELDS, &include_fields_vec},
        {EXCLUDE_FIELDS, &exclude_fields_vec},
        {FACET_RETURN_PARENT, &facet_return_parent},
    };

    std::unordered_map<std::string, std::vector<uint32_t>*> int_list_values = {
        {QUERY_BY_WEIGHTS, &query_by_weights},
        {NUM_TYPOS, &num_typos},
    };

    for(const auto& kv: req_params) {
        const std::string& key = kv.first;
        const std::string& val = kv.second;

        if(key == PREFIX) {
            if(val == "true" || val == "false") {
                prefixes = {(val == "true")};
            } else {
                prefixes.clear();
                std::vector<std::string> prefix_str;
                StringUtils::split(val, prefix_str, ",");
                for(auto& prefix_s : prefix_str) {
                    prefixes.push_back(prefix_s == "true");
                }
            }
        }

        else if(key == SPLIT_JOIN_TOKENS) {
            if(val == "false") {
                split_join_tokens = off;
            } else if(val == "true") {
                split_join_tokens = fallback;
            } else {
                auto enable_op = magic_enum::enum_cast<enable_t>(val);
                if(enable_op.has_value()) {
                    split_join_tokens = enable_op.value();
                }
            }
        }

        else if(key == TEXT_MATCH_TYPE) {
            auto match_op = magic_enum::enum_cast<text_match_type_t>(val);
            if(match_op.has_value()) {
                match_type = match_op.value();
            }
        }

        else {
            auto find_int_it = unsigned_int_values.find(key);
            if(find_int_it != unsigned_int_values.end()) {
                const auto& op = add_unsigned_int_param(key, val, find_int_it->second);
                if(!op.ok()) {
                    return op;
                }

                continue;
            }

            auto find_str_it = str_values.find(key);
            if(find_str_it != str_values.end()) {
                *find_str_it->second = val;
                continue;
            }

            auto find_bool_it = bool_values.find(key);
            if(find_bool_it != bool_values.end()) {
                *find_bool_it->second = (val == "true");
                continue;
            }

            auto find_str_list_it = str_list_values.find(key);
            if(find_str_list_it != str_list_values.end()) {

                if(key == FACET_BY){
                    StringUtils::split_facet(val, *find_str_list_it->second);
                }
                else if(key == INCLUDE_FIELDS){
                    auto op = StringUtils::split_include_fields(val, *find_str_list_it->second);
                    if (!op.ok()) {
                        return op;
                    }
                }
                else{
                    StringUtils::split(val, *find_str_list_it->second, ",");
                }
                continue;
            }

            auto find_int_list_it = int_list_values.find(key);
            if(find_int_list_it != int_list_values.end()) {
                add_unsigned_int_list_param(key, val, find_int_list_it->second);
                continue;
            }
        }
    }

    // special defaults
    if(!req_params[FACET_QUERY].empty() && req_params.count(PER_PAGE) == 0) {
        // for facet query we will set per_page to zero if it is not explicitly overridden
        per_page = 0;
    }

    initialize_ref_include_fields_vec(filter_query, include_fields_vec, ref_include_fields_vec);

    include_fields.insert(include_fields_vec.begin(), include_fields_vec.end());
    exclude_fields.insert(exclude_fields_vec.begin(), exclude_fields_vec.end());

    bool parsed_sort_by = parse_sort_by_str(req_params[SORT_BY], sort_fields);

    if(!parsed_sort_by) {
        return Option<bool>(400,std::string("Parameter `") + SORT_BY + "` is malformed.");
    }

    if(sort_fields.size() > 3) {
        return Option<bool>(400, "Only upto 3 sort fields are allowed.");
    }

    if(req_params.count(INFIX) != 0) {
        std::vector<std::string> infix_strs;
        StringUtils::split(req_params[INFIX], infix_strs, ",");

        for(auto& infix_str: infix_strs) {
            auto infix_op = magic_enum::enum_cast<enable_t>(infix_str);
            if(infix_op.has_value()) {
                infixes.push_back(infix_op.value());
            }
        }
    } else {
        infixes.push_back(off);
    }

    if(req_params.count(RANK_TOKENS_BY) != 0) {
        StringUtils::toupper(req_params[RANK_TOKENS_BY]);
        if (req_params[RANK_TOKENS_BY] == "DEFAULT_SORTING_FIELD") {
            token_order = MAX_SCORE;
        } else if(req_params[RANK_TOKENS_BY] == "FREQUENCY") {
            token_order = FREQUENCY;
        }
    }

    if(!max_candidates) {
        max_candidates = exhaustive_search ? Index::COMBINATION_MAX_LIMIT :
                         (collection->get_num_documents() < 500000 ? Index::NUM_CANDIDATES_DEFAULT_MAX :
                          Index::NUM_CANDIDATES_DEFAULT_MIN);
    }

    Option<nlohmann::json> result_op = collection->search(raw_query, search_fields, filter_query, facet_fields,
                                                          sort_fields, num_typos,
                                                          per_page,
                                                          page,
                                                          token_order, prefixes, drop_tokens_threshold,
                                                          include_fields, exclude_fields,
                                                          max_facet_values,
                                                          simple_facet_query,
                                                          snippet_threshold,
                                                          highlight_affix_num_tokens,
                                                          highlight_full_fields,
                                                          typo_tokens_threshold,
                                                          pinned_hits_str,
                                                          hidden_hits_str,
                                                          group_by_fields,
                                                          group_limit,
                                                          highlight_start_tag,
                                                          highlight_end_tag,
                                                          query_by_weights,
                                                          limit_hits,
                                                          prioritize_exact_match,
                                                          pre_segmented_query,
                                                          enable_overrides,
                                                          highlight_fields,
                                                          exhaustive_search,
                                                          search_cutoff_ms,
                                                          min_len_1typo,
                                                          min_len_2typo,
                                                          split_join_tokens,
                                                          max_candidates,
                                                          infixes,
                                                          max_extra_prefix,
                                                          max_extra_suffix,
                                                          facet_query_num_typos,
                                                          filter_curated_hits_option,
                                                          prioritize_token_position,
                                                          vector_query,
                                                          enable_highlight_v1,
                                                          start_ts,
                                                          match_type,
                                                          facet_sample_percent,
                                                          facet_sample_threshold,
                                                          offset,
                                                          HASH,
                                                          remote_embedding_timeout_ms,
                                                          remote_embedding_num_tries,
                                                          stopwords_set,
                                                          facet_return_parent,
                                                          ref_include_fields_vec);

    uint64_t timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - begin).count();

    AppMetrics::get_instance().increment_count(AppMetrics::SEARCH_LABEL, 1);
    AppMetrics::get_instance().increment_duration(AppMetrics::SEARCH_LABEL, timeMillis);

    if(!result_op.ok()) {
        return Option<bool>(result_op.code(), result_op.error());
    }

    nlohmann::json result = result_op.get();

    if(Config::get_instance().get_enable_search_analytics()) {
        if(result.count("found") != 0 && result["found"].get<size_t>() != 0) {
            std::string analytics_query = Tokenizer::normalize_ascii_no_spaces(raw_query);
            AnalyticsManager::get_instance().add_suggestion(orig_coll_name, analytics_query,
                                                            true, req_params["x-typesense-user-id"]);
        }
    }

    if(exclude_fields.count("search_time_ms") == 0) {
        result["search_time_ms"] = timeMillis;
    }

    if(page == 0 && offset != 0) {
        result["offset"] = offset;
    } else {
        result["page"] = (page == 0) ? 1 : page;
    }

    results_json_str = result.dump(-1, ' ', false, nlohmann::detail::error_handler_t::ignore);

    //LOG(INFO) << "Time taken: " << timeMillis << "ms";

    return Option<bool>(true);
}

ThreadPool* CollectionManager::get_thread_pool() const {
    return thread_pool;
}

nlohmann::json CollectionManager::get_collection_summaries() const {
    std::shared_lock lock(mutex);

    std::vector<Collection*> colls = get_collections();
    nlohmann::json json_summaries = nlohmann::json::array();

    for(Collection* collection: colls) {
        nlohmann::json collection_json = collection->get_summary_json();
        json_summaries.push_back(collection_json);
    }

    return json_summaries;
}

Option<Collection*> CollectionManager::create_collection(nlohmann::json& req_json) {
    const char* NUM_MEMORY_SHARDS = "num_memory_shards";
    const char* SYMBOLS_TO_INDEX = "symbols_to_index";
    const char* TOKEN_SEPARATORS = "token_separators";
    const char* ENABLE_NESTED_FIELDS = "enable_nested_fields";
    const char* DEFAULT_SORTING_FIELD = "default_sorting_field";

    // validate presence of mandatory fields

    if(req_json.count("name") == 0) {
        return Option<Collection*>(400, "Parameter `name` is required.");
    }

    if(!req_json["name"].is_string() || req_json["name"].get<std::string>().empty()) {
        return Option<Collection*>(400, "Parameter `name` must be a non-empty string.");
    }

    if(req_json.count(NUM_MEMORY_SHARDS) == 0) {
        req_json[NUM_MEMORY_SHARDS] = CollectionManager::DEFAULT_NUM_MEMORY_SHARDS;
    }

    if(req_json.count(SYMBOLS_TO_INDEX) == 0) {
        req_json[SYMBOLS_TO_INDEX] = std::vector<std::string>();
    }

    if(req_json.count(TOKEN_SEPARATORS) == 0) {
        req_json[TOKEN_SEPARATORS] = std::vector<std::string>();
    }

    if(req_json.count(ENABLE_NESTED_FIELDS) == 0) {
        req_json[ENABLE_NESTED_FIELDS] = false;
    }

    if(req_json.count("fields") == 0) {
        return Option<Collection*>(400, "Parameter `fields` is required.");
    }

    if(req_json.count(DEFAULT_SORTING_FIELD) == 0) {
        req_json[DEFAULT_SORTING_FIELD] = "";
    }

    if(!req_json[DEFAULT_SORTING_FIELD].is_string()) {
        return Option<Collection*>(400, std::string("`") + DEFAULT_SORTING_FIELD +
                                        "` should be a string. It should be the name of an int32/float field.");
    }

    if(!req_json[NUM_MEMORY_SHARDS].is_number_unsigned()) {
        return Option<Collection*>(400, std::string("`") + NUM_MEMORY_SHARDS + "` should be a positive integer.");
    }

    if(!req_json[SYMBOLS_TO_INDEX].is_array()) {
        return Option<Collection*>(400, std::string("`") + SYMBOLS_TO_INDEX + "` should be an array of character symbols.");
    }

    if(!req_json[TOKEN_SEPARATORS].is_array()) {
        return Option<Collection*>(400, std::string("`") + TOKEN_SEPARATORS + "` should be an array of character symbols.");
    }

    if(!req_json[ENABLE_NESTED_FIELDS].is_boolean()) {
        return Option<Collection*>(400, std::string("`") + ENABLE_NESTED_FIELDS + "` should be a boolean.");
    }

    for (auto it = req_json[SYMBOLS_TO_INDEX].begin(); it != req_json[SYMBOLS_TO_INDEX].end(); ++it) {
        if(!it->is_string() || it->get<std::string>().size() != 1 ) {
            return Option<Collection*>(400, std::string("`") + SYMBOLS_TO_INDEX + "` should be an array of character symbols.");
        }
    }

    for (auto it = req_json[TOKEN_SEPARATORS].begin(); it != req_json[TOKEN_SEPARATORS].end(); ++it) {
        if(!it->is_string() || it->get<std::string>().size() != 1 ) {
            return Option<Collection*>(400, std::string("`") + TOKEN_SEPARATORS + "` should be an array of character symbols.");
        }
    }

    size_t num_memory_shards = req_json[NUM_MEMORY_SHARDS].get<size_t>();
    if(num_memory_shards == 0) {
        return Option<Collection*>(400, std::string("`") + NUM_MEMORY_SHARDS + "` should be a positive integer.");
    }

    // field specific validation

    if(!req_json["fields"].is_array() || req_json["fields"].empty()) {
        return Option<Collection *>(400, "The `fields` value should be an array of objects containing "
                     "`name`, `type` and optionally, `facet` properties.");
    }

    const std::string& default_sorting_field = req_json[DEFAULT_SORTING_FIELD].get<std::string>();

    if(default_sorting_field == "id") {
        return Option<Collection *>(400, "Invalid `default_sorting_field` value: cannot be `id`.");
    }

    std::string fallback_field_type;
    std::vector<field> fields;
    auto parse_op = field::json_fields_to_fields(req_json[ENABLE_NESTED_FIELDS].get<bool>(),
                                                 req_json["fields"], fallback_field_type, fields);

    if(!parse_op.ok()) {
        return Option<Collection*>(parse_op.code(), parse_op.error());
    }

    const auto created_at = static_cast<uint64_t>(std::time(nullptr));

    return CollectionManager::get_instance().create_collection(req_json["name"], num_memory_shards,
                                                                fields, default_sorting_field, created_at,
                                                                fallback_field_type,
                                                                req_json[SYMBOLS_TO_INDEX],
                                                                req_json[TOKEN_SEPARATORS],
                                                                req_json[ENABLE_NESTED_FIELDS]);
}

Option<bool> CollectionManager::load_collection(const nlohmann::json &collection_meta,
                                                const size_t batch_size,
                                                const StoreStatus& next_coll_id_status,
                                                const std::atomic<bool>& quit) {

    auto& cm = CollectionManager::get_instance();

    if(!collection_meta.contains(Collection::COLLECTION_NAME_KEY)) {
        return Option<bool>(500, "No collection name in collection meta: " + collection_meta.dump());
    }

    if(!collection_meta[Collection::COLLECTION_NAME_KEY].is_string()) {
        LOG(ERROR) << collection_meta[Collection::COLLECTION_NAME_KEY];
        LOG(ERROR) << Collection::COLLECTION_NAME_KEY;
        LOG(ERROR) << "";
    }
    const std::string & this_collection_name = collection_meta[Collection::COLLECTION_NAME_KEY].get<std::string>();

    std::string collection_next_seq_id_str;
    StoreStatus next_seq_id_status = cm.store->get(Collection::get_next_seq_id_key(this_collection_name),
                                                collection_next_seq_id_str);

    if(next_seq_id_status == StoreStatus::ERROR) {
        LOG(ERROR) << "Error while fetching next sequence ID for " << this_collection_name;
        return Option<bool>(500, "Error while fetching collection's next sequence ID from the disk for "
                                 "`" + this_collection_name + "`");
    }

    if(next_seq_id_status == StoreStatus::NOT_FOUND && next_coll_id_status == StoreStatus::FOUND) {
        LOG(ERROR) << "collection's next sequence ID is missing";
        return Option<bool>(500, "Next collection id was found, but collection's next sequence ID is missing for "
                                 "`" + this_collection_name + "`");
    }

    uint32_t collection_next_seq_id = next_seq_id_status == StoreStatus::NOT_FOUND ? 0 :
                                      StringUtils::deserialize_uint32_t(collection_next_seq_id_str);

    {
        std::shared_lock lock(cm.mutex);
        Collection *existing_collection = cm.get_collection_unsafe(this_collection_name);

        if(existing_collection != nullptr) {
            // To maintain idempotency, if the collection already exists in-memory, drop it from memory
            LOG(WARNING) << "Dropping duplicate collection " << this_collection_name << " before loading it again.";
            lock.unlock();
            cm.drop_collection(this_collection_name, false);
        }
    }

    Collection* collection = init_collection(collection_meta, collection_next_seq_id, cm.store, 1.0f);

    LOG(INFO) << "Loading collection " << collection->get_name();

    // initialize overrides
    std::vector<std::string> collection_override_jsons;
    cm.store->scan_fill(Collection::get_override_key(this_collection_name, ""),
                        std::string(Collection::COLLECTION_OVERRIDE_PREFIX) + "_" + this_collection_name + "`",
                        collection_override_jsons);

    for(const auto & collection_override_json: collection_override_jsons) {
        nlohmann::json collection_override = nlohmann::json::parse(collection_override_json);
        override_t override;
        auto parse_op = override_t::parse(collection_override, "", override, "", collection->get_symbols_to_index(),
                                          collection->get_token_separators());
        if(parse_op.ok()) {
            collection->add_override(override, false);
        } else {
            LOG(ERROR) << "Skipping loading of override: " << parse_op.error();
        }
    }

    // initialize synonyms
    std::vector<std::string> collection_synonym_jsons;
    cm.store->scan_fill(SynonymIndex::get_synonym_key(this_collection_name, ""),
                        std::string(SynonymIndex::COLLECTION_SYNONYM_PREFIX) + "_" + this_collection_name + "`",
                        collection_synonym_jsons);

    for(const auto & collection_synonym_json: collection_synonym_jsons) {
        nlohmann::json collection_synonym = nlohmann::json::parse(collection_synonym_json);
        collection->add_synonym(collection_synonym, false);
    }

    // Fetch records from the store and re-create memory index
    const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();
    std::string upper_bound_key = collection->get_seq_id_collection_prefix() + "`";  // cannot inline this
    rocksdb::Slice upper_bound(upper_bound_key);

    rocksdb::Iterator* iter = cm.store->scan(seq_id_prefix, &upper_bound);
    std::unique_ptr<rocksdb::Iterator> iter_guard(iter);

    std::vector<index_record> index_records;

    size_t num_found_docs = 0;
    size_t num_valid_docs = 0;
    size_t num_indexed_docs = 0;
    size_t batch_doc_str_size = 0;

    auto begin = std::chrono::high_resolution_clock::now();

    while(iter->Valid() && iter->key().starts_with(seq_id_prefix)) {
        num_found_docs++;
        const uint32_t seq_id = Collection::get_seq_id_from_key(iter->key().ToString());

        nlohmann::json document;
        const std::string& doc_string = iter->value().ToString();

        try {
            document = nlohmann::json::parse(doc_string);
        } catch(const std::exception& e) {
            LOG(ERROR) << "JSON error: " << e.what();
            return Option<bool>(400, "Bad JSON.");
        }

        batch_doc_str_size += doc_string.size();

        if(collection->get_enable_nested_fields()) {
            std::vector<field> flattened_fields;
            field::flatten_doc(document, collection->get_nested_fields(), {}, true, flattened_fields);
        }

        auto dirty_values = DIRTY_VALUES::COERCE_OR_DROP;

        num_valid_docs++;

        index_records.emplace_back(index_record(0, seq_id, document, CREATE, dirty_values));

        // Peek and check for last record right here so that we handle batched indexing correctly
        // Without doing this, the "last batch" would have to be indexed outside the loop.
        iter->Next();
        bool last_record = !(iter->Valid() && iter->key().starts_with(seq_id_prefix));

        // if expected memory usage exceeds 250M, we index the accumulated set without caring about batch size
        bool exceeds_batch_mem_threshold = ((batch_doc_str_size * 7) > (250 * 1014 * 1024));

        // batch must match atleast the number of shards
         if(exceeds_batch_mem_threshold || (num_valid_docs % batch_size == 0) || last_record) {
            size_t num_records = index_records.size();
            size_t num_indexed = collection->batch_index_in_memory(index_records, 200, false);
            batch_doc_str_size = 0;

            if(num_indexed != num_records) {
                const Option<std::string> & index_error_op = get_first_index_error(index_records);
                if(!index_error_op.ok()) {
                    return Option<bool>(400, index_error_op.get());
                }
            }

            index_records.clear();
            num_indexed_docs += num_indexed;
        }

        if(num_found_docs % ((1 << 14)) == 0) {
            // having a cheaper higher layer check to prevent checking clock too often
            auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::high_resolution_clock::now() - begin).count();

            if(time_elapsed > 30) {
                begin = std::chrono::high_resolution_clock::now();
                LOG(INFO) << "Loaded " << num_found_docs << " documents from " << collection->get_name() << " so far.";
            }
        }

        if(quit) {
            break;
        }
    }

    cm.add_to_collections(collection);

    LOG(INFO) << "Indexed " << num_indexed_docs << "/" << num_found_docs
              << " documents into collection " << collection->get_name();

    return Option<bool>(true);
}

spp::sparse_hash_map<std::string, nlohmann::json> CollectionManager::get_presets() const {
    std::shared_lock lock(mutex);
    return preset_configs;
}

Option<bool> CollectionManager::get_preset(const string& preset_name, nlohmann::json& preset) const {
    std::shared_lock lock(mutex);

    const auto& it = preset_configs.find(preset_name);
    if(it != preset_configs.end()) {
        preset = it->second;
        return Option<bool>(true);
    }

    return Option<bool>(404, "Not found.");
}

Option<bool> CollectionManager::upsert_preset(const string& preset_name, const nlohmann::json& preset_config) {
    std::unique_lock lock(mutex);

    bool inserted = store->insert(get_preset_key(preset_name), preset_config.dump());
    if(!inserted) {
        return Option<bool>(500, "Unable to insert into store.");
    }

    preset_configs[preset_name] = preset_config;
    return Option<bool>(true);
}

std::string CollectionManager::get_preset_key(const string& preset_name) {
    return std::string(PRESET_PREFIX) + "_" + preset_name;
}

Option<bool> CollectionManager::delete_preset(const string& preset_name) {
    std::unique_lock lock(mutex);

    bool removed = store->remove(get_preset_key(preset_name));
    if(!removed) {
        return Option<bool>(500, "Unable to delete from store.");
    }

    preset_configs.erase(preset_name);
    return Option<bool>(true);
}

Option<Collection*> CollectionManager::clone_collection(const string& existing_name, const nlohmann::json& req_json) {
    std::shared_lock lock(mutex);

    if(collections.count(existing_name) == 0) {
        return Option<Collection*>(400, "Collection with name `" + existing_name + "` not found.");
    }

    if(req_json.count("name") == 0 || !req_json["name"].is_string()) {
        return Option<Collection*>(400, "Collection name must be provided.");
    }

    const std::string& new_name = req_json["name"].get<std::string>();

    if(collections.count(new_name) != 0) {
        return Option<Collection*>(400, "Collection with name `" + new_name + "` already exists.");
    }

    Collection* existing_coll = collections[existing_name];

    std::vector<std::string> symbols_to_index;
    std::vector<std::string> token_separators;

    for(auto c: existing_coll->get_symbols_to_index()) {
        symbols_to_index.emplace_back(1, c);
    }

    for(auto c: existing_coll->get_token_separators()) {
        token_separators.emplace_back(1, c);
    }

    lock.unlock();

    auto coll_create_op = create_collection(new_name, DEFAULT_NUM_MEMORY_SHARDS, existing_coll->get_fields(),
                              existing_coll->get_default_sorting_field(), static_cast<uint64_t>(std::time(nullptr)),
                              existing_coll->get_fallback_field_type(), symbols_to_index, token_separators,
                              existing_coll->get_enable_nested_fields());

    lock.lock();

    if(!coll_create_op.ok()) {
        return Option<Collection*>(coll_create_op.code(), coll_create_op.error());
    }

    Collection* new_coll = coll_create_op.get();

    // copy synonyms
    auto synonyms = existing_coll->get_synonyms();
    for(const auto& synonym: synonyms) {
        new_coll->get_synonym_index()->add_synonym(new_name, synonym.second);
    }

    // copy overrides
    auto overrides = existing_coll->get_overrides();
    for(const auto& override: overrides) {
        new_coll->add_override(override.second);
    }

    return Option<Collection*>(new_coll);
}

void CollectionManager::add_referenced_in_backlog(const std::string& collection_name, reference_pair&& pair) {
    std::shared_lock lock(mutex);
    referenced_in_backlog[collection_name].insert(pair);
}

std::map<std::string, std::set<reference_pair>> CollectionManager::_get_referenced_in_backlog() const {
    std::shared_lock lock(mutex);
    return referenced_in_backlog;
}