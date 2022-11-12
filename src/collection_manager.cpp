#include <string>
#include <vector>
#include <json.hpp>
#include <app_metrics.h>
#include "collection_manager.h"
#include "batched_indexer.h"
#include "logger.h"
#include "magic_enum.hpp"

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

        vector_distance_type_t vec_dist_type = vector_distance_type_t::cosine;

        if(field_obj.count(fields::vec_dist) != 0) {
            auto vec_dist_type_op = magic_enum::enum_cast<vector_distance_type_t>(fields::vec_dist);
            if(vec_dist_type_op.has_value()) {
                vec_dist_type = vec_dist_type_op.value();
            }
        }

        field f(field_obj[fields::name], field_obj[fields::type], field_obj[fields::facet],
                field_obj[fields::optional], field_obj[fields::index], field_obj[fields::locale],
                -1, field_obj[fields::infix], field_obj[fields::nested], field_obj[fields::nested_array],
                field_obj[fields::num_dim], vec_dist_type);

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
    store->scan_fill(Collection::COLLECTION_META_PREFIX, collection_meta_jsons);

    const size_t num_collections = collection_meta_jsons.size();
    LOG(INFO) << "Found " << num_collections << " collection(s) on disk.";

    ThreadPool loading_pool(collection_batch_size);

    size_t num_processed = 0;
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
                              &m_process, &cv_process, &num_processed, &next_coll_id_status, quit = quit]() {

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

    // load aliases

    std::string symlink_prefix_key = std::string(SYMLINK_PREFIX) + "_";
    rocksdb::Iterator* iter = store->scan(symlink_prefix_key);
    while(iter->Valid() && iter->key().starts_with(symlink_prefix_key)) {
        std::vector<std::string> parts;
        StringUtils::split(iter->key().ToString(), parts, symlink_prefix_key);
        collection_symlinks[parts[0]] = iter->value().ToString();
        iter->Next();
    }

    delete iter;

    // load presets

    std::string preset_prefix_key = std::string(PRESET_PREFIX) + "_";
    iter = store->scan(preset_prefix_key);
    while(iter->Valid() && iter->key().starts_with(preset_prefix_key)) {
        std::vector<std::string> parts;
        StringUtils::split(iter->key().ToString(), parts, preset_prefix_key);
        preset_configs[parts[0]] = iter->value().ToString();
        iter->Next();
    }

    delete iter;

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

    if(req_auth_key.empty()) {
        return false;
    }

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
    std::unique_lock lock(mutex);

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

    lock.unlock();
    add_to_collections(new_collection);

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
    std::unique_lock lock(mutex);

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

        // delete overrides
        const std::string& del_override_prefix =
                std::string(Collection::COLLECTION_OVERRIDE_PREFIX) + "_" + actual_coll_name + "_";
        rocksdb::Iterator* iter = store->scan(del_override_prefix);
        while(iter->Valid() && iter->key().starts_with(del_override_prefix)) {
            store->remove(iter->key().ToString());
            iter->Next();
        }
        delete iter;

        // delete synonyms
        const std::string& del_synonym_prefix =
                std::string(SynonymIndex::COLLECTION_SYNONYM_PREFIX) + "_" + actual_coll_name + "_";
        iter = store->scan(del_synonym_prefix);
        while(iter->Valid() && iter->key().starts_with(del_synonym_prefix)) {
            store->remove(iter->key().ToString());
            iter->Next();
        }
        delete iter;

        store->remove(Collection::get_next_seq_id_key(actual_coll_name));
        store->remove(Collection::get_meta_key(actual_coll_name));
    }

    collections.erase(actual_coll_name);
    collection_id_names.erase(collection->get_collection_id());

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

Option<bool> CollectionManager::do_search(std::map<std::string, std::string>& req_params,
                                          nlohmann::json& embedded_params,
                                          std::string& results_json_str,
                                          const bool aggregate_from_multiple_collections) {
    auto begin = std::chrono::high_resolution_clock::now();

    auto raw_args_op = get_raw_search_args(req_params, embedded_params);

    if(!raw_args_op.ok()) {
        return Option<bool>(raw_args_op.code(), raw_args_op.error());
    }

    nlohmann::json result;

    auto& collectionManager = CollectionManager::get_instance();

    auto raw_args = raw_args_op.get();


    bool exclude_search_time = raw_args->exclude_fields.count("search_time_ms") != 0;


    auto collection = collectionManager.get_collection(req_params["collection"]);
    if(collection == nullptr) {
        return Option<bool>(404, "Not found.");
    }
    Option<nlohmann::json> result_op = collection->search(raw_args);

    if(!result_op.ok()) {
        return Option<bool>(result_op.code(), result_op.error());
    }
    result = result_op.get();

    uint64_t timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - begin).count();

    AppMetrics::get_instance().increment_count(AppMetrics::SEARCH_LABEL, 1);
    AppMetrics::get_instance().increment_duration(AppMetrics::SEARCH_LABEL, timeMillis);


    if(!exclude_search_time) {
        result["search_time_ms"] = timeMillis;
    }

    result["page"] = raw_args->page;
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

    std::string fallback_field_type;
    std::vector<field> fields;
    auto parse_op = field::json_fields_to_fields(req_json["fields"], fallback_field_type, fields);

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
    cm.store->scan_fill(Collection::get_override_key(this_collection_name, ""), collection_override_jsons);

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
    cm.store->scan_fill(SynonymIndex::get_synonym_key(this_collection_name, ""), collection_synonym_jsons);

    for(const auto & collection_synonym_json: collection_synonym_jsons) {
        nlohmann::json collection_synonym = nlohmann::json::parse(collection_synonym_json);
        collection->add_synonym(collection_synonym);
    }

    // Fetch records from the store and re-create memory index
    const std::string seq_id_prefix = collection->get_seq_id_collection_prefix();

    rocksdb::Iterator* iter = cm.store->scan(seq_id_prefix);
    std::unique_ptr<rocksdb::Iterator> iter_guard(iter);

    std::vector<index_record> index_records;

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
            return Option<bool>(400, "Bad JSON.");
        }

        auto dirty_values = DIRTY_VALUES::DROP;

        num_valid_docs++;

        index_records.emplace_back(index_record(0, seq_id, document, CREATE, dirty_values));

        // Peek and check for last record right here so that we handle batched indexing correctly
        // Without doing this, the "last batch" would have to be indexed outside the loop.
        iter->Next();
        bool last_record = !(iter->Valid() && iter->key().starts_with(seq_id_prefix));

        // batch must match atleast the number of shards
        if((num_valid_docs % batch_size == 0) || last_record) {
            size_t num_records = index_records.size();
            size_t num_indexed = collection->batch_index_in_memory(index_records);

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

bool CollectionManager::parse_vector_query_str(std::string vector_query_str, vector_query_t& vector_query) {
    // FORMAT:
    // field_name([0.34, 0.66, 0.12, 0.68], exact: false, k: 10)
    size_t i = 0;
    while(i < vector_query_str.size()) {
        if(vector_query_str[i] != ':') {
            vector_query.field_name += vector_query_str[i];
            i++;
        } else {
            if(vector_query_str[i] != ':') {
                // missing ":"
                return false;
            }

            // field name is done
            i++;

            while(i < vector_query_str.size() && vector_query_str[i] != '(') {
                i++;
            }

            if(vector_query_str[i] != '(') {
                // missing "("
                return false;
            }

            i++;

            while(i < vector_query_str.size() && vector_query_str[i] != '[') {
                i++;
            }

            if(vector_query_str[i] != '[') {
                // missing opening "["
                return false;
            }

            i++;

            std::string values_str;
            while(i < vector_query_str.size() && vector_query_str[i] != ']') {
                values_str += vector_query_str[i];
                i++;
            }

            if(vector_query_str[i] != ']') {
                // missing closing "]"
                return false;
            }

            i++;

            std::vector<std::string> svalues;
            StringUtils::split(values_str, svalues, ",");

            for(auto& svalue: svalues) {
                if(!StringUtils::is_float(svalue)) {
                    return false;
                }

                vector_query.values.push_back(std::stof(svalue));
            }

            if(i == vector_query_str.size()-1) {
                // missing params
                return true;
            }

            std::string param_str = vector_query_str.substr(i, (vector_query_str.size() - i));
            std::vector<std::string> param_kvs;
            StringUtils::split(param_str, param_kvs, ",");

            for(auto& param_kv_str: param_kvs) {
                if(param_kv_str.back() == ')') {
                    param_kv_str.pop_back();
                }

                std::vector<std::string> param_kv;
                StringUtils::split(param_kv_str, param_kv, ":");
                if(param_kv.size() != 2) {
                    return false;
                }

                if(param_kv[0] == "k") {
                    if(!StringUtils::is_uint32_t(param_kv[1])) {
                        return false;
                    }

                    vector_query.k = std::stoul(param_kv[1]);
                }
                
                if(param_kv[0] == "flat_search_cutoff") {
                    if(!StringUtils::is_uint32_t(param_kv[1])) {
                        return false;
                    }

                    vector_query.flat_search_cutoff = std::stoi(param_kv[1]);
                }
            }
            
            return true;
        }
    }

    return false;
}

Option<nlohmann::json> CollectionManager::search_multiple_collections(std::vector<std::map<std::string, std::string>>& req_params, std::vector<nlohmann::json>& embedded_params_vec) {
    auto begin = std::chrono::high_resolution_clock::now();
    nlohmann::json result;
    size_t total_doc_count = 0;
    std::vector<search_args*> search_args_vec;
    std::vector<CollectionKVGroup> collection_kvs_vec;
    std::vector<raw_search_args*> raw_search_args_vec;
    std::vector<collection_highlight_vars_t> collection_highlight_vars_vec(req_params.size());
    std::vector<std::pair<raw_search_args*, search_args*>> search_args_pair_vec(req_params.size());

    std::unordered_map<uint64_t, int> group_key_map;
    // To retrieve collections in get_result
    std::unordered_map<uint32_t, Collection*> collection_id_map;

    // handle which fields have to be highlighted
    std::vector<highlight_field_t> highlight_items;
    bool has_atleast_one_fully_highlighted_field = false;

    std::vector<std::string> highlight_field_names;
    std::vector<std::string> highlight_full_field_names;
    std::vector<std::string> include_fields_vec;
    std::vector<std::string> exclude_fields_vec;
    tsl::htrie_set<char> include_fields_full;
    tsl::htrie_set<char> exclude_fields_full;



    for(int i = 0;i < req_params.size();i++) {
        if(req_params[i].count("collection") == 0) {
            return Option<nlohmann::json>(400, "Missing collection name");
        }

        auto collection = get_collection(req_params[i]["collection"]);
        if(collection == nullptr) {
            return Option<nlohmann::json>(404, "One or more of collections not found.");
        }
        collection_id_map[collection->get_collection_id()] = collection.get();
        total_doc_count += collection->get_num_documents();

        auto args_op = get_raw_search_args(req_params[i], embedded_params_vec[i]);
        if(!args_op.ok()) {
            return Option<nlohmann::json>(args_op.code(), args_op.error());
        }

        auto args = args_op.get();
        raw_search_args_vec.push_back(args);

        auto search_args_op = collection->get_search_args(args->query, args->search_fields, args->simple_filter_query, args->facet_fields, args->sort_fields,
                                                args->num_typos, args->per_page, args->page, args->token_order, args->prefixes, args->drop_tokens_threshold,
                                                args->include_fields, args->exclude_fields, args->max_facet_values, args->simple_facet_query, args->snippet_threshold,
                                                args->highlight_affix_num_tokens, args->highlight_full_fields, args->typo_tokens_threshold, args->pinned_hits_str,
                                                args->hidden_hits, args->group_by_fields, args->group_limit, args->highlight_start_tag, args->highlight_end_tag,
                                                args->query_by_weights, args->limit_hits, args->prioritize_exact_match, args->pre_segmented_query, args->enable_overrides,
                                                args->highlight_fields, args->exhaustive_search, args->search_stop_millis, args->min_len_1typo, args->min_len_2typo,
                                                args->split_join_tokens, args->max_candidates, args->infixes, args->max_extra_prefix, args->max_extra_suffix,
                                                args->facet_query_num_typos, args->filter_curated_hits_option, args->prioritize_token_position, args->vector_query_str);
        if(!search_args_op.ok()) {
            return Option<nlohmann::json>(search_args_op.code(), search_args_op.error());
        }

        auto search_params = search_args_op.get();

        if(i == 0) {
            auto sort_fields = search_params->sort_fields_std;
            auto fields = collection->get_fields();
            for(auto& sort_field: sort_fields) {
                auto field_it = std::find_if(fields.begin(), fields.end(), [&sort_field](const auto& field) {
                    return field.name == sort_field.name;
                });
                if(field_it != fields.end() && ((*field_it).is_string_star() || (*field_it).is_string())) {
                    return Option<nlohmann::json>(400, "Sorting on string fields is not supported while merging multi search results.");
                }
            }
        }

        std::vector<CollectionKVGroup> search_results;

        auto search_results_op = collection->run_search(search_params, search_results);

        if(!search_results_op.ok()) {
            return Option<nlohmann::json>(search_results_op.code(), search_results_op.error());
        }

        for(int j = 0;j < search_results.size();j++) {
            if(args->group_by_fields.empty()) {
                collection_kvs_vec.push_back(search_results[j]);
                for(auto& collection_kv : collection_kvs_vec.back().collection_kvs) {
                    collection_kv.query_id = i;
                    search_args_pair_vec[i] = std::make_pair(args, search_params);
                }
            }
            else {
                if(group_key_map.find(search_results[j].group_key) == group_key_map.end()) {
                    collection_kvs_vec.emplace_back(search_results[j]);
                    group_key_map[search_results[j].group_key] = collection_kvs_vec.size() - 1;
                    for(auto& collection_kv : collection_kvs_vec.back().collection_kvs) {
                        collection_kv.query_id = i;
                        search_args_pair_vec[i] = std::make_pair(args, search_params);
                    }
                }
                else {
                    auto& group = collection_kvs_vec[group_key_map[search_results[j].group_key]];
                    for(auto& collection_kv : search_results[j].collection_kvs) {
                        collection_kv.query_id = i;
                        group.collection_kvs.emplace_back(collection_kv);
                        search_args_pair_vec[i] = std::make_pair(args, search_params);
                    }
                }
            }
        }
        search_args_vec.push_back(search_params);

        highlight_field_names.clear();
        highlight_full_field_names.clear();
        include_fields_vec.clear();
        exclude_fields_vec.clear();
        highlight_items.clear();

        const auto& search_schema = collection->_get_schema();

        StringUtils::split(args->highlight_fields, highlight_field_names, ",");
        StringUtils::split(args->highlight_full_fields, highlight_full_field_names, ",");

        for(auto& f_name: args->include_fields) {
            auto field_op = collection->extract_field_name(f_name, search_schema, include_fields_vec, false, collection->get_enable_nested_fields());
            if(!field_op.ok()) {
                if(field_op.code() == 404) {
                    // field need not be part of schema to be included (could be a stored value in the doc)
                    include_fields_vec.push_back(f_name);
                    continue;
                }
                return Option<nlohmann::json>(field_op.code(), field_op.error());
            }
        }
        for(auto& f_name: args->exclude_fields) {
            auto field_op = collection->extract_field_name(f_name, search_schema, exclude_fields_vec, false, collection->get_enable_nested_fields());
            if(!field_op.ok()) {
                if(field_op.code() == 404) {
                    // field need not be part of schema to be excluded (could be a stored value in the doc)
                    exclude_fields_vec.push_back(f_name);
                    continue;
                }
                return Option<nlohmann::json>(field_op.code(), field_op.error());
            }
        }
        for(auto& f_name: include_fields_vec) {
            include_fields_full.insert(f_name);
        }
        for(auto& f_name: exclude_fields_vec) {
                if(f_name == "out_of") {
                // `out_of` is strictly a meta-field, but we handle it since it's useful
                continue;
            }
            exclude_fields_full.insert(f_name);
        }
        highlight_items.clear();
        if(args->query != "*") {
            auto index = collection->_get_index();
            collection->process_highlight_fields(search_params->search_fields, include_fields_full, exclude_fields_full,
                                    highlight_field_names, highlight_full_field_names, args->infixes, search_params->q_tokens,
                                    search_params->qtoken_set, highlight_items, index, collection->_get_schema(), collection->get_enable_nested_fields());

            for(auto& highlight_item: highlight_items) {
                if(highlight_item.fully_highlighted) {
                    has_atleast_one_fully_highlighted_field = true;
                }
            }
            collection_highlight_vars_vec[i] = {highlight_items, has_atleast_one_fully_highlighted_field, true, highlight_field_names, highlight_full_field_names, include_fields_full, exclude_fields_full};
        }
        else {
            collection_highlight_vars_vec[i] = {highlight_items, has_atleast_one_fully_highlighted_field, false, highlight_field_names, highlight_full_field_names, include_fields_full, exclude_fields_full};
        }

        auto index_symbols = collection_highlight_vars_vec[i].index_symbols;
        for(char c: collection->get_symbols_to_index()) {
            index_symbols[uint8_t(c)] = 1;
        }
    }
    size_t topster_size = 0;
    size_t curated_topster_size = 0;
    size_t total_found = 0;

    for(auto search_args: search_args_vec) {
        search_args->topster->sort();
        search_args->curated_topster->sort();
        for(auto i = 0; i< search_args->topster->size; i++) {
            search_args->topster->getKV(i)->query_index += topster_size;
        }
        for(auto i = 0; i< search_args->curated_topster->size; i++) {
            search_args->curated_topster->getKV(i)->query_index += curated_topster_size;
        }
        topster_size += search_args->topster->size;
        curated_topster_size += search_args->curated_topster->size;
        total_found += search_args->all_result_ids_len;
    }
    if(search_args_vec.size() > 0) {
        search_args_vec[0]->all_result_ids_len = total_found;
        search_args_vec[0]->out_of = total_doc_count;
        auto& acc_facets = search_args_vec[0]->facets;
        for(auto i = 1;i < search_args_vec.size(); i++) {
            for(auto it = search_args_vec[i]->qtoken_set.begin(); it != search_args_vec[i]->qtoken_set.end(); it++) {
                search_args_vec[0]->qtoken_set.insert(it.key(), it.value());
            }

            //search_args_vec[0]->search_fields.insert(search_args_vec[0]->search_fields.end(), search_args_vec[i]->search_fields.begin(), search_args_vec[i]->search_fields.end());
            auto& facets = search_args_vec[i]->facets;
            for(auto fi = 0; fi < facets.size(); fi++) {
                auto& the_facet = facets[fi];
                // find facet with same field name in acc_facets
                auto acc_facet_it = std::find_if(acc_facets.begin(), acc_facets.end(), [&the_facet](const facet& f) {
                    return f.field_name == the_facet.field_name;
                });

                if(acc_facet_it != acc_facets.end()) {
                    for(auto& facet_kv : the_facet.result_map) {
                        if(acc_facet_it->result_map.find(facet_kv.first) != acc_facet_it->result_map.end()) {
                            acc_facet_it->result_map[facet_kv.first].count += facet_kv.second.count;
                        }
                        else {
                            acc_facet_it->result_map[facet_kv.first] = facet_kv.second;
                        }
                    }

                    for(auto& facet_kv : the_facet.hash_tokens) {
                        if(acc_facet_it->hash_tokens.find(facet_kv.first) != acc_facet_it->hash_tokens.end()) {
                            acc_facet_it->hash_tokens[facet_kv.first].insert(acc_facet_it->hash_tokens[facet_kv.first].end(), facet_kv.second.begin(), facet_kv.second.end());
                        }
                        else {
                            acc_facet_it->hash_tokens[facet_kv.first] = facet_kv.second;
                        }
                    }

                    for(auto& facet_kv : the_facet.hash_groups) {
                        if(acc_facet_it->hash_groups.find(facet_kv.first) != acc_facet_it->hash_groups.end()) {
                            acc_facet_it->hash_groups[facet_kv.first].insert(facet_kv.second.begin(), facet_kv.second.end());
                        }
                        else {
                            acc_facet_it->hash_groups[facet_kv.first] = facet_kv.second;
                        }
                    }

                    acc_facet_it->stats.fvsum += the_facet.stats.fvsum;
                    acc_facet_it->stats.fvmin = std::min(acc_facet_it->stats.fvmin, the_facet.stats.fvmin);
                    acc_facet_it->stats.fvmax = std::max(acc_facet_it->stats.fvmax, the_facet.stats.fvmax);
                    acc_facet_it->stats.fvcount += the_facet.stats.fvcount;
                }
                else {
                    acc_facets.push_back(the_facet);
                }
            }
        }
    }

    std::stable_sort(collection_kvs_vec.begin(), collection_kvs_vec.end(), [&collection_id_map](const CollectionKVGroup& a, const CollectionKVGroup& b) {
        auto score_a = std::tie(a.collection_kvs[0].kv->scores[0], a.collection_kvs[0].kv->scores[1], a.collection_kvs[0].kv->scores[2]);
        auto score_b = std::tie(b.collection_kvs[0].kv->scores[0], b.collection_kvs[0].kv->scores[1], b.collection_kvs[0].kv->scores[2]);

        return score_a > score_b;
    });

    for(auto& search_result: collection_kvs_vec) {
        std::stable_sort(search_result.collection_kvs.begin(), search_result.collection_kvs.end(), [](const CollectionKV& a, const CollectionKV& b) {
            auto score_a = std::tie(a.kv->scores[0], a.kv->scores[1], a.kv->scores[2]);
            auto score_b = std::tie(b.kv->scores[0], b.kv->scores[1], b.kv->scores[2]);

            return score_a > score_b;
        });
    }

    auto result_op = get_collection(req_params[0]["collection"])->get_result(*raw_search_args_vec[0], search_args_vec[0], collection_kvs_vec,collection_id_map, collection_highlight_vars_vec, search_args_pair_vec);
    result = result_op.get();
    result["out_of"] = total_doc_count;
    uint64_t timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - begin).count();

    AppMetrics::get_instance().increment_count(AppMetrics::SEARCH_LABEL, 1);
    AppMetrics::get_instance().increment_duration(AppMetrics::SEARCH_LABEL, timeMillis);

    if(raw_search_args_vec[0]->exclude_fields.count("search_time_ms") == 0) {
        result["search_time_ms"] = timeMillis;
    }

    result["page"] = raw_search_args_vec[0]->page;

    for(auto& req_param : req_params) {
        result["request_params"]["collections"].push_back(req_param["collection"]);
    }
    result["request_params"]["per_page"] = raw_search_args_vec[0]->per_page;

    //LOG(INFO) << "Time taken: " << timeMillis << "ms";
    for(auto& search_args: search_args_vec) {
        delete search_args;
    }

    for(auto& raw_search_args: raw_search_args_vec) {
        delete raw_search_args;
    }

    return Option<nlohmann::json>(result);
}

Option<Collection*> CollectionManager::get_collection_with_id_unsafe(const uint32_t& collection_id) {
    if(collection_id_names.count(collection_id) != 0) {
        auto coll_ptr = get_collection_unsafe(collection_id_names[collection_id]);
        if(!coll_ptr) {
            return Option<Collection*>(404, "Collection not found");
        } else {
            return Option<Collection*>(coll_ptr);
        }
    }
    return Option<Collection*>(404, "Collection not found");
}

Option<raw_search_args*> CollectionManager::get_raw_search_args(std::map<std::string, std::string>& req_params, nlohmann::json& embedded_params) {
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

    const char *VECTOR_QUERY = "vector_query";

    const char *GROUP_BY = "group_by";
    const char *GROUP_LIMIT = "group_limit";

    const char *LIMIT_HITS = "limit_hits";
    const char *PER_PAGE = "per_page";
    const char *PAGE = "page";
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

    // enrich params with values from embedded params
    for(auto& item: embedded_params.items()) {
        if(item.key() == "expires_at") {
            continue;
        }

        // overwrite = true as embedded params have higher priority
        AuthManager::add_item_to_params(req_params, item, true);
    }

    CollectionManager & collectionManager = CollectionManager::get_instance();


    // check presence of mandatory params here

    if(req_params.count(QUERY) == 0) {
        return Option<raw_search_args*>(400, std::string("Parameter `") + QUERY + "` is required.");
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
    std::string simple_filter_query;
    std::vector<std::string> facet_fields;
    std::vector<sort_by> sort_fields;
    size_t per_page = 10;
    size_t page = 1;
    token_ordering token_order = NOT_SET;

    std::string vector_query;

    std::vector<std::string> include_fields_vec;
    std::vector<std::string> exclude_fields_vec;
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
    size_t search_cutoff_ms = 3600000;
    enable_t split_join_tokens = fallback;
    size_t max_candidates = 0;
    std::vector<enable_t> infixes;
    size_t max_extra_prefix = INT16_MAX;
    size_t max_extra_suffix = INT16_MAX;

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
        {PER_PAGE, &per_page},
        {GROUP_LIMIT, &group_limit},
        {SEARCH_CUTOFF_MS, &search_cutoff_ms},
        {MAX_EXTRA_PREFIX, &max_extra_prefix},
        {MAX_EXTRA_SUFFIX, &max_extra_suffix},
        {MAX_CANDIDATES, &max_candidates},
        {FACET_QUERY_NUM_TYPOS, &facet_query_num_typos},
        {FILTER_CURATED_HITS, &filter_curated_hits_option},
    };

    std::unordered_map<std::string, std::string*> str_values = {
        {FILTER, &simple_filter_query},
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
    };

    std::unordered_map<std::string, std::vector<std::string>*> str_list_values = {
        {QUERY_BY, &search_fields},
        {FACET_BY, &facet_fields},
        {GROUP_BY, &group_by_fields},
        {INCLUDE_FIELDS, &include_fields_vec},
        {EXCLUDE_FIELDS, &exclude_fields_vec},
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

        else {
            auto find_int_it = unsigned_int_values.find(key);
            if(find_int_it != unsigned_int_values.end()) {
                const auto& op = add_unsigned_int_param(key, val, find_int_it->second);
                if(!op.ok()) {
                    return Option<raw_search_args*>(op.code(), op.error());
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
                StringUtils::split(val, *find_str_list_it->second, ",");
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

    include_fields.insert(include_fields_vec.begin(), include_fields_vec.end());
    exclude_fields.insert(exclude_fields_vec.begin(), exclude_fields_vec.end());

    bool parsed_sort_by = parse_sort_by_str(req_params[SORT_BY], sort_fields);

    if(!parsed_sort_by) {
        return Option<raw_search_args*>(400,std::string("Parameter `") + SORT_BY + "` is malformed.");
    }

    if(sort_fields.size() > 3) {
        return Option<raw_search_args*>(400, "Only upto 3 sort fields are allowed.");
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
        max_candidates = exhaustive_search ? Index::COMBINATION_MAX_LIMIT : Index::MAX_CANDIDATES_DEFAULT;
    }

    auto args = new raw_search_args{ 
                        raw_query, search_fields, simple_filter_query, facet_fields,
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
                        vector_query};
    return Option<raw_search_args*>(args);
}