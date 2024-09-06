#pragma once

#include <iostream>
#include <string>
#include <sparsepp.h>
#include "store.h"
#include "field.h"
#include "collection.h"
#include "auth_manager.h"
#include "threadpool.h"
#include "batched_indexer.h"

template<typename ResourceType>
struct locked_resource_view_t {
    locked_resource_view_t(std::shared_mutex &mutex, ResourceType &resource) : _lock(mutex), _resource(&resource) {}

    locked_resource_view_t(std::shared_mutex &mutex, ResourceType *resource) : _lock(mutex), _resource(resource) {}

    locked_resource_view_t(ResourceType &&) = delete;

    locked_resource_view_t(const ResourceType &) = delete;

    locked_resource_view_t &operator=(const ResourceType &) = delete;

    ResourceType* operator->() {
        return _resource;
    }

    bool operator==(const ResourceType* other) {
        return other == _resource;
    }

    bool operator!=(const ResourceType* other) {
        return other != _resource;
    }

    void unlock() {
        _lock.unlock();
    }

    ResourceType* get() {
        return _resource;
    }

private:
    std::shared_lock<std::shared_mutex> _lock;
    ResourceType* _resource;
};


// Singleton, for managing meta information of all collections and house keeping
class CollectionManager {
private:
    mutable std::shared_mutex mutex;

    mutable std::shared_mutex noop_coll_mutex;

    Store *store;
    ThreadPool* thread_pool;

    AuthManager auth_manager;

    spp::sparse_hash_map<std::string, Collection*> collections;

    spp::sparse_hash_map<uint32_t, std::string> collection_id_names;

    spp::sparse_hash_map<std::string, std::string> collection_symlinks;

    spp::sparse_hash_map<std::string, nlohmann::json> preset_configs;

    // Auto incrementing ID assigned to each collection
    // Using a ID instead of a collection's name makes renaming possible
    std::atomic<uint32_t> next_collection_id;

    std::string bootstrap_auth_key;

    std::atomic<float> max_memory_ratio;

    std::atomic<bool>* quit;

    // All the references to a particular collection are stored until it is created.
    std::map<std::string, std::set<reference_info_t>> referenced_in_backlog;

    CollectionManager();

    ~CollectionManager() = default;

    static std::string get_first_index_error(const std::vector<index_record>& index_records) {
        for(const auto & index_record: index_records) {
            if(!index_record.indexed.ok()) {
                return index_record.indexed.error();
            }
        }

        return "";
    }

public:
    static constexpr const size_t DEFAULT_NUM_MEMORY_SHARDS = 4;

    static constexpr const char* NEXT_COLLECTION_ID_KEY = "$CI";
    static constexpr const char* SYMLINK_PREFIX = "$SL";
    static constexpr const char* PRESET_PREFIX = "$PS";

    uint16_t filter_by_max_ops;

    static CollectionManager & get_instance() {
        static CollectionManager instance;
        return instance;
    }

    CollectionManager(CollectionManager const&) = delete;
    void operator=(CollectionManager const&) = delete;

    static Collection* init_collection(const nlohmann::json & collection_meta,
                                       const uint32_t collection_next_seq_id,
                                       Store* store,
                                       float max_memory_ratio,
                                       spp::sparse_hash_map<std::string, std::string>& referenced_in,
                                       spp::sparse_hash_map<std::string, std::vector<reference_pair_t>>& async_referenced_ins);

    static Option<bool> load_collection(const nlohmann::json& collection_meta,
                                        const size_t batch_size,
                                        const StoreStatus& next_coll_id_status,
                                        const std::atomic<bool>& quit,
                                        spp::sparse_hash_map<std::string, std::string>& referenced_in,
                                        spp::sparse_hash_map<std::string, std::vector<reference_pair_t>>& async_referenced_ins);

    Option<Collection*> clone_collection(const std::string& existing_name, const nlohmann::json& req_json);

    void add_to_collections(Collection* collection);

    Option<std::vector<Collection*>> get_collections(uint32_t limit = 0, uint32_t offset = 0,
                                                     const std::vector<std::string>& api_key_collections = {}) const;

    std::vector<std::string> get_collection_names() const;

    Collection* get_collection_unsafe(const std::string & collection_name) const;

    // PUBLICLY EXPOSED API

    void init(Store *store, ThreadPool* thread_pool, const float max_memory_ratio,
              const std::string & auth_key, std::atomic<bool>& quit,
              const uint16_t& filter_by_max_operations = Config::FILTER_BY_DEFAULT_OPERATIONS);

    // only for tests!
    void init(Store *store, const float max_memory_ratio, const std::string & auth_key, std::atomic<bool>& exit,
              const uint16_t& filter_by_max_operations = Config::FILTER_BY_DEFAULT_OPERATIONS);

    Option<bool> load(const size_t collection_batch_size, const size_t document_batch_size);

    // frees in-memory data structures when server is shutdown - helps us run a memory leak detector properly
    void dispose();

    bool auth_key_matches(const string& req_auth_key, const string& action,
                          const std::vector<collection_key_t>& collection_keys,
                          std::map<std::string, std::string>& params,
                          std::vector<nlohmann::json>& embedded_params_vec) const;

    static Option<Collection*> create_collection(nlohmann::json& req_json);

    Option<Collection*> create_collection(const std::string& name, const size_t num_memory_shards,
                                          const std::vector<field> & fields,
                                          const std::string & default_sorting_field="",
                                          const uint64_t created_at = static_cast<uint64_t>(std::time(nullptr)),
                                          const std::string& fallback_field_type = "",
                                          const std::vector<std::string>& symbols_to_index = {},
                                          const std::vector<std::string>& token_separators = {},
                                          const bool enable_nested_fields = false, std::shared_ptr<VQModel> model = nullptr,
                                          const nlohmann::json& metadata = {});

    locked_resource_view_t<Collection> get_collection(const std::string & collection_name) const;

    locked_resource_view_t<Collection> get_collection_with_id(uint32_t collection_id) const;

    Option<nlohmann::json> get_collection_summaries(uint32_t limit = 0 , uint32_t offset = 0,
                                                    const std::vector<std::string>& exclude_fields = {},
                                                    const std::vector<std::string>& api_key_collections = {}) const;

    Option<nlohmann::json> drop_collection(const std::string& collection_name,
                                           const bool remove_from_store = true,
                                           const bool compact_store = true);

    uint32_t get_next_collection_id() const;

    static std::string get_symlink_key(const std::string & symlink_name);

    static std::string get_preset_key(const std::string & preset_name);

    Store* get_store();

    ThreadPool* get_thread_pool() const;

    AuthManager& getAuthManager();

    static Option<bool> do_search(std::map<std::string, std::string>& req_params,
                                  nlohmann::json& embedded_params,
                                  std::string& results_json_str,
                                  uint64_t start_ts);

    static bool parse_sort_by_str(std::string sort_by_str, std::vector<sort_by>& sort_fields);

    // symlinks
    Option<std::string> resolve_symlink(const std::string & symlink_name) const;

    spp::sparse_hash_map<std::string, std::string> get_symlinks() const;

    Option<bool> upsert_symlink(const std::string & symlink_name, const std::string & collection_name);

    Option<bool> delete_symlink(const std::string & symlink_name);

    // presets
    spp::sparse_hash_map<std::string, nlohmann::json> get_presets() const;

    Option<bool> get_preset(const std::string & preset_name, nlohmann::json& preset) const;

    Option<bool> upsert_preset(const std::string & preset_name, const nlohmann::json& preset_config);

    Option<bool> delete_preset(const std::string & preset_name);

    void add_referenced_in_backlog(const std::string& collection_name, reference_info_t&& ref_info);

    std::map<std::string, std::set<reference_info_t>> _get_referenced_in_backlog() const;

    void process_embedding_field_delete(const std::string& model_name);

    static void _populate_referenced_ins(const std::string& collection_meta_json,
                                         std::map<std::string, spp::sparse_hash_map<std::string, std::string>>& referenced_ins,
                                         std::map<std::string, spp::sparse_hash_map<std::string, std::vector<reference_pair_t>>>& async_referenced_ins);

    std::unordered_set<std::string> get_collection_references(const std::string& coll_name);

    bool is_valid_api_key_collection(const std::vector<std::string>& api_key_collections, Collection* coll) const;

    bool update_collection_metadata(const std::string& collection, const nlohmann::json& metadata);
};
