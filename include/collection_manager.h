#pragma once

#include <iostream>
#include <string>
#include <sparsepp.h>
#include "store.h"
#include "field.h"
#include "collection.h"
#include "auth_manager.h"

// Singleton, for managing meta information of all collections and house keeping
class CollectionManager {
private:
    Store *store;
    AuthManager auth_manager;

    spp::sparse_hash_map<std::string, Collection*> collections;

    spp::sparse_hash_map<uint32_t, std::string> collection_id_names;

    spp::sparse_hash_map<std::string, std::string> collection_symlinks;

    // Auto incrementing ID assigned to each collection
    // Using a ID instead of a collection's name makes renaming possible
    uint32_t next_collection_id;

    static constexpr const char* COLLECTION_NAME_KEY = "name";
    static constexpr const char* COLLECTION_ID_KEY = "id";
    static constexpr const char* COLLECTION_SEARCH_FIELDS_KEY = "fields";
    static constexpr const char* COLLECTION_DEFAULT_SORTING_FIELD_KEY = "default_sorting_field";
    static constexpr const char* COLLECTION_CREATED = "created_at";
    static constexpr const char* COLLECTION_NUM_INDICES = "num_indices";

    std::string bootstrap_auth_key;
    std::string bootstrap_search_only_auth_key;

    float max_memory_ratio;

    CollectionManager();

    ~CollectionManager() = default;

    Option<std::string> get_first_index_error(const std::vector<index_record>& index_records) {
        for(const auto & index_record: index_records) {
            if(!index_record.indexed.ok()) {
                return Option<std::string>(index_record.indexed.error());
            }
        }

        return Option<std::string>(404, "Not found");
    }

public:
    static CollectionManager & get_instance() {
        static CollectionManager instance;
        return instance;
    }

    CollectionManager(CollectionManager const&) = delete;
    void operator=(CollectionManager const&) = delete;

    void init(Store *store, const float max_memory_ratio, const std::string & auth_key);

    Option<bool> load(const size_t init_batch_size=1000);

    // frees in-memory data structures when server is shutdown - helps us run a memory leak detecter properly
    void dispose();

    Collection* init_collection(const nlohmann::json & collection_meta, const uint32_t collection_next_seq_id);

    void add_to_collections(Collection* collection);

    bool auth_key_matches(const std::string& auth_key_sent, const std::string& action,
                          const std::string& collection, std::map<std::string, std::string>& params);

    Option<Collection*> create_collection(const std::string name, const size_t num_indices,
                                          const std::vector<field> & fields,
                                          const std::string & default_sorting_field,
                                          const uint64_t created_at = static_cast<uint64_t>(std::time(nullptr)));

    Collection* get_collection(const std::string & collection_name);

    Collection* get_collection_with_id(uint32_t collection_id);

    std::vector<Collection*> get_collections();

    Option<bool> drop_collection(std::string collection_name, const bool remove_from_store = true);

    uint32_t get_next_collection_id();

    static std::string get_symlink_key(const std::string & symlink_name);

    void set_next_collection_id(uint32_t next_id);

    Store* get_store();

    AuthManager& getAuthManager();

    // symlinks
    Option<std::string> resolve_symlink(const std::string & symlink_name);

    spp::sparse_hash_map<std::string, std::string> & get_symlinks();

    Option<bool> upsert_symlink(const std::string & symlink_name, const std::string & collection_name);

    Option<bool> delete_symlink(const std::string & symlink_name);

    static const size_t DEFAULT_NUM_INDICES = 4;

    static constexpr const char* NEXT_COLLECTION_ID_KEY = "$CI";
    static constexpr const char* SYMLINK_PREFIX = "$SL";
};