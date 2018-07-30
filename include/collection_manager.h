#pragma once

#include <iostream>
#include <string>
#include <sparsepp.h>
#include "store.h"
#include "field.h"
#include "collection.h"

// Singleton, for managing meta information of all collections and house keeping
class CollectionManager {
private:
    Store *store;

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

    std::string auth_key;
    std::string search_only_auth_key;

    CollectionManager();

    ~CollectionManager() = default;

public:
    static CollectionManager & get_instance() {
        static CollectionManager instance;
        return instance;
    }

    CollectionManager(CollectionManager const&) = delete;
    void operator=(CollectionManager const&) = delete;

    Option<bool> init(Store *store, const std::string & auth_key, const std::string & search_only_auth_key);

    // frees in-memory data structures when server is shutdown - helps us run a memory leak detecter properly
    void dispose();

    Collection* init_collection(const nlohmann::json & collection_meta, const uint32_t collection_next_seq_id);

    void add_to_collections(Collection* collection);

    bool auth_key_matches(std::string auth_key_sent);

    bool search_only_auth_key_matches(std::string auth_key_sent);

    Option<Collection*> create_collection(const std::string name, const std::vector<field> & fields,
                                          const std::string & default_sorting_field);

    Collection* get_collection(const std::string & collection_name);

    Collection* get_collection_with_id(uint32_t collection_id);

    std::vector<Collection*> get_collections();

    Option<bool> drop_collection(std::string collection_name, const bool remove_from_store = true);

    uint32_t get_next_collection_id();

    static std::string get_symlink_key(const std::string & symlink_name);

    void set_next_collection_id(uint32_t next_id);

    Store* get_store();

    // symlinks
    Option<std::string> resolve_symlink(const std::string & symlink_name);

    spp::sparse_hash_map<std::string, std::string> get_symlinks();

    bool upsert_symlink(const std::string & symlink_name, const std::string & collection_name);

    bool delete_symlink(const std::string & symlink_name);

    static constexpr const char* NEXT_COLLECTION_ID_KEY = "$CI";
    static constexpr const char* SYMLINK_PREFIX = "$SL";
};