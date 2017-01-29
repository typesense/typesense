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

    // Auto incrementing ID assigned to each collection
    // Using a ID instead of a collection's name makes renaming possible
    uint32_t next_collection_id;

    static constexpr const char* COLLECTION_META_PREFIX = "$CM";

    static constexpr const char* COLLECTION_NAME_KEY = "name";
    static constexpr const char* COLLECTION_ID_KEY = "id";
    static constexpr const char* COLLECTION_SEARCH_FIELDS_KEY = "search_fields";
    static constexpr const char* COLLECTION_RANK_FIELDS_KEY = "rank_fields";

    CollectionManager();

    static std::string get_collection_meta_key(std::string collection_name);

public:
    static CollectionManager& get_instance() {
        static CollectionManager instance;
        return instance;
    }

    ~CollectionManager();

    CollectionManager(CollectionManager const&) = delete;
    void operator=(CollectionManager const&) = delete;

    void init(Store *store);

    Collection* create_collection(std::string name, const std::vector<field> & search_fields,
                           const std::vector<std::string> & rank_fields);

    Collection* get_collection(std::string collection_name);

    bool drop_collection(std::string collection_name);

    uint32_t get_next_collection_id();

    static constexpr const char* NEXT_COLLECTION_ID_KEY = "$CI";
};