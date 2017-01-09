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

    const std::string NEXT_COLLECTION_ID_KEY = "$CI";
    const std::string COLLECTION_NAME_PREFIX = "$CN";
    const std::string COLLECTION_NEXT_SEQ_PREFIX = "$CS";

    const std::string COLLECTION_NAME_KEY = "name";
    const std::string COLLECTION_ID_KEY = "id";
    const std::string COLLECTION_SEARCH_FIELDS_KEY = "search_fields";
    const std::string COLLECTION_RANK_FIELDS_KEY = "rank_fields";

    CollectionManager();

    std::string get_collection_name_key(std::string collection_name);
    std::string get_collection_next_seq_id_key(std::string collection_name);

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
};