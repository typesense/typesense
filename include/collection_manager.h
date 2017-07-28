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

    static constexpr const char* COLLECTION_NAME_KEY = "name";
    static constexpr const char* COLLECTION_ID_KEY = "id";
    static constexpr const char* COLLECTION_SEARCH_FIELDS_KEY = "search_fields";
    static constexpr const char* COLLECTION_FACET_FIELDS_KEY = "facet_fields";
    static constexpr const char* COLLECTION_SORT_FIELDS_KEY = "sort_fields";
    static constexpr const char* COLLECTION_TOKEN_ORDERING_FIELD_KEY = "token_ranking_field";

    std::string auth_key;

    CollectionManager();

    ~CollectionManager() = default;

public:
    static CollectionManager & get_instance() {
        static CollectionManager instance;
        return instance;
    }

    CollectionManager(CollectionManager const&) = delete;
    void operator=(CollectionManager const&) = delete;

    void init(Store *store, const std::string & auth_key);

    bool auth_key_matches(std::string auth_key_sent);

    Collection* create_collection(std::string name, const std::vector<field> & search_fields,
                                  const std::vector<field> & facet_fields,
                                  const std::vector<field> & sort_fields,
                                  const std::string & token_ranking_field = "");

    Collection* get_collection(const std::string & collection_name);

    std::vector<Collection*> get_collections();

    Option<bool> drop_collection(std::string collection_name);

    uint32_t get_next_collection_id();

    static constexpr const char* NEXT_COLLECTION_ID_KEY = "$CI";
};