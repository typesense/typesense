#pragma once

#include <vector>
#include "store.h"
#include "collection.h"

class CollectionMetadata {
private:
    CollectionMetadata() {}

public:
    std::vector<std::string> collection_meta_jsons;

    CollectionMetadata(CollectionMetadata const&) = delete;
    void operator=(CollectionMetadata const&)  = delete;

    static CollectionMetadata& get_instance() {
        static CollectionMetadata instance;
        return instance;
    }

    void init(Store* store) {
        collection_meta_jsons.clear();
        store->scan_fill(std::string(Collection::COLLECTION_META_PREFIX) + "_",
                         std::string(Collection::COLLECTION_META_PREFIX) + "`",
                         collection_meta_jsons);
    }
};
