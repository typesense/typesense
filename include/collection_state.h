#pragma once

#include <stdint.h>

/*
 *  Stores all information about a collection.
 *  Uses RocksDB for persistence.
 */
class CollectionState {

private:

    uint32_t id = 0;

public:

    uint32_t nextId() {
        return ++id;
    }
};