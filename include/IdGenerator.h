#pragma once

#include <stdint.h>

/*
 *  Generates IDs for new records in sequential fashion.
 *  Uses RocksDB for persistence. (FIXME)
 */
class IdGenerator {
private:
    int32_t id = 0;
public:
    int32_t Next() {
        return ++id;
    }
};