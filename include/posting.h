#pragma once

#include <cstdint>
#include <vector>
#include "posting_list.h"

#define IS_COMPACT_POSTING(x) (((uintptr_t)x & 1))
#define SET_COMPACT_POSTING(x) ((void*)((uintptr_t)x | 1))
#define RAW_POSTING_PTR(x) ((void*)((uintptr_t)x & ~1))
#define COMPACT_POSTING_PTR(x) ((compact_posting_list_t*)((uintptr_t)x & ~1))

struct compact_posting_list_t {
    // use uint16_t to get 4 byte alignment for `id_offsets`
    uint16_t length = 0;
    uint16_t capacity = 0;

    // format: num_offsets, offset1,..,offsetn, id1 | num_offsets, offset1,..,offsetn, id2
    uint32_t id_offsets[];

    static compact_posting_list_t* create(uint32_t num_ids, uint32_t* ids, const uint32_t* offset_index,
                                          uint32_t num_offsets, uint32_t* offsets);

    posting_list_t* to_full_posting_list();

    int64_t upsert(uint32_t id, const std::vector<uint32_t>& offsets);
    int64_t upsert(uint32_t id, const uint32_t* offsets, uint32_t num_offsets);

    void erase(uint32_t id);

    uint32_t last_id();
};

class posting_t {
private:
    static constexpr size_t COMPACT_LIST_THRESHOLD_LENGTH = 64;

public:

    static void upsert(void*& obj, uint32_t id, const std::vector<uint32_t>& offsets);

    static void erase(void*& obj, uint32_t id);

};