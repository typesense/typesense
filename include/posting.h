#pragma once

#include <cstdint>
#include <vector>
#include "posting_list.h"

#define IS_COMPACT_POSTING(x) (((uintptr_t)x & 1))
#define SET_COMPACT_POSTING(x) ((void*)((uintptr_t)x | 1))
#define RAW_POSTING_PTR(x) ((void*)((uintptr_t)x & ~1))
#define COMPACT_POSTING_PTR(x) ((compact_posting_list_t*)((uintptr_t)x & ~1))

struct compact_posting_list_t {
    // structured to get 4 byte alignment for `id_offsets`
    uint8_t length = 0;
    uint8_t ids_length = 0;
    uint16_t capacity = 0;

    // format: num_offsets, offset1,..,offsetn, id1 | num_offsets, offset1,..,offsetn, id2
    uint32_t id_offsets[];

    static compact_posting_list_t* create(uint32_t num_ids, const uint32_t* ids, const uint32_t* offset_index,
                                          uint32_t num_offsets, uint32_t* offsets);

    posting_list_t* to_full_posting_list();

    bool contains(uint32_t id);

    int64_t upsert(uint32_t id, const std::vector<uint32_t>& offsets);
    int64_t upsert(uint32_t id, const uint32_t* offsets, uint32_t num_offsets);

    void erase(uint32_t id);

    uint32_t first_id();
    uint32_t last_id();

    uint32_t num_ids() const;

    bool contains_atleast_one(const uint32_t* target_ids, size_t target_ids_size);
};

class posting_t {
private:
    static constexpr size_t COMPACT_LIST_THRESHOLD_LENGTH = 64;

    static void to_expanded_plists(const std::vector<void*>& raw_posting_lists, std::vector<posting_list_t*>& plists,
                                   std::vector<uint32_t>& expanded_plist_indices);

public:

    struct block_intersector_t {
        size_t batch_size;
        std::vector<posting_list_t::iterator_t> its;
        std::vector<posting_list_t*> plists;
        std::vector<uint32_t> expanded_plist_indices;

        posting_list_t::result_iter_state_t& iter_state;

        block_intersector_t(const std::vector<void*>& raw_posting_lists,
                            size_t batch_size,
                            posting_list_t::result_iter_state_t& iter_state):
                            batch_size(batch_size), iter_state(iter_state) {
            to_expanded_plists(raw_posting_lists, plists, expanded_plist_indices);

            its.reserve(plists.size());
            for(const auto& posting_list: plists) {
                its.push_back(posting_list->new_iterator());
            }
        }

        ~block_intersector_t() {
            for(uint32_t expanded_plist_index: expanded_plist_indices) {
                delete plists[expanded_plist_index];
            }
        }

        bool intersect() {
            return posting_list_t::block_intersect(plists, batch_size, its, iter_state);;
        }
    };

    static void upsert(void*& obj, uint32_t id, const std::vector<uint32_t>& offsets);

    static void erase(void*& obj, uint32_t id);

    static void destroy_list(void*& obj);

    static uint32_t num_ids(const void* obj);

    static uint32_t first_id(const void* obj);

    static bool contains(const void* obj, uint32_t id);

    static bool contains_atleast_one(const void* obj, const uint32_t* target_ids, size_t target_ids_size);

    static void merge(const std::vector<void*>& posting_lists, std::vector<uint32_t>& result_ids);

    static void intersect(const std::vector<void*>& posting_lists, std::vector<uint32_t>& result_ids);
};