#pragma once

#include <cstdint>
#include <vector>
#include "posting_list.h"
#include "threadpool.h"

#define IS_COMPACT_POSTING(x) (((uintptr_t)(x) & 1))
#define SET_COMPACT_POSTING(x) ((void*)((uintptr_t)(x) | 1))
#define RAW_POSTING_PTR(x) ((void*)((uintptr_t)(x) & ~1))
#define COMPACT_POSTING_PTR(x) ((compact_posting_list_t*)((uintptr_t)(x) & ~1))

struct compact_posting_list_t {
    // structured to get 4 byte alignment for `id_offsets`
    uint8_t length = 0;
    uint8_t ids_length = 0;
    uint16_t capacity = 0;

    // format: num_offsets, offset1,..,offsetn, id1 | num_offsets, offset1,..,offsetn, id2
    uint32_t id_offsets[];

    static compact_posting_list_t* create(uint32_t num_ids, const uint32_t* ids, const uint32_t* offset_index,
                                          uint32_t num_offsets, const uint32_t* offsets);

    [[nodiscard]] posting_list_t* to_full_posting_list() const;

    bool contains(uint32_t id);

    int64_t upsert(uint32_t id, const std::vector<uint32_t>& offsets);
    int64_t upsert(uint32_t id, const uint32_t* offsets, uint32_t num_offsets);

    void erase(uint32_t id);

    uint32_t first_id();
    uint32_t last_id();

    [[nodiscard]] uint32_t num_ids() const;

    bool contains_atleast_one(const uint32_t* target_ids, size_t target_ids_size);
};

class posting_t {
public:
    static constexpr size_t COMPACT_LIST_THRESHOLD_LENGTH = 64;
    static constexpr size_t MAX_BLOCK_ELEMENTS = 256;

    struct block_intersector_t {
        std::vector<posting_list_t*> plists;
        std::vector<posting_list_t*> expanded_plists;
        result_iter_state_t& iter_state;

        block_intersector_t(const std::vector<void*>& raw_posting_lists,
                            result_iter_state_t& iter_state):
                            iter_state(iter_state) {

            to_expanded_plists(raw_posting_lists, plists, expanded_plists);

            if(plists.size() > 1) {
                std::sort(this->plists.begin(), this->plists.end(), [](posting_list_t* a, posting_list_t* b) {
                    return a->num_blocks() < b->num_blocks();
                });
            }
        }

        ~block_intersector_t() {
            for(auto expanded_plist: expanded_plists) {
                delete expanded_plist;
            }
        }

        template<class T>
        bool intersect(T func);
    };

    static void to_expanded_plists(const std::vector<void*>& raw_posting_lists, std::vector<posting_list_t*>& plists,
                                   std::vector<posting_list_t*>& expanded_plists);

    static void upsert(void*& obj, uint32_t id, const std::vector<uint32_t>& offsets);

    static void erase(void*& obj, uint32_t id);

    static void destroy_list(void*& obj);

    static uint32_t num_ids(const void* obj);

    static uint32_t first_id(const void* obj);

    static bool contains(const void* obj, uint32_t id);

    static bool contains_atleast_one(const void* obj, const uint32_t* target_ids, size_t target_ids_size);

    static void merge(const std::vector<void*>& posting_lists, std::vector<uint32_t>& result_ids);

    static void intersect(const std::vector<void*>& posting_lists, std::vector<uint32_t>& result_ids);

    static void get_array_token_positions(
        uint32_t id,
        const std::vector<void*>& posting_lists,
        std::map<size_t, std::vector<token_positions_t>>& array_token_positions
    );

    static void get_exact_matches(const std::vector<void*>& raw_posting_lists, bool field_is_array,
                                  const uint32_t* ids, uint32_t num_ids,
                                  uint32_t*& exact_ids, size_t& num_exact_ids);

    static void get_phrase_matches(const std::vector<void*>& raw_posting_lists, bool field_is_array,
                                   const uint32_t* ids, uint32_t num_ids,
                                   uint32_t*& phrase_ids, size_t& num_phrase_ids);

    static void get_matching_array_indices(const std::vector<void*>& raw_posting_lists,
                                           uint32_t id, std::vector<size_t>& indices);
};

template<class T>
bool posting_t::block_intersector_t::intersect(T func) {
    if(plists.empty()) {
        return true;
    }

    std::vector<posting_list_t::iterator_t> its;
    its.reserve(plists.size());

    for(const auto& posting_list: plists) {
        its.push_back(posting_list->new_iterator());
    }

    posting_list_t::block_intersect<T>(its, iter_state, func);
    return true;
}
