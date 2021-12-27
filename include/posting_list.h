#pragma once

#include <map>
#include <unordered_map>
#include "sorted_array.h"
#include "array.h"
#include "match_score.h"

typedef uint32_t last_id_t;

#define FOR_ELE_SIZE sizeof(uint32_t)
#define METADATA_OVERHEAD 5

/*
    Compressed chain of blocks that store the document IDs and offsets of a given token.
    Offsets of singular and multi-valued fields are encoded differently.
*/
class posting_list_t {
public:

    // A block stores a list of Document IDs, Token Offsets and a Mapping of ID => Offset indices efficiently
    // Layout of *data: [ids...mappings..offsets]
    // IDs and Mappings are sorted integers, while offsets are not sorted
    struct block_t {
        sorted_array ids;
        sorted_array offset_index;
        array offsets;

        // link to next block
        block_t* next = nullptr;

        bool contains(uint32_t id);

        void remove_and_shift_offset_index(const uint32_t* indices_sorted, uint32_t num_indices);

        uint32_t upsert(uint32_t id, const std::vector<uint32_t>& offsets);

        uint32_t erase(uint32_t id);

        uint32_t size() {
            return ids.getLength();
        }
    };

    class iterator_t {
    private:
        block_t* curr_block;
        uint32_t curr_index;

        block_t* end_block;

    public:
        // uncompressed data structures for performance
        uint32_t* ids = nullptr;
        uint32_t* offset_index = nullptr;
        uint32_t* offsets = nullptr;

        explicit iterator_t(block_t* start, block_t* end);
        iterator_t(iterator_t&& rhs) noexcept;
        ~iterator_t();
        [[nodiscard]] bool valid() const;
        void next();
        void skip_to(uint32_t id);
        [[nodiscard]] uint32_t id() const;
        [[nodiscard]] inline uint32_t index() const;
        [[nodiscard]] inline block_t* block() const;
    };

    struct result_iter_state_t {
        const uint32_t* excluded_result_ids = nullptr;
        const size_t excluded_result_ids_size = 0;

        const uint32_t* filter_ids = nullptr;
        const size_t filter_ids_length = 0;

        size_t excluded_result_ids_index = 0;
        size_t filter_ids_index = 0;
        size_t index = 0;

        result_iter_state_t() = default;

        result_iter_state_t(uint32_t* excluded_result_ids, size_t excluded_result_ids_size,
                            const uint32_t* filter_ids, const size_t filter_ids_length) : excluded_result_ids(excluded_result_ids),
                                                        excluded_result_ids_size(excluded_result_ids_size),
                                                        filter_ids(filter_ids), filter_ids_length(filter_ids_length) {}
    };

private:

    // maximum number of IDs (and associated offsets) to store in each block before another block is created
    const uint16_t BLOCK_MAX_ELEMENTS;
    uint32_t ids_length = 0;

    block_t root_block;

    // keeps track of the *last* ID in each block and is used for partial random access
    // e.g. 0..[9], 10..[19], 20..[29]
    // MUST be ordered
    std::map<last_id_t, block_t*> id_block_map;

    static bool at_end(const std::vector<posting_list_t::iterator_t>& its);
    static bool at_end2(const std::vector<posting_list_t::iterator_t>& its);

    static bool equals(std::vector<posting_list_t::iterator_t>& its);
    static bool equals2(std::vector<posting_list_t::iterator_t>& its);

    static void advance_all(std::vector<posting_list_t::iterator_t>& its);
    static void advance_all2(std::vector<posting_list_t::iterator_t>& its);

    static void advance_non_largest(std::vector<posting_list_t::iterator_t>& its);
    static void advance_non_largest2(std::vector<posting_list_t::iterator_t>& its);

    static uint32_t advance_smallest(std::vector<posting_list_t::iterator_t>& its);
    static uint32_t advance_smallest2(std::vector<posting_list_t::iterator_t>& its);

public:

    posting_list_t() = delete;

    explicit posting_list_t(uint16_t max_block_elements);

    ~posting_list_t();

    static void split_block(block_t* src_block, block_t* dst_block);

    static void merge_adjacent_blocks(block_t* block1, block_t* block2, size_t num_block2_ids_to_move);

    void upsert(uint32_t id, const std::vector<uint32_t>& offsets);

    void erase(uint32_t id);

    block_t* get_root();

    size_t num_blocks() const;

    size_t num_ids() const;

    uint32_t first_id();

    block_t* block_of(uint32_t id);

    bool contains(uint32_t id);

    bool contains_atleast_one(const uint32_t* target_ids, size_t target_ids_size);

    iterator_t new_iterator(block_t* start_block = nullptr, block_t* end_block = nullptr);

    static void merge(const std::vector<posting_list_t*>& posting_lists, std::vector<uint32_t>& result_ids);

    static void intersect(const std::vector<posting_list_t*>& posting_lists, std::vector<uint32_t>& result_ids);

    template<class T>
    static bool block_intersect(
        std::vector<posting_list_t::iterator_t>& its,
        result_iter_state_t& istate,
        T func
    );

    static bool take_id(result_iter_state_t& istate, uint32_t id);

    static bool get_offsets(
        const std::vector<iterator_t>& its,
        std::unordered_map<size_t, std::vector<token_positions_t>>& array_token_pos
    );

    static bool is_single_token_verbatim_match(const posting_list_t::iterator_t& it, bool field_is_array);

    static void get_exact_matches(std::vector<iterator_t>& its, bool field_is_array,
                                  const uint32_t* ids, const uint32_t num_ids,
                                  uint32_t*& exact_ids, size_t& num_exact_ids);

    static void get_phrase_matches(std::vector<iterator_t>& its, bool field_is_array,
                                   const uint32_t* ids, const uint32_t num_ids,
                                   uint32_t*& phrase_ids, size_t& num_phrase_ids);

    static void get_matching_array_indices(uint32_t id, std::vector<iterator_t>& its,
                                           std::vector<size_t>& indices);
};

template<class T>
bool posting_list_t::block_intersect(std::vector<posting_list_t::iterator_t>& its, result_iter_state_t& istate,
                                     T func) {

    switch (its.size()) {
        case 0:
            break;
        case 1:
            while(its[0].valid()) {
                if(posting_list_t::take_id(istate, its[0].id())) {
                    func(its[0].id(), its, istate.index);
                }

                its[0].next();
            }
            break;
        case 2:
            while(!at_end2(its)) {
                if(equals2(its)) {
                    if(posting_list_t::take_id(istate, its[0].id())) {
                        func(its[0].id(), its, istate.index);
                    }

                    advance_all2(its);
                } else {
                    advance_non_largest2(its);
                }
            }
            break;
        default:
            while(!at_end(its)) {
                if(equals(its)) {
                    //LOG(INFO) << its[0].id();
                    if(posting_list_t::take_id(istate, its[0].id())) {
                        func(its[0].id(), its, istate.index);
                    }

                    advance_all(its);
                } else {
                    advance_non_largest(its);
                }
            }
    }

    return false;
}
