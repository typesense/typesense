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

        // uncompressed data structures for performance
        block_t* uncompressed_block;
        uint32_t* ids;

    public:
        explicit iterator_t(block_t* root);
        iterator_t(iterator_t&& rhs) noexcept;
        ~iterator_t();
        [[nodiscard]] bool valid() const;
        void next();
        void skip_to(uint32_t id);
        [[nodiscard]] inline uint32_t id();
        [[nodiscard]] inline uint32_t index() const;
        [[nodiscard]] inline block_t* block() const;
    };

    struct result_iter_state_t {
        std::vector<std::vector<block_t*>> blocks;
        std::vector<std::vector<uint32_t>> indices;
        std::vector<uint32_t> ids;
    };

private:

    // maximum number of IDs (and associated offsets) to store in each block before another block is created
    const uint16_t BLOCK_MAX_ELEMENTS;
    uint16_t ids_length = 0;

    block_t root_block;

    // keeps track of the *last* ID in each block and is used for partial random access
    // e.g. 0..[9], 10..[19], 20..[29]
    // MUST be ordered
    std::map<last_id_t, block_t*> id_block_map;

    static void split_block(block_t* src_block, block_t* dst_block);

    static void merge_adjacent_blocks(block_t* block1, block_t* block2, size_t num_block2_ids);

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

    void upsert(uint32_t id, const std::vector<uint32_t>& offsets);

    void erase(uint32_t id);

    block_t* get_root();

    size_t num_blocks();

    size_t num_ids();

    uint32_t first_id();

    block_t* block_of(uint32_t id);

    bool contains(uint32_t id);

    bool contains_atleast_one(const uint32_t* target_ids, size_t target_ids_size);

    iterator_t new_iterator();

    static void merge(const std::vector<posting_list_t*>& posting_lists, std::vector<uint32_t>& result_ids);

    static void intersect(const std::vector<posting_list_t*>& posting_lists, std::vector<uint32_t>& result_ids);

    static bool block_intersect(
        const std::vector<posting_list_t*>& posting_lists,
        size_t batch_size,
        std::vector<posting_list_t::iterator_t>& its,
        result_iter_state_t& iter_state
    );

    static bool get_offsets(
        result_iter_state_t& iter_state,
        std::vector<std::unordered_map<size_t, std::vector<token_positions_t>>>& array_token_positions
    );
};
