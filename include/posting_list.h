#pragma once
#include <map>
#include "sorted_array.h"
#include "array.h"

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

        void insert_and_shift_offset_index(uint32_t index, uint32_t num_offsets);

        void remove_and_shift_offset_index(const uint32_t* indices_sorted, uint32_t num_indices);

        void upsert(uint32_t id, const std::vector<uint32_t>& offsets);

        void erase(uint32_t id);

        uint32_t size() {
            return ids.getLength();
        }
    };

private:

    // when a block reaches pre-allocated storage, it is expanded by this factor
    static constexpr float BLOCK_GROWTH_FACTOR = 1.3;

    // maximum number of IDs (and associated offsets) to store in each block before another block is created
    const uint16_t BLOCK_MAX_ELEMENTS;

    block_t root_block;

    // keeps track of the *last* ID in each block and is used for partial random access
    // e.g. 0..[9], 10..[19], 20..[29]
    // MUST be ordered
    std::map<last_id_t, block_t*> id_block_map;

    static void split_block(block_t* src_block, block_t* dst_block);

    static void merge_adjacent_blocks(block_t* block1, block_t* block2, size_t num_block2_ids);

public:

    posting_list_t() = delete;

    explicit posting_list_t(uint16_t max_block_elements);

    ~posting_list_t();

    void upsert(uint32_t id, const std::vector<uint32_t>& offsets);

    void erase(uint32_t id);

    block_t* get_root();

    size_t size();

    block_t* block_of(last_id_t id);
};