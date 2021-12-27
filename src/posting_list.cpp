#include "posting_list.h"
#include <bitset>
#include "for.h"
#include "array_utils.h"

/* block_t operations */

uint32_t posting_list_t::block_t::upsert(const uint32_t id, const std::vector<uint32_t>& positions) {
    if(id <= ids.last() && ids.getLength() != 0) {
        // we have to check if `id` already exists, for an opportunity to do in-place updates
        uint32_t id_index = ids.indexOf(id);

        if(id_index != ids.getLength()) {
            // id is already present, so we will only update offset index and offsets
            uint32_t start_offset_index = offset_index.at(id_index);
            uint32_t end_offset_index = (id == ids.last()) ? offsets.getLength()-1 : offset_index.at(id_index + 1)-1;
            uint32_t num_offsets = (end_offset_index - start_offset_index) + 1;
            uint32_t* curr_offsets = offsets.uncompress();
            uint32_t m = offsets.getMin(), M = offsets.getMax();

            if(num_offsets == positions.size()) {
                // no need to touch the offset index and need to just do inplace updates of offsets
                bool find_new_min_max = false;
                for(size_t i = 0; i < positions.size(); i++) {
                    if((curr_offsets[start_offset_index + i] == m || curr_offsets[start_offset_index + i] == M) &&
                        curr_offsets[start_offset_index + i] != positions[i]) {
                        // when an existing min/max is affected we will have to find the new min/max
                        find_new_min_max = true;
                    }

                    if(positions[i] < m) {
                        m = positions[i];
                    }

                    if(positions[i] > M) {
                        M = positions[i];
                    }

                    curr_offsets[start_offset_index + i] = positions[i];
                }

                if(find_new_min_max) {
                    for(size_t i = 0; i < offsets.getLength(); i++) {
                        if(curr_offsets[i] < m) {
                            m = curr_offsets[i];
                        }

                        if(curr_offsets[i] > M) {
                            M = curr_offsets[i];
                        }
                    }
                }

                offsets.load(curr_offsets, offsets.getLength(), m, M);
            } else {
                // need to resize offsets array
                int64_t size_diff = int64_t(positions.size()) - num_offsets;   // size_diff can be negative
                size_t new_offsets_length = offsets.getLength() + size_diff;
                uint32_t* new_offsets = new uint32_t[new_offsets_length];
                std::memmove(new_offsets, curr_offsets, sizeof(uint32_t) * start_offset_index);

                bool find_new_min_max = false;
                for(size_t i = 0; i < num_offsets; i++) {
                    if(curr_offsets[start_offset_index + i] == m || curr_offsets[start_offset_index + i] == M) {
                        // when an existing min/max is affected we will have to find the new min/max
                        find_new_min_max = true;
                    }
                }

                for(size_t i = 0; i < positions.size(); i++) {
                    if(positions[i] < m) {
                        m = positions[i];
                    }

                    if(positions[i] > M) {
                        M = positions[i];
                    }

                    new_offsets[start_offset_index + i] = positions[i];
                }

                std::memmove(new_offsets + start_offset_index + positions.size(),
                             curr_offsets + end_offset_index + 1,
                             sizeof(uint32_t) * (offsets.getLength() - (end_offset_index + 1)));

                if(find_new_min_max) {
                    for(size_t i = 0; i < offsets.getLength(); i++) {
                        if(curr_offsets[i] < m) {
                            m = curr_offsets[i];
                        }

                        if(curr_offsets[i] > M) {
                            M = curr_offsets[i];
                        }
                    }
                }

                offsets.load(new_offsets, new_offsets_length, m, M);
                delete [] new_offsets;

                // shift offset index
                uint32_t* current_offset_index = offset_index.uncompress();
                for(size_t i = id_index+1; i < ids.getLength(); i++) {
                    current_offset_index[i] += size_diff;
                }

                offset_index.load(current_offset_index, offset_index.getLength());
                delete [] current_offset_index;
            }

            delete [] curr_offsets;
            return 0;
        }
    }

    // treat as regular append (either id not found or exceeds max id)

    ids.append(id);
    uint32_t curr_index = offsets.getLength();
    offset_index.append(curr_index);
    for(uint32_t position : positions) {
        offsets.append(position);
    }

    return 1;
}

uint32_t posting_list_t::block_t::erase(const uint32_t id) {
    uint32_t doc_index = ids.indexOf(id);

    if (doc_index == ids.getLength()) {
        return 0;
    }

    uint32_t start_offset = offset_index.at(doc_index);
    uint32_t end_offset = (doc_index == ids.getLength() - 1) ?
                          offsets.getLength() :
                          offset_index.at(doc_index + 1);

    uint32_t doc_indices[1] = {doc_index};
    remove_and_shift_offset_index(doc_indices, 1);

    offsets.remove_index(start_offset, end_offset);
    ids.remove_value(id);

    return 1;
}

void posting_list_t::block_t::remove_and_shift_offset_index(const uint32_t* indices_sorted,
                                                            const uint32_t num_indices) {
    uint32_t *curr_array = offset_index.uncompress();
    uint32_t *new_array = new uint32_t[offset_index.getLength()];

    new_array[0] = 0;
    uint32_t new_index = 0;
    uint32_t curr_index = 0;
    uint32_t indices_counter = 0;
    uint32_t shift_value = 0;

    while(curr_index < offset_index.getLength()) {
        if(indices_counter < num_indices && curr_index >= indices_sorted[indices_counter]) {
            // skip copying
            if(curr_index == indices_sorted[indices_counter]) {
                curr_index++;
                const uint32_t diff = curr_index == offset_index.getLength() ?
                                      0 : (offset_index.at(curr_index) - offset_index.at(curr_index-1));

                shift_value += diff;
            }
            indices_counter++;
        } else {
            new_array[new_index++] = curr_array[curr_index++] - shift_value;
        }
    }

    offset_index.load(new_array, new_index);

    delete[] curr_array;
    delete[] new_array;
}

bool posting_list_t::block_t::contains(uint32_t id) {
    return ids.contains(id);
}

/* posting_list_t operations */

posting_list_t::posting_list_t(uint16_t max_block_elements): BLOCK_MAX_ELEMENTS(max_block_elements) {
    if(max_block_elements <= 1) {
        throw std::invalid_argument("max_block_elements must be > 1");
    }
}

posting_list_t::~posting_list_t() {
    block_t* block = root_block.next;
    while(block != nullptr) {
        block_t* next_block = block->next;
        delete block;
        block = next_block;
    }
}

void posting_list_t::merge_adjacent_blocks(posting_list_t::block_t* block1, posting_list_t::block_t* block2,
                                           size_t num_block2_ids_to_move) {
    // merge ids
    uint32_t* ids1 = block1->ids.uncompress();
    uint32_t* ids2 = block2->ids.uncompress();

    size_t block1_orig_size = block1->size();
    size_t block2_orig_size = block2->size();

    size_t block1_orig_offset_size = block1->offsets.getLength();
    size_t block2_orig_offset_size = block2->offsets.getLength();

    size_t block1_orig_offset_index_size = block1->offset_index.getLength();
    size_t block2_orig_offset_index_size = block2->offset_index.getLength();

    uint32_t* new_ids = new uint32_t[block1->size() + num_block2_ids_to_move];
    std::memmove(new_ids, ids1, sizeof(uint32_t) * block1->size());
    std::memmove(new_ids + block1->size(), ids2, sizeof(uint32_t) * num_block2_ids_to_move);

    block1->ids.load(new_ids, block1->size() + num_block2_ids_to_move);
    if(block2->size() != num_block2_ids_to_move) {
        block2->ids.load(ids2 + num_block2_ids_to_move, block2->size() - num_block2_ids_to_move);
    } else {
        block2->ids.load(nullptr, 0);
    }

    delete [] ids1;
    delete [] ids2;
    delete [] new_ids;

    // merge offset indices
    uint32_t* offset_index1 = block1->offset_index.uncompress();
    uint32_t* offset_index2 = block2->offset_index.uncompress();
    uint32_t* new_offset_index = new uint32_t[block1_orig_size + block2_orig_size];

    size_t num_block2_offsets_to_move = (num_block2_ids_to_move == block2_orig_size) ? block2->offsets.getLength() :
                                        offset_index2[num_block2_ids_to_move];

    std::memmove(new_offset_index, offset_index1, sizeof(uint32_t) * block1->offset_index.getLength());
    size_t start_index = block1->offset_index.getLength();
    size_t base_offset_len = block1->offsets.getLength();

    for(size_t i = 0; i < num_block2_ids_to_move; i++) {
        new_offset_index[start_index + i] = offset_index2[i] + base_offset_len;
    }

    block1->offset_index.load(new_offset_index, block1->offset_index.getLength() + num_block2_ids_to_move);

    if(block2->offset_index.getLength() != num_block2_ids_to_move) {
        const uint32_t offset_index2_base_index = offset_index2[num_block2_ids_to_move];

        for(size_t i = 0; i < (block2_orig_size - num_block2_ids_to_move); i++) {
            offset_index2[num_block2_ids_to_move + i] -= offset_index2_base_index;
        }

        block2->offset_index.load(offset_index2 + num_block2_ids_to_move, block2_orig_size - num_block2_ids_to_move);
    } else {
        block2->offset_index.load(nullptr, 0);
    }

    // merge offsets
    uint32_t* offsets1 = block1->offsets.uncompress();
    uint32_t* offsets2 = block2->offsets.uncompress();

    // we will have to compute new min and max for new block1 and block2 offsets

    size_t new_block1_offsets_size = block1->offsets.getLength() + num_block2_offsets_to_move;
    uint32_t* new_block1_offsets = new uint32_t[new_block1_offsets_size];

    uint32_t min = block1->offsets.getLength() != 0 ? offsets1[0] : 0;
    uint32_t max = min;

    // we have to manually copy over so we can find the new min and max
    for(size_t i = 0; i < block1->offsets.getLength(); i++) {
        new_block1_offsets[i] = offsets1[i];
        if(new_block1_offsets[i] < min) {
            min = new_block1_offsets[i];
        }

        if(new_block1_offsets[i] > max) {
            max = new_block1_offsets[i];
        }
    }

    size_t block2_base_index = block1->offsets.getLength();

    for(size_t i = 0; i < num_block2_offsets_to_move; i++) {
        size_t j = block2_base_index + i;
        new_block1_offsets[j] = offsets2[i];

        if(new_block1_offsets[j] < min) {
            min = new_block1_offsets[j];
        }

        if(new_block1_offsets[j] > max) {
            max = new_block1_offsets[j];
        }
    }

    block1->offsets.load(new_block1_offsets, new_block1_offsets_size, min, max);

    // reset block2 offsets with remaining elements
    if(block2->offsets.getLength() != num_block2_offsets_to_move) {
        const size_t block2_new_offsets_length = (block2->offsets.getLength() - num_block2_offsets_to_move);
        uint32_t* block2_new_raw_offsets = new uint32_t[block2_new_offsets_length];
        min = max = offsets2[num_block2_offsets_to_move];
        for(size_t i = 0; i < block2_new_offsets_length; i++) {
            block2_new_raw_offsets[i] = offsets2[num_block2_offsets_to_move + i];
            if(block2_new_raw_offsets[i] < min) {
                min = block2_new_raw_offsets[i];
            }

            if(block2_new_raw_offsets[i] > max) {
                max = block2_new_raw_offsets[i];
            }
        }
        block2->offsets.load(block2_new_raw_offsets, block2_new_offsets_length, min, max);
        delete [] block2_new_raw_offsets;
    } else {
        block2->offsets.load(nullptr, 0, 0, 0);
    }

    if(block1->offsets.getLength() < block1->offset_index.getLength()) {
        LOG(ERROR) << "Block offset length is smaller than offset index length after merging.";
    }

    delete [] offset_index1;
    delete [] offset_index2;
    delete [] new_offset_index;

    delete [] offsets1;
    delete [] offsets2;
    delete [] new_block1_offsets;
}

/*void print_vec(const std::vector<uint32_t>& vec) {
    LOG(INFO) << "---";
    for(auto x: vec) {
        LOG(INFO) << x;
    }
    LOG(INFO) << "---";
}*/

void posting_list_t::split_block(posting_list_t::block_t* src_block, posting_list_t::block_t* dst_block) {
    if(src_block->size() <= 1) {
        return;
    }

    uint32_t* raw_ids = src_block->ids.uncompress();
    size_t ids_first_half_length = (src_block->size() / 2);
    size_t ids_second_half_length = (src_block->size() - ids_first_half_length);
    src_block->ids.load(raw_ids, ids_first_half_length);
    dst_block->ids.load(raw_ids + ids_first_half_length, ids_second_half_length);

    uint32_t* raw_offset_indices = src_block->offset_index.uncompress();
    size_t offset_indices_first_half_length = (src_block->offset_index.getLength() / 2);
    size_t offset_indices_second_half_length = (src_block->offset_index.getLength() - offset_indices_first_half_length);
    src_block->offset_index.load(raw_offset_indices, offset_indices_first_half_length);

    // update second half to use zero based index
    uint32_t base_index_diff = raw_offset_indices[offset_indices_first_half_length];
    for(size_t i = 0; i < offset_indices_second_half_length; i++) {
        raw_offset_indices[offset_indices_first_half_length + i] -= base_index_diff;
    }

    dst_block->offset_index.load(raw_offset_indices + offset_indices_first_half_length, offset_indices_second_half_length);

    uint32_t* raw_offsets = src_block->offsets.uncompress();
    size_t src_offsets_length = src_block->offsets.getLength();

    // load first half of offsets

    size_t offset_first_half_length = base_index_diff;

    // we need to find new min and max
    uint32_t min = raw_offsets[0], max = raw_offsets[0];

    for(size_t i = 0; i < offset_first_half_length; i++) {
        if(raw_offsets[i] < min) {
            min = raw_offsets[i];
        }

        if(raw_offsets[i] > max) {
            max = raw_offsets[i];
        }
    }

    src_block->offsets.load(raw_offsets, offset_first_half_length, min, max);

    // load second half

    min = max = raw_offsets[offset_first_half_length];
    for(size_t i = offset_first_half_length; i < src_offsets_length; i++) {
        if(raw_offsets[i] < min) {
            min = raw_offsets[i];
        }

        if(raw_offsets[i] > max) {
            max = raw_offsets[i];
        }
    }

    size_t offsets_second_half_length = src_offsets_length - offset_first_half_length;
    dst_block->offsets.load(raw_offsets + offset_first_half_length, offsets_second_half_length, min, max);

    if(dst_block->offsets.getLength() < dst_block->offset_index.getLength() ||
       src_block->offsets.getLength() < src_block->offset_index.getLength()) {
        LOG(ERROR) << "Block offset length is smaller than offset index length after splitting.";
    }

    delete [] raw_ids;
    delete [] raw_offset_indices;
    delete [] raw_offsets;
}

void posting_list_t::upsert(const uint32_t id, const std::vector<uint32_t>& offsets) {
    // first we will locate the block where `id` should reside
    block_t* upsert_block;
    last_id_t before_upsert_last_id;

    if(id_block_map.empty()) {
        upsert_block = &root_block;
        before_upsert_last_id = UINT32_MAX;
    } else {
        const auto it = id_block_map.lower_bound(id);
        upsert_block = (it == id_block_map.end()) ? id_block_map.rbegin()->second : it->second;
        before_upsert_last_id = upsert_block->ids.last();
    }

    // happy path: upsert_block is not full
    if(upsert_block->size() < BLOCK_MAX_ELEMENTS) {
        uint32_t num_inserted = upsert_block->upsert(id, offsets);
        ids_length += num_inserted;

        last_id_t after_upsert_last_id = upsert_block->ids.last();
        if(before_upsert_last_id != after_upsert_last_id) {
            id_block_map.erase(before_upsert_last_id);
            id_block_map.emplace(after_upsert_last_id, upsert_block);
        }
    } else {
        block_t* new_block = new block_t;

        if(upsert_block->next == nullptr && upsert_block->ids.last() < id) {
            // appending to the end of the last block where the id will reside on a newly block
            uint32_t num_inserted = new_block->upsert(id, offsets);
            ids_length += num_inserted;
        } else {
            // upsert and then split block
            uint32_t num_inserted = upsert_block->upsert(id, offsets);
            ids_length += num_inserted;

            // evenly divide elements between both blocks
            split_block(upsert_block, new_block);

            last_id_t after_upsert_last_id = upsert_block->ids.last();
            id_block_map.erase(before_upsert_last_id);
            id_block_map.emplace(after_upsert_last_id, upsert_block);
        }

        last_id_t after_new_block_id = new_block->ids.last();
        id_block_map.emplace(after_new_block_id, new_block);

        new_block->next = upsert_block->next;
        upsert_block->next = new_block;
    }
}

void posting_list_t::erase(const uint32_t id) {
    const auto it = id_block_map.lower_bound(id);

    if(it == id_block_map.end()) {
        return ;
    }

    block_t* erase_block = it->second;
    last_id_t before_last_id = it->first;
    uint32_t num_erased = erase_block->erase(id);
    ids_length -= num_erased;

    size_t new_ids_length = erase_block->size();

    if(new_ids_length == 0) {
        // happens when the last element of last block is deleted

        if(erase_block != &root_block) {
            // since we will be deleting the empty node, set the previous node's next pointer to null
            std::prev(it)->second->next = nullptr;
            delete erase_block;
        } else {
            // The root block cannot be empty if there are other blocks so we will pull some contents from next block
            // This is only an issue for blocks with max size of 2
            if(root_block.next != nullptr) {
                auto next_block_last_id = erase_block->next->ids.last();
                merge_adjacent_blocks(erase_block, erase_block->next, erase_block->next->size()/2);
                id_block_map.erase(next_block_last_id);

                id_block_map.emplace(erase_block->next->ids.last(), erase_block->next);
                id_block_map.emplace(erase_block->ids.last(), erase_block);
            }
        }

        id_block_map.erase(before_last_id);

        return;
    }

    if(new_ids_length >= BLOCK_MAX_ELEMENTS/2 || erase_block->next == nullptr) {
        last_id_t after_last_id = erase_block->ids.last();
        if(before_last_id != after_last_id) {
            id_block_map.erase(before_last_id);
            id_block_map.emplace(after_last_id, erase_block);
        }

        return ;
    }

    // block is less than 50% of max capacity and contains a next node which we can refill from

    auto next_block = erase_block->next;
    last_id_t next_block_last_id = next_block->ids.last();

    if(erase_block->size() + next_block->size() <= BLOCK_MAX_ELEMENTS) {
        // we can merge the contents of next block with `erase_block` and delete the next block
        merge_adjacent_blocks(erase_block, next_block, next_block->size());
        erase_block->next = next_block->next;
        delete next_block;

        id_block_map.erase(next_block_last_id);
    } else {
        // Only part of the next block can be moved over.
        // We will move only 50% of max elements to ensure that we don't end up "flipping" adjacent blocks:
        // 1, 5 -> 5, 1
        size_t num_block2_ids = BLOCK_MAX_ELEMENTS/2;
        merge_adjacent_blocks(erase_block, next_block, num_block2_ids);
        // NOTE: we don't have to update `id_block_map` for `next_block` as last element doesn't change
    }

    last_id_t after_last_id = erase_block->ids.last();
    if(before_last_id != after_last_id) {
        id_block_map.erase(before_last_id);
        id_block_map.emplace(after_last_id, erase_block);
    }
}

posting_list_t::block_t* posting_list_t::get_root() {
    return &root_block;
}

size_t posting_list_t::num_blocks() const {
    return id_block_map.size();
}

uint32_t posting_list_t::first_id() {
    if(ids_length == 0) {
        return 0;
    }

    return root_block.ids.at(0);
}

posting_list_t::block_t* posting_list_t::block_of(uint32_t id) {
    const auto it = id_block_map.lower_bound(id);
    if(it == id_block_map.end()) {
        return nullptr;
    }

    return it->second;
}


void posting_list_t::merge(const std::vector<posting_list_t*>& posting_lists, std::vector<uint32_t>& result_ids) {
    auto its = std::vector<posting_list_t::iterator_t>();
    its.reserve(posting_lists.size());

    size_t sum_sizes = 0;

    for(const auto& posting_list: posting_lists) {
        its.push_back(posting_list->new_iterator());
        sum_sizes += posting_list->num_ids();
    }

    result_ids.reserve(sum_sizes);
    size_t num_lists = its.size();

    switch (num_lists) {
        case 2:
            while(!at_end2(its)) {
                if(equals2(its)) {
                    //LOG(INFO) << its[0].id();
                    result_ids.push_back(its[0].id());
                    advance_all2(its);
                } else {
                    uint32_t smallest_value = advance_smallest2(its);
                    result_ids.push_back(smallest_value);
                }
            }

            while(its[0].valid()) {
                result_ids.push_back(its[0].id());
                its[0].next();
            }

            while(its[1].valid()) {
                result_ids.push_back(its[1].id());
                its[1].next();
            }

            break;
        default:
            while(!at_end(its)) {
                if(equals(its)) {
                    result_ids.push_back(its[0].id());
                    advance_all(its);
                } else {
                    uint32_t smallest_value = advance_smallest(its);
                    result_ids.push_back(smallest_value);
                }
            }

            for(auto& it: its) {
                while(it.valid()) {
                    result_ids.push_back(it.id());
                    it.next();
                }
            }
    }
}

// Inspired by: https://stackoverflow.com/a/25509185/131050
void posting_list_t::intersect(const std::vector<posting_list_t*>& posting_lists, std::vector<uint32_t>& result_ids) {
    if(posting_lists.empty()) {
        return;
    }

    if(posting_lists.size() == 1) {
        result_ids.reserve(posting_lists[0]->ids_length);
        auto it = posting_lists[0]->new_iterator();
        while(it.valid()) {
            result_ids.push_back(it.id());
            it.next();
        }

        return ;
    }

    auto its = std::vector<posting_list_t::iterator_t>();
    its.reserve(posting_lists.size());

    for(const auto& posting_list: posting_lists) {
        its.push_back(posting_list->new_iterator());
    }

    size_t num_lists = its.size();

    switch (num_lists) {
        case 2:
            while(!at_end2(its)) {
                if(equals2(its)) {
                    //LOG(INFO) << its[0].id();
                    result_ids.push_back(its[0].id());
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
                    result_ids.push_back(its[0].id());
                    advance_all(its);
                } else {
                    advance_non_largest(its);
                }
            }
    }
}

bool posting_list_t::take_id(result_iter_state_t& istate, uint32_t id) {
    // decide if this result id should be excluded
    if(istate.excluded_result_ids_size != 0) {
        if (std::binary_search(istate.excluded_result_ids,
                               istate.excluded_result_ids + istate.excluded_result_ids_size, id)) {
            return false;
        }
    }

    // decide if this result be matched with filter results
    if(istate.filter_ids_length != 0) {
        return std::binary_search(istate.filter_ids, istate.filter_ids + istate.filter_ids_length, id);
    }

    return true;
}

bool posting_list_t::get_offsets(const std::vector<iterator_t>& its,
                                 std::unordered_map<size_t, std::vector<token_positions_t>>& array_token_pos) {

    // Plain string format:
    // offset1, offset2, ... , 0 (if token is the last offset for the document)

    // Array string format:
    // offset1, ... , offsetn, offsetn, array_index, 0 (if token is the last offset for the document)
    // (last offset is repeated to indicate end of offsets for a given array index)

    // For each result ID and for each block it is contained in, calculate offsets

    size_t id_block_index = 0;

    for(size_t j = 0; j < its.size(); j++) {
        block_t* curr_block = its[j].block();
        uint32_t curr_index = its[j].index();

        if(curr_block == nullptr || curr_index == UINT32_MAX) {
            continue;
        }

        /*uint32_t* offsets = curr_block->offsets.uncompress();

        uint32_t start_offset = curr_block->offset_index.at(curr_index);
        uint32_t end_offset = (curr_index == curr_block->size() - 1) ?
                              curr_block->offsets.getLength() :
                              curr_block->offset_index.at(curr_index + 1);*/

        uint32_t* offsets = its[j].offsets;

        uint32_t start_offset = its[j].offset_index[curr_index];
        uint32_t end_offset = (curr_index == curr_block->size() - 1) ?
                              curr_block->offsets.getLength() :
                              its[j].offset_index[curr_index + 1];

        std::vector<uint16_t> positions;
        int prev_pos = -1;
        bool is_last_token = false;

        /*LOG(INFO) << "id: " << its[j].id() << ", start_offset: " << start_offset << ", end_offset: " << end_offset;
        for(size_t x = 0; x < end_offset; x++) {
            LOG(INFO) << "x: " << x << ", pos: " << offsets[x];
        }*/

        while(start_offset < end_offset) {
            int pos = offsets[start_offset];
            start_offset++;

            if(pos == 0) {
                // indicates that token is the last token on the doc
                is_last_token = true;
                start_offset++;
                continue;
            }

            if(pos == prev_pos) {  // indicates end of array index
                if(!positions.empty()) {
                    size_t array_index = (size_t) offsets[start_offset];
                    is_last_token = false;

                    if(start_offset+1 < end_offset) {
                        size_t next_offset = (size_t) offsets[start_offset + 1];
                        if(next_offset == 0) {
                            // indicates that token is the last token on the doc
                            is_last_token = true;
                            start_offset++;
                        }
                    }

                    array_token_pos[array_index].push_back(token_positions_t{is_last_token, positions});
                    positions.clear();
                }

                start_offset++;  // skip current value which is the array index or flag for last index
                prev_pos = -1;
                continue;
            }

            prev_pos = pos;
            positions.push_back((uint16_t)pos - 1);
        }

        if(!positions.empty()) {
            // for plain string fields
            array_token_pos[0].push_back(token_positions_t{is_last_token, positions});
        }

        //delete [] offsets;
    }

    return true;
}

bool posting_list_t::is_single_token_verbatim_match(const posting_list_t::iterator_t& it, bool field_is_array) {
    block_t* curr_block = it.block();
    uint32_t curr_index = it.index();

    if(curr_block == nullptr || curr_index == UINT32_MAX) {
        return false;
    }

    uint32_t* offsets = it.offsets;
    uint32_t start_offset = it.offset_index[curr_index];

    if(!field_is_array && offsets[start_offset] != 1) {
        // allows us to skip other computes fast
        return false;
    }

    uint32_t end_offset = (curr_index == curr_block->size() - 1) ?
                          curr_block->offsets.getLength() :
                          it.offset_index[curr_index + 1];

    if(field_is_array) {
       int prev_pos = -1;

        while(start_offset < end_offset) {
            int pos = offsets[start_offset];
            start_offset++;

            if(pos == prev_pos && pos == 1 && start_offset+1 < end_offset && offsets[start_offset+1] == 0) {
                return true;
            }

            prev_pos = pos;
        }

        return false;
    } else if((end_offset - start_offset) == 2 && offsets[end_offset-1] == 0) {
        // already checked for `offsets[start_offset] == 1` first
        return true;
    }

    return false;
}

bool posting_list_t::at_end(const std::vector<posting_list_t::iterator_t>& its) {
    // if any one iterator is at end, we can stop
    for(const auto& it : its) {
        if(!it.valid()) {
            return true;
        }
    }

    return false;
}

bool posting_list_t::at_end2(const std::vector<posting_list_t::iterator_t>& its) {
    // if any one iterator is at end, we can stop
    return !its[0].valid() || !its[1].valid();
}

bool posting_list_t::equals(std::vector<posting_list_t::iterator_t>& its) {
    for(size_t i = 0; i < its.size() - 1; i++) {
        if(its[i].id() != its[i+1].id()) {
            return false;
        }
    }

    return true;
}

bool posting_list_t::equals2(std::vector<posting_list_t::iterator_t>& its) {
    return its[0].id() == its[1].id();
}

posting_list_t::iterator_t posting_list_t::new_iterator(block_t* start_block, block_t* end_block) {
    start_block = (start_block == nullptr) ? &root_block : start_block;
    return posting_list_t::iterator_t(start_block, end_block);
}

void posting_list_t::advance_all(std::vector<posting_list_t::iterator_t>& its) {
    for(auto& it: its) {
        it.next();
    }
}

void posting_list_t::advance_all2(std::vector<posting_list_t::iterator_t>& its) {
    its[0].next();
    its[1].next();
}

void posting_list_t::advance_non_largest(std::vector<posting_list_t::iterator_t>& its) {
    // we will find the iter with greatest value and then advance the rest until their value catches up
    uint32_t greatest_value = 0;

    for(size_t i = 0; i < its.size(); i++) {
        if(its[i].id() > greatest_value) {
            greatest_value = its[i].id();
        }
    }

    for(size_t i = 0; i < its.size(); i++) {
        if(its[i].id() != greatest_value) {
            its[i].skip_to(greatest_value);
        }
    }
}

void posting_list_t::advance_non_largest2(std::vector<posting_list_t::iterator_t>& its) {
    if(its[0].id() > its[1].id()) {
        its[1].skip_to(its[0].id());
    } else {
        its[0].skip_to(its[1].id());
    }
}

uint32_t posting_list_t::advance_smallest(std::vector<posting_list_t::iterator_t>& its) {
    // we will advance the iterator(s) with the smallest value and then return that value
    uint32_t smallest_value = UINT32_MAX;

    for(size_t i = 0; i < its.size(); i++) {
        if(its[i].id() < smallest_value) {
            smallest_value = its[i].id();
        }
    }

    for(size_t i = 0; i < its.size(); i++) {
        if(its[i].id() == smallest_value) {
            its[i].next();
        }
    }

    return smallest_value;
}

uint32_t posting_list_t::advance_smallest2(std::vector<posting_list_t::iterator_t>& its) {
    uint32_t smallest_value = 0;

    if(its[0].id() < its[1].id()) {
        smallest_value = its[0].id();
        its[0].next();
    } else {
        smallest_value = its[1].id();
        its[1].next();
    }

    return smallest_value;
}

size_t posting_list_t::num_ids() const {
    return ids_length;
}

bool posting_list_t::contains(uint32_t id) {
    const auto it = id_block_map.lower_bound(id);

    if(it == id_block_map.end()) {
        return false;
    }

    block_t* potential_block = it->second;
    return potential_block->contains(id);
}

bool posting_list_t::contains_atleast_one(const uint32_t* target_ids, size_t target_ids_size) {
    posting_list_t::iterator_t it = new_iterator();
    size_t target_ids_index = 0;

    while(target_ids_index < target_ids_size && it.valid()) {
        uint32_t id = it.id();

        if(id == target_ids[target_ids_index]) {
            return true;
        } else {
            // advance smallest value
            if(id > target_ids[target_ids_index]) {
                while(target_ids_index < target_ids_size && target_ids[target_ids_index] < id) {
                    target_ids_index++;
                }
            } else {
                it.skip_to(target_ids[target_ids_index]);
            }
        }
    }

    return false;
}

void posting_list_t::get_exact_matches(std::vector<iterator_t>& its, const bool field_is_array,
                                       const uint32_t* ids, const uint32_t num_ids,
                                       uint32_t*& exact_ids, size_t& num_exact_ids) {

    size_t exact_id_index = 0;

    if(its.size() == 1) {
        for(size_t i = 0; i < num_ids; i++) {
            uint32_t id = ids[i];
            its[0].skip_to(id);
            if(is_single_token_verbatim_match(its[0], field_is_array)) {
                exact_ids[exact_id_index++] = id;
            }
        }
    } else {

        if(!field_is_array) {
            for(size_t i = 0; i < num_ids; i++) {
                uint32_t id = ids[i];
                bool is_exact_match = true;

                for(int j = its.size()-1; j >= 0; j--) {
                    posting_list_t::iterator_t& it = its[j];
                    it.skip_to(id);

                    block_t* curr_block = it.block();
                    uint32_t curr_index = it.index();

                    if(curr_block == nullptr || curr_index == UINT32_MAX) {
                        is_exact_match = false;
                        break;
                    }

                    uint32_t* offsets = it.offsets;

                    uint32_t start_offset_index = it.offset_index[curr_index];
                    uint32_t end_offset_index = (curr_index == curr_block->size() - 1) ?
                                                curr_block->offsets.getLength() :
                                                it.offset_index[curr_index + 1];

                    if(j == its.size()-1) {
                        // check if the last query token is the last offset
                        if( offsets[end_offset_index-1] != 0 ||
                            (end_offset_index-2 >= 0 && offsets[end_offset_index-2] != its.size())) {
                            // not the last token for the document, so skip
                            is_exact_match = false;
                            break;
                        }
                    }

                    // looping handles duplicate query tokens, e.g. "hip hip hurray hurray"
                    while(start_offset_index < end_offset_index) {
                        uint32_t offset = offsets[start_offset_index];
                        start_offset_index++;

                        if(offset == (j + 1)) {
                            // we have found a matching index, no need to look further
                            is_exact_match = true;
                            break;
                        }

                        if(offset > (j + 1)) {
                            is_exact_match = false;
                            break;
                        }
                    }

                    if(!is_exact_match) {
                        break;
                    }
                }

                if(is_exact_match) {
                    exact_ids[exact_id_index++] = id;
                }
            }
        }

        else {
            // field is an array

            for(size_t i = 0; i < num_ids; i++) {
                uint32_t id = ids[i];

                std::map<size_t, std::bitset<32>> array_index_to_token_index;
                bool premature_exit = false;

                for(int j = its.size()-1; j >= 0; j--) {
                    posting_list_t::iterator_t& it = its[j];

                    it.skip_to(id);

                    block_t* curr_block = it.block();
                    uint32_t curr_index = it.index();

                    if(curr_block == nullptr || curr_index == UINT32_MAX) {
                        premature_exit = true;
                        break;
                    }

                    uint32_t* offsets = it.offsets;
                    uint32_t start_offset_index = it.offset_index[curr_index];
                    uint32_t end_offset_index = (curr_index == curr_block->size() - 1) ?
                                                curr_block->offsets.getLength() :
                                                it.offset_index[curr_index + 1];

                    int prev_pos = -1;
                    bool has_atleast_one_last_token = false;
                    bool found_matching_index = false;

                    while(start_offset_index < end_offset_index) {
                        int pos = offsets[start_offset_index];
                        start_offset_index++;

                        if(pos == prev_pos) {  // indicates end of array index
                            size_t array_index = (size_t) offsets[start_offset_index];

                            if(start_offset_index+1 < end_offset_index) {
                                size_t next_offset = (size_t) offsets[start_offset_index + 1];
                                if(next_offset == 0 && pos == its.size()) {
                                    // indicates that token is the last token on the doc
                                    has_atleast_one_last_token = true;
                                    start_offset_index++;
                                }
                            }

                            if(found_matching_index) {
                                array_index_to_token_index[array_index].set(j+1);
                            }

                            start_offset_index++;  // skip current value which is the array index or flag for last index
                            prev_pos = -1;
                            continue;
                        }

                        if(pos == (j + 1)) {
                            // we have found a matching index
                            found_matching_index = true;
                        }

                        prev_pos = pos;
                    }

                    // check if the last query token is the last offset of ANY array element
                    if(j == its.size()-1 && !has_atleast_one_last_token) {
                        premature_exit = true;
                        break;
                    }

                    if(!found_matching_index) {
                        // not even a single matching index found: can never be an exact match
                        premature_exit = true;
                        break;
                    }
                }

                if(!premature_exit) {
                    // iterate array index to token index to check if atleast 1 array position contains all tokens
                    for(auto& kv: array_index_to_token_index) {
                        if(kv.second.count() == its.size()) {
                            exact_ids[exact_id_index++] = id;
                            break;
                        }
                    }
                }
            }
        }
    }

    num_exact_ids = exact_id_index;
}

void posting_list_t::get_phrase_matches(std::vector<iterator_t>& its, bool field_is_array, const uint32_t* ids,
                                        const uint32_t num_ids, uint32_t*& phrase_ids, size_t& num_phrase_ids) {

    size_t phrase_id_index = 0;

    if(its.size() == 1) {
        for(size_t i = 0; i < num_ids; i++) {
            phrase_ids[phrase_id_index] = ids[i];
            phrase_id_index++;
        }
    } else {
        for(size_t i = 0; i < num_ids; i++) {
            uint32_t id = ids[i];

            for (int j = its.size() - 1; j >= 0; j--) {
                posting_list_t::iterator_t& it = its[j];
                it.skip_to(id);
            }

            std::unordered_map<size_t, std::vector<token_positions_t>> array_token_positions;
            get_offsets(its, array_token_positions);

            for(auto& kv: array_token_positions) {
                auto dist = Match(id, kv.second, false, false).distance;
                if(dist == its.size() - 1) {
                    phrase_ids[phrase_id_index] = ids[i];
                    phrase_id_index++;
                    break;
                }
            }
        }
    }

    num_phrase_ids = phrase_id_index;
}

void posting_list_t::get_matching_array_indices(uint32_t id, std::vector<iterator_t>& its,
                                                std::vector<size_t>& indices) {
    std::map<size_t, std::bitset<32>> array_index_to_token_index;

    for(int j = its.size()-1; j >= 0; j--) {
        posting_list_t::iterator_t& it = its[j];

        it.skip_to(id);

        block_t* curr_block = it.block();
        uint32_t curr_index = it.index();

        if(curr_block == nullptr || curr_index == UINT32_MAX) {
            return;
        }

        uint32_t* offsets = it.offsets;
        uint32_t start_offset_index = it.offset_index[curr_index];
        uint32_t end_offset_index = (curr_index == curr_block->size() - 1) ?
                                    curr_block->offsets.getLength() :
                                    it.offset_index[curr_index + 1];

        int prev_pos = -1;
        while(start_offset_index < end_offset_index) {
            int pos = offsets[start_offset_index];
            start_offset_index++;

            if(pos == prev_pos) {  // indicates end of array index
                size_t array_index = (size_t) offsets[start_offset_index];

                if(start_offset_index+1 < end_offset_index) {
                    size_t next_offset = (size_t) offsets[start_offset_index + 1];
                    if(next_offset == 0) {
                        // indicates that token is the last token on the doc
                        start_offset_index++;
                    }
                }

                array_index_to_token_index[array_index].set(j+1);
                start_offset_index++;  // skip current value which is the array index or flag for last index
                prev_pos = -1;
                continue;
            }

            prev_pos = pos;
        }
    }

    // iterate array index to token index to check if atleast 1 array position contains all tokens
    for(auto& kv: array_index_to_token_index) {
        if(kv.second.count() == its.size()) {
            indices.push_back(kv.first);
        }
    }
}

/* iterator_t operations */

posting_list_t::iterator_t::iterator_t(posting_list_t::block_t* start, posting_list_t::block_t* end):
        curr_block(start), curr_index(0), end_block(end) {

    if(curr_block != end_block) {
        ids = curr_block->ids.uncompress();
        offset_index = curr_block->offset_index.uncompress();
        offsets = curr_block->offsets.uncompress();
    }
}

bool posting_list_t::iterator_t::valid() const {
    return (curr_block != end_block) && (curr_index < curr_block->size());
}

void posting_list_t::iterator_t::next() {
    curr_index++;
    if(curr_index == curr_block->size()) {
        curr_index = 0;
        curr_block = curr_block->next;

        delete [] ids;
        delete [] offset_index;
        delete [] offsets;

        ids = offset_index = offsets = nullptr;

        if(curr_block != end_block) {
            ids = curr_block->ids.uncompress();
            offset_index = curr_block->offset_index.uncompress();
            offsets = curr_block->offsets.uncompress();
        }
    }
}

uint32_t posting_list_t::iterator_t::id() const {
    return ids[curr_index];
}

uint32_t posting_list_t::iterator_t::index() const {
    return curr_index;
}

posting_list_t::block_t* posting_list_t::iterator_t::block() const {
    return curr_block;
}

void posting_list_t::iterator_t::skip_to(uint32_t id) {
    bool skipped_block = false;
    while(curr_block != end_block && curr_block->ids.last() < id) {
        curr_block = curr_block->next;

        // FIXME: remove duplication
        delete [] ids;
        delete [] offset_index;
        delete [] offsets;

        ids = offset_index = offsets = nullptr;

        if(curr_block != end_block) {
            ids = curr_block->ids.uncompress();
            offset_index = curr_block->offset_index.uncompress();
            offsets = curr_block->offsets.uncompress();
        }

        skipped_block = true;
    }

    if(skipped_block) {
        curr_index = 0;
    }

    while(curr_block != end_block && curr_index < curr_block->size() && this->id() < id) {
        curr_index++;
    }
}

posting_list_t::iterator_t::~iterator_t() {
    delete [] ids;
    ids = nullptr;

    delete [] offsets;
    offsets = nullptr;

    delete [] offset_index;
    offset_index = nullptr;
}

posting_list_t::iterator_t::iterator_t(iterator_t&& rhs) noexcept {
    curr_block = rhs.curr_block;
    curr_index = rhs.curr_index;
    end_block = rhs.end_block;
    ids = rhs.ids;
    offset_index = rhs.offset_index;
    offsets = rhs.offsets;

    rhs.curr_block = nullptr;
    rhs.end_block = nullptr;
    rhs.ids = nullptr;
    rhs.offset_index = nullptr;
    rhs.offsets = nullptr;
}
