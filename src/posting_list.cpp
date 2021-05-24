#include "posting_list.h"
#include "for.h"
#include "array_utils.h"

/* block_t operations */

void posting_list_t::block_t::insert_and_shift_offset_index(const uint32_t index, const uint32_t num_offsets) {
    uint32_t existing_offset_index = offset_index.at(index);
    uint32_t length = offset_index.getLength();
    uint32_t new_length = length + 1;
    uint32_t* curr_array = offset_index.uncompress(new_length);

    memmove(&curr_array[index+1], &curr_array[index], sizeof(uint32_t)*(length - index));
    curr_array[index] = existing_offset_index;

    uint32_t curr_index = index + 1;
    while(curr_index < new_length) {
        curr_array[curr_index] += num_offsets;
        curr_index++;
    }

    offset_index.load(curr_array, new_length);

    delete [] curr_array;
}

void posting_list_t::block_t::upsert(const uint32_t id, const std::vector<uint32_t>& positions) {
    size_t inserted_index = ids.append(id);

    if(inserted_index == ids.getLength()-1) {
        // treat as appends
        uint32_t curr_index = offsets.getLength();
        offset_index.append(curr_index);
        for(uint32_t position : positions) {
            offsets.append(position);
        }
    } else {
        uint32_t existing_offset_index = offset_index.at(inserted_index);
        insert_and_shift_offset_index(inserted_index, positions.size());
        offsets.insert(existing_offset_index, &positions[0], positions.size());
    }
}

void posting_list_t::block_t::erase(const uint32_t id) {
    uint32_t doc_index = ids.indexOf(id);

    if (doc_index == ids.getLength()) {
        return;
    }

    uint32_t start_offset = offset_index.at(doc_index);
    uint32_t end_offset = (doc_index == ids.getLength() - 1) ?
                          offsets.getLength() :
                          offset_index.at(doc_index + 1);

    uint32_t doc_indices[1] = {doc_index};
    remove_and_shift_offset_index(doc_indices, 1);

    offsets.remove_index(start_offset, end_offset);
    ids.remove_value(id);
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

/* posting_list_t operations */

posting_list_t::posting_list_t(uint16_t max_block_elements): BLOCK_MAX_ELEMENTS(max_block_elements) {

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
                                           size_t num_block2_ids) {
    // merge ids
    uint32_t* raw_ids1 = block1->ids.uncompress();
    uint32_t* raw_ids2 = block2->ids.uncompress();

    size_t block1_orig_size = block1->size();
    size_t block2_orig_size = block2->size();

    uint32_t* raw_ids = new uint32_t[block1->size() + num_block2_ids];
    std::memmove(raw_ids, raw_ids1, sizeof(uint32_t) * block1->size());
    std::memmove(raw_ids + block1->size(), raw_ids2, sizeof(uint32_t) * num_block2_ids);

    block1->ids.load(raw_ids, block1->size() + num_block2_ids);
    block2->ids.load(raw_ids2 + num_block2_ids, block2->size() - num_block2_ids);

    delete [] raw_ids1;
    delete [] raw_ids2;
    delete [] raw_ids;

    // merge offset indices
    uint32_t* raw_offset_index1 = block1->offset_index.uncompress();
    uint32_t* raw_offset_index2 = block2->offset_index.uncompress();
    uint32_t* raw_offset_index = new uint32_t[block1_orig_size + block2_orig_size];

    std::memmove(raw_offset_index, raw_offset_index1, sizeof(uint32_t) * block1->offset_index.getLength());
    size_t start_index = block1->offset_index.getLength();
    size_t base_offset_len = block1->offsets.getLength();

    for(size_t i = 0; i < num_block2_ids; i++) {
        raw_offset_index[start_index + i] = raw_offset_index2[i] + base_offset_len;
    }

    block1->offset_index.load(raw_offset_index, block1->offset_index.getLength() + num_block2_ids);

    for(size_t i = 0; i < (block2_orig_size - num_block2_ids); i++) {
        raw_offset_index2[num_block2_ids + i] -= raw_offset_index2[num_block2_ids];
    }

    block2->offset_index.load(raw_offset_index2 + num_block2_ids, block2_orig_size - num_block2_ids);

    // merge offsets
    uint32_t* raw_offsets1 = block1->offsets.uncompress();
    uint32_t* raw_offsets2 = block2->offsets.uncompress();
    size_t num_block2_offset_elements = (num_block2_ids == block2_orig_size) ? block2->offsets.getLength() :
                                        raw_offset_index2[num_block2_ids];

    uint32_t* raw_offsets = new uint32_t[block1->offsets.getLength() + num_block2_offset_elements];

    uint32_t min = raw_offsets1[0], max = raw_offsets1[0];

    // we have to manually copy over so we can find the new min and max
    for(size_t i = 0; i < block1->offsets.getLength(); i++) {
        raw_offsets[i] = raw_offsets1[i];
        if(raw_offsets[i] < min) {
            min = raw_offsets[i];
        }

        if(raw_offsets[i] > max) {
            max = raw_offsets[i];
        }
    }

    size_t block2_base_index = block1->offsets.getLength();

    for(size_t i = 0; i < num_block2_offset_elements; i++) {
        size_t j = block2_base_index + i;
        raw_offsets[j] = raw_offsets2[i];

        if(raw_offsets[j] < min) {
            min = raw_offsets[j];
        }

        if(raw_offsets[j] > max) {
            max = raw_offsets[j];
        }
    }

    block1->offsets.load(raw_offsets, block1->offsets.getLength() + num_block2_offset_elements, min, max);

    // reset block2 offsets with remaining elements
    if(block2->offsets.getLength() != num_block2_offset_elements) {
        const size_t block2_new_offsets_length = (block2->offsets.getLength() - num_block2_offset_elements);
        uint32_t* block2_new_raw_offsets = new uint32_t[block2_new_offsets_length];
        min = max = raw_offsets2[num_block2_offset_elements];
        for(size_t i = 0; i < block2_new_offsets_length; i++) {
            block2_new_raw_offsets[i] = raw_offsets2[num_block2_offset_elements + i];
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

    delete [] raw_offset_index1;
    delete [] raw_offset_index2;
    delete [] raw_offset_index;

    delete [] raw_offsets1;
    delete [] raw_offsets2;
    delete [] raw_offsets;
}

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

    delete [] raw_ids;
    delete [] raw_offset_indices;
    delete [] raw_offsets;
}

void posting_list_t::upsert(const uint32_t id, const std::vector<uint32_t>& offsets) {
    // first we will locate the block where `id` should reside
    block_t* upsert_block;
    last_id_t before_upsert_last_id;

    if(id_block_map.empty()) {
        //id_block_map.emplace(id, &root_block);
        upsert_block = &root_block;
        before_upsert_last_id = UINT32_MAX;
    } else {
        const auto it = id_block_map.lower_bound(id);
        upsert_block = (it == id_block_map.end()) ? id_block_map.rbegin()->second : it->second;
        before_upsert_last_id = upsert_block->ids.at(upsert_block->size() - 1);
    }

    // happy path: upsert_block is not full
    if(upsert_block->size() < BLOCK_MAX_ELEMENTS) {
        upsert_block->upsert(id, offsets);
        last_id_t after_upsert_last_id = upsert_block->ids.at(upsert_block->size() - 1);
        if(before_upsert_last_id != after_upsert_last_id) {
            id_block_map.erase(before_upsert_last_id);
            id_block_map.emplace(after_upsert_last_id, upsert_block);
        }
    } else {
        block_t* new_block = new block_t;

        if(upsert_block->next == nullptr && upsert_block->ids.last() < id) {
            // appending to the end of the last block where the id will reside on a newly block
            new_block->upsert(id, offsets);
        } else {
            // upsert and then split block
            upsert_block->upsert(id, offsets);

            // evenly divide elements between both blocks
            split_block(upsert_block, new_block);

            last_id_t after_upsert_last_id = upsert_block->ids.at(upsert_block->size() - 1);
            id_block_map.erase(before_upsert_last_id);
            id_block_map.emplace(after_upsert_last_id, upsert_block);
        }

        last_id_t after_new_block_id = new_block->ids.at(new_block->size() - 1);
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
    erase_block->erase(id);

    size_t new_ids_length = erase_block->size();

    if(new_ids_length == 0) {
        // happens when the last element of last block is deleted

        if(erase_block != &root_block) {
            // since we will be deleting the empty node, set the previous node's next pointer to null
            std::prev(it)->second->next = nullptr;
            delete erase_block;
        }

        id_block_map.erase(before_last_id);

        return;
    }

    if(new_ids_length >= BLOCK_MAX_ELEMENTS/2 || erase_block->next == nullptr) {
        last_id_t after_last_id = erase_block->ids.at(new_ids_length-1);
        if(before_last_id != after_last_id) {
            id_block_map.erase(before_last_id);
            id_block_map.emplace(after_last_id, erase_block);
        }

        return ;
    }

    // block is less than 50% of max capacity and contains a next node which we can refill from

    auto next_block = erase_block->next;
    last_id_t next_block_last_id = next_block->ids.at(next_block->ids.getLength()-1);

    if(erase_block->size() + next_block->size() <= BLOCK_MAX_ELEMENTS) {
        // we can merge the contents of next block with `erase_block` and delete the next block
        merge_adjacent_blocks(erase_block, next_block, next_block->size());
        erase_block->next = next_block->next;
        delete next_block;

        id_block_map.erase(next_block_last_id);
    } else {
        // only part of the next block can be moved over
        size_t num_block2_ids = BLOCK_MAX_ELEMENTS - erase_block->size();
        merge_adjacent_blocks(erase_block, next_block, num_block2_ids);
        // NOTE: we don't have to update `id_block_map` for `next_block` as last element doesn't change
    }

    last_id_t after_last_id = erase_block->ids.at(erase_block->ids.getLength()-1);
    if(before_last_id != after_last_id) {
        id_block_map.erase(before_last_id);
        id_block_map.emplace(after_last_id, erase_block);
    }
}

posting_list_t::block_t* posting_list_t::get_root() {
    return &root_block;
}

size_t posting_list_t::size() {
    return id_block_map.size();
}

posting_list_t::block_t* posting_list_t::block_of(last_id_t id) {
    auto it = id_block_map.find(id);
    if(it != id_block_map.end()) {
        return it->second;
    }
    return nullptr;
}

// Inspired by: https://stackoverflow.com/a/25509185/131050
posting_list_t* posting_list_t::intersect(const std::vector<posting_list_t*>& posting_lists,
                                          std::vector<uint32_t>& result_ids) {
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
                    advance_least2(its);
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
                    advance_least(its);
                }
            }
    }

    return nullptr;
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

bool posting_list_t::equals(const std::vector<posting_list_t::iterator_t>& its) {
    for(size_t i = 0; i < its.size() - 1; i++) {
        if(its[i].id() != its[i+1].id()) {
            return false;
        }
    }

    return true;
}

bool posting_list_t::equals2(const std::vector<posting_list_t::iterator_t>& its) {
    return its[0].id() == its[1].id();
}

posting_list_t::iterator_t posting_list_t::new_iterator() {
    return posting_list_t::iterator_t(&root_block);
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

void posting_list_t::advance_least(std::vector<posting_list_t::iterator_t>& its) {
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

void posting_list_t::advance_least2(std::vector<posting_list_t::iterator_t>& its) {
    if(its[0].id() > its[1].id()) {
        its[1].skip_to(its[0].id());
    } else {
        its[0].skip_to(its[1].id());
    }
}

/* iterator_t operations */

posting_list_t::iterator_t::iterator_t(posting_list_t::block_t* root): block(root), index(0) {
    ids = root->ids.uncompress();
}

bool posting_list_t::iterator_t::valid() const {
    return (block != nullptr) && (index < block->size());
}

void posting_list_t::iterator_t::next() {
    index++;
    if(index == block->size()) {
        index = 0;
        block = block->next;
        delete [] ids;
        ids = nullptr;
        if(block != nullptr) {
            ids = block->ids.uncompress();
        }
    }
}

uint32_t posting_list_t::iterator_t::id() const {
    //return block->ids.at(index);
    return ids[index];
}

void posting_list_t::iterator_t::offsets(std::vector<uint32_t>& offsets) {
    // TODO
}

void posting_list_t::iterator_t::skip_to(uint32_t id) {
    bool skipped_block = false;
    while(block != nullptr && block->ids.last() < id) {
        block = block->next;
        skipped_block = true;
    }

    if(skipped_block) {
        index = 0;
        delete [] ids;
        ids = nullptr;

        if(block != nullptr) {
            ids = block->ids.uncompress();
        }
    }

    while(block != nullptr && index < block->size() && ids[index] < id) {
        index++;
    }
}

posting_list_t::iterator_t::~iterator_t() {
    delete [] ids;
    ids = nullptr;
}

posting_list_t::iterator_t::iterator_t(iterator_t&& rhs) noexcept {
    block = rhs.block;
    index = rhs.index;
    ids = rhs.ids;

    rhs.block = nullptr;
    rhs.ids = nullptr;
}
