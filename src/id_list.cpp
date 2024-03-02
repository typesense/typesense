#include "id_list.h"
#include <algorithm>
#include "for.h"

/* block_t operations */

bool id_list_t::block_t::contains(uint32_t id) {
    return ids.contains(id);
}

uint32_t id_list_t::block_t::upsert(const uint32_t id) {
    if (ids.contains(id)) {
        return 0;
    }

    ids.append(id);
    return 1;
}

uint32_t id_list_t::block_t::erase(const uint32_t id) {
    uint32_t doc_index = ids.indexOf(id);

    if (doc_index == ids.getLength()) {
        return 0;
    }

    ids.remove_value(id);
    return 1;
}

/* iterator_t operations */

id_list_t::iterator_t::iterator_t(id_list_t::block_t* start, id_list_t::block_t* end,
                                  std::map<last_id_t, block_t*>* id_block_map, bool reverse):
        curr_block(start), curr_index(0), end_block(end), id_block_map(id_block_map), reverse(reverse) {

    if(curr_block != end_block) {
        ids = curr_block->ids.uncompress();

        if(reverse) {
            curr_index = curr_block->ids.getLength()-1;
        }
    }
}

bool id_list_t::iterator_t::valid() const {
    if(reverse) {
        return (curr_block != end_block) && (curr_index >= 0);
    } else {
        return (curr_block != end_block) && (curr_index < curr_block->size());
    }
}

void id_list_t::iterator_t::next() {
    curr_index++;
    if(curr_index == curr_block->size()) {
        curr_index = 0;
        curr_block = curr_block->next;

        delete [] ids;
        ids = nullptr;

        if(curr_block != end_block) {
            ids = curr_block->ids.uncompress();
        }
    }
}

void id_list_t::iterator_t::previous() {
    curr_index--;
    if(curr_index < 0) {
        // since block stores only the next pointer, we have to use `id_block_map` for reverse iteration
        auto last_ele = ids[curr_block->size()-1];
        auto it = id_block_map->find(last_ele);
        if(it != id_block_map->end() && it != id_block_map->begin()) {
            it--;
            curr_block = it->second;
            curr_index = curr_block->size()-1;

            delete [] ids;
            ids = curr_block->ids.uncompress();
        } else {
            curr_block = end_block;
        }
    }
}

uint32_t id_list_t::iterator_t::id() const {
    return ids[curr_index];
}

uint32_t id_list_t::iterator_t::index() const {
    return curr_index;
}

id_list_t::block_t* id_list_t::iterator_t::block() const {
    return curr_block;
}

uint32_t id_list_t::iterator_t::last_block_id() const {
    auto size = curr_block->size();
    if(size == 0) {
        return 0;
    }

    return ids[size - 1];
}

void id_list_t::iterator_t::reset_cache() {
    delete [] ids;
    ids = nullptr;
    curr_index = 0;
    curr_block = end_block = nullptr;
}

void id_list_t::iterator_t::skip_n(uint32_t n) {
    while(curr_block != end_block) {
        curr_index += n;
        if(curr_index < curr_block->size()) {
            return;
        }

        n = (curr_index - curr_block->size() + 1);
        curr_block = curr_block->next;

        delete [] ids;
        ids = nullptr;

        if(curr_block != end_block) {
            curr_index = 0;
            n--;
            ids = curr_block->ids.uncompress();
        } else {
            reset_cache();
        }
    }
}

void id_list_t::iterator_t::skip_to(uint32_t id) {
    // first look to skip within current block
    if(id <= this->last_block_id()) {
        while(curr_index < curr_block->size() && this->id() < id) {
            curr_index++;
        }

        return ;
    }

    reset_cache();

    const auto it = id_block_map->lower_bound(id);
    if(it == id_block_map->end()) {
        return;
    }

    curr_block = it->second;
    curr_index = 0;
    ids = curr_block->ids.uncompress();

    while(curr_index < curr_block->size() && this->id() < id) {
        curr_index++;
    }

    if(curr_index == curr_block->size()) {
        reset_cache();
    }
}

id_list_t::iterator_t::~iterator_t() {
    delete [] ids;
    ids = nullptr;
}

id_list_t::iterator_t& id_list_t::iterator_t::operator=(id_list_t::iterator_t&& obj) noexcept {
    if (&obj == this) {
        return *this;
    }

    delete [] ids;
    ids = obj.ids;
    obj.ids = nullptr;

    curr_block = obj.curr_block;
    curr_index = obj.curr_index;

    end_block = obj.end_block;
    id_block_map = obj.id_block_map;

    reverse = obj.reverse;

    return *this;
}

id_list_t::iterator_t::iterator_t(iterator_t&& rhs) noexcept {
    curr_block = rhs.curr_block;
    curr_index = rhs.curr_index;
    end_block = rhs.end_block;
    ids = rhs.ids;
    id_block_map = rhs.id_block_map;
    reverse = rhs.reverse;

    rhs.curr_block = nullptr;
    rhs.end_block = nullptr;
    rhs.ids = nullptr;
    rhs.id_block_map = nullptr;
}

/* id_list_t operations */

id_list_t::id_list_t(uint16_t max_block_elements): BLOCK_MAX_ELEMENTS(max_block_elements) {
    if(max_block_elements <= 1) {
        throw std::invalid_argument("max_block_elements must be > 1");
    }
}

id_list_t::~id_list_t() {
    block_t* block = root_block.next;
    while(block != nullptr) {
        block_t* next_block = block->next;
        delete block;
        block = next_block;
    }
}

void id_list_t::merge_adjacent_blocks(id_list_t::block_t* block1, id_list_t::block_t* block2,
                                           size_t num_block2_ids_to_move) {
    // merge ids
    uint32_t* ids1 = block1->ids.uncompress();
    uint32_t* ids2 = block2->ids.uncompress();

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
}

void id_list_t::split_block(id_list_t::block_t* src_block, id_list_t::block_t* dst_block) {
    if(src_block->size() <= 1) {
        return;
    }

    uint32_t* raw_ids = src_block->ids.uncompress();
    size_t ids_first_half_length = (src_block->size() / 2);
    size_t ids_second_half_length = (src_block->size() - ids_first_half_length);
    src_block->ids.load(raw_ids, ids_first_half_length);
    dst_block->ids.load(raw_ids + ids_first_half_length, ids_second_half_length);

    delete [] raw_ids;
}

void id_list_t::upsert(const uint32_t id) {
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
        uint32_t num_inserted = upsert_block->upsert(id);
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
            uint32_t num_inserted = new_block->upsert(id);
            ids_length += num_inserted;
        } else {
            // upsert and then split block
            uint32_t num_inserted = upsert_block->upsert(id);
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

void id_list_t::erase(const uint32_t id) {
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

id_list_t::block_t* id_list_t::get_root() {
    return &root_block;
}

size_t id_list_t::num_blocks() const {
    return id_block_map.size();
}

uint32_t id_list_t::first_id() {
    if(ids_length == 0) {
        return 0;
    }

    return root_block.ids.at(0);
}

uint32_t id_list_t::last_id() {
    if(id_block_map.empty()) {
        return 0;
    }

    return id_block_map.rbegin()->first;
}

id_list_t::block_t* id_list_t::block_of(uint32_t id) {
    const auto it = id_block_map.lower_bound(id);
    if(it == id_block_map.end()) {
        return nullptr;
    }

    return it->second;
}

void id_list_t::merge(const std::vector<id_list_t*>& id_lists, std::vector<uint32_t>& result_ids) {
    auto its = std::vector<id_list_t::iterator_t>();
    its.reserve(id_lists.size());

    size_t sum_sizes = 0;

    for(const auto& id_list: id_lists) {
        its.push_back(id_list->new_iterator());
        sum_sizes += id_list->num_ids();
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
void id_list_t::intersect(const std::vector<id_list_t*>& id_lists, std::vector<uint32_t>& result_ids) {
    if(id_lists.empty()) {
        return;
    }

    if(id_lists.size() == 1) {
        result_ids.reserve(id_lists[0]->ids_length);
        auto it = id_lists[0]->new_iterator();
        while(it.valid()) {
            result_ids.push_back(it.id());
            it.next();
        }

        return ;
    }

    auto its = std::vector<id_list_t::iterator_t>();
    its.reserve(id_lists.size());

    for(const auto& id_list: id_lists) {
        its.push_back(id_list->new_iterator());
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

bool id_list_t::at_end(const std::vector<id_list_t::iterator_t>& its) {
    // if any one iterator is at end, we can stop
    for(const auto& it : its) {
        if(!it.valid()) {
            return true;
        }
    }

    return false;
}

bool id_list_t::at_end2(const std::vector<id_list_t::iterator_t>& its) {
    // if any one iterator is at end, we can stop
    return !its[0].valid() || !its[1].valid();
}

bool id_list_t::equals(std::vector<id_list_t::iterator_t>& its) {
    for(size_t i = 0; i < its.size() - 1; i++) {
        if(its[i].id() != its[i+1].id()) {
            return false;
        }
    }

    return true;
}

bool id_list_t::equals2(std::vector<id_list_t::iterator_t>& its) {
    return its[0].id() == its[1].id();
}

id_list_t::iterator_t id_list_t::new_iterator(block_t* start_block, block_t* end_block) {
    start_block = (start_block == nullptr) ? &root_block : start_block;
    return id_list_t::iterator_t(start_block, end_block, &id_block_map, false);
}

id_list_t::iterator_t id_list_t::new_rev_iterator() {
    block_t* start_block = nullptr;
    if(!id_block_map.empty()) {
        start_block = id_block_map.rbegin()->second;
    }

    auto rev_it = id_list_t::iterator_t(start_block, nullptr, &id_block_map, true);
    return rev_it;
}

void id_list_t::advance_all(std::vector<id_list_t::iterator_t>& its) {
    for(auto& it: its) {
        it.next();
    }
}

void id_list_t::advance_all2(std::vector<id_list_t::iterator_t>& its) {
    its[0].next();
    its[1].next();
}

void id_list_t::advance_non_largest(std::vector<id_list_t::iterator_t>& its) {
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

void id_list_t::advance_non_largest2(std::vector<id_list_t::iterator_t>& its) {
    if(its[0].id() > its[1].id()) {
        its[1].skip_to(its[0].id());
    } else {
        its[0].skip_to(its[1].id());
    }
}

uint32_t id_list_t::advance_smallest(std::vector<id_list_t::iterator_t>& its) {
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

uint32_t id_list_t::advance_smallest2(std::vector<id_list_t::iterator_t>& its) {
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

size_t id_list_t::num_ids() const {
    return ids_length;
}

bool id_list_t::contains(uint32_t id) {
    const auto it = id_block_map.lower_bound(id);

    if(it == id_block_map.end()) {
        return false;
    }

    block_t* potential_block = it->second;
    return potential_block->contains(id);
}

bool id_list_t::contains_atleast_one(const uint32_t* target_ids, size_t target_ids_size) {
    id_list_t::iterator_t it = new_iterator();
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

bool id_list_t::take_id(result_iter_state_t& istate, uint32_t id) {
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

void id_list_t::uncompress(std::vector<uint32_t>& data) {
    auto it = new_iterator();
    data.reserve(data.size() + ids_length);

    while(it.valid()) {
        data.push_back(it.id());
        it.next();
    }
}

uint32_t* id_list_t::uncompress() {
    uint32_t* arr = new uint32_t[ids_length];
    auto it = new_iterator();
    size_t i = 0;

    while(it.valid()) {
        arr[i++] = it.id();
        it.next();
    }

    return arr;
}

size_t id_list_t::intersect_count(const uint32_t *res_ids, size_t res_ids_len,
                                  bool estimate_facets, size_t facet_sample_interval) {
    size_t count = 0;
    size_t res_index = 0;
    auto it = new_iterator();

    if(estimate_facets) {
        while(it.valid() && res_index < res_ids_len) {
            if(it.id() == res_ids[res_index]) {
                count++;
                it.skip_n(facet_sample_interval);
                res_index += facet_sample_interval;
            } else if(it.id() < res_ids[res_index]) {
                it.skip_n(facet_sample_interval);
            } else {
                res_index += facet_sample_interval;
            }
        }
    } else {
        while(it.valid() && res_index < res_ids_len) {
            if(it.id() == res_ids[res_index]) {
                count++;
                it.next();
                res_index += 1;
            } else if(it.id() < res_ids[res_index]) {
                it.next();
            } else {
                res_index += 1;
            }
        }
    }

    //LOG(INFO) << "estimate_facets: " << estimate_facets << ", res_ids_len: " << res_ids_len
    //          << ", skip_interval: " << facet_sample_interval << ", count: " << count;

    if(estimate_facets) {
        count = count * facet_sample_interval * facet_sample_interval;
    }

    return std::min<size_t>(ids_length, count);
}
