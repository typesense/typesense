#include "ids_t.h"
#include "id_list.h"

int64_t compact_id_list_t::upsert(const uint32_t id) {
    // format: id1, id2, id3
    uint32_t last_id = (length == 0) ? 0 : ids[length - 1];
    int64_t extra_length_needed = 0;

    if(length == 0 || id > last_id) {
        extra_length_needed = 1;
        if(length + extra_length_needed > capacity) {
            // enough storage should have been provided upstream
            return (length + extra_length_needed) - capacity;
        }

        // can just append to the end
        ids[length++] = id;
    } else {
        // locate position and shift contents to make space available
        int64_t i = 0;

        while(i < length) {
            size_t existing_id = ids[i];
            if(existing_id == id) {
                break;
            }

            else if(existing_id > id) {
                extra_length_needed = 1;
                if(length + extra_length_needed > capacity) {
                    // enough storage should have been provided upstream
                    return (length + extra_length_needed) - capacity;
                }

                // shift index [i..length-1] by `extra_length_needed` positions
                int64_t shift_index = length + extra_length_needed - 1;
                while((shift_index - extra_length_needed) >= 0 && shift_index >= i) {
                    ids[shift_index] = ids[shift_index - extra_length_needed];
                    shift_index--;
                }
                // now store the new offsets in the shifted space
                ids[i++] = id;
                break;
            }

            i++;
        }

        length += extra_length_needed;
    }

    return 0;
}

void compact_id_list_t::erase(const uint32_t id) {
    // locate position and shift contents to collapse space vacated
    size_t i = 0;
    while(i < length) {
        size_t existing_id = ids[i];
        if(existing_id > id) {
            // not found!
            return ;
        }

        if(existing_id == id) {
            size_t shift_offset = 1;
            while(i+shift_offset < length) {
                ids[i] = ids[i + shift_offset];
                i++;
            }

            length -= shift_offset;
            break;
        }

        i++;
    }
}

compact_id_list_t* compact_id_list_t::create(uint32_t num_ids, const std::vector<uint32_t>& ids) {
    return create(num_ids, &ids[0]);
}

compact_id_list_t* compact_id_list_t::create(uint32_t num_ids, const uint32_t* ids) {
    // format: id1, id2, id3, ...

    compact_id_list_t* pl = (compact_id_list_t*) malloc(sizeof(compact_id_list_t) +
                                                        (num_ids * sizeof(uint32_t)));

    pl->length = 0;
    pl->capacity = num_ids;

    for(size_t i = 0; i < num_ids; i++) {
        pl->upsert(ids[i]);
    }

    return pl;
}

id_list_t* compact_id_list_t::to_full_ids_list() const {
    id_list_t* pl = new id_list_t(ids_t::MAX_BLOCK_ELEMENTS);

    size_t i = 0;
    while(i < length) {
        size_t existing_id = ids[i];
        pl->upsert(existing_id);
        i++;
    }

    return pl;
}

uint32_t compact_id_list_t::last_id() {
    return (length == 0) ? UINT32_MAX : ids[length - 1];
}

uint32_t compact_id_list_t::num_ids() const {
    return length;
}

uint32_t compact_id_list_t::first_id() {
    if(length == 0) {
        return 0;
    }

    return ids[0];
}

bool compact_id_list_t::contains(uint32_t id) {
    size_t i = 0;
    while(i < length) {
        size_t existing_id = ids[i];
        if(existing_id > id) {
            // not found!
            return false;
        }

        if(existing_id == id) {
            return true;
        }

        i++;
    }

    return false;
}

size_t compact_id_list_t::intersect_count(const uint32_t* res_ids, size_t res_ids_len) {
    size_t count = 0;
    size_t i = 0;
    size_t res_index = 0;

    while(i < length && res_index < res_ids_len) {
        size_t curr_id = ids[i];

        if(curr_id < res_ids[res_index]) {
            i++;
        } else if(curr_id > res_ids[res_index]) {
            // returns index that is >= to value or last if no such element is found.
            res_index = std::lower_bound(res_ids + res_index, res_ids + res_ids_len, curr_id) - res_ids;
        } else {
            i++;
            res_index++;
            count++;
        }
    }

    return count;
}

/* posting operations */

void ids_t::upsert(void*& obj, uint32_t id) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = (compact_id_list_t*) RAW_IDS_PTR(obj);
        int64_t extra_capacity_required = list->upsert(id);

        if(extra_capacity_required == 0) {
            // upsert succeeded
            return;
        }

        if((list->capacity + extra_capacity_required) > COMPACT_LIST_THRESHOLD_LENGTH) {
            // we have to convert to a full posting list
            id_list_t* full_list = list->to_full_ids_list();
            free(list);
            obj = full_list;
        }

        else {
            // grow the container by 30%
            size_t new_capacity = std::min<size_t>((list->capacity + extra_capacity_required) * 1.3,
                                                   COMPACT_LIST_THRESHOLD_LENGTH);

            size_t new_capacity_bytes = sizeof(compact_id_list_t) + (new_capacity * sizeof(uint32_t));
            auto new_list = (compact_id_list_t *) realloc(list, new_capacity_bytes);
            if(new_list == nullptr) {
                abort();
            }

            list = new_list;
            list->capacity = new_capacity;
            obj = SET_COMPACT_IDS(list);

            list->upsert(id);

            return ;
        }
    }

    // either `obj` is already a full list or was converted to a full list above
    id_list_t* list = (id_list_t*)(obj);
    list->upsert(id);
}

void ids_t::erase(void*& obj, uint32_t id) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        list->erase(id);

        // if the list becomes too small, we resize it to save memory
        if(list->length < list->capacity/2) {
            // resize container
            size_t new_capacity = list->capacity/2;
            size_t new_capacity_bytes = sizeof(compact_id_list_t) + (new_capacity * sizeof(uint32_t));
            auto new_list = (compact_id_list_t *) realloc(list, new_capacity_bytes);
            if(new_list == nullptr) {
                abort();
            }

            list = new_list;
            list->capacity = new_capacity;
            obj = SET_COMPACT_IDS(list);
        }

    } else {
        id_list_t* list = (id_list_t*)(obj);
        list->erase(id);

        if(list->num_blocks() == 1 && list->get_root()->size() <= COMPACT_LIST_THRESHOLD_LENGTH) {
            // convert to compact posting format
            auto root_block = list->get_root();
            auto ids = root_block->ids.uncompress();

            compact_id_list_t* compact_list = compact_id_list_t::create(root_block->size(), ids);

            delete [] ids;
            delete list;

            obj = SET_COMPACT_IDS(compact_list);
        }
    }
}

uint32_t ids_t::num_ids(const void* obj) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        return list->num_ids();
    } else {
        id_list_t* list = (id_list_t*)(obj);
        return list->num_ids();
    }
}

uint32_t ids_t::first_id(const void* obj) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        return list->first_id();
    } else {
        id_list_t* list = (id_list_t*)(obj);
        return list->first_id();
    }
}

bool ids_t::contains(const void* obj, uint32_t id) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        return list->contains(id);
    } else {
        id_list_t* list = (id_list_t*)(obj);
        return list->contains(id);
    }
}

void ids_t::merge(const std::vector<void*>& raw_posting_lists, std::vector<uint32_t>& result_ids) {
    // we will have to convert the compact posting list (if any) to full form
    std::vector<id_list_t*> id_lists;
    std::vector<id_list_t*> expanded_id_lists;
    to_expanded_id_lists(raw_posting_lists, id_lists, expanded_id_lists);

    id_list_t::merge(id_lists, result_ids);

    for(id_list_t* expanded_plist: expanded_id_lists) {
        delete expanded_plist;
    }
}

void ids_t::intersect(const std::vector<void*>& raw_posting_lists, std::vector<uint32_t>& result_ids) {
    // we will have to convert the compact posting list (if any) to full form
    std::vector<id_list_t*> id_lists;
    std::vector<id_list_t*> expanded_id_lists;
    to_expanded_id_lists(raw_posting_lists, id_lists, expanded_id_lists);

    id_list_t::intersect(id_lists, result_ids);

    for(auto expanded_plist: expanded_id_lists) {
        delete expanded_plist;
    }
}

void ids_t::to_expanded_id_lists(const std::vector<void*>& raw_posting_lists, std::vector<id_list_t*>& id_lists,
                                   std::vector<id_list_t*>& expanded_id_lists) {
    for(size_t i = 0; i < raw_posting_lists.size(); i++) {
        auto raw_posting_list = raw_posting_lists[i];

        if(IS_COMPACT_IDS(raw_posting_list)) {
            auto compact_posting_list = COMPACT_IDS_PTR(raw_posting_list);
            id_list_t* full_posting_list = compact_posting_list->to_full_ids_list();
            id_lists.emplace_back(full_posting_list);
            expanded_id_lists.push_back(full_posting_list);
        } else {
            id_list_t* full_posting_list = (id_list_t*)(raw_posting_list);
            id_lists.emplace_back(full_posting_list);
        }
    }
}

void ids_t::destroy_list(void*& obj) {
    if(obj == nullptr) {
        return;
    }

    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        free(list); // assigned via malloc, so must be free()d
    } else {
        id_list_t* list = (id_list_t*)(obj);
        delete list;
    }

    obj = nullptr;
}

uint32_t* ids_t::uncompress(void*& obj) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        uint32_t* arr = new uint32_t[list->length];
        std::memcpy(arr, list->ids, list->length * sizeof(uint32_t));
        return arr;
    } else {
        id_list_t* list = (id_list_t*)(obj);
        return list->uncompress();
    }
}

void ids_t::uncompress(void*& obj, std::vector<uint32_t>& ids) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        for(size_t i = 0; i < list->length; i++) {
            ids.push_back(list->ids[i]);
        }
    } else {
        id_list_t* list = (id_list_t*)(obj);
        list->uncompress(ids);
    }
}

size_t ids_t::intersect_count(void*& obj, const uint32_t* result_ids, size_t result_ids_len) {
    if(IS_COMPACT_IDS(obj)) {
        compact_id_list_t* list = COMPACT_IDS_PTR(obj);
        return list->intersect_count(result_ids, result_ids_len);
    } else {
        id_list_t* list = (id_list_t*)(obj);
        return list->intersect_count(result_ids, result_ids_len);
    }
}

void* ids_t::create(const std::vector<uint32_t>& ids) {
    if(ids.size() < COMPACT_LIST_THRESHOLD_LENGTH) {
        return SET_COMPACT_IDS(compact_id_list_t::create(ids.size(), ids));
    } else {
        id_list_t* pl = new id_list_t(ids_t::MAX_BLOCK_ELEMENTS);
        for(auto id: ids) {
            pl->upsert(id);
        }

        return pl;
    }
}

void ids_t::block_intersector_t::split_lists(size_t concurrency,
                                             std::vector<std::vector<id_list_t::iterator_t>>& partial_its_vec) {
    const size_t num_blocks = this->id_lists[0]->num_blocks();
    const size_t window_size = (num_blocks + concurrency - 1) / concurrency;  // rounds up

    size_t blocks_traversed = 0;
    id_list_t::block_t* start_block = this->id_lists[0]->get_root();
    id_list_t::block_t* curr_block = start_block;

    size_t window_index = 0;

    while(curr_block != nullptr) {
        blocks_traversed++;
        if(blocks_traversed % window_size == 0 || blocks_traversed == num_blocks) {
            // construct partial iterators and intersect within them

            std::vector<id_list_t::iterator_t>& partial_its = partial_its_vec[window_index];

            for(size_t i = 0; i < this->id_lists.size(); i++) {
                id_list_t::block_t* p_start_block = nullptr;
                id_list_t::block_t* p_end_block = nullptr;

                // [1, 2] [3, 4] [5, 6]
                // [3, 5] [6]

                if(i == 0) {
                    p_start_block = start_block;
                    p_end_block = curr_block->next;
                } else {
                    auto start_block_first_id = start_block->ids.at(0);
                    auto end_block_last_id = curr_block->ids.last();

                    p_start_block = this->id_lists[i]->block_of(start_block_first_id);
                    id_list_t::block_t* last_block = this->id_lists[i]->block_of(end_block_last_id);

                    if(p_start_block == last_block && p_start_block != nullptr) {
                        p_end_block = p_start_block->next;
                    } else {
                        p_end_block = last_block == nullptr ? nullptr : last_block->next;
                    }
                }

                partial_its.emplace_back(p_start_block, p_end_block, nullptr, false);
            }

            start_block = curr_block->next;
            window_index++;
        }

        curr_block = curr_block->next;
    }
}

