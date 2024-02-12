#include "num_tree.h"
#include "parasort.h"
#include "timsort.hpp"

void num_tree_t::insert(int64_t value, uint32_t id, bool is_facet) {
    if (int64map.count(value) == 0) {
        int64map.emplace(value, SET_COMPACT_IDS(compact_id_list_t::create(1, {id})));
    } else {
        auto ids = int64map[value];
        if (!ids_t::contains(ids, id)) {
            ids_t::upsert(ids, id);
            int64map[value] = ids;
        }
    }
}

void num_tree_t::range_inclusive_search(int64_t start, int64_t end, uint32_t** ids, size_t& ids_len) {
    if(int64map.empty()) {
        return ;
    }

    auto it_start = int64map.lower_bound(start);  // iter values will be >= start

    std::vector<uint32_t> consolidated_ids;
    while(it_start != int64map.end() && it_start->first <= end) {
        uint32_t* values = ids_t::uncompress(it_start->second);

        for(size_t i = 0; i < ids_t::num_ids(it_start->second); i++) {
            consolidated_ids.push_back(values[i]);
        }

        delete [] values;
        it_start++;
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());

    uint32_t *out = nullptr;
    ids_len = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                    *ids, ids_len, &out);

    delete [] *ids;
    *ids = out;
}

void num_tree_t::approx_range_inclusive_search_count(int64_t start, int64_t end, uint32_t& ids_len) {
    if (int64map.empty()) {
        return;
    }

    auto it_start = int64map.lower_bound(start);  // iter values will be >= start

    while (it_start != int64map.end() && it_start->first <= end) {
        uint32_t val_ids = ids_t::num_ids(it_start->second);
        ids_len += val_ids;
        it_start++;
    }
}

bool num_tree_t::range_inclusive_contains(const int64_t& start, const int64_t& end, const uint32_t& id) const {
    if (int64map.empty()) {
        return false;
    }

    auto it_start = int64map.lower_bound(start);  // iter values will be >= start

    while (it_start != int64map.end() && it_start->first <= end) {
        if (ids_t::contains(it_start->second, id)) {
            return true;
        }
    }

    return false;
}

void num_tree_t::range_inclusive_contains(const int64_t& start, const int64_t& end,
                                          const uint32_t& context_ids_length,
                                          uint32_t* const& context_ids,
                                          size_t& result_ids_len,
                                          uint32_t*& result_ids) const {
    if (int64map.empty()) {
        return;
    }

    std::vector<uint32_t> consolidated_ids;
    consolidated_ids.reserve(context_ids_length);
    for (uint32_t i = 0; i < context_ids_length; i++) {
        if (range_inclusive_contains(start, end, context_ids[i])) {
            consolidated_ids.push_back(context_ids[i]);
        }
    }

    uint32_t *out = nullptr;
    result_ids_len = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                           result_ids, result_ids_len, &out);

    delete [] result_ids;
    result_ids = out;
}

size_t num_tree_t::get(int64_t value, std::vector<uint32_t>& geo_result_ids) {
    const auto& it = int64map.find(value);
    if(it == int64map.end()) {
        return 0;
    }

    uint32_t* ids = ids_t::uncompress(it->second);
    for(size_t i = 0; i < ids_t::num_ids(it->second); i++) {
        geo_result_ids.push_back(ids[i]);
    }

    delete [] ids;

    return ids_t::num_ids(it->second);
}

void num_tree_t::search(NUM_COMPARATOR comparator, int64_t value, uint32_t** ids, size_t& ids_len) {
    if(int64map.empty()) {
        return ;
    }

    if(comparator == EQUALS) {
        const auto& it = int64map.find(value);
        if(it != int64map.end()) {
            uint32_t *out = nullptr;
            uint32_t* val_ids = ids_t::uncompress(it->second);
            ids_len = ArrayUtils::or_scalar(val_ids, ids_t::num_ids(it->second),
                                            *ids, ids_len, &out);
            delete[] *ids;
            *ids = out;
            delete[] val_ids;
        }
    } else if(comparator == GREATER_THAN || comparator == GREATER_THAN_EQUALS) {
        // iter entries will be >= value, or end() if all entries are before value
        auto iter_ge_value = int64map.lower_bound(value);

        if(iter_ge_value == int64map.end()) {
            return ;
        }

        if(comparator == GREATER_THAN && iter_ge_value->first == value) {
            iter_ge_value++;
        }

        std::vector<uint32_t> consolidated_ids;
        while(iter_ge_value != int64map.end()) {
            ids_t::uncompress(iter_ge_value->second, consolidated_ids);
            iter_ge_value++;
        }

        gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
        consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

        uint32_t *out = nullptr;
        ids_len = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                        *ids, ids_len, &out);

        delete [] *ids;
        *ids = out;

    } else if(comparator == LESS_THAN || comparator == LESS_THAN_EQUALS) {
        // iter entries will be >= value, or end() if all entries are before value
        auto iter_ge_value = int64map.lower_bound(value);

        std::vector<uint32_t> consolidated_ids;
        auto it = int64map.begin();

        while(it != iter_ge_value) {
            ids_t::uncompress(it->second, consolidated_ids);
            it++;
        }

        // for LESS_THAN_EQUALS, check if last iter entry is equal to value
        if(it != int64map.end() && comparator == LESS_THAN_EQUALS && it->first == value) {
            ids_t::uncompress(it->second, consolidated_ids);
        }

        gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
        consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

        uint32_t *out = nullptr;
        ids_len = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                        *ids, ids_len, &out);

        delete [] *ids;
        *ids = out;
    }
}

uint32_t num_tree_t::approx_search_count(NUM_COMPARATOR comparator, int64_t value) {
    if (int64map.empty()) {
        return 0;
    }

    uint32_t ids_len = 0;
    if (comparator == EQUALS) {
        const auto& it = int64map.find(value);
        if (it != int64map.end()) {
            uint32_t val_ids = ids_t::num_ids(it->second);
            ids_len += val_ids;
        }
    } else if (comparator == GREATER_THAN || comparator == GREATER_THAN_EQUALS) {
        // iter entries will be >= value, or end() if all entries are before value
        auto iter_ge_value = int64map.lower_bound(value);

        if (iter_ge_value == int64map.end()) {
            return 0;
        }

        if (comparator == GREATER_THAN && iter_ge_value->first == value) {
            iter_ge_value++;
        }

        while (iter_ge_value != int64map.end()) {
            uint32_t val_ids = ids_t::num_ids(iter_ge_value->second);
            ids_len += val_ids;
            iter_ge_value++;
        }
    } else if (comparator == LESS_THAN || comparator == LESS_THAN_EQUALS) {
        // iter entries will be >= value, or end() if all entries are before value
        auto iter_ge_value = int64map.lower_bound(value);

        auto it = int64map.begin();

        while (it != iter_ge_value) {
            uint32_t val_ids = ids_t::num_ids(it->second);
            ids_len += val_ids;
            it++;
        }

        // for LESS_THAN_EQUALS, check if last iter entry is equal to value
        if (it != int64map.end() && comparator == LESS_THAN_EQUALS && it->first == value) {
            uint32_t val_ids = ids_t::num_ids(it->second);
            ids_len += val_ids;
        }
    }

    return ids_len;
}

void num_tree_t::remove(uint64_t value, uint32_t id) {
    if(int64map.count(value) != 0) {
        void* arr = int64map[value];
        ids_t::erase(arr, id);

        if(ids_t::num_ids(arr) == 0) {
            ids_t::destroy_list(arr);
            int64map.erase(value);
        } else {
            int64map[value] = arr;
        }
    }
}

void num_tree_t::contains(const NUM_COMPARATOR& comparator, const int64_t& value,
                          const uint32_t& context_ids_length,
                          uint32_t* const& context_ids,
                          size_t& result_ids_len,
                          uint32_t*& result_ids) const {
    if (int64map.empty()) {
        return;
    }

    std::vector<uint32_t> consolidated_ids;
    consolidated_ids.reserve(context_ids_length);
    for (uint32_t i = 0; i < context_ids_length; i++) {
        if (comparator == EQUALS) {
            if (contains(value, context_ids[i])) {
                consolidated_ids.push_back(context_ids[i]);
            }
        } else if (comparator == GREATER_THAN || comparator == GREATER_THAN_EQUALS) {
            // iter entries will be >= value, or end() if all entries are before value
            auto iter_ge_value = int64map.lower_bound(value);

            if (iter_ge_value == int64map.end()) {
                continue;
            }

            if (comparator == GREATER_THAN && iter_ge_value->first == value) {
                iter_ge_value++;
            }

            while (iter_ge_value != int64map.end()) {
                if (contains(iter_ge_value->first, context_ids[i])) {
                    consolidated_ids.push_back(context_ids[i]);
                    break;
                }
                iter_ge_value++;
            }
        } else if(comparator == LESS_THAN || comparator == LESS_THAN_EQUALS) {
            // iter entries will be >= value, or end() if all entries are before value
            auto iter_ge_value = int64map.lower_bound(value);
            auto it = int64map.begin();

            while (it != iter_ge_value) {
                if (contains(it->first, context_ids[i])) {
                    consolidated_ids.push_back(context_ids[i]);
                    break;
                }
                it++;
            }

            // for LESS_THAN_EQUALS, check if last iter entry is equal to value
            if (it != int64map.end() && comparator == LESS_THAN_EQUALS && it->first == value) {
                if (contains(it->first, context_ids[i])) {
                    consolidated_ids.push_back(context_ids[i]);
                    break;
                }
            }
        }
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
    consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

    uint32_t *out = nullptr;
    result_ids_len = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                           result_ids, result_ids_len, &out);

    delete[] result_ids;
    result_ids = out;
}

void num_tree_t::seq_ids_outside_top_k(size_t k, std::vector<uint32_t> &seq_ids) {
    size_t ids_skipped = 0;

    for (auto iter = int64map.rbegin(); iter != int64map.rend(); ++iter) {
        auto num_ids = ids_t::num_ids(iter->second);
        if(ids_skipped > k) {
            ids_t::uncompress(iter->second, seq_ids);
        } else if((ids_skipped + num_ids) > k) {
            // this element hits the limit, so we pick partial IDs to satisfy k
            std::vector<uint32_t> ids;
            ids_t::uncompress(iter->second, ids);
            for(size_t i = 0; i < ids.size(); i++) {
                auto seq_id = ids[i];
                if(ids_skipped + i >= k) {
                    seq_ids.push_back(seq_id);
                }
            }
        }

        ids_skipped += num_ids;
    }
}

std::pair<int64_t, int64_t> num_tree_t::get_min_max(const uint32_t* result_ids, size_t result_ids_len) {
    int64_t min, max;
    //first traverse from top to find min
    for(auto int64map_it = int64map.begin(); int64map_it != int64map.end(); ++int64map_it) {
        if(ids_t::intersect_count(int64map_it->second, result_ids, result_ids_len)) {
            min = int64map_it->first;
            break;
        }
    }

    //traverse from end to find max
    for(auto int64map_it = int64map.rbegin(); int64map_it != int64map.rend(); ++int64map_it) {
        if(ids_t::intersect_count(int64map_it->second, result_ids, result_ids_len)) {
            max = int64map_it->first;
            break;
        }
    }

    return std::make_pair(min, max);
}

size_t num_tree_t::size() {
    return int64map.size();
}

num_tree_t::~num_tree_t() {
    for(auto& kv: int64map) {
        ids_t::destroy_list(kv.second);
    }
}

num_tree_t::iterator_t::iterator_t(num_tree_t* num_tree, NUM_COMPARATOR comparator, int64_t value) {
    if (num_tree == nullptr || num_tree->int64map.empty() || comparator != EQUALS) {
        is_valid = false;
        return;
    }

    const auto& it = num_tree->int64map.find(value);
    if (it == num_tree->int64map.end()) {
        is_valid = false;
        return;
    }

    auto obj = it->second;
    is_compact_id_list = IS_COMPACT_IDS(obj);
    if (is_compact_id_list) {
        id_list_array_len = ids_t::num_ids(obj);
        id_list_array = ids_t::uncompress(obj);
        approx_filter_ids_length = id_list_array_len;

        is_valid = id_list_array_len > index;
        if (is_valid) {
            seq_id = id_list_array[index];
        }
    } else {
        id_list = (id_list_t*)(obj);
        id_list_iterator = id_list->new_iterator();
        approx_filter_ids_length = id_list->num_ids();

        is_valid = id_list_iterator.valid();
        if (is_valid) {
            seq_id = id_list_iterator.id();
        }
    }
}

int num_tree_t::iterator_t::is_id_valid(uint32_t id) {
    if (!is_valid) {
        return -1;
    }

    skip_to(id);
    return is_valid ? (seq_id == id) : -1;
}

void num_tree_t::iterator_t::next() {
    if (!is_valid) {
        return;
    }

    if (is_compact_id_list) {
        if (++index >= id_list_array_len) {
            is_valid = false;
            return;
        }

        seq_id = id_list_array[index];
    } else {
        id_list_iterator.next();

        if (!id_list_iterator.valid()) {
            is_valid = false;
            return;
        }
        seq_id = id_list_iterator.id();
    }
}

void num_tree_t::iterator_t::skip_to(uint32_t id) {
    if (!is_valid) {
        return;
    }

    if (is_compact_id_list) {
        ArrayUtils::skip_index_to_id(index, id_list_array, id_list_array_len, id);

        if (index >= id_list_array_len) {
            is_valid = false;
            return;
        }
        seq_id = id_list_array[index];
    } else {
        id_list_iterator.skip_to(id);

        if (!id_list_iterator.valid()) {
            is_valid = false;
            return;
        }
        seq_id = id_list_iterator.id();
    }
}

void num_tree_t::iterator_t::reset() {
    if (is_compact_id_list) {
        index = 0;
        is_valid = index < id_list_array_len;
        if (is_valid) {
            seq_id = id_list_array[index];
        }
    } else {
        id_list_iterator = id_list->new_iterator();
        is_valid = id_list_iterator.valid();
        if (is_valid) {
            seq_id = id_list_iterator.id();
        }
    }
}

num_tree_t::iterator_t::~iterator_t() {
    if (is_compact_id_list) {
        delete[] id_list_array;
    }
}

num_tree_t::iterator_t& num_tree_t::iterator_t::operator=(num_tree_t::iterator_t&& obj) noexcept {
    if (&obj == this) {
        return *this;
    }

    if (is_compact_id_list) {
        delete[] id_list_array;
    }

    if (obj.is_compact_id_list) {
        is_compact_id_list = true;
        id_list_array_len = obj.id_list_array_len;
        id_list_array = obj.id_list_array;
        index = obj.index;

        obj.id_list_array = nullptr;
    } else {
        is_compact_id_list = false;
        id_list = obj.id_list;
        id_list_iterator = id_list->new_iterator();
        id_list_iterator.skip_to(obj.id_list_iterator.id());
    }

    approx_filter_ids_length = obj.approx_filter_ids_length;
    is_valid = obj.is_valid;
    seq_id = obj.seq_id;
    return *this;
}
