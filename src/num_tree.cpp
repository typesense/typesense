#include "num_tree.h"
#include "parasort.h"
#include "timsort.hpp"

void num_tree_t::insert(int64_t value, uint32_t id) {
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

size_t num_tree_t::size() {
    return int64map.size();
}

num_tree_t::~num_tree_t() {
    for(auto& kv: int64map) {
        ids_t::destroy_list(kv.second);
    }
}
