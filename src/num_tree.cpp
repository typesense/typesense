#include "num_tree.h"
#include "parasort.h"

void num_tree_t::insert(int64_t value, uint32_t id) {
    if (int64map.count(value) == 0) {
        int64map.emplace(value, new sorted_array);
    }

    if (!int64map[value]->contains(id)) {
        int64map[value]->append(id);
    }
}

void num_tree_t::range_inclusive_search(int64_t start, int64_t end, uint32_t** ids, size_t& ids_len) {
    if(int64map.empty()) {
        return ;
    }

    auto it_start = int64map.lower_bound(start);  // iter values will be >= start

    std::vector<uint32_t> consolidated_ids;
    while(it_start != int64map.end() && it_start->first <= end) {
        uint32_t* values = it_start->second->uncompress();

        for(size_t i = 0; i < it_start->second->getLength(); i++) {
            consolidated_ids.push_back(values[i]);
        }

        delete [] values;
        it_start++;
    }

    if(consolidated_ids.size() > 50000) {
        parasort(consolidated_ids.size(), &consolidated_ids[0], 4);
    } else {
        std::sort(consolidated_ids.begin(), consolidated_ids.end());
    }

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

    uint32_t* ids = it->second->uncompress();
    for(size_t i = 0; i < it->second->getLength(); i++) {
        geo_result_ids.push_back(ids[i]);
    }

    delete [] ids;

    return it->second->getLength();
}

void num_tree_t::search(NUM_COMPARATOR comparator, int64_t value, uint32_t** ids, size_t& ids_len) {
    if(int64map.empty()) {
        return ;
    }

    if(comparator == EQUALS) {
        const auto& it = int64map.find(value);
        if(it != int64map.end()) {
            uint32_t *out = nullptr;
            uint32_t* val_ids = it->second->uncompress();
            ids_len = ArrayUtils::or_scalar(val_ids, it->second->getLength(),
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
            uint32_t* values = iter_ge_value->second->uncompress();

            for(size_t i = 0; i < iter_ge_value->second->getLength(); i++) {
                consolidated_ids.push_back(values[i]);
            }

            delete [] values;
            iter_ge_value++;
        }

        if(consolidated_ids.size() > 50000) {
            parasort(consolidated_ids.size(), &consolidated_ids[0], 4);
        } else {
            std::sort(consolidated_ids.begin(), consolidated_ids.end());
        }

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
            uint32_t* values = it->second->uncompress();

            for(size_t i = 0; i < it->second->getLength(); i++) {
                consolidated_ids.push_back(values[i]);
            }

            delete [] values;
            it++;
        }

        // for LESS_THAN_EQUALS, check if last iter entry is equal to value
        if(it != int64map.end() && comparator == LESS_THAN_EQUALS && it->first == value) {
            uint32_t* values = it->second->uncompress();
            for(size_t i = 0; i < it->second->getLength(); i++) {
                consolidated_ids.push_back(values[i]);
            }
            delete [] values;
        }

        if(consolidated_ids.size() > 50000) {
            parasort(consolidated_ids.size(), &consolidated_ids[0], 4);
        } else {
            std::sort(consolidated_ids.begin(), consolidated_ids.end());
        }

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
        sorted_array* arr = int64map[value];
        arr->remove_value(id);
        if(arr->getLength() == 0) {
            delete arr;
            int64map.erase(value);
        }
    }
}

size_t num_tree_t::size() {
    return int64map.size();
}