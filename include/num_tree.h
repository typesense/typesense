#pragma once

#include <map>
#include "sparsepp.h"
#include "sorted_array.h"
#include "array_utils.h"

class num_tree_t {
private:
    std::map<int64_t, sorted_array*> int64map;

public:

    ~num_tree_t() {
        for(auto& kv: int64map) {
            delete kv.second;
        }
    }

    void insert(int64_t value, uint32_t id) {
        if(int64map.count(value) == 0) {
            int64map.emplace(value, new sorted_array);
        }

        int64map[value]->append(id);
    }

    void range_inclusive_search(int64_t start, int64_t end, uint32_t** ids, size_t& ids_len) {
        if(int64map.empty()) {
            return ;
        }

        auto it_start = int64map.lower_bound(start);  // iter values will be >= start

        std::vector<uint32_t> consolidated_ids;
        while(it_start != int64map.end() && it_start->first <= end) {
            for(size_t i = 0; i < it_start->second->getLength(); i++) {
                consolidated_ids.push_back(it_start->second->at(i));
            }

            it_start++;
        }

        std::sort(consolidated_ids.begin(), consolidated_ids.end());

        uint32_t *out = nullptr;
        ids_len = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                        *ids, ids_len, &out);

        delete [] *ids;
        *ids = out;
    }

    void search(NUM_COMPARATOR comparator, int64_t value, uint32_t** ids, size_t& ids_len) {
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
                for(size_t i = 0; i < iter_ge_value->second->getLength(); i++) {
                    consolidated_ids.push_back(iter_ge_value->second->at(i));
                }
                iter_ge_value++;
            }

            std::sort(consolidated_ids.begin(), consolidated_ids.end());

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
                for(size_t i = 0; i < it->second->getLength(); i++) {
                    consolidated_ids.push_back(it->second->at(i));
                }
                it++;
            }

            // for LESS_THAN_EQUALS, check if last iter entry is equal to value
            if(it != int64map.end() && comparator == LESS_THAN_EQUALS && it->first == value) {
                for(size_t i = 0; i < it->second->getLength(); i++) {
                    consolidated_ids.push_back(it->second->at(i));
                }
            }

            std::sort(consolidated_ids.begin(), consolidated_ids.end());

            uint32_t *out = nullptr;
            ids_len = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                            *ids, ids_len, &out);

            delete [] *ids;
            *ids = out;
        }
    }

    void remove(uint64_t value, uint32_t id) {
        if(int64map.count(value) != 0) {
            sorted_array* arr = int64map[value];
            arr->remove_value(id);
            if(arr->getLength() == 0) {
                delete arr;
                int64map.erase(value);
            }
        }
    }

    size_t size() {
        return int64map.size();
    }

};