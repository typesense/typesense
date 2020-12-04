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
            auto it = int64map.lower_bound(value);  // iter values will be >= value

            if(it == int64map.end()) {
                return ;
            }

            if(comparator == GREATER_THAN && it->first == value) {
                it++;
            }

            while(it != int64map.end()) {
                uint32_t *out = nullptr;
                uint32_t *val_ids = it->second->uncompress();
                ids_len = ArrayUtils::or_scalar(val_ids, it->second->getLength(),
                                                *ids, ids_len, &out);
                delete [] val_ids;
                delete [] *ids;
                *ids = out;
                it++;
            }
        } else if(comparator == LESS_THAN || comparator == LESS_THAN_EQUALS) {
            auto max_iter = int64map.lower_bound(value);  // iter values will be >= value

            if(comparator == LESS_THAN) {
                if(max_iter == int64map.end()) {
                    max_iter--;
                }

                else if(max_iter == int64map.begin() && max_iter->first != value) {
                    return ;
                }

                else if(max_iter != int64map.begin() && max_iter->first >= value) {
                    max_iter--;
                }
            } else {
                if(max_iter == int64map.end()) {
                    max_iter--;
                }
            }

            auto iter = int64map.begin();

            while(true) {
                uint32_t* out = nullptr;
                uint32_t* val_ids = iter->second->uncompress();
                ids_len = ArrayUtils::or_scalar(val_ids, iter->second->getLength(),
                                                *ids, ids_len, &out);
                delete[] val_ids;
                delete[] *ids;
                *ids = out;

                if(iter == max_iter) {
                    break;
                }

                iter++;
            }
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