#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <sparsepp.h>

struct KV {
    uint8_t field_id;
    uint16_t query_index;
    uint16_t array_index;
    uint64_t key;
    uint64_t distinct_key;
    uint64_t match_score;
    int64_t scores[3]{};  // match score + 2 custom attributes

    KV(uint8_t fieldId, uint16_t queryIndex, uint64_t key, uint64_t distinct_key,
       uint64_t match_score, const int64_t *scores):
            field_id(fieldId), query_index(queryIndex), array_index(0), key(key),
            distinct_key(distinct_key), match_score(match_score) {
        this->scores[0] = scores[0];
        this->scores[1] = scores[1];
        this->scores[2] = scores[2];
    }

    KV() {}
};

/*
* Remembers the max-K elements seen so far using a min-heap
*/
struct Topster {
    const uint32_t MAX_SIZE;
    uint32_t size;

    KV *data;
    KV** kvs;

    spp::sparse_hash_map<uint64_t, KV*> kv_map;

    KV* group_min_kv;
    spp::sparse_hash_map<uint64_t, Topster*> group_kv_map;
    size_t distinct;

    explicit Topster(size_t capacity): Topster(capacity, 0) {
    }

    explicit Topster(size_t capacity, size_t distinct): MAX_SIZE(capacity), size(0), distinct(distinct) {
        // we allocate data first to get a memory block whose indices are then assigned to `kvs`
        // we use separate **kvs for easier pointer swaps
        data = new KV[capacity];
        kvs = new KV*[capacity];

        for(size_t i=0; i<capacity; i++) {
            data[i].field_id = 0;
            data[i].query_index = 0;
            data[i].array_index = i;
            data[i].key = 0;
            data[i].distinct_key = 0;
            data[i].match_score = 0;
            kvs[i] = &data[i];
        }

        group_min_kv = new KV();
    }

    ~Topster() {
        delete[] data;
        delete[] kvs;
        delete group_min_kv;
        for(auto& kv: group_kv_map) {
            delete kv.second;
        }
    }

    static inline void swapMe(KV** a, KV** b) {
        KV *temp = *a;
        *a = *b;
        *b = temp;

        uint16_t a_index = (*a)->array_index;
        (*a)->array_index = (*b)->array_index;
        (*b)->array_index = a_index;
    }

    static inline void copyMe(KV* a, KV* b) {
        size_t b_index = b->array_index;
        *b = *a;
        b->array_index = b_index;
    }

    bool add(KV* kv) {
        //LOG(INFO) << "kv_map size: " << kv_map.size() << " -- kvs[0]: " << kvs[0]->match_score;
        /*for(auto kv: kv_map) {
            LOG(INFO) << "kv key: " << kv.first << " => " << kv.second->match_score;
        }*/

        /*if(kv->key == 5) {
            LOG(INFO) << "Key is 5";
        }*/

        bool less_than_min_heap = (size >= MAX_SIZE) && is_smaller_equal(kv, kvs[0]);
        size_t heap_op_index = 0;

        if(!distinct && less_than_min_heap) {
            // for non-distinct, if incoming value is smaller than min-heap ignore
            return false;
        }

        bool SIFT_DOWN = true;

        if(distinct) {
            const auto& found_it = group_kv_map.find(kv->distinct_key);
            bool is_duplicate_key = (found_it != group_kv_map.end());

            if(!is_duplicate_key && less_than_min_heap) {
                // for distinct, if a non duplicate kv is < than min heap we also ignore
                return false;
            }

            if(is_duplicate_key) {
                // if min heap (group_topster.kvs[0]) changes, we have to update kvs and sift down
                Topster* group_topster = found_it->second;
                uint16_t old_min_heap_array_index = group_min_kv->array_index;
                bool added = group_topster->add(kv);
                if(!added) {
                    return false;
                }

                // if added, guaranteed to be larger than old_min_heap_ele
                copyMe(kv, group_min_kv);
                heap_op_index = old_min_heap_array_index;
            } else {
                // create fresh topster for this distinct group key since it does not exist

                Topster* group_topster = new Topster(distinct, 0);
                group_topster->add(kv);
                copyMe(kv, group_min_kv);

                if(size < MAX_SIZE) {
                    // we just copy to end of array
                    heap_op_index = size;
                    size++;
                } else {
                    // kv is guaranteed to be > current min heap (group_topster.kvs[0])
                    // so we have to replace min heap element (kvs[0])
                    heap_op_index = 0;

                    // remove current min heap group key from map
                    delete group_kv_map[kvs[heap_op_index]->distinct_key];
                    group_kv_map.erase(kvs[heap_op_index]->distinct_key);
                }

                // add new group key to map
                group_kv_map.emplace(kv->distinct_key, group_topster);
            }

        } else { // not distinct
            //LOG(INFO) << "Searching for key: " << kv->key;

            const auto& found_it = kv_map.find(kv->key);
            bool is_duplicate_key = (found_it != kv_map.end());

            /*
               is_duplicate_key: SIFT_DOWN regardless of `size`.
               Else:
                   Do SIFT_UP if size < max_size
                   Else SIFT_DOWN
            */

            if(is_duplicate_key) {
                // Need to check if kv is greater than existing duplicate kv.
                KV* existing_kv = found_it->second;
                //LOG(INFO) << "existing_kv: " << existing_kv->key << " -> " << existing_kv->match_score;

                if(is_smaller_equal(kv, existing_kv)) {
                    return false;
                }

                SIFT_DOWN = true;

                // replace existing kv and sift down
                heap_op_index = existing_kv->array_index;
                kv_map.erase(kvs[heap_op_index]->key);

                // kv will be swapped into heap_op_index
                kv_map.emplace(kv->key, kvs[heap_op_index]);

            } else {  // not duplicate
                
                if(size < MAX_SIZE) {
                    // we just copy to end of array
                    SIFT_DOWN = false;
                    heap_op_index = size;
                    size++;
                } else {
                    // kv is guaranteed to be > min heap.
                    // we have to replace min heap element since array is full
                    SIFT_DOWN = true;
                    heap_op_index = 0;
                    kv_map.erase(kvs[heap_op_index]->key);
                }

                // kv will be swapped into heap_op_index pointer
                kv_map.emplace(kv->key, kvs[heap_op_index]);
            }
        }

        // we have to replace the existing element in the heap and sift down
        copyMe(kv, kvs[heap_op_index]);

        // sift up/down to maintain heap property

        if(SIFT_DOWN) {
            while ((2 * heap_op_index + 1) < size) {
                uint32_t next = (2 * heap_op_index + 1);  // left child
                if (next+1 < size && is_greater_kv(kvs[next], kvs[next+1])) {
                    // for min heap we compare with the minimum of children
                    next++;  // right child (2n + 2)
                }

                if (is_greater_kv(kvs[heap_op_index], kvs[next])) {
                    swapMe(&kvs[heap_op_index], &kvs[next]);
                } else {
                    break;
                }

                heap_op_index = next;
            }
        } else {
            // SIFT UP
            while(heap_op_index > 0) {
                uint32_t parent = (heap_op_index - 1) / 2;
                if (is_greater_kv(kvs[parent], kvs[heap_op_index])) {
                    swapMe(&kvs[heap_op_index], &kvs[parent]);
                    heap_op_index = parent;
                } else {
                    break;
                }
            }
        }

        return true;
    }

    static bool is_greater_kv(const struct KV* i, const struct KV* j) {
        return std::tie(i->scores[0], i->scores[1], i->scores[2], i->key) >
               std::tie(j->scores[0], j->scores[1], j->scores[2], j->key);
    }

    static bool is_smaller_equal(const struct KV* i, const struct KV* j) {
        return std::tie(i->scores[0], i->scores[1], i->scores[2]) <=
               std::tie(j->scores[0], j->scores[1], j->scores[2]);
    }

    static bool is_greater_kv_value(const struct KV & i, const struct KV & j) {
        return std::tie(i.scores[0], i.scores[1], i.scores[2], i.key) >
               std::tie(j.scores[0], j.scores[1], j.scores[2], j.key);
    }

    // topster must be sorted before iterated upon to remove dead array entries
    void sort() {
        std::stable_sort(kvs, kvs+size, is_greater_kv);
    }

    void clear(){
        size = 0;
    }

    uint64_t getKeyAt(uint32_t index) {
        return kvs[index]->key;
    }

    KV* getKV(uint32_t index) {
        return kvs[index];
    }
};