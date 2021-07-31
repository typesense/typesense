#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <sparsepp.h>

struct KV {
    uint8_t field_id{};
    uint8_t match_score_index{};
    uint16_t query_index{};
    uint16_t array_index{};
    uint32_t token_bits{};
    uint64_t key{};
    uint64_t distinct_key{};
    int64_t scores[3]{};  // match score + 2 custom attributes

    // to be used only in final aggregation
    uint64_t* query_indices = nullptr;

    KV(uint8_t field_id, uint16_t queryIndex, uint32_t token_bits, uint64_t key, uint64_t distinct_key,
       uint8_t match_score_index, const int64_t *scores):
            field_id(field_id), match_score_index(match_score_index),
            query_index(queryIndex), array_index(0), token_bits(token_bits), key(key),
            distinct_key(distinct_key) {
        this->scores[0] = scores[0];
        this->scores[1] = scores[1];
        this->scores[2] = scores[2];
    }

    KV() = default;

    KV(KV& kv) = default;

    KV(KV&& kv) noexcept : field_id(kv.field_id), match_score_index(kv.match_score_index),
                 query_index(kv.query_index), array_index(kv.array_index), token_bits(kv.token_bits),
                 key(kv.key), distinct_key(kv.distinct_key) {

        scores[0] = kv.scores[0];
        scores[1] = kv.scores[1];
        scores[2] = kv.scores[2];

        query_indices = kv.query_indices;
        kv.query_indices = nullptr;
    }

    KV& operator=(KV&& kv) noexcept  {
        if (this != &kv) {
            field_id = kv.field_id;
            match_score_index = kv.match_score_index;
            query_index = kv.query_index;
            array_index = kv.array_index;
            token_bits = kv.token_bits;
            key = kv.key;
            distinct_key = kv.distinct_key;

            scores[0] = kv.scores[0];
            scores[1] = kv.scores[1];
            scores[2] = kv.scores[2];

            delete[] query_indices;
            query_indices = kv.query_indices;
            kv.query_indices = nullptr;
        }

        return *this;
    }

    KV& operator=(KV& kv) noexcept  {
        if (this != &kv) {
            field_id = kv.field_id;
            match_score_index = kv.match_score_index;
            query_index = kv.query_index;
            array_index = kv.array_index;
            token_bits = kv.token_bits;
            key = kv.key;
            distinct_key = kv.distinct_key;

            scores[0] = kv.scores[0];
            scores[1] = kv.scores[1];
            scores[2] = kv.scores[2];

            delete[] query_indices;
            query_indices = kv.query_indices;
            kv.query_indices = nullptr;
        }

        return *this;
    }

    ~KV() {
        delete [] query_indices;
        query_indices = nullptr;
    }
};

/*
* Remembers the max-K elements seen so far using a min-heap
*/
struct Topster {
    const uint32_t MAX_SIZE;
    uint32_t size;

    KV *data;
    KV** kvs;

    // For distinct, stores the min heap kv of each group_kv_map topster value
    spp::sparse_hash_map<uint64_t, KV*> kv_map;

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
            data[i].match_score_index = 0;
            data[i].query_index = 0;
            data[i].array_index = i;
            data[i].token_bits = 0;
            data[i].key = 0;
            data[i].distinct_key = 0;
            kvs[i] = &data[i];
        }
    }

    ~Topster() {
        delete[] data;
        delete[] kvs;
        for(auto& kv: group_kv_map) {
            delete kv.second;
        }

        data = nullptr;
        kvs = nullptr;

        group_kv_map.clear();
    }

    static inline void swapMe(KV** a, KV** b) {
        KV *temp = *a;
        *a = *b;
        *b = temp;

        uint16_t a_index = (*a)->array_index;
        (*a)->array_index = (*b)->array_index;
        (*b)->array_index = a_index;
    }

    bool add(KV* kv) {
        /*LOG(INFO) << "kv_map size: " << kv_map.size() << " -- kvs[0]: " << kvs[0]->scores[kvs[0]->match_score_index];
        for(auto& mkv: kv_map) {
            LOG(INFO) << "kv key: " << mkv.first << " => " << mkv.second->scores[mkv.second->match_score_index];
        }*/

        bool less_than_min_heap = (size >= MAX_SIZE) && is_smaller(kv, kvs[0]);
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
                // for distinct, if a non duplicate kv is < than min heap we ignore
                return false;
            }

            if(is_duplicate_key) {
                // if min heap (group_topster.kvs[0]) changes, we have to update kvs and sift
                Topster* group_topster = found_it->second;
                KV old_min_heap_kv = *kv_map[kv->distinct_key];
                bool added = group_topster->add(kv);

                if(!added) {
                    return false;
                }

                // if new kv score is greater than previous min heap score we sift down, otherwise sift up
                SIFT_DOWN = is_greater(kv, &old_min_heap_kv);

                // new kv is different from old_min_heap_kv so we have to sift heap
                heap_op_index = old_min_heap_kv.array_index;

                // erase current min heap key from kv_map
                kv_map.erase(old_min_heap_kv.distinct_key);

            } else {
                // kv is guaranteed to be > current min heap: kvs[0]
                // create fresh topster for this distinct group key since it does not exist
                Topster* group_topster = new Topster(distinct, 0);
                group_topster->add(kv);

                // add new group key to map
                group_kv_map.emplace(kv->distinct_key, group_topster);

                // find heap operation index for updating kvs

                if(size < MAX_SIZE) {
                    // there is enough space in heap we just copy to end
                    SIFT_DOWN = false;
                    heap_op_index = size;
                    size++;
                } else {
                    SIFT_DOWN = true;

                    // max size is reached so we are forced to replace current min heap element (kvs[0])
                    heap_op_index = 0;

                    // remove current min heap group key from maps
                    delete group_kv_map[kvs[heap_op_index]->distinct_key];
                    group_kv_map.erase(kvs[heap_op_index]->distinct_key);
                    kv_map.erase(kvs[heap_op_index]->distinct_key);
                }
            }

            // kv will be copied into the pointer at heap_op_index
            kv_map.emplace(kv->distinct_key, kvs[heap_op_index]);

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

                bool smaller_than_existing = is_smaller(kv, existing_kv);
                if(smaller_than_existing) {
                    return false;
                }

                SIFT_DOWN = true;

                // replace existing kv and sift down
                heap_op_index = existing_kv->array_index;
                kv_map.erase(kvs[heap_op_index]->key);
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
            }

            // kv will be copied into the pointer at heap_op_index
            kv_map.emplace(kv->key, kvs[heap_op_index]);
        }

        // we have to replace the existing element in the heap and sift down
        kv->array_index = heap_op_index;
        *kvs[heap_op_index] = *kv;

        // sift up/down to maintain heap property

        if(SIFT_DOWN) {
            while ((2 * heap_op_index + 1) < size) {
                uint32_t next = (2 * heap_op_index + 1);  // left child
                if (next+1 < size && is_greater(kvs[next], kvs[next + 1])) {
                    // for min heap we compare with the minimum of children
                    next++;  // right child (2n + 2)
                }

                if (is_greater(kvs[heap_op_index], kvs[next])) {
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
                if (is_greater(kvs[parent], kvs[heap_op_index])) {
                    swapMe(&kvs[heap_op_index], &kvs[parent]);
                    heap_op_index = parent;
                } else {
                    break;
                }
            }
        }

        return true;
    }

    static bool is_greater(const struct KV* i, const struct KV* j) {
        return std::tie(i->scores[0], i->scores[1], i->scores[2], i->key) >
               std::tie(j->scores[0], j->scores[1], j->scores[2], j->key);
    }

    static bool is_smaller(const struct KV* i, const struct KV* j) {
        return std::tie(i->scores[0], i->scores[1], i->scores[2]) <
               std::tie(j->scores[0], j->scores[1], j->scores[2]);
    }

    static bool is_greater_kv_group(const std::vector<KV*>& i, const std::vector<KV*>& j) {
        return std::tie(i[0]->scores[0], i[0]->scores[1], i[0]->scores[2], i[0]->key) >
               std::tie(j[0]->scores[0], j[0]->scores[1], j[0]->scores[2], j[0]->key);
    }

    // topster must be sorted before iterated upon to remove dead array entries
    void sort() {
        std::stable_sort(kvs, kvs + size, is_greater);
        for(auto &group_topster: group_kv_map) {
            group_topster.second->sort();
        }
    }

    void clear(){
        size = 0;
    }

    uint64_t getKeyAt(uint32_t index) {
        return kvs[index]->key;
    }

    uint64_t getDistinctKeyAt(uint32_t index) {
        return kvs[index]->distinct_key;
    }

    KV* getKV(uint32_t index) {
        return kvs[index];
    }
};