#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <unordered_map>
#include <field.h>
#include "filter_result_iterator.h"

struct KV {
    int8_t match_score_index{};
    uint16_t query_index{};
    uint16_t array_index{};
    uint64_t key{};
    int64_t scores[3]{};  // match score + 2 custom attributes

    // only to be used in hybrid search
    float vector_distance = -1.0f;
    int64_t text_match_score = 0;

    // to be used only in final aggregation
    uint64_t* query_indices = nullptr;

    std::map<std::string, reference_filter_result_t> reference_filter_results;

    KV(uint16_t queryIndex, uint64_t key, int8_t match_score_index, const int64_t *scores,
       std::map<std::string, reference_filter_result_t>  reference_filter_results = {}):
            match_score_index(match_score_index), query_index(queryIndex), array_index(0), key(key),
            reference_filter_results(std::move(reference_filter_results)) {
        this->scores[0] = scores[0];
        this->scores[1] = scores[1];
        this->scores[2] = scores[2];

        if(match_score_index >= 0){
            this->text_match_score = scores[match_score_index];
        }
    }

    KV() = default;

    KV(KV& kv) = default;

    KV(KV&& kv) noexcept : match_score_index(kv.match_score_index),
                           query_index(kv.query_index), array_index(kv.array_index),
                           key(kv.key) {

        scores[0] = kv.scores[0];
        scores[1] = kv.scores[1];
        scores[2] = kv.scores[2];

        query_indices = kv.query_indices;
        kv.query_indices = nullptr;

        vector_distance = kv.vector_distance;
        text_match_score = kv.text_match_score;

        reference_filter_results = std::move(kv.reference_filter_results);
    }

    KV& operator=(KV&& kv) noexcept  {
        if (this != &kv) {
            match_score_index = kv.match_score_index;
            query_index = kv.query_index;
            array_index = kv.array_index;
            key = kv.key;

            scores[0] = kv.scores[0];
            scores[1] = kv.scores[1];
            scores[2] = kv.scores[2];

            delete[] query_indices;
            query_indices = kv.query_indices;
            kv.query_indices = nullptr;

            vector_distance = kv.vector_distance;
            text_match_score = kv.text_match_score;

            reference_filter_results = std::move(kv.reference_filter_results);
        }

        return *this;
    }

    KV& operator=(KV& kv) noexcept  {
        if (this != &kv) {
            match_score_index = kv.match_score_index;
            query_index = kv.query_index;
            array_index = kv.array_index;
            key = kv.key;

            scores[0] = kv.scores[0];
            scores[1] = kv.scores[1];
            scores[2] = kv.scores[2];

            delete[] query_indices;
            query_indices = kv.query_indices;
            kv.query_indices = nullptr;

            vector_distance = kv.vector_distance;
            text_match_score = kv.text_match_score;

            reference_filter_results.clear();

            for (const auto& item: kv.reference_filter_results) {
                reference_filter_results[item.first] = item.second;
            }
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

    std::unordered_map<uint64_t, KV*> kv_map;

    explicit Topster(size_t capacity): MAX_SIZE(capacity), size(0) {
        // we allocate data first to get a memory block whose indices are then assigned to `kvs`
        // we use separate **kvs for easier pointer swaps
        data = new KV[capacity];
        kvs = new KV*[capacity];

        for(size_t i=0; i<capacity; i++) {
            data[i].match_score_index = 0;
            data[i].query_index = 0;
            data[i].array_index = i;
            data[i].key = 0;
            kvs[i] = &data[i];
        }
    }

    ~Topster() {
        delete[] data;
        delete[] kvs;
        data = nullptr;
        kvs = nullptr;
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

        if(less_than_min_heap) {
            // if incoming value is smaller than min-heap ignore
            return false;
        }

        bool SIFT_DOWN = true;

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
        return std::tie(i->scores[0], i->scores[1], i->scores[2], i->key) <
               std::tie(j->scores[0], j->scores[1], j->scores[2], j->key);
    }

    static bool is_greater_kv_group(const std::vector<KV*>& i, const std::vector<KV*>& j) {
        return std::tie(i[0]->scores[0], i[0]->scores[1], i[0]->scores[2], i[0]->key) >
               std::tie(j[0]->scores[0], j[0]->scores[1], j[0]->scores[2], j[0]->key);
    }

    // topster must be sorted before iterated upon to remove dead array entries
    void sort() {
        std::stable_sort(kvs, kvs + size, is_greater);
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

    bool key_exists(uint64_t distinct_key) const {
        return kv_map.find(distinct_key) != kv_map.end();
    }
};