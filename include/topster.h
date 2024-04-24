#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <unordered_map>
#include <field.h>
#include "filter_result_iterator.h"
#include "hll/distinct_counter.h"

#define HYPERLOGLOG_THRESHOLD 2048

struct KV {
    int8_t match_score_index{};
    uint16_t query_index{};
    uint16_t array_index{};
    uint64_t key{};
    uint64_t distinct_key{};
    int64_t scores[3]{};  // match score + 2 custom attributes

    // only to be used in hybrid search
    float vector_distance = -1.0f;
    int64_t text_match_score = 0;

    // to be used only in final aggregation
    uint64_t* query_indices = nullptr;

    std::map<std::string, reference_filter_result_t> reference_filter_results;

    KV(uint16_t queryIndex, uint64_t key, uint64_t distinct_key, int8_t match_score_index, const int64_t *scores,
       std::map<std::string, reference_filter_result_t>  reference_filter_results = {}):
            match_score_index(match_score_index), query_index(queryIndex), array_index(0), key(key),
            distinct_key(distinct_key), reference_filter_results(std::move(reference_filter_results)) {
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
                 key(kv.key), distinct_key(kv.distinct_key) {
                    
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
            distinct_key = kv.distinct_key;

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
            distinct_key = kv.distinct_key;

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

    spp::sparse_hash_set<uint64_t> group_doc_seq_ids;

    spp::sparse_hash_map<uint64_t, Topster*> group_kv_map;
    size_t distinct;

    //hyperloglog lib counter for counting total unique group values more than 512
    hyperloglog_hip::distinct_counter<uint64_t> hyperloglog_counter;
    std::set<uint64_t> groups_found;    //to keep count of total unique group less than 512
    uint32_t groups_found_count = 0;    //to keep track of groups found in current pass
    bool is_first_pass_completed = false;

    explicit Topster(size_t capacity): Topster(capacity, 0) {
    }

    explicit Topster(size_t capacity, size_t distinct, bool is_first_pass_completed = false): MAX_SIZE(capacity),
                    size(0), distinct(distinct), is_first_pass_completed(is_first_pass_completed) {
        // we allocate data first to get a memory block whose indices are then assigned to `kvs`
        // we use separate **kvs for easier pointer swaps
        data = new KV[capacity];
        kvs = new KV*[capacity];

        for(size_t i=0; i<capacity; i++) {
            data[i].match_score_index = 0;
            data[i].query_index = 0;
            data[i].array_index = i;
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

    int add(KV* kv) {
        /*LOG(INFO) << "kv_map size: " << kv_map.size() << " -- kvs[0]: " << kvs[0]->scores[kvs[0]->match_score_index];
        for(auto& mkv: kv_map) {
            LOG(INFO) << "kv key: " << mkv.first << " => " << mkv.second->scores[mkv.second->match_score_index];
        }*/

       /* returns either 0 or 1
        * 1 -> distinct_id was added to group_kv_map in second pass, which will aggregate found counts to groups_processed
        * 0 -> distinct_id was added to kv_map in first pass
        * -1 -> distinct_id was not added
        */

        bool less_than_min_heap = (size >= MAX_SIZE) && is_smaller(kv, kvs[0]);
        size_t heap_op_index = 0;

        if(!distinct && less_than_min_heap) {
            // for non-distinct, if incoming value is smaller than min-heap ignore
            return -1;
        }

        bool SIFT_DOWN = true;

        if(distinct && is_first_pass_completed) {
            if(kv_map.count(kv->distinct_key) == 0) {
                return -1;
            }

            const auto& doc_seq_id_exists =
                (group_doc_seq_ids.find(kv->key) != group_doc_seq_ids.end());
        
            if(doc_seq_id_exists) {
                return -1;
            }
            group_doc_seq_ids.emplace(kv->key);
            
            // Grouping cannot be a streaming operation, so aggregate the KVs associated with every group.
            auto kvs_it = group_kv_map.find(kv->distinct_key);
            if(kvs_it != group_kv_map.end()) {
                kvs_it->second->add(kv);
            } else {
                Topster* g_topster = new Topster(distinct, 0);
                g_topster->add(kv);
                group_kv_map.insert({kv->distinct_key, g_topster});
                groups_found_count++;
            }
            
            return 1;

        } else { // not distinct
            //LOG(INFO) << "Searching for key: " << kv->key;
            auto key = distinct ? kv->distinct_key : kv->key;

            const auto& found_it = kv_map.find(key);
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
                    return -1;
                }

                SIFT_DOWN = true;

                // replace existing kv and sift down
                heap_op_index = existing_kv->array_index;
                auto heap_op_index_key = distinct ? kvs[heap_op_index]->distinct_key : kvs[heap_op_index]->key;
                kv_map.erase(heap_op_index_key);
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
                    auto heap_op_index_key = distinct ? kvs[heap_op_index]->distinct_key : kvs[heap_op_index]->key;
                    kv_map.erase(heap_op_index_key);
                }
            }

            // kv will be copied into the pointer at heap_op_index
            kv_map.emplace(key, kvs[heap_op_index]);

            if(distinct) {
                hyperloglog_counter.insert(kv->distinct_key);

                if(groups_found.size() < HYPERLOGLOG_THRESHOLD) {
                    groups_found.insert(kv->distinct_key);
                    groups_found_count = groups_found.size();
                } else {
                    groups_found_count = hyperloglog_counter.count();
                }
            }
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

        return 0;
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
        if(!distinct) {
            std::stable_sort(kvs, kvs + size, is_greater);
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

    const size_t get_total_unique_groups() const {
        auto groups_count = groups_found.size();
        return groups_count < 512 ? groups_count : hyperloglog_counter.count();
    }

    void set_first_pass_complete() {
        is_first_pass_completed = true;
        groups_found_count = 0;
    }

    const size_t get_current_groups_count() const {
        return groups_found_count;
    }
};