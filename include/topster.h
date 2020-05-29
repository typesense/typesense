#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <sparsepp.h>
#include <match_score.h>
#include <number.h>

struct KV {
    uint8_t field_id;
    uint16_t query_index;
    uint16_t array_index;
    uint64_t key;
    uint64_t match_score;
    int64_t scores[3];  // match score + 2 custom attributes
};

/*
* Remembers the max-K elements seen so far using a min-heap
*/
struct Topster {
    const uint32_t MAX_SIZE;
    uint32_t size;

    KV *data;
    KV* *kvs;

    spp::sparse_hash_map<uint64_t, KV*> keys;

    explicit Topster(size_t capacity): MAX_SIZE(capacity), size(0) {
        // we allocate data first to get contiguous memory block whose indices are then assigned to `kvs`
        // we use separate **kvs for easier pointer swaps
        data = new KV[capacity];
        kvs = new KV*[capacity];

        for(size_t i=0; i<capacity; i++) {
            data[i].field_id = 0;
            data[i].query_index = 0;
            data[i].key = 0;
            data[i].match_score = 0;
            kvs[i] = &data[i];
        }
    }

    ~Topster() {
        delete [] data;
        delete [] kvs;
    }

    static inline void swapMe(KV** a, KV** b) {
        KV *temp = *a;
        *a = *b;
        *b = temp;

        uint16_t a_index = (*a)->array_index;
        (*a)->array_index = (*b)->array_index;
        (*b)->array_index = a_index;
    }

    static inline void replace_key_values(const uint64_t &key, const uint8_t &field_id, const uint16_t &query_index,
                                          const uint64_t &match_score, const int64_t *scores, uint32_t start,
                                          KV* *kvs, spp::sparse_hash_map<uint64_t, KV*>& keys) {
        kvs[start]->key = key;
        kvs[start]->field_id = field_id;
        kvs[start]->query_index = query_index;
        kvs[start]->array_index = start;
        kvs[start]->match_score = match_score;
        kvs[start]->scores[0] = scores[0];
        kvs[start]->scores[1] = scores[1];
        kvs[start]->scores[2] = scores[2];

        keys.erase(kvs[start]->key);
        keys[key] = kvs[start];
    }

    void add(const uint64_t &key, const uint8_t &field_id, const uint16_t &query_index, const uint64_t &match_score,
             const int64_t scores[3]) {
        if (size >= MAX_SIZE) {
            if(!is_greater(kvs[0], scores)) {
                // when incoming value is less than the smallest in the heap, ignore
                return;
            }

            uint32_t start = 0;

            // When the key already exists and has a greater score, ignore. Otherwise, we have to replace.
            // NOTE: we don't consider primary and secondary attrs here because they will be the same for a given key.
            if(keys.count(key) != 0) {
                const KV* existing = keys.at(key);
                if(match_score <= existing->match_score) {
                    return ;
                }

                // replace and sift down
                start = existing->array_index;
            }

            replace_key_values(key, field_id, query_index, match_score, scores, start, kvs, keys);

            // sift down to maintain heap property
            while ((2*start+1) < MAX_SIZE) {
                uint32_t next = (2 * start + 1);
                if (next+1 < MAX_SIZE && is_greater_kv(kvs[next], kvs[next+1])) {
                    next++;
                }

                if (is_greater_kv(kvs[start], kvs[next])) {
                    swapMe(&kvs[start], &kvs[next]);
                } else {
                    break;
                }

                start = next;
            }
        } else {
            uint32_t start = size;
            bool key_found = false;

            // When the key already exists and has a greater score, ignore. Otherwise, we have to replace
            if(keys.count(key) != 0) {
                const KV* existing = keys.at(key);
                if(match_score <= existing->match_score) {
                    return ;
                }

                // replace and sift down
                start = existing->array_index;
                key_found = true;
            }

            replace_key_values(key, field_id, query_index, match_score, scores, start, kvs, keys);

            if(key_found) {
                // need to sift down if it's a replace
                while ((2*start+1) < size) {
                    uint32_t next = (2 * start + 1);
                    if (next+1 < size && is_greater_kv(kvs[next], kvs[next+1])) {
                        next++;
                    }

                    if (is_greater_kv(kvs[start], kvs[next])) {
                        swapMe(&kvs[start], &kvs[next]);
                    } else {
                        break;
                    }

                    start = next;
                }

                return ;
            }

            while(start > 0) {
                uint32_t parent = (start-1)/2;
                if (is_greater_kv(kvs[parent], kvs[start])) {
                    swapMe(&kvs[start], &kvs[parent]);
                    start = parent;
                } else {
                    break;
                }
            }

            if(keys.count(key) != 0) {
                size++;
            }
        }
    }

    static bool is_greater(const struct KV* i, const int64_t scores[3]) {
        return std::tie(scores[0], scores[1], scores[2]) >
               std::tie(i->scores[0], i->scores[1], i->scores[2]);
    }

    static bool is_greater_kv(const struct KV* i, const struct KV* j) {
        return std::tie(i->scores[0], i->scores[1], i->scores[2], i->key) >
               std::tie(j->scores[0], j->scores[1], j->scores[2], j->key);
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