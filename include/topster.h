#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <sparsepp.h>
#include <match_score.h>
#include <number.h>

/*
* Remembers the max-K elements seen so far using a min-heap
*/
template <size_t MAX_SIZE=512>
struct Topster {
    struct KV {
        uint8_t field_id;
        uint16_t query_index;
        uint16_t array_index;
        uint64_t key;
        uint64_t match_score;
        number_t primary_attr;
        number_t secondary_attr;
    };

    KV *data;
    uint32_t size;

    spp::sparse_hash_map<uint64_t, KV*> keys;

    KV *kvs[MAX_SIZE];

    Topster(): size(0){
        data = new KV[MAX_SIZE];

        for(size_t i=0; i<MAX_SIZE; i++) {
            data[i].field_id = 0;
            data[i].query_index = 0;
            data[i].key = 0;
            data[i].match_score = 0;
            data[i].primary_attr = number_t();
            data[i].secondary_attr = number_t();
            kvs[i] = &data[i];
        }
    }

    ~Topster() {
        delete [] data;
    }

    static inline void swapMe(KV** a, KV** b) {
        KV *temp = *a;
        *a = *b;
        *b = temp;

        uint16_t a_index = (*a)->array_index;
        (*a)->array_index = (*b)->array_index;
        (*b)->array_index = a_index;
    }

    void add(const uint64_t &key, const uint8_t &field_id, const uint16_t &query_index, const uint64_t &match_score,
             const number_t &primary_attr, const number_t &secondary_attr) {
        if (size >= MAX_SIZE) {
            if(!is_greater(kvs[0], match_score, primary_attr, secondary_attr)) {
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

            keys.erase(kvs[start]->key);
            keys.emplace(key, kvs[start]);

            kvs[start]->key = key;
            kvs[start]->field_id = field_id;
            kvs[start]->query_index = query_index;
            kvs[start]->array_index = start;
            kvs[start]->match_score = match_score;
            kvs[start]->primary_attr = primary_attr;
            kvs[start]->secondary_attr = secondary_attr;

            // sift down to maintain heap property
            while ((2*start+1) < MAX_SIZE) {
                uint32_t next = (uint32_t) (2 * start + 1);
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

            kvs[start]->key = key;
            kvs[start]->field_id = field_id;
            kvs[start]->query_index = query_index;
            kvs[start]->array_index = start;
            kvs[start]->match_score = match_score;
            kvs[start]->primary_attr = primary_attr;
            kvs[start]->secondary_attr = secondary_attr;

            keys.emplace(key, kvs[start]);

            if(key_found) {
                // need to sift down if it's a replace
                while ((2*start+1) < size) {
                    uint32_t next = (uint32_t) (2 * start + 1);
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

    static bool is_greater(const struct KV* i, uint64_t match_score, number_t primary_attr, number_t secondary_attr) {
        return std::tie(match_score, primary_attr, secondary_attr) >
               std::tie(i->match_score, i->primary_attr, i->secondary_attr);
    }

    static bool is_greater_kv(const struct KV* i, const struct KV* j) {
        return std::tie(i->match_score, i->primary_attr, i->secondary_attr, i->key) >
               std::tie(j->match_score, j->primary_attr, j->secondary_attr, j->key);
    }

    static bool is_greater_kv_value(const struct KV & i, const struct KV & j) {
        return std::tie(i.match_score, i.primary_attr, i.secondary_attr, i.key) >
               std::tie(j.match_score, j.primary_attr, j.secondary_attr, j.key);
    }

    // topster must be sorted before iterated upon to remove dead array entries
    void sort() {
        std::stable_sort(std::begin(kvs), std::begin(kvs) + size, is_greater_kv);
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