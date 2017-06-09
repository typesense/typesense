#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <sparsepp.h>
#include <match_score.h>

/*
* Remembers the max-K elements seen so far using a min-heap
*/
template <size_t MAX_SIZE=100>
struct Topster {
    struct KV {
        uint16_t start_offset;
        TokenOffsetDiffs offset_diffs;
        uint64_t key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[MAX_SIZE];

    uint32_t size;

    spp::sparse_hash_set<uint64_t> dedup_keys;

    Topster(): size(0){

    }

    template <typename T> static inline void swapMe(T& a, T& b) {
        T c = a;
        a = b;
        b = c;
    }

    void add(const uint64_t &key, const uint64_t &match_score, const int64_t &primary_attr,
             const int64_t &secondary_attr, const uint16_t &start_offset, const int16_t &offset_diffs_packed){
        if (size >= MAX_SIZE) {
            if(!is_greater(data[0], match_score, primary_attr, secondary_attr)) {
                // when incoming value is less than the smallest in the heap, ignore
                return;
            }

            if(dedup_keys.count(key) != 0) {
                // when the key already exists, ignore
                return ;
            }

            dedup_keys.erase(data[0].key);
            dedup_keys.insert(key);

            data[0].key = key;
            data[0].match_score = match_score;
            data[0].primary_attr = primary_attr;
            data[0].secondary_attr = secondary_attr;
            data[0].start_offset = start_offset;
            data[0].offset_diffs.packed = offset_diffs_packed;
            uint32_t i = 0;

            // sift to maintain heap property
            while ((2*i+1) < MAX_SIZE) {
                uint32_t next = (uint32_t) (2 * i + 1);
                if (next+1 < MAX_SIZE && is_greater_kv(data[next], data[next+1])) {
                    next++;
                }

                if (is_greater_kv(data[i], data[next])) {
                    swapMe(data[i], data[next]);
                } else {
                    break;
                }

                i = next;
            }
        } else {
            if(dedup_keys.count(key) != 0) {
                // when the key already exists, ignore
                return ;
            }

            dedup_keys.insert(key);

            data[size].key = key;
            data[size].match_score = match_score;
            data[size].primary_attr = primary_attr;
            data[size].secondary_attr = secondary_attr;
            data[size].start_offset = start_offset;
            data[size].offset_diffs.packed = offset_diffs_packed;
            size++;

            for (uint32_t i = size - 1; i > 0;) {
                uint32_t parent = (i-1)/2;
                if (is_greater_kv(data[parent], data[i])) {
                    swapMe(data[i], data[parent]);
                    i = parent;
                } else {
                    break;
                }
            }
        }
    }

    static bool is_greater(const struct KV& i, uint64_t match_score, int64_t primary_attr, int64_t secondary_attr) {
        return std::tie (match_score, primary_attr, secondary_attr) >
               std::tie (i.match_score, i.primary_attr, i.secondary_attr);
    }

    static bool is_greater_kv(const struct KV &i, const struct KV &j) {
        return std::tie(i.match_score, i.primary_attr, i.secondary_attr) >
               std::tie(j.match_score, j.primary_attr, j.secondary_attr);
    }

    void sort() {
        std::stable_sort(std::begin(data), std::begin(data) + size, is_greater_kv);
    }

    void clear(){
        size = 0;
    }

    uint64_t getKeyAt(uint32_t index) {
        return data[index].key;
    }

    KV getKV(uint32_t index) {
        return data[index];
    }
};