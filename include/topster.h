#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>

/*
* Remembers the max-K elements seen so far using a min-heap
*/
template <size_t MAX_SIZE=100>
struct Topster {
    struct KV {
        uint64_t key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[MAX_SIZE];

    uint32_t size;

    Topster(): size(0){

    }

    template <typename T> static inline void swapMe(T& a, T& b) {
        T c = a;
        a = b;
        b = c;
    }

    void add(const uint64_t &key, const uint64_t &match_score, const int64_t &primary_attr, const int64_t &secondary_attr){
        if (size >= MAX_SIZE) {
            if(match_score <= data[0].match_score && primary_attr <= data[0].primary_attr &&
               secondary_attr <= data[0].secondary_attr) {
                // when incoming value is less than the smallest in the heap, ignore
                return;
            }

            data[0].key = key;
            data[0].match_score = match_score;
            data[0].primary_attr = primary_attr;
            data[0].secondary_attr = secondary_attr;
            uint32_t i = 0;

            // sift to maintain heap property
            while ((2*i+1) < MAX_SIZE) {
                uint32_t next = (uint32_t) (2 * i + 1);
                if (next+1 < MAX_SIZE && data[next].match_score > data[next+1].match_score &&
                    data[next].primary_attr > data[next+1].primary_attr &&
                    data[next].secondary_attr > data[next+1].secondary_attr) {
                    next++;
                }

                if (data[i].match_score > data[next].match_score && data[i].primary_attr > data[next].primary_attr &&
                    data[i].secondary_attr > data[next].secondary_attr) {
                    swapMe(data[i], data[next]);
                } else {
                    break;
                }

                i = next;
            }
        } else {
            data[size].key = key;
            data[size].match_score = match_score;
            data[size].primary_attr = primary_attr;
            data[size].secondary_attr = secondary_attr;
            size++;

            for (uint32_t i = size - 1; i > 0;) {
                uint32_t parent = (i-1)/2;
                if (data[parent].match_score > data[i].match_score && data[parent].primary_attr > data[i].primary_attr &&
                    data[parent].secondary_attr > data[i].secondary_attr) {
                    swapMe(data[i], data[parent]);
                    i = parent;
                } else {
                    break;
                }
            }
        }
    }

    static bool compare_values(const struct KV& i, const struct KV& j) {
        if(i.match_score != j.match_score) return i.match_score > j.match_score;
        if(i.primary_attr != j.primary_attr) return i.primary_attr > j.primary_attr;
        if(i.secondary_attr != j.secondary_attr) return i.secondary_attr > j.secondary_attr;
        return i.key > j.key;
    }

    void sort() {
        std::stable_sort(std::begin(data), std::begin(data)+size, compare_values);
    }

    void clear(){
        size = 0;
    }

    uint64_t getKeyAt(uint32_t index) {
        return data[index].key;
    }
};