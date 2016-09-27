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
        uint64_t value;
    } data[MAX_SIZE];

    uint32_t size;

    Topster(): size(0){

    }

    template <typename T> static inline void swapMe(T& a, T& b) {
        T c = a;
        a = b;
        b = c;
    }

    void add(const uint64_t &key, const uint64_t &value){
        if (size >= MAX_SIZE) {
            if(value <= data[0].value) {
                // when incoming value is less than the smallest in the heap, ignore
                return;
            }

            data[0].key = key;
            data[0].value = value;
            uint32_t i = 0;

            // sift to maintain heap property
            while ((2*i+1) < MAX_SIZE) {
                uint32_t next = (uint32_t) (2 * i + 1);
                if (next+1 < MAX_SIZE && data[next].value > data[next+1].value) {
                    next++;
                }

                if (data[i].value > data[next].value) {
                    swapMe(data[i], data[next]);
                } else {
                    break;
                }

                i = next;
            }
        } else {
            data[size].key = key;
            data[size].value = value;
            size++;

            for (uint32_t i = size - 1; i > 0;) {
                uint32_t parent = (i-1)/2;
                if (data[parent].value > data[i].value) {
                    swapMe(data[i], data[parent]);
                    i = parent;
                } else {
                    break;
                }
            }
        }
    }

    static bool compare_values(const struct KV& i, const struct KV& j) {
        return j.value < i.value;
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