#pragma once

#include <cstdint>
#include <climits>
#include <cstdio>
#include <algorithm>

/*
* A bounded max heap that remembers the top-K elements seen so far
*/
template <size_t MAX_SIZE=100>
struct Topster {
    uint64_t data[MAX_SIZE];
    uint32_t smallest_index = 0;
    uint32_t size = 0;

    Topster(){
        data[smallest_index]= UINT_MAX;
    }

    template <typename T> inline void swapMe(T& a, T& b) {
        T c = a;
        a = b;
        b = c;
    }

    static inline uint64_t pack(const uint32_t&key, const uint32_t& val) {
        uint64_t kv;
        kv = key;
        kv = (kv << 32) + val;
        return kv;
    }

    static inline void unpack(const uint64_t& kv, uint32_t&key, uint32_t& val) {
        key = (uint32_t) (kv >> 32);
        val = (uint32_t) (kv & 0xFFFFFFFF);
    }

    void add(const uint32_t&key, const uint32_t& val){
        uint32_t smallest_key, smallest_value;
        unpack(data[smallest_index], smallest_key, smallest_value);

        if (size >= MAX_SIZE) {
            if(val < smallest_value) {
                // when incoming value is less than the smallest in the heap, ignore
                return;
            }

            data[smallest_index] = pack(key, val);
            int i = 0;

            // sift to maintain heap property
            while ((2*i+1) < MAX_SIZE) {
                int next = 2*i + 1;
                if (data[next] < data[next+1])
                    next++;

                if (data[i] < data[next]) swapMe(data[i], data[next]);
                else break;

                i = next;
            }
        } else {
            // keep track of the smallest element's index
            if(val < smallest_value) {
                smallest_index = size;
            }

            // insert at the end of the array, and sift it up to maintain heap property
            data[size++] = pack(key, val);
            for (int i = size - 1; i > 0;) {
                int parent = (i-1)/2;
                if (data[parent] < data[i]) {
                    swapMe(data[parent], data[i]);
                    i = parent;
                }
                else break;
            }
        }
    }

    static bool compare_values(uint64_t i, uint64_t j) {
        uint32_t ikey, ival;
        uint32_t jkey, jval;

        unpack(i, ikey, ival);
        unpack(j, jkey, jval);

        return jval < ival;
    }

    void sort() {
        std::stable_sort(std::begin(data), std::begin(data)+size, compare_values);
    }

    void clear(){
        size = 0;
    }

    uint32_t getKeyAt(uint32_t& index) {
        uint32_t key;
        uint32_t value;
        unpack(data[index], key, value);
        return key;
    }
};