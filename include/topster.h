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
    uint64_t data[MAX_SIZE];
    uint32_t size;

    Topster(): size(0){

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
        if (size >= MAX_SIZE) {
            if(val <= getValueAt(0)) {
                // when incoming value is less than the smallest in the heap, ignore
                return;
            }

            data[0] = pack(key, val);
            uint32_t i = 0;

            // sift to maintain heap property
            while ((2*i+1) < MAX_SIZE) {
                uint32_t next = (uint32_t) (2 * i + 1);
                if (next+1 < MAX_SIZE && getValueAt(next) > getValueAt(next+1)) {
                    next++;
                }

                if (getValueAt(i) > getValueAt(next)) {
                    swapMe(data[i], data[next]);
                } else {
                    break;
                }

                i = next;
            }
        } else {
            data[size++] = pack(key, val);
            for (uint32_t i = size - 1; i > 0;) {
                uint32_t parent = (i-1)/2;
                if (getValueAt(parent) > getValueAt(i)) {
                    swapMe(data[parent], data[i]);
                    i = parent;
                } else {
                    break;
                }
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

    uint32_t getKeyAt(uint32_t index) {
        uint32_t key;
        uint32_t value;
        unpack(data[index], key, value);
        return key;
    }

    uint32_t getValueAt(uint32_t index) {
        uint32_t key;
        uint32_t value;
        unpack(data[index], key, value);
        return value;
    }
};