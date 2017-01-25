#pragma once

#include <stdio.h>
#include <cstdlib>
#include <for.h>
#include <cstring>
#include <limits>
#include <iostream>

#define FOR_GROWTH_FACTOR 1.3
#define FOR_ELE_SIZE sizeof(uint32_t)
#define METADATA_OVERHEAD 5

class forarray {
private:
    uint8_t* in;
    uint32_t size_bytes = 0;    // allocated size
    uint32_t length_bytes = 0;  // actual size
    uint32_t length = 0;
    uint32_t min = std::numeric_limits<uint32_t>::max();
    uint32_t max = std::numeric_limits<uint32_t>::min();

    static inline uint32_t required_bits(const uint32_t v) {
        return (uint32_t) (v == 0 ? 0 : 32 - __builtin_clz(v));
    }

    uint32_t inline sorted_append_size_required(uint32_t value, uint32_t new_length) {
        uint32_t m = std::min(min, value);
        uint32_t M = std::max(max, value);
        uint32_t bnew = required_bits(M - m);
        return METADATA_OVERHEAD + for_compressed_size_bits(new_length, bnew);
    }

    uint32_t inline unsorted_append_size_required(uint32_t value, uint32_t new_length) {
        uint32_t m = std::min(min, value);
        uint32_t M = std::max(max, value);
        uint32_t bnew = required_bits(M - m);
        return METADATA_OVERHEAD + for_compressed_size_bits(new_length, bnew);
    }

public:
    forarray(const uint32_t n=2) {
        size_bytes = METADATA_OVERHEAD + (n * FOR_ELE_SIZE);
        in = new uint8_t[size_bytes];
        memset(in, 0, size_bytes);
    }

    ~forarray() {
        delete[] in;
        in = nullptr;
    }

    // FIXME: this should be a constructor instead of a setter
    void load_sorted(const uint32_t *sorted_array, const uint32_t array_length);

    // returns false if malloc fails
    bool append_sorted(uint32_t value);

    bool append_unsorted(uint32_t value);

    uint32_t at(uint32_t index);

    // FIXME: contains and indexOf are meant only for sorted arrays
    bool contains(uint32_t value);

    uint32_t indexOf(uint32_t value);

    uint32_t* uncompress();

    void remove_index_unsorted(uint32_t start_index, uint32_t end_index);

    void remove_values_sorted(uint32_t *sorted_values, uint32_t values_length);

    uint32_t getSizeInBytes();

    uint32_t getLength();
};