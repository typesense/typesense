#pragma once

#include <stdio.h>
#include <cstdlib>
#include <for.h>
#include <cstring>
#include <vector>
#include <limits>
#include <iostream>
#include "array_base.h"

class sorted_array: public array_base {
private:

    uint32_t inline sorted_append_size_required(uint32_t value, uint32_t new_length) {
        uint32_t m = std::min(min, value);
        uint32_t M = std::max(max, value);
        uint32_t bnew = required_bits(M - m);
        return METADATA_OVERHEAD + 4 + for_compressed_size_bits(new_length, bnew);
    }

    uint32_t lower_bound_search_bits(const uint8_t *in, uint32_t imin, uint32_t imax, uint32_t base,
                                     uint32_t bits, uint32_t value, uint32_t *actual);

    void binary_search_indices(const uint32_t *values, int low_vindex, int high_vindex,
                               int low_index, int high_index, uint32_t base, uint32_t bits,
                               uint32_t *indices);

public:

    void load(const uint32_t *sorted_array, const uint32_t array_length);

    uint32_t at(uint32_t index);

    bool contains(uint32_t value);

    uint32_t indexOf(uint32_t value);

    void indexOf(const uint32_t *values, const size_t values_len, uint32_t* indices);

    // returns false if malloc fails
    bool append(uint32_t value);

    void remove_values(uint32_t *sorted_values, uint32_t values_length);
};