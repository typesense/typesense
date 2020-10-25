#pragma once

#include <stdio.h>
#include <cstdlib>
#include <for.h>
#include <cstring>
#include <limits>
#include <iostream>
#include "array_base.h"

class array: public array_base {
private:
    uint32_t inline unsorted_append_size_required(uint32_t value, uint32_t new_length) {
        uint32_t m = std::min(min, value);
        uint32_t M = std::max(max, value);
        uint32_t bnew = required_bits(M - m);
        return METADATA_OVERHEAD + 4 + for_compressed_size_bits(new_length, bnew);
    }

public:
    void load(const uint32_t *sorted_array, uint32_t array_length, uint32_t m, uint32_t M);

    uint32_t at(uint32_t index);

    bool contains(uint32_t value);

    uint32_t indexOf(uint32_t value);

    bool append(uint32_t value);

    bool insert(size_t index, const uint32_t* values, size_t num_values);

    void remove_index(uint32_t start_index, uint32_t end_index);
};