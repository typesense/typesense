#pragma once

#include <stdio.h>
#include <cstdlib>
#include <for.h>
#include <cstring>
#include <limits>
#include <iostream>
#include "array_base.h"

class sorted_array: public array_base {
private:

    uint32_t inline sorted_append_size_required(uint32_t value, uint32_t new_length) {
        uint32_t m = std::min(min, value);
        uint32_t M = std::max(max, value);
        uint32_t bnew = required_bits(M - m);
        return METADATA_OVERHEAD + for_compressed_size_bits(new_length, bnew);
    }

public:

    // FIXME: this should be a constructor instead of a setter
    void load(const uint32_t *sorted_array, const uint32_t array_length);

    uint32_t at(uint32_t index);

    bool contains(uint32_t value);

    uint32_t indexOf(uint32_t value);

    // returns false if malloc fails
    bool append(uint32_t value);

    void remove_values(uint32_t *sorted_values, uint32_t values_length);

    size_t intersect(uint32_t* arr, const size_t arr_length, uint32_t* results);

    size_t do_union(uint32_t* arr, const size_t arr_length, uint32_t* results);
};