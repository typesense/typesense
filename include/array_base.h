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

class array_base {
protected:
    uint8_t* in = nullptr;
    uint32_t size_bytes = 0;    // allocated size
    uint32_t length_bytes = 0;  // actual size
    uint32_t length = 0;
    uint32_t min = std::numeric_limits<uint32_t>::max();
    uint32_t max = std::numeric_limits<uint32_t>::min();

    static inline uint32_t required_bits(const uint32_t v) {
        return (uint32_t) (v == 0 ? 0 : 32 - __builtin_clz(v));
    }

public:
    explicit array_base(const uint32_t n=2) {
        size_bytes = METADATA_OVERHEAD + (n * FOR_ELE_SIZE);
        in = (uint8_t *) malloc(size_bytes * sizeof *in);
        memset(in, 0, size_bytes);
    }

    ~array_base() {
        free(in);
        in = nullptr;
    }

    // len determines length of output buffer (default: length of input)
    uint32_t* uncompress(uint32_t len=0) const;

    uint32_t getSizeInBytes();

    uint32_t getLength() const;

    uint32_t getMin() const;

    uint32_t getMax() const;
};