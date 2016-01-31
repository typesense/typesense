#pragma once

#include <stdio.h>
#include <cstdlib>
#include <for.h>

#define FOR_GROWTH_FACTOR 1.3
#define FOR_ELE_SIZE sizeof(uint32_t)
#define FOR_LOAD_FACTOR 0.90

class forarray {
private:
    uint32_t size_bytes = 0;
    uint32_t length_bytes = 0;
    uint32_t length = 0;
    uint8_t* in;
public:
    forarray(const uint32_t n=2) {
        size_bytes = n * FOR_ELE_SIZE;
        in = new uint8_t[size_bytes];
    }

    // returns false if malloc fails
    bool append_sorted(uint32_t value) {
        if(length_bytes > size_bytes*FOR_LOAD_FACTOR) {
            // grow the array first
            size_t new_size = (size_t) (size_bytes * FOR_GROWTH_FACTOR);
            uint8_t *new_location = (uint8_t *) realloc(in, new_size);
            if(new_location == NULL) return false;
            in = new_location;
            size_bytes = (uint32_t) new_size;
        }

        uint32_t new_length_bytes = for_append_sorted(in, length, value);
        //printf("new_length_bytes: %d, size_bytes: %d\n", new_length_bytes, size_bytes);
        if(new_length_bytes == 0) return false;

        length_bytes = new_length_bytes;
        length++;
        return true;
    }

    uint32_t at(uint32_t index) {
        return for_select(in, index);
    }

    uint32_t getSizeInBytes() {
        return size_bytes;
    }

    void print_stats() {
        printf("length: %d, length_bytes: %d, size_bytes: %d\n", length, length_bytes, size_bytes);
    }
};