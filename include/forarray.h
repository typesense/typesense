#pragma once

#include <stdio.h>
#include <cstdlib>
#include <for.h>
#include <limits>

#define FOR_GROWTH_FACTOR 1.3
#define FOR_ELE_SIZE sizeof(uint32_t)
#define METADATA_OVERHEAD 5

class forarray {
private:
    uint8_t* in;
    uint32_t size_bytes = 0;
    uint32_t length_bytes = 0;
    uint32_t length = 0;
    uint32_t min = std::numeric_limits<uint32_t>::max();
    uint32_t max = std::numeric_limits<uint32_t>::min();
public:
    forarray(const uint32_t n=2) {
        size_bytes = METADATA_OVERHEAD + (n * FOR_ELE_SIZE);
        in = new uint8_t[size_bytes];
        memset(in, 0, size_bytes);
    }

    static inline uint32_t required_bits(const uint32_t v) {
        return v == 0 ? 0 : 32 - __builtin_clz(v);
    }

    uint32_t inline sorted_append_size_required(uint32_t value) {
        uint32_t m = std::min(min, value);
        uint32_t M = std::max(max, value);
        uint32_t bnew = required_bits(M - m);
        return METADATA_OVERHEAD + for_compressed_size_bits(length+1, bnew);
    }

    // returns false if malloc fails
    bool append_sorted(uint32_t value) {
        uint32_t size_required = sorted_append_size_required(value);

        if(size_required+4 > size_bytes) {
            // grow the array first
            size_t new_size = (size_t) (size_required * FOR_GROWTH_FACTOR);
            uint8_t *new_location = (uint8_t *) realloc(in, new_size);
            if(new_location == NULL) {
                abort();
            }
            in = new_location;
            size_bytes = (uint32_t) new_size;
        }

        uint32_t new_length_bytes = for_append_sorted(in, length, value);
        if(new_length_bytes == 0) return false;

        length_bytes = new_length_bytes;
        length++;

        if(value < min) min = value;
        if(value > max) max = value;

        return true;
    }

    uint32_t inline unsorted_append_size_required(uint32_t value) {
      uint32_t m = std::min(min, value);
      uint32_t M = std::max(max, value);
      uint32_t bnew = required_bits(M - m);
      return METADATA_OVERHEAD + for_compressed_size_bits(length+1, bnew);
    }

    bool append_unsorted(uint32_t value) {
      uint32_t size_required = unsorted_append_size_required(value);

      if(size_required+4 > size_bytes) {
          // grow the array first
          size_t new_size = (size_t) (size_required * FOR_GROWTH_FACTOR);
          uint8_t *new_location = (uint8_t *) realloc(in, new_size);
          if(new_location == NULL) {
              abort();
          }
          in = new_location;
          size_bytes = (uint32_t) new_size;
      }

      uint32_t new_length_bytes = for_append_unsorted(in, length, value);
      if(new_length_bytes == 0) {
          abort();
      }

      if(value < min) min = value;
      if(value > max) max = value;

      length_bytes = new_length_bytes;
      length++;

      return true;
    }

    uint32_t at(uint32_t index) {
        return for_select(in, index);
    }

    bool contains(uint32_t value) {
        uint32_t actual;
        for_lower_bound_search(in, length, value, &actual);
        return actual == value;
    }

    uint32_t indexOf(uint32_t value) {
      uint32_t actual;
      uint32_t index = for_lower_bound_search(in, length, value, &actual);
      if(actual == value) return index;
      return length;
    }

    uint32_t* uncompress() {
        uint32_t *out = new uint32_t[length];
        for_uncompress(in, out, length);
        return out;
    }

    uint32_t getSizeInBytes() {
        return size_bytes;
    }

    uint32_t getLength() {
        return length;
    }

    void print_stats() {
        printf("length: %d, length_bytes: %d, size_bytes: %d\n", length, length_bytes, size_bytes);
    }
};