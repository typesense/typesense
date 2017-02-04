#include "array.h"

uint32_t array::at(uint32_t index) {
    return for_select(in, index);
}

bool array::contains(uint32_t value) {
    uint32_t index = for_linear_search(in, length, value);
    return index != length;
}

uint32_t array::indexOf(uint32_t value) {
    return for_linear_search(in, length, value);
}

bool array::append(uint32_t value) {
    uint32_t size_required = unsorted_append_size_required(value, length+1);

    if(size_required+FOR_ELE_SIZE > size_bytes) {
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

void array::remove_index(uint32_t start_index, uint32_t end_index) {
    uint32_t *curr_array = uncompress();

    uint32_t *new_array = new uint32_t[length];
    uint32_t new_index = 0;
    uint32_t curr_index = 0;

    min = std::numeric_limits<uint32_t>::max();
    max = std::numeric_limits<uint32_t>::min();

    while(curr_index < length) {
        if(curr_index < start_index || curr_index >= end_index) {
            new_array[new_index++] = curr_array[curr_index];
            if(curr_array[curr_index] < min) min = curr_array[curr_index];
            if(curr_array[curr_index] > max) max = curr_array[curr_index];
        }
        curr_index++;
    }

    uint32_t size_required = (uint32_t) (unsorted_append_size_required(max, new_index) * FOR_GROWTH_FACTOR);
    uint8_t *out = new uint8_t[size_required];
    uint32_t actual_size = for_compress_unsorted(new_array, out, new_index);

    delete[] curr_array;
    delete[] new_array;
    delete[] in;

    in = out;
    length = new_index;
    size_bytes = size_required;
    length_bytes = actual_size;
}