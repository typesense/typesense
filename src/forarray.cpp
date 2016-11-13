#include "forarray.h"

void forarray::load_sorted(const uint32_t *sorted_array, const uint32_t array_length) {
    min = sorted_array[0];
    max = array_length > 1 ? sorted_array[array_length-1] : min;

    uint32_t size_required = (uint32_t) (sorted_append_size_required(max, array_length) * FOR_GROWTH_FACTOR);
    uint8_t *out = new uint8_t[size_required];
    uint32_t actual_size = for_compress_sorted(sorted_array, out, array_length);

    delete[] in;

    in = out;
    length = array_length;
    size_bytes = size_required;
    length_bytes = actual_size;
}

bool forarray::append_sorted(uint32_t value) {
    uint32_t size_required = sorted_append_size_required(value, length+1);

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

    uint32_t new_length_bytes = for_append_sorted(in, length, value);
    if(new_length_bytes == 0) return false;

    length_bytes = new_length_bytes;
    length++;

    if(value < min) min = value;
    if(value > max) max = value;

    return true;
}

bool forarray::append_unsorted(uint32_t value) {
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

uint32_t forarray::at(uint32_t index) {
    return for_select(in, index);
}

bool forarray::contains(uint32_t value) {
    uint32_t actual;
    for_lower_bound_search(in, length, value, &actual);
    return actual == value;
}

uint32_t forarray::indexOf(uint32_t value) {
    uint32_t actual;
    uint32_t index = for_lower_bound_search(in, length, value, &actual);
    if(actual == value) return index;
    return length;
}

uint32_t* forarray::uncompress() {
    uint32_t *out = new uint32_t[length];
    for_uncompress(in, out, length);
    return out;
}

void forarray::remove_index_unsorted(uint32_t start_index, uint32_t end_index) {
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

void forarray::remove_values_sorted(uint32_t *sorted_values, uint32_t values_length) {
    uint32_t *curr_array = uncompress();

    uint32_t *new_array = new uint32_t[length];
    uint32_t new_index = 0;
    uint32_t curr_index = 0;
    uint32_t sorted_values_index = 0;

    while(curr_index < length) {
        if(sorted_values_index < values_length && curr_array[curr_index] >= sorted_values[sorted_values_index]) {
            // skip copying
            if(curr_array[curr_index] == sorted_values[sorted_values_index]) {
                curr_index++;
            }
            sorted_values_index++;
        } else {
            new_array[new_index++] = curr_array[curr_index++];
        }
    }

    load_sorted(new_array, new_index);
    delete[] curr_array;
    delete[] new_array;
}

uint32_t forarray::getSizeInBytes() {
    return size_bytes;
}

uint32_t forarray::getLength() {
    return length;
}
