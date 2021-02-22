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

void array::load(const uint32_t *sorted_array, const uint32_t array_length, const uint32_t m, const uint32_t M) {
    min = m;
    max = M;

    uint32_t size_required = (uint32_t) (unsorted_append_size_required(max, array_length) * FOR_GROWTH_FACTOR);
    uint8_t *out = (uint8_t *) malloc(size_required * sizeof *out);
    memset(out, 0, size_required);
    uint32_t actual_size = for_compress_unsorted(sorted_array, out, array_length);

    free(in);
    in = nullptr;

    in = out;
    length = array_length;
    size_bytes = size_required;
    length_bytes = actual_size;
}

bool array::insert(size_t index, const uint32_t* values, size_t num_values) {
    if(index >= length) {
        return false;
    }

    uint32_t *curr_array = uncompress(length+num_values);
    memmove(&curr_array[index+num_values], &curr_array[index], sizeof(uint32_t)*(length-index));

    uint32_t m = min, M = max;

    for(size_t i=0; i<num_values; i++) {
        uint32_t value = values[i];
        if(value < m) m = value;
        if(value > M) M = value;
        curr_array[index+i] = value;
    }

    load(curr_array, length+num_values, m, M);

    delete [] curr_array;

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
    uint8_t *out = (uint8_t *) malloc(size_required * sizeof *out);
    memset(out, 0, size_required);
    uint32_t actual_size = for_compress_unsorted(new_array, out, new_index);

    delete[] curr_array;
    delete[] new_array;
    free(in);

    in = out;
    length = new_index;
    size_bytes = size_required;
    length_bytes = actual_size;
}