#include "sorted_array.h"
#include "intersection.h"

void sorted_array::load(const uint32_t *sorted_array, const uint32_t array_length) {
    min = sorted_array[0];
    max = array_length > 1 ? sorted_array[array_length-1] : min;

    uint32_t size_required = (uint32_t) (sorted_append_size_required(max, array_length) * FOR_GROWTH_FACTOR);
    uint8_t *out = new uint8_t[size_required];
    uint32_t actual_size = for_compress_sorted(sorted_array, out, array_length);

    delete[] in;
    in = nullptr;

    in = out;
    length = array_length;
    size_bytes = size_required;
    length_bytes = actual_size;
}

bool sorted_array::append(uint32_t value) {
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

uint32_t sorted_array::at(uint32_t index) {
    return for_select(in, index);
}

bool sorted_array::contains(uint32_t value) {
    uint32_t actual;
    for_lower_bound_search(in, length, value, &actual);
    return actual == value;
}

uint32_t sorted_array::indexOf(uint32_t value) {
    uint32_t actual;
    uint32_t index = for_lower_bound_search(in, length, value, &actual);
    if(actual == value) return index;
    return length;
}

void sorted_array::remove_values(uint32_t *sorted_values, uint32_t values_length) {
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

    load(new_array, new_index);
    delete[] curr_array;
    delete[] new_array;
}

size_t sorted_array::intersect(uint32_t* arr, size_t arr_size, uint32_t* results) {
    uint32_t* curr = uncompress();
    size_t results_size = Intersection::scalar(arr, arr_size, curr, length, results);
    delete[] curr;
    return results_size;
}