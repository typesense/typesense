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

uint32_t sorted_array::lower_bound_search_bits(const uint8_t *in, uint32_t imin, uint32_t imax, uint32_t base,
                                               uint32_t bits, uint32_t value, uint32_t *actual) {
    uint32_t imid;
    uint32_t v;

    while (imin + 1 < imax) {
        imid = imin + ((imax - imin) / 2);

        v = for_select_bits(in, base, bits, imid);
        if (v >= value) {
            imax = imid;
        }
        else if (v < value) {
            imin = imid;
        }
    }

    v = for_select_bits(in, base, bits, imin);
    if (v >= value) {
        *actual = v;
        return imin;
    }

    v = for_select_bits(in, base, bits, imax);
    *actual = v;
    return imax;
}

void sorted_array::binary_search_indices(const uint32_t *values, int low_vindex, int high_vindex,
                                         int low_index, int high_index, uint32_t base, uint32_t bits,
                                         uint32_t *indices) {
    uint32_t actual_value =  0;

    if(high_vindex >= low_vindex && high_index >= low_index) {
        size_t pivot_vindex = (low_vindex + high_vindex) / 2;

        uint32_t in_index = lower_bound_search_bits(in+METADATA_OVERHEAD, low_index, high_index, base, bits,
                                                    values[pivot_vindex], &actual_value);
        //if(actual_value == values[pivot_vindex]) {
            indices[pivot_vindex] = in_index;
        //}

        size_t pivot_index = (low_index + high_index) / 2;

        binary_search_indices(values, low_vindex, pivot_vindex-1, low_index, pivot_index-1,
                              base, bits, indices);
        binary_search_indices(values, pivot_vindex+1, high_vindex, pivot_index+1, high_index,
                              base, bits, indices);
    }
}

void sorted_array::indexOf(const uint32_t *values, const size_t values_len, uint32_t *indices) {
    if(values_len == 0) {
        return ;
    }

    uint32_t base = *(uint32_t *)(in + 0);
    uint32_t bits = *(in + 4);

    uint32_t low_index, high_index;
    uint32_t actual_value = 0;

    do {
        low_index = lower_bound_search_bits(in+METADATA_OVERHEAD, 0, length-1, base, bits, values[0], &actual_value);
    } while(actual_value != values[0]);

    do {
        high_index = lower_bound_search_bits(in+METADATA_OVERHEAD, 0, length-1, base, bits, values[values_len-1], &actual_value);
    } while(actual_value != values[values_len-1]);

    binary_search_indices(values, 0, values_len-1, low_index, high_index, base, bits, indices);
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

size_t sorted_array::intersect(uint32_t* arr, const size_t arr_length, uint32_t* results) {
    uint32_t* curr = uncompress();
    size_t results_length = Intersection::scalar(arr, arr_length, curr, length, results);
    delete[] curr;
    return results_length;
}

size_t sorted_array::do_union(uint32_t *arr, const size_t arr_length, uint32_t *results) {
    size_t curr_index = 0, arr_index = 0, res_index = 0;
    uint32_t* curr = uncompress();

    while (curr_index < length && arr_index < arr_length) {
        if (curr[curr_index] < arr[arr_index]) {
            if(res_index == 0 || results[res_index-1] != curr[curr_index]) {
                results[res_index] = curr[curr_index];
                res_index++;
            }
            curr_index++;
        } else {
            if(res_index == 0 || results[res_index-1] != arr[arr_index]) {
                results[res_index] = arr[arr_index];
                res_index++;
            }
            arr_index++;
        }
    }

    while (arr_index < arr_length) {
        if(results[res_index-1] != arr[arr_index]) {
            results[res_index] = arr[arr_index];
            res_index++;
        }

        arr_index++;
    }

    while (curr_index < length) {
        if(results[res_index-1] != curr[curr_index]) {
            results[res_index] = curr[curr_index];
            res_index++;
        }

        curr_index++;
    }

    delete[] curr;
    return res_index;
}