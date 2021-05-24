#include "sorted_array.h"
#include "array_utils.h"
#include "logger.h"

void sorted_array::load(const uint32_t *sorted_array, const uint32_t array_length) {
    min = array_length != 0 ? sorted_array[0] : 0;
    max = array_length > 1 ? sorted_array[array_length-1] : min;

    uint32_t size_required = (uint32_t) (sorted_append_size_required(max, array_length) * FOR_GROWTH_FACTOR);
    uint8_t *out = (uint8_t *) malloc(size_required * sizeof *out);
    memset(out, 0, size_required);
    uint32_t actual_size = for_compress_sorted(sorted_array, out, array_length);

    free(in);
    in = nullptr;

    in = out;
    length = array_length;
    size_bytes = size_required;
    length_bytes = actual_size;
}

size_t sorted_array::append(uint32_t value) {
    if(value < max) {
        // we will have to re-encode the whole sequence again
        uint32_t* arr = uncompress(length+1);

        // find the index of the element which is >= to `value`
        uint32_t found_val;
        uint32_t gte_index = for_lower_bound_search(in, length, value, &found_val);

        for(size_t j=length; j>gte_index; j--) {
            arr[j] = arr[j-1];
        }

        arr[gte_index] = value;

        load(arr, length+1);
        delete [] arr;

        return gte_index;
    } else {
        uint32_t size_required = sorted_append_size_required(value, length+1);
        size_t min_expected_size = size_required + FOR_ELE_SIZE;

        if(size_bytes < min_expected_size) {
            // grow the array first
            size_t new_size = min_expected_size * FOR_GROWTH_FACTOR;
            uint8_t *new_location = (uint8_t *) realloc(in, new_size);
            if(new_location == NULL) {
                abort();
            }
            in = new_location;
            size_bytes = (uint32_t) new_size;

            //LOG(INFO) << "new_size: " << new_size;
        }

        uint32_t new_length_bytes = for_append_sorted(in, length, value);
        if(new_length_bytes == 0) return false;

        length_bytes = new_length_bytes;
        length++;

        if(value < min) min = value;
        if(value > max) max = value;

        return length-1;
    }
}

bool sorted_array::insert(size_t index, uint32_t value) {
    if(index >= length) {
        return false;
    }

    uint32_t *curr_array = uncompress(length+1);
    memmove(&curr_array[index+1], &curr_array[index], sizeof(uint32_t)*(length-index));
    curr_array[index] = value;

    load(curr_array, length+1);

    delete [] curr_array;

    return true;
}

uint32_t sorted_array::at(uint32_t index) {
    return for_select(in, index);
}

bool sorted_array::contains(uint32_t value) {
    if(length == 0) {
        return false;
    }

    uint32_t actual;
    for_lower_bound_search(in, length, value, &actual);
    return actual == value;
}

uint32_t sorted_array::indexOf(uint32_t value) {
    if(length == 0) {
        return length;
    }

    uint32_t actual;
    uint32_t index = for_lower_bound_search(in, length, value, &actual);

    if(actual == value) {
        return index;
    }

    return length;
}

// returns the first element in the sequence which does not compare less than |value|.
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


// returns the first element in the sequence which does not compare less than |value|.
uint32_t sorted_array::lower_bound_search(const uint32_t *in, uint32_t imin, uint32_t imax,
                                          uint32_t value, uint32_t *actual) {
    uint32_t imid;
    uint32_t v;

    while (imin + 1 < imax) {
        imid = imin + ((imax - imin) / 2);

        v = in[imid];
        if (v >= value) {
            imax = imid;
        }
        else if (v < value) {
            imin = imid;
        }
    }

    v = in[imin];
    if (v >= value) {
        *actual = v;
        return imin;
    }

    v = in[imax];
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
        if(actual_value == values[pivot_vindex]) {
            indices[pivot_vindex] = in_index;
        } else {
            indices[pivot_vindex] = length;
        }

        binary_search_indices(values, low_vindex, pivot_vindex-1, low_index, in_index, base, bits, indices);
        binary_search_indices(values, pivot_vindex+1, high_vindex, in_index, high_index, base, bits, indices);
    }
}

void sorted_array::binary_search_indices(const uint32_t *values, int low_vindex, int high_vindex, int low_index,
                                         int high_index, uint32_t *indices) {

    uint32_t actual_value =  0;

    if(high_vindex >= low_vindex && high_index >= low_index) {
        size_t pivot_vindex = (low_vindex + high_vindex) / 2;

        uint32_t in_index = lower_bound_search(values, low_index, high_index,
                                               values[pivot_vindex], &actual_value);
        if(actual_value == values[pivot_vindex]) {
            indices[pivot_vindex] = in_index;
        } else {
            indices[pivot_vindex] = length;
        }

        binary_search_indices(values, low_vindex, pivot_vindex-1, low_index, in_index, indices);
        binary_search_indices(values, pivot_vindex+1, high_vindex, in_index, high_index, indices);
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

    // identify the upper and lower bounds of the search space
    int head = -1;
    do {
        head++;
        low_index = lower_bound_search_bits(in+METADATA_OVERHEAD, 0, length-1, base, bits, values[head], &actual_value);
    } while(head < int(values_len - 1) && actual_value > values[head]);

    int tail = values_len;
    do {
        tail--;
        high_index = lower_bound_search_bits(in+METADATA_OVERHEAD, 0, length-1, base, bits, values[tail], &actual_value);
    } while(tail > 0 && actual_value < values[tail]);

    for(int i = 0; i < head; i++) {
        indices[i] = length;
    }

    for(int j = values_len-1; j > tail; j--) {
        indices[j] = length;
    }

    // recursively search within the bounds for all values
    binary_search_indices(values, head, tail, low_index, high_index, base, bits, indices);
}

void sorted_array::remove_value(uint32_t value) {
    if(length == 0) {
        return ;
    }

    // A lower bound search returns the first element in the sequence that is >= `value`
    // So, `found_val` will be either equal or greater than `value`
    uint32_t found_val;
    uint32_t found_index = for_lower_bound_search(in, length, value, &found_val);

    if(found_val != value) {
        return ;
    }

    uint32_t *curr_array = uncompress();

    if(found_index + 1 < length) {
        memmove(&curr_array[found_index], &curr_array[found_index+1], sizeof(uint32_t) * (length - found_index - 1));
    }

    size_t new_length = (length == 0) ? 0 : (length - 1);
    load(curr_array, new_length);

    delete [] curr_array;
}

void sorted_array::remove_values(uint32_t *sorted_values, uint32_t sorted_values_length) {
    uint32_t *curr_array = uncompress();

    uint32_t *new_array = new uint32_t[length];
    uint32_t new_index = 0;

    uint32_t sorted_values_index = 0;
    uint32_t curr_index = 0;

    while(curr_index < length) {
        if(sorted_values_index < sorted_values_length && sorted_values[sorted_values_index] == curr_array[curr_index]) {
            curr_index++;
            sorted_values_index++;
        } else {
            new_array[new_index++] = curr_array[curr_index++];
        }
    }

    load(new_array, new_index);
    delete[] curr_array;
    delete[] new_array;
}

size_t sorted_array::numFoundOf(const uint32_t *values, const size_t values_len) {
    size_t num_found = 0;

    if(length == 0 || values_len == 0) {
        return num_found;
    }

    uint32_t low_index, high_index;
    uint32_t actual_value = 0;

    if(length > values_len) {
        uint32_t base = *(uint32_t *)(in + 0);
        uint32_t bits = *(in + 4);

        // identify the upper and lower bounds of the search space
        int head = -1;
        do {
            head++;
            low_index = lower_bound_search_bits(in+METADATA_OVERHEAD, 0, length-1, base, bits, values[head], &actual_value);
        } while(head < int(values_len - 1) && actual_value > values[head]);

        int tail = values_len;
        do {
            tail--;
            high_index = lower_bound_search_bits(in+METADATA_OVERHEAD, 0, length-1, base, bits, values[tail], &actual_value);
        } while(tail > 0 && actual_value < values[tail]);

        // recursively search within the bounds for all values
        binary_count_indices(values, head, tail, low_index, high_index, base, bits, num_found);
    } else {
        // identify the upper and lower bounds of the search space
        uint32_t* src = uncompress(length);

        int head = -1;
        do {
            head++;
            low_index = lower_bound_search(values, 0, values_len-1, src[head], &actual_value);
        } while(head < int(length - 1) && actual_value > src[head]);

        int tail = length;
        do {
            tail--;
            high_index = lower_bound_search(values, 0, values_len-1, src[tail], &actual_value);
        } while(tail > 0 && actual_value < src[tail]);

        // recursively search within the bounds for all values
        binary_count_indices(src, head, tail, values, low_index, high_index, num_found);

        delete [] src;
    }

    return num_found;
}

void sorted_array::binary_count_indices(const uint32_t *values, int low_vindex, int high_vindex, int low_index,
                                        int high_index, uint32_t base, uint32_t bits, size_t& num_found) {

    uint32_t actual_value =  0;

    if(high_vindex >= low_vindex && high_index >= low_index) {
        int pivot_vindex = (low_vindex + high_vindex) / 2;

        uint32_t in_index = lower_bound_search_bits(in+METADATA_OVERHEAD, low_index, high_index, base, bits,
                                                    values[pivot_vindex], &actual_value);

        //LOG(INFO) << "pivot_vindex: " << pivot_vindex << ", values[pivot_vindex]: " << values[pivot_vindex];
        if(actual_value == values[pivot_vindex]) {
            //LOG(INFO) << actual_value;
            num_found++;
        }

        binary_count_indices(values, low_vindex, pivot_vindex-1, low_index, in_index, base, bits, num_found);
        binary_count_indices(values, pivot_vindex+1, high_vindex, in_index, high_index, base, bits, num_found);
    }
}

void sorted_array::binary_count_indices(const uint32_t *values, int low_vindex, int high_vindex,
                                        const uint32_t* src, int low_index, int high_index, size_t &num_found) {

    uint32_t actual_value =  0;

    if(high_vindex >= low_vindex && high_index >= low_index) {
        int pivot_vindex = (low_vindex + high_vindex) / 2;

        uint32_t in_index = lower_bound_search(src, low_index, high_index, values[pivot_vindex], &actual_value);

        //LOG(INFO) << "pivot_vindex: " << pivot_vindex << ", values[pivot_vindex]: " << values[pivot_vindex];
        if(actual_value == values[pivot_vindex]) {
            //LOG(INFO) << actual_value;
            num_found++;
        }

        binary_count_indices(values, low_vindex, pivot_vindex-1, src, low_index, in_index, num_found);
        binary_count_indices(values, pivot_vindex+1, high_vindex, src, in_index, high_index, num_found);
    }
}

uint32_t sorted_array::last() {
    return (length == 0) ? UINT32_MAX : max;
}
