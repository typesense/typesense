#include "array_base.h"

uint32_t* array_base::uncompress(uint32_t len) const {
    const uint32_t actual_len = std::max(len, length);

    // Allocate at least 1 so callers can always delete[] safely and to avoid oddities with new[0]
    uint32_t* out = new uint32_t[actual_len ? actual_len : 1];

    if (length == 0) {
        // Nothing to decompress; return an empty buffer.
        return out;
    }

    for_uncompress(in, out, length);
    return out;
}

uint32_t array_base::getSizeInBytes() {
    return size_bytes;
}

uint32_t array_base::getLength() const {
    return length;
}

uint32_t array_base::getMin() const {
    return min;
}

uint32_t array_base::getMax() const {
    return max;
}
