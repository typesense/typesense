#include "array_base.h"

uint32_t* array_base::uncompress() {
    uint32_t *out = new uint32_t[length];
    for_uncompress(in, out, length);
    return out;
}

uint32_t array_base::getSizeInBytes() {
    return size_bytes;
}

uint32_t array_base::getLength() {
    return length;
}
