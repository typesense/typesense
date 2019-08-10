#pragma once

#include <cstddef>
#include <stdint.h>
#include <array>

/* Different intersection routines adapted from:
 * https://github.com/lemire/SIMDCompressionAndIntersection/blob/master/src/intersection.cpp
 */
class ArrayUtils {
public:
  // Fast scalar scheme designed by N. Kurz. Returns the size of out (intersected set)
  static size_t and_scalar(const uint32_t *A, const size_t lenA, const uint32_t *B, const size_t lenB, uint32_t **out);

  static size_t or_scalar(const uint32_t *A, const size_t lenA, const uint32_t *B, const size_t lenB, uint32_t **out);

  static size_t exclude_scalar(const uint32_t *src, const size_t lenSrc, const uint32_t *filter, const size_t lenFilter,
                              uint32_t **out);
};