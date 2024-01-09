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

  /// Performs binary search to find the index of id. If id is not found, curr_index is set to the index of next bigger
  /// number than id in the array.
  /// \return Whether or not id was found in array.
  static bool skip_index_to_id(uint32_t& curr_index, uint32_t const* const array, const uint32_t& array_len,
                               const uint32_t& id);
};