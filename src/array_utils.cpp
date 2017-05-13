#include "array_utils.h"

size_t ArrayUtils::and_scalar(const uint32_t *A, const size_t lenA,
                              const uint32_t *B, const size_t lenB, uint32_t *out) {
  const uint32_t *const initout(out);
  if (lenA == 0 || lenB == 0)
    return 0;

  const uint32_t *endA = A + lenA;
  const uint32_t *endB = B + lenB;

  while (1) {
    while (*A < *B) {
      SKIP_FIRST_COMPARE:
      if (++A == endA)
        return (out - initout);
    }
    while (*A > *B) {
      if (++B == endB)
        return (out - initout);
    }
    if (*A == *B) {
      *out++ = *A;
      if (++A == endA || ++B == endB)
        return (out - initout);
    } else {
      goto SKIP_FIRST_COMPARE;
    }
  }

  return (out - initout); // NOTREACHED
}

size_t ArrayUtils::or_scalar(const uint32_t *A, const size_t lenA,
                             const uint32_t *B, const size_t lenB, uint32_t **out) {
    size_t indexA = 0, indexB = 0, res_index = 0;

    if(A == nullptr) {
        *out = new uint32_t[lenB];
        memcpy(*out, B, lenB * sizeof(uint32_t));
        return lenB;
    }

    uint32_t* results = new uint32_t[lenA+lenB];

    while (indexA < lenA && indexB < lenB) {
    if (A[indexA] < B[indexB]) {
      if(res_index == 0 || results[res_index-1] != A[indexA]) {
        results[res_index] = A[indexA];
        res_index++;
      }
      indexA++;
    } else {
      if(res_index == 0 || results[res_index-1] != B[indexB]) {
        results[res_index] = B[indexB];
        res_index++;
      }
      indexB++;
    }
  }

  while (indexA < lenA) {
    if(results[res_index-1] != A[indexA]) {
      results[res_index] = A[indexA];
      res_index++;
    }

    indexA++;
  }

  while (indexB < lenB) {
    if(results[res_index-1] != B[indexB]) {
      results[res_index] = B[indexB];
      res_index++;
    }

    indexB++;
  }

  // shrink fit
  *out = new uint32_t[res_index];
  std::memmove(*out, results, res_index * sizeof(uint32_t));
  delete[] results;

  return res_index;
}