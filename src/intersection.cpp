#include "intersection.h"

size_t Intersection::scalar(const uint32_t *A, const size_t lenA,
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
