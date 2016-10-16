#include <gtest/gtest.h>
#include "forarray.h"

TEST(ForarrayTest, AppendSorted) {
    forarray arr;
    const int SIZE = 10 * 1000;

    EXPECT_EQ(arr.getLength(), 0);

    for(uint32_t i=0; i < SIZE; i++) {
        arr.append_sorted(i);
    }

    EXPECT_EQ(arr.getLength(), SIZE);

    for(uint32_t i=0; i < SIZE; i++) {
        EXPECT_EQ(arr.at(i), i);
    }
}