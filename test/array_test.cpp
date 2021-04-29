#include <gtest/gtest.h>
#include "array.h"
#include <vector>

TEST(ArrayTest, Append) {
    array arr;
    int SIZE = 10 * 1000;

    EXPECT_EQ(arr.getLength(), 0);

    // First try inserting sorted ints

    for(int i=0; i < SIZE; i++) {
        arr.append(i);
    }

    EXPECT_EQ(arr.getLength(), SIZE);

    for(int i=0; i < SIZE; i++) {
        EXPECT_EQ(arr.at(i), i);
        EXPECT_EQ(arr.indexOf(i), i);
        EXPECT_EQ(arr.contains(i), true);
    }

    EXPECT_EQ(arr.contains(SIZE), false);
    EXPECT_EQ(arr.indexOf(SIZE), SIZE);
    EXPECT_EQ(arr.indexOf(SIZE+1), SIZE);

    // Insert in unsorted fashion
    array arr2;

    std::vector<uint32_t> unsorted;

    for(int i=0; i < SIZE; i++) {
        uint32_t r = (uint32_t) rand();
        unsorted.push_back(r);
        arr2.append(r);
    }

    EXPECT_EQ(arr2.getLength(), SIZE);

    for(int i=0; i < SIZE; i++) {
        uint32_t value = unsorted.at(i);
        EXPECT_EQ(arr2.at(i), value);
    }
}

TEST(ArrayTest, InsertValues) {
    std::vector<uint32_t> eles = {10, 1, 4, 5, 7};
    array arr;

    for(size_t i=0; i < eles.size(); i++) {
        arr.append(eles[i]);
    }

    uint32_t insert_arr[2] = {2, 3};
    arr.insert(2, insert_arr, 2);
    eles = {10, 1, 2, 3, 4, 5, 7};

    for(size_t i=0; i < eles.size(); i++) {
        ASSERT_EQ(eles[i], arr.at(i));
    }

    uint32_t insert_arr2[2] = {20, 25};
    arr.insert(6, insert_arr2, 2);

    eles = {10, 1, 2, 3, 4, 5, 20, 25, 7};
    for(size_t i=0; i < eles.size(); i++) {
        ASSERT_EQ(eles[i], arr.at(i));
    }
}

TEST(ArrayTest, Uncompress) {
    const size_t SIZE = 10*1000;

    array unsorted_arr;
    std::vector<uint32_t> unsorted;

    for(size_t i=0; i<SIZE; i++) {
        uint32_t r = (uint32_t) rand();
        unsorted.push_back(r);
        unsorted_arr.append(r);
    }

    uint32_t *raw_unsorted_arr = unsorted_arr.uncompress();
    for(size_t i=0; i<unsorted.size(); i++) {
        ASSERT_EQ(raw_unsorted_arr[i], unsorted.at(i));
    }

    delete[] raw_unsorted_arr;
}

TEST(ArrayTest, RemoveBetweenIndices) {
    array arr;

    const size_t SIZE = 10*1000;
    std::vector<uint32_t> unsorted;

    // try removing from empty array
    arr.append(100);
    arr.remove_index(0, 1);
    arr.contains(100);
    arr.remove_index(0, 1);

    for(size_t i=0; i<SIZE; i++) {
        uint32_t r = (uint32_t) rand();
        unsorted.push_back(r);
        arr.append(r);
    }

    // Remove first two elements

    arr.remove_index(0, 2);
    unsorted.erase(unsorted.begin(), unsorted.begin()+2);

    ASSERT_EQ(arr.getLength(), SIZE-2);

    for(size_t i=0; i<SIZE-2; i++) {
        ASSERT_EQ(arr.at(i), unsorted.at(i));
    }

    // Remove from the middle

    arr.remove_index(1200, 2400);
    unsorted.erase(unsorted.begin()+1200, unsorted.begin()+2400);

    ASSERT_EQ(arr.getLength(), SIZE-2-1200);

    for(size_t i=0; i<SIZE-2-1200; i++) {
        ASSERT_EQ(arr.at(i), unsorted.at(i));
    }

    // Remove from the end

    const uint32_t NEW_SIZE = arr.getLength();

    arr.remove_index(NEW_SIZE - 3, NEW_SIZE);
    unsorted.erase(unsorted.begin()+NEW_SIZE-3, unsorted.begin()+NEW_SIZE);

    ASSERT_EQ(arr.getLength(), NEW_SIZE-3);

    for(size_t i=0; i<NEW_SIZE-3; i++) {
        ASSERT_EQ(arr.at(i), unsorted.at(i));
    }
}