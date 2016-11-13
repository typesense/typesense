#include <gtest/gtest.h>
#include "forarray.h"
#include <vector>

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
        EXPECT_EQ(arr.indexOf(i), i);
        EXPECT_EQ(arr.contains(i), true);
    }

    forarray arr_small;
    arr_small.append_sorted(100);
    EXPECT_EQ(arr_small.getLength(), 1);
    EXPECT_EQ(arr_small.at(0), 100);
}

TEST(ForarrayTest, AppendUnsorted) {
    forarray arr;
    int SIZE = 10 * 1000;

    EXPECT_EQ(arr.getLength(), 0);

    // First try inserting sorted ints

    for(uint32_t i=0; i < SIZE; i++) {
        arr.append_unsorted(i);
    }

    EXPECT_EQ(arr.getLength(), SIZE);

    for(uint32_t i=0; i < SIZE; i++) {
        EXPECT_EQ(arr.at(i), i);
        EXPECT_EQ(arr.indexOf(i), i);
        EXPECT_EQ(arr.contains(i), true);
    }

    // Insert in unsorted fashion
    forarray arr2;

    std::vector<uint32_t> unsorted;

    for(uint32_t i=0; i < SIZE; i++) {
        uint32_t r = (uint32_t) rand();
        unsorted.push_back(r);
        arr2.append_unsorted(r);
    }

    EXPECT_EQ(arr2.getLength(), SIZE);

    for(uint32_t i=0; i < SIZE; i++) {
        uint32_t value = unsorted.at(i);
        EXPECT_EQ(arr2.at(i), value);
    }
}

TEST(ForarrayTest, LoadSorted) {
    forarray arr;

    // To ensure that previous contents are erased
    arr.append_sorted(100);
    arr.append_sorted(200);

    const size_t SIZE = 10*1000;
    uint32_t *array = new uint32_t[SIZE];

    for(size_t i=0; i<SIZE; i++) {
        array[i] = (uint32_t) i;
    }

    arr.load_sorted(array, SIZE);

    for(size_t i=0; i<SIZE; i++) {
        ASSERT_EQ(array[i], arr.at(i));
    }
}

TEST(ForarrayTest, Uncompress) {
    // Sorted array

    forarray sorted_arr;

    const size_t SIZE = 10*1000;
    for(size_t i=0; i<SIZE; i++) {
        sorted_arr.append_sorted(i);
    }

    uint32_t *raw_sorted_arr = sorted_arr.uncompress();
    for(size_t i=0; i<sorted_arr.getLength(); i++) {
        ASSERT_EQ(raw_sorted_arr[i], sorted_arr.at(i));
    }

    delete raw_sorted_arr;

    // Unsorted array

    forarray unsorted_arr;
    std::vector<uint32_t> unsorted;

    for(size_t i=0; i<SIZE; i++) {
        uint32_t r = (uint32_t) rand();
        unsorted.push_back(r);
        unsorted_arr.append_unsorted(r);
    }

    uint32_t *raw_unsorted_arr = unsorted_arr.uncompress();
    for(size_t i=0; i<unsorted.size(); i++) {
        ASSERT_EQ(raw_unsorted_arr[i], unsorted.at(i));
    }

    delete raw_unsorted_arr;
}

TEST(ForarrayTest, RemoveIndexUnsorted) {
    forarray arr;

    const size_t SIZE = 10*1000;
    std::vector<uint32_t> unsorted;

    for(size_t i=0; i<SIZE; i++) {
        uint32_t r = (uint32_t) rand();
        unsorted.push_back(r);
        arr.append_unsorted(r);
    }

    // Remove first two elements

    arr.remove_index_unsorted(0, 2);
    unsorted.erase(unsorted.begin(), unsorted.begin()+2);

    ASSERT_EQ(arr.getLength(), SIZE-2);

    for(size_t i=0; i<SIZE-2; i++) {
        ASSERT_EQ(arr.at(i), unsorted.at(i));
    }

    // Remove from the middle

    arr.remove_index_unsorted(1200, 2400);
    unsorted.erase(unsorted.begin()+1200, unsorted.begin()+2400);

    ASSERT_EQ(arr.getLength(), SIZE-2-1200);

    for(size_t i=0; i<SIZE-2-1200; i++) {
        ASSERT_EQ(arr.at(i), unsorted.at(i));
    }

    // Remove from the end

    const uint32_t NEW_SIZE = arr.getLength();

    arr.remove_index_unsorted(NEW_SIZE-3, NEW_SIZE);
    unsorted.erase(unsorted.begin()+NEW_SIZE-3, unsorted.begin()+NEW_SIZE);

    ASSERT_EQ(arr.getLength(), NEW_SIZE-3);

    for(size_t i=0; i<NEW_SIZE-3; i++) {
        ASSERT_EQ(arr.at(i), unsorted.at(i));
    }
}

TEST(ForarrayTest, RemoveValuesSorted) {
    forarray arr;

    const size_t SIZE = 10*1000;
    for(size_t i=0; i<SIZE; i++) {
        arr.append_sorted(i);
    }

    uint32_t values[5] = {0, 100, 1000, 2000, SIZE-1};
    arr.remove_values_sorted(values, 5);

    ASSERT_EQ(arr.getLength(), SIZE-5);

    for(size_t i=0; i<SIZE-5; i++) {
        uint32_t value = arr.at(i);
        ASSERT_FALSE(value == 0);
        ASSERT_FALSE(value == 100);
        ASSERT_FALSE(value == 1000);
        ASSERT_FALSE(value == 2000);
        ASSERT_FALSE(value == SIZE-1);
    }
}