#include <gtest/gtest.h>
#include "sorted_array.h"
#include <vector>
#include <fstream>
#include "string_utils.h"

TEST(SortedArrayTest, Append) {
    sorted_array arr;
    const int SIZE = 10 * 1000;

    EXPECT_EQ(arr.getLength(), 0);

    for(uint32_t i=0; i < SIZE; i++) {
        arr.append(i);
    }

    EXPECT_EQ(arr.getLength(), SIZE);

    for(uint32_t i=0; i < SIZE; i++) {
        EXPECT_EQ(arr.at(i), i);
        EXPECT_EQ(arr.indexOf(i), i);
        EXPECT_EQ(arr.contains(i), true);
    }

    EXPECT_EQ(arr.contains(SIZE), false);
    EXPECT_EQ(arr.indexOf(SIZE), SIZE);
    EXPECT_EQ(arr.indexOf(SIZE+1), SIZE);

    sorted_array arr_small;
    arr_small.append(100);
    EXPECT_EQ(arr_small.getLength(), 1);
    EXPECT_EQ(arr_small.at(0), 100);
}

TEST(SortedArrayTest, Load) {
    sorted_array arr;

    // To ensure that previous contents are erased
    arr.append(100);
    arr.append(200);

    const size_t SIZE = 10*1000;
    uint32_t *array = new uint32_t[SIZE];

    for(size_t i=0; i<SIZE; i++) {
        array[i] = (uint32_t) i;
    }

    arr.load(array, SIZE);

    for(size_t i=0; i<SIZE; i++) {
        ASSERT_EQ(array[i], arr.at(i));
    }
}

TEST(SortedArrayTest, Uncompress) {
    sorted_array sorted_arr;

    const size_t SIZE = 10*1000;
    for(size_t i=0; i<SIZE; i++) {
        sorted_arr.append(i);
    }

    uint32_t *raw_sorted_arr = sorted_arr.uncompress();
    for(size_t i=0; i<sorted_arr.getLength(); i++) {
        ASSERT_EQ(raw_sorted_arr[i], sorted_arr.at(i));
    }

    delete raw_sorted_arr;
}

TEST(SortedArrayTest, RemoveValues) {
    sorted_array arr;

    const size_t SIZE = 10*1000;
    for(size_t i=0; i<SIZE; i++) {
        arr.append(i);
    }

    uint32_t values[5] = {0, 100, 1000, 2000, SIZE-1};
    arr.remove_values(values, 5);

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

TEST(SortedArrayTest, Intersect) {
    sorted_array arr1;
    const size_t size1 = 9;
    for(size_t i = 0; i < size1; i++) {
        arr1.append(i);
    }

    const size_t size2 = 10;
    uint32_t *arr2 = new uint32_t[size2];
    size_t arr2_len = 0;
    for(size_t i = 2; i < size2; i++) {
        if(i % 3 == 0) {
            arr2[arr2_len++] = i;
        }
    }

    // arr1: [0..8] , arr2: [3, 6, 9]
    uint32_t *results = new uint32_t[size2];
    uint32_t results_size = arr1.intersect(arr2, arr2_len, results);
    ASSERT_EQ(2, results_size);

    std::vector<uint32_t> expected = {3, 6};

    for(size_t i = 0; i < results_size; i++) {
        ASSERT_EQ(expected[i], results[i]);
    }
}

TEST(SortedArrayTest, MergeShouldRemoveDuplicates) {
    sorted_array arr1;
    const size_t size1 = 9;
    for(size_t i = 0; i < size1; i++) {
        arr1.append(i);
    }

    const size_t size2 = 10;
    uint32_t *arr2 = new uint32_t[size2];
    size_t arr2_len = 0;
    for(size_t i = 2; i < size2; i++) {
        if(i % 3 == 0) {
            arr2[arr2_len++] = i;
        }
    }

    // arr1: [0..8] , arr2: [3, 6, 9]
    uint32_t *results = new uint32_t[size1];
    uint32_t results_size = arr1.do_union(arr2, arr2_len, results);
    ASSERT_EQ(10, results_size);

    std::vector<uint32_t> expected = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    for(size_t i = 0; i < results_size; i++) {
        ASSERT_EQ(expected[i], results[i]);
    }

    delete[] results;
}

TEST(SortedArrayTest, MergeShouldRemoveDuplicatesAtBoundary) {
    std::vector<uint32_t> vec1 = {1, 2, 3, 5, 6, 7, 8};
    sorted_array arr1;
    for(auto i: vec1) {
        arr1.append(i);
    }

    std::vector<uint32_t> vec2 = {0, 4, 5};
    uint32_t *arr2 = new uint32_t[vec2.size()];
    auto j = 0;
    for(auto i: vec2) {
        arr2[j++] = i;
    }

    uint32_t *results = new uint32_t[10];
    uint32_t results_size = arr1.do_union(arr2, vec2.size(), results);
    ASSERT_EQ(9, results_size);

    std::vector<uint32_t> expected = {0, 1, 2, 3, 4, 5, 6, 7, 8};

    for(size_t i = 0; i < results_size; i++) {
        ASSERT_EQ(expected[i], results[i]);
    }

    delete[] results;
}

TEST(SortedArrayTest, BulkIndexOf) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/ids.txt");

    sorted_array ids;

    std::string line;
    while (std::getline(infile, line)) {
        ids.append(std::stoi(line));
    }
    infile.close();

    std::vector<uint32_t> search_ids = { 17879, 37230, 412020, 445251, 447484, 501102, 640551, 656498, 656531,
                                         770014, 877700, 1034172, 1115941, 1129099, 1159053, 1221486, 1295375 };

    uint32_t *results = new uint32_t[search_ids.size()];
    ids.indexOf(&search_ids[0], search_ids.size(), results);

    for(auto i = 0; i < search_ids.size(); i++) {
        auto search_id = search_ids.at(i);
        ASSERT_EQ(ids.indexOf(search_id), results[i]);
    }

    // when some IDs are not to be found

    search_ids.clear();
    search_ids = { 7879, 37230, 422020, 445251, 457484, 501102, 630551};

    delete [] results;
    results = new uint32_t[search_ids.size()];

    ids.indexOf(&search_ids[0], search_ids.size(), results);

    for(auto i = 0; i < search_ids.size(); i++) {
        auto search_id = search_ids.at(i);
        ASSERT_EQ(ids.indexOf(search_id), results[i]);
    }
}