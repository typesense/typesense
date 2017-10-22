#include <gtest/gtest.h>
#include "sorted_array.h"
#include <vector>
#include <fstream>
#include "string_utils.h"

TEST(SortedArrayTest, Append) {
    sorted_array arr;
    const int SIZE = 10 * 1000;

    EXPECT_EQ(arr.getLength(), 0);
    EXPECT_EQ(arr.indexOf(100), 0);  // when not found must be equal to length (0 in this case)

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

    delete[] raw_sorted_arr;
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