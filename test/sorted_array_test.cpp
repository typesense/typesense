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
        size_t appended_index = arr.append(i);
        ASSERT_EQ(i, appended_index);
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
    size_t appended_index = arr_small.append(100);
    EXPECT_EQ(0, appended_index);
    EXPECT_EQ(arr_small.getLength(), 1);
    EXPECT_EQ(arr_small.at(0), 100);
}

TEST(SortedArrayTest, AppendOutOfOrder) {
    sorted_array arr;
    for(size_t i=5; i<=10; i++) {
        size_t appended_index = arr.append(i);
        ASSERT_EQ(i-5, appended_index);
    }

    EXPECT_EQ(6, arr.getLength());

    int appended_index = -1;

    appended_index = arr.append(1);
    ASSERT_EQ(0, appended_index);

    appended_index = arr.append(3);
    ASSERT_EQ(1, appended_index);

    appended_index = arr.append(2);
    ASSERT_EQ(1, appended_index);

    appended_index = arr.append(4);
    ASSERT_EQ(3, appended_index);

    appended_index = arr.append(11);
    ASSERT_EQ(10, appended_index);

    appended_index = arr.append(14);
    ASSERT_EQ(11, appended_index);

    appended_index = arr.append(12);
    ASSERT_EQ(11, appended_index);

    EXPECT_EQ(13, arr.getLength());
}

TEST(SortedArrayTest, InsertAtIndex) {
    std::vector<uint32_t> eles;
    sorted_array arr;
    for(size_t i=5; i<=9; i++) {
        arr.append(i);
    }

    arr.append(11);
    eles = {5, 6, 7, 8, 9, 11};

    for(size_t i=0; i < eles.size(); i++) {
        ASSERT_EQ(eles[i], arr.at(i));
    }

    arr.insert(0, 1);
    eles = { 1, 5, 6, 7, 8, 9, 11 };

    for(size_t i=0; i < eles.size(); i++) {
        ASSERT_EQ(eles[i], arr.at(i));
    }

    ASSERT_EQ(1, arr.at(0));
    ASSERT_EQ(5, arr.at(1));

    arr.insert(1, 2);
    eles = {1, 2, 5, 6, 7, 8, 9, 11};
    ASSERT_EQ(1, arr.at(0));
    ASSERT_EQ(2, arr.at(1));
    ASSERT_EQ(8, arr.getLength());

    for(size_t i=0; i < eles.size(); i++) {
        ASSERT_EQ(eles[i], arr.at(i));
    }

    arr.insert(7, 10);
    eles = { 1, 2, 5, 6, 7, 8, 9, 10, 11};
    ASSERT_EQ(10, arr.at(7));
    ASSERT_EQ(11, arr.at(8));
    ASSERT_EQ(9, arr.getLength());

    for(size_t i=0; i < eles.size(); i++) {
        ASSERT_EQ(eles[i], arr.at(i));
    }

    ASSERT_FALSE(arr.insert(9, 12));  // index out of range
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

TEST(SortedArrayTest, RemoveValue) {
    sorted_array arr;

    const size_t SIZE = 10*1000;
    for(size_t i=0; i<SIZE; i++) {
        arr.append(i);
    }

    uint32_t values[5] = {0, 100, 1000, 2000, SIZE-1};

    for(size_t i=0; i<5; i++) {
        arr.remove_value(values[i]);
    }

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

    for(size_t i = 0; i < search_ids.size(); i++) {
        auto search_id = search_ids.at(i);
        ASSERT_EQ(ids.indexOf(search_id), results[i]);
    }

    // when some IDs are not to be found

    search_ids.clear();
    search_ids = { 7879, 37230, 422020, 445251, 457484, 501102, 630551};

    delete [] results;
    results = new uint32_t[search_ids.size()];

    ids.indexOf(&search_ids[0], search_ids.size(), results);

    for(size_t i = 0; i < search_ids.size(); i++) {
        auto search_id = search_ids.at(i);
        ASSERT_EQ(ids.indexOf(search_id), results[i]);
    }
}