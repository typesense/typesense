#include <gtest/gtest.h>
#include "sorted_array.h"
#include <vector>
#include <fstream>

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

    delete [] array;
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

    // remove value on an empty arr
    arr.append(100);
    arr.remove_value(100);
    arr.remove_value(110);

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

    // search with IDs that don't exist

    search_ids = {100};
    delete [] results;
    results = new uint32_t[search_ids.size()];

    ids.indexOf(&search_ids[0], search_ids.size(), results);
    ASSERT_EQ(562, results[0]);

    search_ids = {100, 105};
    delete [] results;
    results = new uint32_t[search_ids.size()];

    ids.indexOf(&search_ids[0], search_ids.size(), results);
    ASSERT_EQ(562, results[0]);
    ASSERT_EQ(562, results[1]);

    delete [] results;
}

TEST(SortedArrayTest, BulkIndexOf2) {
    std::vector<uint32_t> ids = {3, 44, 51, 54, 57, 60, 121, 136, 232, 238, 278, 447, 452, 454, 455, 456, 457, 459, 463,
                                 465, 471, 472, 473, 474, 475, 478, 479, 480, 486, 490, 492, 496, 503, 508, 510, 512,
                                 515, 526, 527, 533, 534, 537, 544, 547, 551, 553, 565, 573, 574, 577, 579, 617, 621,
                                 626, 628, 635, 653, 667, 672, 675, 689, 696, 705, 711, 714, 716, 725, 731, 735, 738,
                                 739, 747, 751, 758, 762, 773, 778, 786, 787, 801, 810, 817, 821, 826, 829, 835, 836,
                                 844, 852, 853, 854, 856, 860, 861, 895, 906, 952, 953, 955, 961, 966, 968, 985, 987,
                                 988, 994, 996, 999, 1005, 1007, 1027, 1030, 1034, 1037, 1040, 1041, 1043, 1057, 1060,
                                 1062, 1065, 1073, 1095, 1119, 1127, 1136, 1137, 1144, 1148, 1150, 1158, 1161, 1167,
                                 1168, 1170, 1182, 1191, 1223, 1229, 1241, 1247, 1279, 1282, 1287, 1290, 1293, 1302,
                                 1308, 1319, 1323, 1328, 1329, 1344, 1345, 1349, 1351, 1353, 1357, 1364, 1368, 1374,
                                 1386, 1389, 1405, 1411, 1421, 1423, 1424, 1439, 1442, 1449, 1452, 1453, 1455, 1458,
                                 1496, 1500, 1501, 1508, 1512, 1526, 1533, 1541, 1546, 1551, 1568, 1579, 1582, 1588,
                                 1589, 1604, 1656, 1658, 1662, 1667, 1682, 1699, 1704, 1714, 1719, 1723, 1728, 1736,
                                 1737, 1744, 1749, 1764, 1768, 1772, 1778, 1820, 1841, 1860, 1880, 1882, 1896, 1907,
                                 1921, 1949, 1959, 1988, 1990, 1995, 2006, 2027, 2090, 2097, 2110, 2130, 2135, 2138,
                                 2144, 2154, 2159, 2165, 2177, 2186, 2204, 2229, 2234, 2255, 2272, 2301, 2319, 2360,
                                 2372, 2383, 2400, 2404, 2441, 2444, 2447, 2460, 2464, 2477, 2480, 2490, 2497, 2513,
                                 2519, 2539, 2547, 2553, 2562, 2570, 2585, 2587, 2590, 2607, 2625, 2633, 2641, 2649,
                                 2650, 2679, 2680, 2698, 2699, 2752, 2782, 2788, 2818, 2829, 2834, 2885, 2892, 2926,
                                 2948, 2954, 2958, 3071, 3088, 3094, 3099, 3124, 3148, 3149, 3151, 3152, 3197, 3212,
                                 3250, 3256, 3269};

    std::vector<uint32_t> filter_ids = {9, 19, 21, 22, 23, 25, 26, 27, 28, 29, 30, 32, 33, 34, 35, 36, 38, 39, 41, 42,
                                        46, 47, 48, 49, 52, 57, 58, 60, 61, 63, 67, 68, 69, 71, 72, 74, 75, 76, 77, 78,
                                        79, 80, 85, 86, 87, 89, 91, 93, 94, 96, 100, 102, 103, 104, 106, 109, 111, 112,
                                        113, 114, 115, 117, 118, 119, 123, 124, 127, 128, 129, 132, 133, 134, 135, 139,
                                        141, 142, 143, 144, 146, 147, 148, 149, 151, 152, 154, 155, 157, 158, 159, 161,
                                        162, 163, 164, 169, 170, 172, 174, 178, 179, 180, 181, 182, 183, 184, 185, 186,
                                        187, 188, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 206,
                                        207, 211, 212, 213, 215, 217, 219, 222, 223, 225, 226, 229, 230, 232, 233, 234,
                                        237, 239, 240, 241, 243, 244, 245, 246, 247, 248, 256, 257, 261, 262, 263, 264,
                                        265, 266, 267, 269, 270, 271, 272, 273, 274, 275, 279, 280, 281, 282, 284, 286,
                                        288, 289, 291, 292, 296, 297, 298, 299, 303, 304, 305, 307, 308, 309, 310, 311,
                                        312, 313, 314, 317, 318, 320, 321, 325, 326, 333, 337, 340, 341, 344, 345, 346,
                                        347, 350, 352, 354, 357, 359, 360, 361, 362, 363, 368, 375, 381, 383, 384, 385,
                                        386, 390, 391, 394, 395, 398, 399, 401, 404, 405, 407, 408, 409, 410, 411, 413,
                                        414, 417, 418, 419, 421, 424, 425, 427, 433, 434, 435, 437, 441, 445, 446, 1140,
                                        1495, 1590, 1646, 1707, 1872, 2201, 2844, 2866};

    sorted_array arr;
    for(auto id: ids) {
        arr.append(id);
    }

    uint32_t* indices = new uint32_t[filter_ids.size()];
    arr.indexOf(&filter_ids[0], filter_ids.size(), indices);

    ASSERT_EQ(57, filter_ids[25]);
    ASSERT_EQ(4, indices[25]);

    ASSERT_EQ(60, filter_ids[27]);
    ASSERT_EQ(5, indices[27]);

    ASSERT_EQ(232, filter_ids[135]);
    ASSERT_EQ(8, indices[135]);

    delete [] indices;
    indices = nullptr;

    ids = {4,5,6,7,8};
    filter_ids = {1,2,3,4,6,7,8,9,10};

    sorted_array arr2;
    for(auto id: ids) {
        arr2.append(id);
    }

    indices = new uint32_t[filter_ids.size()];
    arr2.indexOf(&filter_ids[0], filter_ids.size(), indices);

    ASSERT_EQ(4, filter_ids[3]);
    ASSERT_EQ(0, indices[3]);

    ASSERT_EQ(6, filter_ids[4]);
    ASSERT_EQ(2, indices[4]);

    ASSERT_EQ(7, filter_ids[5]);
    ASSERT_EQ(3, indices[5]);

    ASSERT_EQ(8, filter_ids[6]);
    ASSERT_EQ(4, indices[6]);

    delete [] indices;
}

TEST(SortedArrayTest, NumFoundOfSortedArrayGreaterThanValues) {
    std::vector<uint32_t> ids = {3, 44, 51, 54, 57, 60, 121, 136, 232, 238, 278, 447, 452, 454, 455, 456, 457, 459, 463,
                                 465, 471, 472, 473, 474, 475, 478, 479, 480, 486, 490, 492, 496, 503, 508, 510, 512,
                                 515, 526, 527, 533, 534, 537, 544, 547, 551, 553, 565, 573, 574, 577, 579, 617, 621,
                                 626, 628, 635, 653, 667, 672, 675, 689, 696, 705, 711, 714, 716, 725, 731, 735, 738,
                                 739, 747, 751, 758, 762, 773, 778, 786, 787, 801, 810, 817, 821, 826, 829, 835, 836,
                                 844, 852, 853, 854, 856, 860, 861, 895, 906, 952, 953, 955, 961, 966, 968, 985, 987,
                                 988, 994, 996, 999, 1005, 1007, 1027, 1030, 1034, 1037, 1040, 1041, 1043, 1057, 1060,
                                 1062, 1065, 1073, 1095, 1119, 1127, 1136, 1137, 1144, 1148, 1150, 1158, 1161, 1167,
                                 1168, 1170, 1182, 1191, 1223, 1229, 1241, 1247, 1279, 1282, 1287, 1290, 1293, 1302,
                                 1308, 1319, 1323, 1328, 1329, 1344, 1345, 1349, 1351, 1353, 1357, 1364, 1368, 1374,
                                 1386, 1389, 1405, 1411, 1421, 1423, 1424, 1439, 1442, 1449, 1452, 1453, 1455, 1458,
                                 1496, 1500, 1501, 1508, 1512, 1526, 1533, 1541, 1546, 1551, 1568, 1579, 1582, 1588,
                                 1589, 1604, 1656, 1658, 1662, 1667, 1682, 1699, 1704, 1714, 1719, 1723, 1728, 1736,
                                 1737, 1744, 1749, 1764, 1768, 1772, 1778, 1820, 1841, 1860, 1880, 1882, 1896, 1907,
                                 1921, 1949, 1959, 1988, 1990, 1995, 2006, 2027, 2090, 2097, 2110, 2130, 2135, 2138,
                                 2144, 2154, 2159, 2165, 2177, 2186, 2204, 2229, 2234, 2255, 2272, 2301, 2319, 2360,
                                 2372, 2383, 2400, 2404, 2441, 2444, 2447, 2460, 2464, 2477, 2480, 2490, 2497, 2513,
                                 2519, 2539, 2547, 2553, 2562, 2570, 2585, 2587, 2590, 2607, 2625, 2633, 2641, 2649,
                                 2650, 2679, 2680, 2698, 2699, 2752, 2782, 2788, 2818, 2829, 2834, 2885, 2892, 2926,
                                 2948, 2954, 2958, 3071, 3088, 3094, 3099, 3124, 3148, 3149, 3151, 3152, 3197, 3212,
                                 3250, 3256, 3269};

    std::vector<uint32_t> filter_ids = {9, 19, 21, 22, 23, 25, 26, 27, 28, 29, 30, 32, 33, 34, 35, 36, 38, 39, 41, 42,
                                        46, 47, 48, 49, 52, 57, 58, 60, 61, 63, 67, 68, 69, 71, 72, 74, 75, 76, 77, 78,
                                        79, 80, 85, 86, 87, 89, 91, 93, 94, 96, 100, 102, 103, 104, 106, 109, 111, 112,
                                        113, 114, 115, 117, 118, 119, 123, 124, 127, 128, 129, 132, 133, 134, 135, 139,
                                        141, 142, 143, 144, 146, 147, 148, 149, 151, 152, 154, 155, 157, 158, 159, 161,
                                        162, 163, 164, 169, 170, 172, 174, 178, 179, 180, 181, 182, 183, 184, 185, 186,
                                        187, 188, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 206,
                                        207, 211, 212, 213, 215, 217, 219, 222, 223, 225, 226, 229, 230, 232, 233, 234,
                                        237, 239, 240, 241, 243, 244, 245, 246, 247, 248, 256, 257, 261, 262, 263, 264,
                                        265, 266, 267, 269, 270, 271, 272, 273, 274, 275, 279, 280, 281, 282, 284, 286,
                                        288, 289, 291, 292, 296, 297, 298, 299, 303, 304, 305, 307, 308, 309, 310, 311,
                                        312, 313, 314, 317, 318, 320, 321, 325, 326, 333, 337, 340, 341, 344, 345, 346,
                                        347, 350, 352, 354, 357, 359, 360, 361, 362, 363, 368, 375, 381, 383, 384, 385,
                                        386, 390, 391, 394, 395, 398, 399, 401, 404, 405, 407, 408, 409, 410, 411, 413,
                                        414, 417, 418, 419, 421, 424, 425, 427, 433, 434, 435, 437, 441, 445, 446, 1140,
                                        1495, 1590, 1646, 1707, 1872, 2201, 2844, 2866};

    sorted_array arr;
    for(auto id: ids) {
        arr.append(id);
    }

    auto num_found = arr.numFoundOf(&filter_ids[0], filter_ids.size());
    ASSERT_EQ(3, num_found);

    filter_ids = {4,5,6,7,8};
    ids = {1,2,3,4,6,7,8,9,10};

    sorted_array arr2;
    for(auto id: ids) {
        arr2.append(id);
    }

    num_found = arr2.numFoundOf(&filter_ids[0], filter_ids.size());
    ASSERT_EQ(4, num_found);
}

TEST(SortedArrayTest, NumFoundOfSortedArrayLessThanValues) {
    std::vector<uint32_t> filter_ids = {3, 44, 51, 54, 57, 60, 121, 136, 232, 238, 278, 447, 452, 454, 455, 456, 457, 459, 463,
                                 465, 471, 472, 473, 474, 475, 478, 479, 480, 486, 490, 492, 496, 503, 508, 510, 512,
                                 515, 526, 527, 533, 534, 537, 544, 547, 551, 553, 565, 573, 574, 577, 579, 617, 621,
                                 626, 628, 635, 653, 667, 672, 675, 689, 696, 705, 711, 714, 716, 725, 731, 735, 738,
                                 739, 747, 751, 758, 762, 773, 778, 786, 787, 801, 810, 817, 821, 826, 829, 835, 836,
                                 844, 852, 853, 854, 856, 860, 861, 895, 906, 952, 953, 955, 961, 966, 968, 985, 987,
                                 988, 994, 996, 999, 1005, 1007, 1027, 1030, 1034, 1037, 1040, 1041, 1043, 1057, 1060,
                                 1062, 1065, 1073, 1095, 1119, 1127, 1136, 1137, 1144, 1148, 1150, 1158, 1161, 1167,
                                 1168, 1170, 1182, 1191, 1223, 1229, 1241, 1247, 1279, 1282, 1287, 1290, 1293, 1302,
                                 1308, 1319, 1323, 1328, 1329, 1344, 1345, 1349, 1351, 1353, 1357, 1364, 1368, 1374,
                                 1386, 1389, 1405, 1411, 1421, 1423, 1424, 1439, 1442, 1449, 1452, 1453, 1455, 1458,
                                 1496, 1500, 1501, 1508, 1512, 1526, 1533, 1541, 1546, 1551, 1568, 1579, 1582, 1588,
                                 1589, 1604, 1656, 1658, 1662, 1667, 1682, 1699, 1704, 1714, 1719, 1723, 1728, 1736,
                                 1737, 1744, 1749, 1764, 1768, 1772, 1778, 1820, 1841, 1860, 1880, 1882, 1896, 1907,
                                 1921, 1949, 1959, 1988, 1990, 1995, 2006, 2027, 2090, 2097, 2110, 2130, 2135, 2138,
                                 2144, 2154, 2159, 2165, 2177, 2186, 2204, 2229, 2234, 2255, 2272, 2301, 2319, 2360,
                                 2372, 2383, 2400, 2404, 2441, 2444, 2447, 2460, 2464, 2477, 2480, 2490, 2497, 2513,
                                 2519, 2539, 2547, 2553, 2562, 2570, 2585, 2587, 2590, 2607, 2625, 2633, 2641, 2649,
                                 2650, 2679, 2680, 2698, 2699, 2752, 2782, 2788, 2818, 2829, 2834, 2885, 2892, 2926,
                                 2948, 2954, 2958, 3071, 3088, 3094, 3099, 3124, 3148, 3149, 3151, 3152, 3197, 3212,
                                 3250, 3256, 3269};

    std::vector<uint32_t> ids = {9, 19, 21, 22, 23, 25, 26, 27, 28, 29, 30, 32, 33, 34, 35, 36, 38, 39, 41, 42,
                                46, 47, 48, 49, 52, 57, 58, 60, 61, 63, 67, 68, 69, 71, 72, 74, 75, 76, 77, 78,
                                79, 80, 85, 86, 87, 89, 91, 93, 94, 96, 100, 102, 103, 104, 106, 109, 111, 112,
                                113, 114, 115, 117, 118, 119, 123, 124, 127, 128, 129, 132, 133, 134, 135, 139,
                                141, 142, 143, 144, 146, 147, 148, 149, 151, 152, 154, 155, 157, 158, 159, 161,
                                162, 163, 164, 169, 170, 172, 174, 178, 179, 180, 181, 182, 183, 184, 185, 186,
                                187, 188, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 206,
                                207, 211, 212, 213, 215, 217, 219, 222, 223, 225, 226, 229, 230, 232, 233, 234,
                                237, 239, 240, 241, 243, 244, 245, 246, 247, 248, 256, 257, 261, 262, 263, 264,
                                265, 266, 267, 269, 270, 271, 272, 273, 274, 275, 279, 280, 281, 282, 284, 286,
                                288, 289, 291, 292, 296, 297, 298, 299, 303, 304, 305, 307, 308, 309, 310, 311,
                                312, 313, 314, 317, 318, 320, 321, 325, 326, 333, 337, 340, 341, 344, 345, 346,
                                347, 350, 352, 354, 357, 359, 360, 361, 362, 363, 368, 375, 381, 383, 384, 385,
                                386, 390, 391, 394, 395, 398, 399, 401, 404, 405, 407, 408, 409, 410, 411, 413,
                                414, 417, 418, 419, 421, 424, 425, 427, 433, 434, 435, 437, 441, 445, 446, 1140,
                                1495, 1590, 1646, 1707, 1872, 2201, 2844, 2866};

    sorted_array arr;
    for(auto id: ids) {
        arr.append(id);
    }

    auto num_found = arr.numFoundOf(&filter_ids[0], filter_ids.size());
    //ASSERT_EQ(3, num_found);

    ids = {4,5,6,7,8};
    filter_ids = {1,2,3,4,6,7,8,9,10};

    sorted_array arr2;
    for(auto id: ids) {
        arr2.append(id);
    }

    num_found = arr2.numFoundOf(&filter_ids[0], filter_ids.size());
    ASSERT_EQ(4, num_found);
}