#include <gtest/gtest.h>
#include "array_utils.h"
#include "logger.h"

TEST(SortedArrayTest, AndScalar) {
    const size_t size1 = 9;
    uint32_t *arr1 = new uint32_t[size1];
    for(size_t i = 0; i < size1; i++) {
        arr1[i] = i;
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
    uint32_t *results = new uint32_t[std::max(size1, size2)];
    uint32_t results_size = ArrayUtils::and_scalar(arr1, size1, arr2, arr2_len, &results);
    ASSERT_EQ(2, results_size);

    std::vector<uint32_t> expected = {3, 6};

    for(size_t i = 0; i < results_size; i++) {
        ASSERT_EQ(expected[i], results[i]);
    }

    delete [] results;
    delete [] arr1;
    delete [] arr2;
}

TEST(SortedArrayTest, OrScalarMergeShouldRemoveDuplicates) {
    const size_t size1 = 9;
    uint32_t *arr1 = new uint32_t[size1];
    for(size_t i = 0; i < size1; i++) {
        arr1[i] = i;
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
    uint32_t *results = nullptr;
    uint32_t results_size = ArrayUtils::or_scalar(arr1, size1, arr2, arr2_len, &results);
    ASSERT_EQ(10, results_size);

    std::vector<uint32_t> expected = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    for(size_t i = 0; i < results_size; i++) {
        ASSERT_EQ(expected[i], results[i]);
    }

    delete[] results;
    delete[] arr1;
    delete[] arr2;
}

TEST(SortedArrayTest, OrScalarMergeShouldRemoveDuplicatesAtBoundary) {
    const size_t size1 = 9;
    uint32_t *arr1 = new uint32_t[size1];
    for(auto i = 0; i < 9; i++) {
        arr1[i] = i;
    }

    std::vector<uint32_t> vec2 = {0, 4, 5};
    uint32_t *arr2 = new uint32_t[vec2.size()];
    auto j = 0;
    for(auto i: vec2) {
        arr2[j++] = i;
    }

    uint32_t *results = nullptr;
    uint32_t results_size = ArrayUtils::or_scalar(arr1, size1, arr2, vec2.size(), &results);
    ASSERT_EQ(9, results_size);

    std::vector<uint32_t> expected = {0, 1, 2, 3, 4, 5, 6, 7, 8};

    for(size_t i = 0; i < results_size; i++) {
        ASSERT_EQ(expected[i], results[i]);
    }

    delete[] results;
    delete[] arr1;
    delete[] arr2;
}

TEST(SortedArrayTest, OrScalarWithEitherArrayAsNull) {
    const size_t size1 = 9;
    uint32_t *arr1 = new uint32_t[size1];
    for(auto i = 0; i < 9; i++) {
        arr1[i] = i;
    }

    uint32_t *results = nullptr;
    uint32_t results_size = ArrayUtils::or_scalar(arr1, size1, nullptr, 0, &results);
    ASSERT_EQ(9, results_size);

    delete[] results;
    results = nullptr;

    results_size = ArrayUtils::or_scalar(nullptr, 0, arr1, size1, &results);
    ASSERT_EQ(9, results_size);

    delete[] results;
    results = nullptr;
}

TEST(SortedArrayTest, FilterArray) {
    const size_t size1 = 9;
    uint32_t *arr1 = new uint32_t[size1];
    for(auto i = 0; i < 9; i++) {
        arr1[i] = i;
    }

    std::vector<uint32_t> vec2 = {0, 1, 5, 7, 8};
    uint32_t *arr2 = new uint32_t[vec2.size()];
    auto j = 0;
    for(auto i: vec2) {
        arr2[j++] = i;
    }

    uint32_t *results = nullptr;
    uint32_t results_size = ArrayUtils::exclude_scalar(arr1, size1, arr2, vec2.size(), &results);
    ASSERT_EQ(4, results_size);

    std::vector<uint32_t> expected = {2, 3, 4, 6};

    for(size_t i = 0; i < results_size; i++) {
        ASSERT_EQ(expected[i], results[i]);
    }

    delete[] arr2;
    delete[] results;

    vec2 = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    arr2 = new uint32_t[vec2.size()];

    j = 0;
    for(auto i: vec2) {
        arr2[j++] = i;
    }

    results = nullptr;
    results_size = ArrayUtils::exclude_scalar(arr1, size1, arr2, vec2.size(), &results);
    ASSERT_EQ(0, results_size);

    // on a larger array
    results = nullptr;
    
    std::vector<uint32_t> vec3 = {58, 118, 185, 260, 322, 334, 353};
    std::vector<uint32_t> filter_ids = {58, 103, 116, 117, 137, 154, 191, 210, 211, 284, 299, 302, 306, 309, 332, 334, 360};
    std::vector<uint32_t> expected_res = {118, 185, 260, 322, 353};

    results_size = ArrayUtils::exclude_scalar(&vec3[0], vec3.size(), &filter_ids[0], filter_ids.size(), &results);
    ASSERT_EQ(expected_res.size(), results_size);
    
    for(size_t i=0; i<expected_res.size(); i++) {
        ASSERT_EQ(expected_res[i], results[i]);
    }

    delete[] arr2;
    delete[] arr1;
    delete[] results;
}

TEST(SortedArrayTest, SkipToID) {
    std::vector<uint32_t> array;
    for (uint32_t i = 0; i < 10; i++) {
        array.push_back(i * 3);
    }

    uint32_t index = 0;
    bool found = ArrayUtils::skip_index_to_id(index, array.data(), array.size(), 15);
    ASSERT_TRUE(found);
    ASSERT_EQ(5, index);

    index = 4;
    found = ArrayUtils::skip_index_to_id(index, array.data(), array.size(), 3);
    ASSERT_FALSE(found);
    ASSERT_EQ(4, index);

    index = 4;
    found = ArrayUtils::skip_index_to_id(index, array.data(), array.size(), 12);
    ASSERT_TRUE(found);
    ASSERT_EQ(4, index);

    index = 4;
    found = ArrayUtils::skip_index_to_id(index, array.data(), array.size(), 24);
    ASSERT_TRUE(found);
    ASSERT_EQ(8, index);

    index = 4;
    found = ArrayUtils::skip_index_to_id(index, array.data(), array.size(), 25);
    ASSERT_FALSE(found);
    ASSERT_EQ(9, index);

    index = 4;
    found = ArrayUtils::skip_index_to_id(index, array.data(), array.size(), 30);
    ASSERT_FALSE(found);
    ASSERT_EQ(10, index);

    index = 12;
    found = ArrayUtils::skip_index_to_id(index, array.data(), array.size(), 30);
    ASSERT_FALSE(found);
    ASSERT_EQ(12, index);
}