#include <gtest/gtest.h>
#include "array_utils.h"

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

    delete[] arr2;
    delete[] arr1;
    delete[] results;
}