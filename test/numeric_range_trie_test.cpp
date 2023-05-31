#include <gtest/gtest.h>
#include "numeric_range_trie_test.h"

class NumericRangeTrieTest : public ::testing::Test {
protected:

    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(NumericRangeTrieTest, SearchRange) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-8192, 8},
            {-16384, 32},
            {-24576, 35},
            {-32768, 43},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91}
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_range(32768, true, -32768, true, ids, ids_length);
    std::unique_ptr<uint32_t[]> ids_guard(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(-32768, true, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(pairs.size(), ids_length);
    for (uint32_t i = 0; i < pairs.size(); i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_range(-32768, true, 32768, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(pairs.size() - 1, ids_length);
    for (uint32_t i = 0; i < pairs.size() - 1; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_range(-32768, true, 134217728, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(pairs.size(), ids_length);
    for (uint32_t i = 0; i < pairs.size(); i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_range(-32768, true, 0, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 0; i < 4; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_range(-32768, true, 0, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 0; i < 4; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_range(-32768, false, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(pairs.size() - 1, ids_length);
    for (uint32_t i = 0, j = 0; i < pairs.size(); i++) {
        if (i == 3) continue; // id for -32768 would not be present
        ASSERT_EQ(pairs[i].second, ids[j++]);
    }

    trie->search_range(-134217728, true, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(pairs.size(), ids_length);
    for (uint32_t i = 0; i < pairs.size(); i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_range(-134217728, true, 134217728, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(pairs.size(), ids_length);
    for (uint32_t i = 0; i < pairs.size(); i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_range(-1, true, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < pairs.size(); i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_range(-1, false, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < pairs.size(); i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_range(-1, true, 0, true, ids, ids_length);
    ASSERT_EQ(0, ids_length);

    trie->search_range(-1, false, 0, false, ids, ids_length);
    ASSERT_EQ(0, ids_length);

    trie->search_range(8192, true, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < ids_length; i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_range(8192, true, 0x2000000, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < ids_length; i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_range(16384, true, 16384, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(1, ids_length);
    ASSERT_EQ(56, ids[0]);

    trie->search_range(16384, true, 16384, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(16384, false, 16384, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(16383, true, 16383, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(8193, true, 16383, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(-32768, true, -8192, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }
}

TEST_F(NumericRangeTrieTest, SearchGreater) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-8192, 8},
            {-16384, 32},
            {-24576, 35},
            {-32768, 43},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91}
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_greater_than(0, true, ids, ids_length);
    std::unique_ptr<uint32_t[]> ids_guard(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < pairs.size(); i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_greater_than(-1, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < pairs.size(); i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_greater_than(-1, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < pairs.size(); i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_greater_than(-24576, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(7, ids_length);
    for (uint32_t i = 0, j = 0; i < pairs.size(); i++) {
        if (i == 3) continue; // id for -32768 would not be present
        ASSERT_EQ(pairs[i].second, ids[j++]);
    }

    trie->search_greater_than(-32768, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(7, ids_length);
    for (uint32_t i = 0, j = 0; i < pairs.size(); i++) {
        if (i == 3) continue; // id for -32768 would not be present
        ASSERT_EQ(pairs[i].second, ids[j++]);
    }

    trie->search_greater_than(8192, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < pairs.size(); i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_greater_than(8192, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(3, ids_length);
    for (uint32_t i = 5, j = 0; i < pairs.size(); i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_greater_than(1000000, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_greater_than(-1000000, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(8, ids_length);
    for (uint32_t i = 0; i < pairs.size(); i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }
}

TEST_F(NumericRangeTrieTest, SearchLesser) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-32768, 8},
            {-24576, 32},
            {-16384, 35},
            {-8192, 43},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91}
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_less_than(0, true, ids, ids_length);
    std::unique_ptr<uint32_t[]> ids_guard(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 4, j = 0; i < ids_length; i++, j++) {
        ASSERT_EQ(pairs[i].second, ids[j]);
    }

    trie->search_less_than(0, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_less_than(-1, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_less_than(-16384, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(3, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_less_than(-16384, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(2, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_less_than(8192, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(5, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_less_than(8192, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    trie->search_less_than(-1000000, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_less_than(1000000, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(8, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }
}

TEST_F(NumericRangeTrieTest, MultivalueData) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-0x202020, 32},
            {-32768, 5},
            {-32768, 8},
            {-24576, 32},
            {-16384, 35},
            {-8192, 43},
            {0, 43},
            {0, 49},
            {1, 8},
            {256, 91},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91},
            {0x202020, 35},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_less_than(0, false, ids, ids_length);
    std::unique_ptr<uint32_t[]> ids_guard(ids);

    std::vector<uint32_t> expected = {5, 8, 32, 35, 43};

    ASSERT_EQ(5, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->search_less_than(-16380, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(4, ids_length);

    expected = {5, 8, 32, 35};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->search_less_than(16384, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(7, ids_length);

    expected = {5, 8, 32, 35, 43, 49, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->search_greater_than(0, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(7, ids_length);

    expected = {8, 35, 43, 49, 56, 58, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->search_greater_than(256, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(5, ids_length);

    expected = {35, 49, 56, 58, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->search_greater_than(-32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(9, ids_length);

    expected = {5, 8, 32, 35, 43, 49, 56, 58, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->search_range(-32768, true, 0, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(6, ids_length);

    expected = {5, 8, 32, 35, 43, 49};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }
}

TEST_F(NumericRangeTrieTest, EmptyTrieOperations) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_range(-32768, true, 32768, true, ids, ids_length);
    std::unique_ptr<uint32_t[]> ids_guard(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(-32768, true, -1, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(1, true, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_greater_than(0, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_greater_than(15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_greater_than(-15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_less_than(0, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_less_than(-15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_less_than(15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);
}
