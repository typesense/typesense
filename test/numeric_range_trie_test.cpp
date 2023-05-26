#include <gtest/gtest.h>
#include "numeric_range_trie_test.h"

class NumericRangeTrieTest : public ::testing::Test {
protected:

    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(NumericRangeTrieTest, Insert) {
    auto trie = new NumericTrie();
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

    for (auto const pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_range(-32768, true, 32768, true, ids, ids_length);

    ASSERT_EQ(pairs.size(), ids_length);
    for (uint32_t i = 0; i < pairs.size(); i++) {
        ASSERT_EQ(pairs[i].second, ids[i]);
    }

    delete [] ids;
    delete trie;
}