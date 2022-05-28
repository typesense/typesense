#include "adi_tree.h"
#include <gtest/gtest.h>
#include "logger.h"
#include <fstream>

class ADITreeTest : public ::testing::Test {
protected:

    virtual void SetUp() {

    }

    virtual void TearDown() {

    }
};

TEST_F(ADITreeTest, BasicOps) {
    adi_tree_t tree;

    // operations on fresh tree
    ASSERT_EQ(INT64_MAX, tree.rank(100));
    tree.remove(100);

    tree.index(100, "f");
    ASSERT_EQ(1, tree.rank(100));

    tree.index(101, "e");
    ASSERT_EQ(2, tree.rank(100));
    ASSERT_EQ(1, tree.rank(101));

    tree.remove(101);
    ASSERT_EQ(1, tree.rank(100));

    tree.remove(100);
    ASSERT_EQ(INT64_MAX, tree.rank(100));
    ASSERT_EQ(INT64_MAX, tree.rank(101));
}

TEST_F(ADITreeTest, OverlappedString) {
    adi_tree_t tree;
    tree.index(1, "t");
    tree.index(2, "to");

    ASSERT_EQ(2, tree.rank(2));
    ASSERT_EQ(1, tree.rank(1));

    tree.remove(1);
    tree.remove(2);

    ASSERT_EQ(INT64_MAX, tree.rank(2));
    ASSERT_EQ(INT64_MAX, tree.rank(1));
}

TEST_F(ADITreeTest, OrderInsertedStrings) {
    std::vector<std::pair<uint32_t, std::string>> records = {
        {1, "alpha"}, {2, "beta"},
        {3, "foo"}, {4, "ant"}, {5, "foobar"},
        {6, "buzz"}
    };

    adi_tree_t tree;
    for(auto& record: records) {
        tree.index(record.first, record.second);
    }

    std::sort(records.begin(), records.end(),
              [](const std::pair<uint32_t, std::string>& a, const std::pair<uint32_t, std::string>& b) -> bool {
                  return a.second < b.second;
              });

    // alpha, ant, beta, buzz, foo, foobar
    ASSERT_EQ(1, tree.rank(1));
    ASSERT_EQ(3, tree.rank(2));
    ASSERT_EQ(5, tree.rank(3));
    ASSERT_EQ(2, tree.rank(4));
    ASSERT_EQ(6, tree.rank(5));
    ASSERT_EQ(4, tree.rank(6));

    // remove "foo"
    tree.remove(3);
    ASSERT_EQ(5, tree.rank(5));

    // remove "foobar"
    tree.remove(5);
    ASSERT_EQ(4, tree.rank(6));

    // remove "alpha"
    tree.remove(1);
    ASSERT_EQ(1, tree.rank(4));
    ASSERT_EQ(2, tree.rank(2));
    ASSERT_EQ(3, tree.rank(6));
}

TEST_F(ADITreeTest, InsertDuplicateAndDelete) {
    adi_tree_t tree;
    tree.index(100, "map");
    tree.index(101, "map");

    tree.remove(100);
    tree.remove(101);

    ASSERT_EQ(INT64_MAX, tree.rank(100));
    ASSERT_EQ(INT64_MAX, tree.rank(101));

    ASSERT_EQ(nullptr, tree.get_root());
}

TEST_F(ADITreeTest, InsertDeleteManyElements) {
    adi_tree_t tree;
    size_t num_elements = UINT16_MAX + 100;

    for(size_t i = 0; i < num_elements; i++) {
        tree.index(i, "key");
    }

    for(size_t i = 0; i < num_elements; i++) {
        tree.remove(i);
    }
}
