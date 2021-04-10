#include <gtest/gtest.h>
#include <art.h>
#include "num_tree.h"

TEST(NumTreeTest, Searches) {
    num_tree_t tree;
    tree.insert(-1200, 0);
    tree.insert(-1750, 1);
    tree.insert(0, 2);
    tree.insert(100, 3);
    tree.insert(2000, 4);

    tree.insert(-1200, 5);
    tree.insert(100, 6);

    uint32_t* ids;
    size_t ids_len;

    tree.search(NUM_COMPARATOR::EQUALS, -1750, &ids, ids_len);
    ASSERT_EQ(1, ids_len);
    ASSERT_EQ(1, ids[0]);
    delete [] ids;
    ids = nullptr;

    tree.search(NUM_COMPARATOR::GREATER_THAN_EQUALS, -1200, &ids, ids_len);
    ASSERT_EQ(6, ids_len);
    delete [] ids;
    ids = nullptr;

    tree.search(NUM_COMPARATOR::GREATER_THAN, -1200, &ids, ids_len);
    ASSERT_EQ(4, ids_len);
    delete [] ids;
    ids = nullptr;

    tree.search(NUM_COMPARATOR::LESS_THAN_EQUALS, 100, &ids, ids_len);
    ASSERT_EQ(6, ids_len);
    delete [] ids;
    ids = nullptr;

    tree.search(NUM_COMPARATOR::LESS_THAN, 100, &ids, ids_len);
    ASSERT_EQ(4, ids_len);
    delete [] ids;
    ids = nullptr;
}
