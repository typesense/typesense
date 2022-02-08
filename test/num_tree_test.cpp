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

    uint32_t* ids = nullptr;
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

TEST(NumTreeTest, EraseFullList) {
    num_tree_t tree;

    // this stores the IDs as a full list
    for(size_t i = 0; i < 200; i++) {
        tree.insert(0, i);
    }

    // we erase all but 1 ID
    for(size_t i = 0; i < 199; i++) {
        tree.remove(0, i);
    }

    // now try searching for the value
    uint32_t* ids = nullptr;
    size_t ids_len;

    tree.search(NUM_COMPARATOR::EQUALS, 0, &ids, ids_len);
    ASSERT_EQ(1, ids_len);
    ASSERT_EQ(199, ids[0]);
    delete [] ids;
    ids = nullptr;

    // deleting the last ID as well
    tree.remove(0, 199);

    tree.search(NUM_COMPARATOR::EQUALS, 0, &ids, ids_len);
    ASSERT_EQ(nullptr, ids);
}
