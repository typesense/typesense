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

TEST(NumTreeTest, Iterator) {
    num_tree_t compact_tree;
    compact_tree.insert(-1200, 0);
    compact_tree.insert(-1750, 1);
    compact_tree.insert(0, 2);
    compact_tree.insert(100, 3);
    compact_tree.insert(2000, 4);

    compact_tree.insert(-1200, 5);
    compact_tree.insert(100, 6);

    auto iterator = num_tree_t::iterator_t(&compact_tree, EQUALS, 1);
    ASSERT_FALSE(iterator.is_valid);
    iterator.reset();
    ASSERT_FALSE(iterator.is_valid);

    iterator = num_tree_t::iterator_t(&compact_tree, GREATER_THAN_EQUALS, 0);
    ASSERT_FALSE(iterator.is_valid);
    iterator.reset();
    ASSERT_FALSE(iterator.is_valid);

    iterator = num_tree_t::iterator_t(&compact_tree, EQUALS, 0);
    std::vector<uint32_t> expected_ids = {2};

    for (const auto& id: expected_ids) {
        ASSERT_TRUE(iterator.is_valid);
        ASSERT_EQ(id, iterator.seq_id);
        iterator.next();
    }
    ASSERT_FALSE(iterator.is_valid);
    iterator.reset();
    for (const auto& id: expected_ids) {
        ASSERT_TRUE(iterator.is_valid);
        ASSERT_EQ(id, iterator.seq_id);
        iterator.next();
    }
    ASSERT_FALSE(iterator.is_valid);

    iterator = num_tree_t::iterator_t(&compact_tree, EQUALS, -1200);
    expected_ids = {0, 5};

    for (const auto& id: expected_ids) {
        ASSERT_TRUE(iterator.is_valid);
        ASSERT_EQ(id, iterator.seq_id);
        iterator.next();
    }
    ASSERT_FALSE(iterator.is_valid);
    iterator.reset();
    for (const auto& id: expected_ids) {
        ASSERT_TRUE(iterator.is_valid);
        ASSERT_EQ(id, iterator.seq_id);
        iterator.next();
    }
    ASSERT_FALSE(iterator.is_valid);

    iterator.reset();
    iterator.skip_to(1);
    ASSERT_TRUE(iterator.is_valid);
    ASSERT_EQ(5, iterator.seq_id);

    iterator.skip_to(10);
    ASSERT_FALSE(iterator.is_valid);

    num_tree_t tree;
    for (uint32_t i = 0; i < 100; i++) {
        tree.insert(1, i);
    }

    iterator = num_tree_t::iterator_t(&tree, EQUALS, 1);
    expected_ids = {};
    for (uint32_t i = 0; i < 100; i++) {
        expected_ids.push_back(i);
    }

    for (const auto& id: expected_ids) {
        ASSERT_TRUE(iterator.is_valid);
        ASSERT_EQ(id, iterator.seq_id);
        iterator.next();
    }
    ASSERT_FALSE(iterator.is_valid);
    iterator.reset();
    for (const auto& id: expected_ids) {
        ASSERT_TRUE(iterator.is_valid);
        ASSERT_EQ(id, iterator.seq_id);
        iterator.next();
    }
    ASSERT_FALSE(iterator.is_valid);

    iterator.reset();
    iterator.skip_to(50);
    ASSERT_TRUE(iterator.is_valid);
    ASSERT_EQ(50, iterator.seq_id);
    iterator.skip_to(100);
    ASSERT_FALSE(iterator.is_valid);
}
