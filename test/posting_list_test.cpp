#include <gtest/gtest.h>
#include "posting.h"
#include "array_utils.h"
#include <chrono>
#include <vector>

class PostingListTest : public ::testing::Test {
protected:
    ThreadPool* pool;

    virtual void SetUp() {
        pool = new ThreadPool(4);
    }

    virtual void TearDown() {
        pool->shutdown();
        delete pool;
    }
};

TEST_F(PostingListTest, Insert) {
    std::vector<uint32_t> offsets = {0, 1, 3};

    posting_list_t pl(5);

    // insert elements sequentially

    for(size_t i = 0; i < 15; i++) {
        pl.upsert(i, offsets);
    }

    posting_list_t::block_t* root = pl.get_root();
    ASSERT_EQ(5, root->ids.getLength());
    ASSERT_EQ(5, root->next->ids.getLength());
    ASSERT_EQ(5, root->next->next->ids.getLength());

    ASSERT_EQ(root->next->next->next, nullptr);

    ASSERT_EQ(3, pl.num_blocks());
    ASSERT_EQ(15, pl.num_ids());
    ASSERT_EQ(root, pl.block_of(4));
    ASSERT_EQ(root->next, pl.block_of(9));
    ASSERT_EQ(root->next->next, pl.block_of(14));

    // insert alternate values

    posting_list_t pl2(5);

    for(size_t i = 0; i < 15; i+=2) {
        // [0, 2, 4, 6, 8], [10, 12, 14]
        pl2.upsert(i, offsets);
    }

    root = pl2.get_root();
    ASSERT_EQ(5, root->ids.getLength());
    ASSERT_EQ(3, root->next->ids.getLength());

    ASSERT_EQ(root->next->next, nullptr);
    ASSERT_EQ(2, pl2.num_blocks());
    ASSERT_EQ(8, pl2.num_ids());

    ASSERT_EQ(root, pl2.block_of(8));
    ASSERT_EQ(root->next, pl2.block_of(14));

    // insert in the middle
    // case 1

    posting_list_t pl3(5);

    for(size_t i = 0; i < 5; i++) {
        pl3.upsert(i, offsets);
    }

    pl3.upsert(6, offsets);
    pl3.upsert(8, offsets);
    pl3.upsert(9, offsets);
    pl3.upsert(10, offsets);
    pl3.upsert(12, offsets);
    ASSERT_EQ(10, pl3.num_ids());

    // [0,1,2,3,4], [6,8,9,10,12]
    pl3.upsert(5, offsets);
    ASSERT_EQ(3, pl3.num_blocks());
    ASSERT_EQ(11, pl3.num_ids());
    ASSERT_EQ(5, pl3.get_root()->ids.getLength());
    ASSERT_EQ(3, pl3.get_root()->next->ids.getLength());
    ASSERT_EQ(8, pl3.get_root()->next->ids.last());
    ASSERT_EQ(3, pl3.get_root()->next->next->ids.getLength());
    ASSERT_EQ(12, pl3.get_root()->next->next->ids.last());

    for(size_t i = 0; i < pl3.get_root()->next->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl3.get_root()->next->offset_index.at(i));
    }

    for(size_t i = 0; i < pl3.get_root()->next->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl3.get_root()->next->offsets.at(i));
    }

    // case 2
    posting_list_t pl4(5);

    for(size_t i = 0; i < 5; i++) {
        pl4.upsert(i, offsets);
    }

    pl4.upsert(6, offsets);
    pl4.upsert(8, offsets);
    pl4.upsert(9, offsets);
    pl4.upsert(10, offsets);
    pl4.upsert(12, offsets);

    // [0,1,2,3,4], [6,8,9,10,12]
    pl4.upsert(11, offsets);
    ASSERT_EQ(3, pl4.num_blocks());
    ASSERT_EQ(11, pl4.num_ids());

    ASSERT_EQ(5, pl4.get_root()->ids.getLength());
    ASSERT_EQ(3, pl4.get_root()->next->ids.getLength());
    ASSERT_EQ(9, pl4.get_root()->next->ids.last());
    ASSERT_EQ(3, pl4.get_root()->next->next->ids.getLength());
    ASSERT_EQ(12, pl4.get_root()->next->next->ids.last());

    for(size_t i = 0; i < pl4.get_root()->next->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl4.get_root()->next->offset_index.at(i));
    }

    for(size_t i = 0; i < pl4.get_root()->next->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl4.get_root()->next->offsets.at(i));
    }
}

TEST_F(PostingListTest, InsertInMiddle) {
    posting_list_t pl(3);

    pl.upsert(1, {1});
    pl.upsert(3, {3});
    pl.upsert(2, {2});

    ASSERT_EQ(1, pl.get_root()->ids.at(0));
    ASSERT_EQ(2, pl.get_root()->ids.at(1));
    ASSERT_EQ(3, pl.get_root()->ids.at(2));

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(1, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(2, pl.get_root()->offset_index.at(2));

    ASSERT_EQ(1, pl.get_root()->offsets.at(0));
    ASSERT_EQ(2, pl.get_root()->offsets.at(1));
    ASSERT_EQ(3, pl.get_root()->offsets.at(2));
}

TEST_F(PostingListTest, InplaceUpserts) {
    std::vector<uint32_t> offsets = {1, 2, 3};
    posting_list_t pl(5);

    pl.upsert(2, offsets);
    pl.upsert(5, offsets);
    pl.upsert(7, offsets);

    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(9, pl.get_root()->offsets.getLength());

    // update starting ID with same length of offsets
    pl.upsert(2, {1, 2, 4});
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(9, pl.get_root()->offsets.getLength());

    ASSERT_EQ(1, pl.get_root()->offsets.at(0));
    ASSERT_EQ(2, pl.get_root()->offsets.at(1));
    ASSERT_EQ(4, pl.get_root()->offsets.at(2));
    ASSERT_EQ(4, pl.get_root()->offsets.getMax());
    ASSERT_EQ(1, pl.get_root()->offsets.getMin());

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(3, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(6, pl.get_root()->offset_index.at(2));

    // update starting ID with smaller number of offsets
    pl.upsert(2, {5, 7});
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(8, pl.get_root()->offsets.getLength());

    ASSERT_EQ(5, pl.get_root()->offsets.at(0));
    ASSERT_EQ(7, pl.get_root()->offsets.at(1));
    ASSERT_EQ(1, pl.get_root()->offsets.at(2));
    ASSERT_EQ(7, pl.get_root()->offsets.getMax());
    ASSERT_EQ(1, pl.get_root()->offsets.getMin());

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(2, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(5, pl.get_root()->offset_index.at(2));

    // update starting ID with larger number of offsets
    pl.upsert(2, {0, 2, 8});
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(9, pl.get_root()->offsets.getLength());

    ASSERT_EQ(0, pl.get_root()->offsets.at(0));
    ASSERT_EQ(2, pl.get_root()->offsets.at(1));
    ASSERT_EQ(8, pl.get_root()->offsets.at(2));
    ASSERT_EQ(1, pl.get_root()->offsets.at(3));
    ASSERT_EQ(8, pl.get_root()->offsets.getMax());
    ASSERT_EQ(0, pl.get_root()->offsets.getMin());

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(3, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(6, pl.get_root()->offset_index.at(2));

    // update middle ID with smaller number of offsets
    pl.upsert(5, {1, 10});
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(8, pl.get_root()->offsets.getLength());

    ASSERT_EQ(0, pl.get_root()->offsets.at(0));
    ASSERT_EQ(2, pl.get_root()->offsets.at(1));
    ASSERT_EQ(8, pl.get_root()->offsets.at(2));
    ASSERT_EQ(1, pl.get_root()->offsets.at(3));
    ASSERT_EQ(10, pl.get_root()->offsets.at(4));

    ASSERT_EQ(10, pl.get_root()->offsets.getMax());
    ASSERT_EQ(0, pl.get_root()->offsets.getMin());

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(3, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(5, pl.get_root()->offset_index.at(2));

    // update middle ID with larger number of offsets
    pl.upsert(5, {2, 4, 12});
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(9, pl.get_root()->offsets.getLength());

    ASSERT_EQ(0, pl.get_root()->offsets.at(0));
    ASSERT_EQ(2, pl.get_root()->offsets.at(1));
    ASSERT_EQ(8, pl.get_root()->offsets.at(2));
    ASSERT_EQ(2, pl.get_root()->offsets.at(3));
    ASSERT_EQ(4, pl.get_root()->offsets.at(4));
    ASSERT_EQ(12, pl.get_root()->offsets.at(5));
    ASSERT_EQ(1, pl.get_root()->offsets.at(6));
    ASSERT_EQ(2, pl.get_root()->offsets.at(7));
    ASSERT_EQ(3, pl.get_root()->offsets.at(8));

    ASSERT_EQ(12, pl.get_root()->offsets.getMax());
    ASSERT_EQ(0, pl.get_root()->offsets.getMin());

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(3, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(6, pl.get_root()->offset_index.at(2));

    // update last ID with smaller number of offsets

    pl.upsert(7, {3});
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(7, pl.get_root()->offsets.getLength());

    ASSERT_EQ(0, pl.get_root()->offsets.at(0));
    ASSERT_EQ(2, pl.get_root()->offsets.at(1));
    ASSERT_EQ(8, pl.get_root()->offsets.at(2));
    ASSERT_EQ(2, pl.get_root()->offsets.at(3));
    ASSERT_EQ(4, pl.get_root()->offsets.at(4));
    ASSERT_EQ(12, pl.get_root()->offsets.at(5));
    ASSERT_EQ(3, pl.get_root()->offsets.at(6));

    ASSERT_EQ(12, pl.get_root()->offsets.getMax());
    ASSERT_EQ(0, pl.get_root()->offsets.getMin());

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(3, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(6, pl.get_root()->offset_index.at(2));

    // update last ID with larger number of offsets

    pl.upsert(7, {5, 20});
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(3, pl.num_ids());
    ASSERT_EQ(3, pl.get_root()->ids.getLength());
    ASSERT_EQ(8, pl.get_root()->offsets.getLength());

    ASSERT_EQ(0, pl.get_root()->offsets.at(0));
    ASSERT_EQ(2, pl.get_root()->offsets.at(1));
    ASSERT_EQ(8, pl.get_root()->offsets.at(2));
    ASSERT_EQ(2, pl.get_root()->offsets.at(3));
    ASSERT_EQ(4, pl.get_root()->offsets.at(4));
    ASSERT_EQ(12, pl.get_root()->offsets.at(5));
    ASSERT_EQ(5, pl.get_root()->offsets.at(6));
    ASSERT_EQ(20, pl.get_root()->offsets.at(7));

    ASSERT_EQ(20, pl.get_root()->offsets.getMax());
    ASSERT_EQ(0, pl.get_root()->offsets.getMin());

    ASSERT_EQ(0, pl.get_root()->offset_index.at(0));
    ASSERT_EQ(3, pl.get_root()->offset_index.at(1));
    ASSERT_EQ(6, pl.get_root()->offset_index.at(2));
}

TEST_F(PostingListTest, RemovalsOnFirstBlock) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    ASSERT_EQ(0, pl.num_blocks());
    ASSERT_EQ(0, pl.num_ids());

    // try to erase when posting list is empty
    pl.erase(0);
    ASSERT_FALSE(pl.contains(0));

    ASSERT_EQ(0, pl.num_ids());
    ASSERT_EQ(0, pl.num_blocks());

    // insert a single element and erase it
    pl.upsert(0, offsets);
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(1, pl.num_ids());
    pl.erase(0);
    ASSERT_EQ(0, pl.num_blocks());
    ASSERT_EQ(0, pl.num_ids());

    ASSERT_EQ(0, pl.get_root()->ids.getLength());
    ASSERT_EQ(0, pl.get_root()->offset_index.getLength());
    ASSERT_EQ(0, pl.get_root()->offsets.getLength());

    // insert until one past max block size
    for(size_t i = 0; i < 6; i++) {
        pl.upsert(i, offsets);
    }

    ASSERT_EQ(2, pl.num_blocks());
    ASSERT_EQ(6, pl.num_ids());

    ASSERT_TRUE(pl.contains(2));
    ASSERT_TRUE(pl.contains(5));
    ASSERT_FALSE(pl.contains(6));
    ASSERT_FALSE(pl.contains(1000));

    // delete non-existing element
    pl.erase(1000);
    ASSERT_EQ(6, pl.num_ids());

    // delete elements from first block: blocks should not be merged until it falls below 50% occupancy
    pl.erase(1);
    ASSERT_EQ(2, pl.num_blocks());
    ASSERT_EQ(5, pl.num_ids());

    // [0, 2, 3, 4], [5]

    for(size_t i = 0; i < pl.get_root()->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->offsets.at(i));
    }

    pl.erase(2);
    ASSERT_EQ(2, pl.num_blocks());
    pl.erase(3);
    ASSERT_EQ(3, pl.num_ids());

    // [0, 4], [5]
    ASSERT_EQ(2, pl.num_blocks());
    ASSERT_EQ(2, pl.get_root()->size());
    ASSERT_EQ(1, pl.get_root()->next->size());
    ASSERT_EQ(pl.get_root(), pl.block_of(4));
    ASSERT_EQ(pl.get_root()->next, pl.block_of(5));

    for(size_t i = 0; i < pl.get_root()->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->offsets.at(i));
    }

    pl.erase(4);  // this will trigger the merge

    // [0, 5]
    // ensure that merge has happened
    ASSERT_EQ(2, pl.num_ids());
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(pl.get_root(), pl.block_of(5));
    ASSERT_EQ(nullptr, pl.get_root()->next);
    ASSERT_EQ(2, pl.get_root()->size());

    for(size_t i = 0; i < pl.get_root()->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->offsets.at(i));
    }
}

TEST_F(PostingListTest, RemovalsOnLaterBlocks) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    // insert until one past max block size
    for(size_t i = 0; i < 6; i++) {
        pl.upsert(i, offsets);
    }

    // erase last element of last, non-first block

    pl.erase(5);
    ASSERT_EQ(5, pl.num_ids());
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(5, pl.get_root()->size());
    ASSERT_EQ(4, pl.get_root()->ids.last());
    ASSERT_EQ(nullptr, pl.get_root()->next);

    for(size_t i = 0; i < pl.get_root()->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->offsets.at(i));
    }

    // erase last element of the only block when block is atleast half full
    pl.erase(4);
    ASSERT_EQ(4, pl.num_ids());
    ASSERT_EQ(1, pl.num_blocks());
    ASSERT_EQ(4, pl.get_root()->size());
    ASSERT_EQ(3, pl.get_root()->ids.last());
    ASSERT_EQ(pl.get_root(), pl.block_of(3));

    for(size_t i = 4; i < 15; i++) {
        pl.upsert(i, offsets);
    }

    // [0..4], [5..9], [10..14]
    pl.erase(5);
    pl.erase(6);
    pl.erase(7);

    ASSERT_EQ(12, pl.num_ids());

    for(size_t i = 0; i < pl.get_root()->next->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->next->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->next->offsets.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->next->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->next->next->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->next->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->next->next->offsets.at(i));
    }

    // only part of the next node contents can be moved over when we delete 8 since (1 + 5) > 5
    pl.erase(8);

    // [0..4], [9], [10..14] => [0..4], [9,10,11], [12,13,14]

    ASSERT_EQ(3, pl.num_blocks());
    ASSERT_EQ(11, pl.num_ids());
    ASSERT_EQ(3, pl.get_root()->next->size());
    ASSERT_EQ(3, pl.get_root()->next->next->size());
    ASSERT_EQ(11, pl.get_root()->next->ids.last());
    ASSERT_EQ(14, pl.get_root()->next->next->ids.last());

    for(size_t i = 0; i < pl.get_root()->next->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->next->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->next->offsets.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->next->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->next->next->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->next->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->next->next->offsets.at(i));
    }
}

TEST_F(PostingListTest, OutOfOrderUpserts) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    for(int i = 5; i > 0; i--) {
        pl.upsert(i, offsets);
    }

    pl.upsert(0, offsets);
    pl.upsert(200000, offsets);

    ASSERT_EQ(2, pl.num_blocks());

    ASSERT_EQ(3, pl.get_root()->size());
    ASSERT_EQ(4, pl.get_root()->next->size());

    for(size_t i = 0; i < pl.get_root()->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->offsets.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->next->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->next->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->next->offsets.at(i));
    }
}

TEST_F(PostingListTest, RandomInsertAndDeletes) {
    time_t t;
    srand((unsigned) time(&t));

    posting_list_t pl(100);
    std::vector<uint32_t> offsets1 = {0, 1, 3};
    std::vector<uint32_t> offsets2 = {10, 12};

    std::vector<uint32_t> ids;

    for(size_t i = 0; i < 100000; i++) {
        ids.push_back(rand() % 100000);
    }

    size_t index = 0;

    for(auto id: ids) {
        const std::vector<uint32_t>& offsets = (index % 2 == 0) ? offsets1 : offsets2;
        pl.upsert(id, offsets);
        index++;
    }

    for(size_t i = 0; i < 10000; i++) {
        pl.erase(rand() % 100000);
    }

    ASSERT_GT(pl.num_blocks(), 750);
    ASSERT_LT(pl.num_blocks(), 1000);
}

TEST_F(PostingListTest, MergeBasics) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    std::vector<posting_list_t*> lists;

    // [0, 2] [3, 20]
    // [1, 3], [5, 10], [20]
    // [2, 3], [5, 7], [20]

    posting_list_t p1(2);
    p1.upsert(0, offsets);
    p1.upsert(2, offsets);
    p1.upsert(3, offsets);
    p1.upsert(20, offsets);

    posting_list_t p2(2);
    p2.upsert(1, offsets);
    p2.upsert(3, offsets);
    p2.upsert(5, offsets);
    p2.upsert(10, offsets);
    p2.upsert(20, offsets);

    posting_list_t p3(2);
    p3.upsert(2, offsets);
    p3.upsert(3, offsets);
    p3.upsert(5, offsets);
    p3.upsert(7, offsets);
    p3.upsert(20, offsets);

    lists.push_back(&p1);
    lists.push_back(&p2);
    lists.push_back(&p3);

    std::vector<uint32_t> result_ids;

    posting_list_t::merge(lists, result_ids);

    std::vector<uint32_t> expected_ids = {0, 1, 2, 3, 5, 7, 10, 20};
    ASSERT_EQ(expected_ids.size(), result_ids.size());

    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], result_ids[i]);
    }
}

TEST_F(PostingListTest, IntersectionBasics) {
    std::vector<uint32_t> offsets = {0, 1, 3};

    // [0, 2] [3, 20]
    // [1, 3], [5, 10], [20]
    // [2, 3], [5, 7], [20]

    posting_list_t p1(2);
    p1.upsert(0, offsets);
    p1.upsert(2, offsets);
    p1.upsert(3, offsets);
    p1.upsert(20, offsets);

    posting_list_t p2(2);
    p2.upsert(1, offsets);
    p2.upsert(3, offsets);
    p2.upsert(5, offsets);
    p2.upsert(10, offsets);
    p2.upsert(20, offsets);

    posting_list_t p3(2);
    p3.upsert(2, offsets);
    p3.upsert(3, offsets);
    p3.upsert(5, offsets);
    p3.upsert(7, offsets);
    p3.upsert(20, offsets);

    std::vector<void*> raw_lists = {&p1, &p2, &p3};
    std::vector<posting_list_t*> posting_lists = {&p1, &p2, &p3};
    std::vector<uint32_t> result_ids;
    std::mutex vecm;

    posting_list_t::intersect(posting_lists, result_ids);

    ASSERT_EQ(2, result_ids.size());
    ASSERT_EQ(3, result_ids[0]);
    ASSERT_EQ(20, result_ids[1]);

    std::vector<posting_list_t::iterator_t> its;
    result_iter_state_t iter_state;
    result_ids.clear();

    posting_t::block_intersector_t(raw_lists, iter_state).intersect([&](auto id, auto& its){
        std::unique_lock lk(vecm);
        result_ids.push_back(id);
    });

    std::sort(result_ids.begin(), result_ids.end());

    ASSERT_EQ(2, result_ids.size());
    ASSERT_EQ(3, result_ids[0]);
    ASSERT_EQ(20, result_ids[1]);

    // single item itersection
    std::vector<posting_list_t*> single_item_list = {&p1};
    result_ids.clear();
    posting_list_t::intersect(single_item_list, result_ids);
    std::vector<uint32_t> expected_ids = {0, 2, 3, 20};
    ASSERT_EQ(expected_ids.size(), result_ids.size());

    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], result_ids[i]);
    }

    result_iter_state_t iter_state2;
    result_ids.clear();
    raw_lists = {&p1};

    posting_t::block_intersector_t(raw_lists, iter_state2).intersect([&](auto id, auto& its){
        std::unique_lock lk(vecm);
        result_ids.push_back(id);
    });

    std::sort(result_ids.begin(), result_ids.end());  // because of concurrent intersection order is not guaranteed

    ASSERT_EQ(4, result_ids.size());
    ASSERT_EQ(0, result_ids[0]);
    ASSERT_EQ(2, result_ids[1]);
    ASSERT_EQ(3, result_ids[2]);
    ASSERT_EQ(20, result_ids[3]);

    // empty intersection list
    std::vector<posting_list_t*> empty_list;
    result_ids.clear();
    posting_list_t::intersect(empty_list, result_ids);
    ASSERT_EQ(0, result_ids.size());

    result_iter_state_t iter_state3;
    result_ids.clear();
    raw_lists.clear();

    posting_t::block_intersector_t(raw_lists, iter_state3).intersect([&](auto id, auto& its){
        std::unique_lock lk(vecm);
        result_ids.push_back(id);
    });

    ASSERT_EQ(0, result_ids.size());
}

TEST_F(PostingListTest, ResultsAndOffsetsBasics) {
    // NOTE: due to the way offsets1 are parsed, the actual positions are 1 less than the offset values stored
    // (to account for the special offset `0` which indicates last offset
    std::vector<uint32_t> offsets1 = {1, 2, 4};
    std::vector<uint32_t> offsets2 = {5, 6};
    std::vector<uint32_t> offsets3 = {7};

    std::vector<posting_list_t*> lists;

    // T1: [0, 2] [3, 20]
    // T2: [1, 3], [5, 10], [20]
    // T3: [2, 3], [5, 7], [20]

    // 3: (0, 1, 3} {4, 5} {6}
    // 2: {6}       {4, 5} {0, 1, 3}

    std::vector<token_positions_t> actual_offsets_3 = {
        token_positions_t{false, {0, 1, 3}},
        token_positions_t{false, {4, 5}},
        token_positions_t{false, {6}},
    };

    std::vector<token_positions_t> actual_offsets_20 = {
        token_positions_t{false, {6}},
        token_positions_t{false, {4, 5}},
        token_positions_t{false, {0, 1, 3}},
    };

    posting_list_t p1(2);
    p1.upsert(0, offsets1);
    p1.upsert(2, offsets1);
    p1.upsert(3, offsets1);
    p1.upsert(20, offsets3);

    posting_list_t p2(2);
    p2.upsert(1, offsets1);
    p2.upsert(3, offsets2);
    p2.upsert(5, offsets1);
    p2.upsert(10, offsets1);
    p2.upsert(20, offsets2);

    posting_list_t p3(2);
    p3.upsert(2, offsets1);
    p3.upsert(3, offsets3);
    p3.upsert(5, offsets1);
    p3.upsert(7, offsets1);
    p3.upsert(20, offsets1);

    lists.push_back(&p1);
    lists.push_back(&p2);
    lists.push_back(&p3);

    /*
    std::vector<posting_list_t::iterator_t> its;
    result_iter_state_t iter_state;
    bool has_more = posting_list_t::block_intersect(lists, 2, its, iter_state);
    ASSERT_FALSE(has_more);

    std::vector<std::unordered_map<size_t, std::vector<token_positions_t>>> array_token_positions_vec;
    posting_list_t::get_offsets(iter_state, array_token_positions_vec);
    ASSERT_EQ(2, array_token_positions_vec.size());

    ASSERT_EQ(actual_offsets_3[0].positions, array_token_positions_vec[0].at(0)[0].positions);
    ASSERT_EQ(actual_offsets_3[1].positions, array_token_positions_vec[0].at(0)[1].positions);
    ASSERT_EQ(actual_offsets_3[2].positions, array_token_positions_vec[0].at(0)[2].positions);

    ASSERT_EQ(actual_offsets_20[0].positions, array_token_positions_vec[1].at(0)[0].positions);
    ASSERT_EQ(actual_offsets_20[1].positions, array_token_positions_vec[1].at(0)[1].positions);
    ASSERT_EQ(actual_offsets_20[2].positions, array_token_positions_vec[1].at(0)[2].positions);
    */
}

TEST_F(PostingListTest, IntersectionSkipBlocks) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    std::vector<posting_list_t*> lists;

    std::vector<uint32_t> p1_ids = {9, 11};
    std::vector<uint32_t> p2_ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 11};
    std::vector<uint32_t> p3_ids = {2, 3, 8, 9, 11, 20};

    // [9, 11]
    // [1, 2], [3, 4], [5, 6], [7, 8], [9, 11]
    // [2, 3], [8, 9], [11, 20]

    posting_list_t p1(2);
    posting_list_t p2(2);
    posting_list_t p3(2);

    for(auto id: p1_ids) {
        p1.upsert(id, offsets);
    }

    for(auto id: p2_ids) {
        p2.upsert(id, offsets);
    }

    for(auto id: p3_ids) {
        p3.upsert(id, offsets);
    }

    lists.push_back(&p1);
    lists.push_back(&p2);
    lists.push_back(&p3);

    std::vector<uint32_t> result_ids;
    posting_list_t::intersect(lists, result_ids);

    uint32_t* p1_p2_intersected;
    uint32_t* final_results;

    size_t temp_len = ArrayUtils::and_scalar(&p1_ids[0], p1_ids.size(), &p2_ids[0], p2_ids.size(), &p1_p2_intersected);
    size_t final_len = ArrayUtils::and_scalar(&p3_ids[0], p3_ids.size(), p1_p2_intersected, temp_len, &final_results);

    ASSERT_EQ(final_len, result_ids.size());

    for(size_t i = 0; i < result_ids.size(); i++) {
        ASSERT_EQ(final_results[i], result_ids[i]);
    }

    delete [] p1_p2_intersected;
    delete [] final_results;
}

TEST_F(PostingListTest, PostingListContainsAtleastOne) {
    // when posting list is larger than target IDs
    posting_list_t p1(100);

    for(size_t i = 20; i < 1000; i++) {
        p1.upsert(i, {1, 2, 3});
    }

    std::vector<uint32_t> target_ids1 = {200, 300};
    std::vector<uint32_t> target_ids2 = {200, 3000};
    std::vector<uint32_t> target_ids3 = {2000, 3000};

    ASSERT_TRUE(p1.contains_atleast_one(&target_ids1[0], target_ids1.size()));
    ASSERT_TRUE(p1.contains_atleast_one(&target_ids2[0], target_ids2.size()));
    ASSERT_FALSE(p1.contains_atleast_one(&target_ids3[0], target_ids3.size()));

    // when posting list is smaller than target IDs
    posting_list_t p2(2);
    for(size_t i = 10; i < 20; i++) {
        p2.upsert(i, {1, 2, 3});
    }

    target_ids1.clear();
    for(size_t i = 5; i < 1000; i++) {
        target_ids1.push_back(i);
    }

    target_ids2.clear();
    for(size_t i = 25; i < 1000; i++) {
        target_ids2.push_back(i);
    }

    ASSERT_TRUE(p2.contains_atleast_one(&target_ids1[0], target_ids1.size()));
    ASSERT_FALSE(p2.contains_atleast_one(&target_ids2[0], target_ids2.size()));
}

TEST_F(PostingListTest, PostingListMergeAdjancentBlocks) {
    // when posting list is larger than target IDs
    posting_list_t p1(6);

    for(size_t i = 0; i < 18; i++) {
        p1.upsert(i, {2, 3});
    }

    p1.erase(0);
    p1.erase(1);

    // IDs: [4] [6] [6]
    //      [6] [4] [6]
    // Offsets:
    //     Before: [8]   [12]  [12]
    //     After:  [12]  [8]  [12]

    posting_list_t::block_t* next_block = p1.get_root()->next;
    posting_list_t::merge_adjacent_blocks(p1.get_root(), next_block, 2);

    ASSERT_EQ(6, p1.get_root()->ids.getLength());
    ASSERT_EQ(6, p1.get_root()->offset_index.getLength());
    ASSERT_EQ(12, p1.get_root()->offsets.getLength());

    std::vector<uint32_t> ids = {2, 3, 4, 5, 6, 7};
    for(size_t i = 0 ; i < ids.size(); i++) {
        auto id = ids[i];
        ASSERT_EQ(id, p1.get_root()->ids.at(i));
    }

    for(size_t i = 0; i < p1.get_root()->offset_index.getLength(); i++) {
        ASSERT_EQ(i*2, p1.get_root()->offset_index.at(i));
    }

    for(size_t i = 0; i < p1.get_root()->offsets.getLength(); i++) {
        auto expected_offset = (i % 2 == 0) ? 2 : 3;
        ASSERT_EQ(expected_offset, p1.get_root()->offsets.at(i));
    }

    ASSERT_EQ(4, next_block->ids.getLength());
    ASSERT_EQ(4, next_block->offset_index.getLength());
    ASSERT_EQ(8, next_block->offsets.getLength());

    ids = {8, 9, 10, 11};
    for(size_t i = 0 ; i < ids.size(); i++) {
        auto id = ids[i];
        ASSERT_EQ(id, next_block->ids.at(i));
    }

    for(size_t i = 0; i < next_block->offset_index.getLength(); i++) {
        ASSERT_EQ(i*2, next_block->offset_index.at(i));
    }

    for(size_t i = 0; i < next_block->offsets.getLength(); i++) {
        auto expected_offset = (i % 2 == 0) ? 2 : 3;
        ASSERT_EQ(expected_offset, next_block->offsets.at(i));
    }

    // full merge

    posting_list_t::block_t* block1 = next_block;
    posting_list_t::block_t* block2 = next_block->next;

    posting_list_t::merge_adjacent_blocks(block1, block2, 6);
    ASSERT_EQ(10, block1->ids.getLength());
    ASSERT_EQ(10, block1->offset_index.getLength());
    ASSERT_EQ(20, block1->offsets.getLength());

    ids = {8, 9, 10, 11, 12, 13, 14, 15, 16, 17};

    for(size_t i = 0; i < ids.size(); i++) {
        auto id = ids[i];
        ASSERT_EQ(id, block1->ids.at(i));
    }

    for(size_t i = 0; i < block1->offset_index.getLength(); i++) {
        ASSERT_EQ(i*2, block1->offset_index.at(i));
    }

    for(size_t i = 0; i < block1->offsets.getLength(); i++) {
        auto expected_offset = (i % 2 == 0) ? 2 : 3;
        ASSERT_EQ(expected_offset, block1->offsets.at(i));
    }

    ASSERT_EQ(0, block2->ids.getLength());
    ASSERT_EQ(0, block2->offset_index.getLength());
    ASSERT_EQ(0, block2->offsets.getLength());
}

TEST_F(PostingListTest, PostingListSplitBlock) {
    posting_list_t p1(6);

    for (size_t i = 0; i < 6; i++) {
        p1.upsert(i, {2, 3});
    }

    posting_list_t::block_t* block1 = p1.get_root();
    posting_list_t::block_t block2;
    posting_list_t::split_block(block1, &block2);

    ASSERT_EQ(3, block1->ids.getLength());
    ASSERT_EQ(3, block1->offset_index.getLength());
    ASSERT_EQ(6, block1->offsets.getLength());

    std::vector<uint32_t> ids = {0, 1, 2};

    for(size_t i = 0; i < ids.size(); i++) {
        ASSERT_EQ(ids[i], block1->ids.at(i));
    }

    for(size_t i = 0; i < block1->offset_index.getLength(); i++) {
        ASSERT_EQ(i*2, block1->offset_index.at(i));
    }

    for(size_t i = 0; i < block1->offsets.getLength(); i++) {
        auto expected_offset = (i % 2 == 0) ? 2 : 3;
        ASSERT_EQ(expected_offset, block1->offsets.at(i));
    }

    ASSERT_EQ(3, block2.ids.getLength());
    ASSERT_EQ(3, block2.offset_index.getLength());
    ASSERT_EQ(6, block2.offsets.getLength());

    ids = {3, 4, 5};

    for(size_t i = 0; i < ids.size(); i++) {
        ASSERT_EQ(ids[i], block2.ids.at(i));
    }

    for(size_t i = 0; i < block2.offset_index.getLength(); i++) {
        ASSERT_EQ(i*2, block2.offset_index.at(i));
    }

    for(size_t i = 0; i < block2.offsets.getLength(); i++) {
        auto expected_offset = (i % 2 == 0) ? 2 : 3;
        ASSERT_EQ(expected_offset, block2.offsets.at(i));
    }
}

TEST_F(PostingListTest, CompactPostingListUpsertAppends) {
    uint32_t ids[] = {0, 1000, 1002};
    uint32_t offset_index[] = {0, 3, 6};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* list = compact_posting_list_t::create(3, ids, offset_index, 9, offsets);
    ASSERT_EQ(15, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());

    ASSERT_TRUE(list->contains(0));
    ASSERT_TRUE(list->contains(1000));
    ASSERT_TRUE(list->contains(1002));
    ASSERT_FALSE(list->contains(500));
    ASSERT_FALSE(list->contains(2));

    // no-op since the container expects resizing to be done outside
    list->upsert(1003, {1, 2});
    ASSERT_EQ(15, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());

    // now resize
    void* obj = SET_COMPACT_POSTING(list);
    posting_t::upsert(obj, 1003, {1, 2});
    ASSERT_EQ(1003, COMPACT_POSTING_PTR(obj)->last_id());

    ASSERT_EQ(19, (COMPACT_POSTING_PTR(obj))->length);
    ASSERT_EQ(24, (COMPACT_POSTING_PTR(obj))->capacity);
    ASSERT_EQ(4, (COMPACT_POSTING_PTR(obj))->ids_length);

    // insert enough docs to NOT exceed compact posting list threshold
    posting_t::upsert(obj, 1004, {1, 2, 3, 4, 5, 6, 7, 8});
    ASSERT_EQ(1004, COMPACT_POSTING_PTR(obj)->last_id());
    posting_t::upsert(obj, 1005, {1, 2, 3, 4, 5, 6, 7, 8});
    ASSERT_EQ(1005, COMPACT_POSTING_PTR(obj)->last_id());
    posting_t::upsert(obj, 1006, {1, 2, 3, 4, 5, 6, 7, 8});
    ASSERT_EQ(1006, COMPACT_POSTING_PTR(obj)->last_id());
    posting_t::upsert(obj, 1007, {1, 2, 3, 4, 5, 6, 7, 8});
    ASSERT_EQ(1007, COMPACT_POSTING_PTR(obj)->last_id());
    ASSERT_TRUE(IS_COMPACT_POSTING(obj));
    ASSERT_EQ(1007, COMPACT_POSTING_PTR(obj)->last_id());
    ASSERT_EQ(8, (COMPACT_POSTING_PTR(obj))->ids_length);

    // next upsert will exceed threshold
    posting_t::upsert(obj, 1008, {1, 2, 3, 4, 5, 6, 7, 8});
    ASSERT_FALSE(IS_COMPACT_POSTING(obj));

    ASSERT_EQ(1, ((posting_list_t*)(obj))->num_blocks());
    ASSERT_EQ(9, ((posting_list_t*)(obj))->get_root()->size());
    ASSERT_EQ(1008, ((posting_list_t*)(obj))->get_root()->ids.last());
    ASSERT_EQ(9, ((posting_list_t*)(obj))->get_root()->ids.getLength());
    ASSERT_EQ(9, ((posting_list_t*)(obj))->num_ids());

    delete ((posting_list_t*)(obj));
}

TEST_F(PostingListTest, CompactPostingListUpserts) {
    uint32_t ids[] = {3, 1000, 1002};
    uint32_t offset_index[] = {0, 3, 6};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* list = compact_posting_list_t::create(3, ids, offset_index, 9, offsets);
    ASSERT_EQ(15, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());

    // insert before first ID

    void* obj = SET_COMPACT_POSTING(list);
    posting_t::upsert(obj, 2, {1, 2});
    ASSERT_EQ(1002, COMPACT_POSTING_PTR(obj)->last_id());
    ASSERT_EQ(19, COMPACT_POSTING_PTR(obj)->length);
    ASSERT_EQ(24, COMPACT_POSTING_PTR(obj)->capacity);
    ASSERT_EQ(4, COMPACT_POSTING_PTR(obj)->num_ids());

    // insert in the middle
    posting_t::upsert(obj, 999, {1, 2});
    ASSERT_EQ(1002, COMPACT_POSTING_PTR(obj)->last_id());
    ASSERT_EQ(23, COMPACT_POSTING_PTR(obj)->length);
    ASSERT_EQ(24, COMPACT_POSTING_PTR(obj)->capacity);
    ASSERT_EQ(5, COMPACT_POSTING_PTR(obj)->num_ids());

    uint32_t expected_id_offsets[] = {
        2, 1, 2, 2,
        3, 0, 3, 4, 3,
        2, 1, 2, 999,
        3, 0, 3, 4, 1000,
        3, 0, 3, 4, 1002
    };

    ASSERT_EQ(23, COMPACT_POSTING_PTR(obj)->length);

    for(size_t i = 0; i < COMPACT_POSTING_PTR(obj)->length; i++) {
        ASSERT_EQ(expected_id_offsets[i], COMPACT_POSTING_PTR(obj)->id_offsets[i]);
    }

    free(COMPACT_POSTING_PTR(obj));
}

TEST_F(PostingListTest, CompactPostingListUpdateWithLessOffsets) {
    uint32_t ids[] = {0, 1000, 1002};
    uint32_t offset_index[] = {0, 3, 6};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* list = compact_posting_list_t::create(3, ids, offset_index, 9, offsets);
    ASSERT_EQ(15, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());

    // update middle

    list->upsert(1000, {1, 2});
    ASSERT_EQ(14, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());
    uint32_t expected_id_offsets[] = {3, 0, 3, 4, 0, 2, 1, 2, 1000, 3, 0, 3, 4, 1002};
    for(size_t i = 0; i < list->length; i++) {
        ASSERT_EQ(expected_id_offsets[i], list->id_offsets[i]);
    }

    // update start
    list->upsert(0, {2, 4});
    ASSERT_EQ(13, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());
    uint32_t expected_id_offsets2[] = {2, 2, 4, 0, 2, 1, 2, 1000, 3, 0, 3, 4, 1002};
    for(size_t i = 0; i < list->length; i++) {
        ASSERT_EQ(expected_id_offsets2[i], list->id_offsets[i]);
    }

    // update end
    list->upsert(1002, {2, 4});
    ASSERT_EQ(12, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());
    uint32_t expected_id_offsets3[] = {2, 2, 4, 0, 2, 1, 2, 1000, 2, 2, 4, 1002};
    for(size_t i = 0; i < list->length; i++) {
        ASSERT_EQ(expected_id_offsets3[i], list->id_offsets[i]);
    }

    free(list);
}

TEST_F(PostingListTest, CompactPostingListUpdateWithMoreOffsets) {
    uint32_t ids[] = {0, 1000, 1002};
    uint32_t offset_index[] = {0, 3, 6};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* list = compact_posting_list_t::create(3, ids, offset_index, 9, offsets);
    ASSERT_EQ(15, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());

    // update middle
    void* obj = SET_COMPACT_POSTING(list);
    posting_t::upsert(obj, 1000, {1, 2, 3, 4});
    list = COMPACT_POSTING_PTR(obj);
    ASSERT_EQ(16, list->length);
    ASSERT_EQ(20, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());
    uint32_t expected_id_offsets[] = {3, 0, 3, 4, 0, 4, 1, 2, 3, 4, 1000, 3, 0, 3, 4, 1002};
    for(size_t i = 0; i < list->length; i++) {
        ASSERT_EQ(expected_id_offsets[i], list->id_offsets[i]);
    }

    // update start
    list->upsert(0, {1, 2, 3, 4});
    ASSERT_EQ(17, list->length);
    ASSERT_EQ(20, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());
    uint32_t expected_id_offsets2[] = {4, 1, 2, 3, 4, 0, 4, 1, 2, 3, 4, 1000, 3, 0, 3, 4, 1002};
    for(size_t i = 0; i < list->length; i++) {
        ASSERT_EQ(expected_id_offsets2[i], list->id_offsets[i]);
    }

    // update end
    list->upsert(1002, {1, 2, 3, 4});
    ASSERT_EQ(18, list->length);
    ASSERT_EQ(20, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());
    uint32_t expected_id_offsets3[] = {4, 1, 2, 3, 4, 0, 4, 1, 2, 3, 4, 1000, 4, 1, 2, 3, 4, 1002};
    for(size_t i = 0; i < list->length; i++) {
        ASSERT_EQ(expected_id_offsets3[i], list->id_offsets[i]);
    }

    free(list);
}

TEST_F(PostingListTest, CompactPostingListErase) {
    uint32_t ids[] = {0, 1000, 1002};
    uint32_t offset_index[] = {0, 3, 6};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* list = compact_posting_list_t::create(3, ids, offset_index, 9, offsets);

    list->erase(3); // erase non-existing small ID

    ASSERT_EQ(15, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());

    list->erase(3000); // erase non-existing large ID

    ASSERT_EQ(15, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(3, list->num_ids());

    list->erase(1000);
    ASSERT_EQ(10, list->length);
    ASSERT_EQ(15, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(2, list->num_ids());

    // deleting using posting wrapper
    void* obj = SET_COMPACT_POSTING(list);
    posting_t::erase(obj, 1002);
    ASSERT_TRUE(IS_COMPACT_POSTING(obj));
    ASSERT_EQ(5, (COMPACT_POSTING_PTR(obj))->length);
    ASSERT_EQ(7, (COMPACT_POSTING_PTR(obj))->capacity);
    ASSERT_EQ(0, (COMPACT_POSTING_PTR(obj))->last_id());
    ASSERT_EQ(1, (COMPACT_POSTING_PTR(obj))->num_ids());

    // upsert again
    posting_t::upsert(obj, 1002, {0, 3, 4});
    list = COMPACT_POSTING_PTR(obj);
    ASSERT_EQ(10, list->length);
    ASSERT_EQ(13, list->capacity);
    ASSERT_EQ(1002, list->last_id());
    ASSERT_EQ(2, list->num_ids());

    free(list);
}

TEST_F(PostingListTest, CompactPostingListContainsAtleastOne) {
    uint32_t ids[] = {5, 6, 7, 8};
    uint32_t offset_index[] = {0, 3, 6, 9};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4, 0, 3, 4};

    std::vector<uint32_t> target_ids1 = {4, 7, 11};
    std::vector<uint32_t> target_ids2 = {2, 3, 4, 20};

    compact_posting_list_t* list1 = compact_posting_list_t::create(4, ids, offset_index, 12, offsets);
    ASSERT_TRUE(list1->contains_atleast_one(&target_ids1[0], target_ids1.size()));
    ASSERT_FALSE(list1->contains_atleast_one(&target_ids2[0], target_ids2.size()));
    free(list1);

    compact_posting_list_t* list2 = static_cast<compact_posting_list_t*>(malloc(sizeof(compact_posting_list_t)));
    list2->capacity = list2->ids_length = list2->length = 0;
    void* obj = SET_COMPACT_POSTING(list2);
    posting_t::upsert(obj, 3, {1, 5});

    std::vector<uint32_t> target_ids3 = {1, 2, 3, 4, 100};
    std::vector<uint32_t> target_ids4 = {4, 5, 6, 100};

    ASSERT_TRUE(COMPACT_POSTING_PTR(obj)->contains_atleast_one(&target_ids3[0], target_ids3.size()));
    ASSERT_FALSE(COMPACT_POSTING_PTR(obj)->contains_atleast_one(&target_ids4[0], target_ids4.size()));

    std::vector<uint32_t> target_ids5 = {2, 3};
    ASSERT_TRUE(COMPACT_POSTING_PTR(obj)->contains_atleast_one(&target_ids5[0], target_ids5.size()));

    std::vector<uint32_t> target_ids6 = {0, 1, 2};
    ASSERT_FALSE(COMPACT_POSTING_PTR(obj)->contains_atleast_one(&target_ids6[0], target_ids6.size()));

    posting_t::destroy_list(obj);
}

TEST_F(PostingListTest, CompactToFullPostingListConversion) {
    uint32_t ids[] = {5, 6, 7, 8};
    uint32_t offset_index[] = {0, 3, 6, 9};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* c1 = compact_posting_list_t::create(4, ids, offset_index, 12, offsets);
    posting_list_t* p1 = c1->to_full_posting_list();

    ASSERT_EQ(4, c1->num_ids());
    ASSERT_EQ(4, p1->num_ids());

    free(c1);
    delete p1;
}

TEST_F(PostingListTest, BlockIntersectionOnMixedLists) {
    uint32_t ids[] = {5, 6, 7, 8};
    uint32_t offset_index[] = {0, 3, 6, 9};
    uint32_t offsets[] = {0, 3, 4, 0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* list1 = compact_posting_list_t::create(4, ids, offset_index, 12, offsets);

    posting_list_t p1(2);
    std::vector<uint32_t> offsets1 = {2, 4};

    p1.upsert(0, offsets1);
    p1.upsert(5, offsets1);
    p1.upsert(8, offsets1);
    p1.upsert(20, offsets1);

    std::vector<void*> raw_posting_lists = {SET_COMPACT_POSTING(list1), &p1};
    result_iter_state_t iter_state;
    std::vector<uint32_t> result_ids;
    std::mutex vecm;

    posting_t::block_intersector_t(raw_posting_lists, iter_state)
    .intersect([&](auto seq_id, auto& its) {
        std::unique_lock lock(vecm);
        result_ids.push_back(seq_id);
    });

    std::sort(result_ids.begin(), result_ids.end());

    ASSERT_EQ(2, result_ids.size());
    ASSERT_EQ(5, result_ids[0]);
    ASSERT_EQ(8, result_ids[1]);

    free(list1);
}

TEST_F(PostingListTest, InsertAndEraseSequence) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    pl.upsert(0, offsets);
    pl.upsert(2, offsets);
    pl.upsert(4, offsets);
    pl.upsert(6, offsets);
    pl.upsert(8, offsets);

    // this will cause a split of the root block
    pl.upsert(3, offsets); // 0,2,3 | 4,6,8
    pl.erase(0); // 2,3 | 4,6,8
    pl.upsert(5, offsets); // 2,3 | 4,5,6,8
    pl.upsert(7, offsets); // 2,3 | 4,5,6,7,8
    pl.upsert(10, offsets); // 2,3 | 4,5,6,7,8 | 10

    // this will cause adjacent block refill
    pl.erase(2); // 3,4,5,6,7 | 8 | 10

    // deletes second block
    pl.erase(8);

    // remove all elements
    pl.erase(3);
    pl.erase(4);
    pl.erase(5);
    pl.erase(6);
    pl.erase(7);
    pl.erase(10);

    ASSERT_EQ(0, pl.num_ids());
}

TEST_F(PostingListTest, InsertAndEraseSequenceWithBlockSizeTwo) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(2);

    pl.upsert(2, offsets);
    pl.upsert(3, offsets);
    pl.upsert(1, offsets);  // inserting 2 again here?  // inserting 4 here?

    // 1 | 2,3

    pl.erase(1);

    ASSERT_EQ(1, pl.get_root()->size());
    ASSERT_EQ(2, pl.num_blocks());

    pl.erase(3);
    pl.erase(2);

    ASSERT_EQ(0, pl.get_root()->size());
}

TEST_F(PostingListTest, PostingListMustHaveAtleast1Element) {
    try {
        std::vector<uint32_t> offsets = {0, 1, 3};
        posting_list_t pl(1);
        FAIL() << "Expected std::invalid_argument";
    }
    catch(std::invalid_argument const & err) {
        EXPECT_EQ(err.what(),std::string("max_block_elements must be > 1"));
    } catch(...) {
        FAIL() << "Expected std::invalid_argument";
    }
}

TEST_F(PostingListTest, DISABLED_RandInsertAndErase) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    time_t t;
    srand((unsigned) time(&t));

    for(size_t i = 0; i < 10000; i++) {
        LOG(INFO) << "i: " << i;
        uint32_t add_id = rand() % 15;
        pl.upsert(add_id, offsets);

        uint32_t del_id = rand() % 15;
        LOG(INFO) << "add: " << add_id << ", erase: " << del_id;
        pl.erase(del_id);
    }

    LOG(INFO) << "Num ids: " << pl.num_ids() << ", num bocks: " << pl.num_blocks();
}

TEST_F(PostingListTest, DISABLED_Benchmark) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(4096);
    sorted_array arr;

    for(size_t i = 0; i < 500000; i++) {
        pl.upsert(i, offsets);
        arr.append(i);
    }

    auto begin = std::chrono::high_resolution_clock::now();

    for(size_t i = 250000; i < 250005; i++) {
        pl.upsert(i, offsets);
    }

    long long int timeMicros =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    LOG(INFO) << "Time taken for 5 posting list updates: " << timeMicros;

    begin = std::chrono::high_resolution_clock::now();

    for(size_t i = 250000; i < 250005; i++) {
        arr.remove_value(i);
        arr.append(i);
    }

    timeMicros =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    LOG(INFO) << "Time taken for 5 sorted array updates: " << timeMicros;
}

TEST_F(PostingListTest, DISABLED_BenchmarkIntersection) {
    std::vector<uint32_t> offsets = {0, 1, 3};

    time_t t;
    srand((unsigned) time(&t));

    std::set<uint32_t> rand_ids1;
    std::set<uint32_t> rand_ids2;
    std::set<uint32_t> rand_ids3;

    const size_t list1_size = 100000;
    const size_t list2_size = 50000;
    const size_t list3_size = 25000;
    const size_t num_range = 1000000;

    /*const size_t list1_size = 10;
    const size_t list2_size = 10;
    const size_t num_range = 50;*/

    for(size_t i = 0; i < list1_size; i++) {
        rand_ids1.insert(rand() % num_range);
    }

    for(size_t i = 0; i < list2_size; i++) {
        rand_ids2.insert(rand() % num_range);
    }

    for(size_t i = 0; i < list3_size; i++) {
        rand_ids3.insert(rand() % num_range);
    }

    posting_list_t pl1(1024);
    posting_list_t pl2(1024);
    posting_list_t pl3(1024);
    sorted_array arr1;
    sorted_array arr2;
    sorted_array arr3;

    std::string id1_str = "";
    std::string id2_str = "";
    std::string id3_str = "";

    for(auto id: rand_ids1) {
        //id1_str += std::to_string(id) + " ";
        pl1.upsert(id, offsets);
        arr1.append(id);
    }

    for(auto id: rand_ids2) {
        //id2_str += std::to_string(id) + " ";
        pl2.upsert(id, offsets);
        arr2.append(id);
    }

    for(auto id: rand_ids3) {
        //id2_str += std::to_string(id) + " ";
        pl3.upsert(id, offsets);
        arr3.append(id);
    }

    //LOG(INFO) << "id1_str: " << id1_str;
    //LOG(INFO) << "id2_str: " << id2_str;

    std::vector<uint32_t> result_ids;
    auto begin = std::chrono::high_resolution_clock::now();

    posting_list_t::intersect({&pl1, &pl2, &pl3}, result_ids);

    long long int timeMicros =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    LOG(INFO) << "Posting list result len: " << result_ids.size();
    LOG(INFO) << "Time taken for posting list intersection: " << timeMicros;

    begin = std::chrono::high_resolution_clock::now();

    auto a = arr1.uncompress();
    auto b = arr2.uncompress();
    auto c = arr3.uncompress();

    uint32_t* ab;
    size_t ab_len = ArrayUtils::and_scalar(a, arr1.getLength(), b, arr2.getLength(), &ab);

    uint32_t* abc;
    size_t abc_len = ArrayUtils::and_scalar(ab, ab_len, c, arr3.getLength(), &abc);

    delete [] a;
    delete [] b;
    delete [] c;
    delete [] ab;
    delete [] abc;

    timeMicros =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    LOG(INFO) << "Sorted array result len: " << abc_len;
    LOG(INFO) << "Time taken for sorted array intersection: " << timeMicros;
}

TEST_F(PostingListTest, GetOrIterator) {
    std::vector<uint32_t> ids = {1, 3, 5};
    std::vector<uint32_t> offset_index = {0, 3, 6};
    std::vector<uint32_t> offsets = {0, 3, 4, 0, 3, 4, 0, 3, 4};
    compact_posting_list_t* c_list = compact_posting_list_t::create(3, &ids[0], &offset_index[0], 9, &offsets[0]);
    void* raw_pointer = SET_COMPACT_POSTING(c_list);

    std::vector<or_iterator_t> or_iterators;
    std::vector<posting_list_t*> expanded_plists;

    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    ASSERT_EQ(1, or_iterators.size());
    ASSERT_EQ(1, expanded_plists.size());

    for (const auto &id: ids) {
        ASSERT_TRUE(or_iterators.front().valid());
        ASSERT_EQ(id, or_iterators.front().id());
        or_iterators.front().next();
    }
    ASSERT_FALSE(or_iterators.front().valid());

    free(c_list);
    or_iterators.clear();
    for (auto& item: expanded_plists) {
        delete item;
    }
    expanded_plists.clear();

    posting_list_t p_list(2);
    for (const auto &id: ids) {
        p_list.upsert(id, {1, 2, 3});
    }
    raw_pointer = &p_list;

    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    ASSERT_EQ(1, or_iterators.size());
    ASSERT_TRUE(expanded_plists.empty());

    for (const auto &id: ids) {
        ASSERT_TRUE(or_iterators.front().valid());
        ASSERT_EQ(id, or_iterators.front().id());
        or_iterators.front().next();
    }
    ASSERT_FALSE(or_iterators.front().valid());

    or_iterators.clear();
}
