#include <gtest/gtest.h>
#include "posting_list.h"
#include <vector>

TEST(PostingListTest, Insert) {
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

    ASSERT_EQ(3, pl.size());
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
    ASSERT_EQ(2, pl2.size());

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

    // [0,1,2,3,4], [6,8,9,10,12]
    pl3.upsert(5, offsets);
    ASSERT_EQ(3, pl3.size());
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
    ASSERT_EQ(3, pl4.size());

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

TEST(PostingListTest, RemovalsOnFirstBlock) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    ASSERT_EQ(0, pl.size());

    // try to erase when posting list is empty
    pl.erase(0);

    ASSERT_EQ(0, pl.size());

    // insert a single element and erase it
    pl.upsert(0, offsets);
    ASSERT_EQ(1, pl.size());
    pl.erase(0);
    ASSERT_EQ(0, pl.size());

    ASSERT_EQ(0, pl.get_root()->ids.getLength());
    ASSERT_EQ(0, pl.get_root()->offset_index.getLength());
    ASSERT_EQ(0, pl.get_root()->offsets.getLength());

    // insert until one past max block size
    for(size_t i = 0; i < 6; i++) {
        pl.upsert(i, offsets);
    }

    ASSERT_EQ(2, pl.size());

    // delete non-existing element
    pl.erase(1000);

    // delete elements from first block: blocks should not be merged until it falls below 50% occupancy
    pl.erase(1);
    ASSERT_EQ(2, pl.size());

    // [0, 2, 3, 4], [5]

    for(size_t i = 0; i < pl.get_root()->offset_index.getLength(); i++) {
        ASSERT_EQ(i * 3, pl.get_root()->offset_index.at(i));
    }

    for(size_t i = 0; i < pl.get_root()->offsets.getLength(); i++) {
        ASSERT_EQ(offsets[i % 3], pl.get_root()->offsets.at(i));
    }

    pl.erase(2);
    ASSERT_EQ(2, pl.size());
    pl.erase(3);

    // [0, 4], [5]
    ASSERT_EQ(2, pl.size());
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
    ASSERT_EQ(1, pl.size());
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

TEST(PostingListTest, RemovalsOnLaterBlocks) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    // insert until one past max block size
    for(size_t i = 0; i < 6; i++) {
        pl.upsert(i, offsets);
    }

    // erase last element of last, non-first block

    pl.erase(5);
    ASSERT_EQ(1, pl.size());
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
    ASSERT_EQ(1, pl.size());
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

    // [0..4], [9], [10..14] => [0..4], [9,10,11,12,13], [14]

    ASSERT_EQ(3, pl.size());
    ASSERT_EQ(5, pl.get_root()->next->size());
    ASSERT_EQ(1, pl.get_root()->next->next->size());
    ASSERT_EQ(13, pl.get_root()->next->ids.last());
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

TEST(PostingListTest, OutOfOrderUpserts) {
    std::vector<uint32_t> offsets = {0, 1, 3};
    posting_list_t pl(5);

    for(int i = 5; i > 0; i--) {
        pl.upsert(i, offsets);
    }

    pl.upsert(0, offsets);
    pl.upsert(200000, offsets);

    ASSERT_EQ(2, pl.size());

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

TEST(PostingListTest, RandomInsertAndDeletes) {
    time_t t;
    srand((unsigned) time(&t));

    posting_list_t pl(100);
    std::vector<uint32_t> offsets1 = {0, 1, 3};
    std::vector<uint32_t> offsets2 = {10, 12};

    for(size_t i = 0; i < 100000; i++) {
        const std::vector<uint32_t>& offsets = (i % 2 == 0) ? offsets1 : offsets2;
        pl.upsert(rand() % 100000, offsets);
    }

    for(size_t i = 0; i < 10000; i++) {
        const std::vector<uint32_t>& offsets = (i % 2 == 0) ? offsets1 : offsets2;
        pl.erase(rand() % 100000);
    }

    bool size_within_range = (pl.size() < 1500) && (pl.size() > 1000);
    ASSERT_TRUE(size_within_range);
}
