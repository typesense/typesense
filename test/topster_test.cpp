#include <gtest/gtest.h>
#include "topster.h"
#include "match_score.h"

TEST(TopsterTest, StoreMaxValuesWithoutRepetition) {
    Topster<5> topster;

    struct {
        uint16_t start_offset;
        char offset_diffs[16];
        uint64_t key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[10] = {
        {10, { 10 }, 1, 10, 20, 30},
        {0, { 10 }, 2, 4, 20, 30},
        {2, { 10 }, 3, 7, 20, 30},
        {11, { 20 }, 4, 11, 20, 30},
        {78, { 30 }, 5, 9, 20, 30},
        {246, { 10 }, 6, 6, 20, 30},
        {0, { 10, 11 }, 7, 6, 22, 30},
        {20, { 10 }, 8, 9, 20, 30},
        {22, { 15, 17, 18 }, 9, 8, 20, 30},
        {77, { 10 }, 10, 5, 20, 30},
    };

    for(int i = 0; i < 10; i++) {
        topster.add(data[i].key, data[i].match_score, data[i].primary_attr, data[i].secondary_attr,
                    data[i].start_offset, data[i].offset_diffs);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(int i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));
    }
}