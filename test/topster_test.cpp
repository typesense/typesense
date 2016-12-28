#include <gtest/gtest.h>
#include "topster.h"

TEST(TopsterTest, StoreMaxValuesWithoutRepetition) {
    Topster<5> topster;

    struct {
        uint64_t key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[10] = {
        {1, 10, 20, 30},
        {2, 4, 20, 30},
        {3, 7, 20, 30},
        {4, 11, 20, 30},
        {5, 9, 20, 30},
        {6, 6, 20, 30},
        {7, 6, 22, 30},
        {8, 9, 20, 30},
        {9, 8, 20, 30},
        {10, 5, 20, 30},
    };

    for(int i = 0; i < 10; i++) {
        topster.add(data[i].key, data[i].match_score, data[i].primary_attr, data[i].secondary_attr);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 8, 5, 9};

    for(int i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));
    }
}