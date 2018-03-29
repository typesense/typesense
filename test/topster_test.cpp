#include <gtest/gtest.h>
#include "topster.h"
#include "match_score.h"

TEST(TopsterTest, StoreMaxIntValuesWithoutRepetition) {
    Topster<5> topster;

    struct {
        uint16_t query_index;
        uint64_t key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[14] = {
        {0, 1, 10, 20, 30},
        {0, 1, 10, 20, 30},
        {0, 2, 4, 20, 30},
        {2, 3, 7, 20, 30},
        {0, 4, 11, 20, 30},
        {1, 5, 9, 20, 30},
        {1, 5, 9, 20, 30},
        {1, 5, 9, 20, 30},
        {0, 6, 6, 20, 30},
        {2, 7, 6, 22, 30},
        {2, 7, 6, 22, 30},
        {1, 8, 9, 20, 30},
        {0, 9, 8, 20, 30},
        {3, 10, 5, 20, 30},
    };

    for(int i = 0; i < 14; i++) {
        topster.add(data[i].key, data[i].query_index, data[i].match_score, data[i].primary_attr,
                    data[i].secondary_attr);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(uint32_t i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));
    }
}

TEST(TopsterTest, StoreMaxFloatValuesWithoutRepetition) {
    Topster<5> topster;

    struct {
        uint16_t query_index;
        uint64_t key;
        uint64_t match_score;
        float primary_attr;
        int64_t secondary_attr;
    } data[12] = {
            {0, 1, 11, 1.09, 30},
            {0, 2, 11, -20, 30},
            {2, 3, 11, -20, 30},
            {0, 4, 11, 7.812, 30},
            {0, 4, 11, 7.812, 30},
            {1, 5, 11, 0.0, 34},
            {0, 6, 11, -22, 30},
            {2, 7, 11, -22, 30},
            {1, 8, 11, -9.998, 30},
            {1, 8, 11, -9.998, 30},
            {0, 9, 11, -9.999, 30},
            {3, 10, 11, -20, 30},
    };

    for(int i = 0; i < 12; i++) {
        topster.add(data[i].key, data[i].query_index, data[i].match_score, data[i].primary_attr,
                    data[i].secondary_attr);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(uint32_t i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));
    }
}