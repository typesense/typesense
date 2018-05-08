#include <gtest/gtest.h>
#include "topster.h"
#include "match_score.h"

TEST(TopsterTest, MaxIntValues) {
    Topster<5> topster;

    struct {
        uint8_t field_id;
        uint16_t query_index;
        uint64_t key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[14] = {
        {1, 0, 1, 11, 20, 30},
        {1, 0, 1, 12, 20, 32},
        {1, 0, 2, 4, 20, 30},
        {1, 2, 3, 7, 20, 30},
        {1, 0, 4, 14, 20, 30},
        {1, 1, 5, 9, 20, 30},
        {1, 1, 5, 10, 20, 32},
        {1, 1, 5, 9, 20, 30},
        {1, 0, 6, 6, 20, 30},
        {1, 2, 7, 6, 22, 30},
        {1, 2, 7, 6, 22, 30},
        {1, 1, 8, 9, 20, 30},
        {1, 0, 9, 8, 20, 30},
        {1, 3, 10, 5, 20, 30},
    };

    for(int i = 0; i < 14; i++) {
        topster.add(data[i].key, data[i].field_id, data[i].query_index, data[i].match_score, data[i].primary_attr,
                    data[i].secondary_attr);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(uint32_t i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));

        if(ids[i] == 1) {
            EXPECT_EQ(12, (int) topster.getKV(i)->match_score);
        }

        if(ids[i] == 5) {
            EXPECT_EQ(10, (int) topster.getKV(i)->match_score);
        }
    }
}

TEST(TopsterTest, MaxFloatValues) {
    Topster<5> topster;

    struct {
        uint8_t field_id;
        uint16_t query_index;
        uint64_t key;
        uint64_t match_score;
        float primary_attr;
        int64_t secondary_attr;
    } data[12] = {
            {1, 0, 1, 11, 1.09, 30},
            {1, 0, 2, 11, -20, 30},
            {1, 2, 3, 11, -20, 30},
            {1, 0, 4, 11, 7.812, 30},
            {1, 0, 4, 11, 7.912, 30},
            {1, 1, 5, 11, 0.0, 34},
            {1, 0, 6, 11, -22, 30},
            {1, 2, 7, 11, -22, 30},
            {1, 1, 8, 11, -9.998, 30},
            {1, 1, 8, 11, -9.998, 30},
            {1, 0, 9, 11, -9.999, 30},
            {1, 3, 10, 11, -20, 30},
    };

    for(int i = 0; i < 12; i++) {
        topster.add(data[i].key, data[i].field_id, data[i].query_index, data[i].match_score, data[i].primary_attr,
                    data[i].secondary_attr);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(uint32_t i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));
    }
}