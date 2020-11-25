#include <gtest/gtest.h>
#include <index.h>
#include "topster.h"
#include "match_score.h"

TEST(TopsterTest, MaxIntValues) {
    Topster topster(5);

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
        int64_t scores[3];
        scores[0] = int64_t(data[i].match_score);
        scores[1] = data[i].primary_attr;
        scores[2] = data[i].secondary_attr;

        KV kv(data[i].field_id, data[i].query_index, data[i].key, data[i].key, 0, scores);
        topster.add(&kv);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(uint32_t i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));

        if(ids[i] == 1) {
            EXPECT_EQ(12, (int) topster.getKV(i)->scores[topster.getKV(i)->match_score_index]);
        }

        if(ids[i] == 5) {
            EXPECT_EQ(10, (int) topster.getKV(i)->scores[topster.getKV(i)->match_score_index]);
        }
    }
}

TEST(TopsterTest, MaxFloatValues) {
    Topster topster(5);

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
        int64_t scores[3];
        scores[0] = int64_t(data[i].match_score);
        scores[1] = Index::float_to_in64_t(data[i].primary_attr);
        scores[2] = data[i].secondary_attr;

        KV kv(data[i].field_id, data[i].query_index, data[i].key, data[i].key, 0, scores);
        topster.add(&kv);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(uint32_t i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));
    }
}

TEST(TopsterTest, DistinctIntValues) {
    Topster dist_topster(5, 2);

    struct {
        uint8_t field_id;
        uint16_t query_index;
        uint64_t distinct_key;
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
        int64_t scores[3];
        scores[0] = int64_t(data[i].match_score);
        scores[1] = data[i].primary_attr;
        scores[2] = data[i].secondary_attr;

        KV kv(data[i].field_id, data[i].query_index, i+100, data[i].distinct_key, 0, scores);
        dist_topster.add(&kv);
    }

    dist_topster.sort();

    std::vector<uint64_t> distinct_ids = {4, 1, 8, 5, 9};

    for(uint32_t i = 0; i < dist_topster.size; i++) {
        EXPECT_EQ(distinct_ids[i], dist_topster.getDistinctKeyAt(i));

        if(distinct_ids[i] == 1) {
            EXPECT_EQ(12, (int) dist_topster.getKV(i)->scores[dist_topster.getKV(i)->match_score_index]);
            EXPECT_EQ(2, dist_topster.group_kv_map[dist_topster.getDistinctKeyAt(i)]->size);
            EXPECT_EQ(12, dist_topster.group_kv_map[dist_topster.getDistinctKeyAt(i)]->getKV(0)->scores[0]);
            EXPECT_EQ(11, dist_topster.group_kv_map[dist_topster.getDistinctKeyAt(i)]->getKV(1)->scores[0]);
        }

        if(distinct_ids[i] == 5) {
            EXPECT_EQ(9, (int) dist_topster.getKV(i)->scores[dist_topster.getKV(i)->match_score_index]);
            EXPECT_EQ(2, dist_topster.group_kv_map[dist_topster.getDistinctKeyAt(i)]->size);
            EXPECT_EQ(10, dist_topster.group_kv_map[dist_topster.getDistinctKeyAt(i)]->getKV(0)->scores[0]);
            EXPECT_EQ(9, dist_topster.group_kv_map[dist_topster.getDistinctKeyAt(i)]->getKV(1)->scores[0]);
        }
    }
}