#include <gtest/gtest.h>
#include <index.h>
#include "topster.h"
#include "match_score.h"
#include <fstream>
#include <unordered_set>

TEST(TopsterTest, MaxIntValues) {
    Topster<KV> topster(5);

    struct {
        uint16_t query_index;
        uint64_t key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[14] = {
        {0, 1, 11, 20, 30},
        {0, 1, 12, 20, 32},
        {0, 2, 4, 20, 30},
        {2, 3, 7, 20, 30},
        {0, 4, 14, 20, 30},
        {1, 5, 9, 20, 30},
        {1, 5, 10, 20, 32},
        {1, 5, 9, 20, 30},
        {0, 6, 6, 20, 30},
        {2, 7, 6, 22, 30},
        {2, 7, 6, 22, 30},
        {1, 8, 9, 20, 30},
        {0, 9, 8, 20, 30},
        {3, 10, 5, 20, 30},
    };

    for(int i = 0; i < 14; i++) {
        int64_t scores[3];
        scores[0] = int64_t(data[i].match_score);
        scores[1] = data[i].primary_attr;
        scores[2] = data[i].secondary_attr;

        KV kv(data[i].query_index, data[i].key, data[i].key, 0, scores);
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

TEST(TopsterTest, StableSorting) {
    // evaluate if the positions of the documents in Topster<1000> is the same in Topster 250, 500 and 750

    std::ifstream infile("test/resources/record_values.txt");
    std::string line;
    std::vector<std::pair<uint64_t, int64_t>> records;

    while (std::getline(infile, line)) {
        std::vector<std::string> parts;
        StringUtils::split(line, parts, ",");
        uint64_t key = std::stoll(parts[0]);
        records.emplace_back(key, std::stoi(parts[1]));
    }

    infile.close();

    Topster<KV> topster1K(1000);

    for(auto id_score: records) {
        int64_t scores[3] = {id_score.second, 0, 0};
        KV kv(0, id_score.first, id_score.first, 0, scores);
        topster1K.add(&kv);
    }

    topster1K.sort();

    std::vector<uint64_t> record_ids;

    for(uint32_t i = 0; i < topster1K.size; i++) {
        record_ids.push_back(topster1K.getKeyAt(i));
    }

    // check on Topster<250>
    Topster<KV> topster250(250);

    for(auto id_score: records) {
        int64_t scores[3] = {id_score.second, 0, 0};
        KV kv(0, id_score.first, id_score.first, 0, scores);
        topster250.add(&kv);
    }

    topster250.sort();

    for(uint32_t i = 0; i < topster250.size; i++) {
        ASSERT_EQ(record_ids[i], topster250.getKeyAt(i));
    }

    // check on Topster<500>
    Topster<KV> topster500(500);

    for(auto id_score: records) {
        int64_t scores[3] = {id_score.second, 0, 0};
        KV kv(0, id_score.first, id_score.first, 0, scores);
        topster500.add(&kv);
    }

    topster500.sort();

    for(uint32_t i = 0; i < topster500.size; i++) {
        ASSERT_EQ(record_ids[i], topster500.getKeyAt(i));
    }

    // check on Topster<750>
    Topster<KV> topster750(750);

    for(auto id_score: records) {
        int64_t scores[3] = {id_score.second, 0, 0};
        KV kv(0, id_score.first, id_score.first, 0, scores);
        topster750.add(&kv);
    }

    topster750.sort();

    for(uint32_t i = 0; i < topster750.size; i++) {
        ASSERT_EQ(record_ids[i], topster750.getKeyAt(i));
    }
}

TEST(TopsterTest, MaxFloatValues) {
    Topster<KV> topster(5);

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
        {0, 4, 11, 7.912, 30},
        {1, 5, 11, 0.0, 34},
        {0, 6, 11, -22, 30},
        {2, 7, 11, -22, 30},
        {1, 8, 11, -9.998, 30},
        {1, 8, 11, -9.998, 30},
        {0, 9, 11, -9.999, 30},
        {3, 10, 11, -20, 30},
    };

    for(int i = 0; i < 12; i++) {
        int64_t scores[3];
        scores[0] = int64_t(data[i].match_score);
        scores[1] = Index::float_to_int64_t(data[i].primary_attr);
        scores[2] = data[i].secondary_attr;

        KV kv(data[i].query_index, data[i].key, data[i].key, 0, scores);
        topster.add(&kv);
    }

    topster.sort();

    std::vector<uint64_t> ids = {4, 1, 5, 8, 9};

    for(uint32_t i = 0; i < topster.size; i++) {
        EXPECT_EQ(ids[i], topster.getKeyAt(i));
    }
}

TEST(TopsterTest, DistinctIntValues) {
    Topster<KV> dist_topster(5, 2, false);

    struct {
        uint16_t query_index;
        uint64_t distinct_key;
        uint64_t match_score;
        int64_t primary_attr;
        int64_t secondary_attr;
    } data[14] = {
        {0, 1, 11, 20, 30},
        {0, 1, 12, 20, 32},
        {0, 2, 4, 20, 30},
        {2, 3, 7, 20, 30},
        {0, 4, 14, 20, 30},
        {1, 5, 9, 20, 30},
        {1, 5, 10, 20, 32},
        {1, 5, 9, 20, 30},
        {0, 6, 6, 20, 30},
        {2, 7, 6, 22, 30},
        {2, 7, 6, 22, 30},
        {1, 8, 9, 20, 30},
        {0, 9, 8, 20, 30},
        {3, 10,  5, 20, 30},
    };

    for(int i = 0; i < 14; i++) {
        int64_t scores[3];
        scores[0] = int64_t(data[i].match_score);
        scores[1] = data[i].primary_attr;
        scores[2] = data[i].secondary_attr;

        KV kv(data[i].query_index, i+100, data[i].distinct_key, 0, scores);
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

    Topster<KV> dist_topster_first_pass(7, 2, true);

    for(int i = 0; i < 14; i++) {
        int64_t scores[3];
        scores[0] = int64_t(data[i].match_score);
        scores[1] = data[i].primary_attr;
        scores[2] = data[i].secondary_attr;

        KV kv(data[i].query_index, i+100, data[i].distinct_key, 0, scores);
        dist_topster_first_pass.add(&kv);
    }

    dist_topster_first_pass.sort(); // No need to sort the topster in case of group_by first-pass.

    distinct_ids = {7, 5, 3, 4, 1, 9, 8};
    std::vector<uint64_t> ids = {110, 106, 103, 104, 101, 112, 111};

    for(uint32_t i = 0; i < dist_topster_first_pass.size; i++) {
        ASSERT_EQ(distinct_ids[i], dist_topster_first_pass.getDistinctKeyAt(i));
        ASSERT_EQ(ids[i], dist_topster_first_pass.getKeyAt(i));
    }

    ASSERT_TRUE(dist_topster_first_pass.group_kv_map.empty());
    ASSERT_EQ(10, dist_topster_first_pass.loglog_counter->cardinality());
}

TEST(TopsterTest, GroupAllowlistBoundsSecondPassAggregations) {
    auto group_key_allowlist = std::make_shared<spp::sparse_hash_set<uint64_t>>();
    group_key_allowlist->insert(2);
    group_key_allowlist->insert(997);
    Topster<KV> topster(250, 3, false, group_found_params_t{}, true, group_key_allowlist);
    group_key_allowlist.reset();

    ASSERT_TRUE(topster.is_group_key_allowed(2));
    ASSERT_TRUE(topster.is_group_key_allowed(997));
    ASSERT_FALSE(topster.is_group_key_allowed(3));

    for (uint64_t group_key = 0; group_key < 1000; group_key++) {
        for (uint64_t document_index = 0; document_index < 5; document_index++) {
            const uint64_t document_id = group_key * 5 + document_index;
            int64_t scores[3] = {static_cast<int64_t>(document_id), 0, 0};
            KV kv(0, document_id, group_key, 0, scores);
            topster.add(&kv);
        }
    }

    ASSERT_EQ(2, topster.group_kv_map.size());
    ASSERT_EQ(10, topster.group_doc_seq_ids.size());
    ASSERT_EQ(3, topster.group_kv_map.at(2)->size);
    ASSERT_EQ(3, topster.group_kv_map.at(997)->size);
}

TEST(TopsterTest, EmptyGroupAllowlistRejectsEveryGroup) {
    auto empty_group_key_allowlist = std::make_shared<spp::sparse_hash_set<uint64_t>>();
    Topster<KV> topster(10, 3, false, group_found_params_t{}, true, empty_group_key_allowlist);
    ASSERT_FALSE(topster.is_group_key_allowed(42));

    int64_t scores[3] = {0, 0, 0};
    KV kv(0, 1, 42, 0, scores);
    ASSERT_EQ(2, topster.add(&kv));
    ASSERT_TRUE(topster.group_kv_map.empty());
    ASSERT_TRUE(topster.group_doc_seq_ids.empty());
}

TEST(TopsterTest, UnionKVKeyIsUniquePerDocument) {
    // Every collection numbers its documents from 0, so a union sees the same sequence ids from
    // every collection and search. The keys must still tell all of them apart.
    int64_t scores[3] = {0, 0, 0};
    std::unordered_set<uint64_t> collection_keys, search_keys;
    const uint32_t ids = 32, seq_ids = 4096;

    for(uint32_t id = 0; id < ids; id++) {
        for(uint32_t seq_id = 0; seq_id < seq_ids; seq_id++) {
            KV kv(0, seq_id, seq_id, 0, scores);
            Union_KV by_collection(kv, id, id, true);
            Union_KV by_search(kv, id, id, false);
            collection_keys.insert(Union_KV::get_key(&by_collection));
            search_keys.insert(Union_KV::get_key(&by_search));
        }
    }

    ASSERT_EQ(ids * seq_ids, collection_keys.size());
    ASSERT_EQ(ids * seq_ids, search_keys.size());
}
