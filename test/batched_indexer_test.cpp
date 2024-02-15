#include <gtest/gtest.h>
#include <vector>
#include "batched_indexer.h"

class BatchedIndexerTest : public ::testing::Test {};

TEST_F(BatchedIndexerTest, GroupRelatedCollections) {
    std::unordered_map<std::string, std::string> coll_name_to_group_id;
    std::unordered_map<std::string, std::unordered_set<std::string>> group_id_to_collections;

    // E <- A <- B -> C <- D  F
    BatchedIndexer::group_related_collections("D", {"C"}, coll_name_to_group_id, group_id_to_collections);
    ASSERT_EQ(coll_name_to_group_id["D"], "DC");
    ASSERT_EQ(coll_name_to_group_id["C"], "DC");

    std::vector<std::string> group_collections = {"D", "C"};
    ASSERT_EQ(group_id_to_collections.count("DC"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["DC"].count(coll), 1);
    }

    BatchedIndexer::group_related_collections("A", {"E"}, coll_name_to_group_id, group_id_to_collections);
    ASSERT_EQ(coll_name_to_group_id["D"], "DC");
    ASSERT_EQ(coll_name_to_group_id["C"], "DC");
    ASSERT_EQ(coll_name_to_group_id["A"], "AE");
    ASSERT_EQ(coll_name_to_group_id["E"], "AE");

    group_collections = {"D", "C"};
    ASSERT_EQ(group_id_to_collections.count("DC"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["DC"].count(coll), 1);
    }
    group_collections = {"A", "E"};
    ASSERT_EQ(group_id_to_collections.count("AE"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["AE"].count(coll), 1);
    }

    BatchedIndexer::group_related_collections("B", {"A", "C"}, coll_name_to_group_id, group_id_to_collections);
    ASSERT_EQ(coll_name_to_group_id["D"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["C"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["A"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["E"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["B"], "BAEDC");

    group_collections = {"B", "A", "E", "D", "C"};
    ASSERT_EQ(group_id_to_collections.count("DC"), 0);
    ASSERT_EQ(group_id_to_collections.count("AE"), 0);
    ASSERT_EQ(group_id_to_collections.count("BAEDC"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["BAEDC"].count(coll), 1);
    }

    BatchedIndexer::group_related_collections("C", {}, coll_name_to_group_id, group_id_to_collections);
    ASSERT_EQ(coll_name_to_group_id["D"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["C"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["A"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["E"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["B"], "BAEDC");

    group_collections = {"B", "A", "E", "D", "C"};
    ASSERT_EQ(group_id_to_collections.count("BAEDC"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["BAEDC"].count(coll), 1);
    }

    BatchedIndexer::group_related_collections("E", {}, coll_name_to_group_id, group_id_to_collections);
    ASSERT_EQ(coll_name_to_group_id["D"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["C"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["A"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["E"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["B"], "BAEDC");

    group_collections = {"B", "A", "E", "D", "C"};
    ASSERT_EQ(group_id_to_collections.count("BAEDC"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["BAEDC"].count(coll), 1);
    }

    BatchedIndexer::group_related_collections("F", {}, coll_name_to_group_id, group_id_to_collections);
    ASSERT_EQ(coll_name_to_group_id["D"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["C"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["A"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["E"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["B"], "BAEDC");
    ASSERT_EQ(coll_name_to_group_id["F"], "F");

    group_collections = {"B", "A", "E", "D", "C"};
    ASSERT_EQ(group_id_to_collections.count("BAEDC"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["BAEDC"].count(coll), 1);
    }
    group_collections = {"F"};
    ASSERT_EQ(group_id_to_collections.count("F"), 1);
    for (const auto &coll: group_collections) {
        ASSERT_EQ(group_id_to_collections["F"].count(coll), 1);
    }
}