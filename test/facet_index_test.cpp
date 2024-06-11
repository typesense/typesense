#include <gtest/gtest.h>
#include "facet_index.h"

TEST(FacetIndexTest, FacetValueDeletionString) {
    facet_index_t findex;
    findex.initialize("brand");

    std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
    std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;

    facet_value_id_t nike("nike", 1);

    fvalue_to_seq_ids[nike] = {0, 1, 2};
    seq_id_to_fvalues[0] = {nike};
    seq_id_to_fvalues[1] = {nike};
    seq_id_to_fvalues[2] = {nike};

    field brandf("brand", field_types::STRING, true);
    nlohmann::json doc;
    doc["brand"] = "nike";

    findex.insert("brand", fvalue_to_seq_ids, seq_id_to_fvalues, true);
    ASSERT_EQ(3, findex.facet_val_num_ids("brand", "nike"));

    findex.remove(doc, brandf, 0);
    findex.remove(doc, brandf, 1);
    ASSERT_EQ(1, findex.facet_val_num_ids("brand", "nike"));

    findex.remove(doc, brandf, 2);
    ASSERT_FALSE(findex.facet_value_exists("brand", "nike"));
}

TEST(FacetIndexTest, HighCardinalityCheck) {
    facet_index_t findex;

    for(size_t i = 0; i < 101; i++) {
        findex.initialize("field_" + std::to_string(i));
    }

    std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
    std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;

    facet_value_id_t nike("nike", 1);

    fvalue_to_seq_ids[nike] = {0, 1, 2};
    seq_id_to_fvalues[0] = {nike};
    seq_id_to_fvalues[1] = {nike};
    seq_id_to_fvalues[2] = {nike};

    findex.insert("field_1", fvalue_to_seq_ids, seq_id_to_fvalues, true);
    ASSERT_EQ(3, findex.facet_val_num_ids("field_1", "nike"));

    findex.check_for_high_cardinality("field_1", 20000);
    ASSERT_TRUE(findex.facet_value_exists("field_1", "nike"));

    ASSERT_EQ(0, findex.facet_val_num_ids("field_1", "nike"));
}

TEST(FacetIndexTest, FacetValueDeletionOfLongString) {
    facet_index_t findex;
    findex.initialize("brand");

    std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
    std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;

    std::string longval;

    for(size_t i = 0; i < 200; i++) {
        longval += "a";
    }

    facet_value_id_t longfval(longval.substr(0, 100), 1);

    fvalue_to_seq_ids[longfval] = {0, 1, 2};
    seq_id_to_fvalues[0] = {longfval};
    seq_id_to_fvalues[1] = {longfval};
    seq_id_to_fvalues[2] = {longfval};

    field brandf("brand", field_types::STRING, true);
    nlohmann::json doc;
    doc["brand"] = longval;

    findex.insert("brand", fvalue_to_seq_ids, seq_id_to_fvalues, true);
    ASSERT_EQ(3, findex.facet_val_num_ids("brand", longval.substr(0, 100)));

    findex.remove(doc, brandf, 0);
    findex.remove(doc, brandf, 1);
    ASSERT_EQ(1, findex.facet_val_num_ids("brand", longval.substr(0, 100)));

    findex.remove(doc, brandf, 2);
    ASSERT_FALSE(findex.facet_value_exists("brand", longval.substr(0, 100)));
}

TEST(FacetIndexTest, FacetValueDeletionFloat) {
    facet_index_t findex;
    findex.initialize("price");
    std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
    std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;

    facet_value_id_t price1("99.95", 1);

    fvalue_to_seq_ids[price1] = {0, 1, 2};
    seq_id_to_fvalues[0] = {price1};
    seq_id_to_fvalues[1] = {price1};
    seq_id_to_fvalues[2] = {price1};

    field pricef("price", field_types::FLOAT, true);
    nlohmann::json doc;
    doc["price"] = 99.95;

    findex.insert("price", fvalue_to_seq_ids, seq_id_to_fvalues, true);
    ASSERT_EQ(3, findex.facet_val_num_ids("price", "99.95"));

    findex.remove(doc, pricef, 0);
    findex.remove(doc, pricef, 1);
    ASSERT_EQ(1, findex.facet_val_num_ids("price", "99.95"));

    findex.remove(doc, pricef, 2);
    ASSERT_FALSE(findex.facet_value_exists("price", "99.95"));
}

TEST(FacetIndexTest, UpdateWhenAllCountsLessThanNewCount) {
    // 5, 4, [4 -> 7]
    std::list<facet_index_t::facet_count_t> count_list;
    count_list.push_back(facet_index_t::facet_count_t("", 5, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 4, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 4, 2));

    std::map<uint32_t, std::list<facet_index_t::facet_count_t>::iterator> count_map;
    count_map.emplace(5, count_list.begin());
    count_map.emplace(4, std::prev(count_list.end()));

    auto curr = std::prev(count_list.end());
    curr->count = 7;
    facet_index_t::update_count_nodes(count_list, count_map, 4, 7, curr);

    // New order: 7, 5, 4
    ASSERT_EQ(7, count_list.begin()->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(4, std::next(count_list.begin(), 2)->count);

    ASSERT_EQ(3, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[5]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[4]);

    // 5, 4, [4 -> 7], 3

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 5, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 4, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 4, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 3, 3));

    count_map.emplace(5, count_list.begin());
    count_map.emplace(4, std::next(count_list.begin(), 2));
    count_map.emplace(3, std::next(count_list.begin(), 3));

    curr = std::next(count_list.begin(), 2);
    curr->count = 7;
    facet_index_t::update_count_nodes(count_list, count_map, 4, 7, curr);

    // New order: 7, 5, 4, 3
    ASSERT_EQ(7, count_list.begin()->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(4, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(3, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(4, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[5]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[4]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[3]);

    // 5, [4 -> 7]

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 5, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 4, 1));

    count_map.emplace(5, count_list.begin());
    count_map.emplace(4, std::next(count_list.begin(), 1));

    curr = std::next(count_list.begin(), 1);
    curr->count = 7;
    facet_index_t::update_count_nodes(count_list, count_map, 4, 7, curr);

    // New order: 7, 5
    ASSERT_EQ(7, count_list.begin()->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 1)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[5]);

    // [4 -> 5]

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 4, 0));
    count_map.emplace(4, count_list.begin());

    curr = count_list.begin();
    curr->count = 5;
    facet_index_t::update_count_nodes(count_list, count_map, 4, 5, curr);

    ASSERT_EQ(1, count_list.size());
    ASSERT_EQ(5, count_list.begin()->count);

    ASSERT_EQ(1, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[5]);
}

TEST(FacetIndexTest, UpdateWhenCountAlreadyExists) {
    // 10, 7, [5 -> 7], 3
    std::list<facet_index_t::facet_count_t> count_list;
    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 3, 3));

    std::map<uint32_t, std::list<facet_index_t::facet_count_t>::iterator> count_map;
    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 1));
    count_map.emplace(5, std::next(count_list.begin(), 2));
    count_map.emplace(3, std::next(count_list.begin(), 3));

    auto curr = std::next(count_list.begin(), 2);
    curr->count = 7;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 7, curr);

    // New order: 10, 7, 7, 3
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(3, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(3, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[3]);

    // 10, 7, 5, [5 -> 7]

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 3));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 1));
    count_map.emplace(5, std::next(count_list.begin(), 3));

    curr = std::next(count_list.begin(), 3);
    curr->count = 7;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 7, curr);

    // New order: 10, [7], 7, 5
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(3, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[5]);

    // 10, 7, 5, [5 -> 8]

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 3));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 1));
    count_map.emplace(5, std::next(count_list.begin(), 3));

    curr = std::next(count_list.begin(), 3);
    curr->count = 8;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 8, curr);

    // New order: 10, [8], 7, 5
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(8, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(4, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[8]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[5]);

    // 10, 7, [5 -> 7]

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 1));
    count_map.emplace(5, std::next(count_list.begin(), 2));

    curr = std::next(count_list.begin(), 2);
    curr->count = 7;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 7, curr);

    // New order: 10, [7], 7
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);

    // 10, 7, [5 -> 7], 5

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 3));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 1));
    count_map.emplace(5, std::next(count_list.begin(), 3));

    curr = std::next(count_list.begin(), 2);
    curr->count = 7;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 7, curr);

    // New order: 10, [7], 7, 5
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(3, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[5]);
}

TEST(FacetIndexTest, UpdateWhenGreaterNodeExists) {
    // 10, 7, [7 -> 9], 3
    std::list<facet_index_t::facet_count_t> count_list;
    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 3, 3));

    std::map<uint32_t, std::list<facet_index_t::facet_count_t>::iterator> count_map;
    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 2));
    count_map.emplace(3, std::next(count_list.begin(), 3));

    auto curr = std::next(count_list.begin(), 2);
    curr->count = 9;
    facet_index_t::update_count_nodes(count_list, count_map, 7, 9, curr);

    // New order: 10, [9], 7, 3
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(9, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(3, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(4, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[9]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[3]);

    // 10, 7, [7 -> 9]

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 2));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 2));

    curr = std::next(count_list.begin(), 2);
    curr->count = 9;
    facet_index_t::update_count_nodes(count_list, count_map, 7, 9, curr);

    // New order: 10, [9], 7
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(9, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);

    ASSERT_EQ(3, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[9]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);

    // 10, [7 -> 9]

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 1));

    curr = std::next(count_list.begin(), 1);
    curr->count = 9;
    facet_index_t::update_count_nodes(count_list, count_map, 7, 9, curr);

    // New order: 10, [9]
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(9, std::next(count_list.begin(), 1)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[9]);

    // 10, [7 -> 9], 7

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 2));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 2));

    curr = std::next(count_list.begin(), 1);
    curr->count = 9;
    facet_index_t::update_count_nodes(count_list, count_map, 7, 9, curr);

    // New order: 10, [9], 7
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(9, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);

    ASSERT_EQ(3, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[9]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);

    // 10, 7, [5 -> 9], 3

    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 7, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 3, 3));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(7, std::next(count_list.begin(), 1));
    count_map.emplace(5, std::next(count_list.begin(), 2));
    count_map.emplace(3, std::next(count_list.begin(), 3));

    curr = std::next(count_list.begin(), 2);
    curr->count = 9;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 9, curr);

    // New order: 10, [9], 7, 3
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(9, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(7, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(3, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(4, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[9]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[7]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[3]);
}

TEST(FacetIndexTest, DecrementSingleCount) {
    // [10 -> 9]
    std::list<facet_index_t::facet_count_t> count_list;
    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));

    std::map<uint32_t, std::list<facet_index_t::facet_count_t>::iterator> count_map;
    count_map.emplace(10, count_list.begin());

    auto curr = count_list.begin();
    curr->count = 9;
    facet_index_t::update_count_nodes(count_list, count_map, 10, 9, curr);

    ASSERT_EQ(1, count_list.size());
    ASSERT_EQ(9, count_list.begin()->count);

    ASSERT_EQ(1, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[9]);

    // [9 -> 8], 8
    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 9, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 8, 1));

    count_map.emplace(9, count_list.begin());
    count_map.emplace(8, std::next(count_list.begin(), 1));

    curr = count_list.begin();
    curr->count = 8;
    facet_index_t::update_count_nodes(count_list, count_map, 9, 8, curr);

    ASSERT_EQ(2, count_list.size());
    ASSERT_EQ(8, count_list.begin()->count);
    ASSERT_EQ(8, std::next(count_list.begin(), 1)->count);

    ASSERT_EQ(1, count_map.size());
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[8]);

    // 10, [9 -> 8], 8
    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 10, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 9, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 8, 2));

    count_map.emplace(10, count_list.begin());
    count_map.emplace(9, std::next(count_list.begin(), 1));
    count_map.emplace(8, std::next(count_list.begin(), 2));

    curr = std::next(count_list.begin(), 1);
    curr->count = 8;
    facet_index_t::update_count_nodes(count_list, count_map, 9, 8, curr);

    ASSERT_EQ(3, count_list.size());
    ASSERT_EQ(10, count_list.begin()->count);
    ASSERT_EQ(8, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(8, std::next(count_list.begin(), 2)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[10]);
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[8]);

    // [5 -> 4], 2
    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 5, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 2, 1));

    count_map.emplace(5, count_list.begin());
    count_map.emplace(2, std::next(count_list.begin(), 1));

    curr = count_list.begin();
    curr->count = 4;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 4, curr);

    ASSERT_EQ(2, count_list.size());
    ASSERT_EQ(4, count_list.begin()->count);
    ASSERT_EQ(2, std::next(count_list.begin(), 1)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[4]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[2]);

    // [5 -> 1], 2
    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 5, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 2, 1));

    count_map.emplace(5, count_list.begin());
    count_map.emplace(2, std::next(count_list.begin(), 1));

    curr = count_list.begin();
    curr->count = 1;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 1, curr);

    ASSERT_EQ(2, count_list.size());
    ASSERT_EQ(2, count_list.begin()->count);
    ASSERT_EQ(1, std::next(count_list.begin(), 1)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(count_list.begin(), count_map[2]);
    ASSERT_EQ(std::next(count_list.begin(), 1), count_map[1]);

    // 5, 5, [5 -> 4], 5
    // new order: 5, 5, 5, 4
    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 5, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 3));

    count_map.emplace(5, std::next(count_list.begin(), 3));

    curr = std::next(count_list.begin(), 2);
    curr->count = 4;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 4, curr);

    ASSERT_EQ(4, count_list.size());
    ASSERT_EQ(5, count_list.begin()->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(4, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[5]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[4]);

    // 5, 5, 5, [5 -> 4]
    // new order: 5, 5, 5, 4
    count_list.clear();
    count_map.clear();

    count_list.push_back(facet_index_t::facet_count_t("", 5, 0));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 1));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 2));
    count_list.push_back(facet_index_t::facet_count_t("", 5, 3));

    count_map.emplace(5, std::next(count_list.begin(), 3));

    curr = std::next(count_list.begin(), 3);
    curr->count = 4;
    facet_index_t::update_count_nodes(count_list, count_map, 5, 4, curr);

    ASSERT_EQ(4, count_list.size());
    ASSERT_EQ(5, count_list.begin()->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 1)->count);
    ASSERT_EQ(5, std::next(count_list.begin(), 2)->count);
    ASSERT_EQ(4, std::next(count_list.begin(), 3)->count);

    ASSERT_EQ(2, count_map.size());
    ASSERT_EQ(std::next(count_list.begin(), 2), count_map[5]);
    ASSERT_EQ(std::next(count_list.begin(), 3), count_map[4]);
}
