#include <gtest/gtest.h>
#include "facet_index.h"

TEST(FacetIndexTest, FacetValueDeletion) {
    facet_index_t findex;
    std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
    std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;

    facet_value_id_t nike("nike", 1);

    fvalue_to_seq_ids[nike] = {0, 1, 2};
    seq_id_to_fvalues[0] = {nike};
    seq_id_to_fvalues[1] = {nike};
    seq_id_to_fvalues[2] = {nike};

    findex.insert("brands", fvalue_to_seq_ids, seq_id_to_fvalues, true);
    findex.remove("nike", 0);
    findex.remove("nike", 1);
    findex.remove("nike", 2);

    ASSERT_FALSE(findex.facet_value_exists("brands", "nike"));
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
}
