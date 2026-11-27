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

    for(size_t i = 0; i < 300; i++) {
        longval += "a";
    }

    facet_value_id_t longfval(longval.substr(0, 255), 1);

    fvalue_to_seq_ids[longfval] = {0, 1, 2};
    seq_id_to_fvalues[0] = {longfval};
    seq_id_to_fvalues[1] = {longfval};
    seq_id_to_fvalues[2] = {longfval};

    field brandf("brand", field_types::STRING, true);
    nlohmann::json doc;
    doc["brand"] = longval;

    findex.insert("brand", fvalue_to_seq_ids, seq_id_to_fvalues, true);
    ASSERT_EQ(3, findex.facet_val_num_ids("brand", longval.substr(0, 255)));

    findex.remove(doc, brandf, 0);
    findex.remove(doc, brandf, 1);
    ASSERT_EQ(1, findex.facet_val_num_ids("brand", longval.substr(0, 255)));

    findex.remove(doc, brandf, 2);
    ASSERT_FALSE(findex.facet_value_exists("brand", longval.substr(0, 255)));
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

TEST(FacetIndexTest, FacetCountIteratorSurvivesRepeatedUpdatesAndReorders) {
    facet_index_t findex;
    findex.initialize("brand");

    const facet_value_id_t nike("nike", 1);
    const facet_value_id_t adidas("adidas", 2);

    auto insert_batch = [&](const facet_value_id_t& fvalue, const std::vector<uint32_t>& seq_ids) {
        std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
        std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;

        fvalue_to_seq_ids[fvalue] = seq_ids;
        for(const auto seq_id : seq_ids) {
            seq_id_to_fvalues[seq_id] = {fvalue};
        }

        findex.insert("brand", fvalue_to_seq_ids, seq_id_to_fvalues, true);
    };

    field brandf("brand", field_types::STRING, true);
    nlohmann::json nike_doc;
    nike_doc["brand"] = "nike";

    nlohmann::json adidas_doc;
    adidas_doc["brand"] = "adidas";

    insert_batch(adidas, {0, 1});
    insert_batch(nike, {2});
    ASSERT_EQ(2, findex.facet_node_count("brand", "adidas"));
    ASSERT_EQ(1, findex.facet_node_count("brand", "nike"));

    // Reinserting the same facet value forces the multiset node to be extracted and reinserted.
    insert_batch(nike, {3, 4, 5});
    ASSERT_EQ(4, findex.facet_val_num_ids("brand", "nike"));
    ASSERT_EQ(4, findex.facet_node_count("brand", "nike"));

    findex.remove(nike_doc, brandf, 2);
    ASSERT_EQ(3, findex.facet_val_num_ids("brand", "nike"));
    ASSERT_EQ(3, findex.facet_node_count("brand", "nike"));

    findex.remove(adidas_doc, brandf, 0);
    ASSERT_EQ(1, findex.facet_val_num_ids("brand", "adidas"));
    ASSERT_EQ(1, findex.facet_node_count("brand", "adidas"));

    insert_batch(adidas, {6, 7});
    ASSERT_EQ(3, findex.facet_val_num_ids("brand", "adidas"));
    ASSERT_EQ(3, findex.facet_node_count("brand", "adidas"));
}

TEST(FacetIndexTest, Int32NegativeOneAndOneFacetIdCollision) {
    facet_index_t findex;
    findex.initialize("experience_min");

    // Int32 value -1 has raw uint32 representation 0xFFFFFFFF (UINT32_MAX)
    int32_t val_neg = -1;
    uint32_t hash_neg = reinterpret_cast<uint32_t&>(val_neg);
    facet_value_id_t fval_neg("-1", hash_neg);

    // Int32 value 1 has raw uint32 representation 1
    int32_t val_pos = 1;
    uint32_t hash_pos = reinterpret_cast<uint32_t&>(val_pos);
    facet_value_id_t fval_pos("1", hash_pos);

    // Insert document 0 with -1
    {
        std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
        std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;
        fvalue_to_seq_ids[fval_neg] = {0};
        seq_id_to_fvalues[0] = {fval_neg};
        findex.insert("experience_min", fvalue_to_seq_ids, seq_id_to_fvalues, false);
    }

    // Insert document 1 with 1
    {
        std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash> fvalue_to_seq_ids;
        std::unordered_map<uint32_t, std::vector<facet_value_id_t>> seq_id_to_fvalues;
        fvalue_to_seq_ids[fval_pos] = {1};
        seq_id_to_fvalues[1] = {fval_pos};
        findex.insert("experience_min", fvalue_to_seq_ids, seq_id_to_fvalues, false);
    }

    // Verify -1 and 1 resolve to their distinct string representations without collision
    ASSERT_EQ("-1", findex.get_facet_str_val("experience_min", hash_neg));
    ASSERT_EQ("1", findex.get_facet_str_val("experience_min", hash_pos));

    ASSERT_EQ(1, findex.facet_val_num_ids("experience_min", "-1"));
    ASSERT_EQ(1, findex.facet_val_num_ids("experience_min", "1"));
}
