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
