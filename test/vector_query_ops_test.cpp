#include <gtest/gtest.h>
#include "vector_query_ops.h"

class VectorQueryOpsTest : public ::testing::Test {
protected:
    void setupCollection() {
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {

    }
};

TEST_F(VectorQueryOpsTest, ParseVectorQueryString) {
    vector_query_t vector_query;
    auto parsed = VectorQueryOps::parse_vector_query_str("vec:([0.34, 0.66, 0.12, 0.68], k: 10)", vector_query, false, nullptr);
    ASSERT_TRUE(parsed.ok());
    ASSERT_EQ("vec", vector_query.field_name);
    ASSERT_EQ(10, vector_query.k);
    std::vector<float> fvs = {0.34, 0.66, 0.12, 0.68};
    ASSERT_EQ(fvs.size(), vector_query.values.size());
    for (size_t i = 0; i < fvs.size(); i++) {
        ASSERT_EQ(fvs[i], vector_query.values[i]);
    }

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:([0.34, 0.66, 0.12, 0.68], k: 10)", vector_query, false, nullptr);
    ASSERT_TRUE(parsed.ok());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:([])", vector_query, false, nullptr);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("When a vector query value is empty, an `id` parameter must be present.", parsed.error());

    // cannot pass both vector and id
    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:([0.34, 0.66, 0.12, 0.68], id: 10)", vector_query, false, nullptr);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("Malformed vector query string: cannot pass both vector query and `id` parameter.", parsed.error());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:([], k: 10)", vector_query, false, nullptr);
    ASSERT_TRUE(parsed.ok());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:([], k: 10)", vector_query, true, nullptr);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("When a vector query value is empty, an `id` parameter must be present.", parsed.error());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:[0.34, 0.66, 0.12, 0.68], k: 10)", vector_query, false, nullptr);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("Malformed vector query string.", parsed.error());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:([0.34, 0.66, 0.12, 0.68], k: 10", vector_query, false, nullptr);
    ASSERT_TRUE(parsed.ok());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:(0.34, 0.66, 0.12, 0.68, k: 10)", vector_query, false, nullptr);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("Malformed vector query string.", parsed.error());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec:([0.34, 0.66, 0.12, 0.68], )", vector_query, false, nullptr);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("Malformed vector query string.", parsed.error());

    vector_query._reset();
    parsed = VectorQueryOps::parse_vector_query_str("vec([0.34, 0.66, 0.12, 0.68])", vector_query, false, nullptr);
    ASSERT_FALSE(parsed.ok());
    ASSERT_EQ("Malformed vector query string.", parsed.error());
}
