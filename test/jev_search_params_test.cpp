#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include "jev_search_params.h"
#include "jev_client.h"
#include "filter.h"
#include "field.h"
#include "json.hpp"

class JevSearchParamsTest : public ::testing::Test {
protected:
    void SetUp() override {
        JevClient::clear_mock_responses();
        JevClient::enable_request_capture();
    }

    void TearDown() override {
        JevClient::clear_mock_responses();
        JevClient::disable_request_capture();
    }
};

// every field the catalog helpers below name has to exist here or parse_filter_query 404s
static tsl::htrie_map<char, field> test_schema() {
    tsl::htrie_map<char, field> schema;
    schema.emplace("brand", field("brand", field_types::STRING, true));
    schema.emplace("categories", field("categories", field_types::STRING_ARRAY, true));
    schema.emplace("rating_label", field("rating_label", field_types::STRING, true));
    schema.emplace("price", field("price", field_types::FLOAT, false));
    schema.emplace("year", field("year", field_types::INT32, false));
    schema.emplace("total_reviews_count", field("total_reviews_count", field_types::INT32, false));
    schema.emplace("food", field("food", field_types::FLOAT, false));
    schema.emplace("service", field("service", field_types::FLOAT, false));
    schema.emplace("in_stock", field("in_stock", field_types::BOOL, false));
    schema.emplace("gluten_free", field("gluten_free", field_types::BOOL, false));
    schema.emplace("vegetarian_friendly", field("vegetarian_friendly", field_types::BOOL, false));
    return schema;
}

// validate_max_ops reads CollectionManager::filter_by_max_ops, zero in the test binary
static bool filter_parses(const std::string& filter_by) {
    auto schema = test_schema();
    filter_node_t* root = nullptr;
    auto parse_op = filter::parse_filter_query(filter_by, schema, nullptr, "", root, true, "", false);
    std::unique_ptr<filter_node_t> guard(root);
    return parse_op.ok();
}

static jev_catalog_t brand_catalog() {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.facet_fields.push_back({"brand", {"Sony", "Bose", "Sennheiser"}});
    return catalog;
}

static jev_query_facts_t facts_for(const std::string& query) {
    return JevSearchParams::pre_parse(query);
}

static nlohmann::json token_answer(double content, double excluded = 0.0) {
    const double filler = std::max(0.0, 1.0 - content - excluded);
    std::string choice = "filler";
    if(content >= excluded && content >= filler) {
        choice = "content";
    } else if(excluded >= filler) {
        choice = "excluded";
    }

    nlohmann::json answer;
    answer["type"] = "choice";
    answer["choice"] = choice;
    answer["confidence"] = std::max(content, std::max(excluded, filler));
    answer["probabilities"] = {{"content", content}, {"excluded", excluded}, {"filler", filler}};
    return answer;
}

TEST_F(JevSearchParamsTest, EscapePlainValueIsLeftAlone) {
    ASSERT_EQ("Sony", JevSearchParams::escape_filter_value("Sony"));
    ASSERT_EQ("rear wheel drive", JevSearchParams::escape_filter_value("rear wheel drive"));
}

TEST_F(JevSearchParamsTest, EscapeWrapsParentheses) {
    ASSERT_EQ("`Live (Remastered)`", JevSearchParams::escape_filter_value("Live (Remastered)"));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("Live (Remastered)")));
}

TEST_F(JevSearchParamsTest, EscapeWrapsBooleanOperators) {
    ASSERT_EQ("`Tom && Jerry`", JevSearchParams::escape_filter_value("Tom && Jerry"));
    ASSERT_EQ("`Rock || Pop`", JevSearchParams::escape_filter_value("Rock || Pop"));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("Tom && Jerry")));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("Rock || Pop")));
}

TEST_F(JevSearchParamsTest, EscapeWrapsCommasSoArraysDoNotSplit) {
    const std::string escaped = JevSearchParams::escape_filter_value("Smith, John");
    ASSERT_EQ("`Smith, John`", escaped);
    ASSERT_TRUE(filter_parses("brand:[" + escaped + ",Sony]"));
}

TEST_F(JevSearchParamsTest, EscapeWrapsLeadingComparators) {
    ASSERT_EQ("`=mc2`", JevSearchParams::escape_filter_value("=mc2"));
    ASSERT_EQ("`!important`", JevSearchParams::escape_filter_value("!important"));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("=mc2")));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("!important")));
}

TEST_F(JevSearchParamsTest, EscapeTrimsTrailingStarBecauseArraysReadItAsAPrefix) {
    // backticks are stripped inside [...], `Rated R*` would silently become a prefix match there
    ASSERT_EQ("Rated R", JevSearchParams::escape_filter_value("Rated R*"));
    ASSERT_EQ("Batman", JevSearchParams::escape_filter_value("*Batman*"));
    ASSERT_EQ("", JevSearchParams::escape_filter_value("***"));
}

TEST_F(JevSearchParamsTest, EscapeStripsEmbeddedBackticksAndBackslashes) {
    ASSERT_EQ("quoted", JevSearchParams::escape_filter_value("`quoted`"));
    ASSERT_EQ("a b", JevSearchParams::escape_filter_value("a\\ b"));
    ASSERT_EQ("cd", JevSearchParams::escape_filter_value("c\\`d"));
    ASSERT_TRUE(filter_parses("brand:[" + JevSearchParams::escape_filter_value("c\\`d") + ",Sony]"));
}

TEST_F(JevSearchParamsTest, EscapeStripsDoubleQuotesSoArraysDoNotPhraseMatch) {
    ASSERT_EQ("exact words", JevSearchParams::escape_filter_value("\"exact words\""));
}

TEST_F(JevSearchParamsTest, EscapeDropsValuesThatSanitizeToNothing) {
    ASSERT_EQ("", JevSearchParams::escape_filter_value(""));
    ASSERT_EQ("", JevSearchParams::escape_filter_value("   "));
    ASSERT_EQ("", JevSearchParams::escape_filter_value("``"));
}

TEST_F(JevSearchParamsTest, EscapeQuotesTheMissingValuesOperator) {
    // bare _missing is the missing values operator to the filter parser, backticked it is a literal
    ASSERT_EQ("`_missing`", JevSearchParams::escape_filter_value("_missing"));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("_missing")));
}

TEST_F(JevSearchParamsTest, EscapeWrapsBracketsAndColons) {
    ASSERT_EQ("`[boxed]`", JevSearchParams::escape_filter_value("[boxed]"));
    ASSERT_EQ("`a:b`", JevSearchParams::escape_filter_value("a:b"));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("[boxed]")));
    ASSERT_TRUE(filter_parses("brand:" + JevSearchParams::escape_filter_value("a:b")));
}

TEST_F(JevSearchParamsTest, PreParseNormalizesNumericLiterals) {
    auto facts = JevSearchParams::pre_parse("laptops under $1,200 but over 19.99 and 10k reviews");
    ASSERT_EQ(3, facts.numbers.size());
    ASSERT_EQ("1200", facts.numbers[0]);
    ASSERT_EQ("19.99", facts.numbers[1]);
    ASSERT_EQ("10000", facts.numbers[2]);
    ASSERT_EQ("$1,200", facts.number_spans[0]);
}

TEST_F(JevSearchParamsTest, PreParseDoesNotReadAWordAsAMultiplier) {
    auto facts = JevSearchParams::pre_parse("20 kids bikes");
    ASSERT_EQ(1, facts.numbers.size());
    ASSERT_EQ("20", facts.numbers[0]);
}

TEST_F(JevSearchParamsTest, PreParseFlagsPhraseAndExclusionOperators) {
    ASSERT_TRUE(JevSearchParams::pre_parse("\"noise cancelling\" headphones").has_query_operators);
    ASSERT_TRUE(JevSearchParams::pre_parse("headphones -wireless").has_query_operators);
    ASSERT_FALSE(JevSearchParams::pre_parse("red running shoes").has_query_operators);
}

TEST_F(JevSearchParamsTest, PreParseReadsADashDigitAsANegativeNumber) {
    auto facts = JevSearchParams::pre_parse("temperatures below -5");
    ASSERT_FALSE(facts.has_query_operators);
    ASSERT_EQ(1, facts.numbers.size());
    ASSERT_EQ("-5", facts.numbers[0]);
    ASSERT_EQ("-5", facts.number_spans[0]);
}

TEST_F(JevSearchParamsTest, PreParseReadsAHyphenatedRangeAsTwoPositives) {
    // the minus in `10-20` separates a range, only a minus not preceded by a digit is a sign
    auto facts = JevSearchParams::pre_parse("price between 10-20");
    ASSERT_EQ(2, facts.numbers.size());
    ASSERT_EQ("10", facts.numbers[0]);
    ASSERT_EQ("20", facts.numbers[1]);
    ASSERT_EQ("20", facts.number_spans[1]);

    auto dollars = JevSearchParams::pre_parse("from $1,200-$1,500");
    ASSERT_EQ(2, dollars.numbers.size());
    ASSERT_EQ("1200", dollars.numbers[0]);
    ASSERT_EQ("1500", dollars.numbers[1]);
    ASSERT_EQ("$1,500", dollars.number_spans[1]);

    auto suffixed = JevSearchParams::pre_parse("salaries 10k-20k");
    ASSERT_EQ(2, suffixed.numbers.size());
    ASSERT_EQ("10000", suffixed.numbers[0]);
    ASSERT_EQ("20000", suffixed.numbers[1]);
    ASSERT_EQ("20k", suffixed.number_spans[1]);

    auto millions = JevSearchParams::pre_parse("budget $1.5m-$2m");
    ASSERT_EQ(2, millions.numbers.size());
    ASSERT_EQ("1500000", millions.numbers[0]);
    ASSERT_EQ("2000000", millions.numbers[1]);
    ASSERT_EQ("$2m", millions.number_spans[1]);
}

TEST_F(JevSearchParamsTest, PreParseBoundsTheNumberScan) {
    // libstdc++ regex recursion overflowed the stack on an unbounded digit run and killed the server
    const std::string bomb(40000, '9');
    auto facts = JevSearchParams::pre_parse(bomb);
    ASSERT_EQ(1, facts.numbers.size());
}

TEST_F(JevSearchParamsTest, PreParseKeepsDuplicateNumberOccurrences) {
    auto facts = JevSearchParams::pre_parse("price under 50 and weight over 50");
    ASSERT_EQ(2, facts.numbers.size());
    ASSERT_EQ("50", facts.numbers[0]);
    ASSERT_EQ("50", facts.numbers[1]);
    ASSERT_NE(facts.number_contexts[0], facts.number_contexts[1]);
    ASSERT_TRUE(facts.number_contexts[0].find("price") != std::string::npos);
    ASSERT_TRUE(facts.number_contexts[1].find("weight") != std::string::npos);
}

TEST_F(JevSearchParamsTest, AssembleSingleFacetClause) {
    auto catalog = brand_catalog();
    auto facts = facts_for("sony headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.95},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.97,
                      "probabilities": {"Sony": 0.97, "Bose": 0.02, "Sennheiser": 0.01}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:=Sony", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleOrListFromTheSecondValueQuestion) {
    // the value choice concentrates its mass on one winner, the second value has its own question
    auto catalog = brand_catalog();
    auto facts = facts_for("sony or bose headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.93},
        "f0__negate": {"type": "noul", "noul": 0.01},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.95,
                      "probabilities": {"Sony": 0.95, "Bose": 0.03, "Sennheiser": 0.02}},
        "f0__value2": {"type": "choice", "choice": "Bose", "confidence": 0.88,
                       "probabilities": {"Sony": 0.02, "Bose": 0.88, "Sennheiser": 0.02, "__none__": 0.08}},
        "f0__both": {"type": "noul", "noul": 0.05}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:=[Sony,Bose]", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleAndPairFromTheBothJudgment) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"categories", {"Wheelchair Accessible", "Outdoor Seating", "Delivery"}});
    auto facts = facts_for("wheelchair accessible with outdoor seating");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.95},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Wheelchair Accessible", "confidence": 0.94,
                      "probabilities": {"Wheelchair Accessible": 0.94, "Outdoor Seating": 0.04, "Delivery": 0.02}},
        "f0__value2": {"type": "choice", "choice": "Outdoor Seating", "confidence": 0.9,
                       "probabilities": {"Wheelchair Accessible": 0.02, "Outdoor Seating": 0.9,
                                         "Delivery": 0.02, "__none__": 0.06}},
        "f0__both": {"type": "noul", "noul": 0.91}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("(categories:=Wheelchair Accessible && categories:=Outdoor Seating)",
              params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleRejectsASecondValueBelowThreshold) {
    // a confused split must not smuggle a second value in
    auto catalog = brand_catalog();
    auto facts = facts_for("sony headphones not from france");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.51,
                      "probabilities": {"Sony": 0.54, "Bose": 0.46}},
        "f0__value2": {"type": "choice", "choice": "Bose", "confidence": 0.3,
                       "probabilities": {"Sony": 0.05, "Bose": 0.35, "Sennheiser": 0.05, "__none__": 0.55}},
        "f0__both": {"type": "noul", "noul": 0.1}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:=Sony", params["filter_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleRejectsASecondValueEqualToThePrimary) {
    auto catalog = brand_catalog();
    auto facts = facts_for("sony headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.95,
                      "probabilities": {"Sony": 0.95, "Bose": 0.03, "Sennheiser": 0.02}},
        "f0__value2": {"type": "choice", "choice": "Sony", "confidence": 0.7,
                       "probabilities": {"Sony": 0.7, "Bose": 0.1, "Sennheiser": 0.1, "__none__": 0.1}},
        "f0__both": {"type": "noul", "noul": 0.1}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:=Sony", params["filter_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleNegatedClause) {
    auto catalog = brand_catalog();
    auto facts = facts_for("headphones that are not sony");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.91},
        "f0__negate": {"type": "noul", "noul": 0.88},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.94,
                      "probabilities": {"Sony": 0.94, "Bose": 0.04, "Sennheiser": 0.02}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:!=Sony", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleNegatedOrList) {
    // excluding two values means neither may appear, the both judgment has no say under negation
    auto catalog = brand_catalog();
    auto facts = facts_for("headphones that are neither sony nor bose");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.85},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.9,
                      "probabilities": {"Sony": 0.9, "Bose": 0.07, "Sennheiser": 0.03}},
        "f0__value2": {"type": "choice", "choice": "Bose", "confidence": 0.85,
                       "probabilities": {"Sony": 0.03, "Bose": 0.85, "Sennheiser": 0.04, "__none__": 0.08}},
        "f0__both": {"type": "noul", "noul": 0.88}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:!=[Sony,Bose]", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleDropsAWeakNegatedClause) {
    auto catalog = brand_catalog();
    auto facts = facts_for("headphones but not that one brand");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.7},
        "f0__negate": {"type": "noul", "noul": 0.85},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.5,
                      "probabilities": {"Sony": 0.55, "Bose": 0.3, "Sennheiser": 0.15}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleDemandsConvictionForASecondNegatedValue) {
    // a negated list holds every named value out of the results, a coin flip second value must not join
    auto catalog = brand_catalog();
    auto facts = facts_for("headphones that are not sony");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.88},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.92,
                      "probabilities": {"Sony": 0.92, "Bose": 0.05, "Sennheiser": 0.03}},
        "f0__value2": {"type": "choice", "choice": "Bose", "confidence": 0.5,
                       "probabilities": {"Sony": 0.05, "Bose": 0.58, "Sennheiser": 0.05, "__none__": 0.32}},
        "f0__both": {"type": "noul", "noul": 0.1}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:!=Sony", params["filter_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsClauseWhenPresentIsBelowThreshold) {
    auto catalog = brand_catalog();
    auto facts = facts_for("wireless headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.21},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.99,
                      "probabilities": {"Sony": 0.99, "Bose": 0.005, "Sennheiser": 0.005}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleDropsClauseWhenTheDistributionIsFlat) {
    auto catalog = brand_catalog();
    auto facts = facts_for("good headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.82},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.12,
                      "probabilities": {"Sony": 0.2, "Bose": 0.14, "Sennheiser": 0.13}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleDropsTheWholeClauseWhenEverythingScoresNone) {
    auto catalog = brand_catalog();
    auto facts = facts_for("panasonic headphones");

    // `field:[]` matches zero documents with no error at all, the clause has to disappear entirely
    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.88},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "__none__", "confidence": 0.9,
                      "probabilities": {"Sony": 0.03, "Bose": 0.03, "Sennheiser": 0.04, "__none__": 0.9}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleEscapesTheChosenFacetValue) {
    jev_catalog_t catalog;
    catalog.collection_name = "movies";
    catalog.facet_fields.push_back({"rating_label", {"Rated R", "PG (13)", "Tom && Jerry"}});
    auto facts = facts_for("pg 13 movies");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.01},
        "f0__value": {"type": "choice", "choice": "PG (13)", "confidence": 0.93,
                      "probabilities": {"Rated R": 0.04, "PG (13)": 0.93, "Tom && Jerry": 0.03}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("rating_label:=`PG (13)`", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleNumericComparisons) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    auto facts = facts_for("headphones under 200");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "price", "confidence": 0.93,
                        "probabilities": {"price": 0.93, "__none__": 0.07}},
        "num0__op": {"type": "choice", "choice": "lte", "confidence": 0.91,
                     "probabilities": {"eq": 0.01, "gt": 0.01, "gte": 0.02, "lt": 0.05, "lte": 0.91}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:<=200", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleBindsALiteralToExactlyOneField) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.numeric_fields.push_back({"food", field_types::INT32});
    catalog.numeric_fields.push_back({"service", field_types::INT32});
    catalog.numeric_fields.push_back({"total_reviews_count", field_types::INT32});
    auto facts = facts_for("gluten free spots with more than 500 reviews");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "total_reviews_count", "confidence": 0.84,
                        "probabilities": {"food": 0.05, "service": 0.04, "total_reviews_count": 0.84, "__none__": 0.07}},
        "num0__op": {"type": "choice", "choice": "gt", "confidence": 0.9,
                     "probabilities": {"eq": 0.02, "gt": 0.9, "gte": 0.05, "lt": 0.01, "lte": 0.02}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("total_reviews_count:>500", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
    ASSERT_EQ(1, params["llm_response"]["clauses"].size());
}

TEST_F(JevSearchParamsTest, AssembleFansALiteralOutToCoordinatedFields) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.numeric_fields.push_back({"food", field_types::FLOAT});
    catalog.numeric_fields.push_back({"service", field_types::FLOAT});
    catalog.numeric_fields.push_back({"total_reviews_count", field_types::INT32});
    auto facts = facts_for("5 stars food and service rating");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "food", "confidence": 0.5,
                        "probabilities": {"food": 0.55, "service": 0.4, "total_reviews_count": 0.02, "__none__": 0.03}},
        "num0__op": {"type": "choice", "choice": "eq", "confidence": 0.9,
                     "probabilities": {"eq": 0.9, "gte": 0.1}},
        "num0__also0": {"type": "noul", "noul": 0.95},
        "num0__also1": {"type": "noul", "noul": 0.9},
        "num0__also2": {"type": "noul", "noul": 0.1}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("food:5 && service:5", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleBindsALiteralWhenOwnersCompete) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.numeric_fields.push_back({"food", field_types::FLOAT});
    catalog.numeric_fields.push_back({"avg_rating", field_types::FLOAT});
    auto facts = facts_for("5 stars food rating");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "food", "confidence": 0.44,
                        "probabilities": {"food": 0.49, "avg_rating": 0.42, "__none__": 0.03}},
        "num0__op": {"type": "choice", "choice": "eq", "confidence": 0.85,
                     "probabilities": {"eq": 0.88, "gte": 0.12}},
        "num0__also0": {"type": "noul", "noul": 0.9},
        "num0__also1": {"type": "noul", "noul": 0.1}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("food:5", params["filter_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleKeepsASingleOwnerWhenAlsoNoulsStayLow) {
    // one field owns the number, the also nouls must not resurrect the bind everywhere defect
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.numeric_fields.push_back({"food", field_types::FLOAT});
    catalog.numeric_fields.push_back({"total_reviews_count", field_types::INT32});
    auto facts = facts_for("more than 500 reviews");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "total_reviews_count", "confidence": 0.85,
                        "probabilities": {"food": 0.05, "total_reviews_count": 0.87, "__none__": 0.08}},
        "num0__op": {"type": "choice", "choice": "gt", "confidence": 0.9,
                     "probabilities": {"eq": 0.02, "gt": 0.9, "gte": 0.08}},
        "num0__also0": {"type": "noul", "noul": 0.6},
        "num0__also1": {"type": "noul", "noul": 0.97}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("total_reviews_count:>500", params["filter_by"].get<std::string>());
    ASSERT_EQ(1, params["llm_response"]["clauses"].size());
}

TEST_F(JevSearchParamsTest, AssembleNumericRangeFromTwoLiterals) {
    // a range is two literals binding the same field with opposite bounds, there is no range operator
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    auto facts = facts_for("headphones between 100 and 300");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "__none__": 0.1}},
        "num0__op": {"type": "choice", "choice": "gte", "confidence": 0.88,
                     "probabilities": {"eq": 0.02, "gt": 0.06, "gte": 0.88, "lt": 0.02, "lte": 0.02}},
        "num1__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "__none__": 0.1}},
        "num1__op": {"type": "choice", "choice": "lte", "confidence": 0.87,
                     "probabilities": {"eq": 0.02, "gt": 0.02, "gte": 0.03, "lt": 0.06, "lte": 0.87}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:>=100 && price:<=300", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleBindsDuplicateLiteralsIndependently) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    catalog.numeric_fields.push_back({"weight", field_types::FLOAT});
    auto facts = facts_for("price under 50 and weight over 50");
    ASSERT_EQ(2, facts.numbers.size());

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "weight": 0.05, "__none__": 0.05}},
        "num0__op": {"type": "choice", "choice": "lte", "confidence": 0.88,
                     "probabilities": {"eq": 0.02, "gt": 0.02, "gte": 0.02, "lt": 0.06, "lte": 0.88}},
        "num1__field": {"type": "choice", "choice": "weight", "confidence": 0.9,
                        "probabilities": {"price": 0.05, "weight": 0.9, "__none__": 0.05}},
        "num1__op": {"type": "choice", "choice": "gt", "confidence": 0.9,
                     "probabilities": {"eq": 0.02, "gt": 0.9, "gte": 0.04, "lt": 0.02, "lte": 0.02}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:<=50 && weight:>50", params["filter_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsDuplicateClauses) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    auto facts = facts_for("50 dollars yes 50 dollars");
    ASSERT_EQ(2, facts.numbers.size());

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "__none__": 0.1}},
        "num0__op": {"type": "choice", "choice": "eq", "confidence": 0.9,
                     "probabilities": {"eq": 0.9, "lte": 0.1}},
        "num1__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "__none__": 0.1}},
        "num1__op": {"type": "choice", "choice": "eq", "confidence": 0.9,
                     "probabilities": {"eq": 0.9, "lte": 0.1}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:50", params["filter_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, BuildQuestionsCarryOccurrenceContexts) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});

    auto facts = facts_for("price under 50 and weight over 50");
    auto questions_op = JevSearchParams::build_questions(catalog, facts, jev_options_t());
    ASSERT_TRUE(questions_op.ok());

    auto questions = questions_op.get();
    ASSERT_TRUE(questions["num0__field"]["instructions"].get<std::string>()
                    .find("under 50") != std::string::npos);
    ASSERT_TRUE(questions["num1__field"]["instructions"].get<std::string>()
                    .find("over 50") != std::string::npos);
}

TEST_F(JevSearchParamsTest, AssembleDropsNumericLiteralThatDoesNotFitTheFieldType) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"year", field_types::INT32});
    auto facts = facts_for("cars over 19.99");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "year", "confidence": 0.9,
                        "probabilities": {"year": 0.9, "__none__": 0.1}},
        "num0__op": {"type": "choice", "choice": "gt", "confidence": 0.9,
                     "probabilities": {"eq": 0.02, "gt": 0.9, "gte": 0.04, "lt": 0.02, "lte": 0.02}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleDropsALiteralBoundToNoField) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    auto facts = facts_for("top 10 headphones");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "__none__", "confidence": 0.9,
                        "probabilities": {"price": 0.1, "__none__": 0.9}},
        "num0__op": {"type": "choice", "choice": "lte", "confidence": 0.5,
                     "probabilities": {"eq": 0.2, "gt": 0.1, "gte": 0.1, "lt": 0.1, "lte": 0.5}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleBoolClause) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.bool_fields.push_back("in_stock");
    auto facts = facts_for("headphones available now");

    nlohmann::json answers = R"json({
        "b0": {"type": "choice", "choice": "true", "confidence": 0.89,
               "probabilities": {"true": 0.89, "false": 0.02, "unspecified": 0.09}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("in_stock:true", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleSkipsUnspecifiedBoolField) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.bool_fields.push_back("in_stock");
    auto facts = facts_for("headphones");

    nlohmann::json answers = R"json({
        "b0": {"type": "choice", "choice": "unspecified", "confidence": 0.93,
               "probabilities": {"true": 0.04, "false": 0.03, "unspecified": 0.93}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleConjoinsClausesAcrossFieldTypes) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.facet_fields.push_back({"brand", {"Sony", "Bose"}});
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    catalog.bool_fields.push_back("in_stock");
    auto facts = facts_for("sony headphones under 200 in stock");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.95},
        "f0__negate": {"type": "noul", "noul": 0.01},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.96,
                      "probabilities": {"Sony": 0.96, "Bose": 0.04}},
        "num0__field": {"type": "choice", "choice": "price", "confidence": 0.92,
                        "probabilities": {"price": 0.92, "__none__": 0.08}},
        "num0__op": {"type": "choice", "choice": "lte", "confidence": 0.9,
                     "probabilities": {"eq": 0.02, "gt": 0.02, "gte": 0.02, "lt": 0.04, "lte": 0.9}},
        "b0": {"type": "choice", "choice": "true", "confidence": 0.87,
               "probabilities": {"true": 0.87, "false": 0.03, "unspecified": 0.1}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:=Sony && price:<=200 && in_stock:true", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));

    ASSERT_NEAR(0.87, params["llm_response"]["confidence"].get<double>(), 0.0001);
    ASSERT_EQ(3, params["llm_response"]["clauses"].size());
}

TEST_F(JevSearchParamsTest, AssembleSortBy) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.sort_fields = {"price", "year"};
    auto facts = facts_for("cheapest headphones");

    nlohmann::json answers = R"json({
        "sort__field": {"type": "choice", "choice": "price", "confidence": 0.92,
                        "probabilities": {"price": 0.92, "year": 0.05, "__none__": 0.03}},
        "sort0__dir": {"type": "choice", "choice": "asc", "confidence": 0.95,
                       "probabilities": {"asc": 0.95, "desc": 0.05}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:asc", params["sort_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleCombinesCoordinatedSorts) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.sort_fields = {"avg_rating", "price_min", "year"};
    auto facts = facts_for("best and cheapest restaurants");

    nlohmann::json answers = R"json({
        "sort__field": {"type": "choice", "choice": "avg_rating", "confidence": 0.35,
                        "probabilities": {"avg_rating": 0.52, "price_min": 0.44, "year": 0.01, "__none__": 0.03}},
        "sort0__dir": {"type": "choice", "choice": "desc", "confidence": 0.95,
                       "probabilities": {"asc": 0.05, "desc": 0.95}},
        "sort1__dir": {"type": "choice", "choice": "asc", "confidence": 0.9,
                       "probabilities": {"asc": 0.92, "desc": 0.08}},
        "sort2__dir": {"type": "choice", "choice": "desc", "confidence": 0.5,
                       "probabilities": {"asc": 0.5, "desc": 0.5}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("avg_rating:desc,price_min:asc", params["sort_by"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleSortSurvivesAnUncertainDirection) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.sort_fields = {"price", "year"};
    auto facts = facts_for("sort by price");

    nlohmann::json answers = R"json({
        "sort__field": {"type": "choice", "choice": "price", "confidence": 0.98,
                        "probabilities": {"price": 0.98, "year": 0.01, "__none__": 0.01}},
        "sort0__dir": {"type": "choice", "choice": "desc", "confidence": 0.06,
                       "probabilities": {"asc": 0.47, "desc": 0.53}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:desc", params["sort_by"].get<std::string>());
    ASSERT_NEAR(0.98, params["llm_response"]["confidence"].get<double>(), 0.0001);
}

TEST_F(JevSearchParamsTest, AssembleEmitsStarWhenTheSortOwnsTheQuery) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.sort_fields = {"price"};
    auto facts = facts_for("sort by price");

    nlohmann::json answers = R"json({
        "sort__field": {"type": "choice", "choice": "price", "confidence": 0.97,
                        "probabilities": {"price": 0.97, "__none__": 0.03}},
        "sort0__dir": {"type": "choice", "choice": "asc", "confidence": 0.9,
                       "probabilities": {"asc": 0.92, "desc": 0.08}}
    })json"_json;
    answers["q0"] = token_answer(0.1);
    answers["q1"] = token_answer(0.05);
    answers["q2"] = token_answer(0.7);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:asc", params["sort_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
    ASSERT_FALSE(params.contains("filter_by"));
}

TEST_F(JevSearchParamsTest, AssembleSkipsSortWhenNoFieldIsNamed) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.sort_fields = {"price", "year"};
    auto facts = facts_for("headphones");

    nlohmann::json answers = R"json({
        "sort__field": {"type": "choice", "choice": "__none__", "confidence": 0.94,
                        "probabilities": {"price": 0.03, "year": 0.03, "__none__": 0.94}},
        "sort0__dir": {"type": "choice", "choice": "asc", "confidence": 0.51,
                       "probabilities": {"asc": 0.51, "desc": 0.49}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("sort_by"));
}

TEST_F(JevSearchParamsTest, AssembleRewritesQFromSurvivingTokens) {
    auto catalog = brand_catalog();
    auto facts = facts_for("red shoes under 50");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;
    answers["q0"] = token_answer(0.88);
    answers["q1"] = token_answer(0.93);
    answers["q2"] = token_answer(0.06);
    answers["q3"] = token_answer(0.04);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("red shoes", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleEmitsStarWhenFiltersConsumeTheWholeQuery) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    auto facts = facts_for("under 50 dollars");

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "__none__": 0.1}},
        "num0__op": {"type": "choice", "choice": "lte", "confidence": 0.88,
                     "probabilities": {"eq": 0.02, "gt": 0.02, "gte": 0.02, "lt": 0.06, "lte": 0.88}}
    })json"_json;
    answers["q0"] = token_answer(0.04);
    answers["q1"] = token_answer(0.03);
    answers["q2"] = token_answer(0.28);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:<=50", params["filter_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleKeepsTheRawQueryWhenNothingSurvivesUnfiltered) {
    // every token judged a non content word but no clause was built either, star would match everything
    auto catalog = brand_catalog();
    auto facts = facts_for("under 50 dollars");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;
    answers["q0"] = token_answer(0.04);
    answers["q1"] = token_answer(0.03);
    answers["q2"] = token_answer(0.28);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_FALSE(params.contains("filter_by"));
    ASSERT_EQ("under 50 dollars", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsTokensAFilterClauseConsumed) {
    // the dinner regression, keeping a consumed token in q re-filters the text and collapses recall
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"meals", {"Breakfast", "Lunch", "Dinner"}});
    auto facts = facts_for("dinner");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.94},
        "f0__negate": {"type": "noul", "noul": 0.03},
        "f0__value": {"type": "choice", "choice": "Dinner", "confidence": 0.97,
                      "probabilities": {"Breakfast": 0.01, "Lunch": 0.02, "Dinner": 0.97}}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("meals:=Dinner", params["filter_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsTokensTheClauseFieldNameConsumed) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.bool_fields.push_back("gluten_free");
    auto facts = facts_for("gluten free spots");

    nlohmann::json answers = R"json({
        "b0": {"type": "choice", "choice": "true", "confidence": 0.9,
               "probabilities": {"true": 0.9, "false": 0.02, "unspecified": 0.08}}
    })json"_json;
    answers["q0"] = token_answer(0.8);
    answers["q1"] = token_answer(0.7);
    answers["q2"] = token_answer(0.2);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("gluten_free:true", params["filter_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsTokensThatShareAPrefixWithConsumedTerms) {
    // nothing in this path stems, `accept` has to hit the clause's `accepts` through the prefix rule
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"features", {"Accepts Credit Cards", "Delivery"}});
    auto facts = facts_for("places that accept credit cards");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.92},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Accepts Credit Cards", "confidence": 0.95,
                      "probabilities": {"Accepts Credit Cards": 0.95, "Delivery": 0.05}}
    })json"_json;
    answers["q0"] = token_answer(0.1);
    answers["q1"] = token_answer(0.05);
    answers["q2"] = token_answer(0.85);
    answers["q3"] = token_answer(0.8);
    answers["q4"] = token_answer(0.8);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("features:=Accepts Credit Cards", params["filter_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsTokensAnAbbreviatedValueConsumed) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"open_hours.day", {"Mon", "Tue", "Sun"}});
    auto facts = facts_for("open on mondays");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Mon", "confidence": 0.9,
                      "probabilities": {"Mon": 0.9, "Tue": 0.05, "Sun": 0.05}}
    })json"_json;
    answers["q0"] = token_answer(0.7);
    answers["q1"] = token_answer(0.1);
    answers["q2"] = token_answer(0.8);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("open_hours.day:=Mon", params["filter_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsTokensTheStemmerMatches) {
    // reviewed and reviews only meet at the stem, neither is a prefix of the other
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.sort_fields = {"total_reviews_count"};
    auto facts = facts_for("most reviewed");

    nlohmann::json answers = R"json({
        "sort__field": {"type": "choice", "choice": "total_reviews_count", "confidence": 0.97,
                        "probabilities": {"total_reviews_count": 0.97, "__none__": 0.03}},
        "sort0__dir": {"type": "choice", "choice": "desc", "confidence": 0.95,
                       "probabilities": {"asc": 0.05, "desc": 0.95}}
    })json"_json;
    answers["q0"] = token_answer(0.1);
    answers["q1"] = token_answer(0.8);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("total_reviews_count:desc", params["sort_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleJudgesASingleTokenQuery) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.sort_fields = {"price"};
    auto facts = facts_for("cheapest");

    nlohmann::json answers = R"json({
        "sort__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "__none__": 0.1}},
        "sort0__dir": {"type": "choice", "choice": "asc", "confidence": 0.9,
                       "probabilities": {"asc": 0.92, "desc": 0.08}}
    })json"_json;
    answers["q0"] = token_answer(0.1);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:asc", params["sort_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleKeepsAStandaloneDigitTheClauseDidNotConsume) {
    jev_catalog_t catalog;
    catalog.collection_name = "flats";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    auto facts = facts_for("2 rooms under 2500");
    ASSERT_EQ(2, facts.numbers.size());

    nlohmann::json answers = R"json({
        "num0__field": {"type": "choice", "choice": "__none__", "confidence": 0.9,
                        "probabilities": {"price": 0.1, "__none__": 0.9}},
        "num0__op": {"type": "choice", "choice": "eq", "confidence": 0.5,
                     "probabilities": {"eq": 0.5, "gte": 0.5}},
        "num1__field": {"type": "choice", "choice": "price", "confidence": 0.9,
                        "probabilities": {"price": 0.9, "__none__": 0.1}},
        "num1__op": {"type": "choice", "choice": "lte", "confidence": 0.9,
                     "probabilities": {"eq": 0.02, "gt": 0.02, "gte": 0.02, "lt": 0.04, "lte": 0.9}}
    })json"_json;
    answers["q0"] = token_answer(0.7);
    answers["q1"] = token_answer(0.8);
    answers["q2"] = token_answer(0.1);
    answers["q3"] = token_answer(0.6);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("price:<=2500", params["filter_by"].get<std::string>());
    ASSERT_EQ("2 rooms", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleKeepsTokensWithIndependentSignal) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"meals", {"Breakfast", "Lunch", "Dinner"}});
    auto facts = facts_for("dinner near the beach");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.03},
        "f0__value": {"type": "choice", "choice": "Dinner", "confidence": 0.95,
                      "probabilities": {"Breakfast": 0.01, "Lunch": 0.04, "Dinner": 0.95}}
    })json"_json;
    answers["q0"] = token_answer(0.85);
    answers["q1"] = token_answer(0.1);
    answers["q2"] = token_answer(0.05);
    answers["q3"] = token_answer(0.9);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("meals:=Dinner", params["filter_by"].get<std::string>());
    ASSERT_EQ("beach", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleKeepsEveryTokenThatClearsTheThreshold) {
    auto catalog = brand_catalog();
    auto facts = facts_for("nice looking headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;
    answers["q0"] = token_answer(0.52);
    answers["q1"] = token_answer(0.54);
    answers["q2"] = token_answer(0.55);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("nice looking headphones", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleKeepsTheRawQueryWhenTokenAnswersAreMissing) {
    auto catalog = brand_catalog();
    auto facts = facts_for("noise cancelling headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("noise cancelling headphones", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleSkipsTokenRewritingForPhraseQueries) {
    auto catalog = brand_catalog();
    auto facts = facts_for("\"noise cancelling\" headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;
    answers["q0"] = token_answer(0.02);
    answers["q1"] = token_answer(0.02);
    answers["q2"] = token_answer(0.99);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("\"noise cancelling\" headphones", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleEmitsExcludedTokensWithTheExclusionOperator) {
    // no facet caught `fast food`, the excluded judgment is the only home the negation has
    auto catalog = brand_catalog();
    auto facts = facts_for("family friendly but not fast food");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;
    answers["q0"] = token_answer(0.9);
    answers["q1"] = token_answer(0.85);
    answers["q2"] = token_answer(0.05);
    answers["q3"] = token_answer(0.03);
    answers["q4"] = token_answer(0.05, 0.9);
    answers["q5"] = token_answer(0.05, 0.88);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("family friendly -fast -food", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleAnchorsExclusionOnlyQOnTheStar) {
    auto catalog = brand_catalog();
    auto facts = facts_for("not sushi");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;
    answers["q0"] = token_answer(0.05);
    answers["q1"] = token_answer(0.1, 0.9);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("* -sushi", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleDropsQToStarWhenTokensExceedTheCapAndFiltersStand) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"meals", {"Breakfast", "Lunch", "Dinner"}});
    auto facts = facts_for("i am looking for somewhere really nice to eat tonight with my family");
    ASSERT_GT(facts.tokens.size(), 3);

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.03},
        "f0__value": {"type": "choice", "choice": "Dinner", "confidence": 0.9,
                      "probabilities": {"Breakfast": 0.02, "Lunch": 0.08, "Dinner": 0.9}}
    })json"_json;

    jev_options_t opts;
    opts.max_token_questions = 3;

    auto params = JevSearchParams::assemble(catalog, facts, answers, opts);
    ASSERT_EQ("meals:=Dinner", params["filter_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleKeepsTheRawQueryOverTheCapWhenNothingFilters) {
    auto catalog = brand_catalog();
    auto facts = facts_for("one two three four five");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.05}
    })json"_json;

    jev_options_t opts;
    opts.max_token_questions = 3;

    auto params = JevSearchParams::assemble(catalog, facts, answers, opts);
    ASSERT_EQ("one two three four five", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleConsumesAGreekValueAgainstAGreekToken) {
    // a non latin clause value has to consume its own query token or the leftover q strangles the filter
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"city", {"Αθήνα", "Θεσσαλονίκη"}});
    auto facts = facts_for("Ταβέρνες Αθήνα");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.93},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Αθήνα", "confidence": 0.95,
                      "probabilities": {"Αθήνα": 0.95, "Θεσσαλονίκη": 0.05}}
    })json"_json;
    answers["q0"] = token_answer(0.9);
    answers["q1"] = token_answer(0.85);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("city:=Αθήνα", params["filter_by"].get<std::string>());
    ASSERT_EQ("Ταβέρνες", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleConsumesAnAccentedValueAgainstItsAsciiForm) {
    // both sides run through the index's default tokenizer, accent folding lets `Café` consume `cafe`
    jev_catalog_t catalog;
    catalog.collection_name = "places";
    catalog.facet_fields.push_back({"categories", {"Café", "Bar"}});
    auto facts = facts_for("cafe spots");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.92},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__value": {"type": "choice", "choice": "Café", "confidence": 0.94,
                      "probabilities": {"Café": 0.94, "Bar": 0.06}}
    })json"_json;
    answers["q0"] = token_answer(0.9);
    answers["q1"] = token_answer(0.1);

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("categories:=Café", params["filter_by"].get<std::string>());
    ASSERT_EQ("*", params["q"].get<std::string>());
}

TEST_F(JevSearchParamsTest, AssembleGroupsAlternativesWithOr) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.bool_fields = {"gluten_free", "vegetarian_friendly"};
    auto facts = facts_for("gluten free or vegetarian friendly places");
    ASSERT_TRUE(facts.has_or);

    nlohmann::json answers = R"json({
        "b0": {"type": "choice", "choice": "true", "confidence": 0.9,
               "probabilities": {"true": 0.9, "false": 0.02, "unspecified": 0.08}},
        "b0__alt": {"type": "noul", "noul": 0.92},
        "b1": {"type": "choice", "choice": "true", "confidence": 0.88,
               "probabilities": {"true": 0.88, "false": 0.02, "unspecified": 0.1}},
        "b1__alt": {"type": "noul", "noul": 0.9}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("(gluten_free:true || vegetarian_friendly:true)", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleAndsTheAlternativeGroupOntoTheRest) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"brand", {"Sony", "Bose"}});
    catalog.bool_fields = {"gluten_free", "vegetarian_friendly"};
    auto facts = facts_for("sony gear that is gluten free or vegetarian friendly");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.9},
        "f0__negate": {"type": "noul", "noul": 0.02},
        "f0__alt": {"type": "noul", "noul": 0.1},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.95,
                      "probabilities": {"Sony": 0.95, "Bose": 0.05}},
        "b0": {"type": "choice", "choice": "true", "confidence": 0.9,
               "probabilities": {"true": 0.9, "false": 0.02, "unspecified": 0.08}},
        "b0__alt": {"type": "noul", "noul": 0.9},
        "b1": {"type": "choice", "choice": "true", "confidence": 0.88,
               "probabilities": {"true": 0.88, "false": 0.02, "unspecified": 0.1}},
        "b1__alt": {"type": "noul", "noul": 0.88}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:=Sony && (gluten_free:true || vegetarian_friendly:true)",
              params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, AssembleTreatsALoneAlternativeAsARequirement) {
    auto catalog = brand_catalog();
    auto facts = facts_for("sony or bose headphones");

    nlohmann::json answers = R"json({
        "f0__present": {"type": "noul", "noul": 0.93},
        "f0__negate": {"type": "noul", "noul": 0.01},
        "f0__alt": {"type": "noul", "noul": 0.95},
        "f0__value": {"type": "choice", "choice": "Sony", "confidence": 0.93,
                      "probabilities": {"Sony": 0.93, "Bose": 0.05, "Sennheiser": 0.02}},
        "f0__value2": {"type": "choice", "choice": "Bose", "confidence": 0.87,
                       "probabilities": {"Sony": 0.02, "Bose": 0.87, "Sennheiser": 0.03, "__none__": 0.08}},
        "f0__both": {"type": "noul", "noul": 0.06}
    })json"_json;

    auto params = JevSearchParams::assemble(catalog, facts, answers);
    ASSERT_EQ("brand:=[Sony,Bose]", params["filter_by"].get<std::string>());
    ASSERT_TRUE(filter_parses(params["filter_by"].get<std::string>()));
}

TEST_F(JevSearchParamsTest, BuildQuestionsAsksAlternativesOnlyWhenTheQueryHasOr) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.facet_fields.push_back({"brand", {"Sony", "Bose"}});
    catalog.bool_fields = {"in_stock"};

    auto plain = JevSearchParams::build_questions(catalog, facts_for("sony headphones"), jev_options_t());
    ASSERT_TRUE(plain.ok());
    ASSERT_FALSE(plain.get().contains("f0__alt"));
    ASSERT_FALSE(plain.get().contains("b0__alt"));

    auto alt = JevSearchParams::build_questions(catalog, facts_for("sony or bose headphones"), jev_options_t());
    ASSERT_TRUE(alt.ok());
    ASSERT_TRUE(alt.get().contains("f0__alt"));
    ASSERT_TRUE(alt.get().contains("b0__alt"));
}

TEST_F(JevSearchParamsTest, AssembleCapsTheNumberOfClauses) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.bool_fields = {"a", "b", "c", "d"};
    auto facts = facts_for("everything");

    nlohmann::json answers = R"json({
        "b0": {"type": "choice", "choice": "true", "confidence": 0.6, "probabilities": {"true": 0.6}},
        "b1": {"type": "choice", "choice": "true", "confidence": 0.9, "probabilities": {"true": 0.9}},
        "b2": {"type": "choice", "choice": "true", "confidence": 0.7, "probabilities": {"true": 0.7}},
        "b3": {"type": "choice", "choice": "true", "confidence": 0.8, "probabilities": {"true": 0.8}}
    })json"_json;

    jev_options_t opts;
    opts.max_clauses = 2;

    auto params = JevSearchParams::assemble(catalog, facts, answers, opts);
    ASSERT_EQ("b:true && d:true", params["filter_by"].get<std::string>());
}

static jev_catalog_t cuisine_catalog(const std::vector<std::string>& values) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.facet_fields.push_back({"cuisines", values});
    return catalog;
}

static std::vector<std::string> numbered_values(size_t count) {
    std::vector<std::string> values;
    for(size_t i = 0; i < count; i++) {
        values.push_back("Cuisine" + std::to_string(i));
    }
    return values;
}

TEST_F(JevSearchParamsTest, ShortlistKeepsAVocabularyThatAlreadyFits) {
    auto catalog = cuisine_catalog({"Greek", "Turkish", "Italian"});
    auto trace = JevSearchParams::shortlist_values(catalog, facts_for("greek food"), jev_options_t());

    // three values against a cap of forty, narrowing could only lose recall for nothing
    ASSERT_EQ(3, catalog.facet_fields[0].values.size());
    ASSERT_EQ(3, catalog.facet_fields[0].vocabulary_size);
    ASSERT_EQ(1, catalog.facet_fields[0].num_matched);
    ASSERT_EQ("Greek", catalog.facet_fields[0].values[0]);

    ASSERT_EQ(1, trace.size());
    ASSERT_EQ("shortlist", trace[0]["stage"].get<std::string>());
    ASSERT_EQ("cuisines", trace[0]["field"].get<std::string>());
    ASSERT_EQ(3, trace[0]["vocabulary"].get<size_t>());
    ASSERT_EQ(1, trace[0]["matched"].get<size_t>());
    ASSERT_EQ(3, trace[0]["offered"].get<size_t>());
    ASSERT_EQ("Greek", trace[0]["matched_values"][0].get<std::string>());
}

TEST_F(JevSearchParamsTest, ShortlistNarrowsAWideVocabularyToMatchesPlusTheFloor) {
    auto values = numbered_values(200);
    values.push_back("Greek");
    auto catalog = cuisine_catalog(values);

    jev_options_t opts;
    opts.max_shortlist_values = 10;
    opts.shortlist_floor_values = 4;

    JevSearchParams::shortlist_values(catalog, facts_for("greek food"), opts);

    ASSERT_EQ(201, catalog.facet_fields[0].vocabulary_size);
    ASSERT_EQ(1, catalog.facet_fields[0].num_matched);
    ASSERT_EQ(5, catalog.facet_fields[0].values.size());
    ASSERT_EQ("Greek", catalog.facet_fields[0].values[0]);
    ASSERT_EQ("Cuisine0", catalog.facet_fields[0].values[1]);
    ASSERT_EQ("Cuisine3", catalog.facet_fields[0].values[4]);
}

TEST_F(JevSearchParamsTest, ShortlistFallsBackToTheFrequencyFloorWhenNothingMatches) {
    auto catalog = cuisine_catalog(numbered_values(100));

    jev_options_t opts;
    opts.max_shortlist_values = 10;
    opts.shortlist_floor_values = 6;

    JevSearchParams::shortlist_values(catalog, facts_for("somewhere nice tonight"), opts);

    ASSERT_EQ(0, catalog.facet_fields[0].num_matched);
    ASSERT_EQ(6, catalog.facet_fields[0].values.size());
    ASSERT_EQ("Cuisine0", catalog.facet_fields[0].values[0]);
}

TEST_F(JevSearchParamsTest, ShortlistCapBindsOnAFloodOfMatches) {
    std::vector<std::string> values;
    for(size_t i = 0; i < 50; i++) {
        values.push_back("Greek" + std::to_string(i));
    }
    auto catalog = cuisine_catalog(values);

    jev_options_t opts;
    opts.max_shortlist_values = 6;
    opts.shortlist_floor_values = 4;

    JevSearchParams::shortlist_values(catalog, facts_for("greek food"), opts);

    ASSERT_EQ(6, catalog.facet_fields[0].num_matched);
    ASSERT_EQ(6, catalog.facet_fields[0].values.size());
}

TEST_F(JevSearchParamsTest, ShortlistOffOffersTheWholeVocabulary) {
    auto catalog = cuisine_catalog(numbered_values(100));

    jev_options_t opts;
    opts.max_shortlist_values = 0;

    auto trace = JevSearchParams::shortlist_values(catalog, facts_for("greek food"), opts);

    ASSERT_EQ(100, catalog.facet_fields[0].values.size());
    ASSERT_EQ(100, catalog.facet_fields[0].vocabulary_size);
    ASSERT_EQ(0, catalog.facet_fields[0].num_matched);
    ASSERT_TRUE(trace.empty());
}

TEST_F(JevSearchParamsTest, ShortlistMatchesThroughTheStemmer) {
    auto catalog = cuisine_catalog({"Bakeries", "Pizza"});
    JevSearchParams::shortlist_values(catalog, facts_for("a good bakery"), jev_options_t());

    ASSERT_EQ(1, catalog.facet_fields[0].num_matched);
    ASSERT_EQ("Bakeries", catalog.facet_fields[0].values[0]);
}

TEST_F(JevSearchParamsTest, ShortlistMatchesThroughATypoWithinTheEnginesBudget) {
    // a transposition of the last two letters, exactly what art_fuzzy_search forgives
    auto catalog = cuisine_catalog({"Japanese", "Pizza"});
    JevSearchParams::shortlist_values(catalog, facts_for("japanees food"), jev_options_t());

    ASSERT_EQ(1, catalog.facet_fields[0].num_matched);
    ASSERT_EQ("Japanese", catalog.facet_fields[0].values[0]);
}

TEST_F(JevSearchParamsTest, ShortlistGivesShortWordsNoTypoBudget) {
    // min_len_1typo is 4, three letter words must not collapse into one another
    auto catalog = cuisine_catalog({"Tha", "Pizza"});
    JevSearchParams::shortlist_values(catalog, facts_for("thi food"), jev_options_t());

    ASSERT_EQ(0, catalog.facet_fields[0].num_matched);
}

TEST_F(JevSearchParamsTest, ShortlistRanksAFullyCoveredValueAboveAClippedOne) {
    auto catalog = cuisine_catalog({"York Street Diner", "New York"});

    jev_options_t opts;
    opts.max_shortlist_values = 2;
    opts.shortlist_floor_values = 0;

    JevSearchParams::shortlist_values(catalog, facts_for("new york"), opts);

    ASSERT_EQ(2, catalog.facet_fields[0].num_matched);
    ASSERT_EQ("New York", catalog.facet_fields[0].values[0]);
}

TEST_F(JevSearchParamsTest, ShortlistPrefersTheValueMatchedByTheRarerQueryWord) {
    auto catalog = cuisine_catalog({"Barcelona", "Barbecue", "Bartender", "Barrel", "Ramen"});

    jev_options_t opts;
    opts.max_shortlist_values = 1;
    opts.shortlist_floor_values = 0;

    JevSearchParams::shortlist_values(catalog, facts_for("bar ram"), opts);

    ASSERT_EQ(1, catalog.facet_fields[0].num_matched);
    ASSERT_EQ("Ramen", catalog.facet_fields[0].values[0]);
}

TEST_F(JevSearchParamsTest, ShortlistOnlyEverDropsValues) {
    const std::vector<std::string> vocabulary = {"Greek", "Turkish", "Italian", "Thai", "Ramen", "Sushi"};
    auto catalog = cuisine_catalog(vocabulary);

    jev_options_t opts;
    opts.max_shortlist_values = 3;
    opts.shortlist_floor_values = 1;

    JevSearchParams::shortlist_values(catalog, facts_for("greek or thai"), opts);

    ASSERT_EQ(2, catalog.facet_fields[0].num_matched);
    ASSERT_EQ(3, catalog.facet_fields[0].values.size());
    for(const auto& value : catalog.facet_fields[0].values) {
        ASSERT_TRUE(std::find(vocabulary.begin(), vocabulary.end(), value) != vocabulary.end());
    }
}

TEST_F(JevSearchParamsTest, ShortlistNarrowsEveryFacetFieldIndependently) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    auto cuisines = numbered_values(50);
    cuisines.push_back("Greek");
    catalog.facet_fields.push_back({"cuisines", cuisines});
    catalog.facet_fields.push_back({"neighbourhood", {"Kolonaki", "Exarchia"}});

    jev_options_t opts;
    opts.max_shortlist_values = 4;
    opts.shortlist_floor_values = 2;

    auto trace = JevSearchParams::shortlist_values(catalog, facts_for("greek food"), opts);

    ASSERT_EQ(2, trace.size());
    ASSERT_EQ(51, catalog.facet_fields[0].vocabulary_size);
    ASSERT_EQ(1, catalog.facet_fields[0].num_matched);
    ASSERT_EQ(3, catalog.facet_fields[0].values.size());

    ASSERT_EQ(2, catalog.facet_fields[1].vocabulary_size);
    ASSERT_EQ(0, catalog.facet_fields[1].num_matched);
    ASSERT_EQ(2, catalog.facet_fields[1].values.size());
}

TEST_F(JevSearchParamsTest, ShortlistShrinksTheStateAndTheOfferedOptions) {
    auto values = numbered_values(120);
    values.push_back("Greek");
    auto catalog = cuisine_catalog(values);

    jev_options_t opts;
    opts.max_shortlist_values = 8;
    opts.shortlist_floor_values = 4;

    auto facts = facts_for("greek food");
    JevSearchParams::shortlist_values(catalog, facts, opts);

    auto state = JevSearchParams::build_state(facts, catalog);
    ASSERT_EQ(5, state["values"]["cuisines"].size());

    auto questions_op = JevSearchParams::build_questions(catalog, facts, opts);
    ASSERT_TRUE(questions_op.ok());

    // five values plus the none escape hatch, down from a hundred and twenty two
    auto criteria = questions_op.get()["f0__value"]["criteria"];
    ASSERT_EQ(6, criteria.size());
    ASSERT_TRUE(criteria.contains("Greek"));
    ASSERT_TRUE(criteria.contains("__none__"));
    ASSERT_FALSE(criteria.contains("Cuisine90"));
}

TEST_F(JevSearchParamsTest, ShortlistHandlesACatalogWithNoFacetFields) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});

    auto trace = JevSearchParams::shortlist_values(catalog, facts_for("cheap spots"), jev_options_t());
    ASSERT_TRUE(trace.empty());
}

TEST_F(JevSearchParamsTest, ShortlistKnobsComeOffTheModelConfig) {
    jev_options_t opts;

    nlohmann::json config = R"json({"max_shortlist_values": 12, "shortlist_floor_values": 3})json"_json;
    ASSERT_TRUE(JevSearchParams::options_from_config(config, opts).ok());
    ASSERT_EQ(12, opts.max_shortlist_values);
    ASSERT_EQ(3, opts.shortlist_floor_values);

    // zero is the off switch, not an out of range value
    nlohmann::json off = R"json({"max_shortlist_values": 0})json"_json;
    ASSERT_TRUE(JevSearchParams::options_from_config(off, opts).ok());
    ASSERT_EQ(0, opts.max_shortlist_values);

    nlohmann::json over_cap = R"json({"max_shortlist_values": 900})json"_json;
    ASSERT_FALSE(JevSearchParams::options_from_config(over_cap, opts).ok());
}

TEST_F(JevSearchParamsTest, BuildQuestionsBatchesEveryJudgment) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.facet_fields.push_back({"brand", {"Sony", "Bose"}});
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});
    catalog.bool_fields.push_back("in_stock");
    catalog.sort_fields = {"price"};

    auto facts = facts_for("sony headphones under 200");
    auto questions_op = JevSearchParams::build_questions(catalog, facts, jev_options_t());
    ASSERT_TRUE(questions_op.ok());

    auto questions = questions_op.get();
    // 5 facet + (2 + 1 also) per literal + 1 bool + (1 + 1 dir per sort field) + 4 tokens
    ASSERT_EQ(15, questions.size());
    ASSERT_EQ("noul", questions["f0__present"]["type"].get<std::string>());
    ASSERT_EQ("choice", questions["f0__value"]["type"].get<std::string>());
    ASSERT_TRUE(questions["f0__value"]["criteria"].contains("__none__"));
    ASSERT_EQ("choice", questions["f0__value2"]["type"].get<std::string>());
    ASSERT_TRUE(questions["f0__value2"]["criteria"].contains("__none__"));
    ASSERT_EQ("noul", questions["f0__both"]["type"].get<std::string>());
    ASSERT_TRUE(questions["sort__field"]["criteria"].contains("__none__"));
    ASSERT_FALSE(questions.contains("q0__num"));

    ASSERT_TRUE(questions["num0__field"]["criteria"].contains("price"));
    ASSERT_TRUE(questions["num0__field"]["criteria"].contains("__none__"));
    ASSERT_TRUE(questions["num0__op"]["criteria"].contains("lte"));
    ASSERT_FALSE(questions["num0__op"]["criteria"].contains("none"));
    ASSERT_FALSE(questions["num0__op"]["criteria"].contains("range"));
}

TEST_F(JevSearchParamsTest, BuildQuestionsSkipsNumericsWhenTheQueryHasNoNumbers) {
    jev_catalog_t catalog;
    catalog.collection_name = "products";
    catalog.numeric_fields.push_back({"price", field_types::FLOAT});

    auto facts = facts_for("nice headphones");
    auto questions_op = JevSearchParams::build_questions(catalog, facts, jev_options_t());
    ASSERT_TRUE(questions_op.ok());
    ASSERT_FALSE(questions_op.get().contains("num0__field"));
}

TEST_F(JevSearchParamsTest, BuildQuestionsRefusesAnOversizedSchema) {
    jev_catalog_t catalog;
    catalog.collection_name = "wide";
    for(size_t i = 0; i < 40; i++) {
        catalog.facet_fields.push_back({"f" + std::to_string(i), {"a", "b"}});
    }

    auto facts = facts_for("something");
    jev_options_t opts;
    opts.max_questions = 10;

    auto questions_op = JevSearchParams::build_questions(catalog, facts, opts);
    ASSERT_FALSE(questions_op.ok());
    ASSERT_EQ(400, questions_op.code());
    ASSERT_TRUE(questions_op.error().find("nl_facet_fields") != std::string::npos);
}

TEST_F(JevSearchParamsTest, BuildQuestionsCarryFieldDescriptions) {
    // the schema's description rides wherever the field is named
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.numeric_fields.push_back({"value", field_types::FLOAT});
    catalog.sort_fields = {"value"};
    catalog.field_descriptions["value"] = "value for money rating, 0 to 5, higher is better";

    auto facts = facts_for("spots rated 5 for value");
    auto questions_op = JevSearchParams::build_questions(catalog, facts, jev_options_t());
    ASSERT_TRUE(questions_op.ok());

    auto questions = questions_op.get();
    ASSERT_TRUE(questions["num0__field"]["criteria"]["value"].get<std::string>()
                    .find("higher is better") != std::string::npos);
    ASSERT_TRUE(questions["sort0__dir"]["instructions"].get<std::string>()
                    .find("higher is better") != std::string::npos);
    ASSERT_TRUE(questions["sort__field"]["criteria"]["value"].get<std::string>()
                    .find("higher is better") != std::string::npos);
}

TEST_F(JevSearchParamsTest, BuildQuestionsAsksNumeralQuestionsOnlyWhenDigitless) {
    jev_catalog_t catalog;
    catalog.collection_name = "restaurants";
    catalog.numeric_fields.push_back({"service", field_types::FLOAT});

    jev_options_t on;
    on.word_numbers = true;

    auto digitless = JevSearchParams::build_questions(catalog, facts_for("five star service"), on);
    ASSERT_TRUE(digitless.ok());
    ASSERT_TRUE(digitless.get().contains("q0__num"));
    ASSERT_TRUE(digitless.get()["q0__num"]["criteria"].contains("5"));
    ASSERT_TRUE(digitless.get()["q0__num"]["criteria"].contains("__none__"));
    // no digits found means no binding questions yet, they run in the follow up round on a hit
    ASSERT_FALSE(digitless.get().contains("num0__field"));

    auto with_digits = JevSearchParams::build_questions(catalog, facts_for("5 star service"), on);
    ASSERT_TRUE(with_digits.ok());
    ASSERT_FALSE(with_digits.get().contains("q0__num"));
    ASSERT_TRUE(with_digits.get().contains("num0__field"));

    // the shipping default asks no numeral questions at all
    auto disabled = JevSearchParams::build_questions(catalog, facts_for("five star service"), jev_options_t());
    ASSERT_TRUE(disabled.ok());
    ASSERT_FALSE(disabled.get().contains("q0__num"));
}

TEST_F(JevSearchParamsTest, OptionsFromConfigReadsAndValidatesKnobs) {
    jev_options_t opts;
    nlohmann::json config = R"json({
        "model_name": "jev/jev-latest", "api_key": "ts-test",
        "present_threshold": 0.7, "max_token_questions": 40, "word_numbers": true
    })json"_json;
    ASSERT_TRUE(JevSearchParams::options_from_config(config, opts).ok());
    ASSERT_EQ(0.7, opts.present_threshold);
    ASSERT_EQ(40, opts.max_token_questions);
    ASSERT_TRUE(opts.word_numbers);
    ASSERT_EQ(0.5, opts.confidence_threshold);
    ASSERT_FALSE(opts.consumed_check);

    nlohmann::json out_of_range = R"json({"present_threshold": 1.5})json"_json;
    auto op = JevSearchParams::options_from_config(out_of_range, opts);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ(400, op.code());

    nlohmann::json wrong_type = R"json({"consumed_check": "yes"})json"_json;
    ASSERT_FALSE(JevSearchParams::options_from_config(wrong_type, opts).ok());

    nlohmann::json over_cap = R"json({"max_sort_fields": 7})json"_json;
    ASSERT_FALSE(JevSearchParams::options_from_config(over_cap, opts).ok());
}

TEST_F(JevSearchParamsTest, BuildStateCarriesTheSystemPromptAsContext) {
    auto facts = facts_for("cheap spots");
    jev_catalog_t catalog;
    auto state = JevSearchParams::build_state(facts, catalog, "a restaurant directory, all ratings run 0 to 5");
    ASSERT_EQ("a restaurant directory, all ratings run 0 to 5", state["context"].get<std::string>());

    ASSERT_FALSE(JevSearchParams::build_state(facts, catalog).contains("context"));
}

TEST_F(JevSearchParamsTest, BuildStateStaysSmall) {
    auto facts = facts_for("headphones under 200");
    auto state = JevSearchParams::build_state(facts, jev_catalog_t());

    ASSERT_EQ(3, state.size());
    ASSERT_EQ("headphones under 200", state["query"].get<std::string>());
    ASSERT_EQ(1, state["numbers_found"].size());
    ASSERT_EQ("200", state["numbers_found"][0].get<std::string>());
}

TEST_F(JevSearchParamsTest, BuildStateCarriesTheSchemaVocabulary) {
    // described fields ride in state once, sorted
    auto facts = facts_for("top rated spots");
    jev_catalog_t catalog;
    catalog.field_descriptions["food"] = "food quality rating";
    catalog.field_descriptions["avg_rating"] = "overall guest rating";

    auto state = JevSearchParams::build_state(facts, catalog);
    ASSERT_EQ(2, state["attributes"].size());
    ASSERT_EQ("avg_rating: overall guest rating", state["attributes"][0].get<std::string>());
    ASSERT_EQ("food: food quality rating", state["attributes"][1].get<std::string>());
}

TEST_F(JevSearchParamsTest, ClientSendsOneBatchedRequest) {
    JevClient::add_mock_response(R"json({
        "model": "jev-1.13.0",
        "answers": {"x": {"type": "noul", "noul": 0.9}},
        "usage": {"input_tokens": 120, "output_tokens": 8}
    })json", 200, {});

    nlohmann::json model_config = R"json({"model_name": "jev/jev-latest", "api_key": "ts-test"})json"_json;
    nlohmann::json questions;
    questions["x"] = JevClient::noul_question("is this a test?");

    auto response_op = JevClient::ask(nlohmann::json{{"query", "hello"}}, questions, model_config);
    ASSERT_TRUE(response_op.ok());
    ASSERT_EQ("jev-1.13.0", response_op.get()["model"].get<std::string>());

    ASSERT_EQ(1, JevClient::get_num_captured_requests());
    ASSERT_EQ("https://api.typesafe.ai/v1/systemone", JevClient::get_last_request_url());

    auto body = nlohmann::json::parse(JevClient::get_last_request_body());
    ASSERT_EQ("jev-latest", body["model"].get<std::string>());
    ASSERT_EQ("hello", body["state"]["query"].get<std::string>());
    ASSERT_EQ(1, body["questions"].size());
}

TEST_F(JevSearchParamsTest, ClientSurfacesApiErrors) {
    JevClient::add_mock_response(R"json({"error": {"message": "invalid api key"}})json", 401, {});

    nlohmann::json model_config = R"json({"model_name": "jev/jev-latest", "api_key": "bad"})json"_json;
    nlohmann::json questions;
    questions["x"] = JevClient::noul_question("is this a test?");

    auto response_op = JevClient::ask(nlohmann::json{{"query", "hello"}}, questions, model_config);
    ASSERT_FALSE(response_op.ok());
    ASSERT_TRUE(response_op.error().find("invalid api key") != std::string::npos);
}

TEST_F(JevSearchParamsTest, ClientRejectsAnEmptyQuestionSet) {
    nlohmann::json model_config = R"json({"model_name": "jev/jev-latest", "api_key": "ts-test"})json"_json;
    auto response_op = JevClient::ask(nlohmann::json{{"query", "hello"}}, nlohmann::json::object(), model_config);
    ASSERT_FALSE(response_op.ok());
    ASSERT_EQ(400, response_op.code());
}

TEST_F(JevSearchParamsTest, ClientAccessorsDegradeInsteadOfThrowing) {
    nlohmann::json answers = R"json({
        "a": {"type": "noul", "noul": 0.42},
        "b": {"type": "choice", "choice": "x", "confidence": 0.8}
    })json"_json;

    ASSERT_TRUE(JevClient::get_noul(answers, "a").ok());
    ASSERT_NEAR(0.42, JevClient::get_noul(answers, "a").get(), 0.0001);
    ASSERT_FALSE(JevClient::get_noul(answers, "missing").ok());
    ASSERT_FALSE(JevClient::get_noul(answers, "b").ok());

    ASSERT_EQ("x", JevClient::get_choice(answers, "b").get());
    ASSERT_FALSE(JevClient::get_choice(answers, "a").ok());
    ASSERT_NEAR(0.8, JevClient::get_confidence(answers, "b").get(), 0.0001);
    ASSERT_FALSE(JevClient::get_confidence(answers, "a").ok());
}

TEST_F(JevSearchParamsTest, ClientCapsChoiceOptions) {
    std::vector<std::pair<std::string, std::string>> options;
    for(size_t i = 0; i < 300; i++) {
        options.emplace_back("opt" + std::to_string(i), "option " + std::to_string(i));
    }

    auto question = JevClient::choice_question("pick one", options);
    ASSERT_EQ(JevClient::MAX_CHOICE_OPTIONS, question["criteria"].size());
}

