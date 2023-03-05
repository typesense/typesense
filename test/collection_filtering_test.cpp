#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionFilteringTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_filtering";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionFilteringTest, FilterOnTextFields) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("years", field_types::INT32_ARRAY, false),
            field("tags", field_types::STRING_ARRAY, true)
    };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "tags: gold", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "tags : fine PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // using just ":", filtering should return documents that contain ALL tokens in the filter expression
    results = coll_array_fields->search("Jeremy", query_fields, "tags : PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // no documents contain both "white" and "platinum", so
    results = coll_array_fields->search("Jeremy", query_fields, "tags : WHITE PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // with exact match operator (:=) partial matches are not allowed
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags : bronze", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"4", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // search with a list of tags, also testing extra padding of space
    results = coll_array_fields->search("Jeremy", query_fields, "tags: [bronze,   silver]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // need to be exact matches
    results = coll_array_fields->search("Jeremy", query_fields, "tags: bronze", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    // when comparators are used, they should be ignored
    results = coll_array_fields->search("Jeremy", query_fields, "tags:<bronze", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:<=BRONZE", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:>BRONZE", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    // bad filter value (empty)
    auto res_op = coll_array_fields->search("Jeremy", query_fields, "tags:=", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `tags`: Filter value cannot be empty.", res_op.error());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFilteringTest, FacetFieldStringFiltering) {
    Collection *coll_str;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("starring", field_types::STRING, true),
            field("cast", field_types::STRING_ARRAY, false),
            field("points", field_types::INT32, false)
    };

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll_str = collectionManager.get_collection("coll_str").get();
    if(coll_str == nullptr) {
        coll_str = collectionManager.create_collection("coll_str", 1, fields, "points").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        nlohmann::json document = nlohmann::json::parse(json_line);
        coll_str->add(document.dump());
    }

    infile.close();

    query_fields = {"title"};
    std::vector<std::string> facets;

    // exact filter on string field must fail when single token is used
    facets.clear();
    facets.emplace_back("starring");
    auto results = coll_str->search("*", query_fields, "starring:= samuel", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<size_t>());

    // multiple tokens but with a typo on one of them
    results = coll_str->search("*", query_fields, "starring:= ssamuel l. Jackson", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<size_t>());

    // same should succeed when verbatim filter is made
    results = coll_str->search("*", query_fields, "starring:= samuel l. Jackson", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    // with backticks
    results = coll_str->search("*", query_fields, "starring:= `samuel l. Jackson`", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    // contains filter with a single token should work as well
    results = coll_str->search("*", query_fields, "starring: jackson", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    results = coll_str->search("*", query_fields, "starring: samuel", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    // contains when only 1 token so should not match
    results = coll_str->search("*", query_fields, "starring: samuel johnson", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll_str");
}

TEST_F(CollectionFilteringTest, FacetFieldStringArrayFiltering) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("name_facet", field_types::STRING, true),
                                 field("age", field_types::INT32, true),
                                 field("years", field_types::INT32_ARRAY, true),
                                 field("rating", field_types::FLOAT, true),
                                 field("timestamps", field_types::INT64_ARRAY, true),
                                 field("tags", field_types::STRING_ARRAY, true)};

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", 1, fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        nlohmann::json document = nlohmann::json::parse(json_line);
        document["name_facet"] = document["name"];
        const std::string & patched_json_line = document.dump();
        coll_array_fields->add(patched_json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets = {"tags"};

    // facet with filter on string array field must fail when exact token is used
    facets.clear();
    facets.push_back("tags");
    auto results = coll_array_fields->search("Jeremy", query_fields, "tags:= PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:= FINE", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:= FFINE PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // partial token filter should be made without "=" operator
    results = coll_array_fields->search("Jeremy", query_fields, "tags: PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll_array_fields->search("Jeremy", query_fields, "tags: FINE", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<size_t>());

    // to make tokens match facet value exactly, use "=" operator
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= FINE PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<size_t>());

    // allow exact filter on non-faceted field
    results = coll_array_fields->search("Jeremy", query_fields, "name:= Jeremy Howard", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(5, results["found"].get<size_t>());

    // multi match exact query (OR condition)
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= [Gold, bronze]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(3, results["found"].get<size_t>());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:= [Gold, bronze, fine PLATINUM]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(4, results["found"].get<size_t>());

    // single array multi match
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= [fine PLATINUM]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<size_t>());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFilteringTest, FilterOnTextFieldWithColon) {
    Collection *coll1;

    std::vector<field> fields = {field("url", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "1";
    doc1["url"] = "https://example.com/1";
    doc1["points"] = 1;

    coll1->add(doc1.dump());

    query_fields = {"url"};
    std::vector<std::string> facets;

    auto res = coll1->search("*", query_fields, "url:= https://example.com/1", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("1", res["hits"][0]["document"]["id"].get<std::string>().c_str());

    res = coll1->search("*", query_fields, "url: https://example.com/1", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("1", res["hits"][0]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, HandleBadlyFormedFilterQuery) {
    // should not crash when filter query is malformed!
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING, false), field("age", field_types::INT32, false),
                                 field("years", field_types::INT32_ARRAY, false),
                                 field("timestamps", field_types::INT64_ARRAY, false),
                                 field("tags", field_types::STRING_ARRAY, true)};

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;

    // when filter field does not exist in the schema
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "tagzz: gold", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // compound filter expression containing an unknown field
    results = coll_array_fields->search("Jeremy", query_fields,
               "(age:>0 ||  timestamps:> 0) || tagzz: gold", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // unbalanced paranthesis
    results = coll_array_fields->search("Jeremy", query_fields,
                                        "(age:>0 ||  timestamps:> 0) || ", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // searching using a string for a numeric field
    results = coll_array_fields->search("Jeremy", query_fields, "age: abcdef", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // searching using a string for a numeric array field
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps: abcdef", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // malformed k:v syntax
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps abcdef", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // just spaces - must be treated as empty filter
    results = coll_array_fields->search("Jeremy", query_fields, "  ", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    // wrapping number with quotes
    results = coll_array_fields->search("Jeremy", query_fields, "age: '21'", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // empty value for a numerical filter field
    auto res_op = coll_array_fields->search("Jeremy", query_fields, "age:", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `age`: Numerical field has an invalid comparator.", res_op.error());

    // empty value for string filter field
    res_op = coll_array_fields->search("Jeremy", query_fields, "tags:", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `tags`: Filter value cannot be empty.", res_op.error());

    res_op = coll_array_fields->search("Jeremy", query_fields, "tags:= ", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `tags`: Filter value cannot be empty.", res_op.error());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFilteringTest, FilterAndQueryFieldRestrictions) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {
        field("title", field_types::STRING, false),
        field("starring", field_types::STRING, false),
        field("cast", field_types::STRING_ARRAY, true),
        field("points", field_types::INT32, false)
    };

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_mul_fields->add(json_line);
    }

    infile.close();

    std::vector<std::string> facets;

    // query shall be allowed on faceted text fields as well
    query_fields = {"cast"};
    Option<nlohmann::json> result_op =
            coll_mul_fields->search("anton", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(result_op.ok());

    nlohmann::json results = result_op.get();
    ASSERT_EQ(1, results["hits"].size());
    std::string solo_id = results["hits"].at(0)["document"]["id"];
    ASSERT_STREQ("14", solo_id.c_str());

    // filtering on string field should be possible
    query_fields = {"title"};
    result_op = coll_mul_fields->search("captain", query_fields, "starring: Samuel L. Jackson", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(true, result_op.ok());
    results = result_op.get();
    ASSERT_EQ(1, results["hits"].size());
    solo_id = results["hits"].at(0)["document"]["id"];
    ASSERT_STREQ("6", solo_id.c_str());

    // filtering on facet field should be possible (supports partial word search but without typo tolerance)
    query_fields = {"title"};
    result_op = coll_mul_fields->search("*", query_fields, "cast: chris", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(true, result_op.ok());
    results = result_op.get();
    ASSERT_EQ(3, results["hits"].size());

    // bad query string
    result_op = coll_mul_fields->search("captain", query_fields, "BLAH", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Could not parse the filter query.", result_op.error().c_str());

    // missing field
    result_op = coll_mul_fields->search("captain", query_fields, "age: 100", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Could not find a filter field named `age` in the schema.", result_op.error().c_str());

    // bad filter value type
    result_op = coll_mul_fields->search("captain", query_fields, "points: \"100\"", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Numerical field has an invalid comparator.", result_op.error().c_str());

    // bad filter value type - equaling float on an integer field
    result_op = coll_mul_fields->search("captain", query_fields, "points: 100.34", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Not an int32.", result_op.error().c_str());

    // bad filter value type - less than float on an integer field
    result_op = coll_mul_fields->search("captain", query_fields, "points: <100.0", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Not an int32.", result_op.error().c_str());

    // when an int32 field is queried with a 64-bit number
    result_op = coll_mul_fields->search("captain", query_fields, "points: <2230070399", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Not an int32.", result_op.error().c_str());

    // using a string filter value against an integer field
    result_op = coll_mul_fields->search("captain", query_fields, "points: <sdsdfsdf", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());

    // large negative number
    result_op = coll_mul_fields->search("captain", query_fields, "points: >-3230070399", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(false, result_op.ok());

    // but should allow small negative number
    result_op = coll_mul_fields->search("captain", query_fields, "points: >-3230", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false});
    ASSERT_EQ(true, result_op.ok());

    collectionManager.drop_collection("coll_mul_fields");
}

TEST_F(CollectionFilteringTest, FilterOnNumericFields) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("rating", field_types::FLOAT, false),
            field("age", field_types::INT32, false),
            field("years", field_types::INT32_ARRAY, false),
            field("timestamps", field_types::INT64_ARRAY, false),
            field("tags", field_types::STRING_ARRAY, true)
    };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if(coll_array_fields == nullptr) {
        // ensure that default_sorting_field is a non-array numerical field
        auto coll_op = collectionManager.create_collection("coll_array_fields", 4, fields, "years");
        ASSERT_EQ(false, coll_op.ok());
        ASSERT_STREQ("Default sorting field `years` is not a sortable type.", coll_op.error().c_str());

        // let's try again properly
        coll_op = collectionManager.create_collection("coll_array_fields", 4, fields, "age");
        coll_array_fields = coll_op.get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    // Plain search with no filters - results should be sorted by rank fields
    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    std::vector<std::string> ids = {"3", "1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Searching on an int32 field
    results = coll_array_fields->search("Jeremy", query_fields, "age:>24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"3", "1", "4"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "age:>=24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "age:24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // alternative `:=` syntax
    results = coll_array_fields->search("Jeremy", query_fields, "age:=24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "age:= 24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // Searching a number against an int32 array field
    results = coll_array_fields->search("Jeremy", query_fields, "years:>2002", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"1", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "years:<1989", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"3"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // not equals
    results = coll_array_fields->search("Jeremy", query_fields, "age:!= 24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "1", "4", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple filters
    results = coll_array_fields->search("Jeremy", query_fields, "years:<2005 && years:>1987", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"4"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple search values (works like SQL's IN operator) against a single int field
    results = coll_array_fields->search("Jeremy", query_fields, "age:[21, 24, 63]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"3", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // alternative `:=` syntax
    results = coll_array_fields->search("Jeremy", query_fields, "age:= [21, 24, 63]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    // individual comparators can still be applied.
    results = coll_array_fields->search("Jeremy", query_fields, "age: [!=21, >30]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "1", "4", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_EQ(id, result_id);
    }

    // negate multiple search values (works like SQL's NOT IN) against a single int field
    results = coll_array_fields->search("Jeremy", query_fields, "age:!= [21, 24, 63]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"1", "4"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_EQ(id, result_id);
    }

    // individual comparators can still be applied.
    results = coll_array_fields->search("Jeremy", query_fields, "age: != [<30, >60]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"1", "4"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_EQ(id, result_id);
    }

    // multiple search values against an int32 array field - also use extra padding between symbols
    results = coll_array_fields->search("Jeremy", query_fields, "years : [ 2015, 1985 , 1999]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "1", "4", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // searching on an int64 array field - also ensure that padded space causes no issues
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps : > 475205222", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // range based filter
    results = coll_array_fields->search("Jeremy", query_fields, "age: 21..32", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"4", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "age: 0 .. 100", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "age: [21..24, 40..65]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "1", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "rating: 7.812 .. 9.999", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"1", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "rating: [7.812 .. 9.999, 1.05 .. 1.09]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    // when filters don't match any record, no results should be returned
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps:>1591091288061", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFilteringTest, FilterOnFloatFields) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("top_3", field_types::FLOAT_ARRAY, false),
            field("rating", field_types::FLOAT, false)
    };
    std::vector<sort_by> sort_fields_desc = { sort_by("rating", "DESC") };
    std::vector<sort_by> sort_fields_asc = { sort_by("rating", "ASC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        auto add_op = coll_array_fields->add(json_line);
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    // Plain search with no filters - results should be sorted by rating field DESC
    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    std::vector<std::string> ids = {"1", "2", "4", "0", "3"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Plain search with no filters - results should be sorted by rating field ASC
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields_asc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"3", "0", "4", "2", "1"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str()); //?
    }

    results = coll_array_fields->search("Jeremy", query_fields, "rating:!=0", facets, sort_fields_asc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"0", "4", "2", "1"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str()); //?
    }

    // Searching on a float field, sorted desc by rating
    results = coll_array_fields->search("Jeremy", query_fields, "rating:>0.0", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"1", "2", "4", "0"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Searching a float against an float array field
    results = coll_array_fields->search("Jeremy", query_fields, "top_3:>7.8", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"1", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple filters
    results = coll_array_fields->search("Jeremy", query_fields, "top_3:>7.8 && rating:>7.9", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"1"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple search values (works like SQL's IN operator) against a single float field
    results = coll_array_fields->search("Jeremy", query_fields, "rating:[1.09, 7.812]", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"2", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // negate multiple search values (works like SQL's NOT IN operator) against a single float field
    results = coll_array_fields->search("Jeremy", query_fields, "rating:!= [1.09, 7.812]", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"1", "4", "3"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_EQ(id, result_id);
    }

    // individual comparators can still be applied.
    results = coll_array_fields->search("Jeremy", query_fields, "rating: != [<5.4, >9]", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"2", "4"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_EQ(id, result_id);
    }

    // multiple search values against a float array field - also use extra padding between symbols
    results = coll_array_fields->search("Jeremy", query_fields, "top_3 : [ 5.431, 0.001 , 7.812, 11.992]", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"2", "4", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // when filters don't match any record, no results should be returned
    auto results_op = coll_array_fields->search("Jeremy", query_fields, "rating:<-2.78", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(results_op.ok());
    results = results_op.get();
    ASSERT_EQ(0, results["hits"].size());

    // rank tokens by default sorting field
    results_op = coll_array_fields->search("j", query_fields, "", facets, sort_fields_desc, {0}, 10, 1, MAX_SCORE, {true});
    ASSERT_TRUE(results_op.ok());
    results = results_op.get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"1", "2", "4", "0", "3"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFilteringTest, FilterOnNegativeNumericalFields) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("int32_field", field_types::INT32, false),
                                 field("int64_field", field_types::INT64, false),
                                 field("float_field", field_types::FLOAT, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "int32_field").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Title 1", "-100", "5000000", "-10.45124"},
        {"Title 2", "100", "-1000000", "0.45124"},
        {"Title 3", "-200", "3000000", "-0.45124"},
        {"Title 4", "150", "10000000", "1.45124"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["int32_field"] = std::stoi(records[i][1]);
        doc["int64_field"] = std::stoll(records[i][2]);
        doc["float_field"] = std::stof(records[i][3]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {}, "int32_field:<0", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "int64_field:<0", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "float_field:<0", {}, {sort_by("float_field", "desc")}, {0}, 10, 1, FREQUENCY,
                            {true}, 10).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, ComparatorsOnMultiValuedNumericalField) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("top_3", field_types::FLOAT_ARRAY, false),
            field("rating", field_types::FLOAT, false)
    };

    std::vector<sort_by> sort_fields_desc = {sort_by("rating", "DESC")};

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if (coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        auto add_op = coll_array_fields->add(json_line);
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "age: [24, >32]",
            facets, sort_fields_desc, {0}, 10, 1,FREQUENCY, {false}).get();

    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"1", "0", "3"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with <= and >=

    results = coll_array_fields->search("Jeremy", query_fields, "age: [<=24, >=44]",
                                        facets, sort_fields_desc, {0}, 10, 1,FREQUENCY, {false}).get();

    ASSERT_EQ(4, results["hits"].size());

    ids = {"1", "2", "0", "3"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFilteringTest, GeoPointFiltering) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Palais Garnier", "48.872576479306765, 2.332291112241466"},
        {"Sacre Coeur", "48.888286721920934, 2.342340862419206"},
        {"Arc de Triomphe", "48.87538726829884, 2.296113163780903"},
        {"Place de la Concorde", "48.86536119187326, 2.321850747347093"},
        {"Louvre Musuem", "48.86065813197502, 2.3381285349616725"},
        {"Les Invalides", "48.856648379569904, 2.3118555692631357"},
        {"Eiffel Tower", "48.85821022164442, 2.294239067890161"},
        {"Notre-Dame de Paris", "48.852455825574495, 2.35071182406452"},
        {"Musee Grevin", "48.872370541246816, 2.3431536410008906"},
        {"Pantheon", "48.84620987789056, 2.345152755563131"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a location close to only the Sacre Coeur
    auto results = coll1->search("*",
                                 {}, "loc: (48.90615915923891, 2.3435897727061175, 3 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // pick location close to none of the spots
    results = coll1->search("*",
                            {}, "loc: (48.910544830985785, 2.337218333651177, 2 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // pick a large radius covering all points

    results = coll1->search("*",
                            {}, "loc: (48.910544830985785, 2.337218333651177, 20 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(10, results["found"].get<size_t>());

    // 1 mile radius

    results = coll1->search("*",
                            {}, "loc: (48.85825332869331, 2.303816427653377, 1 mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("5", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // when geo field is formatted as string, show meaningful error
    nlohmann::json bad_doc;
    bad_doc["id"] = "1000";
    bad_doc["title"] = "Test record";
    bad_doc["loc"] = {"48.91", "2.33"};
    bad_doc["points"] = 1000;

    auto add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    bad_doc["loc"] = "foobar";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a 2 element array: [lat, lng].", add_op.error());

    bad_doc["loc"] = "loc: (48.910544830985785, 2.337218333651177, 2k)";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a 2 element array: [lat, lng].", add_op.error());

    bad_doc["loc"] = "loc: (48.910544830985785, 2.337218333651177, 2)";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a 2 element array: [lat, lng].", add_op.error());

    bad_doc["loc"] = {"foo", "bar"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    bad_doc["loc"] = {"2.33", "bar"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    bad_doc["loc"] = {"foo", "2.33"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be a geopoint.", add_op.error());

    // under coercion mode, it should work
    bad_doc["loc"] = {"48.91", "2.33"};
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, GeoPointArrayFiltering) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::vector<std::string>>> records = {
        {   {"Alpha Inc", "Ennore", "13.22112, 80.30511"},
            {"Alpha Inc", "Velachery", "12.98973, 80.23095"}
        },

        {
            {"Veera Inc", "Thiruvallur", "13.12752, 79.90136"},
        },

        {
            {"B1 Inc", "Bengaluru", "12.98246, 77.5847"},
            {"B1 Inc", "Hosur", "12.74147, 77.82915"},
            {"B1 Inc", "Vellore", "12.91866, 79.13075"},
        },

        {
            {"M Inc", "Nashik", "20.11282, 73.79458"},
            {"M Inc", "Pune", "18.56309, 73.855"},
        }
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0][0];
        doc["points"] = i;

        std::vector<std::vector<double>> lat_lngs;
        for(size_t k = 0; k < records[i].size(); k++) {
            std::vector<std::string> lat_lng_str;
            StringUtils::split(records[i][k][2], lat_lng_str, ", ");

            std::vector<double> lat_lng = {
                std::stod(lat_lng_str[0]),
                std::stod(lat_lng_str[1])
            };

            lat_lngs.push_back(lat_lng);
        }

        doc["loc"] = lat_lngs;
        auto add_op = coll1->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    // pick a location close to Chennai
    auto results = coll1->search("*",
                                 {}, "loc: (13.12631, 80.20252, 100km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // pick location close to none of the spots
    results = coll1->search("*",
                            {}, "loc: (13.62601, 79.39559, 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // pick a large radius covering all points

    results = coll1->search("*",
                            {}, "loc: (21.20714729927276, 78.99153966917213, 1000 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(4, results["found"].get<size_t>());

    // 1 mile radius

    results = coll1->search("*",
                            {}, "loc: (12.98941, 80.23073, 1mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // when geo field is formatted badly, show meaningful error
    nlohmann::json bad_doc;
    bad_doc["id"] = "1000";
    bad_doc["title"] = "Test record";
    bad_doc["loc"] = {"48.91", "2.33"};
    bad_doc["points"] = 1000;

    auto add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must contain 2 element arrays: [ [lat, lng],... ].", add_op.error());

    bad_doc["loc"] = "foobar";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array.", add_op.error());

    bad_doc["loc"] = nlohmann::json::array();
    nlohmann::json points = nlohmann::json::array();
    points.push_back("foo");
    points.push_back("bar");
    bad_doc["loc"].push_back(points);

    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array of geopoint.", add_op.error());

    bad_doc["loc"][0][0] = "2.33";
    bad_doc["loc"][0][1] = "bar";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array of geopoint.", add_op.error());

    bad_doc["loc"][0][0] = "foo";
    bad_doc["loc"][0][1] = "2.33";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `loc` must be an array of geopoint.", add_op.error());

    // under coercion mode, it should work
    bad_doc["loc"][0][0] = "48.91";
    bad_doc["loc"][0][1] = "2.33";
    add_op = coll1->add(bad_doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, GeoPointRemoval) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc1", field_types::GEOPOINT, false),
                                 field("loc2", field_types::GEOPOINT_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Palais Garnier";
    doc["loc1"] = {48.872576479306765, 2.332291112241466};
    doc["loc2"] = nlohmann::json::array();
    doc["loc2"][0] = {48.84620987789056, 2.345152755563131};
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("*",
                                 {}, "loc1: (48.87491151802846, 2.343945883701618, 1 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*",
                            {}, "loc2: (48.87491151802846, 2.343945883701618, 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    // remove the document, index another document and try querying again
    coll1->remove("0");
    doc["id"] = "1";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    results = coll1->search("*",
                            {}, "loc1: (48.87491151802846, 2.343945883701618, 1 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*",
                            {}, "loc2: (48.87491151802846, 2.343945883701618, 10 km)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionFilteringTest, GeoPolygonFiltering) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Palais Garnier", "48.872576479306765, 2.332291112241466"},
        {"Sacre Coeur", "48.888286721920934, 2.342340862419206"},
        {"Arc de Triomphe", "48.87538726829884, 2.296113163780903"},
        {"Place de la Concorde", "48.86536119187326, 2.321850747347093"},
        {"Louvre Musuem", "48.86065813197502, 2.3381285349616725"},
        {"Les Invalides", "48.856648379569904, 2.3118555692631357"},
        {"Eiffel Tower", "48.85821022164442, 2.294239067890161"},
        {"Notre-Dame de Paris", "48.852455825574495, 2.35071182406452"},
        {"Musee Grevin", "48.872370541246816, 2.3431536410008906"},
        {"Pantheon", "48.84620987789056, 2.345152755563131"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a location close to only the Sacre Coeur
    auto results = coll1->search("*",
                                 {}, "loc: (48.875223042424125,2.323509661928681, "
                                     "48.85745408145392, 2.3267084486160856, "
                                     "48.859636574404355,2.351469427048221, "
                                     "48.87756059389807, 2.3443610121873206)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("8", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // should work even if points of polygon are clockwise

    results = coll1->search("*",
                            {}, "loc: (48.87756059389807, 2.3443610121873206, "
                                    "48.859636574404355,2.351469427048221, "
                                    "48.85745408145392, 2.3267084486160856, "
                                    "48.875223042424125,2.323509661928681)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, GeoPolygonFilteringSouthAmerica) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"North of Equator", "4.48615, -71.38049"},
        {"South of Equator", "-8.48587, -71.02892"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a polygon that covers both points

    auto results = coll1->search("*",
                                 {}, "loc: (13.3163, -82.3585, "
                                     "-29.134, -82.3585, "
                                     "-29.134, -59.8528, "
                                     "13.3163, -59.8528)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, GeoPointFilteringWithNonSortableLocationField) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string", "sort": false},
            {"name": "loc", "type": "geopoint", "sort": false},
            {"name": "points", "type": "int32", "sort": false}
        ]
    })"_json;

    auto coll_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_op.ok());
    Collection* coll1 = coll_op.get();

    std::vector<std::vector<std::string>> records = {
        {"Palais Garnier", "48.872576479306765, 2.332291112241466"},
        {"Sacre Coeur", "48.888286721920934, 2.342340862419206"},
        {"Arc de Triomphe", "48.87538726829884, 2.296113163780903"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a location close to only the Sacre Coeur
    auto results = coll1->search("*",
                                 {}, "loc: (48.90615915923891, 2.3435897727061175, 3 km)",
                                 {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionFilteringTest, FilteringWithPrefixSearch) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
            {"elephant"}, {"emerald"}, {"effective"}, {"esther"}, {"eagle"},
            {"empty"}, {"elite"}, {"example"}, {"elated"}, {"end"},
            {"ear"}, {"eager"}, {"earmark"}, {"envelop"}, {"excess"},
            {"ember"}, {"earth"}, {"envoy"}, {"emerge"}, {"emigrant"},
            {"envision"}, {"envy"}, {"envisage"}, {"executive"}, {"end"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a location close to only the Sacre Coeur
    auto res_op = coll1->search("e",
                                {"title"}, "points: 23",
                                {}, {}, {0}, 10, 1, FREQUENCY, {true});

    auto results = res_op.get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("23", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, NumericalFilteringWithAnd) {
    Collection *coll1;

    std::vector<field> fields = {field("company_name", field_types::STRING, false),
                                 field("num_employees", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "num_employees").get();
    }

    std::vector<std::vector<std::string>> records = {
            {"123", "Company 1", "50"},
            {"125", "Company 2", "150"},
            {"127", "Company 3", "250"},
            {"129", "Stark Industries 4", "500"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = records[i][0];
        doc["company_name"] = records[i][1];
        doc["num_employees"] = std::stoi(records[i][2]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by("num_employees", "ASC") };

    auto results = coll1->search("*",
                                {}, "num_employees:>=100 && num_employees:<=300",
                                {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // when filter number is well below all values
    results = coll1->search("*",
                                 {}, "num_employees:>=100 && num_employees:<=10",
                                 {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // check boundaries
    results = coll1->search("*",
                            {}, "num_employees:>=150 && num_employees:<=250",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*",
                            {}, "num_employees:>150 && num_employees:<250",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(0, results["found"].get<size_t>());


    results = coll1->search("*",
                            {}, "num_employees:>50 && num_employees:<250",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // extreme boundaries

    results = coll1->search("*",
                            {}, "num_employees:>50 && num_employees:<=500",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("129", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*",
                            {}, "num_employees:>=50 && num_employees:<500",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_STREQ("123", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("125", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // no match
    results = coll1->search("*",
                            {}, "num_employees:>3000 && num_employees:<10",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, FilteringViaDocumentIds) {
    Collection *coll1;

    std::vector<field> fields = {field("company_name", field_types::STRING, false),
                                 field("num_employees", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "num_employees").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"123", "Company 1", "50"},
        {"125", "Company 2", "150"},
        {"127", "Company 3", "250"},
        {"129", "Stark Industries 4", "500"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = records[i][0];
        doc["company_name"] = records[i][1];
        doc["num_employees"] = std::stoi(records[i][2]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by("num_employees", "ASC") };

    auto results = coll1->search("*",
                                 {}, "id: 123",
                                 {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("123", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // single ID with backtick

    results = coll1->search("*",
                            {}, "id: `123`",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("123", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // single ID with condition
    results = coll1->search("*",
                            {}, "id: 125 && num_employees: 150",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // multiple IDs
    results = coll1->search("*",
                            {}, "id: [123, 125, 127, 129] && num_employees: <300",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_STREQ("123", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("125", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // multiple IDs with exact equals operator with IDs not being ordered
    results = coll1->search("*",
                            {}, "id:= [129, 123, 127, 125] && num_employees: <300",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_STREQ("123", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("125", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // multiple IDs with exact equals operator and backticks
    results = coll1->search("*",
                            {}, "id:= [`123`, `125`, `127`, `129`] && num_employees: <300",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_STREQ("123", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("125", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // empty id list not allowed
    auto res_op = coll1->search("*", {}, "id:=", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `id`: Filter value cannot be empty.", res_op.error());

    res_op = coll1->search("*", {}, "id:= ", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `id`: Filter value cannot be empty.", res_op.error());

    res_op = coll1->search("*", {}, "id: ", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `id`: Filter value cannot be empty.", res_op.error());

    // not equals is not supported yet
    res_op = coll1->search("*",
                            {}, "id:!= [123,125] && num_employees: <300",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Not equals filtering is not supported on the `id` field.", res_op.error());

    // when no IDs exist
    results = coll1->search("*",
                            {}, "id: [1000] && num_employees: <300",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll1->search("*",
                            {}, "id: 1000",
                            {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, NumericalFilteringWithArray) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("prices", field_types::INT32_ARRAY, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"1", "T Shirt 1", "1", "2", "3"},
        {"2", "T Shirt 2", "1", "2", "3"},
        {"3", "T Shirt 3", "1", "2", "3"},
        {"4", "T Shirt 4", "1", "1", "1"},
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = records[i][0];
        doc["title"] = records[i][1];

        std::vector<int32_t> prices;
        for(size_t j = 2; j <= 4; j++) {
            prices.push_back(std::stoi(records[i][j]));
        }

        doc["prices"] = prices;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // check equals on a repeating price
    auto results = coll1->search("*",
                                 {}, "prices:1",
                                 {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    // check ranges

    results = coll1->search("*",
                            {}, "prices:>=1",
                            {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    results = coll1->search("*",
                            {}, "prices:>=2",
                            {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    results = coll1->search("*",
                            {}, "prices:<4",
                            {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    results = coll1->search("*",
                            {}, "prices:<=2",
                            {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, NegationOperatorBasics) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 2, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Taylor Swift Karaoke: reputation", "Taylor Swift"},
        {"Beat it", "Michael Jackson"},
        {"Style", "Taylor Swift"},
        {"Thriller", "Michael Joseph Jackson"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {"artist"}, "artist:!=Michael Jackson", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*", {"artist"}, "artist:!= Michael Jackson && points: >0", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // negation operation on multiple values

    results = coll1->search("*", {"artist"}, "artist:!= [Michael Jackson, Taylor Swift]", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // when no such value exists: should return all results
    results = coll1->search("*", {"artist"}, "artist:!=Foobar", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(4, results["found"].get<size_t>());

    // empty value (bad filtering)
    auto res_op = coll1->search("*", {"artist"}, "artist:!=", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `artist`: Filter value cannot be empty.", res_op.error());

    res_op = coll1->search("*", {"artist"}, "artist:!= ", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `artist`: Filter value cannot be empty.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, FilterStringsWithComma) {
    Collection *coll1;

    std::vector<field> fields = {field("place", field_types::STRING, true),
                                 field("state", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"St. John's Cathedral, Denver, Colorado", "Colorado"},
        {"Crater Lake National Park, Oregon", "Oregon"},
        {"St. Patrick's Cathedral, Manhattan", "New York"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["place"] = records[i][0];
        doc["state"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {"place"}, "place:= St. John's Cathedral, Denver, Colorado", {}, {}, {0}, 10, 1,
                                 FREQUENCY, {true}, 10).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*", {"place"}, "place:= `St. John's Cathedral, Denver, Colorado`", {}, {}, {0}, 10, 1,
                            FREQUENCY, {true}, 10).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*", {"place"}, "place:= [`St. John's Cathedral, Denver, Colorado`]", {}, {}, {0}, 10, 1,
                            FREQUENCY, {true}, 10).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*", {"place"}, "place:= [`St. John's Cathedral, Denver, Colorado`, `St. Patrick's Cathedral, Manhattan`]", {}, {}, {0}, 10, 1,
                            FREQUENCY, {true}, 10).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_STREQ("2", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*", {"place"}, "place: [`Cathedral, Denver, Colorado`]", {}, {}, {0}, 10, 1,
                            FREQUENCY, {true}, 10).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, NumericalRangeFilter) {
    std::vector<field> fields = {field("company", field_types::STRING, true),
                                 field("num_employees", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "num_employees").get();

    std::vector<std::vector<std::string>> records = {
        {"123", "Company 1", "50"},
        {"125", "Company 2", "150"},
        {"127", "Company 3", "250"},
        {"129", "Stark Industries 4", "500"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = records[i][0];
        doc["company"] = records[i][1];
        doc["num_employees"] = std::stoi(records[i][2]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by("num_employees", "ASC") };

    auto results = coll1->search("*", {}, "num_employees:>=100 && num_employees:<=300", {}, sort_fields, {0}, 10, 1,
                                 FREQUENCY, {true}, 10).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, RangeFilterOnTimestamp) {
    std::vector<field> fields = {field("ts", field_types::INT64, false)};

    Collection* coll1 = collectionManager.create_collection(
            "coll1", 1, fields, "", 0, "", {}, {"."}
    ).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["ts"] = 1646092800000;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["ts"] = 1648771199000;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["ts"] = 1647111199000;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    auto results = coll1->search("*", {},"ts:[1646092800000..1648771199000]", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false}).get();

    ASSERT_EQ(3, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, QueryBoolFields) {
    Collection *coll_bool;

    std::ifstream infile(std::string(ROOT_DIR)+"test/bool_documents.jsonl");
    std::vector<field> fields = {
            field("popular", field_types::BOOL, false),
            field("title", field_types::STRING, false),
            field("rating", field_types::FLOAT, false),
            field("bool_array", field_types::BOOL_ARRAY, false),
    };

    std::vector<sort_by> sort_fields = { sort_by("popular", "DESC"), sort_by("rating", "DESC") };

    coll_bool = collectionManager.get_collection("coll_bool").get();
    if(coll_bool == nullptr) {
        coll_bool = collectionManager.create_collection("coll_bool", 1, fields, "rating").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_bool->add(json_line);
    }

    infile.close();

    // Plain search with no filters - results should be sorted correctly
    query_fields = {"title"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_bool->search("the", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    std::vector<std::string> ids = {"1", "3", "4", "9", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Searching on a bool field
    results = coll_bool->search("the", query_fields, "popular:true", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"1", "3", "4"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // alternative `:=` syntax
    results = coll_bool->search("the", query_fields, "popular:=true", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    results = coll_bool->search("the", query_fields, "popular:false", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_bool->search("the", query_fields, "popular:= false", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"9", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // searching against a bool array field

    // should be able to filter with an array of boolean values
    Option<nlohmann::json> res_op = coll_bool->search("the", query_fields, "bool_array:[true, false]", facets,
                                                      sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(5, results["hits"].size());

    results = coll_bool->search("the", query_fields, "bool_array: true", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());
    ids = {"1", "4", "9", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // should be able to search using array with a single element boolean value

    results = coll_bool->search("the", query_fields, "bool_array:[true]", facets,
                                 sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(4, results["hits"].size());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // not equals on bool field

    results = coll_bool->search("the", query_fields, "popular:!= true", facets,
                             sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("9", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    // not equals on bool array field
    results = coll_bool->search("the", query_fields, "bool_array:!= [true]", facets,
                                sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());

    // empty filter value
    res_op = coll_bool->search("the", query_fields, "bool_array:=", facets,
                                    sort_fields, {0}, 10, 1, FREQUENCY, {false});

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `bool_array`: Filter value cannot be empty.", res_op.error());

    collectionManager.drop_collection("coll_bool");
}

TEST_F(CollectionFilteringTest, FilteringWithTokenSeparators) {
    std::vector<field> fields = {field("code", field_types::STRING, true)};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "", 0, "", {}, {"."}
    ).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["code"] = "7318.15";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("*", {},"code:=7318.15", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*", {},"code:=`7318.15`", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");

    Collection* coll2 = collectionManager.create_collection(
            "coll2", 1, fields, "", 0, "", {"."}, {}
    ).get();

    doc1["id"] = "0";
    doc1["code"] = "7318.15";

    ASSERT_TRUE(coll2->add(doc1.dump()).ok());

    results = coll2->search("*", {},"code:=7318.15", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll2");
}

TEST_F(CollectionFilteringTest, ExactFilteringSingleQueryTerm) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false)};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "", 0, "", {}, {"."}
    ).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "AT&T GoPhone";
    doc1["tags"] = {"AT&T GoPhone"};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "AT&T";
    doc2["tags"] = {"AT&T"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("*", {},"name:=AT&T", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*", {},"tags:=AT&T", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Phone";
    doc3["tags"] = {"Samsung Phone", "Phone"};

    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    results = coll1->search("*", {},"tags:=Phone", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, ExactFilteringRepeatingTokensSingularField) {
    std::vector<field> fields = {field("name", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "", 0, "", {}, {"."}
    ).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Cardiology - Interventional Cardiology";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Cardiology - Interventional";

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Cardiology - Interventional Cardiology Department";

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["name"] = "Interventional Cardiology - Interventional Cardiology";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    auto results = coll1->search("*", {},"name:=Cardiology - Interventional Cardiology", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*", {},"name:=Cardiology - Interventional", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*", {},"name:=Interventional Cardiology", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("*", {},"name:=Cardiology", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, ExactFilteringRepeatingTokensArrayField) {
    std::vector<field> fields = {field("name", field_types::STRING_ARRAY, false)};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "", 0, "", {}, {"."}
    ).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = {"Cardiology - Interventional Cardiology"};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = {"Cardiology - Interventional"};

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = {"Cardiology - Interventional Cardiology Department"};

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["name"] = {"Interventional Cardiology - Interventional Cardiology"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    auto results = coll1->search("*", {},"name:=Cardiology - Interventional Cardiology", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*", {},"name:=Cardiology - Interventional", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*", {},"name:=Interventional Cardiology", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("*", {},"name:=Cardiology", {}, {}, {0}, 10,
                            1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, ExcludeMultipleTokens) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"alpha"},
        {"TXBT0eiYnFhkJHqz02Wv0PWN5hp1"},
        {"3u7RtEn5S9fcnizoUojWUwW23Yf2"},
        {"HpPALvzDDVc3zMmlAAUySwp8Ir33"},
        {"9oF2qhYI8sdBa2xJSerfmntpvBr2"},
        {"5fAnLlld5obG4vhhNIbIeoHe1uB2"},
        {"4OlIYKbzwIUoAOYy6dfDzCREezg1"},
        {"4JK1BvoqCuTeMwEZorlKj8hnSl02"},
        {"3tQBmRH0AQPEWyoKcDNYJyIxQQe2"},
        {"3Mvl5HZgNwQkHykAqL77oMfo8DW2"},
        {"3Ipnw5JATpYFyCcdUKTBhCicjoH3"},
        {"2rizUF2ntNSUVpaXwPdHmSBB6C63"},
        {"2kMHFOUQhAQK9cQbFNoXGpcAFVD2"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search(
            "-TXBT0eiYnFhkJHqz02Wv0PWN5hp1 -3u7RtEn5S9fcnizoUojWUwW23Yf2 -HpPALvzDDVc3zMmlAAUySwp8Ir33 "
            "-9oF2qhYI8sdBa2xJSerfmntpvBr2 -5fAnLlld5obG4vhhNIbIeoHe1uB2 -4OlIYKbzwIUoAOYy6dfDzCREezg1 "
            "-4JK1BvoqCuTeMwEZorlKj8hnSl02 -3tQBmRH0AQPEWyoKcDNYJyIxQQe2 -3Mvl5HZgNwQkHykAqL77oMfo8DW2 "
            "-3Ipnw5JATpYFyCcdUKTBhCicjoH3 -2rizUF2ntNSUVpaXwPdHmSBB6C63 -2kMHFOUQhAQK9cQbFNoXGpcAFVD2",
            {"title"}, "",
            {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, FilteringAfterUpsertOnArrayWithTokenSeparators) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),
                                 field("tag", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, "", {}, {"-"}).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "david";
    doc1["tags"] = {"alpha-beta-gamma", "foo-bar-baz"};
    doc1["tag"] = "foo-bar-baz";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "david";
    doc2["tags"] = {"alpha-gamma-beta", "bar-foo-baz"};
    doc2["tag"] = "alpha-beta";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("david", {"name"},"tags:=[foo-bar-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // upsert with "foo-bar-baz" removed
    doc1["tags"] = {"alpha-beta-gamma"};
    coll1->add(doc1.dump(), UPSERT);

    results = coll1->search("david", {"name"},"tags:=[foo-bar-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("david", {"name"},"tags:=[bar-foo-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // repeat for singular string field: upsert with "foo-bar-baz" removed
    doc1["tag"] = "alpha-beta-gamma";
    coll1->add(doc1.dump(), UPSERT);

    results = coll1->search("david", {"name"},"tag:=[foo-bar-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, FilteringAfterUpsertOnArrayWithSymbolsToIndex) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),
                                 field("tag", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, "", {"-"}, {}).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "david";
    doc1["tags"] = {"alpha-beta-gamma", "foo-bar-baz"};
    doc1["tag"] = "foo-bar-baz";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "david";
    doc2["tags"] = {"alpha-gamma-beta", "bar-foo-baz"};
    doc2["tag"] = "alpha-beta";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("david", {"name"},"tags:=[foo-bar-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // upsert with "foo-bar-baz" removed
    doc1["tags"] = {"alpha-beta-gamma"};
    coll1->add(doc1.dump(), UPSERT);

    results = coll1->search("david", {"name"},"tags:=[foo-bar-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("david", {"name"},"tags:=[bar-foo-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // repeat for singular string field: upsert with "foo-bar-baz" removed
    doc1["tag"] = "alpha-beta-gamma";
    coll1->add(doc1.dump(), UPSERT);

    results = coll1->search("david", {"name"},"tag:=[foo-bar-baz]", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, ComplexFilterQuery) {
    nlohmann::json schema_json =
            R"({
                "name": "ComplexFilterQueryCollection",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int32"},
                    {"name": "years", "type": "int32[]"},
                    {"name": "rating", "type": "float"}
                ]
            })"_json;

    auto op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(op.ok());
    auto coll = op.get();

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::string json_line;
    while (std::getline(infile, json_line)) {
        auto add_op = coll->add(json_line);
        ASSERT_TRUE(add_op.ok());
    }
    infile.close();

    std::vector<sort_by> sort_fields_desc = {sort_by("rating", "DESC")};
    nlohmann::json results = coll->search("Jeremy", {"name"}, "(rating:>=0 && years:>2000) && age:>50",
                                          {}, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll->search("Jeremy", {"name"}, "(age:>50 || rating:>5) && years:<2000",
                                          {}, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    std::vector<std::string> ids = {"4", "3"};
    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll->search("Jeremy", {"name"}, "(age:<50 && rating:10) || (years:>2000 && rating:<5)",
                                          {}, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"0"};
    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll->search("Jeremy", {"name"}, "years:>2000 && ((age:<30 && rating:>5) || (age:>50 && rating:<5))",
                           {}, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"2"};
    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("ComplexFilterQueryCollection");
}