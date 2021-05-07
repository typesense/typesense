#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include <h3api.h>
#include "collection.h"

class CollectionFilteringTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_filtering";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
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
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "tags: gold", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "tags : fine PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    // using just ":", filtering should return documents that contain ALL tokens in the filter expression
    results = coll_array_fields->search("Jeremy", query_fields, "tags : PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    // no documents contain both "white" and "platinum", so
    results = coll_array_fields->search("Jeremy", query_fields, "tags : WHITE PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    // with exact match operator (:=) partial matches are not allowed
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags : bronze", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"4", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // search with a list of tags, also testing extra padding of space
    results = coll_array_fields->search("Jeremy", query_fields, "tags: [bronze,   silver]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // need to be exact matches
    results = coll_array_fields->search("Jeremy", query_fields, "tags: bronze", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    // when comparators are used, they should be ignored
    results = coll_array_fields->search("Jeremy", query_fields, "tags:<bronze", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:<=BRONZE", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:>BRONZE", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

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
    auto results = coll_str->search("*", query_fields, "starring:= samuel", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<size_t>());

    // multiple tokens but with a typo on one of them
    results = coll_str->search("*", query_fields, "starring:= ssamuel l. Jackson", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<size_t>());

    // same should succeed when verbatim filter is made
    results = coll_str->search("*", query_fields, "starring:= samuel l. Jackson", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    // contains filter with a single token should work as well
    results = coll_str->search("*", query_fields, "starring: jackson", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    results = coll_str->search("*", query_fields, "starring: samuel", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    // contains when only 1 token so should not match
    results = coll_str->search("*", query_fields, "starring: samuel johnson", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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
    auto results = coll_array_fields->search("Jeremy", query_fields, "tags:= PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:= FINE", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:= FFINE PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    // partial token filter should be made without "=" operator
    results = coll_array_fields->search("Jeremy", query_fields, "tags: PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll_array_fields->search("Jeremy", query_fields, "tags: FINE", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<size_t>());

    // to make tokens match facet value exactly, use "=" operator
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= FINE PLATINUM", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<size_t>());

    // don't allow exact filter on non-faceted field
    auto res_op = coll_array_fields->search("Jeremy", query_fields, "name:= Jeremy Howard", facets, sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("To perform exact filtering, filter field `name` must be a facet field.", res_op.error().c_str());

    // multi match exact query (OR condition)
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= [Gold, bronze]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(3, results["found"].get<size_t>());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:= [Gold, bronze, fine PLATINUM]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(4, results["found"].get<size_t>());

    // single array multi match
    results = coll_array_fields->search("Jeremy", query_fields, "tags:= [fine PLATINUM]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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

    auto res = coll1->search("*", query_fields, "url:= https://example.com/1", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("1", res["hits"][0]["document"]["id"].get<std::string>().c_str());

    res = coll1->search("*", query_fields, "url: https://example.com/1", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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
                                 field("tags", field_types::STRING_ARRAY, false)};

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
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "tagzz: gold", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    // searching using a string for a numeric field
    results = coll_array_fields->search("Jeremy", query_fields, "age: abcdef", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    // searching using a string for a numeric array field
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps: abcdef", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    // malformed k:v syntax
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps abcdef", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    // just spaces - must be treated as empty filter
    results = coll_array_fields->search("Jeremy", query_fields, "  ", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    // wrapping number with quotes
    results = coll_array_fields->search("Jeremy", query_fields, "age: '21'", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

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
            coll_mul_fields->search("anton", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_TRUE(result_op.ok());

    nlohmann::json results = result_op.get();
    ASSERT_EQ(1, results["hits"].size());
    std::string solo_id = results["hits"].at(0)["document"]["id"];
    ASSERT_STREQ("14", solo_id.c_str());

    // filtering on string field should be possible
    query_fields = {"title"};
    result_op = coll_mul_fields->search("captain", query_fields, "starring: Samuel L. Jackson", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(true, result_op.ok());
    results = result_op.get();
    ASSERT_EQ(1, results["hits"].size());
    solo_id = results["hits"].at(0)["document"]["id"];
    ASSERT_STREQ("6", solo_id.c_str());

    // filtering on facet field should be possible (supports partial word search but without typo tolerance)
    query_fields = {"title"};
    result_op = coll_mul_fields->search("*", query_fields, "cast: chris", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(true, result_op.ok());
    results = result_op.get();
    ASSERT_EQ(3, results["hits"].size());

    // bad query string
    result_op = coll_mul_fields->search("captain", query_fields, "BLAH", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Could not parse the filter query.", result_op.error().c_str());

    // missing field
    result_op = coll_mul_fields->search("captain", query_fields, "age: 100", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Could not find a filter field named `age` in the schema.", result_op.error().c_str());

    // bad filter value type
    result_op = coll_mul_fields->search("captain", query_fields, "points: \"100\"", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Numerical field has an invalid comparator.", result_op.error().c_str());

    // bad filter value type - equaling float on an integer field
    result_op = coll_mul_fields->search("captain", query_fields, "points: 100.34", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Not an int32.", result_op.error().c_str());

    // bad filter value type - less than float on an integer field
    result_op = coll_mul_fields->search("captain", query_fields, "points: <100.0", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Not an int32.", result_op.error().c_str());

    // when an int32 field is queried with a 64-bit number
    result_op = coll_mul_fields->search("captain", query_fields, "points: <2230070399", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());
    ASSERT_STREQ("Error with filter field `points`: Not an int32.", result_op.error().c_str());

    // using a string filter value against an integer field
    result_op = coll_mul_fields->search("captain", query_fields, "points: <sdsdfsdf", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());

    // large negative number
    result_op = coll_mul_fields->search("captain", query_fields, "points: >-3230070399", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(false, result_op.ok());

    // but should allow small negative number
    result_op = coll_mul_fields->search("captain", query_fields, "points: >-3230", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
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
        ASSERT_STREQ("Default sorting field `years` must be a single valued numerical field.", coll_op.error().c_str());

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
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    std::vector<std::string> ids = {"3", "1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Searching on an int32 field
    results = coll_array_fields->search("Jeremy", query_fields, "age:>24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"3", "1", "4"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "age:>=24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "age:24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    // alternative `:=` syntax
    results = coll_array_fields->search("Jeremy", query_fields, "age:=24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "age:= 24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    // Searching a number against an int32 array field
    results = coll_array_fields->search("Jeremy", query_fields, "years:>2002", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"1", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "years:<1989", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"3"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple filters
    results = coll_array_fields->search("Jeremy", query_fields, "years:<2005 && years:>1987", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"4"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple search values (works like SQL's IN operator) against a single int field
    results = coll_array_fields->search("Jeremy", query_fields, "age:[21, 24, 63]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"3", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // alternative `:=` syntax
    results = coll_array_fields->search("Jeremy", query_fields, "age:= [21, 24, 63]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    // multiple search values against an int32 array field - also use extra padding between symbols
    results = coll_array_fields->search("Jeremy", query_fields, "years : [ 2015, 1985 , 1999]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "1", "4", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // searching on an int64 array field - also ensure that padded space causes no issues
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps : > 475205222", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // range based filter
    results = coll_array_fields->search("Jeremy", query_fields, "age: 21..32", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"4", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "age: 0 .. 100", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "age: [21..24, 40..65]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "1", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "rating: 7.812 .. 9.999", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"1", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "rating: [7.812 .. 9.999, 1.05 .. 1.09]", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    // when filters don't match any record, no results should be returned
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps:>1591091288061", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    std::vector<std::string> ids = {"1", "2", "4", "0", "3"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Plain search with no filters - results should be sorted by rating field ASC
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields_asc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"3", "0", "4", "2", "1"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str()); //?
    }

    // Searching on a float field, sorted desc by rating
    results = coll_array_fields->search("Jeremy", query_fields, "rating:>0.0", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"1", "2", "4", "0"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Searching a float against an float array field
    results = coll_array_fields->search("Jeremy", query_fields, "top_3:>7.8", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"1", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple filters
    results = coll_array_fields->search("Jeremy", query_fields, "top_3:>7.8 && rating:>7.9", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"1"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple search values (works like SQL's IN operator) against a single float field
    results = coll_array_fields->search("Jeremy", query_fields, "rating:[1.09, 7.812]", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"2", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple search values against a float array field - also use extra padding between symbols
    results = coll_array_fields->search("Jeremy", query_fields, "top_3 : [ 5.431, 0.001 , 7.812, 11.992]", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"2", "4", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // when filters don't match any record, no results should be returned
    auto results_op = coll_array_fields->search("Jeremy", query_fields, "rating:<-2.78", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false);
    ASSERT_TRUE(results_op.ok());
    results = results_op.get();
    ASSERT_EQ(0, results["hits"].size());

    // rank tokens by default sorting field
    results_op = coll_array_fields->search("j", query_fields, "", facets, sort_fields_desc, 0, 10, 1, MAX_SCORE, true);
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

    auto results = coll1->search("*", {}, "int32_field:<0", {}, {}, 0, 10, 1, FREQUENCY, true, 10).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "int64_field:<0", {}, {}, 0, 10, 1, FREQUENCY, true, 10).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "float_field:<0", {}, {sort_by("float_field", "desc")}, 0, 10, 1, FREQUENCY,
                            true, 10).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, ComparatorsOnMultiValuedNumericalField) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR) + "test/numeric_array_documents.jsonl");
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
            facets, sort_fields_desc, 0, 10, 1,FREQUENCY, false).get();

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
                                        facets, sort_fields_desc, 0, 10, 1,FREQUENCY, false).get();

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
                                 {}, {}, 0, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // pick location close to none of the spots
    results = coll1->search("*",
                            {}, "loc: (48.910544830985785, 2.337218333651177, 2 km)",
                            {}, {}, 0, 10, 1, FREQUENCY).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // pick a large radius covering all points

    results = coll1->search("*",
                            {}, "loc: (48.910544830985785, 2.337218333651177, 20 km)",
                            {}, {}, 0, 10, 1, FREQUENCY).get();

    ASSERT_EQ(10, results["found"].get<size_t>());

    // 1 mile radius

    results = coll1->search("*",
                            {}, "loc: (48.85825332869331, 2.303816427653377, 1 mi)",
                            {}, {}, 0, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("5", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
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
                                 {}, {}, 0, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("8", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
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
                                {}, {}, 0, 10, 1, FREQUENCY, true);

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
                                {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // when filter number is well below all values
    results = coll1->search("*",
                                 {}, "num_employees:>=100 && num_employees:<=10",
                                 {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(0, results["found"].get<size_t>());

    // check boundaries
    results = coll1->search("*",
                            {}, "num_employees:>=150 && num_employees:<=250",
                            {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*",
                            {}, "num_employees:>150 && num_employees:<250",
                            {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(0, results["found"].get<size_t>());


    results = coll1->search("*",
                            {}, "num_employees:>50 && num_employees:<250",
                            {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // extreme boundaries

    results = coll1->search("*",
                            {}, "num_employees:>50 && num_employees:<=500",
                            {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_STREQ("125", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("129", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*",
                            {}, "num_employees:>=50 && num_employees:<500",
                            {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_STREQ("123", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("125", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("127", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // no match
    results = coll1->search("*",
                            {}, "num_employees:>3000 && num_employees:<10",
                            {}, sort_fields, 0, 10, 1, FREQUENCY, true).get();

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
                                 {}, {}, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    // check ranges

    results = coll1->search("*",
                            {}, "prices:>=1",
                            {}, {}, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    results = coll1->search("*",
                            {}, "prices:>=2",
                            {}, {}, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    results = coll1->search("*",
                            {}, "prices:<4",
                            {}, {}, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    results = coll1->search("*",
                            {}, "prices:<=2",
                            {}, {}, 0, 10, 1, FREQUENCY, true).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFilteringTest, NegationOperatorBasics) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    auto results = coll1->search("*", {"artist"}, "artist:- Michael Jackson", {}, {}, 0, 10, 1, FREQUENCY, true, 10).get();

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*", {"artist"}, "artist:- Michael Jackson && points: >0", {}, {}, 0, 10, 1, FREQUENCY, true, 10).get();
    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // negation operation on multiple values

    results = coll1->search("*", {"artist"}, "artist:- [Michael Jackson, Taylor Swift]", {}, {}, 0, 10, 1, FREQUENCY, true, 10).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}