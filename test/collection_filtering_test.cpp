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

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_filtering";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
        collectionManager.load();
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

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
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

TEST_F(CollectionFilteringTest, FilterOnTextFieldWithColon) {
    Collection *coll1;

    std::vector<field> fields = {field("url", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1");
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

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
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

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields");
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
    ASSERT_STREQ("Error with filter field `points`: Numerical field has an invalid comparator.", result_op.error().c_str());

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
            field("age", field_types::INT32, false),
            field("years", field_types::INT32_ARRAY, false),
            field("timestamps", field_types::INT64_ARRAY, false),
            field("tags", field_types::STRING_ARRAY, true)
    };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
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

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
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
    Option<nlohmann::json> results_op = coll_array_fields->search("Jeremy", query_fields, "rating:<-2.78", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_TRUE(results_op.ok());
    results = results_op.get();
    ASSERT_EQ(0, results["hits"].size());

    // rank tokens by default sorting field
    results_op = coll_array_fields->search("j", query_fields, "", facets, sort_fields_desc, 0, 10, 1, MAX_SCORE, true).get();
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
