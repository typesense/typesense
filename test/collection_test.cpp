#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "collection.h"

class CollectionTest : public ::testing::Test {
protected:
    Collection *collection;
    std::vector<std::string> query_fields;
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::vector<sort_field> sort_fields;
    std::vector<field> facet_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection";
        std::cout << "Truncating and creating: " << state_dir_path << std::endl;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store);

        std::ifstream infile(std::string(ROOT_DIR)+"test/documents.jsonl");
        std::vector<field> search_fields = {field("title", field_types::STRING)};

        query_fields = {"title"};
        facet_fields = { };
        sort_fields = { sort_field("points", "DESC") };

        collection = collectionManager.get_collection("collection");
        if(collection == nullptr) {
            collection = collectionManager.create_collection("collection", search_fields, facet_fields,
                                                             sort_fields, "points");
        }

        std::string json_line;

        // dummy record for record id 0: to make the test record IDs to match with line numbers
        json_line = "{\"points\":10,\"title\":\"z\"}";
        collection->add(json_line);

        while (std::getline(infile, json_line)) {
            collection->add(json_line);
        }

        infile.close();
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.drop_collection("collection");
        delete store;
    }
};

TEST_F(CollectionTest, ExactSearchShouldBeStable) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("the", query_fields, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<int>());

    // For two documents of the same score, the larger doc_id appears first
    std::vector<std::string> ids = {"1", "6", "foo", "13", "10", "8", "16"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, ExactPhraseSearch) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("rocket launch", query_fields, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(5, results["hits"].size());

    /*
       Sort by (match, diff, score)
       8:   score: 12, diff: 0
       1:   score: 15, diff: 4
       17:  score: 8,  diff: 4
       16:  score: 10, diff: 5
       13:  score: 12, (single word match)
    */

    std::vector<std::string> ids = {"8", "1", "17", "16", "13"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Check pagination
    results = collection->search("rocket launch", query_fields, "", facets, sort_fields, 0, 3);
    ASSERT_EQ(3, results["hits"].size());
    for(size_t i = 0; i < 3; i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, SkipUnindexedTokensDuringPhraseSearch) {
    // Tokens that are not found in the index should be skipped
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("DoesNotExist from", query_fields, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(2, results["hits"].size());

    std::vector<std::string> ids = {"2", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with non-zero cost
    results = collection->search("DoesNotExist from", query_fields, "", facets, sort_fields, 1, 10);
    ASSERT_EQ(2, results["hits"].size());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with 2 indexed words
    results = collection->search("from DoesNotExist insTruments", query_fields, "", facets, sort_fields, 1, 10);
    ASSERT_EQ(2, results["hits"].size());
    ids = {"2", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", query_fields, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(0, results["hits"].size());

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", query_fields, "", facets, sort_fields, 2, 10);
    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionTest, PartialPhraseSearch) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("rocket research", query_fields, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"1", "8", "16", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, QueryWithTypo) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("kind biologcal", query_fields, "", facets, sort_fields, 2, 3);
    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"19", "20", "21"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("fer thx", query_fields, "", facets, sort_fields, 1, 3);
    ids = {"1", "10", "13"};

    ASSERT_EQ(3, results["hits"].size());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, TypoTokenRankedByScoreAndFrequency) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 2, MAX_SCORE, false);
    ASSERT_EQ(2, results["hits"].size());
    std::vector<std::string> ids = {"22", "23"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 3, FREQUENCY, false);
    ASSERT_EQ(3, results["hits"].size());
    ids = {"3", "12", "24"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Check pagination
    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 1, FREQUENCY, false);
    ASSERT_EQ(3, results["found"].get<int>());
    ASSERT_EQ(1, results["hits"].size());
    std::string solo_id = results["hits"].at(0)["id"];
    ASSERT_STREQ("3", solo_id.c_str());

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 2, FREQUENCY, false);
    ASSERT_EQ(3, results["found"].get<int>());
    ASSERT_EQ(2, results["hits"].size());

    // Check total ordering

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 10, FREQUENCY, false);
    ASSERT_EQ(5, results["hits"].size());
    ids = {"3", "12", "24", "22", "23"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 10, MAX_SCORE, false);
    ASSERT_EQ(5, results["hits"].size());
    ids = {"22", "23", "3", "12", "24"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, TextContainingAnActualTypo) {
    // A line contains "ISX" but not "what" - need to ensure that correction to "ISS what" happens
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("ISX what", query_fields, "", facets, sort_fields, 1, 4, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"19", "6", "21", "8"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Record containing exact token match should appear first
    results = collection->search("ISX", query_fields, "", facets, sort_fields, 1, 10, FREQUENCY, false);
    ASSERT_EQ(8, results["hits"].size());

    ids = {"20", "19", "6", "3", "21", "4", "10", "8"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, PrefixSearching) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("ex", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, true);
    ASSERT_EQ(2, results["hits"].size());
    std::vector<std::string> ids = {"12", "6"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("ex", query_fields, "", facets, sort_fields, 0, 10, MAX_SCORE, true);
    ASSERT_EQ(2, results["hits"].size());
    ids = {"6", "12"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, MultipleFields) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING), field("starring", field_types::STRING),
                                 field("cast", field_types::STRING_ARRAY)};
    std::vector<sort_field> sort_fields = { sort_field("points", "DESC") };

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields");
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", fields, facet_fields, sort_fields);
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_mul_fields->add(json_line);
    }

    infile.close();

    query_fields = {"title", "starring"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_mul_fields->search("Will", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"3", "2", "1", "0"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // when "starring" takes higher priority than "title"

    query_fields = {"starring", "title"};
    results = coll_mul_fields->search("thomas", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    ids = {"15", "14", "12", "13"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    query_fields = {"starring", "title", "cast"};
    results = coll_mul_fields->search("ben affleck", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(1, results["hits"].size());

    query_fields = {"cast"};
    results = coll_mul_fields->search("chris", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(3, results["hits"].size());

    ids = {"6", "1", "7"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    query_fields = {"cast"};
    results = coll_mul_fields->search("chris pine", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(3, results["hits"].size());

    ids = {"7", "6", "1"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, FilterOnNumericFields) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING), field("age", field_types::INT32),
                                 field("years", field_types::INT32_ARRAY),
                                 field("timestamps", field_types::INT64_ARRAY)};
    std::vector<sort_field> sort_fields = { sort_field("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, facet_fields, sort_fields);
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    // Plain search with no filters - results should be sorted by rank fields
    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(5, results["hits"].size());

    std::vector<std::string> ids = {"3", "1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Searching on an int32 field
    results = coll_array_fields->search("Jeremy", query_fields, "age:>24", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(3, results["hits"].size());

    ids = {"3", "1", "4"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "age:>=24", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "age:24", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(1, results["hits"].size());

    // Searching a number against an int32 array field
    results = coll_array_fields->search("Jeremy", query_fields, "years:>2002", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(3, results["hits"].size());

    ids = {"1", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "years:<1989", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(1, results["hits"].size());

    ids = {"3"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple filters
    results = coll_array_fields->search("Jeremy", query_fields, "years:<2005 && years:>1987", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(1, results["hits"].size());

    ids = {"4"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple search values (works like SQL's IN operator) against a single int field
    results = coll_array_fields->search("Jeremy", query_fields, "age:[21, 24, 63]", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(3, results["hits"].size());

    ids = {"3", "0", "2"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // multiple search values against an int32 array field - also use extra padding between symbols
    results = coll_array_fields->search("Jeremy", query_fields, "years : [ 2015, 1985 , 1999]", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "1", "4", "0"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // searching on an int64 array field - also ensure that padded space causes no issues
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps : > 475205222", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    ids = {"1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // when filters don't match any record, no results should be returned
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps:<1", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, FilterOnTextFields) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING), field("age", field_types::INT32),
                                 field("years", field_types::INT32_ARRAY),
                                 field("tags", field_types::STRING_ARRAY)};
    std::vector<sort_field> sort_fields = { sort_field("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, facet_fields, sort_fields);
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "tags: gold", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "tags : bronze", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(2, results["hits"].size());

    ids = {"4", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // search with a list of tags, also testing extra padding of space
    results = coll_array_fields->search("Jeremy", query_fields, "tags: [bronze,   silver]", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    ids = {"3", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // should be exact matches (no normalization or fuzzy searching should happen)
    results = coll_array_fields->search("Jeremy", query_fields, "tags: BRONZE", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, HandleBadlyFormedFilterQuery) {
    // should not crash when filter query is malformed!
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING), field("age", field_types::INT32),
                                 field("years", field_types::INT32_ARRAY),
                                 field("timestamps", field_types::INT64_ARRAY),
                                 field("tags", field_types::STRING_ARRAY)};
    std::vector<sort_field> sort_fields = { sort_field("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, facet_fields, sort_fields);
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;

    // when filter field does not exist in the schema
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "tagzz: gold", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    // searching using a string for a numeric field
    results = coll_array_fields->search("Jeremy", query_fields, "age: abcdef", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    // searching using a string for a numeric array field
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps: abcdef", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    // malformed k:v syntax
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps abcdef", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    // just empty spaces
    results = coll_array_fields->search("Jeremy", query_fields, "  ", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    // wrapping number with quotes
    results = coll_array_fields->search("Jeremy", query_fields, "age: '21'", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, FacetCounts) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING), field("age", field_types::INT32),
                                 field("years", field_types::INT32_ARRAY),
                                 field("timestamps", field_types::INT64_ARRAY),
                                 field("tags", field_types::STRING_ARRAY)};
    facet_fields = {field("tags", field_types::STRING_ARRAY), field("name", field_types::STRING)};
    std::vector<sort_field> sort_fields = { sort_field("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, facet_fields, sort_fields);
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets = {"tags"};

    // single facet with no filters
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(2, results["facet_counts"][0].size());
    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);

    ASSERT_EQ("gold", results["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(4, (int) results["facet_counts"][0]["counts"][1]["count"]);

    ASSERT_EQ("silver", results["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][2]["count"]);

    ASSERT_EQ("bronze", results["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);

    // 2 facets, 1 text filter with no filters
    facets.clear();
    facets.push_back("tags");
    facets.push_back("name");
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, FREQUENCY, false);

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(2, results["facet_counts"].size());

    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ("name", results["facet_counts"][1]["field_name"]);

    // facet value must one that's stored, not indexed (i.e. no tokenization/standardization)
    ASSERT_EQ("Jeremy Howard", results["facet_counts"][1]["counts"][0]["value"]);
    ASSERT_EQ(5, (int) results["facet_counts"][1]["counts"][0]["count"]);

    // facet with filters
    facets.clear();
    facets.push_back("tags");
    results = coll_array_fields->search("Jeremy", query_fields, "age: >24", facets, sort_fields, 0, 10, FREQUENCY, false);

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());

    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][2]["count"]);

    ASSERT_EQ("bronze", results["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ("gold", results["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ("silver", results["facet_counts"][0]["counts"][2]["value"]);

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, SearchingWithMissingFields) {
    // return error without crashing when searching for fields that do not conform to the schema
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING), field("age", field_types::INT32),
                                 field("years", field_types::INT32_ARRAY),
                                 field("timestamps", field_types::INT64_ARRAY),
                                 field("tags", field_types::STRING_ARRAY)};
    facet_fields = {field("tags", field_types::STRING_ARRAY), field("name", field_types::STRING)};
    std::vector<sort_field> sort_fields = { sort_field("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, facet_fields, sort_fields);
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    // when a query field mentioned in schema does not exist
    std::vector<std::string> facets;
    std::vector<std::string> query_fields_not_found = {"titlez"};

    nlohmann::json res = coll_array_fields->search("the", query_fields_not_found, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_STREQ("Could not find a search field named `titlez` in the schema.",res["error"].get<std::string>().c_str());

    // when a query field is an integer field
    res = coll_array_fields->search("the", {"age"}, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_STREQ("Search field `age` should be a string or a string array.", res["error"].get<std::string>().c_str());

    // when a facet field is not defined in the schema
    res = coll_array_fields->search("the", {"name"}, "", {"timestamps"}, sort_fields, 0, 10);
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_STREQ("Could not find a facet field named `timestamps` in the schema.", res["error"].get<std::string>().c_str());

    // when a rank field is not defined in the schema
    res = coll_array_fields->search("the", {"name"}, "", {}, { sort_field("timestamps", "ASC") }, 0, 10);
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_STREQ("Could not find a sort field named `timestamps` in the schema.", res["error"].get<std::string>().c_str());

    res = coll_array_fields->search("the", {"name"}, "", {}, { sort_field("_rank", "ASC") }, 0, 10);
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_STREQ("Could not find a sort field named `_rank` in the schema.", res["error"].get<std::string>().c_str());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, IndexingWithBadData) {
    // should not crash when document to-be-indexed doesn't match schema
    Collection *sample_collection;

    std::vector<field> fields = {field("name", field_types::STRING)};
    facet_fields = {field("tags", field_types::STRING_ARRAY)};
    std::vector<sort_field> sort_fields = { sort_field("age", "DESC"), sort_field("average", "DESC") };

    sample_collection = collectionManager.get_collection("sample_collection");
    if(sample_collection == nullptr) {
        sample_collection = collectionManager.create_collection("sample_collection", fields, facet_fields,
                                                                sort_fields, "age");
    }

    const Option<std::string> & search_fields_missing_op1 = sample_collection->add("{\"namezz\": \"foo\", \"age\": 29}");
    ASSERT_FALSE(search_fields_missing_op1.ok());
    ASSERT_STREQ("Field `name` has been declared as a search field in the schema, but is not found in the document.",
                 search_fields_missing_op1.error().c_str());

    const Option<std::string> & search_fields_missing_op2 = sample_collection->add("{\"namez\": \"foo\", \"age\": 34}");
    ASSERT_FALSE(search_fields_missing_op2.ok());
    ASSERT_STREQ("Field `name` has been declared as a search field in the schema, but is not found in the document.",
                 search_fields_missing_op2.error().c_str());

    const Option<std::string> & facet_fields_missing_op1 = sample_collection->add("{\"name\": \"foo\", \"age\": 34}");
    ASSERT_FALSE(facet_fields_missing_op1.ok());
    ASSERT_STREQ("Field `tags` has been declared as a facet field in the schema, but is not found in the document.",
                 facet_fields_missing_op1.error().c_str());

    const char *doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [\"red\", \"blue\"]}";
    const Option<std::string> & sort_fields_missing_op1 = sample_collection->add(doc_str);
    ASSERT_FALSE(sort_fields_missing_op1.ok());
    ASSERT_STREQ("Field `average` has been declared as a sort field in the schema, but is not found in the document.",
                 sort_fields_missing_op1.error().c_str());

    // Handle type errors

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": 22}";
    const Option<std::string> & bad_facet_field_op = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_facet_field_op.ok());
    ASSERT_STREQ("Facet field `tags` must be a STRING_ARRAY.",
                 bad_facet_field_op.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [], \"average\": 34}";
    const Option<std::string> & empty_facet_field_op = sample_collection->add(doc_str);
    ASSERT_TRUE(empty_facet_field_op.ok());

    doc_str = "{\"name\": \"foo\", \"age\": \"34\", \"tags\": [], \"average\": 34 }";
    const Option<std::string> & bad_token_ordering_field_op1 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_token_ordering_field_op1.ok());
    ASSERT_STREQ("Token ordering field `age` must be an INT32.", bad_token_ordering_field_op1.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 343234324234233234, \"tags\": [], \"average\": 34 }";
    const Option<std::string> & bad_token_ordering_field_op2 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_token_ordering_field_op2.ok());
    ASSERT_STREQ("Token ordering field `age` exceeds maximum value of INT32.", bad_token_ordering_field_op2.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [], \"average\": \"34\"}";
    const Option<std::string> & bad_rank_field_op = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_rank_field_op.ok());
    ASSERT_STREQ("Sort field `average` must be an integer.", bad_rank_field_op.error().c_str());

    collectionManager.drop_collection("sample_collection");
}

TEST_F(CollectionTest, EmptyIndexShouldNotCrash) {
    Collection *empty_coll;

    std::vector<field> fields = {field("name", field_types::STRING)};
    facet_fields = {field("tags", field_types::STRING_ARRAY)};
    std::vector<sort_field> sort_fields = { sort_field("age", "DESC"), sort_field("average", "DESC") };

    empty_coll = collectionManager.get_collection("empty_coll");
    if(empty_coll == nullptr) {
        empty_coll = collectionManager.create_collection("empty_coll", fields, facet_fields, sort_fields, "age");
    }

    nlohmann::json results = empty_coll->search("a", {"name"}, "", {}, sort_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(0, results["hits"].size());
}