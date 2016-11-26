#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include "collection.h"

class CollectionTest : public ::testing::Test {
protected:
    Collection *collection;

    virtual void SetUp() {
        std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.jsonl");
        collection = new Collection("/tmp/typesense_test/collection");

        std::string json_line;

        while (std::getline(infile, json_line)) {
            collection->add(json_line);
        }

        infile.close();
    }

    virtual void TearDown() {
        delete collection;
    }
};

TEST_F(CollectionTest, ExactSearchShouldBeStable) {
    std::vector<nlohmann::json> results = collection->search("the", 0, 10);
    ASSERT_EQ(7, results.size());

    // For two documents of the same score, the larger doc_id appears first
    std::vector<std::string> ids = {"1", "6", "foo", "13", "10", "8", "16"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, ExactPhraseSearch) {
    std::vector<nlohmann::json> results = collection->search("rocket launch", 0, 10);
    ASSERT_EQ(4, results.size());

    /*
       Sort by (match, diff, score)
       8:   score: 12, diff: 0
       1:   score: 15, diff: 4
       17:  score: 8,  diff: 4
       16:  score: 10, diff: 5
    */

    std::vector<std::string> ids = {"8", "1", "17", "16"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, SkipUnindexedTokensDuringPhraseSearch) {
    // Tokens that are not found in the index should be skipped
    std::vector<nlohmann::json> results = collection->search("DoesNotExist from", 0, 10);
    ASSERT_EQ(2, results.size());

    std::vector<std::string> ids = {"2", "17"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with non-zero cost
    results = collection->search("DoesNotExist from", 2, 10);
    ASSERT_EQ(2, results.size());

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with 2 indexed words
    results = collection->search("from DoesNotExist insTruments", 2, 10);
    ASSERT_EQ(1, results.size());
    nlohmann::json result = results.at(0);
    std::string result_id = result["id"];
    ASSERT_STREQ("2", result_id.c_str());

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", 0, 10);
    ASSERT_EQ(0, results.size());

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", 2, 10);
    ASSERT_EQ(0, results.size());
}

TEST_F(CollectionTest, PartialPhraseSearch) {
    std::vector<nlohmann::json> results = collection->search("rocket research", 0, 10);
    ASSERT_EQ(4, results.size());

    std::vector<std::string> ids = {"1", "8", "16", "17"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, RegressionTest1) {
    std::vector<nlohmann::json> results = collection->search("kind biologcal", 2, 10);
    ASSERT_EQ(1, results.size());

    std::string result_id = results.at(0)["id"];
    ASSERT_STREQ("19", result_id.c_str());
}