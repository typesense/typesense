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