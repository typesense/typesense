#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "collection.h"

class CollectionTest : public ::testing::Test {
protected:
    Collection *collection;
    std::vector<std::string> search_fields;
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection";
        std::cout << "Truncating and creating: " << state_dir_path << std::endl;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store);

        std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.jsonl");
        std::vector<field> fields = {field("title", field_types::STRING)};
        std::vector<std::string> rank_fields = {"points"};
        search_fields = {"title"};

        collection = collectionManager.get_collection("collection");
        if(collection == nullptr) {
            collection = collectionManager.create_collection("collection", fields, rank_fields);
        }

        std::string json_line;

        while (std::getline(infile, json_line)) {
            collection->add(json_line);
        }

        infile.close();
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        delete store;
    }
};

TEST_F(CollectionTest, ExactSearchShouldBeStable) {
    std::vector<nlohmann::json> results = collection->search("the", search_fields, 0, 10);
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
    std::vector<nlohmann::json> results = collection->search("rocket launch", search_fields, 0, 10);
    ASSERT_EQ(5, results.size());

    /*
       Sort by (match, diff, score)
       8:   score: 12, diff: 0
       1:   score: 15, diff: 4
       17:  score: 8,  diff: 4
       16:  score: 10, diff: 5
       13:  score: 12, (single word match)
    */

    std::vector<std::string> ids = {"8", "1", "17", "16", "13"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Check pagination
    results = collection->search("rocket launch", search_fields, 0, 3);
    ASSERT_EQ(3, results.size());
    for(size_t i = 0; i < 3; i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, SkipUnindexedTokensDuringPhraseSearch) {
    // Tokens that are not found in the index should be skipped
    std::vector<nlohmann::json> results = collection->search("DoesNotExist from", search_fields, 0, 10);
    ASSERT_EQ(2, results.size());

    std::vector<std::string> ids = {"2", "17"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with non-zero cost
    results = collection->search("DoesNotExist from", search_fields, 1, 10);
    ASSERT_EQ(2, results.size());

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with 2 indexed words
    results = collection->search("from DoesNotExist insTruments", search_fields, 1, 10);
    ASSERT_EQ(2, results.size());
    ids = {"2", "17"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string id = ids.at(i);
        std::string result_id = result["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", search_fields, 0, 10);
    ASSERT_EQ(0, results.size());

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", search_fields, 2, 10);
    ASSERT_EQ(0, results.size());
}

TEST_F(CollectionTest, PartialPhraseSearch) {
    std::vector<nlohmann::json> results = collection->search("rocket research", search_fields, 0, 10);
    ASSERT_EQ(4, results.size());

    std::vector<std::string> ids = {"1", "8", "16", "17"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, QueryWithTypo) {
    std::vector<nlohmann::json> results = collection->search("kind biologcal", search_fields, 2, 3);
    ASSERT_EQ(3, results.size());

    std::vector<std::string> ids = {"19", "20", "21"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("fer thx", search_fields, 1, 3);
    ids = {"1", "10", "13"};

    ASSERT_EQ(3, results.size());

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, TypoTokenRankedByScoreAndFrequency) {
    std::vector<nlohmann::json> results = collection->search("loox", search_fields, 1, 2, MAX_SCORE, false);
    ASSERT_EQ(2, results.size());
    std::vector<std::string> ids = {"22", "23"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", search_fields, 1, 3, FREQUENCY, false);
    ASSERT_EQ(3, results.size());
    ids = {"3", "12", "24"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Check pagination
    results = collection->search("loox", search_fields, 1, 1, FREQUENCY, false);
    ASSERT_EQ(1, results.size());
    std::string solo_id = results.at(0)["id"];
    ASSERT_STREQ("3", solo_id.c_str());

    results = collection->search("loox", search_fields, 1, 2, FREQUENCY, false);
    ASSERT_EQ(2, results.size());

    // Check total ordering

    results = collection->search("loox", search_fields, 1, 10, FREQUENCY, false);
    ASSERT_EQ(5, results.size());
    ids = {"3", "12", "24", "22", "23"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", search_fields, 1, 10, MAX_SCORE, false);
    ASSERT_EQ(5, results.size());
    ids = {"22", "23", "3", "12", "24"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, TextContainingAnActualTypo) {
    // A line contains "ISX" but not "what" - need to ensure that correction to "ISS what" happens
    std::vector<nlohmann::json> results = collection->search("ISX what", search_fields, 1, 4, FREQUENCY, false);
    ASSERT_EQ(4, results.size());

    std::vector<std::string> ids = {"19", "6", "21", "8"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Record containing exact token match should appear first
    results = collection->search("ISX", search_fields, 1, 10, FREQUENCY, false);
    ASSERT_EQ(8, results.size());

    ids = {"20", "19", "6", "3", "21", "4", "10", "8"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, PrefixSearching) {
    std::vector<nlohmann::json> results = collection->search("ex", search_fields, 0, 10, FREQUENCY, true);
    ASSERT_EQ(2, results.size());
    std::vector<std::string> ids = {"12", "6"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("ex", search_fields, 0, 10, MAX_SCORE, true);
    ASSERT_EQ(2, results.size());
    ids = {"6", "12"};

    for(size_t i = 0; i < results.size(); i++) {
        nlohmann::json result = results.at(i);
        std::string result_id = result["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, MultipleFields) {
    /*Collection *coll_mul_fields;

    std::ifstream infile("/Users/kishore/others/wreally/typesense/test/multi_field_documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING), field("starring", field_types::STRING)};
    std::vector<std::string> rank_fields = {"points"};
    coll_mul_fields = new Collection("/tmp/typesense_test/coll_mul_fields", "coll_mul_fields", fields, rank_fields);

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_mul_fields->add(json_line);
    }

    infile.close();

    search_fields = {"title", "starring"};

    delete coll_mul_fields;*/
}