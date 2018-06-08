#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"
#include "number.h"

class CollectionTest : public ::testing::Test {
protected:
    Collection *collection;
    std::vector<std::string> query_fields;
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, "auth_key", "search_auth_key");

        std::ifstream infile(std::string(ROOT_DIR)+"test/documents.jsonl");
        std::vector<field> search_fields = {
            field("title", field_types::STRING, false),
            field("points", field_types::INT32, false)
        };

        query_fields = {"title"};
        sort_fields = { sort_by("points", "DESC") };

        collection = collectionManager.get_collection("collection");
        if(collection == nullptr) {
            collection = collectionManager.create_collection("collection", search_fields, "points").get();
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

TEST_F(CollectionTest, VerifyCountOfDocuments) {
    // we have 1 dummy record to match the line numbers on the fixtures file with sequence numbers
    ASSERT_EQ(24+1, collection->get_num_documents());
}

TEST_F(CollectionTest, RetrieveADocumentById) {
    Option<nlohmann::json> doc_option = collection->get("1");
    ASSERT_TRUE(doc_option.ok());
    nlohmann::json doc = doc_option.get();
    std::string id = doc["id"];

    doc_option = collection->get("foo");
    ASSERT_TRUE(doc_option.ok());
    doc = doc_option.get();
    id = doc["id"];
    ASSERT_STREQ("foo", id.c_str());

    doc_option = collection->get("baz");
    ASSERT_FALSE(doc_option.ok());
}

TEST_F(CollectionTest, ExactSearchShouldBeStable) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("the", query_fields, "", facets, sort_fields, 0, 10).get();
    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<int>());

    // For two documents of the same score, the larger doc_id appears first
    std::vector<std::string> ids = {"1", "6", "foo", "13", "10", "8", "16"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // check ASC sorting
    std::vector<sort_by> sort_fields_asc = { sort_by("points", "ASC") };

    results = collection->search("the", query_fields, "", facets, sort_fields_asc, 0, 10).get();
    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<int>());

    ids = {"16", "13", "10", "8", "6", "foo", "1"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, ExactPhraseSearch) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("rocket launch", query_fields, "", facets, sort_fields, 0, 10).get();
    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(5, results["found"].get<uint32_t>());

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
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    ASSERT_EQ(results["hits"][0]["highlights"].size(), (unsigned long) 1);
    ASSERT_STREQ(results["hits"][0]["highlights"][0]["field"].get<std::string>().c_str(), "title");
    ASSERT_STREQ(results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str(),
                 "What is the power requirement of a <mark>rocket</mark> <mark>launch</mark> these days?");

    // Check ASC sort order
    std::vector<sort_by> sort_fields_asc = { sort_by("points", "ASC") };
    results = collection->search("rocket launch", query_fields, "", facets, sort_fields_asc, 0, 10).get();
    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(5, results["found"].get<uint32_t>());

    ids = {"8", "17", "1", "16", "13"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Check pagination
    results = collection->search("rocket launch", query_fields, "", facets, sort_fields, 0, 3).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(5, results["found"].get<uint32_t>());

    ids = {"8", "1", "17"};

    for(size_t i = 0; i < 3; i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, SkipUnindexedTokensDuringPhraseSearch) {
    // Tokens that are not found in the index should be skipped
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("DoesNotExist from", query_fields, "", facets, sort_fields, 0, 10).get();
    ASSERT_EQ(2, results["hits"].size());

    std::vector<std::string> ids = {"2", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with non-zero cost
    results = collection->search("DoesNotExist from", query_fields, "", facets, sort_fields, 1, 10).get();
    ASSERT_EQ(2, results["hits"].size());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with 2 indexed words
    results = collection->search("from DoesNotExist insTruments", query_fields, "", facets, sort_fields, 1, 10).get();
    ASSERT_EQ(2, results["hits"].size());
    ids = {"2", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // should not try to drop tokens to expand query
    results.clear();
    results = collection->search("the a", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false, 10).get();
    ASSERT_EQ(8, results["hits"].size());

    results.clear();
    results = collection->search("the a", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false, 0).get();
    ASSERT_EQ(3, results["hits"].size());
    ids = {"8", "16", "10"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("the a DoesNotExist", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // with no indexed word
    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", query_fields, "", facets, sort_fields, 0, 10).get();
    ASSERT_EQ(0, results["hits"].size());

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", query_fields, "", facets, sort_fields, 2, 10).get();
    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionTest, PartialPhraseSearch) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("rocket research", query_fields, "", facets, sort_fields, 0, 10).get();
    ASSERT_EQ(6, results["hits"].size());

    std::vector<std::string> ids = {"19", "1", "10", "8", "16", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, QueryWithTypo) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("kind biologcal", query_fields, "", facets, sort_fields, 2, 3).get();
    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"19", "20", "21"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("fer thx", query_fields, "", facets, sort_fields, 1, 3).get();
    ids = {"1", "10", "13"};

    ASSERT_EQ(3, results["hits"].size());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, TypoTokenRankedByScoreAndFrequency) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 2, 1, MAX_SCORE, false).get();
    ASSERT_EQ(2, results["hits"].size());
    std::vector<std::string> ids = {"22", "3"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 3, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());
    ids = {"22", "3", "12"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Check pagination
    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 1, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["found"].get<int>());
    ASSERT_EQ(1, results["hits"].size());
    std::string solo_id = results["hits"].at(0)["document"]["id"];
    ASSERT_STREQ("22", solo_id.c_str());

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 2, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["found"].get<int>());
    ASSERT_EQ(2, results["hits"].size());

    // Check total ordering

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());
    ids = {"22", "3", "12", "23", "24"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", query_fields, "", facets, sort_fields, 1, 10, 1, MAX_SCORE, false).get();
    ASSERT_EQ(5, results["hits"].size());
    ids = {"22", "3", "12", "23", "24"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, TextContainingAnActualTypo) {
    // A line contains "ISX" but not "what" - need to ensure that correction to "ISS what" happens
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("ISX what", query_fields, "", facets, sort_fields, 1, 4, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(9, results["found"].get<uint32_t>());

    std::vector<std::string> ids = {"8", "19", "6", "21"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Record containing exact token match should appear first
    results = collection->search("ISX", query_fields, "", facets, sort_fields, 1, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(8, results["hits"].size());
    ASSERT_EQ(8, results["found"].get<uint32_t>());

    ids = {"20", "19", "6", "4", "3", "10", "8", "21"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, Pagination) {
    nlohmann::json results = collection->search("the", query_fields, "", {}, sort_fields, 0, 3, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<uint32_t>());

    std::vector<std::string> ids = {"1", "6", "foo"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("the", query_fields, "", {}, sort_fields, 0, 3, 2, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<uint32_t>());

    ids = {"13", "10", "8"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("the", query_fields, "", {}, sort_fields, 0, 3, 3, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<uint32_t>());

    ids = {"16"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, WildcardQuery) {
    nlohmann::json results = collection->search("*", query_fields, "points:>0", {}, sort_fields, 0, 3, 1, FREQUENCY,
                                                false).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<uint32_t>());

    // when no filter is specified, fall back on default sorting field based catch-all filter
    Option<nlohmann::json> results_op = collection->search("*", query_fields, "", {}, sort_fields, 0, 3, 1, FREQUENCY,
                                                           false);

    ASSERT_TRUE(results_op.ok());
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<uint32_t>());
}

TEST_F(CollectionTest, PrefixSearching) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("ex", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, true).get();
    ASSERT_EQ(2, results["hits"].size());
    std::vector<std::string> ids = {"6", "12"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("ex", query_fields, "", facets, sort_fields, 0, 10, 1, MAX_SCORE, true).get();
    ASSERT_EQ(2, results["hits"].size());
    ids = {"6", "12"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("what ex", query_fields, "", facets, sort_fields, 0, 10, 1, MAX_SCORE, true).get();
    ASSERT_EQ(9, results["hits"].size());
    ids = {"6", "12", "19", "22", "13", "8", "15", "24", "21"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // restrict to only 2 results and differentiate between MAX_SCORE and FREQUENCY
    results = collection->search("t", query_fields, "", facets, sort_fields, 0, 2, 1, MAX_SCORE, true).get();
    ASSERT_EQ(2, results["hits"].size());
    ids = {"19", "22"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("t", query_fields, "", facets, sort_fields, 0, 2, 1, FREQUENCY, true).get();
    ASSERT_EQ(2, results["hits"].size());
    ids = {"19", "22"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // only the last token in the query should be used for prefix search - so, "math" should not match "mathematics"
    results = collection->search("math fx", query_fields, "", facets, sort_fields, 0, 1, 1, FREQUENCY, true).get();
    ASSERT_EQ(0, results["hits"].size());

    // single and double char prefixes should set a ceiling on the num_typos possible
    results = collection->search("x", query_fields, "", facets, sort_fields, 2, 2, 1, FREQUENCY, true).get();
    ASSERT_EQ(0, results["hits"].size());

    results = collection->search("xq", query_fields, "", facets, sort_fields, 2, 2, 1, FREQUENCY, true).get();
    ASSERT_EQ(1, results["hits"].size());
    ids = {"6"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // prefix with a typo
    results = collection->search("late propx", query_fields, "", facets, sort_fields, 2, 1, 1, FREQUENCY, true).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("16", results["hits"].at(0)["document"]["id"]);
}

TEST_F(CollectionTest, ArrayStringFieldHighlight) {
    Collection *coll_array_text;

    std::ifstream infile(std::string(ROOT_DIR) + "test/array_text_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("tags", field_types::STRING_ARRAY, false),
            field("points", field_types::INT32, false)
    };

    coll_array_text = collectionManager.get_collection("coll_array_text");
    if (coll_array_text == nullptr) {
        coll_array_text = collectionManager.create_collection("coll_array_text", fields, "points").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_text->add(json_line);
    }

    infile.close();

    query_fields = {"tags"};
    std::vector<std::string> facets;

    nlohmann::json results = coll_array_text->search("truth about", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                                     false, 0).get();
    ASSERT_EQ(1, results["hits"].size());

    std::vector<std::string> ids = {"0"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    ASSERT_EQ(results["hits"][0]["highlights"].size(), 1);
    ASSERT_STREQ(results["hits"][0]["highlights"][0]["field"].get<std::string>().c_str(), "tags");

    // an array's snippets must be sorted on match score, if match score is same, priority to be given to lower indices
    ASSERT_EQ(3, results["hits"][0]["highlights"][0]["snippets"].size());
    ASSERT_STREQ("<mark>truth</mark> <mark>about</mark>", results["hits"][0]["highlights"][0]["snippets"][0].get<std::string>().c_str());
    ASSERT_STREQ("the <mark>truth</mark>", results["hits"][0]["highlights"][0]["snippets"][1].get<std::string>().c_str());
    ASSERT_STREQ("<mark>about</mark> forever", results["hits"][0]["highlights"][0]["snippets"][2].get<std::string>().c_str());

    ASSERT_EQ(3, results["hits"][0]["highlights"][0]["indices"].size());
    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["indices"][0]);
    ASSERT_EQ(0, results["hits"][0]["highlights"][0]["indices"][1]);
    ASSERT_EQ(1, results["hits"][0]["highlights"][0]["indices"][2]);

    results = coll_array_text->search("forever truth", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                      false, 0).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"0"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    ASSERT_STREQ(results["hits"][0]["highlights"][0]["field"].get<std::string>().c_str(), "tags");
    ASSERT_EQ(3, results["hits"][0]["highlights"][0]["snippets"].size());
    ASSERT_STREQ("the <mark>truth</mark>", results["hits"][0]["highlights"][0]["snippets"][0].get<std::string>().c_str());
    ASSERT_STREQ("about <mark>forever</mark>", results["hits"][0]["highlights"][0]["snippets"][1].get<std::string>().c_str());
    ASSERT_STREQ("<mark>truth</mark> about", results["hits"][0]["highlights"][0]["snippets"][2].get<std::string>().c_str());
    ASSERT_EQ(3, results["hits"][0]["highlights"][0]["indices"].size());
    ASSERT_EQ(0, results["hits"][0]["highlights"][0]["indices"][0]);
    ASSERT_EQ(1, results["hits"][0]["highlights"][0]["indices"][1]);
    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["indices"][2]);

    results = coll_array_text->search("truth", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                      false, 0).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"0", "1"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_text->search("asdadasd", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                      false, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    query_fields = {"title", "tags"};
    results = coll_array_text->search("truth", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                      false, 0).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["hits"][0]["highlights"].size());

    ids = {"0", "1"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    ASSERT_EQ(2, results["hits"][0]["highlights"][0].size());
    ASSERT_STREQ(results["hits"][0]["highlights"][0]["field"].get<std::string>().c_str(), "title");
    ASSERT_STREQ(results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str(), "The <mark>Truth</mark> About Forever");

    ASSERT_EQ(3, results["hits"][0]["highlights"][1].size());
    ASSERT_STREQ(results["hits"][0]["highlights"][1]["field"].get<std::string>().c_str(), "tags");
    ASSERT_EQ(2, results["hits"][0]["highlights"][1]["snippets"].size());
    ASSERT_STREQ("the <mark>truth</mark>", results["hits"][0]["highlights"][1]["snippets"][0].get<std::string>().c_str());
    ASSERT_STREQ("<mark>truth</mark> about", results["hits"][0]["highlights"][1]["snippets"][1].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][0]["highlights"][1]["indices"].size());
    ASSERT_EQ(0, results["hits"][0]["highlights"][1]["indices"][0]);
    ASSERT_EQ(2, results["hits"][0]["highlights"][1]["indices"][1]);

    ASSERT_EQ(2, results["hits"][1]["highlights"][0].size());
    ASSERT_STREQ(results["hits"][1]["highlights"][0]["field"].get<std::string>().c_str(), "title");
    ASSERT_STREQ(results["hits"][1]["highlights"][0]["snippet"].get<std::string>().c_str(), "Plain <mark>Truth</mark>");

    ASSERT_EQ(3, results["hits"][1]["highlights"][1].size());
    ASSERT_STREQ(results["hits"][1]["highlights"][1]["field"].get<std::string>().c_str(), "tags");

    ASSERT_EQ(2, results["hits"][1]["highlights"][1]["snippets"].size());
    ASSERT_STREQ("<mark>truth</mark>", results["hits"][1]["highlights"][1]["snippets"][0].get<std::string>().c_str());
    ASSERT_STREQ("plain <mark>truth</mark>", results["hits"][1]["highlights"][1]["snippets"][1].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][1]["highlights"][1]["indices"].size());
    ASSERT_EQ(1, results["hits"][1]["highlights"][1]["indices"][0]);
    ASSERT_EQ(2, results["hits"][1]["highlights"][1]["indices"][1]);

    // highlight fields must be ordered based on match score
    results = coll_array_text->search("amazing movie", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                      false, 0).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(2, results["hits"][0]["highlights"].size());

    ASSERT_EQ(3, results["hits"][0]["highlights"][0].size());
    ASSERT_STREQ("tags", results["hits"][0]["highlights"][0]["field"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>amazing</mark> <mark>movie</mark>", results["hits"][0]["highlights"][0]["snippets"][0].get<std::string>().c_str());
    ASSERT_EQ(1, results["hits"][0]["highlights"][0]["indices"].size());
    ASSERT_EQ(0, results["hits"][0]["highlights"][0]["indices"][0]);

    ASSERT_EQ(2, results["hits"][0]["highlights"][1].size());
    ASSERT_STREQ(results["hits"][0]["highlights"][1]["field"].get<std::string>().c_str(), "title");
    ASSERT_STREQ(results["hits"][0]["highlights"][1]["snippet"].get<std::string>().c_str(),
                 "<mark>Amazing</mark> Spiderman is <mark>amazing</mark>"); // should highlight duplicating tokens

    collectionManager.drop_collection("coll_array_text");
}

TEST_F(CollectionTest, MultipleFields) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("starring", field_types::STRING, false),
            field("cast", field_types::STRING_ARRAY, false),
            field("points", field_types::INT32, false)
    };

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields");
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", fields, "points").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_mul_fields->add(json_line);
    }

    infile.close();

    query_fields = {"title", "starring"};
    std::vector<std::string> facets;

    auto x = coll_mul_fields->search("Will", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false);

    nlohmann::json results = coll_mul_fields->search("Will", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"3", "2", "1", "0"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // when "starring" takes higher priority than "title"

    query_fields = {"starring", "title"};
    results = coll_mul_fields->search("thomas", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"15", "12", "13", "14"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    query_fields = {"starring", "title", "cast"};
    results = coll_mul_fields->search("ben affleck", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    query_fields = {"cast"};
    results = coll_mul_fields->search("chris", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"6", "1", "7"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    query_fields = {"cast"};
    results = coll_mul_fields->search("chris pine", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"7", "6", "1"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // filtering on unfaceted multi-valued string field
    query_fields = {"title"};
    results = coll_mul_fields->search("captain", query_fields, "cast: chris", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ids = {"6"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // when a token exists in multiple fields of the same document, document should be returned only once
    query_fields = {"starring", "title", "cast"};
    results = coll_mul_fields->search("myers", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ids = {"17"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_mul_fields");
}

TEST_F(CollectionTest, FilterAndQueryFieldRestrictions) {
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
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", fields, "points").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_mul_fields->add(json_line);
    }

    infile.close();

    std::vector<std::string> facets;

    // query should be allowed only on non-faceted text fields
    query_fields = {"cast"};
    Option<nlohmann::json> result_op =
            coll_mul_fields->search("anton", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_FALSE(result_op.ok());
    ASSERT_EQ(400, result_op.code());
    ASSERT_EQ("Field `cast` is a faceted field - it cannot be used as a query field.", result_op.error());

    // filtering on string field should be possible
    query_fields = {"title"};
    result_op = coll_mul_fields->search("captain", query_fields, "starring: Samuel L. Jackson", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false);
    ASSERT_EQ(true, result_op.ok());
    nlohmann::json results = result_op.get();
    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll_mul_fields");
}

TEST_F(CollectionTest, FilterOnNumericFields) {
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
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, "age").get();
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
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps:<1", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, FilterOnFloatFields) {
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
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
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

TEST_F(CollectionTest, SortOnFloatFields) {
    Collection *coll_float_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/float_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("score", field_types::FLOAT, false),
            field("average", field_types::FLOAT, false)
    };

    std::vector<sort_by> sort_fields_desc = { sort_by("score", "DESC"), sort_by("average", "DESC") };

    coll_float_fields = collectionManager.get_collection("coll_float_fields");
    if(coll_float_fields == nullptr) {
        coll_float_fields = collectionManager.create_collection("coll_float_fields", fields, "score").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_float_fields->add(json_line);
    }

    infile.close();

    query_fields = {"title"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_float_fields->search("Jeremy", query_fields, "", facets, sort_fields_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(7, results["hits"].size());

    std::vector<std::string> ids = {"2", "0", "3", "1", "5", "4", "6"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        EXPECT_STREQ(id.c_str(), result_id.c_str());
    }

    std::vector<sort_by> sort_fields_asc = { sort_by("score", "ASC"), sort_by("average", "ASC") };
    results = coll_float_fields->search("Jeremy", query_fields, "", facets, sort_fields_asc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(7, results["hits"].size());

    ids = {"6", "4", "5", "1", "3", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        EXPECT_STREQ(id.c_str(), result_id.c_str());
    }

    // second field by desc

    std::vector<sort_by> sort_fields_asc_desc = { sort_by("score", "ASC"), sort_by("average", "DESC") };
    results = coll_float_fields->search("Jeremy", query_fields, "", facets, sort_fields_asc_desc, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(7, results["hits"].size());

    ids = {"5", "4", "6", "1", "3", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        EXPECT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_float_fields");
}

TEST_F(CollectionTest, QueryBoolFields) {
    Collection *coll_bool;

    std::ifstream infile(std::string(ROOT_DIR)+"test/bool_documents.jsonl");
    std::vector<field> fields = {
        field("popular", field_types::BOOL, false),
        field("title", field_types::STRING, false),
        field("rating", field_types::FLOAT, false),
        field("bool_array", field_types::BOOL_ARRAY, false),
    };

    std::vector<sort_by> sort_fields = { sort_by("popular", "DESC"), sort_by("rating", "DESC") };

    coll_bool = collectionManager.get_collection("coll_bool");
    if(coll_bool == nullptr) {
        coll_bool = collectionManager.create_collection("coll_bool", fields, "rating").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_bool->add(json_line);
    }

    infile.close();

    // Plain search with no filters - results should be sorted correctly
    query_fields = {"title"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_bool->search("the", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    std::vector<std::string> ids = {"1", "3", "4", "9", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Searching on a bool field
    results = coll_bool->search("the", query_fields, "popular:true", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"1", "3", "4"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_bool->search("the", query_fields, "popular:false", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"9", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // searching against a bool array field

    // should be able to search only with a single boolean value
    Option<nlohmann::json> res_op = coll_bool->search("the", query_fields, "bool_array:[true, false]", facets,
                                                      sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_FALSE(res_op.ok());

    results = coll_bool->search("the", query_fields, "bool_array: true", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());
    ids = {"1", "4", "9", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_bool");
}

TEST_F(CollectionTest, FilterOnTextFields) {
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
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "tags: gold", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

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

    // need not be exact matches (normalization can happen)
    results = coll_array_fields->search("Jeremy", query_fields, "tags: BrONZe", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    // when comparators are used, should just treat them as part of search string (special chars will be removed)
    results = coll_array_fields->search("Jeremy", query_fields, "tags:<bronze", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:<=BRONZE", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_array_fields->search("Jeremy", query_fields, "tags:>BRONZE", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, HandleBadlyFormedFilterQuery) {
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
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, "age").get();
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

TEST_F(CollectionTest, FacetCounts) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("name_facet", field_types::STRING, true),
                                 field("age", field_types::INT32, false),
                                 field("years", field_types::INT32_ARRAY, false),
                                 field("timestamps", field_types::INT64_ARRAY, false),
                                 field("tags", field_types::STRING_ARRAY, true)};

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, "age").get();
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

    // single facet with no filters
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(2, results["facet_counts"][0].size());
    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);

    ASSERT_EQ("gold", results["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(4, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_EQ("silver", results["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][1]["count"]);

    ASSERT_EQ("bronze", results["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][2]["count"]);

    // 2 facets, 1 text filter with no filters
    facets.clear();
    facets.push_back("tags");
    facets.push_back("name_facet");
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(2, results["facet_counts"].size());

    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ("name_facet", results["facet_counts"][1]["field_name"]);

    // facet value must one that's stored, not indexed (i.e. no tokenization/standardization)
    ASSERT_EQ("Jeremy Howard", results["facet_counts"][1]["counts"][0]["value"]);
    ASSERT_EQ(5, (int) results["facet_counts"][1]["counts"][0]["count"]);

    // facet with filters
    facets.clear();
    facets.push_back("tags");
    results = coll_array_fields->search("Jeremy", query_fields, "age: >24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());

    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][2]["count"]);

    ASSERT_EQ("gold", results["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ("silver", results["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ("bronze", results["facet_counts"][0]["counts"][2]["value"]);

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, SortingOrder) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("starring", field_types::STRING, false),
                                 field("points", field_types::INT32, false),
                                 field("cast", field_types::STRING_ARRAY, false)};

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields");
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", fields, "points").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_mul_fields->add(json_line);
    }

    infile.close();

    query_fields = {"title"};
    std::vector<std::string> facets;
    sort_fields = { sort_by("points", "ASC") };
    nlohmann::json results = coll_mul_fields->search("the", query_fields, "", facets, sort_fields, 0, 15, 1, FREQUENCY, false).get();
    ASSERT_EQ(10, results["hits"].size());

    std::vector<std::string> ids = {"17", "13", "10", "4", "0", "1", "8", "6", "16", "11"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // limiting results to just 5, "ASC" keyword must be case insensitive
    sort_fields = { sort_by("points", "asc") };
    results = coll_mul_fields->search("the", query_fields, "", facets, sort_fields, 0, 5, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"17", "13", "10", "4", "0"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // desc

    sort_fields = { sort_by("points", "dEsc") };
    results = coll_mul_fields->search("the", query_fields, "", facets, sort_fields, 0, 15, 1, FREQUENCY, false).get();
    ASSERT_EQ(10, results["hits"].size());

    ids = {"11", "16", "6", "8", "1", "0", "10", "4", "13", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // With empty list of sort_by fields:
    // should be ordered desc on the default sorting field, since the match score will be the same for all records.
    sort_fields = { };
    results = coll_mul_fields->search("of", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"11", "12", "5", "4", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_mul_fields");
}

TEST_F(CollectionTest, SearchingWithMissingFields) {
    // return error without crashing when searching for fields that do not conform to the schema
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("age", field_types::INT32, false),
                                 field("years", field_types::INT32_ARRAY, false),
                                 field("timestamps", field_types::INT64_ARRAY, false),
                                 field("tags", field_types::STRING_ARRAY, true)};

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_array_fields->add(json_line);
    }

    infile.close();

    // when a query field mentioned in schema does not exist
    std::vector<std::string> facets;
    std::vector<std::string> query_fields_not_found = {"titlez"};

    Option<nlohmann::json> res_op = coll_array_fields->search("the", query_fields_not_found, "", facets, sort_fields, 0, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(404, res_op.code());
    ASSERT_STREQ("Could not find a field named `titlez` in the schema.", res_op.error().c_str());

    // when a query field is an integer field
    res_op = coll_array_fields->search("the", {"age"}, "", facets, sort_fields, 0, 10);
    ASSERT_EQ(400, res_op.code());
    ASSERT_STREQ("Field `age` should be a string or a string array.", res_op.error().c_str());

    // when a facet field is not defined in the schema
    res_op = coll_array_fields->search("the", {"name"}, "", {"timestamps"}, sort_fields, 0, 10);
    ASSERT_EQ(404, res_op.code());
    ASSERT_STREQ("Could not find a facet field named `timestamps` in the schema.", res_op.error().c_str());

    // when a rank field is not defined in the schema
    res_op = coll_array_fields->search("the", {"name"}, "", {}, { sort_by("timestamps", "ASC") }, 0, 10);
    ASSERT_EQ(404, res_op.code());
    ASSERT_STREQ("Could not find a field named `timestamps` in the schema for sorting.", res_op.error().c_str());

    res_op = coll_array_fields->search("the", {"name"}, "", {}, { sort_by("_rank", "ASC") }, 0, 10);
    ASSERT_EQ(404, res_op.code());
    ASSERT_STREQ("Could not find a field named `_rank` in the schema for sorting.", res_op.error().c_str());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionTest, DefaultSortingFieldMustBeInt32OrFloat) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("age", field_types::INT32, false),
                                 field("average", field_types::INT32, false) };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC"), sort_by("average", "DESC") };

    Option<Collection*> collection_op = collectionManager.create_collection("sample_collection", fields, "name");
    EXPECT_FALSE(collection_op.ok());
    EXPECT_EQ("Default sorting field `name` must be of type int32 or float.", collection_op.error());
    collectionManager.drop_collection("sample_collection");
}

TEST_F(CollectionTest, IndexingWithBadData) {
    // should not crash when document to-be-indexed doesn't match schema
    Collection *sample_collection;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("age", field_types::INT32, false),
                                 field("average", field_types::INT32, false) };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC"), sort_by("average", "DESC") };

    sample_collection = collectionManager.get_collection("sample_collection");
    if(sample_collection == nullptr) {
        sample_collection = collectionManager.create_collection("sample_collection", fields, "age").get();
    }

    const Option<nlohmann::json> & search_fields_missing_op1 = sample_collection->add("{\"namezz\": \"foo\", \"age\": 29, \"average\": 78}");
    ASSERT_FALSE(search_fields_missing_op1.ok());
    ASSERT_STREQ("Field `tags` has been declared in the schema, but is not found in the document.",
                 search_fields_missing_op1.error().c_str());

    const Option<nlohmann::json> & search_fields_missing_op2 = sample_collection->add("{\"namez\": \"foo\", \"tags\": [], \"age\": 34, \"average\": 78}");
    ASSERT_FALSE(search_fields_missing_op2.ok());
    ASSERT_STREQ("Field `name` has been declared in the schema, but is not found in the document.",
                 search_fields_missing_op2.error().c_str());

    const Option<nlohmann::json> & facet_fields_missing_op1 = sample_collection->add("{\"name\": \"foo\", \"age\": 34, \"average\": 78}");
    ASSERT_FALSE(facet_fields_missing_op1.ok());
    ASSERT_STREQ("Field `tags` has been declared in the schema, but is not found in the document.",
                 facet_fields_missing_op1.error().c_str());

    const char *doc_str = "{\"name\": \"foo\", \"age\": 34, \"avg\": 78, \"tags\": [\"red\", \"blue\"]}";
    const Option<nlohmann::json> & sort_fields_missing_op1 = sample_collection->add(doc_str);
    ASSERT_FALSE(sort_fields_missing_op1.ok());
    ASSERT_STREQ("Field `average` has been declared in the schema, but is not found in the document.",
                 sort_fields_missing_op1.error().c_str());

    // Handle type errors

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": 22, \"average\": 78}";
    const Option<nlohmann::json> & bad_facet_field_op = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_facet_field_op.ok());
    ASSERT_STREQ("Field `tags` must be a string array.", bad_facet_field_op.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [], \"average\": 34}";
    const Option<nlohmann::json> & empty_facet_field_op = sample_collection->add(doc_str);
    ASSERT_TRUE(empty_facet_field_op.ok());

    doc_str = "{\"name\": \"foo\", \"age\": \"34\", \"tags\": [], \"average\": 34 }";
    const Option<nlohmann::json> & bad_default_sorting_field_op1 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_default_sorting_field_op1.ok());
    ASSERT_STREQ("Default sorting field `age` must be of type int32 or float.", bad_default_sorting_field_op1.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 343234324234233234, \"tags\": [], \"average\": 34 }";
    const Option<nlohmann::json> & bad_default_sorting_field_op2 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_default_sorting_field_op2.ok());
    ASSERT_STREQ("Default sorting field `age` exceeds maximum value of an int32.", bad_default_sorting_field_op2.error().c_str());

    doc_str = "{\"name\": \"foo\", \"tags\": [], \"average\": 34 }";
    const Option<nlohmann::json> & bad_default_sorting_field_op3 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_default_sorting_field_op3.ok());
    ASSERT_STREQ("Field `age` has been declared as a default sorting field, but is not found in the document.",
                 bad_default_sorting_field_op3.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [], \"average\": \"34\"}";
    const Option<nlohmann::json> & bad_rank_field_op = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_rank_field_op.ok());
    ASSERT_STREQ("Field `average` must be an int32.", bad_rank_field_op.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": asdadasd, \"tags\": [], \"average\": 34 }";
    const Option<nlohmann::json> & bad_default_sorting_field_op4 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_default_sorting_field_op4.ok());
    ASSERT_STREQ("Bad JSON.", bad_default_sorting_field_op4.error().c_str());

    // should return an error when a document with pre-existing id is being added
    std::string doc = "{\"id\": \"100\", \"name\": \"foo\", \"age\": 29, \"tags\": [], \"average\": 78}";
    Option<nlohmann::json> add_op = sample_collection->add(doc);
    ASSERT_TRUE(add_op.ok());
    add_op = sample_collection->add(doc);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ(409, add_op.code());
    ASSERT_STREQ("A document with id 100 already exists.", add_op.error().c_str());

    collectionManager.drop_collection("sample_collection");
}

TEST_F(CollectionTest, EmptyIndexShouldNotCrash) {
    Collection *empty_coll;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),
                                 field("age", field_types::INT32, false),
                                 field("average", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC"), sort_by("average", "DESC") };

    empty_coll = collectionManager.get_collection("empty_coll");
    if(empty_coll == nullptr) {
        empty_coll = collectionManager.create_collection("empty_coll", fields, "age").get();
    }

    nlohmann::json results = empty_coll->search("a", {"name"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());
    collectionManager.drop_collection("empty_coll");
}

TEST_F(CollectionTest, IdFieldShouldBeAString) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),
                                 field("age", field_types::INT32, false),
                                 field("average", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC"), sort_by("average", "DESC") };

    coll1 = collectionManager.get_collection("coll1");
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", fields, "age").get();
    }

    nlohmann::json doc;
    doc["id"] = 101010;
    doc["name"] = "Jane";
    doc["age"] = 25;
    doc["average"] = 98;
    doc["tags"] = nlohmann::json::array();
    doc["tags"].push_back("tag1");

    Option<nlohmann::json> inserted_id_op = coll1->add(doc.dump());
    ASSERT_FALSE(inserted_id_op.ok());
    ASSERT_STREQ("Document's `id` field should be a string.", inserted_id_op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, AnIntegerCanBePassedToAFloatField) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("average", field_types::FLOAT, false)};

    std::vector<sort_by> sort_fields = { sort_by("average", "DESC") };

    coll1 = collectionManager.get_collection("coll1");
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", fields, "average").get();
    }

    nlohmann::json doc;
    doc["id"] = "101010";
    doc["name"] = "Jane";
    doc["average"] = 98;

    Option<nlohmann::json> inserted_id_op = coll1->add(doc.dump());
    EXPECT_TRUE(inserted_id_op.ok());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, DeletionOfADocument) {
    collectionManager.drop_collection("collection");

    std::ifstream infile(std::string(ROOT_DIR)+"test/documents.jsonl");

    std::vector<field> search_fields = {field("title", field_types::STRING, false),
                                        field("points", field_types::INT32, false)};


    std::vector<std::string> query_fields = {"title"};
    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    Collection *collection_for_del;
    collection_for_del = collectionManager.get_collection("collection_for_del");
    if(collection_for_del == nullptr) {
        collection_for_del = collectionManager.create_collection("collection_for_del", search_fields, "points").get();
    }

    std::string json_line;
    rocksdb::Iterator* it;
    size_t num_keys = 0;

    // dummy record for record id 0: to make the test record IDs to match with line numbers
    json_line = "{\"points\":10,\"title\":\"z\"}";
    collection_for_del->add(json_line);

    while (std::getline(infile, json_line)) {
        collection_for_del->add(json_line);
    }

    ASSERT_EQ(25, collection_for_del->get_num_documents());

    infile.close();

    nlohmann::json results;

    // asserts before removing any record
    results = collection_for_del->search("cryogenic", query_fields, "", {}, sort_fields, 0, 5, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    it = store->get_iterator();
    num_keys = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        num_keys += 1;
    }
    ASSERT_EQ(25+25+3, num_keys);  // 25 records, 25 id mapping, 3 meta keys
    delete it;

    // actually remove a record now
    collection_for_del->remove("1");

    results = collection_for_del->search("cryogenic", query_fields, "", {}, sort_fields, 0, 5, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    results = collection_for_del->search("archives", query_fields, "", {}, sort_fields, 0, 5, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());

    collection_for_del->remove("foo");   // custom id record
    results = collection_for_del->search("martian", query_fields, "", {}, sort_fields, 0, 5, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    // delete all records
    for(int id = 0; id <= 25; id++) {
        collection_for_del->remove(std::to_string(id));
    }

    ASSERT_EQ(0, collection_for_del->get_num_documents());

    it = store->get_iterator();
    num_keys = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        num_keys += 1;
    }
    delete it;
    ASSERT_EQ(3, num_keys);

    collectionManager.drop_collection("collection_for_del");
}

nlohmann::json get_prune_doc() {
    nlohmann::json document;
    document["one"] = 1;
    document["two"] = 2;
    document["three"] = 3;
    document["four"] = 4;

    return document;
}

TEST_F(CollectionTest, SearchLargeTextField) {
    Collection *coll_large_text;

    std::vector<field> fields = {field("text", field_types::STRING, false),
                                 field("age", field_types::INT32, false),
    };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_large_text = collectionManager.get_collection("coll_large_text");
    if(coll_large_text == nullptr) {
        coll_large_text = collectionManager.create_collection("coll_large_text", fields, "age").get();
    }

    std::string json_line;
    std::ifstream infile(std::string(ROOT_DIR)+"test/large_text_field.jsonl");

    while (std::getline(infile, json_line)) {
        coll_large_text->add(json_line);
    }

    infile.close();

    Option<nlohmann::json> res_op = coll_large_text->search("eguilazer", {"text"}, "", {}, sort_fields, 0, 10);
    ASSERT_TRUE(res_op.ok());
    nlohmann::json results = res_op.get();
    ASSERT_EQ(1, results["hits"].size());

    res_op = coll_large_text->search("tristique", {"text"}, "", {}, sort_fields, 0, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();
    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll_large_text");
}

TEST_F(CollectionTest, PruneFieldsFromDocument) {
    nlohmann::json document = get_prune_doc();
    Collection::prune_document(document, {"one", "two"}, spp::sparse_hash_set<std::string>());
    ASSERT_EQ(2, document.size());
    ASSERT_EQ(1, document["one"]);
    ASSERT_EQ(2, document["two"]);

    // exclude takes precedence
    document = get_prune_doc();
    Collection::prune_document(document, {"one"}, {"one"});
    ASSERT_EQ(0, document.size());

    // when no inclusion is specified, should return all fields not mentioned by exclusion list
    document = get_prune_doc();
    Collection::prune_document(document, spp::sparse_hash_set<std::string>(), {"three"});
    ASSERT_EQ(3, document.size());
    ASSERT_EQ(1, document["one"]);
    ASSERT_EQ(2, document["two"]);
    ASSERT_EQ(4, document["four"]);

    document = get_prune_doc();
    Collection::prune_document(document, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>());
    ASSERT_EQ(4, document.size());

    // when included field does not exist
    document = get_prune_doc();
    Collection::prune_document(document, {"notfound"}, spp::sparse_hash_set<std::string>());
    ASSERT_EQ(0, document.size());

    // when excluded field does not exist
    document = get_prune_doc();
    Collection::prune_document(document, spp::sparse_hash_set<std::string>(), {"notfound"});
    ASSERT_EQ(4, document.size());
}