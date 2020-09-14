#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

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
        collectionManager.init(store, 1.0, "auth_key");
        collectionManager.load();

        std::ifstream infile(std::string(ROOT_DIR)+"test/documents.jsonl");
        std::vector<field> search_fields = {
            field("title", field_types::STRING, false),
            field("points", field_types::INT32, false)
        };

        query_fields = {"title"};
        sort_fields = { sort_by(sort_field_const::text_match, "DESC"), sort_by("points", "DESC") };

        collection = collectionManager.get_collection("collection");
        if(collection == nullptr) {
            collection = collectionManager.create_collection("collection", 4, search_fields, "points").get();
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
        collectionManager.dispose();
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

    ASSERT_STREQ("the", results["request_params"]["q"].get<std::string>().c_str());
    ASSERT_EQ(10, results["request_params"]["per_page"].get<size_t>());

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
    
    // when a query does not return results, hits and found fields should still exist in response
    results = collection->search("zxsadqewsad", query_fields, "", facets, sort_fields_asc, 0, 10).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<int>());
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
    std::vector<sort_by> sort_fields_asc = { sort_by(sort_field_const::text_match, "DESC"), sort_by("points", "ASC") };
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

    ASSERT_EQ(3, results["request_params"]["per_page"].get<size_t>());

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

    // wildcard query with no filters and ASC sort
    std::vector<sort_by> sort_fields = { sort_by("points", "ASC") };
    results = collection->search("*", query_fields, "", {}, sort_fields, 0, 3, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<uint32_t>());

    std::vector<std::string> ids = {"21", "24", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // wildcard query should not require a search field
    results_op = collection->search("*", {}, "", {}, sort_fields, 0, 3, 1, FREQUENCY, false);
    ASSERT_TRUE(results_op.ok());
    results = results_op.get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<uint32_t>());

    // non-wildcard query should require a search field
    results_op = collection->search("the", {}, "", {}, sort_fields, 0, 3, 1, FREQUENCY, false);
    ASSERT_FALSE(results_op.ok());
    ASSERT_STREQ("No search fields specified for the query.", results_op.error().c_str());
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

    ASSERT_EQ(2, results["hits"].size());
    ids = {"6", "12"};

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

TEST_F(CollectionTest, TypoTokensThreshold) {
    // Query expansion should happen only based on the `typo_tokens_threshold` value
    auto results = collection->search("launch", {"title"}, "", {}, sort_fields, 2, 10, 1,
                       token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                       spp::sparse_hash_set<std::string>(), 10, "", 5, "", 0).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(5, results["found"].get<size_t>());

    results = collection->search("launch", {"title"}, "", {}, sort_fields, 2, 10, 1,
                                token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 5, "", 10).get();

    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<size_t>());
}

TEST_F(CollectionTest, MultiOccurrenceString) {
    Collection *coll_multi_string;

    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("points", field_types::INT32, false)
    };

    coll_multi_string = collectionManager.get_collection("coll_multi_string");
    if (coll_multi_string == nullptr) {
        coll_multi_string = collectionManager.create_collection("coll_multi_string", 4, fields, "points").get();
    }

    nlohmann::json document;
    document["title"] = "The brown fox was the tallest of the lot and the quickest of the trot.";
    document["points"] = 100;

    coll_multi_string->add(document.dump());

    query_fields = {"title"};
    nlohmann::json results = coll_multi_string->search("the", query_fields, "", {}, sort_fields, 0, 10, 1,
                                                       FREQUENCY, false, 0).get();
    ASSERT_EQ(1, results["hits"].size());
    collectionManager.drop_collection("coll_multi_string");
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
        coll_array_text = collectionManager.create_collection("coll_array_text", 4, fields, "points").get();
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

    // when query tokens are not found in an array field they should be ignored
    results = coll_array_text->search("winds", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                      false, 0).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["hits"][0]["highlights"].size());

    collectionManager.drop_collection("coll_array_text");
}

TEST_F(CollectionTest, MultipleFields) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("starring", field_types::STRING, false),
            field("starring_facet", field_types::STRING, true),
            field("cast", field_types::STRING_ARRAY, false),
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

    // when a token exists in multiple fields of the same document, document and facet should be returned only once
    query_fields = {"starring", "title", "cast"};
    facets = {"starring_facet"};

    results = coll_mul_fields->search("myers", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ids = {"17"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("starring_facet", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    size_t facet_count = results["facet_counts"][0]["counts"][0]["count"];
    ASSERT_EQ(1, facet_count);

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

std::vector<nlohmann::json> import_res_to_json(const std::vector<std::string>& imported_results) {
    std::vector<nlohmann::json> out;

    for(const auto& imported_result: imported_results) {
        out.emplace_back(nlohmann::json::parse(imported_result));
    }

    return out;
}

TEST_F(CollectionTest, ImportDocuments) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::stringstream strstream;
    strstream << infile.rdbuf();
    infile.close();

    std::vector<std::string> import_records;
    StringUtils::split(strstream.str(), import_records, "\n");

    std::vector<field> fields = {
        field("title", field_types::STRING, false),
        field("starring", field_types::STRING, false),
        field("cast", field_types::STRING_ARRAY, false),
        field("points", field_types::INT32, false)
    };

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields");
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
    }

    // try importing records

    nlohmann::json import_response = coll_mul_fields->add_many(import_records);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(18, import_response["num_imported"].get<int>());

    // now try searching for records

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

    // verify that empty import is handled gracefully
    std::vector<std::string> empty_records;
    import_response = coll_mul_fields->add_many(empty_records);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(0, import_response["num_imported"].get<int>());

    // verify that only bad records are rejected, rest must be imported (records 2 and 4 are bad)
    std::vector<std::string> more_records = {"{\"id\": \"id1\", \"title\": \"Test1\", \"starring\": \"Rand Fish\", \"points\": 12, "
                                   "\"cast\": [\"Tom Skerritt\"] }",
                                "{\"title\": 123, \"starring\": \"Jazz Gosh\", \"points\": 23, "
                                   "\"cast\": [\"Tom Skerritt\"] }",
                               "{\"title\": \"Test3\", \"starring\": \"Brad Fin\", \"points\": 11, "
                                   "\"cast\": [\"Tom Skerritt\"] }",
                               "{\"title\": \"Test4\", \"points\": 55, "
                                   "\"cast\": [\"Tom Skerritt\"] }"};

    import_response = coll_mul_fields->add_many(more_records);
    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    std::vector<nlohmann::json> import_results = import_res_to_json(more_records);

    ASSERT_EQ(4, import_results.size());
    ASSERT_TRUE(import_results[0]["success"].get<bool>());
    ASSERT_FALSE(import_results[1]["success"].get<bool>());
    ASSERT_TRUE(import_results[2]["success"].get<bool>());
    ASSERT_FALSE(import_results[3]["success"].get<bool>());

    ASSERT_STREQ("Field `title` must be a string.", import_results[1]["error"].get<std::string>().c_str());
    ASSERT_STREQ("Field `starring` has been declared in the schema, but is not found in the document.",
                 import_results[3]["error"].get<std::string>().c_str());
    ASSERT_STREQ("{\"cast\":[\"Tom Skerritt\"],\"id\":\"19\",\"points\":23,\"starring\":\"Jazz Gosh\",\"title\":123}",
                 import_results[1]["document"].get<std::string>().c_str());

    // record with duplicate IDs

    more_records = {"{\"id\": \"id2\", \"title\": \"Test1\", \"starring\": \"Rand Fish\", \"points\": 12, "
                    "\"cast\": [\"Tom Skerritt\"] }",
                    "{\"id\": \"id1\", \"title\": \"Test1\", \"starring\": \"Rand Fish\", \"points\": 12, "
                    "\"cast\": [\"Tom Skerritt\"] }"};

    import_response = coll_mul_fields->add_many(more_records);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_EQ(2, import_results.size());
    ASSERT_TRUE(import_results[0]["success"].get<bool>());
    ASSERT_FALSE(import_results[1]["success"].get<bool>());

    ASSERT_STREQ("A document with id id1 already exists.", import_results[1]["error"].get<std::string>().c_str());
    ASSERT_STREQ("{\"cast\":[\"Tom Skerritt\"],\"id\":\"id1\",\"points\":12,\"starring\":\"Rand Fish\",\"title\":\"Test1\"}",
                 import_results[1]["document"].get<std::string>().c_str());

    // handle bad import json

    more_records = {"[]"};
    import_response = coll_mul_fields->add_many(more_records);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(0, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_EQ(1, import_results.size());

    ASSERT_EQ(false, import_results[0]["success"].get<bool>());
    ASSERT_STREQ("Bad JSON: not a properly formed document.", import_results[0]["error"].get<std::string>().c_str());
    ASSERT_STREQ("[]", import_results[0]["document"].get<std::string>().c_str());

    collectionManager.drop_collection("coll_mul_fields");
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
        coll_bool = collectionManager.create_collection("coll_bool", 4, fields, "rating").get();
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

    // alternative `:=` syntax
    results = coll_bool->search("the", query_fields, "popular:=true", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(3, results["hits"].size());

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

    // should be able to filter with an array of boolean values
    Option<nlohmann::json> res_op = coll_bool->search("the", query_fields, "bool_array:[true, false]", facets,
                                                      sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(5, results["hits"].size());

    results = coll_bool->search("the", query_fields, "bool_array: true", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());
    ids = {"1", "4", "9", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // should be able to search using array with a single element boolean value

    auto res = coll_bool->search("the", query_fields, "bool_array:[true]", facets,
                               sort_fields, 0, 10, 1, FREQUENCY, false).get();

    results = coll_bool->search("the", query_fields, "bool_array: true", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

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
        coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields, "age").get();
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
        sample_collection = collectionManager.create_collection("sample_collection", 4, fields, "age").get();
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
    ASSERT_STREQ("Default sorting field `age` must be a single valued numerical field.", bad_default_sorting_field_op1.error().c_str());

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
    ASSERT_STREQ("Bad JSON: [json.exception.parse_error.101] parse error at line 1, column 24: syntax error "
                 "while parsing value - invalid literal; last read: '\"age\": a'",
                bad_default_sorting_field_op4.error().c_str());

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
        empty_coll = collectionManager.create_collection("empty_coll", 4, fields, "age").get();
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
        coll1 = collectionManager.create_collection("coll1", 4, fields, "age").get();
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
        coll1 = collectionManager.create_collection("coll1", 4, fields, "average").get();
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
        collection_for_del = collectionManager.create_collection("collection_for_del", 4, search_fields, "points").get();
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
    ASSERT_EQ(0, results["found"]);

    results = collection_for_del->search("archives", query_fields, "", {}, sort_fields, 0, 5, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"]);

    collection_for_del->remove("foo");   // custom id record
    results = collection_for_del->search("martian", query_fields, "", {}, sort_fields, 0, 5, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"]);

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

TEST_F(CollectionTest, DeletionOfDocumentArrayFields) {
    Collection *coll1;

    std::vector<field> fields = {field("strarray", field_types::STRING_ARRAY, false),
                                 field("int32array", field_types::INT32_ARRAY, false),
                                 field("int64array", field_types::INT64_ARRAY, false),
                                 field("floatarray", field_types::FLOAT_ARRAY, false),
                                 field("boolarray", field_types::BOOL_ARRAY, false),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1");
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["strarray"] = {"Cell Phones", "Cell Phone Accessories", "Cell Phone Cases & Clips"};
    doc["int32array"] = {100, 200, 300};
    doc["int64array"] = {1582369739000, 1582369739000, 1582369739000};
    doc["floatarray"] = {19.99, 400.999};
    doc["boolarray"] = {true, false, true};
    doc["points"] = 25;

    Option<nlohmann::json> add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    nlohmann::json res = coll1->search("phone", {"strarray"}, "", {}, sort_fields, 0, 10, 1,
                                       token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(1, res["found"]);

    Option<std::string> rem_op = coll1->remove("100");

    ASSERT_TRUE(rem_op.ok());

    res = coll1->search("phone", {"strarray"}, "", {}, sort_fields, 0, 10, 1,
                        token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(0, res["found"].get<int32_t>());

    // also assert against the actual index
    Index *index = coll1->_get_indexes()[0];  // seq id will always be zero for first document
    auto search_index = index->_get_search_index();

    auto strarray_tree = search_index["strarray"];
    auto int32array_tree = search_index["int32array"];
    auto int64array_tree = search_index["int64array"];
    auto floatarray_tree = search_index["floatarray"];
    auto boolarray_tree = search_index["boolarray"];

    ASSERT_EQ(0, art_size(strarray_tree));
    ASSERT_EQ(0, art_size(int32array_tree));
    ASSERT_EQ(0, art_size(int64array_tree));
    ASSERT_EQ(0, art_size(floatarray_tree));
    ASSERT_EQ(0, art_size(boolarray_tree));

    collectionManager.drop_collection("coll1");
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

    std::vector<sort_by> sort_fields = { sort_by(sort_field_const::text_match, "DESC"), sort_by("age", "DESC") };

    coll_large_text = collectionManager.get_collection("coll_large_text");
    if(coll_large_text == nullptr) {
        coll_large_text = collectionManager.create_collection("coll_large_text", 4, fields, "age").get();
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

    // query whose length exceeds maximum highlight window (match score's WINDOW_SIZE)
    res_op = coll_large_text->search(
            "Phasellus non tristique elit Praesent non arcu id lectus accumsan venenatis at",
            {"text"}, "", {}, sort_fields, 0, 10
    );

    ASSERT_TRUE(res_op.ok());
    results = res_op.get();
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // only single matched token in match window

    res_op = coll_large_text->search("molestie maecenas accumsan", {"text"}, "", {}, sort_fields, 0, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("non arcu id lectus <mark>accumsan</mark> venenatis at at justo.",
    results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

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

TEST_F(CollectionTest, StringArrayFieldShouldNotAllowPlainString) {
    Collection *coll1;

    std::vector<field> fields = {field("categories", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1");
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["categories"] = "Should not be allowed!";
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_STREQ("Field `categories` must be a string array.", add_op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, SearchHighlightShouldFollowThreshold) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1");
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "The quick brown fox jumped over the lazy dog and ran straight to the forest to sleep.";
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // first with a large threshold

    auto res = coll1->search("lazy", {"title"}, "", {}, sort_fields, 0, 10, 1,
                  token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                  spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_STREQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    // now with with a small threshold (will show only 4 words either side of the matched token)

    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, 0, 10, 1,
                        token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5).get();

    ASSERT_STREQ("fox jumped over the <mark>lazy</mark> dog and ran straight",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, SearchHighlightFieldFully) {
    Collection *coll1;

    std::vector<field> fields = { field("title", field_types::STRING, true),
                                  field("tags", field_types::STRING_ARRAY, true),
                                  field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1");
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "The quick brown fox jumped over the lazy dog and ran straight to the forest to sleep.";
    doc["tags"] = {"NEWS", "LAZY"};
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // look for fully highlighted value in response

    auto res = coll1->search("lazy", {"title"}, "", {}, sort_fields, 0, 10, 1,
                        token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, "title").get();

    ASSERT_EQ(1, res["hits"][0]["highlights"].size());
    ASSERT_STREQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
                 res["hits"][0]["highlights"][0]["value"].get<std::string>().c_str());

    // should not return value key when highlight_full_fields is not specified
    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, 0, 10, 1,
                        token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, "").get();

    ASSERT_EQ(2, res["hits"][0]["highlights"][0].size());

    // query multiple fields
    res = coll1->search("lazy", {"title", "tags"}, "", {}, sort_fields, 0, 10, 1,
                        token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, "title, tags").get();

    ASSERT_EQ(2, res["hits"][0]["highlights"].size());
    ASSERT_STREQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
                 res["hits"][0]["highlights"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, res["hits"][0]["highlights"][1]["values"][0].size());
    ASSERT_STREQ("<mark>LAZY</mark>", res["hits"][0]["highlights"][1]["values"][0].get<std::string>().c_str());

    // excluded fields should not be returned in highlights section
    spp::sparse_hash_set<std::string> excluded_fields = {"tags"};
    res = coll1->search("lazy", {"title", "tags"}, "", {}, sort_fields, 0, 10, 1,
                        token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                        excluded_fields, 10, "", 5, "title, tags").get();

    ASSERT_EQ(1, res["hits"][0]["highlights"].size());
    ASSERT_STREQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
                 res["hits"][0]["highlights"][0]["value"].get<std::string>().c_str());

    // when all fields are excluded
    excluded_fields = {"tags", "title"};
    res = coll1->search("lazy", {"title", "tags"}, "", {}, sort_fields, 0, 10, 1,
                        token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                        excluded_fields, 10, "", 5, "title, tags").get();
    ASSERT_EQ(0, res["hits"][0]["highlights"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, OptionalFields) {
    Collection *coll1;

    std::vector<field> fields = {
        field("title", field_types::STRING, false),
        field("description", field_types::STRING, true, true),
        field("max", field_types::INT32, false),
        field("scores", field_types::INT64_ARRAY, false, true),
        field("average", field_types::FLOAT, false, true),
        field("is_valid", field_types::BOOL, false, true),
    };

    coll1 = collectionManager.get_collection("coll1");
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "max").get();
    }

    std::ifstream infile(std::string(ROOT_DIR)+"test/optional_fields.jsonl");

    std::string json_line;

    while (std::getline(infile, json_line)) {
        auto add_op = coll1->add(json_line);
        if(!add_op.ok()) {
            std::cout << add_op.error() << std::endl;
        }
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    // first must be able to fetch all records (i.e. all must have been indexed)

    auto res = coll1->search("*", {"title"}, "", {}, {}, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(6, res["found"].get<size_t>());

    // search on optional `description` field
    res = coll1->search("book", {"description"}, "", {}, {}, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, res["found"].get<size_t>());

    // filter on optional `average` field
    res = coll1->search("the", {"title"}, "average: >0", {}, {}, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, res["found"].get<size_t>());

    // facet on optional `description` field
    res = coll1->search("the", {"title"}, "", {"description"}, {}, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(6, res["found"].get<size_t>());
    ASSERT_EQ(5, res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_STREQ("description", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    // sort_by optional `average` field should be rejected
    std::vector<sort_by> sort_fields = { sort_by("average", "DESC") };
    auto res_op = coll1->search("*", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Cannot sort by `average` as it is defined as an optional field.", res_op.error().c_str());
    
    // try deleting a record having optional field
    Option<std::string> remove_op = coll1->remove("1");
    ASSERT_TRUE(remove_op.ok());

    // try fetching the schema (should contain optional field)
    nlohmann::json coll_summary = coll1->get_summary_json();
    ASSERT_STREQ("title", coll_summary["fields"][0]["name"].get<std::string>().c_str());
    ASSERT_STREQ("string", coll_summary["fields"][0]["type"].get<std::string>().c_str());
    ASSERT_FALSE(coll_summary["fields"][0]["facet"].get<bool>());
    ASSERT_FALSE(coll_summary["fields"][0]["optional"].get<bool>());

    ASSERT_STREQ("description", coll_summary["fields"][1]["name"].get<std::string>().c_str());
    ASSERT_STREQ("string", coll_summary["fields"][1]["type"].get<std::string>().c_str());
    ASSERT_TRUE(coll_summary["fields"][1]["facet"].get<bool>());
    ASSERT_TRUE(coll_summary["fields"][1]["optional"].get<bool>());

    // default sorting field should not be declared optional
    fields = {
        field("title", field_types::STRING, false),
        field("score", field_types::INT32, false, true),
    };

    auto create_op = collectionManager.create_collection("coll2", 4, fields, "score");

    ASSERT_FALSE(create_op.ok());
    ASSERT_STREQ("Default sorting field `score` cannot be an optional field.", create_op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, ReturnsResultsBasedOnPerPageParam) {
    std::vector<std::string> facets;
    spp::sparse_hash_set<std::string> empty;
    nlohmann::json results = collection->search("*", query_fields, "", facets, sort_fields, 0, 12, 1,
            FREQUENCY, false, 1000, empty, empty, 10).get();

    ASSERT_EQ(12, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    // should match collection size
    results = collection->search("*", query_fields, "", facets, sort_fields, 0, 100, 1,
                                 FREQUENCY, false, 1000, empty, empty, 10).get();

    ASSERT_EQ(25, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    // cannot fetch more than in-built limit of 250
    auto res_op = collection->search("*", query_fields, "", facets, sort_fields, 0, 251, 1,
                                 FREQUENCY, false, 1000, empty, empty, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(422, res_op.code());
    ASSERT_STREQ("Only upto 250 hits can be fetched per page.", res_op.error().c_str());

    // when page number is not valid
    res_op = collection->search("*", query_fields, "", facets, sort_fields, 0, 10, 0,
                                     FREQUENCY, false, 1000, empty, empty, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(422, res_op.code());
    ASSERT_STREQ("Page must be an integer of value greater than 0.", res_op.error().c_str());

    // do pagination

    results = collection->search("*", query_fields, "", facets, sort_fields, 0, 10, 1,
                                 FREQUENCY, false, 1000, empty, empty, 10).get();

    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    results = collection->search("*", query_fields, "", facets, sort_fields, 0, 10, 2,
                                 FREQUENCY, false, 1000, empty, empty, 10).get();

    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    results = collection->search("*", query_fields, "", facets, sort_fields, 0, 10, 3,
                                 FREQUENCY, false, 1000, empty, empty, 10).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());
}
