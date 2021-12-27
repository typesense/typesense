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
    std::atomic<bool> quit = false;
    std::vector<sort_by> sort_fields;

    // used for generating random text
    std::vector<std::string> words;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        std::ifstream infile(std::string(ROOT_DIR)+"test/documents.jsonl");
        std::vector<field> search_fields = {
            field("title", field_types::STRING, false),
            field("points", field_types::INT32, false)
        };

        query_fields = {"title"};
        sort_fields = { sort_by(sort_field_const::text_match, "DESC"), sort_by("points", "DESC") };

        collection = collectionManager.get_collection("collection").get();
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

        std::ifstream words_file(std::string(ROOT_DIR)+"test/resources/common100_english.txt");
        std::stringstream strstream;
        strstream << words_file.rdbuf();
        words_file.close();
        StringUtils::split(strstream.str(), words, "\n");
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.drop_collection("collection");
        collectionManager.dispose();
        delete store;
    }

    std::string get_text(size_t num_words) {
        time_t t;
        srand((unsigned) time(&t));
        std::vector<std::string> strs;

        for(size_t i = 0 ; i < num_words ; i++ ) {
            int word_index = rand() % words.size();
            strs.push_back(words[word_index]);
        }
        return StringUtils::join(strs, " ");
    }
};

TEST_F(CollectionTest, VerifyCountOfDocuments) {
    // we have 1 dummy record to match the line numbers on the fixtures file with sequence numbers
    ASSERT_EQ(24+1, collection->get_num_documents());

    // check default no specific dirty values option is sent for a collection that has explicit schema
    std::string empty_dirty_values;
    ASSERT_EQ(DIRTY_VALUES::REJECT, collection->parse_dirty_values_option(empty_dirty_values));
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
    nlohmann::json results = collection->search("the", query_fields, "", facets, sort_fields, {0}, 10).get();
    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<int>());

    ASSERT_STREQ("collection", results["request_params"]["collection_name"].get<std::string>().c_str());
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

    results = collection->search("the", query_fields, "", facets, sort_fields_asc, {0}, 10).get();
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
    results = collection->search("zxsadqewsad", query_fields, "", facets, sort_fields_asc, {0}, 10).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<int>());
}

TEST_F(CollectionTest, PhraseSearch) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("rocket launch", query_fields, "", facets, sort_fields, {0}, 10,
                                                1, FREQUENCY,
                                                {false}, 10,
                                                spp::sparse_hash_set<std::string>(),
                                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                                "", 10).get();
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
                 "What is the power, requirement of a <mark>rocket</mark> <mark>launch</mark> these days?");

    // Check ASC sort order
    std::vector<sort_by> sort_fields_asc = { sort_by(sort_field_const::text_match, "DESC"), sort_by("points", "ASC") };
    results = collection->search("rocket launch", query_fields, "", facets, sort_fields_asc, {0}, 10,
                                 1, FREQUENCY,
                                 {false}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();
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
    results = collection->search("rocket launch", query_fields, "", facets, sort_fields, {0}, 3,
                                 1, FREQUENCY,
                                 {false}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();
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

TEST_F(CollectionTest, SearchWithExcludedTokens) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("how -propellants -are", query_fields, "", facets, sort_fields, {0}, 10,
                                                1, FREQUENCY,
                                                {false}, 10,
                                                spp::sparse_hash_set<std::string>(),
                                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                                "", 10).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<uint32_t>());

    std::vector<std::string> ids = {"9", "17"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("-rocket", query_fields, "", facets, sort_fields, {0}, 50).get();

    ASSERT_EQ(21, results["found"].get<uint32_t>());
    ASSERT_EQ(21, results["hits"].size());

    results = collection->search("-rocket -cryovolcanism", query_fields, "", facets, sort_fields, {0}, 50).get();

    ASSERT_EQ(20, results["found"].get<uint32_t>());
}

TEST_F(CollectionTest, SkipUnindexedTokensDuringPhraseSearch) {
    // Tokens that are not found in the index should be skipped
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("DoesNotExist from", query_fields, "", facets, sort_fields, {0}, 10).get();
    ASSERT_EQ(2, results["hits"].size());

    std::vector<std::string> ids = {"2", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with non-zero cost
    results = collection->search("DoesNotExist from", query_fields, "", facets, sort_fields, {1}, 10).get();
    ASSERT_EQ(2, results["hits"].size());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // with 2 indexed words
    results = collection->search("from DoesNotExist insTruments", query_fields, "", facets, sort_fields, {1}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ids = {"2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // exhaustive search should throw more results
    results = collection->search("from DoesNotExist insTruments", query_fields, "", facets, sort_fields, {1}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000,
                                 true, false, true, "", true).get();
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
    results = collection->search("the a", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();
    ASSERT_EQ(7, results["hits"].size());

    results.clear();
    results = collection->search("the a", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(3, results["hits"].size());
    ids = {"8", "16", "10"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string id = ids.at(i);
        std::string result_id = result["document"]["id"];
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("the a insurance", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // with no indexed word
    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", query_fields, "", facets, sort_fields, {0}, 10).get();
    ASSERT_EQ(0, results["hits"].size());

    results.clear();
    results = collection->search("DoesNotExist1 DoesNotExist2", query_fields, "", facets, sort_fields, {2}, 10).get();
    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionTest, PartialPhraseSearch) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("rocket research", query_fields, "", facets,
                                                sort_fields, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"1", "8", "16", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, QueryWithTypo) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("kind biologcal", query_fields, "", facets, sort_fields, {2}, 3,
                                                1, FREQUENCY,
                                                {false}, 10,
                                                spp::sparse_hash_set<std::string>(),
                                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                                "", 10).get();
    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"19", "3", "20"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results.clear();
    results = collection->search("lauxnch rcket", query_fields, "", facets, sort_fields, {1}, 3,
                                 1, FREQUENCY,
                                 {false}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

    ids = {"8", "1", "17"};

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
    nlohmann::json results = collection->search("loox", query_fields, "", facets, sort_fields, {1}, 2, 1, MAX_SCORE, {false}).get();
    ASSERT_EQ(2, results["hits"].size());
    std::vector<std::string> ids = {"22", "3"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", query_fields, "", facets, sort_fields, {1}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());
    ids = {"22", "3", "12"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Check pagination
    results = collection->search("loox", query_fields, "", facets, sort_fields, {1}, 1, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["found"].get<int>());
    ASSERT_EQ(1, results["hits"].size());
    std::string solo_id = results["hits"].at(0)["document"]["id"];
    ASSERT_STREQ("22", solo_id.c_str());

    results = collection->search("loox", query_fields, "", facets, sort_fields, {1}, 2, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["found"].get<int>());
    ASSERT_EQ(2, results["hits"].size());

    // Check total ordering

    results = collection->search("loox", query_fields, "", facets, sort_fields, {1}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());
    ids = {"22", "3", "12", "23", "24"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("loox", query_fields, "", facets, sort_fields, {1}, 10, 1, MAX_SCORE, {false}).get();
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
    // A line contains "ISSX" but not "what" - need to ensure that correction to "ISSS what" happens
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("ISSX what", query_fields, "", facets, sort_fields, {1}, 4, 1, FREQUENCY, {false},
                                               10, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(),
                                               10, "", 30, 5, "", 10).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(11, results["found"].get<uint32_t>());

    std::vector<std::string> ids = {"19", "6", "21", "22"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // Record containing exact token match should appear first
    results = collection->search("ISSX", query_fields, "", facets, sort_fields, {1}, 10, 1, FREQUENCY, {false}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(5, results["found"].get<uint32_t>());

    ids = {"20", "19", "6", "3", "21"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionTest, Pagination) {
    nlohmann::json results = collection->search("the", query_fields, "", {}, sort_fields, {0}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<uint32_t>());

    std::vector<std::string> ids = {"1", "6", "foo"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("the", query_fields, "", {}, sort_fields, {0}, 3, 2, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<uint32_t>());

    ids = {"13", "10", "8"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("the", query_fields, "", {}, sort_fields, {0}, 3, 3, FREQUENCY, {false}).get();
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
    nlohmann::json results = collection->search("*", query_fields, "points:>0", {}, sort_fields, {0}, 3, 1, FREQUENCY,
                                                {false}).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<uint32_t>());

    // when no filter is specified, fall back on default sorting field based catch-all filter
    Option<nlohmann::json> results_op = collection->search("*", query_fields, "", {}, sort_fields, {0}, 3, 1, FREQUENCY,
                                                           {false});

    ASSERT_TRUE(results_op.ok());
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<uint32_t>());

    // wildcard query with no filters and ASC sort
    std::vector<sort_by> sort_fields = { sort_by("points", "ASC") };
    results = collection->search("*", query_fields, "", {}, sort_fields, {0}, 3, 1, FREQUENCY, {false}).get();
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
    results_op = collection->search("*", {}, "", {}, sort_fields, {0}, 3, 1, FREQUENCY, {false});
    ASSERT_TRUE(results_op.ok());
    results = results_op.get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<uint32_t>());

    // non-wildcard query should require a search field
    results_op = collection->search("the", {}, "", {}, sort_fields, {0}, 3, 1, FREQUENCY, {false});
    ASSERT_FALSE(results_op.ok());
    ASSERT_STREQ("No search fields specified for the query.", results_op.error().c_str());
}

TEST_F(CollectionTest, PrefixSearching) {
    std::vector<std::string> facets;
    nlohmann::json results = collection->search("ex", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(2, results["hits"].size());
    std::vector<std::string> ids = {"6", "12"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("ex", query_fields, "", facets, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(2, results["hits"].size());
    ids = {"6", "12"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("what ex", query_fields, "", facets, sort_fields, {0}, 10, 1, MAX_SCORE, {true}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();
    ASSERT_EQ(9, results["hits"].size());
    ids = {"6", "12", "19", "22", "13", "8", "15", "24", "21"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // restrict to only 2 results and differentiate between MAX_SCORE and FREQUENCY
    results = collection->search("t", query_fields, "", facets, sort_fields, {0}, 2, 1, MAX_SCORE, {true}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();
    ASSERT_EQ(2, results["hits"].size());
    ids = {"19", "22"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collection->search("t", query_fields, "", facets, sort_fields, {0}, 2, 1, FREQUENCY, {true}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();
    ASSERT_EQ(2, results["hits"].size());
    ids = {"1", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // only the last token in the query should be used for prefix search - so, "math" should not match "mathematics"
    results = collection->search("math fx", query_fields, "", facets, sort_fields, {0}, 1, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // single and double char prefixes should set a ceiling on the num_typos possible
    results = collection->search("x", query_fields, "", facets, sort_fields, {2}, 2, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(0, results["hits"].size());

    // prefix with a typo
    results = collection->search("late propx", query_fields, "", facets, sort_fields, {2}, 1, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("16", results["hits"].at(0)["document"]["id"]);
}

TEST_F(CollectionTest, TypoTokensThreshold) {
    // Query expansion should happen only based on the `typo_tokens_threshold` value
    auto results = collection->search("launch", {"title"}, "", {}, sort_fields, {2}, 10, 1,
                       token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                       spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "", 0).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(5, results["found"].get<size_t>());

    results = collection->search("launch", {"title"}, "", {}, sort_fields, {2}, 10, 1,
                                token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "", 10).get();

    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ(7, results["found"].get<size_t>());
}

TEST_F(CollectionTest, MultiOccurrenceString) {
    Collection *coll_multi_string;

    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("points", field_types::INT32, false)
    };

    coll_multi_string = collectionManager.get_collection("coll_multi_string").get();
    if (coll_multi_string == nullptr) {
        coll_multi_string = collectionManager.create_collection("coll_multi_string", 4, fields, "points").get();
    }

    nlohmann::json document;
    document["title"] = "The brown fox was the tallest of the lot and the quickest of the trot.";
    document["points"] = 100;

    coll_multi_string->add(document.dump()).get();

    query_fields = {"title"};
    nlohmann::json results = coll_multi_string->search("the", query_fields, "", {}, sort_fields, {0}, 10, 1,
                                                       FREQUENCY, {false}, 0).get();
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

    coll_array_text = collectionManager.get_collection("coll_array_text").get();
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

    nlohmann::json results = coll_array_text->search("truth about", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                                     {false}, 0).get();
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

    results = coll_array_text->search("forever truth", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                      {false}, 0).get();
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

    results = coll_array_text->search("truth", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                      {false}, 0).get();
    ASSERT_EQ(2, results["hits"].size());

    ids = {"1", "0"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_text->search("asdadasd", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                      {false}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    query_fields = {"title", "tags"};
    results = coll_array_text->search("truth", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                      {false}, 0).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["hits"][0]["highlights"].size());

    ids = {"1", "0"};

    for (size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    ASSERT_EQ(4, results["hits"][0]["highlights"][0].size());
    ASSERT_STREQ(results["hits"][0]["highlights"][0]["field"].get<std::string>().c_str(), "tags");
    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["snippets"].size());
    ASSERT_STREQ("<mark>truth</mark>", results["hits"][0]["highlights"][0]["snippets"][0].get<std::string>().c_str());
    ASSERT_STREQ("plain <mark>truth</mark>", results["hits"][0]["highlights"][0]["snippets"][1].get<std::string>().c_str());
    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["matched_tokens"].size());
    ASSERT_STREQ("truth", results["hits"][0]["highlights"][0]["matched_tokens"][0][0].get<std::string>().c_str());
    ASSERT_STREQ("truth", results["hits"][0]["highlights"][0]["matched_tokens"][1][0].get<std::string>().c_str());
    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["indices"].size());
    ASSERT_EQ(1, results["hits"][0]["highlights"][0]["indices"][0]);
    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["indices"][1]);

    ASSERT_EQ(3, results["hits"][0]["highlights"][1].size());
    ASSERT_STREQ("title", results["hits"][0]["highlights"][1]["field"].get<std::string>().c_str());
    ASSERT_STREQ("Plain <mark>Truth</mark>", results["hits"][0]["highlights"][1]["snippet"].get<std::string>().c_str());
    ASSERT_EQ(1, results["hits"][0]["highlights"][1]["matched_tokens"].size());
    ASSERT_STREQ("Truth", results["hits"][0]["highlights"][1]["matched_tokens"][0].get<std::string>().c_str());

    ASSERT_EQ(3, results["hits"][1]["highlights"][0].size());
    ASSERT_STREQ("title", results["hits"][1]["highlights"][0]["field"].get<std::string>().c_str());
    ASSERT_STREQ("The <mark>Truth</mark> About Forever", results["hits"][1]["highlights"][0]["snippet"].get<std::string>().c_str());
    ASSERT_EQ(1, results["hits"][1]["highlights"][0]["matched_tokens"].size());
    ASSERT_STREQ("Truth", results["hits"][1]["highlights"][0]["matched_tokens"][0].get<std::string>().c_str());

    ASSERT_EQ(4, results["hits"][1]["highlights"][1].size());
    ASSERT_STREQ(results["hits"][1]["highlights"][1]["field"].get<std::string>().c_str(), "tags");
    ASSERT_EQ(2, results["hits"][1]["highlights"][1]["snippets"].size());
    ASSERT_STREQ("the <mark>truth</mark>", results["hits"][1]["highlights"][1]["snippets"][0].get<std::string>().c_str());
    ASSERT_STREQ("<mark>truth</mark> about", results["hits"][1]["highlights"][1]["snippets"][1].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][1]["highlights"][1]["matched_tokens"].size());
    ASSERT_STREQ("truth", results["hits"][1]["highlights"][1]["matched_tokens"][0][0].get<std::string>().c_str());
    ASSERT_STREQ("truth", results["hits"][1]["highlights"][1]["matched_tokens"][1][0].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][1]["highlights"][1]["indices"].size());
    ASSERT_EQ(0, results["hits"][1]["highlights"][1]["indices"][0]);
    ASSERT_EQ(2, results["hits"][1]["highlights"][1]["indices"][1]);

    // highlight fields must be ordered based on match score
    results = coll_array_text->search("amazing movie", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                      {false}, 0).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(2, results["hits"][0]["highlights"].size());

    ASSERT_EQ(4, results["hits"][0]["highlights"][0].size());
    ASSERT_STREQ("tags", results["hits"][0]["highlights"][0]["field"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>amazing</mark> <mark>movie</mark>", results["hits"][0]["highlights"][0]["snippets"][0].get<std::string>().c_str());
    ASSERT_EQ(1, results["hits"][0]["highlights"][0]["indices"].size());
    ASSERT_EQ(0, results["hits"][0]["highlights"][0]["indices"][0]);
    ASSERT_EQ(1, results["hits"][0]["highlights"][0]["matched_tokens"].size());
    ASSERT_STREQ("amazing", results["hits"][0]["highlights"][0]["matched_tokens"][0][0].get<std::string>().c_str());

    ASSERT_EQ(3, results["hits"][0]["highlights"][1].size());
    ASSERT_STREQ(results["hits"][0]["highlights"][1]["field"].get<std::string>().c_str(), "title");
    ASSERT_STREQ(results["hits"][0]["highlights"][1]["snippet"].get<std::string>().c_str(),
                 "<mark>Amazing</mark> Spiderman is <mark>amazing</mark>"); // should highlight duplicating tokens

    ASSERT_EQ(2, results["hits"][0]["highlights"][1]["matched_tokens"].size());
    ASSERT_STREQ("Amazing", results["hits"][0]["highlights"][1]["matched_tokens"][0].get<std::string>().c_str());
    ASSERT_STREQ("amazing", results["hits"][0]["highlights"][1]["matched_tokens"][1].get<std::string>().c_str());

    // when query tokens are not found in an array field they should be ignored
    results = coll_array_text->search("winds", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                      {false}, 0).get();
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

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
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

    nlohmann::json results = coll_mul_fields->search("Will", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
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
    results = coll_mul_fields->search("thomas", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false},
                                      10, spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                      "<mark>", "</mark>", {2, 1}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"15", "12", "13", "14"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    query_fields = {"starring", "title", "cast"};
    results = coll_mul_fields->search("ben affleck", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    query_fields = {"cast"};
    results = coll_mul_fields->search("chris", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ids = {"6", "1", "7"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    query_fields = {"cast"};
    results = coll_mul_fields->search("chris pine", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    ids = {"7"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // filtering on unfaceted multi-valued string field
    query_fields = {"title"};
    results = coll_mul_fields->search("captain", query_fields, "cast: chris", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
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

    results = coll_mul_fields->search("myers", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
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

TEST_F(CollectionTest, KeywordQueryReturnsResultsBasedOnPerPageParam) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("starring", field_types::STRING, false),
            field("starring_facet", field_types::STRING, true),
            field("cast", field_types::STRING_ARRAY, false),
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

    query_fields = {"title", "starring"};
    std::vector<std::string> facets;

    spp::sparse_hash_set<std::string> empty;
    nlohmann::json results = coll_mul_fields->search("w", query_fields, "", facets, sort_fields, {0}, 3, 1,
                                                FREQUENCY, {true}, 1000, empty, empty, 10).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(6, results["found"].get<int>());

    // cannot fetch more than in-built limit of 250
    auto res_op = coll_mul_fields->search("w", query_fields, "", facets, sort_fields, {0}, 251, 1,
                                     FREQUENCY, {true}, 1000, empty, empty, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(422, res_op.code());
    ASSERT_STREQ("Only upto 250 hits can be fetched per page.", res_op.error().c_str());

    // when page number is not valid
    res_op = coll_mul_fields->search("w", query_fields, "", facets, sort_fields, {0}, 10, 0,
                                FREQUENCY, {true}, 1000, empty, empty, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(422, res_op.code());
    ASSERT_STREQ("Page must be an integer of value greater than 0.", res_op.error().c_str());

    // do pagination

    results = coll_mul_fields->search("w", query_fields, "", facets, sort_fields, {0}, 3, 1,
                                 FREQUENCY, {true}, 1000, empty, empty, 10).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(6, results["found"].get<int>());

    results = coll_mul_fields->search("w", query_fields, "", facets, sort_fields, {0}, 3, 2,
                                 FREQUENCY, {true}, 1000, empty, empty, 10).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(6, results["found"].get<int>());

    collectionManager.drop_collection("coll_mul_fields");
}

std::vector<nlohmann::json> import_res_to_json(const std::vector<std::string>& imported_results) {
    std::vector<nlohmann::json> out;

    for(const auto& imported_result: imported_results) {
        out.emplace_back(nlohmann::json::parse(imported_result));
    }

    return out;
}

TEST_F(CollectionTest, ImportDocumentsUpsert) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::stringstream strstream;
    strstream << infile.rdbuf();
    infile.close();

    std::vector<std::string> import_records;
    StringUtils::split(strstream.str(), import_records, "\n");

    std::vector<field> fields = {
        field("title", field_types::STRING, false),
        field("starring", field_types::STRING, true),
        field("cast", field_types::STRING_ARRAY, false),
        field("points", field_types::INT32, false)
    };

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 1, fields, "points").get();
    }

    // try importing records
    nlohmann::json document;
    nlohmann::json import_response = coll_mul_fields->add_many(import_records, document);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(18, import_response["num_imported"].get<int>());

    // try searching with filter
    auto results = coll_mul_fields->search("*", query_fields, "starring:= [Will Ferrell]", {"starring"}, sort_fields, {0}, 30, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    // update existing record verbatim
    std::vector<std::string> existing_records = {R"({"id": "0", "title": "Wake Up, Ron Burgundy: The Lost Movie"})"};
    import_response = coll_mul_fields->add_many(existing_records, document, UPDATE);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    // update + upsert records
    std::vector<std::string> more_records = {R"({"id": "0", "title": "The Fifth Harry", "starring": "Will Ferrell", "points":62, "cast":["Adam McKay","Steve Carell","Paul Rudd"]})",
                                            R"({"id": "2", "cast": ["Chris Fisher", "Rand Alan"], "points":81, "starring":"Daniel Day-Lewis","title":"There Will Be Blood"})",
                                            R"({"id": "18", "title": "Back Again Forest", "points": 45, "starring": "Ronald Wells", "cast": ["Dant Saren"]})",
                                            R"({"id": "6", "points": 77, "cast":["Chris Evans","Scarlett Johansson"], "starring":"Samuel L. Jackson","title":"Captain America: The Winter Soldier"})"};

    import_response = coll_mul_fields->add_many(more_records, document, UPSERT);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(4, import_response["num_imported"].get<int>());

    std::vector<nlohmann::json> import_results = import_res_to_json(more_records);
    ASSERT_EQ(4, import_results.size());

    for(size_t i=0; i<4; i++) {
        ASSERT_TRUE(import_results[i]["success"].get<bool>());
        ASSERT_EQ(1, import_results[i].size());
    }

    // try with filters again
    results = coll_mul_fields->search("*", query_fields, "starring:= [Will Ferrell]", {"starring"}, sort_fields, {0}, 30, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    results = coll_mul_fields->search("*", query_fields, "", {"starring"}, sort_fields, {0}, 30, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(19, results["hits"].size());
    ASSERT_EQ(19, coll_mul_fields->get_num_documents());

    results = coll_mul_fields->search("back again forest", query_fields, "", {"starring"}, sort_fields, {0}, 30, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("Back Again Forest", coll_mul_fields->get("18").get()["title"].get<std::string>().c_str());

    results = coll_mul_fields->search("fifth", query_fields, "", {"starring"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("The <mark>Fifth</mark> Harry", results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());
    ASSERT_STREQ("The Woman in the <mark>Fifth</mark> from Kristin", results["hits"][1]["highlights"][0]["snippet"].get<std::string>().c_str());

    results = coll_mul_fields->search("burgundy", query_fields, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll_mul_fields->search("harry", query_fields, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    results = coll_mul_fields->search("captain america", query_fields, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(77, results["hits"][0]["document"]["points"].get<size_t>());

    // upserting with some bad docs
    more_records = {R"({"id": "1", "title": "Wake up, Harry", "cast":["Josh Lawson","Chris Parnell"],"points":63,"starring":"Will Ferrell"})",
                    R"({"id": "90", "cast": ["Kim Werrel", "Random Wake"]})",                     // missing fields
                    R"({"id": "5", "points": 60, "cast":["Logan Lerman","Alexandra Daddario"],"starring":"Ron Perlman","starring_facet":"Ron Perlman","title":"Percy Jackson: Sea of Monsters"})",
                    R"({"id": "24", "starring": "John", "cast": ["John Kim"], "points": 11})"};   // missing fields

    import_response = coll_mul_fields->add_many(more_records, document, UPSERT);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_FALSE(import_results[1]["success"].get<bool>());
    ASSERT_FALSE(import_results[3]["success"].get<bool>());
    ASSERT_STREQ("Field `points` has been declared as a default sorting field, but is not found in the document.", import_results[1]["error"].get<std::string>().c_str());
    ASSERT_STREQ("Field `title` has been declared in the schema, but is not found in the document.", import_results[3]["error"].get<std::string>().c_str());

    // try to duplicate records without upsert option

    more_records = {R"({"id": "1", "title": "Wake up, Harry"})",
                    R"({"id": "5", "points": 60})"};

    import_response = coll_mul_fields->add_many(more_records, document, CREATE);
    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(0, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_FALSE(import_results[0]["success"].get<bool>());
    ASSERT_FALSE(import_results[1]["success"].get<bool>());
    ASSERT_STREQ("A document with id 1 already exists.", import_results[0]["error"].get<std::string>().c_str());
    ASSERT_STREQ("A document with id 5 already exists.", import_results[1]["error"].get<std::string>().c_str());

    // update document with verbatim fields, except for points
    more_records = {R"({"id": "3", "cast":["Matt Damon","Ben Affleck","Minnie Driver"],
                        "points":70,"starring":"Robin Williams","starring_facet":"Robin Williams",
                        "title":"Good Will Hunting"})"};

    import_response = coll_mul_fields->add_many(more_records, document, UPDATE);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    results = coll_mul_fields->search("Good Will Hunting", query_fields, "", {"starring"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(70, results["hits"][0]["document"]["points"].get<uint32_t>());

    // updating a document that does not exist should fail, others should succeed
    more_records = {R"({"id": "20", "points": 51})",
                    R"({"id": "1", "points": 64})"};

    import_response = coll_mul_fields->add_many(more_records, document, UPDATE);
    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_FALSE(import_results[0]["success"].get<bool>());
    ASSERT_TRUE(import_results[1]["success"].get<bool>());
    ASSERT_STREQ("Could not find a document with id: 20", import_results[0]["error"].get<std::string>().c_str());
    ASSERT_EQ(404, import_results[0]["code"].get<size_t>());

    results = coll_mul_fields->search("wake up harry", query_fields, "", {"starring"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(64, results["hits"][0]["document"]["points"].get<uint32_t>());

    // trying to create documents with existing IDs should fail
    more_records = {R"({"id": "2", "points": 51})",
                    R"({"id": "1", "points": 64})"};

    import_response = coll_mul_fields->add_many(more_records, document, CREATE);
    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(0, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_FALSE(import_results[0]["success"].get<bool>());
    ASSERT_FALSE(import_results[1]["success"].get<bool>());
    ASSERT_STREQ("A document with id 2 already exists.", import_results[0]["error"].get<std::string>().c_str());
    ASSERT_STREQ("A document with id 1 already exists.", import_results[1]["error"].get<std::string>().c_str());

    ASSERT_EQ(409, import_results[0]["code"].get<size_t>());
    ASSERT_EQ(409, import_results[1]["code"].get<size_t>());
}

TEST_F(CollectionTest, ImportDocumentsEmplace) {
    Collection* coll1;
    std::vector<field> fields = {
            field("title", field_types::STRING, false, false),
            field("points", field_types::INT32, false, true)
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields).get();
    }

    nlohmann::json document;
    std::vector<std::string> records = {R"({"id": "0", "title": "The Matrix", "points":0})",
                                        R"({"id": "1", "title": "Inception", "points":1})"};

    // use `emplace` mode for creating documents
    auto import_response = coll1->add_many(records, document, EMPLACE);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    std::vector<nlohmann::json> import_results = import_res_to_json(records);
    ASSERT_EQ(2, import_results.size());

    for (size_t i = 0; i < 2; i++) {
        ASSERT_TRUE(import_results[i]["success"].get<bool>());
        ASSERT_EQ(1, import_results[i].size());
    }

    auto res = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, token_ordering::FREQUENCY, {true}, 10).get();
    ASSERT_EQ(2, res["found"].get<size_t>());

    // emplace both update + create
    records = {R"({"id": "1", "title": "The Inception"})",
               R"({"id": "2", "title": "Spiderman", "points":2})"};

    import_response = coll1->add_many(records, document, EMPLACE);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(records);
    ASSERT_EQ(2, import_results.size());

    for (size_t i = 0; i < 2; i++) {
        ASSERT_TRUE(import_results[i]["success"].get<bool>());
        ASSERT_EQ(1, import_results[i].size());
    }

    res = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, token_ordering::FREQUENCY, {true}, 10).get();
    ASSERT_EQ(3, res["found"].get<size_t>());

    ASSERT_EQ("2", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(2, res["hits"][0]["document"]["points"].get<size_t>());

    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ(1, res["hits"][1]["document"]["points"].get<size_t>());
    ASSERT_EQ("The Inception", res["hits"][1]["document"]["title"].get<std::string>());

    ASSERT_EQ("0", res["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ(0, res["hits"][2]["document"]["points"].get<size_t>());

    // emplace with an error due to bad data
    records = {R"({"id": "2", "points": "abcd"})",
               R"({"id": "3", "title": "Superman", "points":3})"};

    import_response = coll1->add_many(records, document, EMPLACE);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(records);

    ASSERT_EQ(2, import_results.size());

    ASSERT_FALSE(import_results[0]["success"].get<bool>());

    ASSERT_TRUE(import_results[1]["success"].get<bool>());
    ASSERT_EQ(1, import_results[1].size());
    ASSERT_EQ(1, import_results[1].size());

    // can update individual document via "emplace"
    std::string doc_3_update = R"({"id": "3", "title": "The Superman"})";
    auto add_op = coll1->add(doc_3_update, EMPLACE);
    ASSERT_TRUE(add_op.ok());

    res = coll1->search("superman", {"title"}, "", {}, {}, {0}, 10, 1, token_ordering::FREQUENCY, {true}, 10).get();
    ASSERT_EQ(1, res["found"].get<size_t>());

    ASSERT_EQ("3", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(3, res["hits"][0]["document"]["points"].get<size_t>());
    ASSERT_EQ("The Superman", res["hits"][0]["document"]["title"].get<std::string>());

    // can create individual document via "emplace"
    std::string doc_4_create = R"({"id": "4", "title": "The Avengers", "points": 4})";
    add_op = coll1->add(doc_4_create, EMPLACE);
    ASSERT_TRUE(add_op.ok());

    res = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, token_ordering::FREQUENCY, {true}, 10).get();
    ASSERT_EQ(5, res["found"].get<size_t>());
}

TEST_F(CollectionTest, DISABLED_CrashTroubleshooting) {
    Collection *coll1;
    std::vector<field> fields = {
            field("title", field_types::STRING_ARRAY, false, true),
            field("points", field_types::INT32, false)
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    std::ifstream create_file("/tmp/create.jsonl");
    std::string json_line;
    std::vector<std::string> create_records;

    while (std::getline(create_file, json_line)) {
        create_records.push_back(json_line);
    }

    create_file.close();

    nlohmann::json document;
    auto import_response = coll1->add_many(create_records, document, CREATE);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1000, import_response["num_imported"].get<int>());

    // now try to upsert

    std::ifstream upsert_file("/tmp/upsert.jsonl");
    std::vector<std::string> upsert_records;

    while (std::getline(upsert_file, json_line)) {
        upsert_records.push_back(json_line);
    }

    upsert_file.close();

    import_response = coll1->add_many(upsert_records, document, UPSERT);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1000, import_response["num_imported"].get<int>());
}

TEST_F(CollectionTest, ImportDocumentsUpsertOptional) {
    Collection *coll1;
    std::vector<field> fields = {
            field("title", field_types::STRING_ARRAY, false, true),
            field("points", field_types::INT32, false)
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    std::vector<std::string> records;

    size_t NUM_RECORDS = 1000;

    for(size_t i=0; i<NUM_RECORDS; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["points"] = i;
        records.push_back(doc.dump());
    }

    // import records without title

    nlohmann::json document;
    nlohmann::json import_response = coll1->add_many(records, document, CREATE);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1000, import_response["num_imported"].get<int>());

    // upsert documents with title

    records.clear();

    for(size_t i=0; i<NUM_RECORDS; i++) {
        nlohmann::json updoc;
        updoc["id"] = std::to_string(i);
        updoc["points"] = i;
        updoc["title"] = {
            get_text(10),
            get_text(10),
            get_text(10),
            get_text(10),
        };
        records.push_back(updoc.dump());
    }

    auto begin = std::chrono::high_resolution_clock::now();
    import_response = coll1->add_many(records, document, UPSERT);
    auto time_micros = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - begin).count();
    
    //LOG(INFO) << "Time taken for first upsert: " << time_micros;
    
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1000, import_response["num_imported"].get<int>());

    // run upsert again with title override

    records.clear();

    for(size_t i=0; i<NUM_RECORDS; i++) {
        nlohmann::json updoc;
        updoc["id"] = std::to_string(i);
        updoc["points"] = i;
        updoc["title"] = {
            get_text(10),
            get_text(10),
            get_text(10),
            get_text(10),
        };
        records.push_back(updoc.dump());
    }

    begin = std::chrono::high_resolution_clock::now();
    import_response = coll1->add_many(records, document, UPSERT);
    time_micros = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - begin).count();

    //LOG(INFO) << "Time taken for second upsert: " << time_micros;

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1000, import_response["num_imported"].get<int>());

    // update records (can contain partial fields)

    records.clear();

    for(size_t i=0; i<NUM_RECORDS; i++) {
        nlohmann::json updoc;
        updoc["id"] = std::to_string(i);
        // no points field
        updoc["title"] = {
            get_text(10),
            get_text(10),
            get_text(10),
            get_text(10),
        };
        records.push_back(updoc.dump());
    }

    import_response = coll1->add_many(records, document, UPDATE);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1000, import_response["num_imported"].get<int>());
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

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
    }

    // try importing records
    nlohmann::json document;
    nlohmann::json import_response = coll_mul_fields->add_many(import_records, document);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(18, import_response["num_imported"].get<int>());

    // now try searching for records

    query_fields = {"title", "starring"};
    std::vector<std::string> facets;

    auto x = coll_mul_fields->search("Will", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false});

    nlohmann::json results = coll_mul_fields->search("Will", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
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
    import_response = coll_mul_fields->add_many(empty_records, document);
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

    import_response = coll_mul_fields->add_many(more_records, document, CREATE, "", DIRTY_VALUES::REJECT);
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
    ASSERT_STREQ("{\"title\": 123, \"starring\": \"Jazz Gosh\", \"points\": 23, \"cast\": [\"Tom Skerritt\"] }",
                 import_results[1]["document"].get<std::string>().c_str());

    // record with duplicate IDs

    more_records = {"{\"id\": \"id2\", \"title\": \"Test1\", \"starring\": \"Rand Fish\", \"points\": 12, "
                    "\"cast\": [\"Tom Skerritt\"] }",
                    "{\"id\": \"id1\", \"title\": \"Test1\", \"starring\": \"Rand Fish\", \"points\": 12, "
                    "\"cast\": [\"Tom Skerritt\"] }"};

    import_response = coll_mul_fields->add_many(more_records, document);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_EQ(2, import_results.size());
    ASSERT_TRUE(import_results[0]["success"].get<bool>());
    ASSERT_FALSE(import_results[1]["success"].get<bool>());

    ASSERT_STREQ("A document with id id1 already exists.", import_results[1]["error"].get<std::string>().c_str());
    ASSERT_STREQ("{\"id\": \"id1\", \"title\": \"Test1\", \"starring\": \"Rand Fish\", \"points\": 12, "
                 "\"cast\": [\"Tom Skerritt\"] }",import_results[1]["document"].get<std::string>().c_str());

    // handle bad import json

    // valid JSON but not a document
    more_records = {"[]"};
    import_response = coll_mul_fields->add_many(more_records, document);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(0, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_EQ(1, import_results.size());

    ASSERT_EQ(false, import_results[0]["success"].get<bool>());
    ASSERT_STREQ("Bad JSON: not a properly formed document.", import_results[0]["error"].get<std::string>().c_str());
    ASSERT_STREQ("[]", import_results[0]["document"].get<std::string>().c_str());

    // invalid JSON
    more_records = {"{"};
    import_response = coll_mul_fields->add_many(more_records, document);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(0, import_response["num_imported"].get<int>());

    import_results = import_res_to_json(more_records);
    ASSERT_EQ(1, import_results.size());

    ASSERT_EQ(false, import_results[0]["success"].get<bool>());
    ASSERT_STREQ("Bad JSON: [json.exception.parse_error.101] parse error at line 1, column 2: syntax error "
                 "while parsing object key - unexpected end of input; expected string literal",
                 import_results[0]["error"].get<std::string>().c_str());
    ASSERT_STREQ("{", import_results[0]["document"].get<std::string>().c_str());

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

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
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

    Option<nlohmann::json> res_op = coll_array_fields->search("the", query_fields_not_found, "", facets, sort_fields, {0}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(404, res_op.code());
    ASSERT_STREQ("Could not find a field named `titlez` in the schema.", res_op.error().c_str());

    // when a query field is an integer field
    res_op = coll_array_fields->search("the", {"age"}, "", facets, sort_fields, {0}, 10);
    ASSERT_EQ(400, res_op.code());
    ASSERT_STREQ("Field `age` should be a string or a string array.", res_op.error().c_str());

    // when a facet field is not defined in the schema
    res_op = coll_array_fields->search("the", {"name"}, "", {"timestamps"}, sort_fields, {0}, 10);
    ASSERT_EQ(404, res_op.code());
    ASSERT_STREQ("Could not find a facet field named `timestamps` in the schema.", res_op.error().c_str());

    // when a rank field is not defined in the schema
    res_op = coll_array_fields->search("the", {"name"}, "", {}, { sort_by("timestamps", "ASC") }, {0}, 10);
    ASSERT_EQ(404, res_op.code());
    ASSERT_STREQ("Could not find a field named `timestamps` in the schema for sorting.", res_op.error().c_str());

    res_op = coll_array_fields->search("the", {"name"}, "", {}, { sort_by("_rank", "ASC") }, {0}, 10);
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

    sample_collection = collectionManager.get_collection("sample_collection").get();
    if(sample_collection == nullptr) {
        sample_collection = collectionManager.create_collection("sample_collection", 4, fields, "age").get();
    }

    const Option<nlohmann::json> & search_fields_missing_op1 = sample_collection->add("{\"name\": \"foo\", \"age\": 29, \"average\": 78}");
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
    ASSERT_STREQ("Field `tags` must be an array.", bad_facet_field_op.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [\"red\", 22], \"average\": 78}";
    const Option<nlohmann::json> & bad_array_field_op = sample_collection->add(doc_str, CREATE, "",
                                                                               DIRTY_VALUES::REJECT);
    ASSERT_FALSE(bad_array_field_op.ok());
    ASSERT_STREQ("Field `tags` must be an array of string.", bad_array_field_op.error().c_str());

    // with coercion should work
    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [\"red\", 22], \"average\": 78}";
    const Option<nlohmann::json> &bad_array_field_coercion_op = sample_collection->add(doc_str, CREATE, "",
                                                                                       DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(bad_array_field_coercion_op.ok());

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [], \"average\": 34}";
    const Option<nlohmann::json> & empty_facet_field_op = sample_collection->add(doc_str);
    ASSERT_TRUE(empty_facet_field_op.ok());

    doc_str = "{\"name\": \"foo\", \"age\": [\"34\"], \"tags\": [], \"average\": 34 }";
    const Option<nlohmann::json> & bad_default_sorting_field_op1 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_default_sorting_field_op1.ok());
    ASSERT_STREQ("Field `age` must be an int32.", bad_default_sorting_field_op1.error().c_str());

    doc_str = "{\"name\": \"foo\", \"tags\": [], \"average\": 34 }";
    const Option<nlohmann::json> & bad_default_sorting_field_op3 = sample_collection->add(doc_str);
    ASSERT_FALSE(bad_default_sorting_field_op3.ok());
    ASSERT_STREQ("Field `age` has been declared as a default sorting field, but is not found in the document.",
                 bad_default_sorting_field_op3.error().c_str());

    doc_str = "{\"name\": \"foo\", \"age\": 34, \"tags\": [], \"average\": \"34\"}";
    const Option<nlohmann::json> & bad_rank_field_op = sample_collection->add(doc_str, CREATE, "", DIRTY_VALUES::REJECT);
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

    empty_coll = collectionManager.get_collection("empty_coll").get();
    if(empty_coll == nullptr) {
        empty_coll = collectionManager.create_collection("empty_coll", 4, fields, "age").get();
    }

    nlohmann::json results = empty_coll->search("a", {"name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
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

    coll1 = collectionManager.get_collection("coll1").get();
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

    coll1 = collectionManager.get_collection("coll1").get();
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
    collection_for_del = collectionManager.get_collection("collection_for_del").get();
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
    results = collection_for_del->search("cryogenic", query_fields, "", {}, sort_fields, {0}, 5, 1, FREQUENCY, {false}).get();
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

    results = collection_for_del->search("cryogenic", query_fields, "", {}, sort_fields, {0}, 5, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"]);

    results = collection_for_del->search("archives", query_fields, "", {}, sort_fields, {0}, 5, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["found"]);

    collection_for_del->remove("foo");   // custom id record
    results = collection_for_del->search("martian", query_fields, "", {}, sort_fields, {0}, 5, 1, FREQUENCY, {false}).get();
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

TEST_F(CollectionTest, DeletionOfDocumentSingularFields) {
    Collection *coll1;

    std::vector<field> fields = {field("str", field_types::STRING, false),
                                 field("int32", field_types::INT32, false),
                                 field("int64", field_types::INT64, false),
                                 field("float", field_types::FLOAT, false),
                                 field("bool", field_types::BOOL, false)};

    std::vector<sort_by> sort_fields = { sort_by("int32", "DESC") };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "int32").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["str"] = "[NEW] Cell Phone Cases, Holders & Clips!";
    doc["int32"] = 100032;
    doc["int64"] = 1582369739000;
    doc["float"] = -293.24;
    doc["bool"] = true;

    Option<nlohmann::json> add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    nlohmann::json res = coll1->search("phone", {"str"}, "", {}, sort_fields, {0}, 10, 1,
                                       token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(1, res["found"]);

    Option<std::string> rem_op = coll1->remove("100");

    ASSERT_TRUE(rem_op.ok());

    res = coll1->search("phone", {"str"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(0, res["found"].get<int32_t>());

    // also assert against the actual index
    const Index *index = coll1->_get_index();  // seq id will always be zero for first document
    auto search_index = index->_get_search_index();
    auto numerical_index = index->_get_numerical_index();

    auto str_tree = search_index["str"];
    auto int32_tree = numerical_index["int32"];
    auto int64_tree = numerical_index["int64"];
    auto float_tree = numerical_index["float"];
    auto bool_tree = numerical_index["bool"];

    ASSERT_EQ(0, art_size(str_tree));

    ASSERT_EQ(0, int32_tree->size());
    ASSERT_EQ(0, int64_tree->size());
    ASSERT_EQ(0, float_tree->size());
    ASSERT_EQ(0, bool_tree->size());

    collectionManager.drop_collection("coll1");
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

    coll1 = collectionManager.get_collection("coll1").get();
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

    nlohmann::json res = coll1->search("phone", {"strarray"}, "", {}, sort_fields, {0}, 10, 1,
                                       token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(1, res["found"].get<size_t>());

    Option<std::string> rem_op = coll1->remove("100");

    ASSERT_TRUE(rem_op.ok());

    res = coll1->search("phone", {"strarray"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(0, res["found"].get<int32_t>());

    // also assert against the actual index
    const Index *index = coll1->_get_index();  // seq id will always be zero for first document
    auto search_index = index->_get_search_index();
    auto numerical_index = index->_get_numerical_index();

    auto strarray_tree = search_index["strarray"];
    auto int32array_tree = numerical_index["int32array"];
    auto int64array_tree = numerical_index["int64array"];
    auto floatarray_tree = numerical_index["floatarray"];
    auto boolarray_tree = numerical_index["boolarray"];

    ASSERT_EQ(0, art_size(strarray_tree));

    ASSERT_EQ(0, int32array_tree->size());
    ASSERT_EQ(0, int64array_tree->size());
    ASSERT_EQ(0, floatarray_tree->size());
    ASSERT_EQ(0, boolarray_tree->size());

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

    coll_large_text = collectionManager.get_collection("coll_large_text").get();
    if(coll_large_text == nullptr) {
        coll_large_text = collectionManager.create_collection("coll_large_text", 4, fields, "age").get();
    }

    std::string json_line;
    std::ifstream infile(std::string(ROOT_DIR)+"test/large_text_field.jsonl");

    while (std::getline(infile, json_line)) {
        coll_large_text->add(json_line);
    }

    infile.close();

    Option<nlohmann::json> res_op = coll_large_text->search("eguilazer", {"text"}, "", {}, sort_fields, {0}, 10);
    ASSERT_TRUE(res_op.ok());
    nlohmann::json results = res_op.get();
    ASSERT_EQ(1, results["hits"].size());

    res_op = coll_large_text->search("tristique", {"text"}, "", {}, sort_fields, {0}, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();
    ASSERT_EQ(2, results["hits"].size());

    // query whose length exceeds maximum highlight window (match score's WINDOW_SIZE)
    res_op = coll_large_text->search(
            "Phasellus non tristique elit Praesent non arcu id lectus accumsan venenatis at",
            {"text"}, "", {}, sort_fields, {0}, 10
    );

    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // only single matched token in match window

    res_op = coll_large_text->search("molestie maecenas accumsan", {"text"}, "", {}, sort_fields, {0}, 10);
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

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["categories"] = "Should not be allowed!";
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_STREQ("Field `categories` must be an array.", add_op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, SearchHighlightShouldFollowThreshold) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
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

    auto res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                  token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                  spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_STREQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    // now with with a small threshold (will show only 4 words either side of the matched token)

    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5).get();

    ASSERT_STREQ("fox jumped over the <mark>lazy</mark> dog and ran straight",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    // specify the number of surrounding tokens to return
    size_t highlight_affix_num_tokens = 2;

    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, highlight_affix_num_tokens).get();
    ASSERT_STREQ("over the <mark>lazy</mark> dog and",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    highlight_affix_num_tokens = 0;
    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, highlight_affix_num_tokens).get();
    ASSERT_STREQ("<mark>lazy</mark>",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, SearchHighlightShouldUseHighlightTags) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "The quick brown  fox jumped over the  lazy fox. "; // adding some extra spaces
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // use non-default highlighting tags

    auto res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                             token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                             "<em class=\"h\">", "</em>").get();

    ASSERT_STREQ("The quick brown  fox jumped over the  <em class=\"h\">lazy</em> fox. ",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, SearchHighlightWithNewLine) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "Blah, blah\nStark Industries";
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto res = coll1->search("stark", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                             token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0).get();

    ASSERT_STREQ("Blah, blah\n<mark>Stark</mark> Industries",
                 res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    ASSERT_STREQ("Stark", res["hits"][0]["highlights"][0]["matched_tokens"][0].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, UpdateDocument) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("tags", field_types::STRING_ARRAY, true, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "The quick brown fox jumped over the lazy dog and ran straight to the forest to sleep.";
    doc["tags"] = {"NEWS", "LAZY"};
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto res = coll1->search("lazy", {"title"}, "", {"tags"}, sort_fields, {0}, 10, 1,
                             token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("The quick brown fox jumped over the lazy dog and ran straight to the forest to sleep.",
            res["hits"][0]["document"]["title"].get<std::string>().c_str());

    // reindex the document entirely again verbatim and try querying
    add_op = coll1->add(doc.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());
    ASSERT_EQ(1, coll1->get_num_documents());

    res = coll1->search("lazy", {"title"}, "", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["facet_counts"].size());
    ASSERT_STREQ("tags", res["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, res["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("LAZY", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_STREQ("NEWS", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][1]["count"]);

    // upsert only part of the document -- document should be REPLACED
    nlohmann::json partial_doc = doc;
    partial_doc.erase("tags");
    add_op = coll1->add(partial_doc.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());

    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_FALSE(res["hits"][0].contains("tags"));

    // upserting without a mandatory field should be an error
    partial_doc = doc;
    partial_doc.erase("title");
    add_op = coll1->add(partial_doc.dump(), UPSERT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `title` has been declared in the schema, but is not found in the document.", add_op.error());

    // try changing the title and searching for an older token
    doc["title"] = "The quick brown fox.";
    add_op = coll1->add(doc.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());

    ASSERT_EQ(1, coll1->get_num_documents());

    res = coll1->search("lazy", {"title"}, "", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(0, res["hits"].size());

    res = coll1->search("quick", {"title"}, "", {"title"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("The quick brown fox.", res["hits"][0]["document"]["title"].get<std::string>().c_str());

    // try to update document tags without `id`
    nlohmann::json doc2;
    doc2["tags"] = {"SENTENCE"};
    add_op = coll1->add(doc2.dump(), UPDATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_STREQ("For update, the `id` key must be provided.", add_op.error().c_str());

    // now change tags with id
    doc2["id"] = "100";
    add_op = coll1->add(doc2.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    // check for old tag
    res = coll1->search("NEWS", {"tags"}, "", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(0, res["hits"].size());

    // now check for new tag and also try faceting on that field
    res = coll1->search("SENTENCE", {"tags"}, "", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("SENTENCE", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // try changing points
    nlohmann::json doc3;
    doc3["points"] = 99;
    doc3["id"] = "100";

    add_op = coll1->add(doc3.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    res = coll1->search("*", {"tags"}, "points: > 90", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(99, res["hits"][0]["document"]["points"].get<size_t>());

    // id can be passed by param
    nlohmann::json doc4;
    doc4["points"] = 105;

    add_op = coll1->add(doc4.dump(), UPDATE, "100");
    ASSERT_TRUE(add_op.ok());

    res = coll1->search("*", {"tags"}, "points: > 101", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(105, res["hits"][0]["document"]["points"].get<size_t>());

    // try to change a field with bad value and verify that old document is put back
    doc4["points"] = "abc";
    add_op = coll1->add(doc4.dump(), UPDATE, "100");
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `points` must be an int32.", add_op.error());

    res = coll1->search("*", {"tags"}, "points: > 101", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(105, res["hits"][0]["document"]["points"].get<size_t>());

    // when explicit path id does not match doc id, error should be returned
    nlohmann::json doc5;
    doc5["id"] = "800";
    doc5["title"] = "The Secret Seven";
    doc5["points"] = 250;
    doc5["tags"] = {"BOOK", "ENID BLYTON"};

    add_op = coll1->add(doc5.dump(), UPSERT, "799");
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ(400, add_op.code());
    ASSERT_STREQ("The `id` of the resource does not match the `id` in the JSON body.", add_op.error().c_str());

    // passing an empty id should not succeed
    nlohmann::json doc6;
    doc6["id"] = "";
    doc6["title"] = "The Secret Seven";
    doc6["points"] = 250;
    doc6["tags"] = {"BOOK", "ENID BLYTON"};

    add_op = coll1->add(doc6.dump(), UPDATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ(400, add_op.code());
    ASSERT_STREQ("The `id` should not be empty.", add_op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, UpdateDocumentSorting) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "100";
    doc1["title"] = "The quick brown fox jumped over the lazy dog and ran straight to the forest to sleep.";
    doc1["tags"] = {"NEWS", "LAZY"};
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "101";
    doc2["title"] = "The random sentence.";
    doc2["tags"] = {"RANDOM"};
    doc2["points"] = 101;

    auto add_op = coll1->add(doc1.dump());
    coll1->add(doc2.dump());

    auto res = coll1->search("*", {"tags"}, "", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(101, res["hits"][0]["document"]["points"].get<size_t>());
    ASSERT_STREQ("101", res["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(100, res["hits"][1]["document"]["points"].get<size_t>());
    ASSERT_STREQ("100", res["hits"][1]["document"]["id"].get<std::string>().c_str());

    // now update doc1 points from 100 -> 1000 and it should bubble up
    doc1["points"] = 1000;
    coll1->add(doc1.dump(), UPDATE);

    res = coll1->search("*", {"tags"}, "", {"tags"}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(1000, res["hits"][0]["document"]["points"].get<size_t>());
    ASSERT_STREQ("100", res["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(101, res["hits"][1]["document"]["points"].get<size_t>());
    ASSERT_STREQ("101", res["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, UpdateDocumentUnIndexedField) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "The quick brown fox jumped over the lazy dog and ran straight to the forest to sleep.";
    doc["foo"] = "foo1";
    doc["points"] = 25;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                             token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("The quick brown fox jumped over the lazy dog and ran straight to the forest to sleep.",
                 res["hits"][0]["document"]["title"].get<std::string>().c_str());

    // reindex the document again by changing only the unindexed field
    doc["foo"] = "foo2";
    add_op = coll1->add(doc.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());

    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_STREQ("foo2", res["hits"][0]["document"]["foo"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, SearchHighlightFieldFully) {
    Collection *coll1;

    std::vector<field> fields = { field("title", field_types::STRING, true),
                                  field("tags", field_types::STRING_ARRAY, true),
                                  field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
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

    auto res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title").get();

    ASSERT_EQ(1, res["hits"][0]["highlights"].size());
    ASSERT_STREQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
                 res["hits"][0]["highlights"][0]["value"].get<std::string>().c_str());

    // should not return value key when highlight_full_fields is not specified
    res = coll1->search("lazy", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "").get();

    ASSERT_EQ(3, res["hits"][0]["highlights"][0].size());

    // query multiple fields
    res = coll1->search("lazy", {"title", "tags"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 5, 5, "title, tags").get();

    ASSERT_EQ(2, res["hits"][0]["highlights"].size());

    ASSERT_EQ("tags", res["hits"][0]["highlights"][0]["field"]);
    ASSERT_EQ(1, res["hits"][0]["highlights"][0]["values"].size());
    ASSERT_EQ("<mark>LAZY</mark>", res["hits"][0]["highlights"][0]["values"][0].get<std::string>());
    ASSERT_EQ(1, res["hits"][0]["highlights"][0]["snippets"].size());
    ASSERT_EQ("<mark>LAZY</mark>", res["hits"][0]["highlights"][0]["snippets"][0].get<std::string>());

    ASSERT_EQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
              res["hits"][0]["highlights"][1]["value"].get<std::string>());
    ASSERT_EQ("title", res["hits"][0]["highlights"][1]["field"]);
    ASSERT_EQ(1, res["hits"][0]["highlights"][1]["matched_tokens"].size());
    ASSERT_STREQ("lazy", res["hits"][0]["highlights"][1]["matched_tokens"][0].get<std::string>().c_str());

    // excluded fields should not be returned in highlights section
    spp::sparse_hash_set<std::string> excluded_fields = {"tags"};
    res = coll1->search("lazy", {"title", "tags"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        excluded_fields, 10, "", 5, 5, "title, tags").get();

    ASSERT_EQ(1, res["hits"][0]["highlights"].size());
    ASSERT_STREQ("The quick brown fox jumped over the <mark>lazy</mark> dog and ran straight to the forest to sleep.",
                 res["hits"][0]["highlights"][0]["value"].get<std::string>().c_str());

    // when all fields are excluded
    excluded_fields = {"tags", "title"};
    res = coll1->search("lazy", {"title", "tags"}, "", {}, sort_fields, {0}, 10, 1,
                        token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                        excluded_fields, 10, "", 5, 5, "title, tags").get();
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

    coll1 = collectionManager.get_collection("coll1").get();
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

    auto res = coll1->search("*", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(6, res["found"].get<size_t>());

    // search on optional `description` field
    res = coll1->search("book", {"description"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, res["found"].get<size_t>());

    // filter on optional `average` field
    res = coll1->search("the", {"title"}, "average: >0", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, res["found"].get<size_t>());

    // facet on optional `description` field
    res = coll1->search("the", {"title"}, "", {"description"}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(6, res["found"].get<size_t>());
    ASSERT_EQ(5, res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_STREQ("description", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    // sort_by optional `average` field should be allowed (default used for missing values)
    std::vector<sort_by> sort_fields = { sort_by("average", "DESC") };
    auto res_op = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(res_op.ok());
    res = res_op.get();

    ASSERT_EQ(6, res["found"].get<size_t>());
    ASSERT_EQ(0, res["hits"][5]["document"].count("average"));  // record with missing average is last

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

TEST_F(CollectionTest, OptionalFieldCanBeNull) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false, true),
                                 field("genres", field_types::STRING_ARRAY, false, true),
                                 field("launch_year", field_types::INT32, false, true),
                                 field("updated_at", field_types::INT64, false, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;

    doc["id"] = "0";
    doc["title"] = "Beat it";
    doc["artist"] = nullptr;
    doc["genres"] = nullptr;
    doc["launch_year"] = nullptr;
    doc["updated_at"] = nullptr;
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    ASSERT_EQ(2, coll1->_get_index()->_get_search_index().at("title")->size);
    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().at("artist")->size);
    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().at("genres")->size);

    auto results = coll1->search("beat",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, EmptyStringNotIndexed) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false, true),
                                 field("genres", field_types::STRING_ARRAY, false, true),
                                 field("launch_year", field_types::STRING, false, true),
                                 field("labels", field_types::STRING_ARRAY, false, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;

    doc["id"] = "0";
    doc["title"] = "Beat it";
    doc["artist"] = "";
    doc["launch_year"] = " ";
    doc["genres"] = {""};
    doc["labels"] = {"song", " ", ""};
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("beat",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_EQ(2, coll1->_get_index()->_get_search_index().at("title")->size);
    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().at("artist")->size);
    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().at("launch_year")->size);
    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().at("genres")->size);
    ASSERT_EQ(1, coll1->_get_index()->_get_search_index().at("labels")->size);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, WildcardQueryReturnsResultsBasedOnPerPageParam) {
    std::vector<std::string> facets;
    spp::sparse_hash_set<std::string> empty;
    nlohmann::json results = collection->search("*", query_fields, "", facets, sort_fields, {0}, 12, 1,
            FREQUENCY, {false}, 1000, empty, empty, 10).get();

    ASSERT_EQ(12, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    // should match collection size
    results = collection->search("*", query_fields, "", facets, sort_fields, {0}, 100, 1,
                                 FREQUENCY, {false}, 1000, empty, empty, 10).get();

    ASSERT_EQ(25, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    // cannot fetch more than in-built limit of 250
    auto res_op = collection->search("*", query_fields, "", facets, sort_fields, {0}, 251, 1,
                                 FREQUENCY, {false}, 1000, empty, empty, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(422, res_op.code());
    ASSERT_STREQ("Only upto 250 hits can be fetched per page.", res_op.error().c_str());

    // when page number is not valid
    res_op = collection->search("*", query_fields, "", facets, sort_fields, {0}, 10, 0,
                                     FREQUENCY, {false}, 1000, empty, empty, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ(422, res_op.code());
    ASSERT_STREQ("Page must be an integer of value greater than 0.", res_op.error().c_str());

    // do pagination

    results = collection->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1,
                                 FREQUENCY, {false}, 1000, empty, empty, 10).get();

    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    results = collection->search("*", query_fields, "", facets, sort_fields, {0}, 10, 2,
                                 FREQUENCY, {false}, 1000, empty, empty, 10).get();

    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    results = collection->search("*", query_fields, "", facets, sort_fields, {0}, 10, 3,
                                 FREQUENCY, {false}, 1000, empty, empty, 10).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(25, results["found"].get<int>());

    // enforce limit_hits
    res_op = collection->search("*", query_fields, "", facets, sort_fields, {0}, 10, 3,
                                 FREQUENCY, {false}, 1000,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1}, 20);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ(
            "Only upto 20 hits can be fetched. Ensure that `page` and `per_page` parameters are within this range.",
            res_op.error().c_str());
}

TEST_F(CollectionTest, RemoveIfFound) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    for(size_t i=0; i<10; i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        coll1->add(doc.dump());
    }

    auto res = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1,
                             token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0).get();

    ASSERT_EQ(10, res["found"].get<int>());

    // removing found doc
    Option<bool> found_op = coll1->remove_if_found(0);
    ASSERT_TRUE(found_op.ok());
    ASSERT_TRUE(found_op.get());

    auto get_op = coll1->get("0");
    ASSERT_FALSE(get_op.ok());
    ASSERT_EQ(404, get_op.code());

    // removing doc not found
    found_op = coll1->remove_if_found(100);
    ASSERT_TRUE(found_op.ok());
    ASSERT_FALSE(found_op.get());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, CreateCollectionInvalidFieldType) {
    std::vector<field> fields = {field("title", "blah", true),
                                 field("points", "int", false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    collectionManager.drop_collection("coll1");

    auto create_op = collectionManager.create_collection("coll1", 4, fields, "points");

    ASSERT_FALSE(create_op.ok());
    ASSERT_STREQ("Field `title` has an invalid data type `blah`, see docs for supported data types.",
                 create_op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldRelevance) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Down There by the Train", "Dustin Kensrue"},
        {"Down There by the Train", "Gord Downie"},
        {"State Trooper", "Dustin Kensrue"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("Dustin Kensrue Down There by the Train",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    std::vector<size_t> expected_ids = {0, 1, 2};

    for(size_t i=0; i<expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], std::stoi(results["hits"][i]["document"]["id"].get<std::string>()));
    }

    ASSERT_STREQ("<mark>Down</mark> <mark>There</mark> <mark>by</mark> <mark>the</mark> <mark>Train</mark>",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    ASSERT_STREQ("<mark>Down</mark> <mark>There</mark> <mark>by</mark> <mark>the</mark> <mark>Train</mark>",
                 results["hits"][1]["highlights"][0]["snippet"].get<std::string>().c_str());

    ASSERT_STREQ("<mark>Dustin</mark> <mark>Kensrue</mark>",
                 results["hits"][2]["highlights"][0]["snippet"].get<std::string>().c_str());

    // remove documents, reindex in another order and search again
    for(size_t i=0; i<expected_ids.size(); i++) {
        coll1->remove_if_found(i, true);
    }

    records = {
        {"State Trooper", "Dustin Kensrue"},
        {"Down There by the Train", "Gord Downie"},
        {"Down There by the Train", "Dustin Kensrue"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    results = coll1->search("Dustin Kensrue Down There by the Train",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    expected_ids = {2, 1, 0};

    for(size_t i=0; i<expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], std::stoi(results["hits"][i]["document"]["id"].get<std::string>()));
    }

    // with exclude token syntax
    results = coll1->search("-downie dustin kensrue down there by the train",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    expected_ids = {2, 0};

    for(size_t i=0; i<expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], std::stoi(results["hits"][i]["document"]["id"].get<std::string>()));
    }

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldRelevance2) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"A Daikon Freestyle", "Ghosts on a Trampoline"},
        {"Leaving on a Jetplane", "Coby Grant"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("on a jetplane",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // changing weights to favor artist still favors title because it contains all tokens of the query

    results = coll1->search("on a jetplane",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 4}).get();

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // use same weights

    results = coll1->search("on a jetplane",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}).get();

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // add weights to favor artist without all tokens in a query being found in a field

    results = coll1->search("on a helicopter",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 4}).get();

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, FieldWeightsNotProper) {
    // when weights are not given properly
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    auto results_op = coll1->search("on a jetplane",
                                    {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                                    {true}, 10, spp::sparse_hash_set<std::string>(),
                                    spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                    "<mark>", "</mark>", {1});

    ASSERT_FALSE(results_op.ok());
    ASSERT_STREQ("Number of weights in `query_by_weights` does not match number "
                 "of `query_by` fields.", results_op.error().c_str());

    results_op = coll1->search("on a jetplane",
                               {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                               {true}, 10, spp::sparse_hash_set<std::string>(),
                               spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                               "<mark>", "</mark>", {2, 1});

    ASSERT_FALSE(results_op.ok());
    ASSERT_STREQ("Number of weights in `query_by_weights` does not match number "
                 "of `query_by` fields.", results_op.error().c_str());

    // empty weights are fine (will be defaulted to)

    results_op = coll1->search("on a jetplane",
                               {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                               {true}, 10, spp::sparse_hash_set<std::string>(),
                               spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                               "<mark>", "</mark>", {});

    ASSERT_TRUE(results_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldRelevance3) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Taylor Swift Karaoke: reputation", "Taylor Swift"},
        {"Style", "Taylor Swift"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("style taylor swift",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                                 {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("swift",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldRelevance4) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Maddras Dreams", "Chennai King"},
        {"Maddurai Express", "Maddura Maddy"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("maddras",
                                 {"title", "artist"}, "", {}, {}, {2}, 10, 1, FREQUENCY,
                                 {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldRelevance5) {
    Collection *coll1;

    std::vector<field> fields = {field("company_name", field_types::STRING, false),
                                 field("country", field_types::STRING, false),
                                 field("field_a", field_types::STRING, false),
                                 field("num_employees", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "num_employees").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Stark Industries ™", "Canada", "Canadia", "5215"},
        {"Canaida Corp", "United States", "Canadoo", "200"},
        {"Acme Corp", "Mexico", "Canadoo", "300"}
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["company_name"] = records[i][0];
        doc["country"] = records[i][1];
        doc["field_a"] = records[i][2];
        doc["num_employees"] = std::stoi(records[i][3]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("Canada",
                                 {"company_name","country","field_a"}, "", {}, {}, {2}, 10, 1, FREQUENCY,
                                 {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1, 1}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("Canada",
                             {"company_name","field_a","country"}, "", {}, {}, {2}, 10, 1, FREQUENCY,
                             {true}, 10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                             "<mark>", "</mark>", {1, 1, 1}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][0]["highlights"].size());
    ASSERT_EQ("field_a", results["hits"][0]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("<mark>Canadia</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("country", results["hits"][0]["highlights"][1]["field"].get<std::string>());
    ASSERT_EQ("<mark>Canada</mark>", results["hits"][0]["highlights"][1]["snippet"].get<std::string>());

    ASSERT_EQ(2, results["hits"][1]["highlights"].size());
    ASSERT_EQ("field_a", results["hits"][1]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("<mark>Canadoo</mark>", results["hits"][1]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("company_name", results["hits"][1]["highlights"][1]["field"].get<std::string>());
    ASSERT_EQ("<mark>Canaida</mark> Corp", results["hits"][1]["highlights"][1]["snippet"].get<std::string>());

    ASSERT_EQ(1, results["hits"][2]["highlights"].size());
    ASSERT_EQ("field_a", results["hits"][2]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("<mark>Canadoo</mark>", results["hits"][2]["highlights"][0]["snippet"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldRelevance6) {
    // with exact match preference
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Taylor Swift", "Taylor Swift"},
        {"Taylor Swift Song", "Taylor Swift"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("taylor swift",
                                 {"title", "artist"}, "", {}, {}, {2}, 10, 1, FREQUENCY,
                                 {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // when exact matches are disabled
    results = coll1->search("taylor swift",
                            {"title", "artist"}, "", {}, {}, {2}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}, 100, false).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, ExactMatch) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Alpha", "DJ"},
        {"Alpha Beta", "DJ"},
        {"Alpha Beta Gamma", "DJ"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("alpha beta",
                                 {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY,
                                 {true}, 10).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("alpha", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldHighlighting) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("categories", field_types::STRING_ARRAY, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Best Wireless Vehicle Charger",
         "Easily replenish your cell phone with this wireless charger.",
         "Cell Phones > Cell Phone Accessories > Car Chargers"},

        {"Annie's Song",
        "John Denver",
        "Album > Compilation"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;
        std::vector<std::string> categories;
        StringUtils::split(records[i][2], categories, ">");

        doc["id"] = std::to_string(i);
        doc["name"] = records[i][0];
        doc["description"] = records[i][1];
        doc["categories"] = categories;
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("charger",
                                 {"name","description","categories"}, "", {}, {}, {2}, 10, 1, FREQUENCY,
                                 {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1, 1}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(3, results["hits"][0]["highlights"].size());
    ASSERT_EQ("name", results["hits"][0]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("Best Wireless Vehicle <mark>Charger</mark>",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("description", results["hits"][0]["highlights"][1]["field"].get<std::string>());
    ASSERT_EQ("Easily replenish your cell phone with this wireless <mark>charger.</mark>",
              results["hits"][0]["highlights"][1]["snippet"].get<std::string>());

    ASSERT_EQ("categories", results["hits"][0]["highlights"][2]["field"].get<std::string>());
    ASSERT_EQ("Car <mark>Chargers</mark>", results["hits"][0]["highlights"][2]["snippets"][0].get<std::string>());

    results = coll1->search("John With Denver",
                            {"description"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("description", results["hits"][0]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("<mark>John</mark> <mark>Denver</mark>",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("Annies song John Denver",
                            {"name","description"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][0]["highlights"].size());
    ASSERT_EQ("name", results["hits"][0]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("<mark>Annie's</mark> <mark>Song</mark>",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("description", results["hits"][0]["highlights"][1]["field"].get<std::string>());
    ASSERT_EQ("<mark>John</mark> <mark>Denver</mark>",
              results["hits"][0]["highlights"][1]["snippet"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldMatchRanking) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Style", "Taylor Swift"},
        {"Blank Space", "Taylor Swift"},
        {"Balance Overkill", "Taylor Swift"},
        {"Cardigan", "Taylor Swift"},
        {"Invisible String", "Taylor Swift"},
        {"The Last Great American Dynasty", "Taylor Swift"},
        {"Mirrorball", "Taylor Swift"},
        {"Peace", "Taylor Swift"},
        {"Betty", "Taylor Swift"},
        {"Mad Woman", "Taylor Swift"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("taylor swift style",
                                 {"artist", "title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("9", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("8", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldMatchRankingOnArray) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("strong_skills", field_types::STRING_ARRAY, false),
                                 field("skills", field_types::STRING_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::vector<std::string>>> records = {
        {{"John Snow"}, {"Golang", "Vue", "React"}, {"Docker", "Goa", "Elixir"}},
        {{"Jack Dan"}, {"Golang", "Phoenix", "React"}, {"Docker", "Vue", "Kubernetes"}},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["name"] = records[i][0][0];
        doc["strong_skills"] = records[i][1];
        doc["skills"] = records[i][2];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("golang vue",
                                 {"strong_skills", "skills"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, MultiFieldMatchRankingOnFieldOrder) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Toxic", "Britney Spears"},
        {"Bad", "Michael Jackson"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("michael jackson toxic",
                                 {"title", "artist"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 6}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, PrefixRankedAfterExactMatch) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Rotini Puttanesca"},
        {"Poulet Roti Tout Simple"},
        {"Chapatis (Roti)"},
        {"School Days Rotini Pasta Salad"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("roti", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("2", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, HighlightWithAccentedCharacters) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Mise T.J. à  jour  Timy depuis PC"},
        {"Down There by the T.r.a.i.n"},
        {"State Trooper"},
        {"The Google Nexus Q Is Baffling"},
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("à jour", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("Mise T.J. <mark>à</mark>  <mark>jour</mark>  Timy depuis PC",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["matched_tokens"].size());
    ASSERT_STREQ("à", results["hits"][0]["highlights"][0]["matched_tokens"][0].get<std::string>().c_str());
    ASSERT_STREQ("jour", results["hits"][0]["highlights"][0]["matched_tokens"][1].get<std::string>().c_str());

    results = coll1->search("by train", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("Down There <mark>by</mark> the <mark>T.r.a.i.n</mark>",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    results = coll1->search("state trooper", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("<mark>State</mark> <mark>Trooper</mark>",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    // test single character highlight

    results = coll1->search("q", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("The Google Nexus <mark>Q</mark> Is Baffling",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, DISABLED_SearchingForRecordsWithSpecialChars) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("url", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Amazon Home", "https://amazon.com/"},
        {"Google Home", "https://google.com///"},
        {"Github Issue", "https://github.com/typesense/typesense/issues/241"},
        {"Amazon Search", "https://www.amazon.com/s?k=phone&ref=nb_sb_noss_2"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["url"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("google",
                                 {"title", "url"}, "", {}, {}, {2}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][0]["highlights"].size());
    ASSERT_EQ("<mark>Google</mark> Home", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("https://<mark>google</mark>.com///", results["hits"][0]["highlights"][1]["snippet"].get<std::string>());

    results = coll1->search("amazon.com",
                            {"title", "url"}, "", {}, {}, {2}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("typesense",
                            {"title", "url"}, "", {}, {}, {2}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("2", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("nb_sb_noss_2",
                            {"title", "url"}, "", {}, {}, {2}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("https://www.amazon.com/s?k=phone&ref=<mark>nb</mark>_<mark>sb</mark>_<mark>noss</mark>_<mark>2</mark>",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, FieldSpecificNumTypos) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Taylor Swift Karaoke: reputation", "Taylor Swift"},
        {"Taylor & Friends", "Adam Smith"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("tayylor",
                                 {"title", "artist"}, "", {}, {}, {1, 1}, 10, 1, FREQUENCY,
                                 {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());


    results = coll1->search("tayylor",
                            {"title", "artist"}, "", {}, {}, {0, 1}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // must return error when num_typos does not match length of search fields queried
    auto res_op = coll1->search("tayylor",
                            {"title"}, "", {}, {}, {0, 1}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1});

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Number of weights in `query_by_weights` does not match number of `query_by` fields.", res_op.error());

    // can use a single typo param for multiple fields
    results = coll1->search("tayylor",
                            {"title", "artist"}, "", {}, {}, {1}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    // wildcard search with typos
    results = coll1->search("*",
                            {}, "", {}, {}, {1}, 10, 1, FREQUENCY,
                            {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, BadHighlightingOnText) {
    Collection *coll1;

    std::vector<field> fields = {field("text", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc;

    doc["id"] = "0";
    doc["text"] = "include destruction of natural marine and estuarine\\nhabitats, loss of productive agricultural "
                  "land,\\nand soil erosion. 90 When interviewed, multiple\\nexperts stated that inappropriate land use "
                  "and\\nmanagement is a central factor contributing to\\nenvironmental degradation in the "
                  "Castries-Gros\\nIslet Corridor. 91 The construction is placing greater\\nstress on natural resources "
                  "and biodiversity, and\\nthe capacity to produce food and retain freshwater\\nhas been diminished. "
                  "92 Moreover, increased\\nwater consumption by the tourism sector, when\\ncompounded by climate "
                  "change, is increasing food\\nand water insecurity throughout Saint Lucia, as well\\nas suppressing "
                  "long-term growth prospects. 93";

    doc["points"] = 0;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("natural saint lucia", {"text"}, "", {}, {}, {1}, 10, 1, FREQUENCY,
                                 {true}, 10).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("food\\nand water insecurity throughout <mark>Saint</mark> <mark>Lucia,</mark> as well\\nas suppressing long-term",
                 results["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    ASSERT_EQ(2, results["hits"][0]["highlights"][0]["matched_tokens"].size());
    ASSERT_STREQ("Saint", results["hits"][0]["highlights"][0]["matched_tokens"][0].get<std::string>().c_str());
    ASSERT_STREQ("Lucia,", results["hits"][0]["highlights"][0]["matched_tokens"][1].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, FieldLevelPrefixConfiguration) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
            {"Taylor Swift Karaoke: reputation", "Taylor Swift"},
            {"Style", "Taylor Swift"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("taylo",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                                 {true, false}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("taylo",
                            {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true, true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionTest, QueryParsingForPhraseSearch) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::string> q_include_tokens;
    std::vector<std::vector<std::string>> q_exclude_tokens;
    std::vector<std::vector<std::string>> q_phrases;

    std::string q = R"(the "phrase search" query)";
    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);

    ASSERT_EQ(2, q_include_tokens.size());
    ASSERT_EQ("the", q_include_tokens[0]);
    ASSERT_EQ("query", q_include_tokens[1]);
    ASSERT_EQ(1, q_phrases.size());
    ASSERT_EQ(2, q_phrases[0].size());
    ASSERT_EQ("phrase", q_phrases[0][0]);
    ASSERT_EQ("search", q_phrases[0][1]);

    // quoted string has trailing padded space

    q = R"("space padded " query)";
    q_include_tokens.clear();
    q_exclude_tokens.clear();
    q_phrases.clear();

    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);
    ASSERT_EQ(1, q_include_tokens.size());
    ASSERT_EQ("query", q_include_tokens[0]);
    ASSERT_EQ(1, q_phrases.size());
    ASSERT_EQ(2, q_phrases[0].size());
    ASSERT_EQ("space", q_phrases[0][0]);
    ASSERT_EQ("padded", q_phrases[0][1]);

    // multiple quoted strings

    q = R"("first phrase" "second phrase")";
    q_include_tokens.clear();
    q_exclude_tokens.clear();
    q_phrases.clear();

    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);
    ASSERT_EQ(1, q_include_tokens.size());
    ASSERT_EQ("*", q_include_tokens[0]);
    ASSERT_EQ(2, q_phrases.size());
    ASSERT_EQ(2, q_phrases[0].size());
    ASSERT_EQ("first", q_phrases[0][0]);
    ASSERT_EQ("phrase", q_phrases[0][1]);
    ASSERT_EQ("second", q_phrases[1][0]);
    ASSERT_EQ("phrase", q_phrases[1][1]);

    // single quoted string

    q = R"("hello")";
    q_include_tokens.clear();
    q_exclude_tokens.clear();
    q_phrases.clear();

    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);
    ASSERT_EQ(1, q_include_tokens.size());
    ASSERT_EQ("*", q_include_tokens[0]);
    ASSERT_EQ(1, q_phrases.size());
    ASSERT_EQ(1, q_phrases[0].size());
    ASSERT_EQ("hello", q_phrases[0][0]);

    // stray trailing quote

    q = R"(hello")";
    q_include_tokens.clear();
    q_exclude_tokens.clear();
    q_phrases.clear();

    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);
    ASSERT_EQ(1, q_include_tokens.size());
    ASSERT_EQ("hello", q_include_tokens[0]);
    ASSERT_EQ(0, q_phrases.size());

    // padded space one either side of quote
    q = R"("some query " here)";
    q_include_tokens.clear();
    q_exclude_tokens.clear();
    q_phrases.clear();

    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);
    ASSERT_EQ(1, q_include_tokens.size());
    ASSERT_EQ("here", q_include_tokens[0]);
    ASSERT_EQ(1, q_phrases.size());
    ASSERT_EQ(2, q_phrases[0].size());
    ASSERT_EQ("some", q_phrases[0][0]);
    ASSERT_EQ("query", q_phrases[0][1]);

    // with exclude operator
    q = R"(-"some phrase" here)";
    q_include_tokens.clear();
    q_exclude_tokens.clear();
    q_phrases.clear();
    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);
    ASSERT_EQ(1, q_include_tokens.size());
    ASSERT_EQ("here", q_include_tokens[0]);
    ASSERT_EQ(0, q_phrases.size());
    ASSERT_EQ(1, q_exclude_tokens.size());
    ASSERT_EQ(2, q_exclude_tokens[0].size());
    ASSERT_EQ("some", q_exclude_tokens[0][0]);
    ASSERT_EQ("phrase", q_exclude_tokens[0][1]);

    // with multiple exclude operators
    q = R"(-"some phrase" here -token)";
    q_include_tokens.clear();
    q_exclude_tokens.clear();
    q_phrases.clear();
    coll1->parse_search_query(q, q_include_tokens, q_exclude_tokens, q_phrases, "en", false);
    ASSERT_EQ(1, q_include_tokens.size());
    ASSERT_EQ("here", q_include_tokens[0]);
    ASSERT_EQ(0, q_phrases.size());
    ASSERT_EQ(2, q_exclude_tokens.size());
    ASSERT_EQ(2, q_exclude_tokens[0].size());
    ASSERT_EQ("some", q_exclude_tokens[0][0]);
    ASSERT_EQ("phrase", q_exclude_tokens[0][1]);
    ASSERT_EQ(1, q_exclude_tokens[1].size());
    ASSERT_EQ("token", q_exclude_tokens[1][0]);

    collectionManager.drop_collection("coll1");
}
