#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionSpecificMoreTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_specific_more";
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

TEST_F(CollectionSpecificMoreTest, MaxCandidatesShouldBeRespected) {
    std::vector<field> fields = {field("company", field_types::STRING, true)};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    for (size_t i = 0; i < 200; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["company"] = "prefix"+std::to_string(i);
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("prefix", {"company"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 false, 1000).get();

    ASSERT_EQ(200, results["found"].get<size_t>());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, PrefixExpansionWhenExactMatchExists) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("author", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "The Little Prince [by] Antoine de Saint Exupéry : teacher guide";
    doc1["author"] = "Barbara Valdez";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Little Prince";
    doc2["author"] = "Antoine de Saint-Exupery";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("little prince antoine saint", {"title", "author"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, PrefixExpansionOnSingleField) {
    // when no default sorting field is provided, tokens must be ordered on frequency
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::string> tokens = {
        "Mark Jack", "John Jack", "John James", "John Joseph", "John Jim", "John Jordan",
        "Mark Nicholas", "Mark Abbey", "Mark Boucher", "Mark Bicks", "Mark Potter"
    };

    for(size_t i = 0; i < tokens.size(); i++) {
        std::string title = tokens[i];
        nlohmann::json doc;
        doc["title"] = title;
        doc["points"] = i;
        coll1->add(doc.dump());
    }

    // max candidates as default 4
    auto results = coll1->search("mark j", {"title"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, ArrayElementMatchShouldBeMoreImportantThanTotalMatch) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("author", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Harry Potter and the Prisoner of Azkaban";
    doc1["author"] = "Rowling";
    doc1["tags"] = {"harry", ""};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Fantastic beasts and where to find them";
    doc2["author"] = "Rowling";
    doc2["tags"] = {"harry", "potter", "prisoner", "azkaban", "beasts", "guide", "rowling"};

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["title"] = "Fantastic beasts and where to find them";
    doc3["author"] = "Rowling";
    doc3["tags"] = {"harry potter", "prisoner azkaban", "beasts", "guide", "rowling"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    auto results = coll1->search("harry potter rowling prisoner azkaban", {"title", "author", "tags"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, MatchedSegmentMoreImportantThanTotalMatches) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("author", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "One Two Three Four Five Six Seven Eight Nine Ten Eleven Twelve Thirteen Fourteen";
    doc1["author"] = "Rowling";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "One Four Five Six Seven Eight Nine Ten Eleven Twelve Thirteen Fourteen Three Rowling";
    doc2["author"] = "Two";

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["title"] = "One Three Four Five Six Seven Eight Nine Ten Eleven Twelve Thirteen Fourteen Two Rowling";
    doc3["author"] = "Foo";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    auto results = coll1->search("one two three rowling", {"title", "author"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(3, results["hits"].size());

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
}
