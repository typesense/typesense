#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionInfixSearchTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_infix";
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

TEST_F(CollectionInfixSearchTest, InfixBasics) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", -1, 1),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "GH100037IN8900X";
    doc["points"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("100037",
                                 {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                                 4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // verify off behavior

    results = coll1->search("100037",
                            {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {off}).get();

    ASSERT_EQ(0, results["found"].get<size_t>());
    ASSERT_EQ(0, results["hits"].size());

    // when fallback is used, only the prefix result is returned

    doc["id"] = "1";
    doc["title"] = "100037SG7120X";
    doc["points"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    results = coll1->search("100037",
                            {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {fallback}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // always behavior: both prefix and infix matches are returned but ranked below prefix match

    results = coll1->search("100037",
                            {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_TRUE(results["hits"][0]["text_match"].get<size_t>() > results["hits"][1]["text_match"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionInfixSearchTest, RespectPrefixAndSuffixLimits) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", -1, 1),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "GH100037IN8900X";
    doc["points"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "X100037SG89007120X";
    doc["points"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // check extra prefixes

    auto results = coll1->search("100037",
                            {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}, 1).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("100037",
                            {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}, 2).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // check extra suffixes
    results = coll1->search("8900",
                            {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}, INT16_MAX, 2).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("8900",
                            {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}, INT16_MAX, 5).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionInfixSearchTest, InfixSpecificField) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", -1, 1),
                                 field("description", field_types::STRING, false, false, true, "", -1, 1),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "GH100037IN8900X";
    doc["description"] = "foobar";
    doc["points"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "foobar";
    doc["description"] = "GH100037IN8900X";
    doc["points"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("100037",
                                 {"title", "description"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                                 4, {always, off}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("100037",
                            {"title", "description"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {off, always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}
