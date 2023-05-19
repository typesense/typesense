#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionOperationsTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_operations";
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

TEST_F(CollectionOperationsTest, IncrementInt32Value) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"}
        ]
    })"_json;

    Collection *coll = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Sherlock Holmes";
    doc["points"] = 100;
    ASSERT_TRUE(coll->add(doc.dump()).ok());

    // increment by 1
    doc.erase("points");
    doc["id"] = "0";
    doc["$operations"] = R"({"increment": {"points": 1}})"_json;
    ASSERT_TRUE(coll->add(doc.dump(), UPDATE).ok());

    auto res = coll->search("*", {"title"}, "points:101", {}, {}, {0}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());

    ASSERT_EQ(3, res["hits"][0]["document"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("Sherlock Holmes", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(101, res["hits"][0]["document"]["points"].get<size_t>());

    // increment by 10
    doc["id"] = "0";
    doc["$operations"] = R"({"increment": {"points": 10}})"_json;
    ASSERT_TRUE(coll->add(doc.dump(), UPDATE).ok());

    res = coll->search("*", {"title"}, "points:111", {}, {}, {0}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(3, res["hits"][0]["document"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("Sherlock Holmes", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(111, res["hits"][0]["document"]["points"].get<size_t>());

    // decrement by 10 using negative number
    doc["id"] = "0";
    doc["$operations"] = R"({"increment": {"points": -10}})"_json;
    ASSERT_TRUE(coll->add(doc.dump(), UPDATE).ok());

    res = coll->search("*", {"title"}, "points:101", {}, {}, {0}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(3, res["hits"][0]["document"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("Sherlock Holmes", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(101, res["hits"][0]["document"]["points"].get<size_t>());

    // bad field - should not increment but title field should be updated
    doc["id"] = "0";
    doc["title"] = "The Sherlock Holmes";
    doc["$operations"] = R"({"increment": {"pointsx": -10}})"_json;
    ASSERT_TRUE(coll->add(doc.dump(), UPDATE).ok());
    res = coll->search("*", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(3, res["hits"][0]["document"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("The Sherlock Holmes", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(101, res["hits"][0]["document"]["points"].get<size_t>());
}

TEST_F(CollectionOperationsTest, IncrementInt32ValueCreationViaOptionalField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32", "optional": true}
        ]
    })"_json;

    Collection *coll = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Sherlock Holmes";
    doc["$operations"] = R"({"increment": {"points": 1}})"_json;
    ASSERT_TRUE(coll->add(doc.dump(), EMPLACE).ok());

    auto res = coll->search("*", {"title"}, "points:1", {}, {}, {0}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(3, res["hits"][0]["document"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("Sherlock Holmes", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(1, res["hits"][0]["document"]["points"].get<size_t>());

    // try same with CREATE action
    doc.clear();
    doc["id"] = "1";
    doc["title"] = "Harry Potter";
    doc["$operations"] = R"({"increment": {"points": 10}})"_json;
    ASSERT_TRUE(coll->add(doc.dump(), CREATE).ok());

    res = coll->search("*", {"title"}, "points:10", {}, {}, {0}, 3, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(3, res["hits"][0]["document"].size());
    ASSERT_EQ("1", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("Harry Potter", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(10, res["hits"][0]["document"]["points"].get<size_t>());
}
