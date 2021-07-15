#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionSpecificTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_specific";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
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

TEST_F(CollectionSpecificTest, SearchTextWithHyphen) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "open-access-may-become-mandatory-for-nih-funded-research";
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("open-access-may-become-mandatory-for-nih-funded-research",
                                 {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, ExplicitHighlightFieldsConfig) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("author", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The quick brown fox was too fast.";
    doc["description"] = "A story about a brown fox who was fast.";
    doc["author"] = "David Pernell";
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("brown fox pernell", {"title"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {false}, 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1}, 10000, true, false, true, "description,author").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(2, results["hits"][0]["highlights"].size());

    ASSERT_EQ("description", results["hits"][0]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("A story about a <mark>brown</mark> <mark>fox</mark> who was fast.", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("author", results["hits"][0]["highlights"][1]["field"].get<std::string>());
    ASSERT_EQ("David <mark>Pernell</mark>", results["hits"][0]["highlights"][1]["snippet"].get<std::string>());

    // excluded fields are NOT respected if explicit highlight fields are provided

    results = coll1->search("brown fox pernell", {"title"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false}, 1, spp::sparse_hash_set<std::string>(),
                            {"description"}, 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1}, 10000, true, false, true, "description,author").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(2, results["hits"][0]["highlights"].size());
    ASSERT_FALSE(results["hits"][0]["document"].contains("description"));

    ASSERT_EQ("description", results["hits"][0]["highlights"][0]["field"].get<std::string>());
    ASSERT_EQ("author", results["hits"][0]["highlights"][1]["field"].get<std::string>());

    // query not matching field selected for highlighting

    results = coll1->search("pernell", {"title", "author"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false}, 1, spp::sparse_hash_set<std::string>(),
                            {"description"}, 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1,1}, 10000, true, false, true, "description").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // wildcard query with search field names

    results = coll1->search("*", {"title", "author"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false}, 1, spp::sparse_hash_set<std::string>(),
                            {"description"}, 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1,1}, 10000, true, false, true, "description,author").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // wildcard query without search field names

    results = coll1->search("*", {}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false}, 1, spp::sparse_hash_set<std::string>(),
                            {"description"}, 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1,1}, 10000, true, false, true, "description,author").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, ExactSingleFieldMatch) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Fast Electric Charger";
    doc1["description"] = "A product you should buy.";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Omega Chargex";
    doc2["description"] = "Chargex is a great product.";
    doc2["points"] = 200;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("charger", {"title", "description"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true, true}).get();

    LOG(INFO) << results;

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, OrderMultiFieldFuzzyMatch) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Moto Insta Share";
    doc1["description"] = "Share information with this device.";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Portable USB Store";
    doc2["description"] = "Use it to charge your phone.";
    doc2["points"] = 50;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("charger", {"title", "description"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true, true}).get();

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, FieldWeighting) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "The Quick Brown Fox";
    doc1["description"] = "Share information with this device.";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Random Title";
    doc2["description"] = "The Quick Brown Fox";
    doc2["points"] = 50;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("brown fox", {"title", "description"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true, true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 4}).get();

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, MultiFieldArrayRepeatingTokens) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("attrs", field_types::STRING_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "E182-72/4";
    doc1["description"] = "Nexsan Technologies 18 SAN Array - 18 x HDD Supported - 18 x HDD Installed";
    doc1["attrs"] = {"Hard Drives Supported > 18", "Hard Drives Installed > 18", "SSD Supported > 18"};
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "RV345-K9-NA";
    doc2["description"] = "Cisco RV345P Router - 18 Ports";
    doc2["attrs"] = {"Number of Ports > 18", "Product Type > Router"};
    doc2["points"] = 50;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("rv345 cisco 18", {"title", "description", "attrs"}, "", {}, {}, {1}, 10,
                                 1, FREQUENCY, {true, true, true}).get();

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, PrefixWithTypos) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "PRÍNCIPE - Restaurante e Snack Bar";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("maria", {"title"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true}).get();

    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("maria", {"title"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false}).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, PrefixVsExactMatch) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Equivalent Ratios"},
        {"Simplifying Ratios 1"},
        {"Rational and Irrational Numbers"},
        {"Simplifying Ratios 2"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("ration",
                                 {"title"}, "", {}, {}, {1}, 10, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    ASSERT_STREQ("2", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][3]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, PrefixWithTypos2) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Av. Mal. Humberto Delgado 206, 4760-012 Vila Nova de Famalicão, Portugal";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("maria", {"title"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true}).get();

    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("maria", {"title"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false}).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, ImportDocumentWithIntegerID) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = 100;
    doc1["title"] = "East India House on Wednesday evening";
    doc1["points"] = 100;

    auto add_op = coll1->add(doc1.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Document's `id` field should be a string.", add_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, CreateManyCollectionsAndDeleteOneOfThem) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    for(size_t i = 0; i <= 10; i++) {
        const std::string& coll_name = "coll" + std::to_string(i);
        collectionManager.drop_collection(coll_name);
        ASSERT_TRUE(collectionManager.create_collection(coll_name, 1, fields, "points").ok());
    }

    auto coll1 = collectionManager.get_collection_unsafe("coll1");
    auto coll10 = collectionManager.get_collection_unsafe("coll10");

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The quick brown fox was too fast.";
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());
    ASSERT_TRUE(coll10->add(doc.dump()).ok());

    collectionManager.drop_collection("coll1", true);

    // Record with id "0" should exist in coll10
    ASSERT_TRUE(coll10->get("0").ok());

    for(size_t i = 0; i <= 10; i++) {
        const std::string& coll_name = "coll" + std::to_string(i);
        collectionManager.drop_collection(coll_name);
    }
}

TEST_F(CollectionSpecificTest, DeleteOverridesAndSynonymsOnDiskDuringCollDrop) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    for (size_t i = 0; i <= 10; i++) {
        const std::string& coll_name = "coll" + std::to_string(i);
        collectionManager.drop_collection(coll_name);
        ASSERT_TRUE(collectionManager.create_collection(coll_name, 1, fields, "points").ok());
    }

    auto coll1 = collectionManager.get_collection_unsafe("coll1");

    nlohmann::json override_json = {
        {"id",   "exclude-rule"},
        {
            "rule", {
                 {"query", "of"},
                 {"match", override_t::MATCH_EXACT}
             }
        }
    };
    override_json["excludes"] = nlohmann::json::array();
    override_json["excludes"][0] = nlohmann::json::object();
    override_json["excludes"][0]["id"] = "4";

    override_json["excludes"][1] = nlohmann::json::object();
    override_json["excludes"][1]["id"] = "11";

    override_t override;
    override_t::parse(override_json, "", override);
    coll1->add_override(override);

    // add synonym
    synonym_t synonym1{"ipod-synonyms", {}, {{"ipod"}, {"i", "pod"}, {"pod"}} };
    coll1->add_synonym(synonym1);

    collectionManager.drop_collection("coll1");

    // overrides should have been deleted from the store
    std::vector<std::string> stored_values;
    store->scan_fill(Collection::COLLECTION_OVERRIDE_PREFIX, stored_values);
    ASSERT_TRUE(stored_values.empty());

    // synonyms should also have been deleted from the store
    store->scan_fill(Collection::COLLECTION_SYNONYM_PREFIX, stored_values);
    ASSERT_TRUE(stored_values.empty());
}
