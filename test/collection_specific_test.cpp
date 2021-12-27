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
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_specific";
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

    // highlight field that does not exist

    results = coll1->search("brown fox pernell", {"title"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false}, 1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1}, 10000, true, false, true, "not-found").get();

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
                                 1, FREQUENCY, {true, true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // use weights to push title matching ahead

    results = coll1->search("charger", {"title", "description"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {true, true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                            "<mark>", "</mark>", {2, 1}).get();

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

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

TEST_F(CollectionSpecificTest, ExactMatchOnPrefix) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Yeshivah Gedolah High School";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "GED";
    doc2["points"] = 50;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("ged", {"title"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 1).get();

    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, TypoPrefixSearchWithoutPrefixEnabled) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Cisco SG25026HP Gigabit Smart Switch";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("SG25026H", {"title"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {false}, 0,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 1).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

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
                                 {"title"}, "", {}, {}, {1}, 10, 1, FREQUENCY, {true}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

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

TEST_F(CollectionSpecificTest, SingleCharMatchFullFieldHighlight) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Which of the following is a probable sign of infection?";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("a 3-month", {"title"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {false}, 1,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "title", 1).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ("Which of the following is <mark>a</mark> probable sign of infection?",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("Which of the following is <mark>a</mark> probable sign of infection?",
                 results["hits"][0]["highlights"][0]["value"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, TokensSpreadAcrossFields) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Foo bar baz";
    doc1["description"] = "Share information with this device.";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Foo Random";
    doc2["description"] = "The Bar Fox";
    doc2["points"] = 250;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("foo bar", {"title", "description"}, "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false, false},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {4, 1}).get();

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, GuardAgainstIdFieldInSchema) {
    // The "id" field, if defined in the schema should be ignored

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("id", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    nlohmann::json schema;
    schema["name"] = "books";
    schema["fields"] = nlohmann::json::array();
    schema["fields"][0]["name"] = "title";
    schema["fields"][0]["type"] = "string";
    schema["fields"][1]["name"] = "id";
    schema["fields"][1]["type"] = "string";
    schema["fields"][2]["name"] = "points";
    schema["fields"][2]["type"] = "int32";

    Collection* coll1 = collectionManager.create_collection(schema).get();

    ASSERT_EQ(0, coll1->get_schema().count("id"));

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, HandleBadCharactersInStringGracefully) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    std::string doc_str = "不推荐。\",\"price\":10.12,\"ratings\":5}";

    auto add_op = coll1->add(doc_str);
    ASSERT_FALSE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, HighlightSecondaryFieldWithPrefixMatch) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Functions and Equations";
    doc1["description"] = "Use a function to solve an equation.";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Function of effort";
    doc2["description"] = "Learn all about it.";
    doc2["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("function", {"title", "description"}, "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true, true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1}).get();

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ(2, results["hits"][0]["highlights"].size());

    ASSERT_EQ("<mark>Functions</mark> and Equations",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("Use a <mark>function</mark> to solve an equation.",
              results["hits"][0]["highlights"][1]["snippet"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, HighlightWithDropTokens) {
    std::vector<field> fields = {field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["description"] = "HPE Aruba AP-575 802.11ax Wireless Access Point - TAA Compliant - 2.40 GHz, "
                          "5 GHz - MIMO Technology - 1 x Network (RJ-45) - Gigabit Ethernet - Bluetooth 5";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("HPE Aruba AP-575 Technology Gigabit Bluetooth 5", {"description"}, "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "description", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ("<mark>HPE</mark> <mark>Aruba</mark> <mark>AP-575</mark> 802.11ax Wireless Access Point - "
              "TAA Compliant - 2.40 GHz, <mark>5</mark> GHz - MIMO <mark>Technology</mark> - 1 x Network (RJ-45) - "
              "<mark>Gigabit</mark> Ethernet - <mark>Bluetooth</mark> <mark>5</mark>",
              results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, HighlightLongFieldWithDropTokens) {
    std::vector<field> fields = {field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["description"] = "Tripp Lite USB C to VGA Multiport Video Adapter Converter w/ USB-A Hub, USB-C PD Charging "
                          "Port & Gigabit Ethernet Port, Thunderbolt 3 Compatible, USB Type C to VGA, USB-C, USB "
                          "Type-C - for Notebook/Tablet PC - 2 x USB Ports - 2 x USB 3.0 - "
                          "Network (RJ-45) - VGA - Wired";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("wired charging gigabit port", {"description"}, "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "description", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ("Tripp Lite USB C to VGA Multiport Video Adapter Converter w/ USB-A Hub, "
              "USB-C PD <mark>Charging</mark> <mark>Port</mark> & <mark>Gigabit</mark> Ethernet "
              "<mark>Port,</mark> Thunderbolt 3 Compatible, USB Type C to VGA, USB-C, USB Type-C - for "
              "Notebook/Tablet PC - 2 x USB <mark>Ports</mark> - 2 x USB 3.0 - Network (RJ-45) - "
              "VGA - <mark>Wired</mark>",
              results["hits"][0]["highlights"][0]["value"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, HighlightWithDropTokensAndPrefixSearch) {
    std::vector<field> fields = {field("username", field_types::STRING, false),
                                 field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["username"] = "Pandaabear";
    doc1["name"] = "Panda's Basement";
    doc1["tags"] = {"Foobar", "Panda's Basement"};
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["username"] = "Pandaabear";
    doc2["name"] = "Pandaabear Basic";
    doc2["tags"] = {"Pandaabear Basic"};
    doc2["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("pandaabear bas", {"username", "name"},
                                 "", {}, {}, {2, 2}, 10,
                                 1, FREQUENCY, {true, true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(2, results["hits"][0]["highlights"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ(2, results["hits"][1]["highlights"].size());

    ASSERT_EQ("<mark>Pandaabear</mark>",
              results["hits"][1]["highlights"][0]["snippet"].get<std::string>());

    ASSERT_EQ("Panda's <mark>Basement</mark>",
              results["hits"][1]["highlights"][1]["snippet"].get<std::string>());

    results = coll1->search("pandaabear bas", {"username", "tags"},
                  "", {}, {}, {2, 2}, 10,
                  1, FREQUENCY, {true, true},
                  1, spp::sparse_hash_set<std::string>(),
                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                  "<mark>", "</mark>").get();


    // NOT asserting here because given current highlighting limitations, prefixes which are not directly matched
    // onto a token in an array is not used during highlighting

    // ASSERT_EQ(2, results["hits"][1]["highlights"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, PrefixSearchOnlyOnLastToken) {
    std::vector<field> fields = {field("concat", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["concat"] = "SPZ005 SPACEPOLE Spz005 Space Pole Updated!!! Accessories Stands & Equipment Cabinets POS "
                     "Terminal Stand Spacepole 0 SPZ005";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("spz space", {"concat"},
                                 "", {}, {}, {1}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "concat", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, TokenStartingWithSameLetterAsPrevToken) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "John Jack";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "John Williams";
    doc2["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("john j", {"name"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, DroppedTokensShouldNotBeDeemedAsVerbatimMatch) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "John";
    doc1["description"] = "Vegetable Farmer";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "John";
    doc2["description"] = "Organic Vegetable Farmer";
    doc2["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("john vegetable farmer", {"name", "description"},
                                 "", {}, {}, {0, 0}, 10,
                                 1, FREQUENCY, {true, true},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    results = coll1->search("john vegatable farmer", {"name", "description"},
                            "", {}, {}, {1, 1}, 10,
                            1, FREQUENCY, {true, true},
                            2, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>").get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, HighlightEmptyArray) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "John";
    doc1["tags"] = std::vector<std::string>();
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("john", {"name", "tags"},
                                 "", {}, {}, {0, 0}, 10,
                                 1, FREQUENCY, {true, true},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("name", results["hits"][0]["highlights"][0]["field"]);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, CustomSeparators) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "points", 0, "", {}, {"-"}
    ).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "alpha-beta-gamma-omega-zeta";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("gamma", {"name"},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>",{}, 1000,
                                 true, false, true, "", true).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("name", results["hits"][0]["highlights"][0]["field"]);
    ASSERT_EQ("alpha-beta-<mark>gamma</mark>-omega-zeta", results["hits"][0]["highlights"][0]["snippet"]);

    results = coll1->search("gamma-omega", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {false},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>",{}, 1000,
                            true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("name", results["hits"][0]["highlights"][0]["field"]);
    ASSERT_EQ("alpha-beta-<mark>gamma</mark>-<mark>omega</mark>-zeta", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    // ensure that symbols are validated

    nlohmann::json coll_def;
    coll_def["fields"] = {
        {{"name", "foo"}, {"type", "string"}, {"facet", false}}
    };
    coll_def["name"] = "foo";
    coll_def["token_separators"] = {"foo"};

    auto coll_op = collectionManager.create_collection(coll_def);

    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("`token_separators` should be an array of character symbols.", coll_op.error());

    coll_def["token_separators"] = "f";
    coll_op = collectionManager.create_collection(coll_def);

    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("`token_separators` should be an array of character symbols.", coll_op.error());

    coll_def["token_separators"] = 123;
    coll_op = collectionManager.create_collection(coll_def);

    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("`token_separators` should be an array of character symbols.", coll_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, CustomSymbolsForIndexing) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "points", 0, "", {"+"}, {}
    ).get();

    nlohmann::json coll_summary = coll1->get_summary_json();
    ASSERT_EQ(1, coll_summary["symbols_to_index"].size());
    ASSERT_EQ(0, coll_summary["token_separators"].size());

    ASSERT_EQ("+", coll_summary["symbols_to_index"][0].get<std::string>());

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Yes, C++ is great!";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Yes, C is great!";
    doc2["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("c++", {"name"},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>",{}, 1000,
                                 true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(1, results["hits"][0]["highlights"].size());
    ASSERT_EQ("name", results["hits"][0]["highlights"][0]["field"]);
    ASSERT_EQ("Yes, <mark>C++</mark> is great!", results["hits"][0]["highlights"][0]["snippet"]);

    // without custom symbols, + should not be indexed, so the "C" record will show up first

    Collection* coll2 = collectionManager.create_collection("coll2", 1, fields, "points", 0, "").get();

    ASSERT_TRUE(coll2->add(doc1.dump()).ok());
    ASSERT_TRUE(coll2->add(doc2.dump()).ok());

    results = coll2->search("c++", {"name"},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>",{}, 1000,
                                 true, false, true, "", false).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // ensure that symbols are validated

    nlohmann::json coll_def;
    coll_def["fields"] = {
        {{"name", "foo"}, {"type", "string"}, {"facet", false}}
    };
    coll_def["name"] = "foo";
    coll_def["symbols_to_index"] = {"foo"};

    auto coll_op = collectionManager.create_collection(coll_def);

    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("`symbols_to_index` should be an array of character symbols.", coll_op.error());

    coll_def["symbols_to_index"] = "f";
    coll_op = collectionManager.create_collection(coll_def);

    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("`symbols_to_index` should be an array of character symbols.", coll_op.error());

    coll_def["symbols_to_index"] = 123;
    coll_op = collectionManager.create_collection(coll_def);

    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("`symbols_to_index` should be an array of character symbols.", coll_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, CustomSeparatorsHandleQueryVariations) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "points", 0, "", {}, {"-", ".", "*", "&", "/"}
    ).get();

    nlohmann::json coll_summary = coll1->get_summary_json();
    ASSERT_EQ(0, coll_summary["symbols_to_index"].size());
    ASSERT_EQ(5, coll_summary["token_separators"].size());

    ASSERT_EQ("-", coll_summary["token_separators"][0].get<std::string>());
    ASSERT_EQ(".", coll_summary["token_separators"][1].get<std::string>());
    ASSERT_EQ("*", coll_summary["token_separators"][2].get<std::string>());
    ASSERT_EQ("&", coll_summary["token_separators"][3].get<std::string>());
    ASSERT_EQ("/", coll_summary["token_separators"][4].get<std::string>());

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "1&1 Internet Limited";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "bofrost*dienstl";
    doc2["points"] = 100;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "just...grilled";
    doc3["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    auto results = coll1->search("bofrost*dienstl", {"name"},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>",{}, 1000,
                                 true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>bofrost</mark>*<mark>dienstl</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("bofrost * dienstl", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {false},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>",{}, 1000,
                            true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>bofrost</mark>*<mark>dienstl</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("bofrost dienstl", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {false},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>",{}, 1000,
                            true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>bofrost</mark>*<mark>dienstl</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("1&1", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {false},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>",{}, 1000,
                            true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>1</mark>&<mark>1</mark> Internet Limited", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("1 & 1", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {false},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>",{}, 1000,
                            true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>1</mark>&<mark>1</mark> Internet Limited", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    results = coll1->search("just grilled", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {false},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>",{}, 1000,
                            true, false, true, "", false).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("<mark>just</mark>...<mark>grilled</mark>", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, TypoCorrectionWithFaceting) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "points", 0, "", {}, {}
    ).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Salt";
    doc1["brand"] = "Salpices";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Turmeric";
    doc2["brand"] = "Salpices";
    doc2["points"] = 100;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Turmeric";
    doc3["brand"] = "Salpices";
    doc3["points"] = 100;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["name"] = "Tomato";
    doc4["brand"] = "Saltato";
    doc4["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    auto results = coll1->search("salt", {"name", "brand"},
                                 "", {"brand"}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000,
                                 true, false, true, "", true).get();

    ASSERT_EQ(3, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    results = coll1->search("salt", {"name", "brand"},
                            "brand: Salpices", {"brand"}, {}, {2}, 10,
                            1, FREQUENCY, {true},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000,
                            true, false, true, "", true).get();

    ASSERT_EQ(3, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    // without exhaustive search, count be just 1 for non-filtered query

    results = coll1->search("salt", {"name", "brand"},
                            "", {"brand"}, {}, {2}, 10,
                            1, FREQUENCY, {true},
                            1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000,
                            true, false, true, "", false).get();

    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, MultiFieldVerbatimMatchesShouldBeWeighted) {
    // 2 exact matches on low weighted fields should not overpower a single exact match on high weighted field
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, false),
                                 field("label", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Twin";
    doc1["category"] = "kids";
    doc1["label"] = "kids";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Kids";
    doc2["category"] = "children";
    doc2["label"] = "children";
    doc2["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("kids", {"name", "category", "label"},
                                 "", {}, {}, {0, 0, 0}, 10,
                                 1, FREQUENCY, {false, false, false},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {6, 1, 1}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, ZeroWeightedField) {
    // 2 matches on low weighted fields should not overpower a single match on high weighted field
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Energy Kids";
    doc1["category"] = "kids";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Amazing Twin";
    doc2["category"] = "kids";
    doc2["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("kids", {"name", "category"},
                                 "", {}, {}, {0, 0}, 10,
                                 1, FREQUENCY, {false, false},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {0, 1}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, ZeroWeightedFieldCannotPrioritizeExactMatch) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Levis";
    doc1["category"] = "mens";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Amazing from Levis";
    doc2["category"] = "mens";
    doc2["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("levis", {"name", "category"},
                                 "", {}, {}, {0, 0}, 10,
                                 1, FREQUENCY, {false, false},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {0, 1},
                                 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, ImportDocumentWithRepeatingIDInTheSameBatch) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Levis";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "0";
    doc2["name"] = "Amazing from Levis";
    doc2["points"] = 5;

    std::vector<std::string> import_records;
    import_records.push_back(doc1.dump());
    import_records.push_back(doc2.dump());

    nlohmann::json document;
    nlohmann::json import_response = coll1->add_many(import_records, document);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    ASSERT_TRUE(nlohmann::json::parse(import_records[0])["success"].get<bool>());
    ASSERT_FALSE(nlohmann::json::parse(import_records[1])["success"].get<bool>());
    ASSERT_EQ("A document with id 0 already exists.",
              nlohmann::json::parse(import_records[1])["error"].get<std::string>());

    auto results = coll1->search("levis", {"name"},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {0},
                                 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("Levis", results["hits"][0]["document"]["name"].get<std::string>());

    // should allow updates though
    import_records.clear();
    import_records.push_back(doc1.dump());
    import_records.push_back(doc2.dump());
    import_response = coll1->add_many(import_records, document, index_operation_t::UPDATE);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    // should allow upserts also
    import_records.clear();
    import_records.push_back(doc1.dump());
    import_records.push_back(doc2.dump());
    import_response = coll1->add_many(import_records, document, index_operation_t::UPSERT);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    // repeated ID is NOT rejected if the first ID is not indexed due to some error
    import_records.clear();
    doc1.erase("name");
    doc1["id"] = "100";
    doc2["id"] = "100";

    import_records.push_back(doc1.dump());
    import_records.push_back(doc2.dump());

    import_response = coll1->add_many(import_records, document);

    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    ASSERT_FALSE(nlohmann::json::parse(import_records[0])["success"].get<bool>());
    ASSERT_EQ("Field `name` has been declared in the schema, but is not found in the document.",
              nlohmann::json::parse(import_records[0])["error"].get<std::string>());

    ASSERT_TRUE(nlohmann::json::parse(import_records[1])["success"].get<bool>());

    collectionManager.drop_collection("coll1");
}


TEST_F(CollectionSpecificTest, UpdateOfTwoDocsWithSameIdWithinSameBatch) {
    std::vector<field> fields = {field("last_chance", field_types::BOOL, false, true),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    // second update should reflect the result of first update
    std::vector<std::string> updates = {
            R"({"id": "0", "last_chance": false})",
            R"({"id": "0", "points": 200})",
    };

    nlohmann::json update_doc;
    auto import_response = coll1->add_many(updates, update_doc, UPDATE);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    auto results = coll1->search("*", {},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, UpsertOfTwoDocsWithSameIdWithinSameBatch) {
    std::vector<field> fields = {field("last_chance", field_types::BOOL, false, true),
                                 field("points", field_types::INT32, false, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    // first upsert removes both fields, so second upsert should only insert "points"
    std::vector<std::string> upserts = {
            R"({"id": "0", "last_chance": true})",
            R"({"id": "0", "points": 200})",
    };

    nlohmann::json update_doc;
    auto import_response = coll1->add_many(upserts, update_doc, UPSERT);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(2, import_response["num_imported"].get<int>());

    auto results = coll1->search("*", {},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_TRUE(results["hits"][0]["document"].contains("points"));
    ASSERT_FALSE(results["hits"][0]["document"].contains("last_chance"));
    ASSERT_EQ(200, results["hits"][0]["document"]["points"].get<int32_t>());

    ASSERT_EQ(1, coll1->_get_index()->_get_numerical_index().at("points")->size());
    ASSERT_EQ(0, coll1->_get_index()->_get_numerical_index().at("last_chance")->size());

    // update without doc id

    upserts = {
            R"({"last_chance": true})",
    };

    import_response = coll1->add_many(upserts, update_doc, UPDATE);
    ASSERT_FALSE(import_response["success"].get<bool>());
    ASSERT_EQ(0, import_response["num_imported"].get<int>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, UpdateUpsertOfDocWithMissingFields) {
    std::vector<field> fields = {field("last_chance", field_types::BOOL, false, true),
                                 field("points", field_types::INT32, false, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["last_chance"] = true;
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    // upsert doc with missing fields: should be removed from index
    std::vector<std::string> upserts = {
            R"({"id": "0"})"
    };

    nlohmann::json update_doc;
    auto import_response = coll1->add_many(upserts, update_doc, UPSERT);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    auto results = coll1->search("*", {},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(1, results["hits"][0]["document"].size());

    ASSERT_EQ(0, coll1->_get_index()->_get_numerical_index().at("points")->size());
    ASSERT_EQ(0, coll1->_get_index()->_get_numerical_index().at("last_chance")->size());

    // put the original doc back
    ASSERT_TRUE(coll1->add(doc1.dump(), UPSERT).ok());

    results = coll1->search("*", {},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(3, results["hits"][0]["document"].size());

    // update doc with missing fields: existing fields should NOT be removed

    upserts = {
            R"({"id": "0"})"
    };

    import_response = coll1->add_many(upserts, update_doc, UPDATE);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(1, import_response["num_imported"].get<int>());

    results = coll1->search("*", {},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());

    ASSERT_EQ(1, coll1->_get_index()->_get_numerical_index().at("points")->size());
    ASSERT_EQ(1, coll1->_get_index()->_get_numerical_index().at("last_chance")->size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, FacetParallelizationVerification) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    // choose a number that's not a multiple of 4

    for(size_t i = 0; i < 18; i++) {
        nlohmann::json doc1;
        doc1["id"] = std::to_string(i);
        doc1["name"] = "Levis";
        doc1["category"] = "jeans";
        doc1["points"] = 3;

        ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    }

    auto results = coll1->search("levis", {"name"},
                                 "", {"category"}, {}, {0}, 10,
                                 1, FREQUENCY, {false},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {0},
                                 1000, true).get();

    ASSERT_STREQ("category", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(18, (int) results["facet_counts"][0]["counts"][0]["count"]);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, VerbatimMatchShouldConsiderTokensMatchedAcrossAllFields) {
    // dropped tokens on a single field cannot be deemed as verbatim match

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Hamburger";
    doc1["brand"] = "Burger King";
    doc1["points"] = 10;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Hamburger Bun";
    doc2["brand"] = "Trader Joe’s";
    doc2["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("hamburger trader", {"name", "brand"},
                                 "", {}, {}, {0, 0}, 10,
                                 1, FREQUENCY, {false, false},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1, 1},
                                 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Potato Wedges";
    doc3["brand"] = "McDonalds";
    doc3["points"] = 10;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["name"] = "Hot Potato Wedges";
    doc4["brand"] = "KFC Inc.";
    doc4["points"] = 5;

    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    results = coll1->search("potato wedges kfc", {"name", "brand"},
                            "", {}, {}, {0, 0}, 10,
                            1, FREQUENCY, {false, false},
                            2, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1},
                            1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, CustomNumTyposConfiguration) {
    // dropped tokens on a single field cannot be deemed as verbatim match

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Hamburger";
    doc1["brand"] = "Burger and King";
    doc1["points"] = 10;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    // by default a typo on 3 char tokens are ignored (min 4 length is needed)

    auto results = coll1->search("asd", {"brand"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {false},
                                 2, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1},
                                 1000, true, false, true, "", false, 60000*100).get();

    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("asd", {"brand"},
                            "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false},
                            2, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1},
                            1000, true, false, true, "", false, 60000*100, 3, 7).get();

    ASSERT_EQ(1, results["hits"].size());

    // 2 typos are not tolerated by default on 6-len word

    results = coll1->search("bixger", {"brand"},
                            "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false},
                            2, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1},
                            1000, true, false, true, "", false, 60000*100).get();

    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("bixger", {"brand"},
                            "", {}, {}, {2}, 10,
                            1, FREQUENCY, {false},
                            2, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1},
                            1000, true, false, true, "", false, 60000*100, 3, 6).get();

    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, RepeatingStringArrayTokens) {
    std::vector<std::string> tags;

    // when the first document containing a token already cannot fit compact posting list

    for(size_t i = 0; i < 200; i++) {
        tags.emplace_back("spools");
    }

    std::vector<field> fields = {field("tags", field_types::STRING_ARRAY, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["tags"] = tags;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("spools", {"tags"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // when the second document containing a token cannot fit compact posting list
    tags = {"foobar"};
    doc["tags"] = tags;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    for(size_t i = 0; i < 200; i++) {
        tags.emplace_back("foobar");
    }

    doc["tags"] = tags;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    results = coll1->search("foobar", {"tags"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, HighlightOnPrefixRegression) {
    // when the first document containing a token already cannot fit compact posting list

    std::vector<field> fields = {field("title", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "And then there were a storm.";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("and", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, DroppedTokensShouldNotBeUsedForPrefixSearch) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Dog Shoemaker";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Shoe and Sock";
    doc2["points"] = 200;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("shoe cat", {"title"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("cat shoe", {"title"},
                            "", {}, {}, {2}, 10,
                            1, FREQUENCY, {true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, SearchShouldJoinToken) {
    // when the first document containing a token already cannot fit compact posting list
    std::vector<field> fields = {field("title", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["title"] = "The nonstick pressure cooker is a great invention.";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("non stick", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("pressurecooker", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("t h e", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("c o o k e r", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(1, results["hits"].size());

    // three word split won't work

    results = coll1->search("nonstickpressurecooker", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // only first 5 words of the query are used for concat/split

    results = coll1->search("nonstick pressure cooker is a greatinvention", {"title"}, "", {}, {}, {0}, 10, 1,
                            FREQUENCY, {false}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("nonstick pressure cooker is a gr eat", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {false}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, SingleHyphenInQueryNotToBeTreatedAsExclusion) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Saturday Short - Thrive (with Audio Descriptions + Open Captions)";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("Saturday Short - Thrive (with Audio Descriptions + Open Captions)", {"title"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, DuplicateFieldsNotAllowed) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("title", field_types::INT32, true),};
    Option<Collection*> create_op = collectionManager.create_collection("collection", 1, fields);

    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ(create_op.error(), "There are duplicate field names in the schema.");
    ASSERT_EQ(create_op.code(), 400);

    // with dynamic field
    fields = {field("title_.*", field_types::STRING, false, true),
              field("title_.*", field_types::INT32, true, true),};

    create_op = collectionManager.create_collection("collection", 1, fields);

    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ(create_op.error(), "There are duplicate field names in the schema.");
    ASSERT_EQ(create_op.code(), 400);

    // but allow string* with resolved field
    fields = {field("title", "string*", false, true),
              field("title", field_types::STRING, true),};

    create_op = collectionManager.create_collection("collection", 1, fields);

    ASSERT_TRUE(create_op.ok());
}

TEST_F(CollectionSpecificTest, EmptyArrayShouldBeAcceptedAsFirstValue) {
    Collection *coll1;

    std::vector<field> fields = {field("tags", field_types::STRING_ARRAY, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "");
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["company_name"]  = "Amazon Inc.";
    doc["tags"]  = nlohmann::json::array();

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificTest, PhraseSearch) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false, true),
                                 field("points", field_types::INT32, false, true)};
    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "");
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Then and there by the down"},
        {"Down There by the Train"},
        {"The State Trooper"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // without phrase search

    auto results = coll1->search(R"(down there by)", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // with phrase search
    results = coll1->search(R"("down there by")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // phrase search with exclusion
    results = coll1->search(R"("by the" -train)", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // exclusion of an entire phrase
    results = coll1->search(R"(-"by the down")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    results = coll1->search(R"(-"by the")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // phrase search with token with no matching doc
    results = coll1->search(R"("by the dinosaur")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(0, results["hits"].size());

    // phrase search with no matching document
    results = coll1->search(R"("by the state")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(0, results["hits"].size());

    // phrase search with filter condition
    results = coll1->search(R"("there by the")", {"title"}, "points:>=1", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // exclude phrase with tokens that don't have any document matched
    results = coll1->search(R"(-"by the dinosaur")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(3, results["hits"].size());

    // phrase with normal non-matching token
    results = coll1->search(R"("by the" state)", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(0, results["hits"].size());

    // phrase with normal matching token
    results = coll1->search(R"("by the" and)", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // two phrases
    results = coll1->search(R"("by the" "then and")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search(R"("by the" "there by")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(2, results["hits"].size());

    // two phrases with filter
    results = coll1->search(R"("by the" "there by")", {"title"}, "points:>=1", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // single token phrase
    results = coll1->search(R"("trooper")", {"title"}, "points:>=1", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}
