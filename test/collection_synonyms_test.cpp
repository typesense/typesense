#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"
#include "synonym_index.h"
#include "synonym_index_manager.h"

class CollectionSynonymsTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    Collection *coll_mul_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_override";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
        std::vector<field> fields = {
                field("title", field_types::STRING, false),
                field("starring", field_types::STRING, true),
                field("cast", field_types::STRING_ARRAY, true),
                field("points", field_types::INT32, false)
        };

        coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
        if(coll_mul_fields == nullptr) {
            coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
            coll_mul_fields->set_synonym_sets({"index"});
        }

        std::string json_line;

        while (std::getline(infile, json_line)) {
            coll_mul_fields->add(json_line);
        }

        infile.close();

        SynonymIndex synonym_index(store, "index");
        SynonymIndexManager& synonym_index_manager = SynonymIndexManager::get_instance();
        synonym_index_manager.init_store(store);
        synonym_index_manager.add_synonym_index("index", std::move(synonym_index));
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.drop_collection("coll_mul_fields");
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionSynonymsTest, SynonymParsingFromJson) {
    nlohmann::json syn_json = {
        {"id", "syn-1"},
        {"root", "Ocean"},
        {"synonyms", {"Sea"} }
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());

    ASSERT_STREQ("syn-1", synonym.id.c_str());
    ASSERT_STREQ("ocean", synonym.root[0].c_str());
    ASSERT_STREQ("sea", synonym.synonyms[0][0].c_str());

    // should accept without root
    nlohmann::json syn_json_without_root = {
        {"id", "syn-1"},
        {"synonyms", {"Sea", "ocean"} }
    };

    syn_op = synonym_t::parse(syn_json_without_root, synonym);
    ASSERT_TRUE(syn_op.ok());

    // should preserve symbols
    nlohmann::json syn_plus_json = {
        {"id", "syn-plus"},
        {"root", "+"},
        {"synonyms", {"plus", "#"} },
        {"symbols_to_index", {"+", "#"}},
    };

    synonym_t synonym_plus;
    syn_op = synonym_t::parse(syn_plus_json, synonym_plus);
    ASSERT_TRUE(syn_op.ok());

    ASSERT_STREQ("syn-plus", synonym_plus.id.c_str());
    ASSERT_STREQ("+", synonym_plus.root[0].c_str());
    ASSERT_STREQ("plus", synonym_plus.synonyms[0][0].c_str());
    ASSERT_STREQ("#", synonym_plus.synonyms[1][0].c_str());

    nlohmann::json view_json = synonym_plus.to_view_json();
    ASSERT_EQ(2, view_json["symbols_to_index"].size());
    ASSERT_EQ("+", view_json["symbols_to_index"][0].get<std::string>());
    ASSERT_EQ("#", view_json["symbols_to_index"][1].get<std::string>());

    // when `id` is not given
    nlohmann::json syn_json_without_id = {
        {"root", "Ocean"},
        {"synonyms", {"Sea"} }
    };

    syn_op = synonym_t::parse(syn_json_without_id, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Missing `id` field.", syn_op.error().c_str());

    // synonyms missing
    nlohmann::json syn_json_without_synonyms = {
        {"id", "syn-1"},
        {"root", "Ocean"}
    };

    syn_op = synonym_t::parse(syn_json_without_synonyms, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Could not find an array of `synonyms`", syn_op.error().c_str());

    // synonyms bad type

    nlohmann::json syn_json_bad_type1 = R"({
        "id": "syn-1",
        "root": "Ocean",
        "synonyms": [["Sea", 1]]
    })"_json;

    syn_op = synonym_t::parse(syn_json_bad_type1, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Could not find a valid string array of `synonyms`", syn_op.error().c_str());

    nlohmann::json syn_json_bad_type3 = {
        {"id", "syn-1"},
        {"root", "Ocean"},
        {"synonyms", {} }
    };

    syn_op = synonym_t::parse(syn_json_bad_type3, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Could not find an array of `synonyms`", syn_op.error().c_str());

    // empty string in synonym list
    nlohmann::json syn_json_bad_type4 = R"({
        "id": "syn-1",
        "root": "Ocean",
        "synonyms": [["Foo", ""]]
    })"_json;

    syn_op = synonym_t::parse(syn_json_bad_type4, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Could not find a valid string array of `synonyms`", syn_op.error().c_str());

    // root bad type

    nlohmann::json syn_json_root_bad_type = {
        {"id", "syn-1"},
        {"root", 120},
        {"synonyms", {"Sea"} }
    };

    syn_op = synonym_t::parse(syn_json_root_bad_type, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Key `root` should be a string.", syn_op.error().c_str());

    // bad symbols to index
    nlohmann::json syn_json_bad_symbols = {
        {"id", "syn-1"},
        {"root", "Ocean"},
        {"synonyms", {"Sea"} },
        {"symbols_to_index", {}}
    };

    syn_op = synonym_t::parse(syn_json_bad_symbols, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Synonym `symbols_to_index` should be an array of strings.", syn_op.error().c_str());

    syn_json_bad_symbols = {
        {"id", "syn-1"},
        {"root", "Ocean"},
        {"synonyms", {"Sea"} },
        {"symbols_to_index", {"%^"}}
    };

    syn_op = synonym_t::parse(syn_json_bad_symbols, synonym);
    ASSERT_FALSE(syn_op.ok());
    ASSERT_STREQ("Synonym `symbols_to_index` should be an array of single character symbols.", syn_op.error().c_str());
}

TEST_F(CollectionSynonymsTest, SynonymReductionOneWay) {
    std::vector<std::vector<std::string>> results;

    nlohmann::json synonym1 = R"({
        "id": "nyc-expansion",
        "root": "nyc",
        "synonyms": ["new york"]
    })"_json;

    auto add_op = coll_mul_fields->add_synonym(synonym1);
    LOG(INFO) << "Add synonym operation: " << add_op.error();
    ASSERT_TRUE(add_op.ok());

    results.clear();
    coll_mul_fields->synonym_reduction({"red", "nyc", "tshirt"}, "", results);

    ASSERT_EQ(1, results.size());
    ASSERT_EQ(4, results[0].size());

    std::vector<std::string> red_new_york_tshirts = {"red", "new", "york", "tshirt"};
    for(size_t i=0; i<red_new_york_tshirts.size(); i++) {
        ASSERT_STREQ(red_new_york_tshirts[i].c_str(), results[0][i].c_str());
    }

    // when no synonyms exist, reduction should return nothing

    results.clear();
    coll_mul_fields->synonym_reduction({"foo", "bar", "baz"}, "", results);
    ASSERT_EQ(0, results.size());

    // compression and also ensure that it does not revert back to expansion rule

    results.clear();
    nlohmann::json synonym2 = R"({
        "id": "new-york-compression",
        "root": "new york",
        "synonyms": ["nyc"]
    })"_json;
    coll_mul_fields->add_synonym(synonym2);

    coll_mul_fields->synonym_reduction({"red", "new", "york", "tshirt"}, "", results);

    ASSERT_EQ(1, results.size());
    ASSERT_EQ(3, results[0].size());

    std::vector<std::string> red_nyc_tshirts = {"red", "nyc", "tshirt"};
    for(size_t i=0; i<red_nyc_tshirts.size(); i++) {
        ASSERT_STREQ(red_nyc_tshirts[i].c_str(), results[0][i].c_str());
    }

    // replace two synonyms with the same length
    results.clear();
    nlohmann::json synonym3 = R"({
        "id": "t-shirt-compression",
        "root": "t shirt",
        "synonyms": ["tshirt"]
    })"_json;
    coll_mul_fields->add_synonym(synonym3);

    coll_mul_fields->synonym_reduction({"new", "york", "t", "shirt"}, "", results);

    ASSERT_EQ(3, results.size());
    ASSERT_EQ(3, results[0].size());
    ASSERT_EQ(3, results[1].size());
    ASSERT_EQ(2, results[2].size());

    ASSERT_STREQ("new", results[0][0].c_str());
    ASSERT_STREQ("york", results[0][1].c_str());
    ASSERT_STREQ("tshirt", results[0][2].c_str());

    ASSERT_STREQ("nyc", results[1][0].c_str());
    ASSERT_STREQ("t", results[1][1].c_str());
    ASSERT_STREQ("shirt", results[1][2].c_str());

    ASSERT_STREQ("nyc", results[2][0].c_str());
    ASSERT_STREQ("tshirt", results[2][1].c_str());

    // replace two synonyms with different lengths
    results.clear();
    nlohmann::json synonym4 = R"({
        "id": "red-crimson",
        "root": "red",
        "synonyms": ["crimson"]
    })"_json;
    coll_mul_fields->add_synonym(synonym4);

    coll_mul_fields->synonym_reduction({"red", "new", "york", "cap"}, "", results);

    ASSERT_EQ(3, results.size());

    ASSERT_EQ(4, results[0].size());
    ASSERT_EQ(3, results[1].size());
    ASSERT_EQ(3, results[2].size());

    ASSERT_STREQ("crimson", results[0][0].c_str());
    ASSERT_STREQ("new", results[0][1].c_str());
    ASSERT_STREQ("york", results[0][2].c_str());
    ASSERT_STREQ("cap", results[0][3].c_str());

    ASSERT_STREQ("crimson", results[1][0].c_str());
    ASSERT_STREQ("nyc", results[1][1].c_str());
    ASSERT_STREQ("cap", results[1][2].c_str());

    ASSERT_STREQ("red", results[2][0].c_str());
    ASSERT_STREQ("nyc", results[2][1].c_str());
    ASSERT_STREQ("cap", results[2][2].c_str());
}

TEST_F(CollectionSynonymsTest, SynonymReductionMultiWay) {
    nlohmann::json synonym1 = R"({
        "id": "ipod-synonyms",
        "synonyms": ["ipod", "i pod", "pod"]
    })"_json;

    auto op = coll_mul_fields->add_synonym(synonym1);

    std::vector<std::vector<std::string>> results;
    coll_mul_fields->synonym_reduction({"ipod"}, "", results);

    ASSERT_EQ(2, results.size());
    ASSERT_EQ(2, results[0].size());
    ASSERT_EQ(1, results[1].size());

    std::vector<std::string> i_pod = {"i", "pod"};
    for(size_t i=0; i<i_pod.size(); i++) {
        ASSERT_STREQ(i_pod[i].c_str(), results[0][i].c_str());
    }

    ASSERT_STREQ("pod", results[1][0].c_str());

    nlohmann::json synonym2 = R"({
        "id": "car-synonyms",
        "synonyms": ["car", "automobile", "vehicle"]
    })"_json;
    
    op = coll_mul_fields->add_synonym(synonym2);
    ASSERT_TRUE(op.ok());
    results.clear();

    coll_mul_fields->synonym_reduction({"car"}, "", results);

    ASSERT_EQ(2, results.size());
    ASSERT_EQ(1, results[0].size());
    ASSERT_EQ(1, results[1].size());

    ASSERT_STREQ("automobile", results[0][0].c_str());
    ASSERT_STREQ("vehicle", results[1][0].c_str());

    results.clear();

    coll_mul_fields->synonym_reduction({"automobile"}, "", results);
    ASSERT_EQ(2, results.size());

    ASSERT_EQ(1, results[0].size());
    ASSERT_EQ(1, results[1].size());


    nlohmann::json synonym3 = R"({
        "id": "card-synonyms-3",
        "synonyms": ["credit card", "payment card", "cc"]
    })"_json;
    op = coll_mul_fields->add_synonym(synonym3);
    ASSERT_TRUE(op.ok());

    results.clear();
    coll_mul_fields->synonym_reduction({"credit", "card"}, "", results);
    ASSERT_EQ(2, results.size());
    ASSERT_EQ(1, results[0].size());
    ASSERT_EQ(2, results[1].size());

    ASSERT_STREQ("cc", results[0][0].c_str());
    ASSERT_STREQ("payment", results[1][0].c_str());
    ASSERT_STREQ("card", results[1][1].c_str());

    results.clear();
    coll_mul_fields->synonym_reduction({"payment", "card"}, "", results);

    ASSERT_EQ(2, results.size());
    ASSERT_EQ(1, results[0].size());
    ASSERT_EQ(2, results[1].size());

    ASSERT_STREQ("cc", results[0][0].c_str());
    ASSERT_STREQ("credit", results[1][0].c_str());
    ASSERT_STREQ("card", results[1][1].c_str());
}

TEST_F(CollectionSynonymsTest, SynonymBelongingToMultipleSets) {
    nlohmann::json synonym1 = R"({
        "id": "iphone-synonyms",
        "synonyms": ["i phone", "smart phone"]
    })"_json;

    nlohmann::json synonym2 = R"({
        "id": "samsung-synonyms",
        "synonyms": ["smart phone", "galaxy phone", "samsung phone"]
    })"_json;

    coll_mul_fields->add_synonym(synonym1);
    coll_mul_fields->add_synonym(synonym2);

    std::vector<std::vector<std::string>> results;
    coll_mul_fields->synonym_reduction({"smart", "phone"}, "", results);

    ASSERT_EQ(3, results.size());
    ASSERT_EQ(2, results[0].size());
    ASSERT_EQ(2, results[1].size());
    ASSERT_EQ(2, results[2].size());

    ASSERT_STREQ("galaxy", results[0][0].c_str());
    ASSERT_STREQ("phone", results[0][1].c_str());

    ASSERT_STREQ("i", results[1][0].c_str());
    ASSERT_STREQ("phone", results[1][1].c_str());

    ASSERT_STREQ("samsung", results[2][0].c_str());
    ASSERT_STREQ("phone", results[2][1].c_str());
}

TEST_F(CollectionSynonymsTest, OneWaySynonym) {
    nlohmann::json syn_json = {
        {"id", "syn-1"},
        {"root", "Ocean"},
        {"synonyms", {"Sea"} }
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());

    // without synonym

    auto res = coll_mul_fields->search("ocean", {"title"}, "", {}, {}, {0}, 10).get();
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_EQ(0, res["found"].get<uint32_t>());

    // add synonym and redo search
    ASSERT_TRUE(coll_mul_fields->add_synonym(synonym.to_view_json()).ok());

    res = coll_mul_fields->search("ocean", {"title"}, "", {}, {}, {0}, 10).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["found"].get<uint32_t>());
}

TEST_F(CollectionSynonymsTest, SynonymQueryVariantWithDropTokens) {
    std::vector<field> fields = {field("category", field_types::STRING_ARRAY, false),
                                 field("location", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    coll1->set_synonym_sets({"index"});

    nlohmann::json syn_json = {
        {"id", "syn-1"},
        {"root", "us"},
        {"synonyms", {"united states"} }
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());
    coll1->add_synonym(synonym.to_view_json());

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["category"].push_back("sneakers");
    doc1["category"].push_back("jewellery");
    doc1["location"] = "united states";
    doc1["points"] = 10;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["category"].push_back("gloves");
    doc2["category"].push_back("wallets");
    doc2["location"] = "united states";
    doc2["points"] = 20;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["category"].push_back("sneakers");
    doc3["category"].push_back("jewellery");
    doc3["location"] = "england";
    doc3["points"] = 30;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    auto res = coll1->search("us sneakers", {"category", "location"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(3, res["hits"].size());

    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", res["hits"][2]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, SynonymsTextMatchSameAsRootQuery) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    coll1->set_synonym_sets({"index"});

    nlohmann::json syn_json = {
        {"id", "syn-1"},
        {"root", "ceo"},
        {"synonyms", {"chief executive officer"} }
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());
    coll1->add_synonym(synonym.to_view_json());

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Dan Fisher";
    doc1["title"] = "Chief Executive Officer";
    doc1["points"] = 10;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Jack Sparrow";
    doc2["title"] = "CEO";
    doc2["points"] = 20;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto res = coll1->search("ceo", {"name", "title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(2, res["hits"].size());

    ASSERT_EQ("1", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", res["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ(res["hits"][1]["text_match"].get<size_t>(), res["hits"][0]["text_match"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, MultiWaySynonym) {
    nlohmann::json syn_json = {
        {"id",       "syn-1"},
        {"synonyms", {"Home Land", "Homeland", "homǝland"}}
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());

    // without synonym

    auto res = coll_mul_fields->search("homǝland", {"title"}, "", {}, {}, {0}, 10).get();
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_EQ(0, res["found"].get<uint32_t>());

    coll_mul_fields->add_synonym(synonym.to_view_json());

    res = coll_mul_fields->search("homǝland", {"title"}, "", {}, {}, {0}, 10).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["found"].get<uint32_t>());
    ASSERT_STREQ("<mark>Homeland</mark> Security", res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());

    nlohmann::json syn_json2 = {
        {"id",       "syn-2"},
        {"synonyms", {"Samuel L. Jackson", "Sam Jackson", "Leroy"}}
    };

    res = coll_mul_fields->search("samuel leroy jackson", {"starring"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(0, res["hits"].size());

    coll_mul_fields->add_synonym(syn_json2);

    res = coll_mul_fields->search("samuel leroy jackson", {"starring"}, "", {}, {}, {0}, 10).get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(2, res["found"].get<uint32_t>());
    ASSERT_STREQ("<mark>Samuel</mark> <mark>L</mark>. <mark>Jackson</mark>", res["hits"][0]["highlights"][0]["snippet"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Samuel</mark> <mark>L</mark>. <mark>Jackson</mark>", res["hits"][1]["highlights"][0]["snippet"].get<std::string>().c_str());

    // for now we don't support synonyms on ANY prefix

    res = coll_mul_fields->search("ler", {"starring"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_EQ(0, res["found"].get<uint32_t>());
}

TEST_F(CollectionSynonymsTest, ExactMatchRankedSameAsSynonymMatch) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"Laughing out Loud", "Description 1", "100"},
        {"Stop Laughing", "Description 2", "120"},
        {"LOL sure", "Laughing out loud sure", "200"},
        {"Really ROFL now", "Description 3", "250"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["description"] = records[i][1];
        doc["points"] = std::stoi(records[i][2]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    nlohmann::json syn_json = {
        {"id",       "syn-1"},
        {"synonyms", {"Lol", "ROFL", "laughing"}}
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());

    coll1->add_synonym(synonym.to_view_json());

    auto res = coll1->search("laughing", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();

    ASSERT_EQ(4, res["hits"].size());
    ASSERT_EQ(4, res["found"].get<uint32_t>());

    ASSERT_STREQ("3", res["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", res["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", res["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", res["hits"][3]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, ExactMatchVsSynonymMatchCrossFields) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"Head of Marketing", "The Chief Marketing Officer", "100"},
        {"VP of Sales", "Preparing marketing and sales materials.", "120"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["description"] = records[i][1];
        doc["points"] = std::stoi(records[i][2]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    nlohmann::json syn_json = {
        {"id",       "syn-1"},
        {"synonyms", {"cmo", "Chief Marketing Officer", "VP of Marketing"}}
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());

    coll1->add_synonym(synonym.to_view_json());

    auto res = coll1->search("cmo", {"title", "description"}, "", {}, {},
                             {0}, 10, 1, FREQUENCY, {false}, 0).get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(2, res["found"].get<uint32_t>());

    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, SynonymFieldOrdering) {
    // Synonym match on a field earlier in the fields list should rank above exact match of another field
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }
    std::vector<std::vector<std::string>> records = {
        {"LOL really", "Description 1", "50"},
        {"Never stop", "Description 2", "120"},
        {"Yes and no", "Laughing out loud sure", "100"},
        {"And so on", "Description 3", "250"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["description"] = records[i][1];
        doc["points"] = std::stoi(records[i][2]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    nlohmann::json syn_json = {
        {"id",       "syn-1"},
        {"synonyms", {"Lol", "ROFL", "laughing"}}
    };

    synonym_t synonym;
    auto syn_op = synonym_t::parse(syn_json, synonym);
    ASSERT_TRUE(syn_op.ok());

    coll1->add_synonym(synonym.to_view_json());

    auto res = coll1->search("laughing", {"title", "description"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(2, res["found"].get<uint32_t>());

    ASSERT_STREQ("0", res["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", res["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, DeleteAndUpsertDuplicationOfSynonms) {
    coll_mul_fields->add_synonym(R"({"id": "ipod-synonyms", "synonyms": ["i pod", "Apple Phone"]})"_json);
    coll_mul_fields->add_synonym(R"({"id": "case-synonyms", "root": "Cases", "synonyms": ["phone cover", "mobile protector"]})"_json);
    coll_mul_fields->add_synonym(R"({"id": "samsung-synonyms", "root": "s3", "synonyms": ["s3 phone", "samsung"]})"_json);

    ASSERT_EQ(3, coll_mul_fields->get_synonyms().get().size());
    coll_mul_fields->remove_synonym("ipod-synonyms");
    coll_mul_fields->remove_synonym("case-synonyms");

    auto res_op = coll_mul_fields->search("apple phone", {"starring"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());

    res_op = coll_mul_fields->search("cases", {"starring"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());

    auto synonyms = coll_mul_fields->get_synonyms().get();
    ASSERT_EQ(1, synonyms.size());
    ASSERT_EQ("samsung-synonyms", synonyms.begin()->second->id);

    // try to upsert synonym with same ID

    auto upsert_op = coll_mul_fields->add_synonym(R"({"id": "samsung-synonyms", "root": "s3 smartphone",
                                                    "synonyms": ["s3 phone", "samsung"]})"_json);
    ASSERT_TRUE(upsert_op.ok());

    ASSERT_EQ(1, coll_mul_fields->get_synonyms().get().size());

    synonym_t synonym2_updated;
    coll_mul_fields->get_synonym("samsung-synonyms", synonym2_updated);

    ASSERT_EQ("s3", synonym2_updated.root[0]);
    ASSERT_EQ("smartphone", synonym2_updated.root[1]);

    coll_mul_fields->remove_synonym("samsung-synonyms");
    ASSERT_EQ(0, coll_mul_fields->get_synonyms().get().size());
}

TEST_F(CollectionSynonymsTest, UpsertAndSearch) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "title", "type": "string", "locale": "da" },
          {"name": "points", "type": "int32" }
        ],
        "synonym_sets": ["index"]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    nlohmann::json doc;
    doc["title"] = "Rose gold rosenblade, 500 stk";
    doc["points"] = 0;
    coll1->add(doc.dump());

    coll1->add_synonym(R"({"id":"abcde","locale":"da","root":"",
                           "synonyms":["rosegold","rosaguld","rosa guld","rose gold","roseguld","rose guld"]})"_json);

    ASSERT_EQ(1, coll1->get_synonyms().get().size());

    // try to upsert synonym with same ID
    auto upsert_op = coll1->add_synonym(R"({"id":"abcde","locale":"da","root":"",
                           "synonyms":["rosegold","rosaguld","rosa guld","rose gold","roseguld","rose guld"]})"_json);
    ASSERT_TRUE(upsert_op.ok());
    ASSERT_EQ(1, coll1->get_synonyms().get().size());

    // now try searching
    auto res = coll1->search("rosa guld", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["found"].get<uint32_t>());
}

TEST_F(CollectionSynonymsTest, SynonymJsonSerialization) {
    synonym_t synonym1;
    synonym1.id = "ipod-synonyms";
    synonym1.root = {"apple", "ipod"};
    synonym1.raw_root = "apple ipod";

    synonym1.raw_synonyms = {"ipod", "i pod", "pod"};
    synonym1.synonyms.push_back({"ipod"});
    synonym1.synonyms.push_back({"i", "pod"});
    synonym1.synonyms.push_back({"pod"});

    nlohmann::json obj = synonym1.to_view_json();
    ASSERT_STREQ("ipod-synonyms", obj["id"].get<std::string>().c_str());
    ASSERT_STREQ("apple ipod", obj["root"].get<std::string>().c_str());

    ASSERT_EQ(3, obj["synonyms"].size());
    ASSERT_STREQ("ipod", obj["synonyms"][0].get<std::string>().c_str());
    ASSERT_STREQ("i pod", obj["synonyms"][1].get<std::string>().c_str());
    ASSERT_STREQ("pod", obj["synonyms"][2].get<std::string>().c_str());
}

TEST_F(CollectionSynonymsTest, SynonymSingleTokenExactMatch) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"Smashed Lemon", "Description 1", "100"},
        {"Lulu Guinness", "Description 2", "100"},
        {"Lululemon", "Description 3", "100"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["description"] = records[i][1];
        doc["points"] = std::stoi(records[i][2]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    coll1->add_synonym(R"({"id": "syn-1", "root": "lulu lemon", "synonyms": ["lululemon"]})"_json);

    auto res = coll1->search("lulu lemon", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["found"].get<uint32_t>());

    ASSERT_STREQ("2", res["hits"][0]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, SynonymExpansionAndCompressionRanking) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"Smashed Lemon", "100"},
        {"Lulu Lemon", "100"},
        {"Lululemon", "200"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = std::stoi(records[i][1]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    coll1->add_synonym(R"({"id": "syn-1", "root": "lululemon", "synonyms": ["lulu lemon"]})"_json);

    auto res = coll1->search("lululemon", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(2, res["found"].get<uint32_t>());

    // Even thought "lulu lemon" has two token synonym match, it should have same text match score as "lululemon"
    // and hence must be tied and then ranked on "points"
    ASSERT_EQ("2", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ(res["hits"][0]["text_match"].get<size_t>(), res["hits"][1]["text_match"].get<size_t>());

    // now with compression synonym
    coll1->add_synonym(R"({"id": "syn-1", "root": "lulu lemon", "synonyms": ["lululemon"]})"_json);

    res = coll1->search("lulu lemon", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(2, res["found"].get<uint32_t>());

    // Even thought "lululemon" has single token synonym match, it should have same text match score as "lulu lemon"
    // and hence must be tied and then ranked on "points"
    ASSERT_EQ("2", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ(res["hits"][0]["text_match"].get<size_t>(), res["hits"][1]["text_match"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, SynonymQueriesMustHavePrefixEnabled) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"Nonstick Cookware", "100"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = std::stoi(records[i][1]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    coll1->add_synonym(R"({"id": "syn-1", "root": "ns", "synonyms": ["nonstick"]})"_json);

    auto res = coll1->search("ns cook", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["found"].get<uint32_t>());

    res = coll1->search("ns cook", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(0, res["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, SynonymUpsertTwice) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    coll1->add_synonym(R"({"id": "syn-1", "root": "prairie city", "synonyms": ["prairie", "prairiecty"]})"_json);
    coll1->add_synonym(R"({"id": "syn-1", "root": "prairie city", "synonyms": ["prairie", "prairiecty"]})"_json);

    auto res = coll1->search("prairie city", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_EQ(0, res["found"].get<uint32_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, SynonymUpsertTwiceLocale) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    coll1->add_synonym(R"({"id": "syn-1", "locale": "th", "root": "สวัสดีตอนเช้าครับ", "synonyms": ["สวัสดีตอนเช้าค่ะ"]})"_json);
    coll1->add_synonym(R"({"id": "syn-1", "locale": "th", "root": "สวัสดีตอนเช้าครับ", "synonyms": ["สวัสดีตอนเช้าค่ะ"]})"_json);

    auto res = coll1->search("สวัสดีตอนเช้าครับ", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_EQ(0, res["found"].get<uint32_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSynonymsTest, HandleSpecialSymbols) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points",
                                                    0, "", {"+"}, {"."}).get();
        coll1->set_synonym_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"+", "100"},
        {"example.com", "100"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = std::stoi(records[i][1]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    nlohmann::json syn_plus_json = {
        {"id", "syn-1"},
        {"root", "plus"},
        {"synonyms", {"+"} },
        {"symbols_to_index", {"+"}}
    };

    ASSERT_TRUE(coll1->add_synonym(syn_plus_json).ok());

    auto res = coll1->search("plus", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSynonymsTest, SynonymForNonAsciiLanguage) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points",
                                                    0, "", {"+"}, {"."}).get();
        coll1->set_synonym_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"அனைவருக்கும் வணக்கம்", "100"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = std::stoi(records[i][1]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    nlohmann::json syn_plus_json = {
            {"id", "syn-1"},
            {"root", "எல்லோருக்கும்"},
            {"synonyms", {"அனைவருக்கும்"} }
    };

    ASSERT_TRUE(coll1->add_synonym(syn_plus_json).ok());

    auto res = coll1->search("எல்லோருக்கும்", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSynonymsTest, SynonymForKorean) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "title", "type": "string", "locale": "ko"},
          {"name": "points", "type": "int32" }
        ],
        "synonym_sets": ["index"]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    std::vector<std::vector<std::string>> records = {
        {"도쿄구울", "100"},
        {"도쿄 구울", "100"},
        {"구울", "100"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = std::stoi(records[i][1]);

        auto add_op = coll1->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    nlohmann::json synonym1 = R"({
        "id": "syn-1",
        "root": "",
        "synonyms": ["도쿄구울", "도쿄 구울", "구울"],
        "locale": "ko"
    })"_json;

    ASSERT_TRUE(coll1->add_synonym(synonym1).ok());

    auto res = coll1->search("도쿄구울", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(3, res["hits"].size());

    res = coll1->search("도쿄 구울", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(3, res["hits"].size());

    res = coll1->search("구울", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(3, res["hits"].size());
}

TEST_F(CollectionSynonymsTest, SynonymWithLocaleMatch) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "title_en", "type": "string"},
          {"name": "title_es", "type": "string", "locale": "es"},
          {"name": "title_de", "type": "string", "locale": "de"}
        ],
        "synonym_sets": ["index"]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    std::vector<std::vector<std::string>> records = {
        {"Brun New Shoe", "Zapato  nuevo / Sandalen", "Nagelneuer Schuh"},
        {"Marrones socks", "Calcetines marrones / Schuh", "Braune Socken"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title_en"] = records[i][0];
        doc["title_es"] = records[i][1];
        doc["title_de"] = records[i][2];

        auto add_op = coll1->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    nlohmann::json synonym1 = R"({
        "id": "syn-1",
        "root": "",
        "synonyms": ["marrones", "brun"],
        "locale": "es"
    })"_json;

    ASSERT_TRUE(coll1->add_synonym(synonym1).ok());

    nlohmann::json synonym2 = R"({
        "id": "syn-2",
        "root": "",
        "synonyms": ["schuh", "sandalen"],
        "locale": "de"
    })"_json;

    ASSERT_TRUE(coll1->add_synonym(synonym2).ok());

    // the "es" synonym should NOT be resolved to en locale (missing locale field)
    auto res = coll1->search("brun", {"title_en"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"]);

    // the "de" synonym should not work for "es"
    res = coll1->search("schuh", {"title_es"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("1", res["hits"][0]["document"]["id"]);
}

TEST_F(CollectionSynonymsTest, MultipleSynonymSubstitution) {
    nlohmann::json schema = R"({
        "name": "coll2",
        "fields": [
          {"name": "title", "type": "string"},
          {"name": "gender", "type": "string"}
        ],
        "synonym_sets": ["index"]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();

    std::vector<std::vector<std::string>> records = {
            {"Beautiful Blazer", "Male"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["gender"] = records[i][1];

        auto add_op = coll2->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    nlohmann::json synonym1 = R"({
        "id": "foobar",
        "synonyms": ["blazer", "suit"]
    })"_json;

    nlohmann::json synonym2 = R"({
        "id": "foobar2",
        "synonyms": ["male", "man"]
    })"_json;


    ASSERT_TRUE(coll2->add_synonym(synonym1).ok());
    ASSERT_TRUE(coll2->add_synonym(synonym2).ok());

    auto res = coll2->search("blazer male", {"title", "gender"}, "", {},
                             {}, {0}, 10, 1, FREQUENCY, {true},0).get();
    ASSERT_EQ(1, res["hits"].size());

    res = coll2->search("blazer man", {"title", "gender"}, "", {},
                             {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());

    res = coll2->search("suit male", {"title", "gender"}, "", {},
                             {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());

    res = coll2->search("suit man", {"title", "gender"}, "", {},
                             {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
}

TEST_F(CollectionSynonymsTest, EnableSynonymFlag) {
    nlohmann::json schema = R"({
        "name": "coll2",
        "fields": [
          {"name": "title", "type": "string"},
          {"name": "gender", "type": "string"}
        ],
        "synonym_sets": ["index"]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();

    std::vector<std::vector<std::string>> records = {
            {"Beautiful Blazer", "Male"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["gender"] = records[i][1];

        auto add_op = coll2->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    nlohmann::json synonym1 = R"({
        "id": "foobar",
        "synonyms": ["blazer", "suit"]
    })"_json;

    nlohmann::json synonym2 = R"({
        "id": "foobar2",
        "synonyms": ["male", "man"]
    })"_json;


    ASSERT_TRUE(coll2->add_synonym(synonym1).ok());
    ASSERT_TRUE(coll2->add_synonym(synonym2).ok());
    bool enable_synonyms = true;

    auto res = coll2->search("suit man", {"title", "gender"}, "", {},
                           {}, {2}, 10, 1,FREQUENCY, {true},
                           Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "",
                           30, 4, "", 40,
                           {}, {}, {}, 0,"<mark>",
                           "</mark>", {}, 1000,true,
                           false, true, "", false,
                           6000*1000, 4, 7, fallback, 4,
                           {off}, INT16_MAX, INT16_MAX,2,
                           2, false, "", true,
                           0, max_score, 100, 0, 0, 0,
                           "exhaustive", 30000, 2, "",
                           {},{}, "right_to_left", true,
                           true, false, "", "", "",
                           "", false, enable_synonyms).get();

    ASSERT_EQ(1, res["hits"].size());

    enable_synonyms = false;
    res = coll2->search("suit man", {"title", "gender"}, "", {},
                        {}, {2}, 10, 1,FREQUENCY, {true},
                        Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "",
                        30, 4, "", 40,
                        {}, {}, {}, 0,"<mark>",
                        "</mark>", {}, 1000,true,
                        false, true, "", false,
                        6000*1000, 4, 7, fallback, 4,
                        {off}, INT16_MAX, INT16_MAX,2,
                        2, false, "", true,
                        0, max_score, 100, 0, 0, 0,
                        "exhaustive", 30000, 2, "",
                        {},{}, "right_to_left", true,
                        true, false, "", "", "",
                        "", false, enable_synonyms).get();

    ASSERT_EQ(0, res["hits"].size());
}

TEST_F(CollectionSynonymsTest, SynonymTypos) {
    nlohmann::json schema = R"({
        "name": "coll3",
        "fields": [
          {"name": "title", "type": "string"}
        ],
        "synonym_sets": ["index"]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll3 = op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Cool Trousers";

    auto add_op = coll3->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    nlohmann::json synonym1 = R"({
        "id": "foobar",
        "synonyms": ["trousers", "pants"]
    })"_json;

    ASSERT_TRUE(coll3->add_synonym(synonym1).ok());

    auto res = coll3->search("trousers", {"title"}, "", {},
                             {}, {0}, 10, 1, FREQUENCY, {true},0).get();
    ASSERT_EQ(1, res["hits"].size());

    res = coll3->search("pants", {"title"}, "", {},
                        {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());

    //try with typos
    uint32_t synonym_num_typos = 0;
    res = coll3->search("patns", {"title"}, "", {},
                        {}, {2}, 10, 1,FREQUENCY, {true},
                        Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "",
                        30, 4, "", 40,
                        {}, {}, {}, 0,"<mark>",
                        "</mark>", {}, 1000,true,
                        false, true, "", false,
                        6000*1000, 4, 7, fallback, 4,
                        {off}, INT16_MAX, INT16_MAX,2,
                        2, false, "", true,
                        0, max_score, 100, 0, 0, 0,
                        "exhaustive", 30000, 2, "",
                        {},{}, "right_to_left", true,
                        true, false, "", "", "",
                        "", false, true, false, synonym_num_typos).get();
    ASSERT_EQ(0, res["hits"].size());

    synonym_num_typos = 2;

    res = coll3->search("patns", {"title"}, "", {},
                        {}, {2}, 10, 1,FREQUENCY, {true},
                        Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "",
                        30, 4, "", 40,
                        {}, {}, {}, 0,"<mark>",
                        "</mark>", {}, 1000,true,
                        false, true, "", false,
                        6000*1000, 4, 7, fallback, 4,
                        {off}, INT16_MAX, INT16_MAX,2,
                        2, false, "", true,
                        0, max_score, 100, 0, 0, 0,
                        "exhaustive", 30000, 2, "",
                        {},{}, "right_to_left", true,
                        true, false, "", "", "",
                        "", false, true, false, synonym_num_typos).get();
    ASSERT_EQ(1, res["hits"].size());

    //max 2 typos supported
    synonym_num_typos = 3;
    auto search_op = coll3->search("trosuers", {"title"}, "", {},
                        {}, {2}, 10, 1,FREQUENCY, {true},
                        Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "",
                        30, 4, "", 40,
                        {}, {}, {}, 0,"<mark>",
                        "</mark>", {}, 1000,true,
                        false, true, "", false,
                        6000*1000, 4, 7, fallback, 4,
                        {off}, INT16_MAX, INT16_MAX,2,
                        2, false, "", true,
                        0, max_score, 100, 0, 0, 0,
                        "exhaustive", 30000, 2, "",
                        {},{}, "right_to_left", true,
                        true, false, "", "", "",
                        "", false, true, false, synonym_num_typos);

    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ("Value of `synonym_num_typos` must not be greater than 2.",search_op.error());
}

TEST_F(CollectionSynonymsTest, SynonymPrefix) {
    nlohmann::json schema = R"({
        "name": "coll3",
        "fields": [
          {"name": "title", "type": "string"}
        ],
        "synonym_sets": ["index"]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll3 = op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Cool Trousers";
    auto add_op = coll3->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    doc["id"] = "1";
    doc["title"] = "Cool Pants";
    add_op = coll3->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    nlohmann::json synonym1 = R"({
        "id": "foobar",
        "synonyms": ["trousers", "pants"]
    })"_json;
    auto syn_op = coll3->add_synonym(synonym1);
    LOG(INFO) << "Add synonym foobar: " << syn_op.error();
    ASSERT_TRUE(syn_op.ok());

    bool synonym_prefix = false;

    auto res = coll3->search("pan", {"title"}, "", {},
                                   {}, {2}, 10, 1,FREQUENCY, {false},
                                   Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "",
                                   30, 4, "", 40,
                                   {}, {}, {}, 0,"<mark>",
                                   "</mark>", {}, 1000,true,
                                   false, true, "", false,
                                   6000*1000, 4, 7, fallback, 4,
                                   {off}, INT16_MAX, INT16_MAX,2,
                                   2, false, "", true,
                                   0, max_score, 100, 0, 0, 0,
                                   "exhaustive", 30000, 2, "",
                                   {},{}, "right_to_left", true,
                                   true, false, "", "", "",
                                   "", false, true, synonym_prefix).get();

    ASSERT_EQ(0, res["hits"].size());

    synonym_prefix = true;

    res = coll3->search("pan", {"title"}, "", {},
                             {}, {2}, 10, 1,FREQUENCY, {false},
                             Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "",
                             30, 4, "", 40,
                             {}, {}, {}, 0,"<mark>",
                             "</mark>", {}, 1000,true,
                             false, true, "", false,
                             6000*1000, 4, 7, fallback, 4,
                             {off}, INT16_MAX, INT16_MAX,2,
                             2, false, "", true,
                             0, max_score, 100, 0, 0, 0,
                             "exhaustive", 30000, 2, "",
                             {},{}, "right_to_left", true,
                             true, false, "", "", "",
                             "", false, true, synonym_prefix).get();

    ASSERT_EQ(2, res["hits"].size());
}

TEST_F(CollectionSynonymsTest, SynonymsPagination) {
    Collection *coll3;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll3 = collectionManager.get_collection("coll3").get();
    if (coll3 == nullptr) {
        coll3 = collectionManager.create_collection("coll3", 1, fields, "points").get();
        coll3->set_synonym_sets({"index"});
    }

    for (int i = 0; i < 5; ++i) {
        nlohmann::json synonym_json = R"(
                {
                    "id": "foobar",
                    "synonyms": ["blazer", "suit"]
                })"_json;

        synonym_json["id"] = synonym_json["id"].get<std::string>() + std::to_string(i + 1);

        coll3->add_synonym(synonym_json);
    }

    uint32_t limit = 0, offset = 0;

    //limit collections by 2
    limit = 2;
    auto synonym_op = coll3->get_synonyms(limit);
    auto synonym_map = synonym_op.get();
    auto it = synonym_map.begin();
    ASSERT_EQ(2, synonym_map.size());
    ASSERT_EQ("foobar1", it->second->id); it++;
    ASSERT_EQ("foobar2", it->second->id);

    //get 2 collection from offset 3
    offset = 3;
    synonym_op = coll3->get_synonyms(limit, offset);
    synonym_map = synonym_op.get();
    it = synonym_map.begin();
    ASSERT_EQ(2, synonym_map.size());
    ASSERT_EQ("foobar4", it->second->id); it++;
    ASSERT_EQ("foobar5", it->second->id);

    //get all collection except first
    offset = 1;
    limit = 0;
    synonym_op = coll3->get_synonyms(limit, offset);
    synonym_map = synonym_op.get();
    it = synonym_map.begin();
    ASSERT_EQ(4, synonym_map.size());
    ASSERT_EQ("foobar2", it->second->id); it++;
    ASSERT_EQ("foobar3", it->second->id); it++;
    ASSERT_EQ("foobar4", it->second->id); it++;
    ASSERT_EQ("foobar5", it->second->id); it++;

    //get last collection
    offset = 4, limit = 1;
    synonym_op = coll3->get_synonyms(limit, offset);
    synonym_map = synonym_op.get();
    it = synonym_map.begin();
    ASSERT_EQ(1, synonym_map.size());
    ASSERT_EQ("foobar5", it->second->id);

    //if limit is greater than number of collection then return all from offset
    offset = 0;
    limit = 8;
    synonym_op = coll3->get_synonyms(limit, offset);
    synonym_map = synonym_op.get();
    it = synonym_map.begin();
    ASSERT_EQ(5, synonym_map.size());
    ASSERT_EQ("foobar1", it->second->id); it++;
    ASSERT_EQ("foobar2", it->second->id); it++;
    ASSERT_EQ("foobar3", it->second->id); it++;
    ASSERT_EQ("foobar4", it->second->id); it++;
    ASSERT_EQ("foobar5", it->second->id); it++;

    offset = 3;
    limit = 4;
    synonym_op = coll3->get_synonyms(limit, offset);
    synonym_map = synonym_op.get();
    it = synonym_map.begin();
    ASSERT_EQ(2, synonym_map.size());
    ASSERT_EQ("foobar4", it->second->id); it++;
    ASSERT_EQ("foobar5", it->second->id);

    //invalid offset
    offset = 6;
    limit = 0;
    synonym_op = coll3->get_synonyms(limit, offset);
    ASSERT_FALSE(synonym_op.ok());
    ASSERT_EQ("Invalid offset param.", synonym_op.error());
}

TEST_F(CollectionSynonymsTest, AddDuplicateIndexRemovesOld) {

    SynonymIndexManager& mgr = SynonymIndexManager::get_instance();
    // Add first index
    auto add_op1 = mgr.add_synonym_index("dup_index");
    ASSERT_TRUE(add_op1.ok());
    SynonymIndex* idx1 = add_op1.get();

    // Add again, should remove old and create new
    auto add_op2 = mgr.add_synonym_index("dup_index");
    ASSERT_TRUE(add_op2.ok());
    SynonymIndex* idx2 = add_op2.get();

    ASSERT_NE(idx1, idx2);
    // Should only be one in map
    auto get_op = mgr.get_synonym_index("dup_index");
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(get_op.get(), idx2);

    // Remove the index
    auto rem_op = mgr.remove_synonym_index("dup_index");
    ASSERT_TRUE(rem_op.ok());
}

TEST_F(CollectionSynonymsTest, RemoveNonexistentIndex) {
    SynonymIndexManager& mgr = SynonymIndexManager::get_instance();
    auto rem_op = mgr.remove_synonym_index("does_not_exist");
    ASSERT_FALSE(rem_op.ok());
    ASSERT_EQ(rem_op.error(), "Synonym index not found");
}

TEST_F(CollectionSynonymsTest, GetAllSynonymIndicesJson) {
    SynonymIndexManager& mgr = SynonymIndexManager::get_instance();
    mgr.add_synonym_index("idx1");
    mgr.add_synonym_index("idx2");
    auto all_json = mgr.get_all_synonym_indices_json();
    ASSERT_TRUE(all_json.is_array());
    ASSERT_EQ(all_json.size(), 3);
    std::set<std::string> names;
    for (auto& obj : all_json) {
        names.insert(obj["name"].get<std::string>());
    }
    ASSERT_TRUE(names.count("idx1"));
    ASSERT_TRUE(names.count("idx2"));
}

TEST_F(CollectionSynonymsTest, ValidateSynonymIndexPayload) {
    nlohmann::json bad_type = 123;
    auto op = SynonymIndexManager::validate_synonym_index(bad_type);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ(op.error(), "Invalid synonym index format");

    nlohmann::json missing_synonyms = {{"name", "foo"}};
    op = SynonymIndexManager::validate_synonym_index(missing_synonyms);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ(op.error(), "Missing or invalid 'synonyms' field");

    nlohmann::json bad_synonym = {
        {"name", "foo"},
        {"synonyms", { {{"id", "syn-1"}, {"synonyms", {1, 2}} } }}
    };
    op = SynonymIndexManager::validate_synonym_index(bad_synonym);
    ASSERT_FALSE(op.ok());
    ASSERT_TRUE(op.error().find("Could not find a valid string array of `synonyms`") != std::string::npos);
}

TEST_F(CollectionSynonymsTest, SynonymIndexInSearchParams) {
    SynonymIndex idx(store, "tsyn_idx");
    
    nlohmann::json synonym1 = {
        {"id", "syn-1"},
        {"synonyms", {"apple", "fruit"}}
    };
    synonym_t syn;
    synonym_t::parse(synonym1, syn);
    idx.add_synonym(syn);

    SynonymIndexManager& mgr = SynonymIndexManager::get_instance();
    mgr.add_synonym_index("tsyn_idx", std::move(idx));

    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};
    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_synonym_sets({"index"});
    }

    coll1->add(R"({"id": "1", "title": "apple", "points": 100})"_json.dump());

    auto search_op = coll1->search("fruit", {"title"}, "", {}, {}, {0}, 10);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(search_op.get()["hits"].size(), 0);

    search_op = coll1->search("fruit", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, 
                                {true}, 0, {}, {}, 10, "", 30, 4, "", 40,
                                {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true,
                                false, true, "", false, 6000 * 1000, 4, 7, fallback, 4,
                                {off}, INT16_MAX, INT16_MAX, 2, 2, false, "", true,
                                0, max_score, 100, 0, 0,
                                "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true,
                                true, false, "", "", "", "", false,
                                true, false, 0, false, false, DEFAULT_FILTER_BY_CANDIDATES, false, true, true, 
                                "", "", "", "", "", "", "", 0, {"tsyn_idx"});

    ASSERT_TRUE(search_op.ok());
    auto res = search_op.get();
    ASSERT_EQ(res["hits"].size(), 1);
    ASSERT_EQ(res["hits"][0]["document"]["id"].get<std::string>(), "1");
    ASSERT_EQ(res["hits"][0]["document"]["title"].get<std::string>(), "apple");
    ASSERT_EQ(res["hits"][0]["document"]["points"].get<int>(), 100);
}


