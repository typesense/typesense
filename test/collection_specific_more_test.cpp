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
                                 fallback, 1000).get();

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

TEST_F(CollectionSpecificMoreTest, ArrayMatchAcrossElementsMustNotMatter) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("author", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Por do sol immateur";
    doc1["author"] = "Vermelho";
    doc1["tags"] = {"por do sol", "immateur", "gemsor", "praia", "sol", "vermelho", "suyay"};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Sunset Rising";
    doc2["author"] = "Vermelho";
    doc2["tags"] = {"sunset", "por do sol", "praia", "somao", "vermelho"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("praia por sol vermelho", {"title", "author", "tags"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
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

TEST_F(CollectionSpecificMoreTest, VerbatimMatchNotOnPartialTokenMatch) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Thirteen Fourteen";
    doc1["tags"] = {"foo", "bar", "Hundred", "Thirteen Fourteen"};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "One Eleven Thirteen Fourteen Three";
    doc2["tags"] = {"foo", "bar", "Hundred", "One Eleven Thirteen Fourteen Three"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("hundred thirteen fourteen", {"tags"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
}

TEST_F(CollectionSpecificMoreTest, SortByStringEmptyValuesConfigFirstField) {
    std::vector<field> fields = {field("points", field_types::INT32, false, true),
                                 field("points2", field_types::INT32, false, true),
                                 field("points3", field_types::INT32, false, true)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    for(size_t i = 0; i < 4; i++) {
        nlohmann::json doc;
        if(i == 2) {
            doc["points"] = nullptr;
        } else {
            doc["points"] = i;
        }
        doc["points2"] = 100;
        doc["points3"] = 100;
        coll1->add(doc.dump());
    }

    // without any order config: missing integers always end up last
    sort_fields = {sort_by("points", "asc"),};
    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points", "desc"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // ascending
    sort_fields = {sort_by("points(missing_values: first)", "ASC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points(missing_values: last)", "ASC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // descending
    sort_fields = {sort_by("points(missing_values: first)", "DESC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points(missing_values: last)", "DESC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // bad syntax
    sort_fields = {sort_by("points(foo: bar)", "desc"),};
    auto res_op = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `points`", res_op.error());

    sort_fields = {sort_by("points(missing_values: bar)", "desc"),};
    res_op = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `points`", res_op.error());
}

TEST_F(CollectionSpecificMoreTest, SortByStringEmptyValuesConfigSecondField) {
    std::vector<field> fields = {field("points", field_types::INT32, false, true),
                                 field("points2", field_types::INT32, false, true),
                                 field("points3", field_types::INT32, false, true)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    for(size_t i = 0; i < 4; i++) {
        nlohmann::json doc;
        if(i == 2) {
            doc["points"] = nullptr;
        } else {
            doc["points"] = i;
        }
        doc["points2"] = 100;
        doc["points3"] = 100;
        coll1->add(doc.dump());
    }

    // without any order config: missing integers always end up last
    sort_fields = {sort_by("points2", "asc"),sort_by("points", "asc")};
    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points2", "asc"),sort_by("points", "desc"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // ascending
    sort_fields = {sort_by("points2", "asc"),sort_by("points(missing_values: first)", "ASC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points2", "asc"),sort_by("points(missing_values: last)", "ASC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // descending
    sort_fields = {sort_by("points2", "asc"),sort_by("points(missing_values: first)", "DESC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points2", "asc"),sort_by("points(missing_values: last)", "DESC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, SortByStringEmptyValuesConfigThirdField) {
    std::vector<field> fields = {field("points", field_types::INT32, false, true),
                                 field("points2", field_types::INT32, false, true),
                                 field("points3", field_types::INT32, false, true)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    for(size_t i = 0; i < 4; i++) {
        nlohmann::json doc;
        if(i == 2) {
            doc["points"] = nullptr;
        } else {
            doc["points"] = i;
        }
        doc["points2"] = 100;
        doc["points3"] = 100;
        coll1->add(doc.dump());
    }

    // without any order config: missing integers always end up last
    sort_fields = {sort_by("points2", "asc"),sort_by("points3", "asc"),sort_by("points", "asc")};
    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points2", "asc"),sort_by("points3", "asc"),sort_by("points", "desc"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // ascending
    sort_fields = {sort_by("points2", "asc"),sort_by("points3", "asc"),sort_by("points(missing_values: first)", "ASC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points2", "asc"),sort_by("points3", "asc"),sort_by("points(missing_values: last)", "ASC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // descending
    sort_fields = {sort_by("points2", "asc"),sort_by("points3", "asc"),sort_by("points(missing_values: first)", "DESC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {sort_by("points2", "asc"),sort_by("points3", "asc"),sort_by("points(missing_values: last)", "DESC"),};
    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, WrongTypoCorrection) {
    std::vector<field> fields = {field("title", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Gold plated arvin";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("earrings", {"title"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(0, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, PositionalTokenRanking) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::string> tokens = {
        "Alpha Beta Gamma", "Omega Alpha Theta", "Omega Theta Alpha", "Indigo Omega Theta Alpha"
    };

    for(size_t i = 0; i < tokens.size(); i++) {
        std::string title = tokens[i];
        nlohmann::json doc;
        doc["title"] = title;
        doc["points"] = i;
        coll1->add(doc.dump());
    }

    auto results = coll1->search("alpha", {"title"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                                 Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["hits"][3]["document"]["id"].get<std::string>());

    results = coll1->search("alpha", {"title"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, false).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][3]["document"]["id"].get<std::string>());

    results = coll1->search("theta alpha", {"title"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, false).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());

    results = coll1->search("theta alpha", {"title"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["hits"][2]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, PositionalTokenRankingWithArray) {
    Collection *coll1;
    std::vector<field> fields = {field("tags", field_types::STRING_ARRAY, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["tags"] = {"alpha foo", "gamma", "beta alpha"};
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["tags"] = {"omega", "omega beta alpha"};
    doc2["points"] = 200;

    coll1->add(doc1.dump());
    coll1->add(doc2.dump());

    auto results = coll1->search("alpha", {"tags"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                                 Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, false).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    results = coll1->search("alpha", {"tags"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, ExactFilteringOnArray) {
    Collection *coll1;
    std::vector<field> fields = {field("tags", field_types::STRING_ARRAY, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["tags"] = {"§ 23",
                    "§ 34d EStG",
                    "§ 23 Satz EStG"};
    doc1["points"] = 100;

    coll1->add(doc1.dump());

    auto results = coll1->search("*", {"tags"}, "tags:=§ 23 EStG", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                                 Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, false).get();

    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, SplitTokensCrossFieldMatching) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Vitamin C1";
    doc1["brand"] = "Paulas Choice";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("paulaschoice c1", {"name", "brand"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0).get();

    ASSERT_EQ(1, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, PrefixSearchOnSpecificFields) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    // atleast 4 tokens that begin with "girl" to trigger this regression
    std::vector<std::string> names = {
        "Jungle Girl", "Jungle Girlz", "Jam Foo1", "Jam Foo2", "Jam Foo3", "Jam Foo4", "Jam Foo"
    };

    std::vector<std::string> brands = {
        "Foobar", "Foobar2", "Girlx", "Girly", "Girlz", "Girlz", "Girlzz"
    };

    for(size_t i = 0; i < names.size(); i++) {
        nlohmann::json doc1;
        doc1["name"] = names[i];
        doc1["brand"] = brands[i];
        ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    }

    auto results = coll1->search("jungle girl", {"name", "brand"},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {false, true},
                                 0).get();

    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("jam foo", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {true},
                            0).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("6", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("jam foo", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {false},
                            0).get();

    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, OrderWithThreeSortFields) {
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("type", field_types::INT32, false),
                                 field("valid_from", field_types::INT64, false),
                                 field("created_at", field_types::INT64, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["name"] = "should be 1st";
    doc1["type"] = 2;
    doc1["valid_from"] = 1655741107972;
    doc1["created_at"] = 1655741107724;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    doc1["name"] = "should be 2nd";
    doc1["type"] = 1;
    doc1["valid_from"] = 1656309617303;
    doc1["created_at"] = 1656309617194;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    doc1["name"] = "should be 3rd";
    doc1["type"] = 0;
    doc1["valid_from"] = 0;
    doc1["created_at"] = 1656309677131;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    sort_fields = {sort_by("type", "desc"), sort_by("valid_from", "desc"), sort_by("created_at", "desc")};

    auto results = coll1->search("s", {"name"},
                                 "", {}, sort_fields, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][2]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, LongString) {
    std::vector<field> fields = {field("name", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::string name;
    for(size_t i = 0; i < 100; i++) {
        name += "foo" + std::to_string(i) + " ";
    }

    nlohmann::json doc1;
    doc1["name"] = name;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search(name, {"name"},
                                 "", {}, sort_fields, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0).get();

    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, RelevanceConsiderAllFields) {
    std::vector<field> fields = {field("f1", field_types::STRING, false),
                                 field("f2", field_types::STRING, false),
                                 field("f3", field_types::STRING, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["f1"] = "alpha";
    doc1["f2"] = "alpha";
    doc1["f3"] = "alpha";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    doc1["f1"] = "alpha";
    doc1["f2"] = "alpha";
    doc1["f3"] = "beta";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    doc1["f1"] = "alpha";
    doc1["f2"] = "beta";
    doc1["f3"] = "gamma";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("alpha", {"f1", "f2", "f3"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {3, 2, 1}).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][2]["document"]["id"].get<std::string>());

    // verify match score component values
    ASSERT_EQ("578730123365711899", results["hits"][0]["text_match_info"]["score"].get<std::string>());
    ASSERT_EQ(3, results["hits"][0]["text_match_info"]["fields_matched"].get<size_t>());
    ASSERT_EQ(2, results["hits"][1]["text_match_info"]["fields_matched"].get<size_t>());
    ASSERT_EQ(1, results["hits"][2]["text_match_info"]["fields_matched"].get<size_t>());

    ASSERT_EQ(1, results["hits"][0]["text_match_info"]["tokens_matched"].get<size_t>());
    ASSERT_EQ(1, results["hits"][1]["text_match_info"]["tokens_matched"].get<size_t>());
    ASSERT_EQ(1, results["hits"][2]["text_match_info"]["tokens_matched"].get<size_t>());

    ASSERT_EQ("1108091339008", results["hits"][0]["text_match_info"]["best_field_score"].get<std::string>());
    ASSERT_EQ("1108091339008", results["hits"][1]["text_match_info"]["best_field_score"].get<std::string>());
    ASSERT_EQ("1108091339008", results["hits"][2]["text_match_info"]["best_field_score"].get<std::string>());

    ASSERT_EQ(3, results["hits"][0]["text_match_info"]["best_field_weight"].get<size_t>());
    ASSERT_EQ(3, results["hits"][1]["text_match_info"]["best_field_weight"].get<size_t>());
    ASSERT_EQ(3, results["hits"][2]["text_match_info"]["best_field_weight"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, CrossFieldWeightIsNotAugmentated) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("type", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["title"] = "Nike Shoerack";
    doc1["type"] = "shoe_rack";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    doc1["title"] = "Nike Air Force 1";
    doc1["type"] = "shoe";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("nike shoe", {"type", "title"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {5, 1}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, HighlightWithAccentedChars) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false)};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO, {}, {}, true).get();

    auto nested_doc = R"({
      "title": "Rāpeti Early Learning Centre",
      "companies": [
        {"title": "Rāpeti Early Learning Centre"}
      ]
    })"_json;

    ASSERT_TRUE(coll1->add(nested_doc.dump()).ok());

    auto results = coll1->search("rap", {"title", "companies"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("<mark>Rāp</mark>eti Early Learning Centre", results["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    auto highlight_doc = R"({
      "companies":[
        {
          "title":"<mark>Rāp</mark>eti Early Learning Centre"
        }
      ],
      "title":"<mark>Rāp</mark>eti Early Learning Centre"
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());

    ASSERT_EQ(0, results["hits"][0]["highlight"]["full"].size());
    ASSERT_EQ(2, results["hits"][0]["highlight"]["meta"].size());

    ASSERT_EQ(1, results["hits"][0]["highlight"]["meta"]["title"].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["meta"]["title"]["matched_tokens"].size());
    ASSERT_EQ("Rāp", results["hits"][0]["highlight"]["meta"]["title"]["matched_tokens"][0]);

    ASSERT_EQ(1, results["hits"][0]["highlight"]["meta"]["companies.title"].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["meta"]["companies.title"]["matched_tokens"].size());
    ASSERT_EQ("Rāp", results["hits"][0]["highlight"]["meta"]["companies.title"]["matched_tokens"][0]);
}

TEST_F(CollectionSpecificMoreTest, FieldWeightNormalization) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),
                                 field("type", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::vector<std::string> raw_search_fields = {"title", "brand", "type"};
    std::vector<uint32_t> query_by_weights = {110, 25, 55};
    std::vector<search_field_t> weighted_search_fields;
    std::vector<std::string> reordered_search_fields;

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields,
                                        reordered_search_fields);

    ASSERT_EQ(3, reordered_search_fields.size());
    ASSERT_EQ(3, weighted_search_fields.size());

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("type", weighted_search_fields[1].name);
    ASSERT_EQ("brand", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(14, weighted_search_fields[1].weight);
    ASSERT_EQ(13, weighted_search_fields[2].weight);

    // same weights
    weighted_search_fields.clear();
    reordered_search_fields.clear();
    query_by_weights = {15, 15, 15};

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields,
                                        reordered_search_fields);

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("brand", weighted_search_fields[1].name);
    ASSERT_EQ("type", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(15, weighted_search_fields[1].weight);
    ASSERT_EQ(15, weighted_search_fields[2].weight);

    // same weights large
    weighted_search_fields.clear();
    reordered_search_fields.clear();
    query_by_weights = {800, 800, 800};

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields,
                                        reordered_search_fields);

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("brand", weighted_search_fields[1].name);
    ASSERT_EQ("type", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(15, weighted_search_fields[1].weight);
    ASSERT_EQ(15, weighted_search_fields[2].weight);

    // weights desc ordered but exceed max weight
    weighted_search_fields.clear();
    reordered_search_fields.clear();
    query_by_weights = {603, 602, 601};

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields,
                                        reordered_search_fields);

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("brand", weighted_search_fields[1].name);
    ASSERT_EQ("type", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(14, weighted_search_fields[1].weight);
    ASSERT_EQ(13, weighted_search_fields[2].weight);

    // number of fields > 15 (must cap least important fields to weight 0)
    raw_search_fields.clear();
    weighted_search_fields.clear();
    reordered_search_fields.clear();
    query_by_weights.clear();

    for(size_t i = 0; i < 17; i++) {
        raw_search_fields.push_back("field" + std::to_string(17 - i));
        query_by_weights.push_back(17 - i);
    }

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields,
                                        reordered_search_fields);

    ASSERT_EQ("field3", weighted_search_fields[14].name);
    ASSERT_EQ("field2", weighted_search_fields[15].name);
    ASSERT_EQ("field1", weighted_search_fields[16].name);

    ASSERT_EQ(1, weighted_search_fields[14].weight);
    ASSERT_EQ(0, weighted_search_fields[15].weight);
    ASSERT_EQ(0, weighted_search_fields[16].weight);

    // when weights are not given
    raw_search_fields.clear();
    weighted_search_fields.clear();
    reordered_search_fields.clear();
    query_by_weights.clear();

    for(size_t i = 0; i < 17; i++) {
        raw_search_fields.push_back("field" + std::to_string(17 - i));
    }

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields,
                                        reordered_search_fields);

    ASSERT_EQ("field3", weighted_search_fields[14].name);
    ASSERT_EQ("field2", weighted_search_fields[15].name);
    ASSERT_EQ("field1", weighted_search_fields[16].name);

    ASSERT_EQ(1, weighted_search_fields[14].weight);
    ASSERT_EQ(0, weighted_search_fields[15].weight);
    ASSERT_EQ(0, weighted_search_fields[16].weight);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, SearchingForMinusCharacter) {
    // when the minus character is part of symbols_to_index it should not be used as exclusion operator
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection(
        "coll1", 1, fields, "points", 0, "", {"-"}, {}
    ).get();

    nlohmann::json doc1;
    doc1["name"] = "y = -x + 3 + 2 * x";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    doc1["name"] = "foo bar";
    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("-x + 3", {"name"},
                                 "", {}, {}, {0}, 10,
                                 1, FREQUENCY, {true},
                                 0).get();

    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("-", {"name"},
                            "", {}, {}, {0}, 10,
                            1, FREQUENCY, {true},
                            0).get();

    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, UpsertUpdateEmplaceShouldAllRemoveIndex) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "title1", "type": "string", "optional": true},
          {"name": "title2", "type": "string", "optional": true},
          {"name": "title3", "type": "string", "optional": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "title1": "Foo",
        "title2": "Bar",
        "title3": "Baz",
        "data": "abcdefghijk"
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    // via upsert

    auto doc_update = R"({
        "id": "0",
        "title2": "Bar",
        "title3": "Baz"
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPSERT).ok());

    auto results = coll1->search("foo", {"title1"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll1->search("bar", {"title2"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());

    // via update, existing index should not be removed because update can send partial doc

    doc_update = R"({
        "id": "0",
        "title3": "Baz"
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    results = coll1->search("bar", {"title2"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // via emplace, existing index should not be removed because emplace could send partial doc

    doc_update = R"({
        "id": "0"
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), EMPLACE).ok());

    results = coll1->search("baz", {"title3"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionSpecificMoreTest, UnorderedWeightingOfFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "brand", "type": "string"},
            {"name": "sku", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "42f05db9-373a-4372-9bd0-ff4b5aaba28d";
    doc["brand"] = "brand";
    doc["sku"] = "rgx761";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // with num_typos

    auto res_op = coll1->search("rg0761", {"title","brand","sku"}, "", {}, {}, {2,2,0}, 10, 1,
                                FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                                "<mark>", "</mark>", {10, 7, 10});

    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(0, res_op.get()["hits"].size());

    // with prefix
    res_op = coll1->search("rgx", {"title","brand","sku"}, "", {}, {}, {2,2,0}, 10, 1,
                           FREQUENCY, {true, true, false},
                           0, spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                           "<mark>", "</mark>", {10, 7, 10});

    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(0, res_op.get()["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, IncludeFieldsOnlyId) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "Sample Title";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("*", {}, "", {}, {}, {2}, 10, 1,
                                FREQUENCY, {true}, 0, {"id"});

    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();

    ASSERT_EQ(1, res["hits"][0]["document"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, QueryWithOnlySpecialChars) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "Sample Title";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("--", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true});

    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, HandleStringFieldWithObjectValueEarlier) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": ".*", "type": "auto"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    // index a "bad" document with title as an object field

    nlohmann::json doc;
    doc["id"] = "12345";
    doc["title"] = R"({"id": 12345})"_json;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // now add another document where `title` is a string
    doc["id"] = "12346";
    doc["title"] = "Title 2";
    add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // try to update the former document
    doc["id"] = "12345";
    doc["title"] = "Title 1";
    add_op = coll1->add(doc.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());
}

TEST_F(CollectionSpecificMoreTest, CopyDocHelper) {
    std::vector<highlight_field_t> hightlight_items = {
        highlight_field_t("foo.bar", false, false),
        highlight_field_t("baz", false, false),
        highlight_field_t("not-found", false, false),
    };

    nlohmann::json src = R"({
        "baz": {"name": "John"},
        "foo.bar": 12345
    })"_json;

    nlohmann::json dst;
    Collection::copy_highlight_doc(hightlight_items, src, dst);

    ASSERT_EQ(2, dst.size());
    ASSERT_EQ(1, dst.count("baz"));
    ASSERT_EQ(1, dst.count("foo.bar"));
}

TEST_F(CollectionSpecificMoreTest, HighlightObjectShouldBeEmptyWhenNoHighlightFieldFound) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "brand", "type": "string"},
            {"name": "sku", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "42f05db9-373a-4372-9bd0-ff4b5aaba28d";
    doc["brand"] = "brand";
    doc["sku"] = "rgx761";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("brand", {"title", "brand", "sku"}, "", {}, {}, {2, 2, 0}, 10, 1,
                                FREQUENCY, {true},
                                10, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address",
                                20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false, true,
                                "title");

    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();
    ASSERT_EQ(1, res["hits"].size());

    ASSERT_TRUE(res["hits"][0]["highlight"]["snippet"].empty());
}

TEST_F(CollectionSpecificMoreTest, WildcardSearchWithNoSortingField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    // search on empty collection
    auto res_op = coll1->search("*", {}, "", {}, {}, {2}, 10, 1,
                                FREQUENCY, {true});

    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_EQ(0, res["found"].get<size_t>());

    nlohmann::json doc;
    doc["title"] = "Sample Title 1";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "Sample Title 2";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    res_op = coll1->search("*", {}, "", {}, {}, {2}, 10, 1,
                                FREQUENCY, {true});

    ASSERT_TRUE(res_op.ok());
    res = res_op.get();
    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ(2, res["found"].get<size_t>());

    ASSERT_EQ("1", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", res["hits"][1]["document"]["id"].get<std::string>());
}
