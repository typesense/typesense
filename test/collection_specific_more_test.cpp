#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"
#include "synonym_index.h"
#include "synonym_index_manager.h"

static nlohmann::json find_doc_by_id(const nlohmann::json& results, const std::string& doc_id) {
    for (const auto& hit : results["hits"]) {
        if (hit["document"]["id"].get<std::string>() == doc_id) {
            return hit;
        }
    }
    return nlohmann::json();
}

class CollectionSpecificMoreTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    StemmerManager& stemmerManager = StemmerManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_specific_more";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        stemmerManager.init(store);
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
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("mark b", {"title"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("9", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("8", results["hits"][1]["document"]["id"].get<std::string>());

    results = coll1->search("mark b", {"title"}, "points: < 9", {}, {}, {0}, 100, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("8", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, TypoCorrectionShouldUseMaxCandidates) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    for(size_t i = 0; i < 20; i++) {
        nlohmann::json doc;
        doc["title"] = "Independent" + std::to_string(i);
        doc["points"] = i;
        coll1->add(doc.dump());
    }

    size_t max_candidates = 20;
    auto results = coll1->search("independent", {"title"}, "", {}, {}, {2}, 30, 1, FREQUENCY, {false}, 0,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000*1000, 4, 7,
                                 off, max_candidates).get();

    ASSERT_EQ(20, results["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, PrefixExpansionOnMultiField) {
    Collection *coll1;
    std::vector<field> fields = {field("location", field_types::STRING, false),
                                 field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::string> names = {
        "John Stewart", "John Smith", "John Scott", "John Stone", "John Romero", "John Oliver", "John Adams"
    };

    std::vector<std::string> locations = {
        "Switzerland", "Seoul", "Sydney", "Surat", "Stockholm", "Salem", "Sevilla"
    };

    for(size_t i = 0; i < names.size(); i++) {
        nlohmann::json doc;
        doc["location"] = locations[i];
        doc["name"] = names[i];
        doc["points"] = i;
        coll1->add(doc.dump());
    }

    auto results = coll1->search("john s", {"location", "name"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                                 0, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(), 10, "",
                                 30, 4, "title", 20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false,
                                 true, "", false, 6000*1000, 4, 7, off, 4).get();

    // tokens are ordered by max_score and prefix continuation on the same field is prioritized
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][3]["document"]["id"].get<std::string>());

    // when more than 4 candidates are requested, "s" matches with other fields are returned
    results = coll1->search("john s", {"location", "name"}, "", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                            0, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(), 10, "",
                            30, 4, "title", 20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false,
                            true, "", false, 6000*1000, 4, 7, off, 10).get();

    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][3]["document"]["id"].get<std::string>());
    ASSERT_EQ("6", results["hits"][4]["document"]["id"].get<std::string>());
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

    results = coll1->search("*", {"tags"}, "tags:=§ 23", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, false).get();

    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*", {"tags"}, "tags:=§ 23 Satz", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
                            Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, false).get();

    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, ExactFilteringOnArray2) {
    auto schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "capability", "type": "string[]", "facet": true}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc1;
    doc1["capability"] = {"Encoding capabilities for network communications",
                            "Obfuscation capabilities"};
    coll1->add(doc1.dump());

    auto results = coll1->search("*", {}, "capability:=Encoding capabilities", {}, {}, {0}, 100, 1, MAX_SCORE, {true},
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
    ASSERT_EQ("578730123373578267", results["hits"][0]["text_match_info"]["score"].get<std::string>());
    ASSERT_EQ(3, results["hits"][0]["text_match_info"]["fields_matched"].get<size_t>());
    ASSERT_EQ(2, results["hits"][1]["text_match_info"]["fields_matched"].get<size_t>());
    ASSERT_EQ(1, results["hits"][2]["text_match_info"]["fields_matched"].get<size_t>());

    ASSERT_EQ(1, results["hits"][0]["text_match_info"]["tokens_matched"].get<size_t>());
    ASSERT_EQ(1, results["hits"][1]["text_match_info"]["tokens_matched"].get<size_t>());
    ASSERT_EQ(1, results["hits"][2]["text_match_info"]["tokens_matched"].get<size_t>());

    ASSERT_EQ("1108091342849", results["hits"][0]["text_match_info"]["best_field_score"].get<std::string>());
    ASSERT_EQ("1108091342849", results["hits"][1]["text_match_info"]["best_field_score"].get<std::string>());
    ASSERT_EQ("1108091342849", results["hits"][2]["text_match_info"]["best_field_score"].get<std::string>());

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
      "companies": [
        {
          "title": {
            "matched_tokens": [
              "Rāp"
            ],
            "snippet": "<mark>Rāp</mark>eti Early Learning Centre"
          }
        }
      ],
      "title": {
        "matched_tokens": [
          "Rāp"
        ],
        "snippet": "<mark>Rāp</mark>eti Early Learning Centre"
      }
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());
}

TEST_F(CollectionSpecificMoreTest, FieldWeightNormalization) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),
                                 field("type", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::vector<search_field_t> raw_search_fields = {
        search_field_t("title", "title", 110, 2, true, off),
        search_field_t("brand", "brand", 25, 2, true, off),
        search_field_t("type", "type", 55, 2, true, off),
    };
    std::vector<uint32_t> query_by_weights = {110, 25, 55};
    std::vector<search_field_t> weighted_search_fields;

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields);

    ASSERT_EQ(3, weighted_search_fields.size());

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("type", weighted_search_fields[1].name);
    ASSERT_EQ("brand", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(14, weighted_search_fields[1].weight);
    ASSERT_EQ(13, weighted_search_fields[2].weight);

    // same weights
    weighted_search_fields.clear();
    query_by_weights = {15, 15, 15};
    raw_search_fields = {
        search_field_t{"title", "title", 15, 2, true, off},
        search_field_t{"brand", "brand", 15, 2, true, off},
        search_field_t{"type", "type", 15, 2, true, off},
    };

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields);

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("brand", weighted_search_fields[1].name);
    ASSERT_EQ("type", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(15, weighted_search_fields[1].weight);
    ASSERT_EQ(15, weighted_search_fields[2].weight);

    // same weights large
    weighted_search_fields.clear();
    query_by_weights = {800, 800, 800};
    raw_search_fields = {
        search_field_t{"title", "title", 800, 2, true, off},
        search_field_t{"brand", "brand", 800, 2, true, off},
        search_field_t{"type", "type", 800, 2, true, off},
    };

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields);

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("brand", weighted_search_fields[1].name);
    ASSERT_EQ("type", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(15, weighted_search_fields[1].weight);
    ASSERT_EQ(15, weighted_search_fields[2].weight);

    // weights desc ordered but exceed max weight
    weighted_search_fields.clear();
    query_by_weights = {603, 602, 601};
    raw_search_fields = {
        search_field_t{"title", "title", 603, 2, true, off},
        search_field_t{"brand", "brand", 602, 2, true, off},
        search_field_t{"type", "type", 601, 2, true, off},
    };

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields);

    ASSERT_EQ("title", weighted_search_fields[0].name);
    ASSERT_EQ("brand", weighted_search_fields[1].name);
    ASSERT_EQ("type", weighted_search_fields[2].name);

    ASSERT_EQ(15, weighted_search_fields[0].weight);
    ASSERT_EQ(14, weighted_search_fields[1].weight);
    ASSERT_EQ(13, weighted_search_fields[2].weight);

    // number of fields > 15 (must cap least important fields to weight 0)
    raw_search_fields.clear();
    weighted_search_fields.clear();
    query_by_weights.clear();

    for(size_t i = 0; i < 17; i++) {
        auto fname = "field" + std::to_string(17 - i);
        raw_search_fields.push_back(search_field_t{fname, fname, 17 - i, 2, true, off});
        query_by_weights.push_back(17 - i);
    }

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields);

    ASSERT_EQ("field3", weighted_search_fields[14].name);
    ASSERT_EQ("field2", weighted_search_fields[15].name);
    ASSERT_EQ("field1", weighted_search_fields[16].name);

    ASSERT_EQ(1, weighted_search_fields[14].weight);
    ASSERT_EQ(0, weighted_search_fields[15].weight);
    ASSERT_EQ(0, weighted_search_fields[16].weight);

    // when weights are not given
    raw_search_fields.clear();
    weighted_search_fields.clear();
    query_by_weights.clear();

    for(size_t i = 0; i < 17; i++) {
        auto field_name = "field" + std::to_string(17 - i);
        raw_search_fields.push_back(search_field_t{"field" + std::to_string(17 - i),
                                                   field_name, 0, 2, true, off});
    }

    coll1->process_search_field_weights(raw_search_fields, query_by_weights, weighted_search_fields);

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

TEST_F(CollectionSpecificMoreTest, UpdateWithEmptyArray) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "tags", "type": "string[]"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "tags": ["alpha", "beta", "gamma"]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto doc2 = R"({
        "id": "1",
        "tags": ["one", "two"]
    })"_json;

    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());

    // via update

    auto doc_update = R"({
        "id": "0",
        "tags": []
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    auto results = coll1->search("alpha", {"tags"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // via upsert

    doc_update = R"({
        "id": "1",
        "tags": []
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPSERT).ok());

    results = coll1->search("one", {"tags"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
}

TEST_F(CollectionSpecificMoreTest, UpdateArrayWithNullValue) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "tags", "type": "string[]", "optional": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "tags": ["alpha", "beta", "gamma"]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto doc2 = R"({
        "id": "1",
        "tags": ["one", "two"]
    })"_json;

    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());

    // via update

    auto doc_update = R"({
        "id": "0",
        "tags": null
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    auto results = coll1->search("alpha", {"tags"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // update document with no value (optional field) with a null value
    auto doc3 = R"({
        "id": "2"
    })"_json;

    ASSERT_TRUE(coll1->add(doc3.dump(), CREATE).ok());
    results = coll1->search("alpha", {"tags"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    doc_update = R"({
        "id": "2",
        "tags": null
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    // via upsert

    doc_update = R"({
        "id": "1",
        "tags": null
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPSERT).ok());

    results = coll1->search("one", {"tags"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
}

TEST_F(CollectionSpecificMoreTest, ReplaceArrayElement) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "tags", "type": "string[]"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "tags": ["alpha", "beta", "gamma"]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto doc2 = R"({
        "id": "1",
        "tags": ["one", "two", "three"]
    })"_json;

    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());

    // via update

    auto doc_update = R"({
        "id": "0",
        "tags": ["alpha", "gamma"]
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    auto results = coll1->search("beta", {"tags"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // via upsert

    doc_update = R"({
        "id": "1",
        "tags": ["one", "three"]
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPSERT).ok());

    results = coll1->search("two", {"tags"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
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
        highlight_field_t("foo.bar", false, false, true),
        highlight_field_t("baz", false, false, true),
        highlight_field_t("not-found", false, false, true),
    };

    nlohmann::json src = R"({
        "baz": {"name": "John"},
        "foo.bar": 12345
    })"_json;

    nlohmann::json dst;
    Collection::copy_highlight_doc(hightlight_items, true, src, dst);

    ASSERT_EQ(2, dst.size());
    ASSERT_EQ(1, dst.count("baz"));
    ASSERT_EQ(1, dst.count("foo.bar"));

    // when both nested & flattened forms are present, copy only flat form for collection without nesting enabled
    src = R"({
        "baz": {"name": "John"},
        "baz.name": "John"
    })"_json;
    dst.clear();

    hightlight_items = {
        highlight_field_t("baz.name", false, false, true),
    };

    Collection::copy_highlight_doc(hightlight_items, false, src, dst);
    ASSERT_EQ(1, dst.size());
    ASSERT_EQ(1, dst.count("baz.name"));
}

TEST_F(CollectionSpecificMoreTest, HighlightFieldWithBothFlatAndNestedForm) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name.first", "type": "string"}
        ]
    })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["name.first"] = "John";
    doc["name"]["first"] = "John";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("john", {"name.first"}, "", {}, {}, {2}, 10, 1,
                             FREQUENCY, {true},
                             10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("<mark>John</mark>", res["hits"][0]["highlight"]["name.first"]["snippet"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, HighlightWordWithSymbols) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "var(--icon-secondary-neutral); For components with";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("favicon", {"title"}, "", {}, {}, {2}, 10, 1,
                                FREQUENCY, {true},
                                10, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address",
                                20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false, true,
                                "title").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("<mark>var(--icon</mark>-secondary-neutral); For components with",
              res["hits"][0]["highlight"]["title"]["snippet"].get<std::string>());
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

TEST_F(CollectionSpecificMoreTest, AutoSchemaWithObjectValueAsFirstDoc) {
    // when a value is `object` initially and then is integer, updating that object should not cause errors
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": ".*", "type": "auto"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Sample Title 1";
    doc["num"] = nlohmann::json::object();
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "Sample Title 2";
    doc["num"] = 42;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // now try updating first doc
    doc["id"] = "0";
    doc["title"] = "Sample Title 1";
    doc["num"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump(), UPSERT).ok());

    auto res = coll1->search("*", {}, "num:100", {}, {}, {2}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, VerifyDeletionOfFacetStringIndex) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string", "facet": true},
            {"name": "i32", "type": "int32", "facet": true},
            {"name": "float", "type": "float", "facet": true},
            {"name": "i64", "type": "int64", "facet": true},
            {"name": "i32arr", "type": "int32[]", "facet": true},
            {"name": "floatarr", "type": "float[]", "facet": true},
            {"name": "i64arr", "type": "int64[]", "facet": true}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["i32"] = 100;
    doc["float"] = 2.40;
    doc["i64"] = 10000;
    doc["i32arr"] = {100};
    doc["floatarr"] = {2.50};
    doc["i64arr"] = {10000};

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto search_index = coll1->_get_index()->_get_search_index();
    ASSERT_EQ(7, search_index.size());
    for(const auto& kv: search_index) {
        ASSERT_EQ(1, kv.second->size);
    }

    coll1->remove("0");
    ASSERT_EQ(7, search_index.size());

    for(const auto& kv: search_index) {
        ASSERT_EQ(0, kv.second->size);
    }
}

TEST_F(CollectionSpecificMoreTest, MustExcludeOutOf) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "Sample Title 1";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    spp::sparse_hash_set<std::string> include_fields;
    auto res_op = coll1->search("*", {}, "", {}, {}, {2}, 10, 1,
                                FREQUENCY, {true}, 0, include_fields, {"out_of"});

    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(0, res.count("out_of"));
}

TEST_F(CollectionSpecificMoreTest, ValidateQueryById) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "doc-1";
    doc["title"] = "Sample Title 1";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("doc-1", {"id"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Cannot use `id` as a query by field.", res_op.error());
}

TEST_F(CollectionSpecificMoreTest, ConsiderDroppedTokensDuringTextMatchScoring) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "brand", "type": "string"}
            ]
        })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["brand"] = "Neutrogena";
    doc["name"] = "Neutrogena Ultra Sheer Oil-Free Face Serum With Vitamin E + SPF 60";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["brand"] = "Neutrogena";
    doc["name"] = "Neutrogena Ultra Sheer Liquid Sunscreen SPF 70";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("Neutrogena Ultra Sheer Moisturizing Face Serum", {"brand", "name"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                             "<mark>", "</mark>", {3, 2}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_weight).get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, ConsiderDroppedTokensDuringTextMatchScoring2) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "name", "type": "string"}
            ]
        })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["name"] = "Elizabeth Arden 5th Avenue Eau de Parfum 125ml";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["name"] = "Avène Sun Very High Protection Mineral Cream SPF50+ 50ml";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("avène eau mineral", {"name"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                             "<mark>", "</mark>", {3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_weight).get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ("1", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", res["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, DisableFieldCountForScoring) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "brand", "type": "string"}
            ]
        })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["name"] = "Alpha beta gamma";
    doc["brand"] = "Alpha beta gamma";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["name"] = "Alpha beta gamma";
    doc["brand"] = "Theta";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("beta", {"name", "brand"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                                "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score, 100, 0, 0, 0, "exhaustive", 30000, 2, "",
                                {}, {}, "right_to_left", true);


    auto res = coll1->search("beta", {"name", "brand"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                             "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score, 100, 0, 0, 0, "exhaustive", 30000, 2, "",
                             {}, {}, "right_to_left", false).get();

    size_t score1 = std::stoul(res["hits"][0]["text_match_info"]["score"].get<std::string>());
    size_t score2 = std::stoul(res["hits"][1]["text_match_info"]["score"].get<std::string>());
    ASSERT_TRUE(score1 == score2);

    res = coll1->search("beta", {"name", "brand"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                        "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                        4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score, 100, 0, 0, 0, "exhaustive", 30000, 2, "",
                        {}, {}, "right_to_left", true).get();

    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());

    score1 = std::stoul(res["hits"][0]["text_match_info"]["score"].get<std::string>());
    score2 = std::stoul(res["hits"][1]["text_match_info"]["score"].get<std::string>());
    ASSERT_TRUE(score1 > score2);
}

TEST_F(CollectionSpecificMoreTest, NonNestedFieldNameWithDot) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "category", "type": "string"},
            {"name": "category.lvl0", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["category"] = "Shoes";
    doc["category.lvl0"] = "Shoes";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["category"] = "Mens";
    doc["category.lvl0"] = "Shoes";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("shoes", {"category"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "category", 20, {}, {}, {}, 0,
                             "<mark>", "</mark>", {1}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, IncludeExcludeUnIndexedField) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Sample Title 1";
    doc["src"] = "Internet";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("sample", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0,
                             {"src"}).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["hits"][0]["document"].size());
    ASSERT_EQ("Internet", res["hits"][0]["document"]["src"].get<std::string>());

    // check for exclude field of indexed field

    spp::sparse_hash_set<std::string> include_fields;
    res = coll1->search("sample", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0,
                        include_fields, {"src"}).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(2, res["hits"][0]["document"].size());
    ASSERT_EQ("Sample Title 1", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, WildcardIncludeExclude) {
    nlohmann::json schema = R"({
         "name": "posts",
         "enable_nested_fields": true,
         "fields": [
           {"name": "username", "type": "string", "facet": true},
           {"name": "user.rank", "type": "int32", "facet": true},
           {"name": "user.bio", "type": "string"},
           {"name": "likes", "type": "int32"},
           {"name": "content", "type": "object"}
         ],
         "default_sorting_field": "likes"
       })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    std::vector<std::string> json_lines = {
            R"({"id": "124","username": "user_a","user": {"rank": 100,"bio": "Hi! I'm user_a"},"likes": 5215,"content": {"title": "title 1","body": "body 1"}})",
            R"({"id": "125","username": "user_b","user": {"rank": 50,"bio": "user_b here, nice to meet you!"},"likes": 5215,"content": {"title": "title 2","body": "body 2"}})"
    };

    for (auto const& json: json_lines){
        auto add_op = coll->add(json);
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    // include test: user* matches username, user.bio and user.rank
    auto result = coll->search("user_a", {"username"}, "", {}, {}, {0},
                                        10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD, {"user*"}).get();

    ASSERT_EQ(1, result["found"].get<size_t>());
    ASSERT_EQ(1, result["hits"].size());

    ASSERT_EQ(0, result["hits"][0]["document"].count("id"));
    ASSERT_EQ(0, result["hits"][0]["document"].count("likes"));
    ASSERT_EQ(0, result["hits"][0]["document"].count("content"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("user"));
    ASSERT_EQ(1, result["hits"][0]["document"]["user"].count("bio"));
    ASSERT_EQ(1, result["hits"][0]["document"]["user"].count("rank"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("username"));

    spp::sparse_hash_set<std::string> include_fields;
    // exclude test: user.* matches user.rank and user.bio
    result = coll->search("user_a", {"username"}, "", {}, {}, {0},
                               10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD, include_fields, {"user.*"}).get();

    ASSERT_EQ(1, result["found"].get<size_t>());
    ASSERT_EQ(1, result["hits"].size());

    ASSERT_EQ(1, result["hits"][0]["document"].count("id"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("likes"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("content"));
    ASSERT_EQ(1, result["hits"][0]["document"]["content"].count("title"));
    ASSERT_EQ(1, result["hits"][0]["document"]["content"].count("body"));
    ASSERT_EQ(0, result["hits"][0]["document"].count("user"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("username"));

    // No matching field for include_fields/exclude_fields
    result = coll->search("user_a", {"username"}, "", {}, {}, {0},
                          10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD, {"foo.*"}).get();

    ASSERT_EQ(1, result["found"].get<size_t>());
    ASSERT_EQ(1, result["hits"].size());
    ASSERT_EQ(0, result["hits"][0]["document"].size());

    result = coll->search("user_a", {"username"}, "", {}, {}, {0},
                         10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD, include_fields, {"foo.*"}).get();

    ASSERT_EQ(1, result["found"].get<size_t>());
    ASSERT_EQ(1, result["hits"].size());

    ASSERT_EQ(1, result["hits"][0]["document"].count("id"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("likes"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("content"));
    ASSERT_EQ(1, result["hits"][0]["document"]["content"].count("title"));
    ASSERT_EQ(1, result["hits"][0]["document"]["content"].count("body"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("user"));
    ASSERT_EQ(1, result["hits"][0]["document"]["user"].count("bio"));
    ASSERT_EQ(1, result["hits"][0]["document"]["user"].count("rank"));
    ASSERT_EQ(1, result["hits"][0]["document"].count("username"));
}

TEST_F(CollectionSpecificMoreTest, EmplaceWithNullValue) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "is_valid", "type": "bool", "optional": true}
        ]
    })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["is_valid"] = nullptr;
    ASSERT_TRUE(coll1->add(doc.dump(), EMPLACE).ok());
}

TEST_F(CollectionSpecificMoreTest, PhraseMatchRepeatingTokens) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Super easy super fast product";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "The really easy really fast product really";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search(R"("super easy super fast")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());

    res = coll1->search(R"("super easy super")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());

    res = coll1->search(R"("the really easy really fast product really")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("1", res["hits"][0]["document"]["id"].get<std::string>());

    // these should not match
    res = coll1->search(R"("the easy really really product fast really")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, res["hits"].size());

    res = coll1->search(R"("really the easy really fast product really")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, res["hits"].size());

    res = coll1->search(R"("super super easy fast")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, res["hits"].size());

    res = coll1->search(R"("super super easy")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, res["hits"].size());

    res = coll1->search(R"("product fast")", {"title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, PhraseMatchMultipleFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "author", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "A Walk to the Tide Pools";
    doc["author"] = "Nok Nok";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "Random Title";
    doc["author"] = "Tide Pools";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search(R"("tide pools")", {"title", "author"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, PhraseMatchAcrossArrayElements) {
    nlohmann::json schema = R"({
                "name": "coll1",
                "fields": [
                    {"name": "texts", "type": "string[]"}
                ]
            })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["texts"] = {"state of the", "of the art"};

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search(R"("state of the art)", {"texts"}, "", {}, {}, {0}, 10, 1,
                             FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>()).get();
    ASSERT_EQ(1, res["hits"].size());

    res = coll1->search(R"("state of the art")", {"texts"}, "", {}, {}, {0}, 10, 1,
                        FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>()).get();
    ASSERT_EQ(0, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, WeightTakingPrecendeceOverMatch) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "brand", "type": "string"},
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Healthy Mayo";
    doc["brand"] = "Light Plus";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "Healthy Light Mayo";
    doc["brand"] = "Vegabond";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("light mayo", {"brand", "title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                             "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_weight).get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", res["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ("1108091338753", res["hits"][0]["text_match_info"]["best_field_score"].get<std::string>());
    ASSERT_EQ(15, res["hits"][0]["text_match_info"]["best_field_weight"].get<size_t>());
    ASSERT_EQ(2, res["hits"][0]["text_match_info"]["fields_matched"].get<size_t>());
    ASSERT_EQ(2, res["hits"][0]["text_match_info"]["tokens_matched"].get<size_t>());

    ASSERT_EQ("2211897868289", res["hits"][1]["text_match_info"]["best_field_score"].get<std::string>());
    ASSERT_EQ(14, res["hits"][1]["text_match_info"]["best_field_weight"].get<size_t>());
    ASSERT_EQ(1, res["hits"][1]["text_match_info"]["fields_matched"].get<size_t>());
    ASSERT_EQ(2, res["hits"][1]["text_match_info"]["tokens_matched"].get<size_t>());
}

TEST_F(CollectionSpecificMoreTest, IncrementingCount) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "count", "type": "int32"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    // brand new document: create + upsert + emplace should work

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Foo";
    doc["$operations"]["increment"]["count"] = 1;
    ASSERT_TRUE(coll1->add(doc.dump(), CREATE).ok());

    doc.clear();
    doc["id"] = "1";
    doc["title"] = "Bar";
    doc["$operations"]["increment"]["count"] = 1;
    ASSERT_TRUE(coll1->add(doc.dump(), EMPLACE).ok());

    doc.clear();
    doc["id"] = "2";
    doc["title"] = "Taz";
    doc["$operations"]["increment"]["count"] = 1;
    ASSERT_TRUE(coll1->add(doc.dump(), UPSERT).ok());

    auto res = coll1->search("*", {}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(3, res["hits"].size());
    ASSERT_EQ(1, res["hits"][0]["document"]["count"].get<size_t>());
    ASSERT_EQ(1, res["hits"][1]["document"]["count"].get<size_t>());
    ASSERT_EQ(1, res["hits"][2]["document"]["count"].get<size_t>());

    // should support updates

    doc.clear();
    doc["id"] = "0";
    doc["title"] = "Foo";
    doc["$operations"]["increment"]["count"] = 3;
    ASSERT_TRUE(coll1->add(doc.dump(), UPSERT).ok());

    doc.clear();
    doc["id"] = "1";
    doc["title"] = "Bar";
    doc["$operations"]["increment"]["count"] = 3;
    ASSERT_TRUE(coll1->add(doc.dump(), EMPLACE).ok());

    doc.clear();
    doc["id"] = "2";
    doc["title"] = "Bar";
    doc["$operations"]["increment"]["count"] = 3;
    ASSERT_TRUE(coll1->add(doc.dump(), UPDATE).ok());

    res = coll1->search("*", {}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 5,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(3, res["hits"].size());
    ASSERT_EQ(4, res["hits"][0]["document"]["count"].get<size_t>());
    ASSERT_EQ(4, res["hits"][1]["document"]["count"].get<size_t>());
    ASSERT_EQ(4, res["hits"][2]["document"]["count"].get<size_t>());
}

TEST_F(CollectionSpecificMoreTest, HighlightOnFieldNameWithDot) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "org.title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["org.title"] = "Infinity Inc.";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("infinity", {"org.title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(1, res["hits"][0]["highlights"].size());
    ASSERT_EQ("<mark>Infinity</mark> Inc.", res["hits"][0]["highlights"][0]["snippet"].get<std::string>());

    nlohmann::json highlight = R"({"org.title":{"matched_tokens":["Infinity"],"snippet":"<mark>Infinity</mark> Inc."}})"_json;
    ASSERT_EQ(highlight.dump(), res["hits"][0]["highlight"].dump());

    // even if nested fields enabled, plain field names with dots should work fine

    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
        "fields": [
            {"name": "org.title", "type": "string"}
        ]
    })"_json;

    Collection* coll2 = collectionManager.create_collection(schema).get();
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    res = coll2->search("infinity", {"org.title"}, "", {}, {}, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(0, res["hits"][0]["highlights"].size());

    highlight = R"({"org.title":{"matched_tokens":["Infinity"],"snippet":"<mark>Infinity</mark> Inc."}})"_json;
    ASSERT_EQ(highlight.dump(), res["hits"][0]["highlight"].dump());
}

TEST_F(CollectionSpecificMoreTest, SearchCutoffTest) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "desc", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    for(size_t i = 0; i < 70 * 1000; i++) {
        nlohmann::json doc;
        doc["title"] = "foobarbaz1";
        doc["desc"] = "2";
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto coll_op = coll1->search("foobarbar1 2", {"title", "desc"}, "", {}, {}, {2}, 3, 1, FREQUENCY, {false}, 5,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 1);

    ASSERT_TRUE(coll_op.ok());
    ASSERT_TRUE(coll_op.get()["search_cutoff"]);
}

TEST_F(CollectionSpecificMoreTest, ExhaustiveSearchWithoutExplicitDropTokens) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "alpha beta gamma";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "alpha";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    bool exhaustive_search = true;
    size_t drop_tokens_threshold = 1;

    auto res = coll1->search("alpha beta", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, drop_tokens_threshold,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", exhaustive_search).get();

    ASSERT_EQ(2, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, DropTokensLeftToRightFirst) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "alpha beta";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "beta gamma";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    bool exhaustive_search = false;
    size_t drop_tokens_threshold = 1;

    auto res = coll1->search("alpha beta gamma", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, drop_tokens_threshold,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                             "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                             4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                             0, "exhaustive", 30000, 2, "", {}, {}, "left_to_right").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("1", res["hits"][0]["document"]["id"].get<std::string>());

    res = coll1->search("alpha beta gamma", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, drop_tokens_threshold,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                        "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                        4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                        0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left").get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());

    // search on both sides
    res = coll1->search("alpha gamma", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, drop_tokens_threshold,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                        "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                        4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                        0, "exhaustive", 30000, 2, "", {}, {}, "both_sides:3").get();
    ASSERT_EQ(2, res["hits"].size());

    // but must follow token limit
    res = coll1->search("alpha gamma", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, drop_tokens_threshold,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                        "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                        4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                        0, "exhaustive", 30000, 2, "", {}, {}, "both_sides:1").get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());

    // validation checks
    auto res_op = coll1->search("alpha gamma", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, drop_tokens_threshold,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                                0, "exhaustive", 30000, 2, "", {}, {}, "all_sides");
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Invalid format for drop tokens mode.", res_op.error());

    res_op = coll1->search("alpha gamma", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, drop_tokens_threshold,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                           4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                           0, "exhaustive", 30000, 2, "", {}, {}, "both_sides:x");
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Invalid format for drop tokens mode.", res_op.error());
}

TEST_F(CollectionSpecificMoreTest, DoNotHighlightFieldsForSpecialCharacterQuery) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "description", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "alpha beta gamma";
    doc["description"] = "alpha beta gamma";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("'", {"title", "description"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {false}, 1,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(0, res["hits"][0]["highlight"].size());
    ASSERT_EQ(0, res["hits"][0]["highlights"].size());
}

TEST_F(CollectionSpecificMoreTest, SearchForURL) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "url", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["url"] = "https://www.cpf.gov.sg/member/infohub/cpf-clarifies/policy-faqs/"
                 "why-interest-earned-on-cpf-life-premium-not-paid-to-beneficiaries";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("https://www.cpf.gov.sg/member/infohub/cpf-clarifies/policy-faqs/"
                             "why-interest-earned-on-cpf-life-premium-not-paid-to-beneficiaries", {"url"}, "",
                             {}, {}, {2}, 3, 1,
                             FREQUENCY, {true}).get();

    ASSERT_EQ(1, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, CrossFieldTypoAndPrefixWithWeights) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "color", "type": "string"}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Cool trousers";
    doc["color"] = "blue";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("trouzers", {"title", "color"}, "", {}, {}, {2, 0}, 10, 1, FREQUENCY, {true}, 0,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                             "<mark>", "</mark>", {2, 3}).get();
    ASSERT_EQ(1, res["hits"].size());

    res = coll1->search("trou", {"title", "color"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true, false}, 0,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                        "<mark>", "</mark>", {2, 3}).get();
    ASSERT_EQ(1, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, AnalyticsFullFirstQuery) {
    Config::get_instance().set_enable_search_analytics(true);
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "color", "type": "string"}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Cool cotton trousers";
    doc["color"] = "blue";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("co", {"title", "color"}, "", {}, {}, {2, 0}, 10, 1, FREQUENCY, {true}, 0,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                             "<mark>", "</mark>", {2, 3}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("cool", res["request_params"]["first_q"].get<std::string>());

    res = coll1->search("cool pants", {"title", "color"}, "", {}, {}, {2, 0}, 10, 1, FREQUENCY, {true}, 1,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 40, {}, {}, {}, 0,
                        "<mark>", "</mark>", {2, 3}).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("cool pants", res["request_params"]["first_q"].get<std::string>());

    Config::get_instance().set_enable_search_analytics(false);
}

TEST_F(CollectionSpecificMoreTest, TruncateAterTopK) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    for(auto i = -10; i < 5; i++) {
        nlohmann::json doc;
        doc["title"] = std::to_string(i);
        doc["points"] = i;
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    for(auto i = 0; i < 5; i++) {
        nlohmann::json doc;
        doc["title"] = std::to_string(10 + i);
        doc["points"] = i;
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

/*     Values   Doc ids
        -10       0
        -9        1
        -8        2
        -7        3
        -6        4
        -5        5
        -4        6
        -3        7
        -2        8
        -1        9
        0         10, 15
        1         11, 16
        2         12, 17
        3         13, 18
        4         14, 19
 */

    auto results = coll1->search("*", {"*"}, "", {}, {}, {0}).get();
    ASSERT_EQ(20, results["found"]);

    coll1->truncate_after_top_k("points", 15);

    results = coll1->search("*", {"*"}, "", {}, {}, {0}).get();
    ASSERT_EQ(15, results["found"]);

    std::vector<std::string> ids = {"19", "18", "17", "16", "15", "14", "13", "12", "11", "10", "9", "8", "7", "6", "5"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        ASSERT_EQ(ids[i], results["hits"][i]["document"]["id"]);
    }

    coll1->truncate_after_top_k("points", 11);

    results = coll1->search("*", {"*"}, "", {}, {}, {0}).get();
    ASSERT_EQ(11, results["found"]);

    ids = {"19", "18", "17", "16", "15", "14", "13", "12", "11", "10", "9"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        ASSERT_EQ(ids[i], results["hits"][i]["document"]["id"]);
    }

    coll1->truncate_after_top_k("points", 5);

    results = coll1->search("*", {"*"}, "", {}, {}, {0}).get();
    ASSERT_EQ(5, results["found"]);

    ids = {"19", "18", "14", "13", "12"};
    for(size_t i = 0; i < results["hits"].size(); i++) {
        ASSERT_EQ(ids[i], results["hits"][i]["document"]["id"]);
    }
}

TEST_F(CollectionSpecificMoreTest, HybridSearchTextMatchInfo) {
    auto schema_json =
            R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "product_name", "type": "string", "infix": true},
                    {"name": "product_description", "type": "string"},
                    {"name": "embedding", "type":"float[]", "embed":{"from": ["product_description"], "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;
    std::vector<nlohmann::json> documents = {
            R"({
                "product_id": "product_a",
                "product_name": "shampoo",
                "product_description": "Our new moisturizing shampoo is perfect for those with dry or damaged hair."
            })"_json,
            R"({
                "product_id": "product_b",
                "product_name": "soap",
                "product_description": "Introducing our all-natural, organic soap bar made with essential oils and botanical ingredients."
            })"_json
    };

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    for (auto const &json: documents) {
        auto add_op = collection_create_op.get()->add(json.dump());
        ASSERT_TRUE(add_op.ok());
    }

    auto coll1 = collection_create_op.get();
    auto results = coll1->search("natural products", {"product_name", "embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(2, results["hits"].size());

    // It's a hybrid search with only vector match
    ASSERT_EQ("0", results["hits"][0]["text_match_info"]["score"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["text_match_info"]["score"].get<std::string>());

    ASSERT_EQ(0, results["hits"][0]["text_match_info"]["fields_matched"].get<size_t>());
    ASSERT_EQ(0, results["hits"][1]["text_match_info"]["fields_matched"].get<size_t>());

    ASSERT_EQ(0, results["hits"][0]["text_match_info"]["tokens_matched"].get<size_t>());
    ASSERT_EQ(0, results["hits"][1]["text_match_info"]["tokens_matched"].get<size_t>());
}

TEST_F(CollectionSpecificMoreTest, DisableTyposForNumericalTokens) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ],
         "token_separators": ["-"]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "XYZ-12345678";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "XYZ-22345678";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // with disabling typos for numerical tokens

    auto res_op = coll1->search("XYZ-12345678", {"title"}, "", {},
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
                                "", false);

    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["hits"].size());

    res_op = coll1->search("XYZ-12345678", {"title"}, "", {},
                                {}, {2}, 10, 1,FREQUENCY, {true},
                                Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "",
                                30, 4, "", 400);

    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(2, res_op.get()["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, DisableHighlightForLongFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "description", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::string description;
    for(size_t i = 0; i < 70*1000; i++) {
        description += StringUtils::randstring(4) + " ";
    }

    description += "foobar";

    nlohmann::json doc;
    doc["description"] = description;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("foobar", {"description"}, "", {},
                                {}, {2}, 10, 1,FREQUENCY, {true},
                                Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "");

    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["hits"].size());
    ASSERT_EQ(0, res_op.get()["hits"][0]["highlight"].size());

    // if token is found within first 64K offsets, we will highlight
    description = "";
    for(size_t i = 0; i < 1000; i++) {
        description += StringUtils::randstring(4) + " ";
    }

    description += " bazinga ";

    for(size_t i = 0; i < 70*1000; i++) {
        description += StringUtils::randstring(4) + " ";
    }

    doc["description"] = description;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    res_op = coll1->search("bazinga", {"description"}, "", {},
                            {}, {2}, 10, 1,FREQUENCY, {true},
                            Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "");

    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["hits"].size());
    ASSERT_EQ(1, res_op.get()["hits"][0]["highlight"].size());
}

TEST_F(CollectionSpecificMoreTest, StemmingEnglish) {
    nlohmann::json schema = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string", "stem": true}
        ]
    })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());

    auto coll_stem = coll_stem_res.get();

    schema = R"({
        "name": "test2",
        "fields": [
            {"name": "name", "type": "string", "stem": false}
        ]
    })"_json;

    auto coll_no_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_no_stem_res.ok());
    auto coll_no_stem = coll_no_stem_res.get();

    nlohmann::json doc;
    doc["name"] = "running";

    ASSERT_TRUE(coll_stem->add(doc.dump()).ok());
    ASSERT_TRUE(coll_no_stem->add(doc.dump()).ok());

    auto stem_res = coll_stem->search("run", {"name"}, {}, {}, {}, {0}, 10, 1, FREQUENCY, {false}, 1);
    ASSERT_TRUE(stem_res.ok());
    ASSERT_EQ(1, stem_res.get()["hits"].size());
    ASSERT_EQ("running", stem_res.get()["hits"][0]["highlight"]["name"]["matched_tokens"][0].get<std::string>());
    ASSERT_EQ("<mark>running</mark>", stem_res.get()["hits"][0]["highlight"]["name"]["snippet"].get<std::string>());

    auto no_stem_res = coll_no_stem->search("run", {"name"}, {}, {}, {}, {0}, 10, 1, FREQUENCY, {false}, 1);
    ASSERT_TRUE(no_stem_res.ok());
    ASSERT_EQ(0, no_stem_res.get()["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, StemmingEnglishWithCaps) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {
                "facet": false,
                "index": true,
                "infix": true,
                "locale": "",
                "name": "name",
                "optional": false,
                "sort": false,
                "stem": false,
                "store": true,
                "type": "string"
            },
            {
                "facet": true,
                "index": true,
                "infix": true,
                "locale": "",
                "name": "subClass",
                "optional": true,
                "sort": false,
                "stem": true,
                "store": true,
                "type": "string"
            }
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["name"] = "Onion Coo Usa";
    doc["subClass"] = "ONIONS";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["name"] = "Mccormick Onion Dip Mix";
    doc["subClass"] = "GRAVY/SAUCE PACKETS";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    std::vector<sort_by> sort_fields = {};

    auto res = coll1->search("onions", {"subClass","name"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_STREQ("0", res["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", res["hits"][1]["document"]["id"].get<std::string>().c_str());
}

TEST_F(CollectionSpecificMoreTest, StemmingEnglishPrefixHighlight) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {
                "facet": false,
                "index": true,
                "infix": true,
                "locale": "",
                "name": "name",
                "optional": false,
                "sort": false,
                "stem": false,
                "store": true,
                "type": "string"
            },
            {
                "facet": true,
                "index": true,
                "infix": true,
                "locale": "",
                "name": "subClass",
                "optional": true,
                "sort": false,
                "stem": true,
                "store": true,
                "type": "string"
            }
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["name"] = "Generic Red Onions";
    doc["subClass"] = "ONIONS";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    std::vector<sort_by> sort_fields = {};
    auto res = coll1->search("onions", {"subClass","name"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("Generic Red <mark>Onions</mark>", res["hits"][0]["highlight"]["name"]["snippet"].get<std::string>());
    ASSERT_EQ("<mark>ONIONS</mark>", res["hits"][0]["highlight"]["subClass"]["snippet"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, StemmingEnglishHighlights) {
    nlohmann::json schema = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string", "stem": true}
        ]
    })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());

    auto coll_stem = coll_stem_res.get();

    nlohmann::json doc;
    doc["name"] = "Running runs";

    ASSERT_TRUE(coll_stem->add(doc.dump()).ok());

    auto res = coll_stem->search("run", {"name"}, {}, {}, {}, {0}, 10, 1, FREQUENCY, {false}, 1).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ(2, res["hits"][0]["highlight"]["name"]["matched_tokens"].size());
    ASSERT_EQ("Running", res["hits"][0]["highlight"]["name"]["matched_tokens"][0].get<std::string>());
    ASSERT_EQ("runs", res["hits"][0]["highlight"]["name"]["matched_tokens"][1].get<std::string>());
    ASSERT_EQ("<mark>Running</mark> <mark>runs</mark>", res["hits"][0]["highlight"]["name"]["snippet"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, StemmingCyrilic) {
    nlohmann::json schema = R"({
         "name": "words",
         "fields": [
           {"name": "word", "type": "string", "stem": true, "locale": "ru"}
         ]
       })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());
    auto coll_stem = coll_stem_res.get();

    ASSERT_TRUE(coll_stem->add(R"({"word": "доверенное"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "доверенные"})"_json.dump()).ok());

    auto res = coll_stem->search("доверенное", {"word"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(2, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, NumDroppedTokensTest) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "alpha beta";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "beta gamma";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "gamma delta";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "delta epsilon";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "epsilon alpha";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    bool exhaustive_search = false;
    size_t drop_tokens_threshold = 5;

    auto res = coll1->search("alpha zeta gamma", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true},
                             drop_tokens_threshold).get();

    ASSERT_EQ(4, res["hits"].size());
    ASSERT_EQ("4", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("epsilon alpha", res["hits"][0]["document"]["title"]);
    ASSERT_EQ(2, res["hits"][0]["text_match_info"]["num_tokens_dropped"]);

    ASSERT_EQ("2", res["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("gamma delta", res["hits"][1]["document"]["title"]);
    ASSERT_EQ(2, res["hits"][1]["text_match_info"]["num_tokens_dropped"]);

    ASSERT_EQ("1", res["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("beta gamma", res["hits"][2]["document"]["title"]);
    ASSERT_EQ(2, res["hits"][2]["text_match_info"]["num_tokens_dropped"]);

    ASSERT_EQ("0", res["hits"][3]["document"]["id"].get<std::string>());
    ASSERT_EQ("alpha beta", res["hits"][3]["document"]["title"]);
    ASSERT_EQ(2, res["hits"][3]["text_match_info"]["num_tokens_dropped"]);

    res = coll1->search("zeta theta epsilon", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true},
                        drop_tokens_threshold).get();

    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ("4", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("epsilon alpha", res["hits"][0]["document"]["title"]);
    ASSERT_EQ(2, res["hits"][0]["text_match_info"]["num_tokens_dropped"]);

    ASSERT_EQ("3", res["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("delta epsilon", res["hits"][1]["document"]["title"]);
    ASSERT_EQ(2, res["hits"][1]["text_match_info"]["num_tokens_dropped"]);

    drop_tokens_threshold = 1;
    res = coll1->search("alpha beta gamma", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true},
                        drop_tokens_threshold).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("alpha beta", res["hits"][0]["document"]["title"]);
    ASSERT_EQ(1, res["hits"][0]["text_match_info"]["num_tokens_dropped"]);
}

TEST_F(CollectionSpecificMoreTest, TestStemming2) {
    nlohmann::json schema = R"({
         "name": "words",
         "fields": [
           {"name": "word", "type": "string", "stem": true }
         ]
       })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());
    auto coll_stem = coll_stem_res.get();

    ASSERT_TRUE(coll_stem->add(R"({"word": "Walk"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Walks"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Walked"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Walking"})"_json.dump()).ok()); 
    ASSERT_TRUE(coll_stem->add(R"({"word": "Walkings"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Walker"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Walkers"})"_json.dump()).ok());

    auto res = coll_stem->search("Walking", {"word"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(7, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, TestStemmingWithSynonym) {
    SynonymIndexManager& manager = SynonymIndexManager::get_instance();
    manager.init_store(store);
    manager.add_synonym_index("index");

    nlohmann::json schema = R"({
         "name": "words",
         "fields": [
           {"name": "word", "type": "string", "stem": true }
         ],
         "synonym_sets": ["index"]
       })"_json;
    
    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());

    auto coll_stem = coll_stem_res.get();
    
    nlohmann::json synonym_json = R"(
        {
            "id": "id-1",
            "synonyms": ["making", "foobar"]
        }
    )"_json;
    LOG(INFO) << "Adding synonym...";
    auto synonym_op = manager.upsert_synonym_item("index", synonym_json);
    LOG(INFO) << "Synonym added...";
    ASSERT_TRUE(synonym_op.ok());

    ASSERT_TRUE(coll_stem->add(R"({"word": "foobar"})"_json.dump()).ok());

    auto res = coll_stem->search("making", {"word"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("foobar", res["hits"][0]["document"]["word"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, EnsureNoDoubleStemming) {
    nlohmann::json schema = R"({
         "name": "words",
         "fields": [
           {"name": "word", "type": "string", "stem": true }
         ]
       })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());

    auto coll_stem = coll_stem_res.get();
    ASSERT_TRUE(coll_stem->add(R"({"word": "oringer foobar"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "ori foobar"})"_json.dump()).ok());

    auto res = coll_stem->search("oringer", {"word"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("oringer foobar", res["hits"][0]["document"]["word"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, TestFieldStore) {
    nlohmann::json schema = R"({
         "name": "words",
         "fields": [
           {"name": "word_to_store", "type": "string", "store": true },
           {"name": "word_not_to_store", "type": "string", "store": false }
         ]
    })"_json;

    auto coll_store_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_store_res.ok());

    auto coll_store = coll_store_res.get();

    nlohmann::json doc;
    doc["word_to_store"] = "store";
    doc["word_not_to_store"] = "not store";
    ASSERT_TRUE(coll_store->add(doc.dump()).ok());

    auto res = coll_store->search("*", {}, {}, {}, {}, {0}, 10, 1, FREQUENCY, {false}, 1);
    ASSERT_TRUE(res.ok());

    ASSERT_EQ(1, res.get()["hits"].size());
    ASSERT_EQ("store", res.get()["hits"][0]["document"]["word_to_store"].get<std::string>());
    ASSERT_TRUE(res.get()["hits"][0]["document"].count("word_not_to_store") == 0);
}

TEST_F(CollectionSpecificMoreTest, EnableTyposForAlphaNumericalTokens) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ],
        "symbols_to_index":["/"]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "c-136/14";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "13/14";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "(136)214";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "c136/14";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["title"] = "A-136/14";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    bool enable_typos_for_alpha_numerical_tokens = false;

    auto res = coll1->search("c-136/14", {"title"}, "", {},
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
                           "", true, true, false, false, 0, true,
                           enable_typos_for_alpha_numerical_tokens).get();

    ASSERT_EQ(2, res["hits"].size());

    ASSERT_EQ("c136/14", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ("c-136/14", res["hits"][1]["document"]["title"].get<std::string>());

    enable_typos_for_alpha_numerical_tokens = true;

    res = coll1->search("c-136/14", {"title"}, "", {},
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
                        "", true, true, false, 0, true,
                        enable_typos_for_alpha_numerical_tokens).get();

    ASSERT_EQ(5, res["hits"].size());

    ASSERT_EQ("c136/14", res["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ("c-136/14", res["hits"][1]["document"]["title"].get<std::string>());
    ASSERT_EQ("A-136/14", res["hits"][2]["document"]["title"].get<std::string>());
    ASSERT_EQ("(136)214", res["hits"][3]["document"]["title"].get<std::string>());
    ASSERT_EQ("13/14", res["hits"][4]["document"]["title"].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, PopulateIncludeExcludeFields) {
    auto schema_json =
            R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "product_name", "type": "string", "infix": true},
                    {"name": "product_description", "type": "string"},
                    {"name": "product_embedding", "type":"float[]", "embed":{"from": ["product_description"], "model_config": {"model_name": "ts/e5-small"}}},
                    {"name": "rating", "type": "int32"}
                ]
            })"_json;
    std::vector<nlohmann::json> documents = {
            R"({
                "product_id": "product_a",
                "product_name": "shampoo",
                "product_description": "Our new moisturizing shampoo is perfect for those with dry or damaged hair.",
                "rating": "2"
            })"_json,
            R"({
                "product_id": "product_b",
                "product_name": "soap",
                "product_description": "Introducing our all-natural, organic soap bar made with essential oils and botanical ingredients.",
                "rating": "4"
            })"_json
    };

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    for (auto const &json: documents) {
        auto add_op = collection_create_op.get()->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    auto products_coll = collection_create_op.get();

    spp::sparse_hash_set<std::string> input_include_fields;
    spp::sparse_hash_set<std::string> input_exclude_fields;
    tsl::htrie_set<char> output_include_fields;
    tsl::htrie_set<char> output_exclude_fields;

    input_include_fields = {"product_*"};
    products_coll->populate_include_exclude_fields_lk(input_include_fields, input_exclude_fields, output_include_fields,
                                                      output_exclude_fields);
    std::vector<std::string> expected = {"product_id", "product_name", "product_description"};

    for (const auto& item: expected) {
        ASSERT_EQ(1, output_include_fields.count(item));
    }

    input_include_fields = {"product_*"};
    products_coll->populate_include_exclude_fields_lk(input_include_fields, input_exclude_fields, output_include_fields,
                                                      output_exclude_fields);
    expected = {"product_id", "product_name", "product_description", "product_embedding"};
    for (const auto& item: expected) {
        ASSERT_EQ(1, output_include_fields.count(item));
    }
}

TEST_F(CollectionSpecificMoreTest, IgnoreMissingQueryByFields) {
    nlohmann::json schema = R"({
                "name": "test",
                "enable_nested_fields": true,
                "fields": [
                    {
                        "name": "parts",
                        "type": "object"
                    }
                ]
                })"_json;

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "parts": {
            "1": {"id": "foo", "price": 10},
            "2": {"id": "bar", "price": 15},
            "3": {"id": "zip", "price": 20}
        }
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    bool validate_field_names = true;

    auto res_op = coll->search("foo", {"parts.10"}, "", {},
                            {}, {2}, 10, 1,FREQUENCY, {true},
                            Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                            {"embedding"}, 10, "",
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
                            "", true, true, false, 0, true,
                            true, DEFAULT_FILTER_BY_CANDIDATES, false, validate_field_names);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `parts.10` in the schema.", res_op.error());

    validate_field_names = false;

    res_op = coll->search("foo", {"parts.10"}, "", {},
                               {}, {2}, 10, 1,FREQUENCY, {true},
                               Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                               {"embedding"}, 10, "",
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
                               "", true, true, false, false, 0, true,
                               true, DEFAULT_FILTER_BY_CANDIDATES, false, validate_field_names);

    ASSERT_TRUE(res_op.ok());

    auto res = res_op.get();
    ASSERT_EQ(0, res["hits"].size());
    ASSERT_EQ(0, res["found"].get<size_t>());
}

TEST_F(CollectionSpecificMoreTest, CheckForSchemaAlterStatus) {
    nlohmann::json schema = R"({
                "name": "test",
                "enable_nested_fields": true,
                "fields": [
                    {
                        "name": "parts",
                        "type": "object"
                    }
                ]
                })"_json;

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();
    auto sop = coll->get_alter_schema_status();
    ASSERT_FALSE(sop.ok());  // no alter in progress
    ASSERT_EQ("No active alter operation running.", sop.error());  // no alter in progress
}

TEST_F(CollectionSpecificMoreTest, StemmingDictionary) {
    nlohmann::json schema = R"({
         "name": "titles",
         "fields": [
           {"name": "title", "type": "string", "stem_dictionary": "set1" }
         ]
       })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());

    auto coll_stem = coll_stem_res.get();

    std::string json_line = "{\"word\": \"people\", \"root\":\"person\"}";
    std::vector<std::string> json_lines;
    json_lines.push_back(json_line);

    ASSERT_TRUE(stemmerManager.upsert_stemming_dictionary("set1", json_lines).ok());

    schema = R"({
         "name": "titles_no_stem",
         "fields": [
           {"name": "title", "type": "string" }
         ]
       })"_json;

    auto coll_no_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_no_stem_res.ok());

    auto coll_no_stem = coll_no_stem_res.get();


    nlohmann::json doc;
    doc["title"] = "people of america";
    ASSERT_TRUE(coll_stem->add(doc.dump()).ok());

    auto results = coll_stem->search("person", {"title"}, "", {}, sort_fields, {0}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("people of america", results["hits"][0]["document"]["title"].get<std::string>());

    results = coll_no_stem->search("person", {"title"}, "", {}, sort_fields, {0}).get();
    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, StemmingDictionaryBasics) {
    stemmerManager.delete_all_stemming_dictionaries();

    std::string json_line = "{\"word\": \"people\", \"root\":\"person\"}";
    std::vector<std::string> json_lines;
    json_lines.push_back(json_line);

    //add dictionary set
    ASSERT_TRUE(stemmerManager.upsert_stemming_dictionary("set1", json_lines).ok());

    //get dictionary set
    nlohmann::json dictionary;
    ASSERT_TRUE(stemmerManager.get_stemming_dictionary("set1", dictionary));
    ASSERT_EQ("set1", dictionary["id"]);
    ASSERT_EQ(1, dictionary["words"].size());
    ASSERT_EQ("people", dictionary["words"][0]["word"]);
    ASSERT_EQ("person", dictionary["words"][0]["root"]);

    //add another dictionary set and get
    json_lines.clear();
    json_line = "{\"word\": \"qualities\", \"root\":\"quality\"}";
    json_lines.push_back(json_line);
    ASSERT_TRUE(stemmerManager.upsert_stemming_dictionary("set2", json_lines).ok());

    ASSERT_TRUE(stemmerManager.get_stemming_dictionary("set2", dictionary));
    ASSERT_EQ("set2", dictionary["id"]);
    ASSERT_EQ(1, dictionary["words"].size());
    ASSERT_EQ("qualities", dictionary["words"][0]["word"]);
    ASSERT_EQ("quality", dictionary["words"][0]["root"]);

    //add another line to same set and get
    json_lines.clear();
    json_line = "{\"word\": \"mangoes\", \"root\":\"mango\"}";
    json_lines.push_back(json_line);
    ASSERT_TRUE(stemmerManager.upsert_stemming_dictionary("set2", json_lines).ok());

    ASSERT_TRUE(stemmerManager.get_stemming_dictionary("set2", dictionary));
    ASSERT_EQ("set2", dictionary["id"]);
    ASSERT_EQ(2, dictionary["words"].size());
    ASSERT_EQ("qualities", dictionary["words"][0]["word"]);
    ASSERT_EQ("quality", dictionary["words"][0]["root"]);
    ASSERT_EQ("mangoes", dictionary["words"][1]["word"]);
    ASSERT_EQ("mango", dictionary["words"][1]["root"]);

    //get all dictionary sets
    nlohmann::json dictionary_sets;
    stemmerManager.get_stemming_dictionaries(dictionary_sets);
    ASSERT_EQ(2, dictionary_sets["dictionaries"].size());
    ASSERT_EQ("set1", dictionary_sets["dictionaries"][0].get<std::string>());
    ASSERT_EQ("set2", dictionary_sets["dictionaries"][1].get<std::string>());

    //del dictionary set and get
    dictionary_sets.clear();
    stemmerManager.del_stemming_dictionary("set2");
    stemmerManager.get_stemming_dictionaries(dictionary_sets);
    ASSERT_EQ(1, dictionary_sets["dictionaries"].size());
    ASSERT_EQ("set1", dictionary_sets["dictionaries"][0].get<std::string>());
}

TEST_F(CollectionSpecificMoreTest, StemmingDictionaryEmpty) {
    stemmerManager.delete_all_stemming_dictionaries();
    nlohmann::json dictionary_sets;
    stemmerManager.get_stemming_dictionaries(dictionary_sets);
    ASSERT_EQ(0, dictionary_sets["dictionaries"].size());
    ASSERT_TRUE(dictionary_sets["dictionaries"].is_array());
}

TEST_F(CollectionSpecificMoreTest, ReloadStemmingDictionaryOnRestart) {
    stemmerManager.delete_all_stemming_dictionaries();

    std::string json_line = "{\"word\": \"people\", \"root\":\"person\"}";
    std::vector<std::string> json_lines;
    json_lines.push_back(json_line);

    //add dictionary set
    ASSERT_TRUE(stemmerManager.upsert_stemming_dictionary("set1", json_lines).ok());

    //get dictionary set
    nlohmann::json dictionary;
    ASSERT_TRUE(stemmerManager.get_stemming_dictionary("set1", dictionary));
    ASSERT_EQ("set1", dictionary["id"]);
    ASSERT_EQ(1, dictionary["words"].size());
    ASSERT_EQ("people", dictionary["words"][0]["word"]);
    ASSERT_EQ("person", dictionary["words"][0]["root"]);

    //dispose collection manager and reload all stemming dictionaries
    collectionManager.dispose();
    stemmerManager.dispose();
    delete store;

    std::string state_dir_path = "/tmp/typesense_test/collection_specific_more";
    store = new Store(state_dir_path);

    stemmerManager.init(store);
    collectionManager.init(store, 1.0, "auth_key", quit);
    collectionManager.load(8, 1000);

    ASSERT_TRUE(stemmerManager.get_stemming_dictionary("set1", dictionary));
    ASSERT_EQ("set1", dictionary["id"]);
    ASSERT_EQ(1, dictionary["words"].size());
    ASSERT_EQ("people", dictionary["words"][0]["word"]);
    ASSERT_EQ("person", dictionary["words"][0]["root"]);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, StemmingNonCyrilic) {
    nlohmann::json schema = R"({
         "name": "swedish_words",
         "fields": [
           {"name": "word", "type": "string", "stem": true, "locale": "sv"}
         ]
       })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());
    auto coll_stem = coll_stem_res.get();

    ASSERT_TRUE(coll_stem->add(R"({"word": "Tomat"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Tomater"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Tomatsoppa"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Ost"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Osten"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Ostar"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"word": "Ostsås"})"_json.dump()).ok());

    auto res = coll_stem->search("Tomater", {"word"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(2, res["hits"].size());
    ASSERT_EQ("Tomater", res["hits"][0]["document"]["word"].get<std::string>());
    ASSERT_EQ("Tomatsoppa", res["hits"][1]["document"]["word"].get<std::string>());

    res = coll_stem->search("tomat", {"word"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(3, res["hits"].size());
    ASSERT_EQ("Tomat", res["hits"][0]["document"]["word"].get<std::string>());
    ASSERT_EQ("Tomatsoppa", res["hits"][1]["document"]["word"].get<std::string>());
    ASSERT_EQ("Tomater", res["hits"][2]["document"]["word"].get<std::string>());

    res = coll_stem->search("Ostar", {"word"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(4, res["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, StemmingPhraseSearch) {
    nlohmann::json schema = R"({
         "name": "titles",
         "fields": [
           {"name": "title", "type": "string", "stem_dictionary": "set1"}
         ]
       })"_json;

    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());

    auto coll1 = coll_stem_res.get();

    std::string json_line = "{\"word\": \"achievements\", \"root\":\"achievement\"}";
    std::vector<std::string> json_lines;
    json_lines.push_back(json_line);

    ASSERT_TRUE(stemmerManager.upsert_stemming_dictionary("set1", json_lines).ok());

    std::vector<std::vector<std::string>> records = {
            {"Achievements of Stark Industries"},
            {"Achievement of Avengers"},
            {"Achievement of Shield"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    //without phrase search
    auto results = coll1->search(R"(achievements of)", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    // with phrase search
    results = coll1->search(R"(" achievements of ")", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionSpecificMoreTest, StemmingWithDroppingTokens) {
    nlohmann::json schema = R"({
             "name": "test",
             "fields": [ {"name": "content", "type": "string", "stem": true } ]
          })"_json;
    auto coll_stem_res = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_stem_res.ok());

    auto coll_stem = coll_stem_res.get();

    ASSERT_TRUE(coll_stem->add(R"({"content": "gardening tools"})"_json.dump()).ok());
    ASSERT_TRUE(coll_stem->add(R"({"content": "gardening supply"})"_json.dump()).ok());

    auto search_res = coll_stem->search("garden tools", {"content"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, search_res["hits"].size());
    ASSERT_EQ("gardening tools", search_res["hits"][0]["document"]["content"].get<std::string>());

    search_res = coll_stem->search("garden tools", {"content"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(2, search_res["hits"].size());
    ASSERT_EQ("gardening tools", search_res["hits"][0]["document"]["content"].get<std::string>());
    ASSERT_EQ("gardening supply", search_res["hits"][1]["document"]["content"].get<std::string>());
}


TEST_F(CollectionSpecificMoreTest, CustomStemmingDictionaryOverridesDeEnLocale) {
    nlohmann::json schema = R"({
        "name": "custom_stemming_test",
        "fields": [
          {"name": "title_de_en", "type": "string", "locale": "de_en", "stem_dictionary": "absurd_stems"},
          {"name": "title_en", "type": "string", "locale": "en", "stem": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    std::vector<std::string> json_lines;
    json_lines.push_back("{\"word\": \"running\", \"root\": \"foo\"}");
    json_lines.push_back("{\"word\": \"walking\", \"root\": \"bar\"}");
    json_lines.push_back("{\"word\": \"playing\", \"root\": \"baz\"}");
    json_lines.push_back("{\"word\": \"swimming\", \"root\": \"qux\"}");
    json_lines.push_back("{\"word\": \"dancing\", \"root\": \"xyz\"}");

    ASSERT_TRUE(stemmerManager.upsert_stemming_dictionary("absurd_stems", json_lines).ok());

    nlohmann::json doc1;
    doc1["id"] = "1";
    doc1["title_de_en"] = "running";
    doc1["title_en"] = "running";
    ASSERT_TRUE(coll->add(doc1.dump()).ok());

    nlohmann::json doc2;
    doc2["id"] = "2";
    doc2["title_de_en"] = "walking";
    doc2["title_en"] = "walking";
    ASSERT_TRUE(coll->add(doc2.dump()).ok());

    nlohmann::json doc3;
    doc3["id"] = "3";
    doc3["title_de_en"] = "playing";
    doc3["title_en"] = "playing";
    ASSERT_TRUE(coll->add(doc3.dump()).ok());

    nlohmann::json doc4;
    doc4["id"] = "4";
    doc4["title_de_en"] = "swimming";
    doc4["title_en"] = "swimming";
    ASSERT_TRUE(coll->add(doc4.dump()).ok());

    nlohmann::json doc5;
    doc5["id"] = "5";
    doc5["title_de_en"] = "dancing";
    doc5["title_en"] = "dancing";
    ASSERT_TRUE(coll->add(doc5.dump()).ok());

    auto res = coll->search("foo", {"title_de_en"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size()) << "Custom stem 'foo' should find 'running' in de_en field";
    ASSERT_EQ("1", res["hits"][0]["document"]["id"].get<std::string>());

    res = coll->search("bar", {"title_de_en"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size()) << "Custom stem 'bar' should find 'walking' in de_en field";
    ASSERT_EQ("2", res["hits"][0]["document"]["id"].get<std::string>());

    res = coll->search("baz", {"title_de_en"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size()) << "Custom stem 'baz' should find 'playing' in de_en field";
    ASSERT_EQ("3", res["hits"][0]["document"]["id"].get<std::string>());

    res = coll->search("qux", {"title_de_en"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size()) << "Custom stem 'qux' should find 'swimming' in de_en field";
    ASSERT_EQ("4", res["hits"][0]["document"]["id"].get<std::string>());

    res = coll->search("xyz", {"title_de_en"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, res["hits"].size()) << "Custom stem 'xyz' should find 'dancing' in de_en field";
    ASSERT_EQ("5", res["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("custom_stemming_test");
}

TEST_F(CollectionSpecificMoreTest, PhraseQueryHighlightingShouldNotHighlightPartialMatches) {
    std::vector<field> fields = {field("text", field_types::STRING, false)};
    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "1";
    doc1["text"] = "Thank you for being here. You are good";
    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    nlohmann::json doc2;
    doc2["id"] = "2";
    doc2["text"] = "Thank him first. Thank you later";
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    nlohmann::json doc3;
    doc3["id"] = "3";
    doc3["text"] = "Thank him first";
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    auto results = coll1->search("\"Thank you\"", {"text"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "text", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback, 1000).get();

    ASSERT_EQ(2, results["hits"].size());
    
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    std::string snippet1 = results["hits"][1]["highlights"][0]["snippet"].get<std::string>();
    
    ASSERT_TRUE(snippet1.find("<mark>Thank</mark> <mark>you</mark> for being here. You are good") != std::string::npos);
    
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    std::string snippet2 = results["hits"][0]["highlights"][0]["snippet"].get<std::string>();
    
    ASSERT_TRUE(snippet2.find("Thank him first. <mark>Thank</mark> <mark>you</mark> later") != std::string::npos);
    
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSpecificMoreTest, PhraseQueryHighlightingInNestedFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "education", "type": "object[]"},
            {"name": "education.school", "type": "string[]"},
            {"name": "summary", "type": "string"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    // document 1: phrase at start of nested field only
    nlohmann::json doc1;
    doc1["id"] = "1";
    doc1["education"] = nlohmann::json::array();
    nlohmann::json edu1;
    edu1["school"] = "Harvard Business School";
    edu1["degree_name"] = "MBA";
    doc1["education"].push_back(edu1);
    doc1["summary"] = "I grow businesses and develop creative strategies";
    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    // document 2: phrase in flat field only (works correctly)
    nlohmann::json doc2;
    doc2["id"] = "2";
    doc2["education"] = nlohmann::json::array();
    nlohmann::json edu2;
    edu2["school"] = "Duke University";
    edu2["degree_name"] = "BA";
    doc2["education"].push_back(edu2);
    doc2["summary"] = "Eric holds an MBA from Harvard Business School and a B.A. in Economics";
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    // document 3: phrase in both nested and flat fields, with non-matching nested entry
    nlohmann::json doc3;
    doc3["id"] = "3";
    doc3["education"] = nlohmann::json::array();
    nlohmann::json edu3;
    edu3["school"] = "Harvard Business School";
    edu3["degree_name"] = "MBA";
    doc3["education"].push_back(edu3);
    nlohmann::json edu3b;
    edu3b["school"] = "Duke University";
    edu3b["degree_name"] = "BA";
    doc3["education"].push_back(edu3b);
    doc3["summary"] = "Eric holds an MBA from Harvard Business School and a B.A. in Economics";
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    // document 4: phrase in middle of nested field text (like "Northwestern University & Harvard Business School")
    nlohmann::json doc4;
    doc4["id"] = "4";
    doc4["education"] = nlohmann::json::array();
    nlohmann::json edu4;
    edu4["school"] = "Harvard Business School";
    edu4["degree_name"] = "MBA";
    doc4["education"].push_back(edu4);
    nlohmann::json edu4b;
    edu4b["school"] = "Northwestern University & Harvard Business School";
    edu4b["degree_name"] = "BS";
    doc4["education"].push_back(edu4b);
    nlohmann::json edu4c;
    edu4c["school"] = "MIT";
    edu4c["degree_name"] = "PhD";
    doc4["education"].push_back(edu4c);
    doc4["summary"] = "I am a business leader with experience in technology";
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    // document 5: phrase in flat field, no phrase in nested fields
    nlohmann::json doc5;
    doc5["id"] = "5";
    doc5["education"] = nlohmann::json::array();
    nlohmann::json edu5;
    edu5["school"] = "Stanford University";
    edu5["degree_name"] = "MS";
    doc5["education"].push_back(edu5);
    nlohmann::json edu5b;
    edu5b["school"] = "Yale University";
    edu5b["degree_name"] = "BA";
    doc5["education"].push_back(edu5b);
    doc5["summary"] = "John earned an MBA from Harvard Business School, and a B.S., summa cum laude";
    ASSERT_TRUE(coll1->add(doc5.dump()).ok());

    // document 6: multiple nested entries with phrase, phrase in different positions
    nlohmann::json doc6;
    doc6["id"] = "6";
    doc6["education"] = nlohmann::json::array();
    nlohmann::json edu6;
    edu6["school"] = "Harvard Business School";
    edu6["degree_name"] = "MBA";
    doc6["education"].push_back(edu6);
    nlohmann::json edu6b;
    edu6b["school"] = "Northwestern University & Harvard Business School";
    edu6b["degree_name"] = "BS";
    doc6["education"].push_back(edu6b);
    nlohmann::json edu6c;
    edu6c["school"] = "Harvard Business School Online";
    edu6c["degree_name"] = "Certificate";
    doc6["education"].push_back(edu6c);
    doc6["summary"] = "John earned an MBA from Harvard Business School, and a B.S., summa cum laude";
    ASSERT_TRUE(coll1->add(doc6.dump()).ok());

    auto results = coll1->search("\"harvard business school\"", {"education.school", "summary"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 0,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback, 1000).get();


    ASSERT_EQ(6, results["hits"].size());

    // document 1: phrase at start of nested field only
    auto hit1 = find_doc_by_id(results, "1");
    ASSERT_FALSE(hit1.empty()) << "document 1 should be found";
    ASSERT_TRUE(hit1.count("highlight") > 0) << "document 1 should have highlight object";
    ASSERT_TRUE(hit1["highlight"].count("education") > 0) << "document 1 should have education in highlight object";
    ASSERT_GE(hit1["highlight"]["education"].size(), 1) << "document 1 should have at least 1 education entry";
    ASSERT_TRUE(hit1["highlight"]["education"][0].count("school") > 0);
    const auto& school1 = hit1["highlight"]["education"][0]["school"];
    ASSERT_TRUE(school1.count("matched_tokens") > 0);
    ASSERT_GE(school1["matched_tokens"].size(), 3) << "document 1 should have at least 3 matched tokens";

    // document 2: phrase in flat field only
    auto hit2 = find_doc_by_id(results, "2");
    ASSERT_FALSE(hit2.empty()) << "document 2 should be found";
    ASSERT_TRUE(hit2.count("highlights") > 0) << "document 2 should have highlights array";
    ASSERT_GT(hit2["highlights"].size(), 0) << "document 2 should have at least 1 highlight";
    std::string snippet2 = hit2["highlights"][0]["snippet"].get<std::string>();
    ASSERT_TRUE(snippet2.find("<mark>Harvard</mark>") != std::string::npos);
    ASSERT_TRUE(snippet2.find("<mark>Business</mark>") != std::string::npos);
    ASSERT_TRUE(snippet2.find("<mark>School</mark>") != std::string::npos);


    // document 3: phrase in both nested and flat fields
    auto hit3 = find_doc_by_id(results, "3");
    ASSERT_FALSE(hit3.empty()) << "document 3 should be found";
    
    // should have highlight for summary in highlights array (flat field)
    ASSERT_TRUE(hit3.count("highlights") > 0) << "document 3 should have highlights array";
    ASSERT_GT(hit3["highlights"].size(), 0) << "document 3 should have at least 1 highlight";
    ASSERT_EQ(hit3["highlights"][0]["field"], "summary");
    std::string snippet3 = hit3["highlights"][0]["snippet"].get<std::string>();
    ASSERT_TRUE(snippet3.find("<mark>Harvard</mark>") != std::string::npos);
    ASSERT_TRUE(snippet3.find("<mark>Business</mark>") != std::string::npos);
    ASSERT_TRUE(snippet3.find("<mark>School</mark>") != std::string::npos);
    
    // should have highlight for education.school in highlight object (nested field)
    ASSERT_TRUE(hit3.count("highlight") > 0) << "document 3 should have highlight object";
    ASSERT_TRUE(hit3["highlight"].count("education") > 0) << "document 3 should have education in highlight object";
    const auto& edu3_hit = hit3["highlight"]["education"];
    ASSERT_EQ(edu3_hit.size(), 2) << "document 3 should have 2 education entries";
    
    // first entry (Harvard Business School)
    ASSERT_TRUE(edu3_hit[0].count("school") > 0);
    ASSERT_TRUE(edu3_hit[0]["school"].count("matched_tokens") > 0);
    ASSERT_GE(edu3_hit[0]["school"]["matched_tokens"].size(), 3) << "First education entry should have at least 3 matched tokens";
    
    // second entry (Duke University)
    ASSERT_TRUE(edu3_hit[1].count("school") > 0);
    if (edu3_hit[1]["school"].count("matched_tokens") > 0) {
        ASSERT_EQ(edu3_hit[1]["school"]["matched_tokens"].size(), 0) << "Non-matching education entry should have empty matched_tokens";
    }

    // document 4: phrase in nested field with phrase in middle of text ("Northwestern University & Harvard Business School")
    // should highlight BOTH matching entries: "Harvard Business School" AND "Northwestern University & Harvard Business School"
    auto hit4 = find_doc_by_id(results, "4");
    ASSERT_FALSE(hit4.empty()) << "document 4 should be found";
    ASSERT_TRUE(hit4.count("highlight") > 0) << "document 4 should have highlight object";
    ASSERT_TRUE(hit4["highlight"].count("education") > 0) << "document 4 should have education in highlight object";
    const auto& edu4_hit = hit4["highlight"]["education"];
    ASSERT_EQ(edu4_hit.size(), 3) << "document 4 should have 3 education entries";
    
    // Check that first two entries have matched_tokens (Harvard Business School entries)
    ASSERT_TRUE(edu4_hit[0].count("school") > 0);
    ASSERT_TRUE(edu4_hit[0]["school"].count("matched_tokens") > 0);
    ASSERT_GE(edu4_hit[0]["school"]["matched_tokens"].size(), 3) << "First entry should have matched tokens";
    
    ASSERT_TRUE(edu4_hit[1].count("school") > 0);
    ASSERT_TRUE(edu4_hit[1]["school"].count("matched_tokens") > 0);
    ASSERT_GE(edu4_hit[1]["school"]["matched_tokens"].size(), 3) << "Second entry should have matched tokens";
    
    // verify snippets contain the phrase
    if (edu4_hit[0]["school"].count("snippet") > 0) {
        std::string snippet1 = edu4_hit[0]["school"]["snippet"].get<std::string>();
        ASSERT_TRUE(snippet1.find("<mark>Harvard</mark>") != std::string::npos);
    }
    if (edu4_hit[1]["school"].count("snippet") > 0) {
        std::string snippet2 = edu4_hit[1]["school"]["snippet"].get<std::string>();
        ASSERT_TRUE(snippet2.find("<mark>Harvard</mark>") != std::string::npos || 
                   snippet2.find("Northwestern") != std::string::npos);
    }
    
    // 3rd entry (MIT) should have empty matched_tokens
    if (edu4_hit[2].count("school") > 0 && edu4_hit[2]["school"].count("matched_tokens") > 0) {
        ASSERT_EQ(edu4_hit[2]["school"]["matched_tokens"].size(), 0) << "Third entry (MIT) should have empty matched_tokens";
    }

    // document 5: phrase in flat field only, no phrase in nested fields
    auto hit5 = find_doc_by_id(results, "5");
    ASSERT_FALSE(hit5.empty()) << "document 5 should be found";
    ASSERT_TRUE(hit5.count("highlights") > 0) << "document 5 should have highlights array";
    ASSERT_GT(hit5["highlights"].size(), 0) << "document 5 should have at least 1 highlight";
    
    // verify summary highlight exists (check first highlight, assuming it's summary)
    ASSERT_EQ(hit5["highlights"][0]["field"], "summary");
    std::string snippet5 = hit5["highlights"][0]["snippet"].get<std::string>();
    ASSERT_TRUE(snippet5.find("<mark>Harvard</mark>") != std::string::npos);
    ASSERT_TRUE(snippet5.find("<mark>Business</mark>") != std::string::npos);
    ASSERT_TRUE(snippet5.find("<mark>School</mark>") != std::string::npos);
    
    // should NOT have highlight object for education (no matches in nested fields)
    if (hit5.count("highlight") > 0 && hit5["highlight"].count("education") > 0) {
        ASSERT_EQ(hit5["highlight"]["education"].size(), 0) << "document 5 should not have any education highlights";
    }

    // document 6: multiple nested entries with phrase in different positions
    // should highlight ALL matching entries: "Harvard Business School", "Northwestern University & Harvard Business School", "Harvard Business School Online"
    auto hit6 = find_doc_by_id(results, "6");
    ASSERT_FALSE(hit6.empty()) << "document 6 should be found";
    ASSERT_TRUE(hit6.count("highlight") > 0) << "document 6 should have highlight object";
    ASSERT_TRUE(hit6["highlight"].count("education") > 0) << "document 6 should have education in highlight object";
    const auto& edu6_hit = hit6["highlight"]["education"];
    ASSERT_GE(edu6_hit.size(), 3) << "document 6 should have at least 3 education entries";
    
    // all three entries should have matched_tokens (all contain "Harvard Business School")
    ASSERT_TRUE(edu6_hit[0].count("school") > 0);
    ASSERT_TRUE(edu6_hit[0]["school"].count("matched_tokens") > 0);
    ASSERT_GE(edu6_hit[0]["school"]["matched_tokens"].size(), 3) << "First entry should have matched tokens";
    
    ASSERT_TRUE(edu6_hit[1].count("school") > 0);
    ASSERT_TRUE(edu6_hit[1]["school"].count("matched_tokens") > 0);
    ASSERT_GE(edu6_hit[1]["school"]["matched_tokens"].size(), 3) << "Second entry should have matched tokens";
    
    ASSERT_TRUE(edu6_hit[2].count("school") > 0);
    ASSERT_TRUE(edu6_hit[2]["school"].count("matched_tokens") > 0);
    ASSERT_GE(edu6_hit[2]["school"]["matched_tokens"].size(), 3) << "Third entry should have matched tokens";
    
    // verify snippets contain the phrase
    for (size_t i = 0; i < 3; i++) {
        if (edu6_hit[i]["school"].count("snippet") > 0) {
            std::string snippet = edu6_hit[i]["school"]["snippet"].get<std::string>();
            ASSERT_TRUE(snippet.find("<mark>Harvard</mark>") != std::string::npos);
        }
    }
    
    // should also have highlight for summary
    ASSERT_TRUE(hit6.count("highlights") > 0) << "document 6 should have highlights array";
    ASSERT_GT(hit6["highlights"].size(), 0) << "document 6 should have at least 1 highlight";
    ASSERT_EQ(hit6["highlights"][0]["field"], "summary");

    collectionManager.drop_collection("coll1");
}