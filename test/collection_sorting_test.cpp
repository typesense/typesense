#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionSortingTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_sorting";
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

TEST_F(CollectionSortingTest, SortingOrder) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("starring", field_types::STRING, false),
                                 field("points", field_types::INT32, false),
                                 field("cast", field_types::STRING_ARRAY, false)};

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_mul_fields->add(json_line);
    }

    infile.close();

    query_fields = {"title"};
    std::vector<std::string> facets;
    sort_fields = { sort_by("points", "ASC") };
    nlohmann::json results = coll_mul_fields->search("the", query_fields, "", facets, sort_fields, {0}, 15, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(10, results["hits"].size());

    std::vector<std::string> ids = {"17", "13", "10", "4", "0", "1", "8", "6", "16", "11"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // limiting results to just 5, "ASC" keyword must be case insensitive
    sort_fields = { sort_by("points", "asc") };
    results = coll_mul_fields->search("the", query_fields, "", facets, sort_fields, {0}, 5, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"17", "13", "10", "4", "0"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // desc

    sort_fields = { sort_by("points", "dEsc") };
    results = coll_mul_fields->search("the", query_fields, "", facets, sort_fields, {0}, 15, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(10, results["hits"].size());

    ids = {"11", "16", "6", "8", "1", "0", "10", "4", "13", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // With empty list of sort_by fields:
    // should be ordered desc on the default sorting field, since the match score will be the same for all records.
    sort_fields = { };
    results = coll_mul_fields->search("of", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"11", "12", "5", "4", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_mul_fields");
}

TEST_F(CollectionSortingTest, DefaultSortingFieldValidations) {
    // Default sorting field must be a  numerical field
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("age", field_types::INT32, false),
                                 field("in_stock", field_types::BOOL, false),
                                 field("average", field_types::INT32, false) };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC"), sort_by("average", "DESC") };

    Option<Collection*> collection_op = collectionManager.create_collection("sample_collection", 4, fields, "name");
    ASSERT_FALSE(collection_op.ok());
    ASSERT_EQ("Default sorting field `name` is not a sortable type.", collection_op.error());
    collectionManager.drop_collection("sample_collection");

    // Default sorting field must exist as a field in schema

    sort_fields = { sort_by("age", "DESC"), sort_by("average", "DESC") };
    collection_op = collectionManager.create_collection("sample_collection", 4, fields, "NOT-DEFINED");
    ASSERT_FALSE(collection_op.ok());
    ASSERT_EQ("Default sorting field is defined as `NOT-DEFINED` but is not found in the schema.", collection_op.error());
    collectionManager.drop_collection("sample_collection");

    // must be able to use boolean field as default sorting field
    collection_op = collectionManager.create_collection("sample_collection", 4, fields, "in_stock");
    ASSERT_TRUE(collection_op.ok());
    auto coll = collection_op.get();
    nlohmann::json doc;
    doc["name"] = "Example";
    doc["tags"] = {"example"};
    doc["age"] = 100;
    doc["in_stock"] = true;
    doc["average"] = 45;

    ASSERT_TRUE(coll->add(doc.dump()).ok());
}

TEST_F(CollectionSortingTest, NoDefaultSortingField) {
    Collection *coll1;

    std::ifstream infile(std::string(ROOT_DIR)+"test/documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields).get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll1->add(json_line);
    }

    infile.close();

    // without a default sorting field, matches should be sorted by (text_match, seq_id)
    auto results = coll1->search("rocket", {"title"}, "", {}, {}, {1}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(24, results["out_of"]);

    std::vector<std::string> ids = {"16", "15", "7", "0"};

    for(size_t i=0; i < results["hits"].size(); i++) {
        ASSERT_EQ(ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // try removing a document and doing wildcard (tests the seq_id array used for wildcard searches)
    auto remove_op = coll1->remove("0");
    ASSERT_TRUE(remove_op.ok());

    results = coll1->search("*", {}, "", {}, {}, {1}, 30, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(23, results["found"].get<size_t>());
    ASSERT_EQ(23, results["hits"].size());
    ASSERT_EQ(23, results["out_of"]);

    for(size_t i=23; i >= 1; i--) {
        std::string doc_id = (i == 4) ? "foo" : std::to_string(i);
        ASSERT_EQ(doc_id, results["hits"][23 - i]["document"]["id"].get<std::string>());
    }
}

TEST_F(CollectionSortingTest, FrequencyOrderedTokensWithoutDefaultSortingField) {
    // when no default sorting field is provided, tokens must be ordered on frequency
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields).get();
    }

    // since only top 4 tokens are fetched for prefixes, the "enyzme" should not show up in the results
    std::vector<std::string> tokens = {
        "enter", "elephant", "enamel", "ercot", "enyzme", "energy",
        "epoch", "epyc", "express", "everest", "end"
    };

    for(size_t i = 0; i < tokens.size(); i++) {
        size_t num_repeat = tokens.size() - i;

        std::string title = tokens[i];

        for(size_t j = 0; j < num_repeat; j++) {
            nlohmann::json doc;
            doc["title"] = title;
            doc["points"] = num_repeat;
            coll1->add(doc.dump());
        }
    }

    // max candidates as default 4
    auto results = coll1->search("e", {"title"}, "", {}, {}, {0}, 100, 1, NOT_SET, {true}).get();

    // [11 + 10 + 9 + 8] + 7 + 6 + 5 + 4 + 3 + 2
    ASSERT_EQ(38, results["found"].get<size_t>());

    // we have to ensure that no result contains the word "end" since it occurs least number of times
    bool found_end = false;
    for(auto& res: results["hits"].items()) {
        if(res.value()["document"]["title"] == "enyzme") {
            found_end = true;
        }
    }

    // 2 candidates
    results = coll1->search("e", {"title"}, "", {}, {}, {0}, 100, 1, NOT_SET, {true},
                            0, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(),
                            10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            off, 2).get();

    // [11 + 10] + 9 + 8 + 7 + 6 + 5 + 4 + 3 + 2
    ASSERT_EQ(21, results["found"].get<size_t>());

    ASSERT_FALSE(found_end);
}

TEST_F(CollectionSortingTest, TokenOrderingOnFloatValue) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::FLOAT, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::string> tokens = {
        "enter", "elephant", "enamel", "ercot", "enyzme", "energy",
        "epoch", "epyc", "express", "everest", "end"
    };

    for(size_t i = 0; i < tokens.size(); i++) {
        std::string title = tokens[i];
        float fpoint = (0.01 * i);
        nlohmann::json doc;
        doc["title"] = title;
        doc["points"] = fpoint;
        coll1->add(doc.dump());
    }

    auto results = coll1->search("e", {"title"}, "", {}, {}, {0}, 3, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ("10", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("9", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("8", results["hits"][2]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSortingTest, Int64AsDefaultSortingField) {
    Collection *coll_mul_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("starring", field_types::STRING, false),
                                 field("points", field_types::INT64, false),
                                 field("cast", field_types::STRING_ARRAY, false)};

    coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
    if(coll_mul_fields == nullptr) {
        coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
    }

    auto doc_str1 = "{\"title\": \"foo\", \"starring\": \"bar\", \"points\": 343234324234233234, \"cast\": [\"baz\"] }";
    const Option<nlohmann::json> & add_op = coll_mul_fields->add(doc_str1);
    ASSERT_TRUE(add_op.ok());

    auto doc_str2 = "{\"title\": \"foo\", \"starring\": \"bar\", \"points\": 343234324234233232, \"cast\": [\"baz\"] }";
    auto doc_str3 = "{\"title\": \"foo\", \"starring\": \"bar\", \"points\": 343234324234233235, \"cast\": [\"baz\"] }";
    auto doc_str4 = "{\"title\": \"foo\", \"starring\": \"bar\", \"points\": 343234324234233231, \"cast\": [\"baz\"] }";

    coll_mul_fields->add(doc_str2);
    coll_mul_fields->add(doc_str3);
    coll_mul_fields->add(doc_str4);

    query_fields = {"title"};
    std::vector<std::string> facets;
    sort_fields = { sort_by("points", "ASC") };
    nlohmann::json results = coll_mul_fields->search("foo", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    std::vector<std::string> ids = {"3", "1", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // DESC
    sort_fields = { sort_by("points", "desc") };
    results = coll_mul_fields->search("foo", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"2", "0", "1", "3"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }
}

TEST_F(CollectionSortingTest, SortOnFloatFields) {
    Collection *coll_float_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/float_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("score", field_types::FLOAT, false),
            field("average", field_types::FLOAT, false)
    };

    std::vector<sort_by> sort_fields_desc = { sort_by("score", "DESC"), sort_by("average", "DESC") };

    coll_float_fields = collectionManager.get_collection("coll_float_fields").get();
    if(coll_float_fields == nullptr) {
        coll_float_fields = collectionManager.create_collection("coll_float_fields", 4, fields, "score").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_float_fields->add(json_line);
    }

    infile.close();

    query_fields = {"title"};
    std::vector<std::string> facets;
    nlohmann::json results = coll_float_fields->search("Jeremy", query_fields, "", facets, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(7, results["hits"].size());

    std::vector<std::string> ids = {"2", "0", "3", "1", "5", "4", "6"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    std::vector<sort_by> sort_fields_asc = { sort_by("score", "ASC"), sort_by("average", "ASC") };
    results = coll_float_fields->search("Jeremy", query_fields, "", facets, sort_fields_asc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(7, results["hits"].size());

    ids = {"6", "4", "5", "1", "3", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        EXPECT_STREQ(id.c_str(), result_id.c_str());
    }

    // second field by desc

    std::vector<sort_by> sort_fields_asc_desc = { sort_by("score", "ASC"), sort_by("average", "DESC") };
    results = coll_float_fields->search("Jeremy", query_fields, "", facets, sort_fields_asc_desc, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(7, results["hits"].size());

    ids = {"5", "4", "6", "1", "3", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        EXPECT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll_float_fields");
}

TEST_F(CollectionSortingTest, ThreeSortFieldsLimit) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),
                                 field("average", field_types::INT32, false),
                                 field("min", field_types::INT32, false),
                                 field("max", field_types::INT32, false),
                                 };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc1;

    doc1["id"] = "100";
    doc1["title"] = "The quick brown fox";
    doc1["points"] = 25;
    doc1["average"] = 25;
    doc1["min"] = 25;
    doc1["max"] = 25;

    coll1->add(doc1.dump());

    std::vector<sort_by> sort_fields_desc = {
        sort_by("points", "DESC"),
        sort_by("average", "DESC"),
        sort_by("max", "DESC"),
        sort_by("min", "DESC"),
    };

    query_fields = {"title"};
    auto res_op = coll1->search("the", query_fields, "", {}, sort_fields_desc, {0}, 10, 1, FREQUENCY, {false});

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Only upto 3 sort_by fields can be specified.", res_op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, ThreeSortFieldsTextMatchLast) {
    Collection *coll1;

    std::vector<field> fields = { field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("popularity", field_types::INT32, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Coby Grant", "100"},    // text_match: 33684577
        {"Coby Prant", "84642"},  // text_match: 129377
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][0];
        doc["popularity"] = std::stoi(records[i][1]);
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by("popularity", "DESC"), sort_by("points", "DESC"), sort_by(sort_field_const::text_match, "DESC") };

    auto res = coll1->search("grant",
                             {"title","artist"}, "", {}, sort_fields, {1}, 10, 1, FREQUENCY, {false}, 10,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10).get();

    ASSERT_EQ(2, res["found"].get<size_t>());
    ASSERT_STREQ("1", res["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", res["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, SingleFieldTextMatchScoreDefault) {
    // when queried with a single field, _text_match score should be used implicitly as the second sorting field
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Alppha Beta"},
        {"Alpha Beta"},
        {"Alphas Beta"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = 100;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    auto results = coll1->search("alpha",
                                 {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, NegativeInt64Value) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT64, false),
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc1;

    doc1["id"] = "100";
    doc1["title"] = "The quick brown fox";
    doc1["points"] = -2678400;

    coll1->add(doc1.dump());

    std::vector<sort_by> sort_fields_desc = {
      sort_by("points", "DESC")
    };

    query_fields = {"title"};
    auto res = coll1->search("*", query_fields, "points:>=1577836800", {}, sort_fields_desc, {0}, 10, 1, FREQUENCY,
                             {false}).get();

    ASSERT_EQ(0, res["found"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, GeoPointSorting) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Palais Garnier", "48.872576479306765, 2.332291112241466"},
        {"Sacre Coeur", "48.888286721920934, 2.342340862419206"},
        {"Arc de Triomphe", "48.87538726829884, 2.296113163780903"},
        {"Place de la Concorde", "48.86536119187326, 2.321850747347093"},
        {"Louvre Musuem", "48.86065813197502, 2.3381285349616725"},
        {"Les Invalides", "48.856648379569904, 2.3118555692631357"},
        {"Eiffel Tower", "48.85821022164442, 2.294239067890161"},
        {"Notre-Dame de Paris", "48.852455825574495, 2.35071182406452"},
        {"Musee Grevin", "48.872370541246816, 2.3431536410008906"},
        {"Pantheon", "48.84620987789056, 2.345152755563131"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // pick a large radius covering all points, with a point close to Pantheon
    std::vector<sort_by> geo_sort_fields = {
        sort_by("loc(48.84442912268208, 2.3490714964332353)", "ASC")
    };

    auto results = coll1->search("*",
                            {}, "loc: (48.84442912268208, 2.3490714964332353, 20km)",
                            {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(10, results["found"].get<size_t>());

    std::vector<std::string> expected_ids = {
        "9", "7", "4", "5", "3", "8", "0", "6", "1", "2"
    };

    for(size_t i=0; i < expected_ids.size(); i++) {
        ASSERT_STREQ(expected_ids[i].c_str(), results["hits"][i]["document"]["id"].get<std::string>().c_str());
    }

    ASSERT_EQ(348, results["hits"][0]["geo_distance_meters"]["loc"].get<int>());
    ASSERT_EQ(900, results["hits"][1]["geo_distance_meters"]["loc"].get<int>());
    ASSERT_EQ(1973, results["hits"][2]["geo_distance_meters"]["loc"].get<int>());

    // desc, without filter
    geo_sort_fields = {
        sort_by("loc(48.84442912268208, 2.3490714964332353)", "DESC")
    };

    results = coll1->search("*",
                            {}, "",
                            {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(10, results["found"].get<size_t>());

    for(size_t i=0; i < expected_ids.size(); i++) {
        ASSERT_STREQ(expected_ids[expected_ids.size() - 1 - i].c_str(), results["hits"][i]["document"]["id"].get<std::string>().c_str());
    }

    // with bad sort field formats
    std::vector<sort_by> bad_geo_sort_fields = {
        sort_by("loc(,2.3490714964332353)", "ASC")
    };

    auto res_op = coll1->search("*",
                            {}, "",
                            {}, bad_geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Bad syntax for sorting field `loc`", res_op.error().c_str());

    bad_geo_sort_fields = {
            sort_by("loc(x, y)", "ASC")
    };

    res_op = coll1->search("*",
                                {}, "",
                                {}, bad_geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Bad syntax for sorting field `loc`", res_op.error().c_str());

    bad_geo_sort_fields = {
        sort_by("loc(", "ASC")
    };

    res_op = coll1->search("*",
                           {}, "",
                           {}, bad_geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a field named `loc(` in the schema for sorting.", res_op.error().c_str());

    bad_geo_sort_fields = {
        sort_by("loc)", "ASC")
    };

    res_op = coll1->search("*",
                           {}, "",
                           {}, bad_geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a field named `loc)` in the schema for sorting.", res_op.error().c_str());

    bad_geo_sort_fields = {
            sort_by("l()", "ASC")
    };

    res_op = coll1->search("*",
                           {}, "",
                           {}, bad_geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a field named `l` in the schema for sorting.", res_op.error().c_str());

    // should not allow creation of collection with geo field as default_sorting_field
    nlohmann::json schema = R"({
        "name": "coll_geo",
        "fields": [
          {"name": "title", "type": "string"},
          {"name": "location", "type": "geopoint" }
        ],
        "default_sorting_field": "location"
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Default sorting field cannot be of type geopoint.", op.error());

    schema = R"({
        "name": "coll_geo",
        "fields": [
          {"name": "title", "type": "string"},
          {"name": "location", "type": "geopoint[]" }
        ],
        "default_sorting_field": "location"
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Default sorting field cannot be of type geopoint.", op.error());

    collectionManager.drop_collection("coll1");
    collectionManager.drop_collection("coll_geo");
}

TEST_F(CollectionSortingTest, GeoPointSortingWithExcludeRadius) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Tibetan Colony",     "32.24678, 77.19239"},
        {"Civil Hospital",     "32.23959, 77.18763"},
        {"Johnson Lodge",      "32.24751, 77.18814"},

        {"Lion King Rock",     "32.24493, 77.17038"},
        {"Jai Durga Handloom", "32.25749, 77.17583"},
        {"Panduropa",          "32.26059, 77.21798"},
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> geo_sort_fields = {
        sort_by("loc(32.24348, 77.1893, exclude_radius: 1km)", "ASC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("*",
                                 {}, "loc: (32.24348, 77.1893, 20 km)",
                                 {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(6, results["found"].get<size_t>());

    std::vector<std::string> expected_ids = {
        "2", "1", "0", "3", "4", "5"
    };

    for (size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_STREQ(expected_ids[i].c_str(), results["hits"][i]["document"]["id"].get<std::string>().c_str());
    }

    // without exclusion filter

    geo_sort_fields = {
            sort_by("loc(32.24348, 77.1893)", "ASC"),
            sort_by("points", "DESC"),
    };

    results = coll1->search("*",
                            {}, "loc: (32.24348, 77.1893, 20 km)",
                            {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(6, results["found"].get<size_t>());

    expected_ids = {
        "1", "2", "0", "3", "4", "5"
    };

    for (size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_STREQ(expected_ids[i].c_str(), results["hits"][i]["document"]["id"].get<std::string>().c_str());
    }

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, precision: 2mi)", "ASC") };
    auto res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);
    ASSERT_TRUE(res_op.ok());

    // bad vertex -- Edge 0 is degenerate (duplicate vertex)
    geo_sort_fields = { sort_by("loc(28.7040592, 77.10249019999999)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (28.7040592, 77.10249019999999, 28.7040592, "
                                    "77.10249019999999, 28.7040592, 77.10249019999999, 28.7040592, 77.10249019999999)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Polygon is invalid: Edge 0 is degenerate (duplicate vertex)", res_op.error());

    // badly formatted exclusion filter

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, exclude_radius 1 km)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                                {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `loc`", res_op.error());

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, exclude_radius: 1 meter)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Sort field's parameter unit must be either `km` or `mi`.", res_op.error());

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, exclude_radius: -10 km)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Sort field's parameter must be a positive number.", res_op.error());

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, exclude_radius: 10 km 20 mi)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `loc`", res_op.error());

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, exclude_radius: 1k)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Sort field's parameter unit must be either `km` or `mi`.", res_op.error());

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, exclude_radius: 5)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `loc`", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, GeoPointSortingWithPrecision) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Tibetan Colony",     "32.24678, 77.19239"},
        {"Civil Hospital",     "32.23959, 77.18763"},
        {"Johnson Lodge",      "32.24751, 77.18814"},

        {"Lion King Rock",     "32.24493, 77.17038"},
        {"Jai Durga Handloom", "32.25749, 77.17583"},
        {"Panduropa",          "32.26059, 77.21798"},

        {"Police Station",     "32.23743, 77.18639"},
        {"Panduropa Post",     "32.26263, 77.2196"},
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["loc"] = {lat, lng};
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> geo_sort_fields = {
        sort_by("loc(32.24348, 77.1893, precision: 0.9 km)", "ASC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("*",
                                 {}, "loc: (32.24348, 77.1893, 20 km)",
                                 {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(8, results["found"].get<size_t>());

    std::vector<std::string> expected_ids = {
        "6", "2", "1", "0", "3", "4", "7", "5"
    };
    std::vector<float> geo_distance_meters = {0.726,0.461,0.46,0.467,1.786,2.007,3.556,3.299};

    for (size_t i = 0; i < expected_ids.size(); i++) {
        auto const& hit = results["hits"][i];
        ASSERT_EQ(expected_ids[i], hit["document"]["id"]);
        ASSERT_FLOAT_EQ(geo_distance_meters[i], hit["geo_distance_meters"]["loc"]);
    }

    // badly formatted precision

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, precision 1 km)", "ASC") };
    auto res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                                {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `loc`", res_op.error());

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, precision: 1 meter)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Sort field's parameter unit must be either `km` or `mi`.", res_op.error());

    geo_sort_fields = { sort_by("loc(32.24348, 77.1893, precision: -10 km)", "ASC") };
    res_op = coll1->search("*", {}, "loc: (32.24348, 77.1893, 20 km)",
                           {}, geo_sort_fields, {0}, 10, 1, FREQUENCY);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Sort field's parameter must be a positive number.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, GeoPointAsOptionalField) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT, false, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Tibetan Colony",     "32.24678, 77.19239"},
        {"Civil Hospital",     "32.23959, 77.18763"},
        {"Johnson Lodge",      "32.24751, 77.18814"},

        {"Lion King Rock",     "32.24493, 77.17038"},
        {"Jai Durga Handloom", "32.25749, 77.17583"},
        {"Panduropa",          "32.26059, 77.21798"},

        {"Police Station",     "32.23743, 77.18639"},
        {"Panduropa Post",     "32.26263, 77.2196"},
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        std::vector<std::string> lat_lng;
        StringUtils::split(records[i][1], lat_lng, ", ");

        double lat = std::stod(lat_lng[0]);
        double lng = std::stod(lat_lng[1]);

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];

        if(i != 2) {
            doc["loc"] = {lat, lng};
        }

        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> geo_sort_fields = {
        sort_by("loc(32.24348, 77.1893, precision: 0.9 km)", "ASC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("*",
                                 {}, "loc: (32.24348, 77.1893, 20 km)",
                                 {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(7, results["found"].get<size_t>());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, GeoPointArraySorting) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("loc", field_types::GEOPOINT_ARRAY, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::vector<std::string>>> records = {
        {   {"Alpha Inc", "Ennore", "13.22112, 80.30511"},
            {"Alpha Inc", "Velachery", "12.98973, 80.23095"}
        },

        {
            {"Veera Inc", "Thiruvallur", "13.12752, 79.90136"},
        },

        {
            {"B1 Inc", "Bengaluru", "12.98246, 77.5847"},
            {"B1 Inc", "Hosur", "12.74147, 77.82915"},
            {"B1 Inc", "Vellore", "12.91866, 79.13075"},
        },

        {
            {"M Inc", "Nashik", "20.11282, 73.79458"},
            {"M Inc", "Pune", "18.56309, 73.855"},
        }
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0][0];
        doc["points"] = i;

        std::vector<std::vector<double>> lat_lngs;
        for(size_t k = 0; k < records[i].size(); k++) {
            std::vector<std::string> lat_lng_str;
            StringUtils::split(records[i][k][2], lat_lng_str, ", ");

            std::vector<double> lat_lng = {
                    std::stod(lat_lng_str[0]),
                    std::stod(lat_lng_str[1])
            };

            lat_lngs.push_back(lat_lng);
        }

        doc["loc"] = lat_lngs;
        auto add_op = coll1->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    std::vector<sort_by> geo_sort_fields = {
        sort_by("loc(13.12631, 80.20252)", "ASC"),
        sort_by("points", "DESC"),
    };

    // pick a location close to Chennai
    auto results = coll1->search("*",
                                 {}, "loc: (13.12631, 80.20252, 100 km)",
                                 {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // pick a large radius covering all points

    geo_sort_fields = {
        sort_by("loc(13.03388, 79.25868)", "ASC"),
        sort_by("points", "DESC"),
    };

    results = coll1->search("*",
                            {}, "loc: (13.03388, 79.25868, 1000 km)",
                            {}, geo_sort_fields, {0}, 10, 1, FREQUENCY).get();

    ASSERT_EQ(4, results["found"].get<size_t>());

    ASSERT_STREQ("2", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][3]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, SortByTitle) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", true),
                                 field("artist", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        auto create_op = collectionManager.create_collection("coll1", 2, fields, "title");
        ASSERT_TRUE(create_op.ok());
        coll1 = create_op.get();
    }

    std::vector<std::vector<std::string>> records = {
        {"aaa", "ABCD"},
        {"a", "ABCD"},
        {"abcd", "ABCD"},
        {"abdde", "ABCD"},
        {"b", "ABCD"},
        {"bab", "ABCD"},
        {"baa", "ABCD"},
        {"bcma", "ABCD"},
        {"cdma", "ABCD"},
        {"cc", "ABCD"},
        {"c", "ABCD"},
        {"cxya", "ABCD"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by("title", "ASC")
    };

    std::vector<std::string> expected_order = {
        "a",
        "aaa",
        "abcd",
        "abdde",
        "b",
        "baa",
        "bab",
        "bcma",
        "c",
        "cc",
        "cdma",
        "cxya"
    };

    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 20, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(12, results["found"].get<size_t>());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        ASSERT_EQ(expected_order[i], results["hits"][i]["document"]["title"].get<std::string>());
    }

    // descending order
    sort_fields = {
        sort_by("title", "DESC")
    };

    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 20, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(12, results["found"].get<size_t>());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        ASSERT_EQ(expected_order[expected_order.size() - i - 1], results["hits"][i]["document"]["title"].get<std::string>());
    }

    // when querying for a string field with sort disabled
    sort_fields = {
        sort_by("artist", "DESC")
    };

    auto res_op = coll1->search("*", {}, "", {}, sort_fields, {0}, 20, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `artist` in the schema for sorting.", res_op.error());

    // don't allow non-sort string field to be used as default sorting field

    fields = {field("title", field_types::STRING, false, false, true, "", false),
              field("artist", field_types::STRING, true),
              field("points", field_types::INT32, false),};

    auto create_op = collectionManager.create_collection("coll2", 2, fields, "title");
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Default sorting field `title` is not a sortable type.", create_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, SortByIntegerAndString) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto create_op = collectionManager.create_collection("coll1", 2, fields, "title");
        ASSERT_TRUE(create_op.ok());
        coll1 = create_op.get();
    }

    std::vector<std::vector<std::string>> records = {
        {"abdde", "2"},
        {"b",     "2"},
        {"b",     "1"},
        {"a",     "1"},
        {"c",     "1"},
        {"dd",    "4"},
        {"bab",   "3"},
        {"baa",   "3"},
        {"bcma",  "3"},
        {"cdma",  "3"},
        {"c",     "5"},
        {"x",     "6"},
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = std::stoi(records[i][1]);

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by("points", "ASC"),
        sort_by("title", "ASC"),
    };

    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 20, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ("a", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ("b", results["hits"][1]["document"]["title"].get<std::string>());
    ASSERT_EQ("c", results["hits"][2]["document"]["title"].get<std::string>());
    ASSERT_EQ("abdde", results["hits"][3]["document"]["title"].get<std::string>());
    ASSERT_EQ("b", results["hits"][4]["document"]["title"].get<std::string>());
    ASSERT_EQ("baa", results["hits"][5]["document"]["title"].get<std::string>());

    sort_fields = {
        sort_by("_text_match", "DESC"),
        sort_by("points", "ASC"),
        sort_by("title", "ASC"),
    };

    results = coll1->search("b", {"title"}, "", {}, sort_fields, {0}, 20, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ("b", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ("b", results["hits"][1]["document"]["title"].get<std::string>());
    ASSERT_EQ("baa", results["hits"][2]["document"]["title"].get<std::string>());
    ASSERT_EQ("bab", results["hits"][3]["document"]["title"].get<std::string>());
    ASSERT_EQ("bcma", results["hits"][4]["document"]["title"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, SortByStringEmptyValuesConfigFirstField) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", true),
                                 field("points1", field_types::INT32, false),
                                 field("points2", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points1").get();
    }

    std::vector<std::string> tokens = {
        "alpha", "beta", "", "gamma"
    };

    for(size_t i = 0; i < tokens.size(); i++) {
        std::string title = tokens[i];
        nlohmann::json doc;
        doc["title"] = title;
        doc["points1"] = 100;
        doc["points2"] = 100;
        coll1->add(doc.dump());
    }

    // ascending
    std::vector<sort_by> sort_fields = {
        sort_by("title(missing_values: first)", "ASC"),
    };
    auto results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("title(missing_values: last)", "ASC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // descending
    sort_fields = {
        sort_by("title(missing_values: first)", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("title(missing_values: last)", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // without explicit arg, missing values will be deemed as having largest value (same as SQL)
    sort_fields = {
        sort_by("title", "asc"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("title", "desc"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // natural order
    sort_fields = {
        sort_by("title(missing_values: normal)", "asc"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("title(missing_values: normal)", "desc"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // bad syntax
    sort_fields = {
        sort_by("title(foo: bar)", "desc"),
    };
    auto res_op = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `title`", res_op.error());

    sort_fields = {
        sort_by("title(missing_values: bar)", "desc"),
    };
    res_op = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Bad syntax for sorting field `title`", res_op.error());
}

TEST_F(CollectionSortingTest, SortByStringEmptyValuesConfigSecondField) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", true),
                                 field("points1", field_types::INT32, false),
                                 field("points2", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
    coll1 = collectionManager.create_collection("coll1", 1, fields, "points1").get();
    }

    std::vector<std::string> tokens = {
        "alpha", "beta", "", "gamma"
    };

    for(size_t i = 0; i < tokens.size(); i++) {
        std::string title = tokens[i];
        nlohmann::json doc;
        doc["title"] = title;
        doc["points1"] = 100;
        doc["points2"] = 100;
        coll1->add(doc.dump());
    }

    // ascending
    std::vector<sort_by> sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("title(missing_values: first)", "ASC"),
    };
    auto results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("title(missing_values: last)", "ASC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // descending
    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("title(missing_values: first)", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("title(missing_values: last)", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // without explicit arg, missing values will be deemed as having largest value (same as SQL)
    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("title", "ASC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("title", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSortingTest, SortByStringEmptyValuesConfigThirdField) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", true),
                                 field("points1", field_types::INT32, false),
                                 field("points2", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points1").get();
    }

    std::vector<std::string> tokens = {
        "alpha", "beta", "", "gamma"
    };

    for(size_t i = 0; i < tokens.size(); i++) {
        std::string title = tokens[i];
        nlohmann::json doc;
        doc["title"] = title;
        doc["points1"] = 100;
        doc["points2"] = 100;
        coll1->add(doc.dump());
    }

    // ascending
    std::vector<sort_by> sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("points2", "ASC"),
        sort_by("title(missing_values: first)", "ASC"),
    };
    auto results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("points2", "ASC"),
        sort_by("title(missing_values: last)", "ASC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // descending
    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("points2", "ASC"),
        sort_by("title(missing_values: first)", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("points2", "ASC"),
        sort_by("title(missing_values: last)", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    // without explicit arg, missing values will be deemed as having largest value (same as SQL)
    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("points2", "ASC"),
        sort_by("title", "ASC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());

    sort_fields = {
        sort_by("points1", "ASC"),
        sort_by("points2", "ASC"),
        sort_by("title", "DESC"),
    };
    results = coll1->search("*", {"title"}, "", {}, sort_fields, {0}, 10, 1, MAX_SCORE, {true}).get();
    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionSortingTest, SortByStringAccentedChars) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", true),
                                 field("artist", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        auto create_op = collectionManager.create_collection("coll1", 2, fields, "title");
        ASSERT_TRUE(create_op.ok());
        coll1 = create_op.get();
    }

    std::vector<std::vector<std::string>> records = {
        {"The unbearable lightness of being", "ABCD"},
        {"A brief history of time", "ABCD"},
        {"Über den Wolken", "ABCD"},
        {"Ändere deine Coding Gewohnheiten", "ABCD"},
        {"Zodiac", "ABCD"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by("title", "ASC")
    };

    std::vector<std::string> expected_order = {
        "A brief history of time",
        "Ändere deine Coding Gewohnheiten",
        "The unbearable lightness of being",
        "Über den Wolken",
        "Zodiac",
    };

    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 20, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(5, results["found"].get<size_t>());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        ASSERT_EQ(expected_order[i], results["hits"][i]["document"]["title"].get<std::string>());
    }

    // descending order
    sort_fields = {
        sort_by("title", "DESC")
    };

    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 20, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(5, results["found"].get<size_t>());

    for(size_t i = 0; i < results["hits"].size(); i++) {
        ASSERT_EQ(expected_order[expected_order.size() - i - 1], results["hits"][i]["document"]["title"].get<std::string>());
    }
}

TEST_F(CollectionSortingTest, TextMatchBucketRanking) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("description", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Mark Antony";
    doc1["description"] = "Counsellor";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Marks Spencer";
    doc2["description"] = "Sales Expert";
    doc2["points"] = 200;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    sort_fields = {
        sort_by("_text_match(buckets: 10)", "DESC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("mark", {"title"},
                                 "", {}, sort_fields, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {3}, 1000, true).get();

    // when there are more buckets than results, no bucketing will happen
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    // bucketing by 1 makes the text match score the same
    sort_fields = {
        sort_by("_text_match(buckets: 1)", "DESC"),
        sort_by("points", "DESC"),
    };

    results = coll1->search("mark", {"title"},
                            "", {}, sort_fields, {2}, 10,
                            1, FREQUENCY, {true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {3}, 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    size_t score1 = std::stoul(results["hits"][0]["text_match_info"]["score"].get<std::string>());
    size_t score2 = std::stoul(results["hits"][1]["text_match_info"]["score"].get<std::string>());
    ASSERT_TRUE(score1 < score2);

    // bucketing by 0 produces original text match
    sort_fields = {
        sort_by("_text_match(buckets: 0)", "DESC"),
        sort_by("points", "DESC"),
    };

    results = coll1->search("mark", {"title"},
                            "", {}, sort_fields, {2}, 10,
                            1, FREQUENCY, {true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {3}, 1000, true).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    // don't allow bad parameter name
    sort_fields[0] = sort_by("_text_match(foobar: 0)", "DESC");

    auto res_op = coll1->search("mark", {"title"},
                            "", {}, sort_fields, {2}, 10,
                            1, FREQUENCY, {true},
                            10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {3}, 1000, true);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Invalid sorting parameter passed for _text_match.", res_op.error());

    // handle bad syntax
    sort_fields[0] = sort_by("_text_match(foobar:", "DESC");
    res_op = coll1->search("mark", {"title"},
                                "", {}, sort_fields, {2}, 10,
                                1, FREQUENCY, {true},
                                10, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                "<mark>", "</mark>", {3}, 1000, true);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `_text_match(foobar:` in the schema for sorting.", res_op.error());

    // handle bad value
    sort_fields[0] = sort_by("_text_match(buckets: x)", "DESC");
    res_op = coll1->search("mark", {"title"},
                           "", {}, sort_fields, {2}, 10,
                           1, FREQUENCY, {true},
                           10, spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                           "<mark>", "</mark>", {3}, 1000, true);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Invalid value passed for _text_match `buckets` configuration.", res_op.error());

    // handle negative value
    sort_fields[0] = sort_by("_text_match(buckets: -1)", "DESC");
    res_op = coll1->search("mark", {"title"},
                           "", {}, sort_fields, {2}, 10,
                           1, FREQUENCY, {true},
                           10, spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                           "<mark>", "</mark>", {3}, 1000, true);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Invalid value passed for _text_match `buckets` configuration.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, TextMatchMoreDocsThanBuckets) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::vector<std::vector<std::string>> records = {
        {"Mark Antony"},
        {"Marks Spencer"},
        {"Marking Rhine"},
        {"Markolm Spane"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    sort_fields = {
        sort_by("_text_match(buckets: 2)", "DESC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("mark", {"title"},
                                 "", {}, sort_fields, {0}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1}, 1000, true).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][3]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, RepeatingTokenRanking) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Mong Mong";
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Mong Spencer";
    doc2["points"] = 200;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["title"] = "Mong Mong Spencer";
    doc3["points"] = 300;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["title"] = "Spencer Mong Mong";
    doc4["points"] = 400;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    sort_fields = {
        sort_by("_text_match", "DESC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("mong mong", {"title"},
                                 "", {}, sort_fields, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {3}, 1000, true).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][3]["document"]["id"].get<std::string>());

    ASSERT_EQ(1157451471575842841, results["hits"][0]["text_match"].get<size_t>());
    ASSERT_EQ(1157451471575318553, results["hits"][1]["text_match"].get<size_t>());
    ASSERT_EQ(1157451471575318553, results["hits"][2]["text_match"].get<size_t>());
    ASSERT_EQ(1157451471575318553, results["hits"][3]["text_match"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, SortingDoesNotHaveTextMatchComponent) {
    // text_match_score field should not be present in response
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Test Title";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    sort_fields = {
        sort_by("points", "DESC"),
        sort_by("points", "DESC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("test", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0].count("text_match"));

    results = coll1->search("*", {}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0].count("text_match"));

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, IntegerFloatAndBoolShouldDefaultSortTrue) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string", "infix": true },
              {"name": "points", "type": "int32" },
              {"name": "timestamp", "type": "int64" },
              {"name": "max", "type": "float" },
              {"name": "is_valid", "type": "bool" }
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Right on";
    doc1["points"] = 100;
    doc1["timestamp"] = 7273272372732;
    doc1["max"] = 97.6;
    doc1["is_valid"] = true;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto res_op = coll1->search("*", {"title"}, "", {}, {sort_by("points", "DESC")}, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_TRUE(res_op.ok());

    res_op = coll1->search("*", {"title"}, "", {}, {sort_by("timestamp", "DESC")}, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_TRUE(res_op.ok());

    res_op = coll1->search("*", {"title"}, "", {}, {sort_by("max", "DESC")}, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_TRUE(res_op.ok());

    res_op = coll1->search("*", {"title"}, "", {}, {sort_by("is_valid", "DESC")}, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_TRUE(res_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, DisallowSortingOnNonIndexedIntegerField) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "points", "type": "int32", "index": false, "optional": true }
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Right on";
    doc1["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto res_op = coll1->search("*", {"title"}, "", {}, {sort_by("points", "DESC")}, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `points` in the schema for sorting.", res_op.error());

    res_op = coll1->search("*", {"title"}, "", {}, {sort_by("points(missing_values: first)", "DESC")}, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `points` in the schema for sorting.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, WildcardSearchSequenceIdSort) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "category", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["category"] = "Shoes";

    for(size_t i = 0; i < 30; i++) {
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by("_seq_id", "DESC"),
    };

    auto res = coll1->search("*", {"category"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(10, res["hits"].size());
    ASSERT_EQ(30, res["found"].get<size_t>());
}

TEST_F(CollectionSortingTest, OptionalFilteringViaSortingWildcard) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "brand", "type": "string", "infix": true },
              {"name": "points", "type": "int32" }
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    for(size_t i = 0; i < 5; i++) {
        nlohmann::json doc;
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        doc["brand"] =  (i == 0 || i == 3) ? "Nike" : "Adidas";
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by({"brand:nike"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(5, results["hits"].size());
    std::vector<std::string> expected_ids = {"3", "0", "4", "2", "1"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // compound query
    sort_fields = {
        sort_by({"brand:nike && points:0"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    results = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(5, results["hits"].size());

    expected_ids = {"0", "4", "3", "2", "1"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    sort_fields = {
            sort_by({"brand:nike", "points:1"}, {2, 1}, "DESC"),
            sort_by("points", "DESC"),
    };

    results = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(5, results["hits"].size());

    expected_ids = {"3", "0", "1", "4", "2"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // when no results are found for eval query
    sort_fields = {
            sort_by({"brand:foobar"}, {1}, "DESC"),
            sort_by("points", "DESC"),
    };

    results = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(5, results["hits"].size());
    expected_ids = {"4", "3", "2", "1", "0"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    nlohmann::json doc = R"(
        {
            "title": "title5",
            "brand": "puma",
            "points": 5
        }
    )"_json;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    sort_fields = {
            sort_by({"brand:nike", "brand:adidas"}, {3, 2}, "DESC"),
            sort_by("points", "DESC"),
    };

    results = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(6, results["hits"].size());

    expected_ids = {"3", "0", "4", "2", "1", "5"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // Score associated with the first match is assigned to the document.
    sort_fields = {
            sort_by({"brand:nike", "brand:adidas", "points: 1"}, {3, 2, 5}, "DESC"),
            sort_by("points", "DESC"),
    };

    results = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(6, results["hits"].size());

    expected_ids = {"3", "0", "4", "2", "1", "5"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // bad syntax for eval query
    sort_fields = {
        sort_by({"brandnike || points:0"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    auto res_op = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error parsing eval expression in sort_by clause.", res_op.error());

    // when eval condition is empty
    std::map<std::string, std::string> req_params = {
            {"collection", "coll1"},
            {"q", "*"},
            {"query_by", "title"},
            {"sort_by", "_eval():desc"},
    };
    nlohmann::json embedded_params;
    std::string json_res;
    auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    auto search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ("The eval expression in sort_by is empty.", search_op.error());

    req_params = {
            {"collection", "coll1"},
            {"q", "a"},
            {"query_by", "brand"},
            {"sort_by", "_eval(brand:puma):desc, _text_match:desc"},
            {"infix", "always"}
    };

    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    results = nlohmann::json::parse(json_res);

    ASSERT_EQ(4, results["hits"].size()); // 3 Adidas 1 Puma documents
    // Because of `_eval`, Puma document will be on top even when having a lower text match score than Adidas documents.
    expected_ids = {"5", "4", "2", "1"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // more bad syntax!
    sort_fields = {
        sort_by(")", "DESC"),
        sort_by("points", "DESC"),
    };

    res_op = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `)` in the schema for sorting.", res_op.error());

    // don't allow multiple sorting eval expressions
    sort_fields = {
        sort_by({"brand: nike || points:0"}, {1}, "DESC"),
        sort_by({"brand: nike || points:0"}, {1}, "DESC"),
    };

    res_op = coll1->search("*", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Only one sorting eval expression is allowed.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, OptionalFilteringViaSortingSearch) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "brand", "type": "string" },
              {"name": "points", "type": "int32" }
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    for(size_t i = 0; i < 5; i++) {
        nlohmann::json doc;
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        doc["brand"] =  (i == 0 || i == 3) ? "Nike" : "Adidas";
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by({"brand:nike"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(5, results["hits"].size());
    std::vector<std::string> expected_ids = {"3", "0", "4", "2", "1"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // compound query
    sort_fields = {
        sort_by({"brand:nike && points:0"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(5, results["hits"].size());

    std::map<std::string, std::string> req_params = {
            {"collection", "coll1"},
            {"q", "title"},
            {"query_by", "title"},
            {"sort_by", "_eval(brand:[nike, adidas] && points:0):desc, points:DESC"}
    };
    nlohmann::json embedded_params;
    std::string json_res;
    auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    auto search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    results = nlohmann::json::parse(json_res);
    ASSERT_EQ(5, results["hits"].size());

    expected_ids = {"0", "4", "3", "2", "1"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // when no results are found for eval query
    sort_fields = {
        sort_by({"brand:foobar"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(5, results["hits"].size());
    expected_ids = {"4", "3", "2", "1", "0"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // bad syntax for eval query
    sort_fields = {
        sort_by({"brandnike || points:0"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    auto res_op = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error parsing eval expression in sort_by clause.", res_op.error());

    // more bad syntax!
    sort_fields = {
        sort_by(")", "DESC"),
        sort_by("points", "DESC"),
    };

    res_op = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `)` in the schema for sorting.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSortingTest, DisallowIdAsDefaultSortingField) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "default_sorting_field": "id",
            "fields": [
              {"name": "id", "type": "string" }
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    auto coll_op = collectionManager.create_collection(schema);
    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("Invalid `default_sorting_field` value: cannot be `id`.", coll_op.error());
}

TEST_F(CollectionSortingTest, OptionalFilteringViaSortingSecondThirdParams) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "brand", "type": "string" },
              {"name": "points", "type": "int32" },
              {"name": "val", "type": "int32" }
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    for(size_t i = 0; i < 5; i++) {
        nlohmann::json doc;
        doc["title"] = "Title " + std::to_string(i);
        doc["val"] = 0;
        doc["points"] = i;
        doc["brand"] =  (i == 0 || i == 3) ? "Nike" : "Adidas";
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by("val", "DESC"),
        sort_by({"brand:nike"}, {1}, "DESC"),
        sort_by("points", "DESC"),
    };

    auto results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(5, results["hits"].size());
    std::vector<std::string> expected_ids = {"3", "0", "4", "2", "1"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    // eval expression as 3rd sorting argument
    sort_fields = {
        sort_by("val", "DESC"),
        sort_by("val", "DESC"),
        sort_by({"brand:nike"}, {1}, "DESC"),
    };

    results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(5, results["hits"].size());
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    sort_fields = {
            sort_by("val", "DESC"),
            sort_by({"brand:adidas", "brand:nike"}, {2, 1}, "DESC"),
            sort_by("points", "DESC"),
    };
    expected_ids = {"4", "2", "1","3", "0"};

    results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(5, results["hits"].size());
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }

    sort_fields = {
            sort_by("val", "DESC"),
            sort_by("val", "DESC"),
            sort_by({"brand:adidas", "brand:nike"}, {2, 1}, "DESC"),
    };

    results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(5, results["hits"].size());
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }
}


TEST_F(CollectionSortingTest, AscendingVectorDistance) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "points", "type": "float[]", "num_dim": 2}
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::vector<std::vector<float>> points = {
        {3.0, 4.0},
        {9.0, 21.0},
        {8.0, 15.0},
        {1.0, 1.0},
        {5.0, 7.0}
    };

    for(size_t i = 0; i < points.size(); i++) {
        nlohmann::json doc;
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = points[i];
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by("_vector_distance", "asc"),
    };

    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "points:([8.0, 15.0])").get();

    ASSERT_EQ(5, results["hits"].size());
    std::vector<std::string> expected_ids = {"2", "1", "4", "0", "3"};
    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }
}


TEST_F(CollectionSortingTest, DescendingVectorDistance) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "points", "type": "float[]", "num_dim": 2}
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::vector<std::vector<float>> points = {
        {3.0, 4.0},
        {9.0, 21.0},
        {8.0, 15.0},
        {1.0, 1.0},
        {5.0, 7.0}
    };

    for(size_t i = 0; i < points.size(); i++) {
        nlohmann::json doc;
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = points[i];
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    } 

    std::vector<sort_by> sort_fields = {
        sort_by("_vector_distance", "DESC"),
    };

    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "points:([8.0, 15.0])").get();
    
    ASSERT_EQ(5, results["hits"].size());
    std::vector<std::string> expected_ids = {"3", "0", "4", "1", "2"};

    for(size_t i = 0; i < expected_ids.size(); i++) {
        ASSERT_EQ(expected_ids[i], results["hits"][i]["document"]["id"].get<std::string>());
    }
}


TEST_F(CollectionSortingTest, InvalidVectorDistanceSorting) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "points", "type": "float[]", "num_dim": 2}
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::vector<std::vector<float>> points = {
        {1.0, 1.0},
        {2.0, 2.0},
        {3.0, 3.0},
        {4.0, 4.0},
        {5.0, 5.0},
    };

    for(size_t i = 0; i < points.size(); i++) {
        nlohmann::json doc;
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = points[i];
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {
        sort_by("_vector_distance", "desc"),
    };



    auto results = coll1->search("title", {"title"}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10);

    ASSERT_FALSE(results.ok());

    ASSERT_EQ("sort_by vector_distance is only supported for vector queries, semantic search and hybrid search.", results.error());
}

TEST_F(CollectionSortingTest, TestSortByVectorQuery) {
    std::string coll_schema = R"(
        {
            "name": "coll1",
            "fields": [
              {"name": "name", "type": "string" },
              {"name": "points", "type": "float[]", "num_dim": 2}
            ]
        }
    )";

    nlohmann::json schema = nlohmann::json::parse(coll_schema);
    auto create_coll = collectionManager.create_collection(schema);
    ASSERT_TRUE(create_coll.ok());

    auto coll = create_coll.get();

    std::vector<std::vector<float>> points = {
        {7.0, 8.0},
        {8.0, 15.0},
        {5.0, 12.0},
    };

    for(size_t i = 0; i < points.size(); i++) {
        nlohmann::json doc;
        doc["name"] = "Title " + std::to_string(i);
        doc["points"] = points[i];
        ASSERT_TRUE(coll->add(doc.dump()).ok());
    }

    std::vector<sort_by> sort_fields = {};

    auto results = coll->search("title", {"name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "").get();
    
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"]);
    ASSERT_EQ("1", results["hits"][1]["document"]["id"]);
    ASSERT_EQ("0", results["hits"][2]["document"]["id"]);

    sort_fields = {
        sort_by("_vector_query(points:([5.0, 5.0]))", "asc"),
    };

    results = coll->search("title", {"name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                           4, {off}, 32767, 32767, 2,
                           false, true, "").get();
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"]);
    ASSERT_EQ("1", results["hits"][1]["document"]["id"]);
    ASSERT_EQ("2", results["hits"][2]["document"]["id"]);

    sort_fields = {
        sort_by("_vector_query(points:([5.0, 5.0]))", "desc"),
    };

    results = coll->search("title", {"name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                           4, {off}, 32767, 32767, 2,
                           false, true, "").get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"]);
    ASSERT_EQ("1", results["hits"][1]["document"]["id"]);
    ASSERT_EQ("0", results["hits"][2]["document"]["id"]);
}

TEST_F(CollectionSortingTest, TestVectorQueryQsSorting) {
        auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "buttercup"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "butter"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("butter", {"name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                           4, {off}, 32767, 32767, 2,
                           false, true, "").get();
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"]);
    ASSERT_EQ("0", results["hits"][1]["document"]["id"]);

    sort_fields = {
        sort_by("_vector_query(embedding:([], queries: [powerpuff girls, cartoon]))", "asc"),
    };

    results = coll->search("butter", {"name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                           4, {off}, 32767, 32767, 2,
                           false, true, "").get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"]);
    ASSERT_EQ("1", results["hits"][1]["document"]["id"]);
}