#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <filesystem>
#include <collection_manager.h>
#include "collection.h"
#include "embedder_manager.h"
#include "http_client.h"

class CollectionAllFieldsTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_all_fields";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
        system("mkdir -p /tmp/typesense_test/models");

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

TEST_F(CollectionAllFieldsTest, IndexDocsWithoutSchema) {
    Collection *coll1;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    // try to create collection with random fallback field type
    auto bad_coll_op = collectionManager.create_collection("coll_bad", 1, fields, "", 0, "blah");
    ASSERT_FALSE(bad_coll_op.ok());
    ASSERT_EQ("Field `.*` has an invalid type.", bad_coll_op.error());

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        auto coll_op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
        coll1 = coll_op.get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        nlohmann::json document = nlohmann::json::parse(json_line);
        Option<nlohmann::json> add_op = coll1->add(document.dump());
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    query_fields = {"starring"};
    std::vector<std::string> facets;

    // check default no specific dirty values option is sent for a collection that has schema detection enabled
    std::string dirty_values;
    ASSERT_EQ(DIRTY_VALUES::COERCE_OR_REJECT, coll1->parse_dirty_values_option(dirty_values));

    dirty_values = "coerce_or_reject";
    ASSERT_EQ(DIRTY_VALUES::COERCE_OR_REJECT, coll1->parse_dirty_values_option(dirty_values));

    dirty_values = "COERCE_OR_DROP";
    ASSERT_EQ(DIRTY_VALUES::COERCE_OR_DROP, coll1->parse_dirty_values_option(dirty_values));

    dirty_values = "reject";
    ASSERT_EQ(DIRTY_VALUES::REJECT, coll1->parse_dirty_values_option(dirty_values));

    dirty_values = "DROP";
    ASSERT_EQ(DIRTY_VALUES::DROP, coll1->parse_dirty_values_option(dirty_values));

    // same should succeed when verbatim filter is made
    auto results = coll1->search("will", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("chris", {"cast"}, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("7", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // reject field with a different type than already inferred type
    // default for `index_all_fields` is `DIRTY_FIELD_COERCE_IGNORE`

    // unable to coerce
    auto doc_json = R"({"cast":"William Barnes","points":63,"starring":"Will Ferrell",
                        "starring_facet":"Will Ferrell","title":"Anchorman 2: The Legend Continues"})";

    Option<nlohmann::json> add_op = coll1->add(doc_json);
    ASSERT_FALSE(add_op.ok());
    ASSERT_STREQ("Field `cast` must be an array.", add_op.error().c_str());

    // coerce integer to string
    doc_json = R"({"cast": ["William Barnes"],"points": 63, "starring":"Will Ferrell",
                        "starring_facet":"Will Ferrell","title": 300})";

    add_op = coll1->add(doc_json);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("300", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("300", results["hits"][0]["document"]["title"].get<std::string>().c_str());

    // with dirty values set to `COERCE_OR_DROP`
    // `cast` field should not be indexed into store
    doc_json = R"({"cast":"William Barnes","points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title":"With bad cast field."})";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("With bad cast field", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("With bad cast field.", results["hits"][0]["document"]["title"].get<std::string>().c_str());
    ASSERT_EQ(0, results["hits"][0]["document"].count("cast"));

    // with dirty values set to `DROP`
    // no coercion should happen, `title` field will just be dropped, but record indexed
    doc_json = R"({"cast": ["Jeremy Livingston"],"points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title": 1200 })";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("1200", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("Jeremy Livingston", {"cast"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0]["document"].count("title"));

    // with dirty values set to `REJECT`
    doc_json = R"({"cast": ["Jeremy Livingston"],"points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title": 1200 })";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_STREQ("Field `title` must be a string.", add_op.error().c_str());

    // try querying using an non-existing sort field
    sort_fields = { sort_by("not-found", "DESC") };
    auto res_op = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `not-found` in the schema for sorting.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, CoerceDynamicStringField) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field(".*_name", "string", true, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, "").get();
    }

    std::string dirty_values;
    ASSERT_EQ(DIRTY_VALUES::COERCE_OR_REJECT, coll1->parse_dirty_values_option(dirty_values));

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, HandleArrayTypes) {
    Collection *coll1;

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, field_types::AUTO).get();
    }

    nlohmann::json doc;
    doc["title"] = "FIRST";
    doc["int_values"] = {1, 2};

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0");
    ASSERT_TRUE(add_op.ok());

    // coercion of string -> int

    doc["int_values"] = {"3"};

    add_op = coll1->add(doc.dump(), UPDATE, "0");
    ASSERT_TRUE(add_op.ok());

    // bad array type value should be dropped when stored

    doc["title"] = "SECOND";
    doc["int_values"] = {{3}};
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES:: DROP);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("second", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // check that the "bad" value does not exists in the stored document
    ASSERT_EQ(1, results["hits"][0]["document"].count("int_values"));
    ASSERT_EQ(0, results["hits"][0]["document"]["int_values"].size());

    // bad array type should follow coercion rules
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `int_values` must be an array of int64.", add_op.error());

    // non array field should be handled as per coercion rule
    doc["title"] = "THIRD";
    doc["int_values"] = 3;
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `int_values` must be an array.", add_op.error());

    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_TRUE(add_op.ok());
    results = coll1->search("third", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0]["document"].count("int_values"));

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, NonOptionalFieldShouldNotBeDropped) {
    Collection *coll1;

    std::vector<field> fields = {
        field("points", field_types::INT32, false, false)
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0).get();
    }

    nlohmann::json doc;
    doc["title"] = "FIRST";
    doc["points"] = {100};

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0", DIRTY_VALUES::DROP);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `points` must be an int32.", add_op.error());

    add_op = coll1->add(doc.dump(), CREATE, "0", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `points` must be an int32.", add_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, ShouldBeAbleToUpdateSchemaDetectedDocs) {
    Collection *coll1;

    std::vector<field> fields = {

    };

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "", 0, field_types::AUTO).get();
    }

    nlohmann::json doc;
    doc["title"] = "FIRST";
    doc["scores"] = {100, 200, 300};

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0", DIRTY_VALUES::REJECT);
    ASSERT_TRUE(add_op.ok());

    // now update both values and reinsert
    doc["title"] = "SECOND";
    doc["scores"] = {100, 250, "300", 400};

    add_op = coll1->add(doc.dump(), UPDATE, "0", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("second", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("SECOND", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(4, results["hits"][0]["document"]["scores"].size());

    ASSERT_EQ(100, results["hits"][0]["document"]["scores"][0].get<size_t>());
    ASSERT_EQ(250, results["hits"][0]["document"]["scores"][1].get<size_t>());
    ASSERT_EQ(300, results["hits"][0]["document"]["scores"][2].get<size_t>());
    ASSERT_EQ(400, results["hits"][0]["document"]["scores"][3].get<size_t>());

    // insert multiple docs at the same time
    const size_t NUM_DOCS = 20;
    std::vector<std::string> json_lines;

    for(size_t i = 0; i < NUM_DOCS; i++) {
        const std::string &i_str = std::to_string(i);
        doc["title"] = std::string("upserted ") + std::to_string(StringUtils::hash_wy(i_str.c_str(), i_str.size()));
        doc["scores"] = {i};
        doc["max"] = i;
        doc["id"] = std::to_string(i+10);

        json_lines.push_back(doc.dump());
    }

    nlohmann::json insert_doc;
    auto res = coll1->add_many(json_lines, insert_doc, UPSERT);
    ASSERT_TRUE(res["success"].get<bool>());

    // now we will replace all `max` values with the same value and assert that
    json_lines.clear();
    insert_doc.clear();

    for(size_t i = 0; i < NUM_DOCS; i++) {
        const std::string &i_str = std::to_string(i);
        doc.clear();
        doc["title"] = std::string("updated ") + std::to_string(StringUtils::hash_wy(i_str.c_str(), i_str.size()));
        doc["scores"] = {1000, 2000};
        doc["max"] = 2000;
        doc["id"] = std::to_string(i+10);

        json_lines.push_back(doc.dump());
    }

    res = coll1->add_many(json_lines, insert_doc, UPDATE);
    ASSERT_TRUE(res["success"].get<bool>());

    results = coll1->search("updated", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(20, results["hits"].size());

    for(auto& hit: results["hits"]) {
        ASSERT_EQ(2000, hit["document"]["max"].get<int>());
        ASSERT_EQ(2, hit["document"]["scores"].size());
        ASSERT_EQ(1000, hit["document"]["scores"][0].get<int>());
        ASSERT_EQ(2000, hit["document"]["scores"][1].get<int>());
    }

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, StringifyAllValues) {
    Collection *coll1;

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, "string*").get();
    }

    nlohmann::json doc;
    doc["title"] = "FIRST";
    doc["int_values"] = {1, 2};

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0");
    ASSERT_TRUE(add_op.ok());
    auto added_doc = add_op.get();

    auto schema = coll1->get_fields();

    ASSERT_EQ("int_values", schema[0].name);
    ASSERT_EQ(field_types::STRING_ARRAY, schema[0].type);

    ASSERT_EQ("title", schema[1].name);
    ASSERT_EQ(field_types::STRING, schema[1].type);

    ASSERT_EQ("1", added_doc["int_values"][0].get<std::string>());
    ASSERT_EQ("2", added_doc["int_values"][1].get<std::string>());

    auto results = coll1->search("first", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("FIRST", results["hits"][0]["document"]["title"].get<std::string>());

    ASSERT_EQ(1, results["hits"][0]["document"].count("int_values"));
    ASSERT_EQ(2, results["hits"][0]["document"]["int_values"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["int_values"][0].get<std::string>());
    ASSERT_EQ("2", results["hits"][0]["document"]["int_values"][1].get<std::string>());

    // try with DROP
    doc["title"] = "SECOND";
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("second", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("SECOND", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(1, results["hits"][0]["document"].count("int_values"));
    ASSERT_EQ(0, results["hits"][0]["document"]["int_values"].size());  // since both array values are dropped

    // try with REJECT
    doc["title"] = "THIRD";
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `int_values` must be an array of string.", add_op.error());

    // singular field coercion
    doc["int_values"] = {"100"};
    doc["single_int"] = 100;
    doc["title"] = "FOURTH";

    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `single_int` must be a string.", add_op.error());

    // try with empty array
    doc["title"] = "FIFTH";
    doc["int_values"] = {"100"};
    doc["int_values_2"] = nlohmann::json::array();
    doc["single_int"] = "200";

    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, IntegerAllValues) {
    Collection *coll1;

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, "int32").get();
    }

    nlohmann::json doc;
    doc["age"] = 100;
    doc["year"] = 2000;

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0");
    ASSERT_TRUE(add_op.ok());
    auto added_doc = add_op.get();

    auto results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // try with DROP
    doc["age"] = "SECOND";
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    // try with REJECT
    doc["age"] = "THIRD";
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `age` must be an int32.", add_op.error());

    // try with coerce_or_reject
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `age` must be an int32.", add_op.error());

    // try with coerce_or_drop
    doc["age"] = "FOURTH";
    add_op = coll1->add(doc.dump(), CREATE, "66", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, SearchStringifiedField) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("department", "string*", true, true),
                                 field(".*_name", "string*", true, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        const Option<Collection*> &coll_op = collectionManager.create_collection("coll1", 1, fields, "", 0, "");
        ASSERT_TRUE(coll_op.ok());
        coll1 = coll_op.get();
    }

    nlohmann::json doc;
    doc["title"] = "FIRST";
    doc["department"] = "ENGINEERING";
    doc["company_name"] = "Stark Inc.";

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0");
    ASSERT_TRUE(add_op.ok());

    // department field's type must be "solidified" to an actual type

    auto schema = coll1->get_fields();
    ASSERT_EQ("department", schema[4].name);
    ASSERT_EQ(field_types::STRING, schema[4].type);

    auto results_op = coll1->search("stark", {"company_name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(results_op.ok());
    ASSERT_EQ(1, results_op.get()["hits"].size());

    results_op = coll1->search("engineering", {"department"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(results_op.ok());
    ASSERT_EQ(1, results_op.get()["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, StringSingularAllValues) {
    Collection *coll1;

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, "string").get();
    }

    nlohmann::json doc;
    doc["title"] = "FIRST";
    doc["int_values"] = {1, 2};

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0");
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `int_values` must be a string.", add_op.error());

    doc["int_values"] = 123;

    add_op = coll1->add(doc.dump(), CREATE, "0");
    ASSERT_TRUE(add_op.ok());

    auto added_doc = add_op.get();

    ASSERT_EQ("FIRST", added_doc["title"].get<std::string>());
    ASSERT_EQ("123", added_doc["int_values"].get<std::string>());

    auto results = coll1->search("first", {"title"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("FIRST", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ("123", results["hits"][0]["document"]["int_values"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, UpdateOfDocumentsInAutoMode) {
    Collection *coll1;

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, field_types::AUTO).get();
    }

    nlohmann::json doc;
    doc["title"]  = "FIRST";
    doc["single_float"]  = 50.50;

    auto add_op = coll1->add(doc.dump(), CREATE, "0", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    // try updating a value
    nlohmann::json update_doc;
    update_doc["single_float"]  = "123";

    add_op = coll1->add(update_doc.dump(), UPDATE, "0", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, NormalFieldWithAutoType) {
    Collection *coll1;

    std::vector<field> fields = {
        field("city", field_types::AUTO, true, true),
        field("publication_year", field_types::AUTO, true, true),
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto coll_op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
        ASSERT_TRUE(coll_op.ok());
        coll1 = coll_op.get();
    }

    nlohmann::json doc;
    doc["title"]  = "FIRST";
    doc["city"]  = "Austin";
    doc["publication_year"]  = 2010;

    auto add_op = coll1->add(doc.dump(), CREATE, "0", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    auto res_op = coll1->search("austin", {"city"}, "publication_year: 2010", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(res_op.ok());
    auto results = res_op.get();
    ASSERT_EQ(1, results["hits"].size());

    auto schema = coll1->get_fields();
    ASSERT_EQ("city", schema[2].name);
    ASSERT_EQ(field_types::STRING, schema[2].type);

    ASSERT_EQ("publication_year", schema[3].name);
    ASSERT_EQ(field_types::INT64, schema[3].type);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, JsonFieldsToFieldsConversion) {
    nlohmann::json fields_json = nlohmann::json::array();
    nlohmann::json all_field;
    all_field[fields::name] = ".*";
    all_field[fields::type] = "string*";
    fields_json.emplace_back(all_field);

    std::string fallback_field_type;
    std::vector<field> fields;

    auto parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);

    ASSERT_TRUE(parse_op.ok());
    ASSERT_EQ(1, fields.size());
    ASSERT_EQ("string*", fallback_field_type);
    ASSERT_EQ(true, fields[0].optional);
    ASSERT_EQ(false, fields[0].facet);
    ASSERT_EQ(".*", fields[0].name);
    ASSERT_EQ("string*", fields[0].type);

    // non-wildcard string* field should be treated as optional by default
    fields_json = nlohmann::json::array();
    nlohmann::json string_star_field;
    string_star_field[fields::name] = "title";
    string_star_field[fields::type] = "string*";
    fields_json.emplace_back(string_star_field);
    fields.clear();

    parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);
    ASSERT_TRUE(parse_op.ok());
    ASSERT_EQ(true, fields[0].optional);

    fields_json = nlohmann::json::array();
    fields_json.emplace_back(all_field);

    // reject when you try to set optional to false or facet to true
    fields_json[0][fields::optional] = false;
    parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_EQ("Field `.*` must be an optional field.", parse_op.error());

    fields_json[0][fields::optional] = true;
    fields_json[0][fields::facet] = true;
    parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_EQ("Field `.*` cannot be a facet field.", parse_op.error());

    fields_json[0][fields::facet] = false;

    // can have only one ".*" field
    fields_json.emplace_back(all_field);

    parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_EQ("There can be only one field named `.*`.", parse_op.error());

    // try with the `auto` type
    fields_json.clear();
    fields.clear();
    all_field[fields::type] = "auto";
    fields_json.emplace_back(all_field);

    parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);
    ASSERT_TRUE(parse_op.ok());
    ASSERT_EQ("auto", fields[0].type);

    // try with locale on a regular field
    fields_json.clear();
    fields.clear();
    all_field[fields::type] = "string";
    all_field[fields::name] = "title";
    all_field[fields::locale] = "ja";
    fields_json.emplace_back(all_field);

    parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);
    ASSERT_TRUE(parse_op.ok());
    ASSERT_EQ("ja", fields[0].locale);

    // try with locale on fallback field
    fields_json.clear();
    fields.clear();
    all_field[fields::type] = "string";
    all_field[fields::name] = ".*";
    all_field[fields::locale] = "ko";
    fields_json.emplace_back(all_field);

    parse_op = field::json_fields_to_fields(false, fields_json, fallback_field_type, fields);
    ASSERT_TRUE(parse_op.ok());
    ASSERT_EQ("ko", fields[0].locale);
}

TEST_F(CollectionAllFieldsTest, WildcardFacetFieldsOnAutoSchema) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field(".*_name", field_types::STRING, true, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO).get();
    }

    nlohmann::json doc;
    doc["title"]  = "Org";
    doc["org_name"]  = "Amazon";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    doc["title"]  = "Org";
    doc["org_name"]  = "Walmart";

    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("org", {"title"}, "", {"org_name"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("Walmart", results["hits"][0]["document"]["org_name"].get<std::string>());
    ASSERT_EQ("Amazon", results["hits"][1]["document"]["org_name"].get<std::string>());

    ASSERT_EQ("Amazon", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_EQ("Walmart", results["facet_counts"][0]["counts"][1]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);

    // add another type of .*_name field

    doc.clear();
    doc["title"]  = "Company";
    doc["company_name"]  = "Stark";

    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {"title"}, "", {"company_name", "org_name"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_EQ("company_name", results["facet_counts"][0]["field_name"].get<std::string>());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("Stark", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_EQ("org_name", results["facet_counts"][1]["field_name"].get<std::string>());
    ASSERT_EQ(2, results["facet_counts"][1]["counts"].size());
    ASSERT_EQ("Amazon", results["facet_counts"][1]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][1]["counts"][0]["count"]);

    ASSERT_EQ("Walmart", results["facet_counts"][1]["counts"][1]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][1]["counts"][1]["count"]);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, WildcardFacetFieldsWithAuoFacetFieldType) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field(".*_name", field_types::AUTO, true, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO).get();
    }

    nlohmann::json doc;
    doc["title"]  = "Org";
    doc["org_name"]  = "Amazon";
    doc["year_name"]  = 1990;

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    doc["title"]  = "Org";
    doc["org_name"]  = "Walmart";

    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("org", {"title"}, "", {"org_name"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("Walmart", results["hits"][0]["document"]["org_name"].get<std::string>());
    ASSERT_EQ("Amazon", results["hits"][1]["document"]["org_name"].get<std::string>());

    ASSERT_EQ("Amazon", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_EQ("Walmart", results["facet_counts"][0]["counts"][1]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, WildcardFacetFieldsWithoutAutoSchema) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field(".*_name", field_types::STRING, true, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0).get();
    }

    nlohmann::json doc;
    doc["title"]  = "Org";
    doc["org_name"]  = "Amazon";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    doc["title"]  = "Org";
    doc["org_name"]  = "Walmart";

    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("org", {"title"}, "", {"org_name"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("Walmart", results["hits"][0]["document"]["org_name"].get<std::string>());
    ASSERT_EQ("Amazon", results["hits"][1]["document"]["org_name"].get<std::string>());

    ASSERT_EQ("Amazon", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_EQ("Walmart", results["facet_counts"][0]["counts"][1]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);

    // add another type of .*_name field

    doc.clear();
    doc["title"]  = "Company";
    doc["company_name"]  = "Stark";

    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {"title"}, "", {"company_name", "org_name"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_EQ("company_name", results["facet_counts"][0]["field_name"].get<std::string>());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("Stark", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_EQ("org_name", results["facet_counts"][1]["field_name"].get<std::string>());
    ASSERT_EQ(2, results["facet_counts"][1]["counts"].size());
    ASSERT_EQ("Amazon", results["facet_counts"][1]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][1]["counts"][0]["count"]);

    ASSERT_EQ("Walmart", results["facet_counts"][1]["counts"][1]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][1]["counts"][1]["count"]);

    // Don't allow auto detection of schema when AUTO mode is not chosen
    doc["description"]  = "Stark company.";
    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto res_op = coll1->search("*", {"description"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `description` in the schema.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, RegexpExplicitFieldTypeCoercion) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("i.*", field_types::INT32, false, true),
                                 field("s.*", field_types::STRING, false, true),
                                 field("a.*", field_types::STRING_ARRAY, false, true),
                                 field("nullsa.*", field_types::STRING_ARRAY, false, true),
                                 field("num.*", "string*", false, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0).get();
    }

    nlohmann::json doc;
    doc["title"]  = "Rand Building";
    doc["i_age"]  = "28";
    doc["s_name"]  = nullptr;
    doc["a_name"]  = {};
    doc["nullsa"]  = nullptr;
    doc["num_employees"] = 28;

    // should coerce while retaining expected type

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto schema = coll1->get_fields();

    ASSERT_EQ("a_name", schema[6].name);
    ASSERT_EQ(field_types::STRING_ARRAY, schema[6].type);

    ASSERT_EQ("i_age", schema[7].name);
    ASSERT_EQ(field_types::INT32, schema[7].type);

    ASSERT_EQ("nullsa", schema[8].name);
    ASSERT_EQ(field_types::STRING_ARRAY, schema[8].type);

    // num_employees field's type must be "solidified" to an actual type
    ASSERT_EQ("num_employees", schema[9].name);
    ASSERT_EQ(field_types::STRING, schema[9].type);

    ASSERT_EQ("s_name", schema[10].name);
    ASSERT_EQ(field_types::STRING, schema[10].type);

    auto results = coll1->search("rand", {"title"}, "i_age: 28", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, DynamicFieldsMustOnlyBeOptional) {
    Collection *coll1;

    std::vector<field> bad_fields = {field("title", field_types::STRING, true),
                                 field(".*_name", field_types::STRING, true, false),};

    auto op = collectionManager.create_collection("coll1", 1, bad_fields, "", 0);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Field `.*_name` must be an optional field.", op.error());

    // string* fields should only be optional
    std::vector<field> bad_fields2 = {field("title", field_types::STRING, true),
                                      field("name", "string*", true, false),};

    op = collectionManager.create_collection("coll1", 1, bad_fields2, "", 0);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Field `name` must be an optional field.", op.error());

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field(".*_name", field_types::STRING, true, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        op = collectionManager.create_collection("coll1", 1, fields, "", 0);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    ASSERT_TRUE(coll1->get_dynamic_fields()[".*_name"].optional);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, AutoAndStringStarFieldsShouldAcceptNullValues) {
    Collection *coll1;

    std::vector<field> fields = {
        field("foo", "string*", true, true),
        field("buzz", "auto", true, true),
        field("bar.*", "string*", true, true),
        field("baz.*", "auto", true, true),
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto coll_op = collectionManager.create_collection("coll1", 1, fields, "", 0);
        ASSERT_TRUE(coll_op.ok());
        coll1 = coll_op.get();
    }

    nlohmann::json doc;
    doc["foo"]  = nullptr;
    doc["buzz"]  = nullptr;
    doc["bar_one"]  = nullptr;
    doc["baz_one"]  = nullptr;

    // should allow indexing of null values since all are optional
    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto res = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(1, res["hits"][0]["document"].size());

    auto schema = coll1->get_fields();
    ASSERT_EQ(4, schema.size());

    doc["foo"]  = {"hello", "world"};
    doc["buzz"]  = 123;
    doc["bar_one"]  = "hello";
    doc["baz_one"]  = true;

    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    schema = coll1->get_fields();
    ASSERT_EQ(8, schema.size());

    ASSERT_EQ("bar_one", schema[4].name);
    ASSERT_EQ(field_types::STRING, schema[4].type);

    ASSERT_EQ("baz_one", schema[5].name);
    ASSERT_EQ(field_types::BOOL, schema[5].type);

    ASSERT_EQ("buzz", schema[6].name);
    ASSERT_EQ(field_types::INT64, schema[6].type);

    ASSERT_EQ("foo", schema[7].name);
    ASSERT_EQ(field_types::STRING_ARRAY, schema[7].type);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, BothFallbackAndDynamicFields) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field(".*_name", field_types::STRING, false, true),
                                 field(".*_year", field_types::INT32, true, true),
                                 field(".*", field_types::AUTO, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    ASSERT_EQ(4, coll1->get_fields().size());
    ASSERT_EQ(2, coll1->get_dynamic_fields().size());

    ASSERT_TRUE(coll1->get_dynamic_fields().count(".*_name") != 0);
    ASSERT_TRUE(coll1->get_dynamic_fields()[".*_name"].optional);
    ASSERT_FALSE(coll1->get_dynamic_fields()[".*_name"].facet);

    ASSERT_TRUE(coll1->get_dynamic_fields().count(".*_year") != 0);
    ASSERT_TRUE(coll1->get_dynamic_fields()[".*_year"].optional);
    ASSERT_TRUE(coll1->get_dynamic_fields()[".*_year"].facet);

    nlohmann::json doc;
    doc["title"]  = "Amazon Inc.";
    doc["org_name"]  = "Amazon";
    doc["org_year"]  = 1994;
    doc["rand_int"]  = 42;
    doc["rand_str"]  = "fizzbuzz";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // org_year should be of type int32
    auto schema = coll1->get_fields();
    ASSERT_EQ("org_year", schema[5].name);
    ASSERT_EQ(field_types::INT32, schema[5].type);

    auto res_op = coll1->search("Amazon", {"org_name"}, "", {"org_name"}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a facet field named `org_name` in the schema.", res_op.error());

    auto results = coll1->search("Amazon", {"org_name"}, "", {"org_year"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    res_op = coll1->search("fizzbuzz", {"rand_str"}, "", {"rand_str"}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_EQ("Could not find a facet field named `rand_str` in the schema.", res_op.error());

    results = coll1->search("fizzbuzz", {"rand_str"}, "", {"org_year"}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, WildcardFieldAndDictionaryField) {
    Collection *coll1;

    std::vector<field> fields = {field(".*", field_types::AUTO, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO, {}, {}, true);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["year"]  = 2000;
    doc["kinds"]  = nlohmann::json::object();
    doc["kinds"]["CGXX"]  = 13;
    doc["kinds"]["ZBXX"]  = 24;

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("*", {}, "year: 2000", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    auto schema = coll1->get_fields();
    ASSERT_EQ(5, schema.size());
    ASSERT_EQ(".*", schema[0].name);
    ASSERT_EQ("kinds", schema[1].name);
    ASSERT_EQ("year", schema[2].name);
    ASSERT_EQ("kinds.ZBXX", schema[3].name);
    ASSERT_EQ("kinds.CGXX", schema[4].name);

    // filter on object key
    results = coll1->search("*", {}, "kinds.CGXX: 13", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, DynamicFieldAndDictionaryField) {
    Collection *coll1;

    std::vector<field> fields = {field("k.*", field_types::STRING, false, true),
                                 field(".*", field_types::AUTO, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["kinds"]  = nlohmann::json::object();
    doc["kinds"]["CGXX"]  = 13;
    doc["kinds"]["ZBXX"]  = 24;

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `kinds` must be a string.", add_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, RegexpIntFieldWithFallbackStringType) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("n.*", field_types::INT32, false, true),
                                 field("s.*", "string*", false, true),
                                 field(".*", field_types::STRING, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::STRING);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["title"]  = "Amazon Inc.";
    doc["n_age"]  = 32;
    doc["s_tags"]  = {"shopping"};
    doc["rand_str"]  = "fizzbuzz";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // n_age should be of type int32
    auto schema = coll1->get_fields();

    ASSERT_EQ("n_age", schema[4].name);
    ASSERT_EQ(field_types::INT32, schema[4].type);

    ASSERT_EQ("rand_str", schema[5].name);
    ASSERT_EQ(field_types::STRING, schema[5].type);

    ASSERT_EQ("s_tags", schema[6].name);
    ASSERT_EQ(field_types::STRING_ARRAY, schema[6].type);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, ContainingWildcardOnlyField) {
    Collection *coll1;

    std::vector<field> fields = {field("company_name", field_types::STRING, false),
                                 field("num_employees", field_types::INT32, false),
                                 field(".*", field_types::BOOL, true, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::BOOL);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["company_name"]  = "Amazon Inc.";
    doc["num_employees"]  = 2000;
    doc["country"]  = "USA";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `country` must be a bool.", add_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, DoNotIndexFieldMarkedAsNonIndex) {
    Collection *coll1;

    std::vector<field> fields = {field("company_name", field_types::STRING, false),
                                 field("num_employees", field_types::INT32, false),
                                 field("post", field_types::STRING, false, true, false),
                                 field(".*_txt", field_types::STRING, false, true, false),
                                 field(".*", field_types::AUTO, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["company_name"]  = "Amazon Inc.";
    doc["num_employees"]  = 2000;
    doc["post"]  = "Some post.";
    doc["description_txt"]  = "Rome was not built in a day.";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().count("post"));

    auto res_op = coll1->search("Amazon", {"description_txt"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `description_txt` in the schema.", res_op.error());

    res_op = coll1->search("Amazon", {"post"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Field `post` is marked as a non-indexed field in the schema.", res_op.error());

    // wildcard pattern should exclude non-indexed field while searching,
    res_op = coll1->search("Amazon", {"*"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["hits"].size());

    // try updating a document with non-indexable field
    doc["post"] = "Some post updated.";
    auto update_op = coll1->add(doc.dump(), UPDATE, "0");
    ASSERT_TRUE(add_op.ok());

    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().count("post"));

    auto res = coll1->search("Amazon", {"company_name"}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ("Some post updated.", res["hits"][0]["document"]["post"].get<std::string>());

    // try to delete doc with non-indexable field
    auto del_op = coll1->remove("0");
    ASSERT_TRUE(del_op.ok());

    // facet search should also be disabled
    auto fs_op = coll1->search("Amazon", {"company_name"}, "", {"description_txt"}, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(fs_op.ok());
    ASSERT_EQ("Could not find a facet field named `description_txt` in the schema.", fs_op.error());

    fields = {field("company_name", field_types::STRING, false),
              field("num_employees", field_types::INT32, false),
              field("post", field_types::STRING, false, false, false),
              field(".*_txt", field_types::STRING, true, true, false),
              field(".*", field_types::AUTO, false, true)};

    auto op = collectionManager.create_collection("coll2", 1, fields, "", 0, field_types::AUTO);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Field `.*_txt` cannot be a facet since it's marked as non-indexable.", op.error());

    fields = {field("company_name", field_types::STRING, false),
              field("num_employees", field_types::INT32, false),
              field("post", field_types::STRING, false, true, false),
              field(".*_txt", field_types::STRING, true, false, false),
              field(".*", field_types::AUTO, false, true)};

    op = collectionManager.create_collection("coll2", 1, fields, "", 0, field_types::AUTO);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Field `.*_txt` must be an optional field.", op.error());

    // don't allow catch all field to be non-indexable

    fields = {field("company_name", field_types::STRING, false),
              field("num_employees", field_types::INT32, false),
              field(".*_txt", field_types::STRING, false, true, false),
              field(".*", field_types::AUTO, false, true, false)};

    op = collectionManager.create_collection("coll2", 1, fields, "", 0, field_types::AUTO);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Field `.*` cannot be marked as non-indexable.", op.error());

    // allow auto field to be non-indexable

    fields = {field("company_name", field_types::STRING, false),
              field("num_employees", field_types::INT32, false),
              field("noidx_.*", field_types::AUTO, false, true, false)};

    op = collectionManager.create_collection("coll3", 1, fields, "", 0, field_types::AUTO);
    ASSERT_TRUE(op.ok());

    // don't allow facet to be true when index is false
    fields = {field("company_name", field_types::STRING, false),
              field("num_employees", field_types::INT32, false),
              field("facet_noindex", field_types::STRING, true, true, false)};

    op = collectionManager.create_collection("coll4", 1, fields, "", 0, field_types::AUTO);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Field `facet_noindex` cannot be a facet since it's marked as non-indexable.", op.error());

    collectionManager.drop_collection("coll1");
    collectionManager.drop_collection("coll2");
    collectionManager.drop_collection("coll3");
    collectionManager.drop_collection("coll4");
}

TEST_F(CollectionAllFieldsTest, NullValueUpdate) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false, true),
                                 field(".*_name", field_types::STRING, true, true),
                                 field("unindexed", field_types::STRING, false, true, false),
                                 field(".*", field_types::STRING, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::STRING);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["id"]  = "0";
    doc["title"]  = "Running Shoes";
    doc["company_name"]  = "Nike";
    doc["country"]  = "USA";
    doc["unindexed"]  = "Hello";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    doc["title"] = nullptr;
    doc["company_name"]  = nullptr;
    doc["country"]  = nullptr;

    add_op = coll1->add(doc.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    // try updating the doc with null value again
    add_op = coll1->add(doc.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    // ensure that the fields are removed from the document
    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(2, results["hits"][0]["document"].size());
    ASSERT_EQ("Hello", results["hits"][0]["document"]["unindexed"].get<std::string>());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, NullValueArrayUpdate) {
    Collection *coll1;

    std::vector<field> fields = {field("titles", field_types::STRING_ARRAY, false, true),
                                 field(".*_names", field_types::STRING_ARRAY, true, true),
                                 field("unindexed", field_types::STRING, false, true, false),
                                 field(".*", field_types::STRING_ARRAY, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::STRING_ARRAY);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["id"]  = "0";
    doc["titles"]  = {"Running Shoes"};
    doc["company_names"]  = {"Nike"};
    doc["countries"]  = {"USA", nullptr};
    doc["unindexed"]  = "Hello";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `countries` must be an array of string.", add_op.error());

    doc["countries"]  = {nullptr};
    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `countries` must be an array of string.", add_op.error());

    doc["countries"]  = {"USA"};
    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    ASSERT_EQ(1, coll1->get_num_documents());
    ASSERT_EQ(1, coll1->_get_index()->num_seq_ids());

    doc["titles"] = nullptr;
    doc["company_names"]  = nullptr;
    doc["countries"]  = nullptr;

    add_op = coll1->add(doc.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    // try updating the doc with null value again
    add_op = coll1->add(doc.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    ASSERT_EQ(1, coll1->get_num_documents());

    // ensure that the fields are removed from the document
    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(2, results["hits"][0]["document"].size());
    ASSERT_EQ("Hello", results["hits"][0]["document"]["unindexed"].get<std::string>());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // update with null values inside array
    doc["countries"]  = {nullptr};
    add_op = coll1->add(doc.dump(), UPDATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `countries` must be an array of string.", add_op.error());

    doc["countries"]  = {"USA", nullptr};
    add_op = coll1->add(doc.dump(), UPDATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `countries` must be an array of string.", add_op.error());

    ASSERT_EQ(1, coll1->get_num_documents());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, EmptyArrayShouldBeAcceptedAsFirstValueOfAutoField) {
    Collection *coll1;

    std::vector<field> fields = {field(".*", field_types::AUTO, false, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "",
                                                      0, field_types::AUTO);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    nlohmann::json doc;
    doc["company_name"]  = "Amazon Inc.";
    doc["tags"]  = nlohmann::json::array();
    doc["country"]  = "USA";

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, DISABLED_SchemaUpdateShouldBeAtomicForAllFields) {
    // when a given field in a document is "bad", other fields should not be partially added to schema
    Collection *coll1;

    std::vector<field> fields = {field(".*", field_types::AUTO, false, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        auto op = collectionManager.create_collection("coll1", 1, fields, "",
                                                      0, field_types::AUTO);
        ASSERT_TRUE(op.ok());
        coll1 = op.get();
    }

    // insert a document with bad data for that key, but surrounded by "good" keys
    // this should NOT end up creating schema changes
    nlohmann::json doc;
    doc["int_2"]  = 200;

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    doc["int_1"]  = 100;
    doc["int_2"]  = nlohmann::json::array();
    doc["int_2"].push_back(nlohmann::json::object());
    doc["int_3"]  = 300;

    add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());

    auto f = coll1->get_fields();

    ASSERT_EQ(1, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_sort_fields().size());
    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().size());
    ASSERT_EQ(0, coll1->_get_index()->_get_numerical_index().size());

    // now insert document with just "int_1" key
    nlohmann::json doc2;
    doc2["int_1"]  = 200;
    add_op = coll1->add(doc2.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_sort_fields().size());
    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().size());
    ASSERT_EQ(1, coll1->_get_index()->_get_numerical_index().size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, FieldNameMatchingRegexpShouldNotBeIndexed) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true),
                                 field("title", field_types::STRING, false),
                                 field("name.*", field_types::STRING, true, true)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "One Two Three";
    doc1["name.*"] = "Rowling";
    doc1["name.*barbaz"] = "JK";
    doc1[".*"] = "foo";

    std::vector<std::string> json_lines;
    json_lines.push_back(doc1.dump());

    coll1->add_many(json_lines, doc1, UPSERT);
    json_lines[0] = doc1.dump();
    coll1->add_many(json_lines, doc1, UPSERT);

    ASSERT_EQ(1, coll1->_get_index()->_get_search_index().size());
    ASSERT_EQ(3, coll1->get_fields().size());

    auto results = coll1->search("one", {"title"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionAllFieldsTest, FieldNameMatchingRegexpShouldNotBeIndexedInNonAutoSchema) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("name.*", field_types::STRING, true, true)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "One Two Three";
    doc1["name.*"] = "Rowling";
    doc1["name.*barbaz"] = "JK";
    doc1[".*"] = "foo";

    std::vector<std::string> json_lines;
    json_lines.push_back(doc1.dump());

    coll1->add_many(json_lines, doc1, UPSERT);
    json_lines[0] = doc1.dump();
    coll1->add_many(json_lines, doc1, UPSERT);

    ASSERT_EQ(1, coll1->_get_index()->_get_search_index().size());
    ASSERT_EQ(2, coll1->get_fields().size());

    auto results = coll1->search("one", {"title"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 5, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true).get();

    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionAllFieldsTest, EmbedFromFieldJSONInvalidField) {
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
    nlohmann::json field_json;
    field_json["name"] = "embedding";
    field_json["type"] = "float[]";
    field_json["embed"] = nlohmann::json::object();
    field_json["embed"]["from"] = {"name"};
    field_json["embed"]["model_config"] = nlohmann::json::object();
    field_json["embed"]["model_config"]["model_name"] = "ts/e5-small";

    std::vector<field> fields;
    std::string fallback_field_type;
    auto arr = nlohmann::json::array();
    arr.push_back(field_json);

    auto field_op = field::json_fields_to_fields(false, arr, fallback_field_type, fields);

    ASSERT_FALSE(field_op.ok());
    ASSERT_EQ("Property `embed.from` can only refer to string, string array or image (for supported models) fields.", field_op.error());
}

TEST_F(CollectionAllFieldsTest, EmbedFromNotArray) {
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
    nlohmann::json field_json;
    field_json["name"] = "embedding";
    field_json["type"] = "float[]";
    field_json["embed"] = nlohmann::json::object();
    field_json["embed"]["from"] = "name";
    field_json["embed"]["model_config"] = nlohmann::json::object();
    field_json["embed"]["model_config"]["model_name"] = "ts/e5-small";

    std::vector<field> fields;
    std::string fallback_field_type;
    auto arr = nlohmann::json::array();
    arr.push_back(field_json);

    auto field_op = field::json_fields_to_fields(false, arr, fallback_field_type, fields);

    ASSERT_FALSE(field_op.ok());
    ASSERT_EQ("Property `embed.from` must be an array.", field_op.error());
}

TEST_F(CollectionAllFieldsTest, ModelParametersWithoutEmbedFrom) {
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
    nlohmann::json field_json;
    field_json["name"] = "embedding";
    field_json["type"] = "float[]";
    field_json["embed"]["model_config"] = nlohmann::json::object();
    field_json["embed"]["model_config"]["model_name"] = "ts/e5-small";

    std::vector<field> fields;
    std::string fallback_field_type;
    auto arr = nlohmann::json::array();
    arr.push_back(field_json);

    auto field_op = field::json_fields_to_fields(false, arr, fallback_field_type, fields);
    ASSERT_FALSE(field_op.ok());
    ASSERT_EQ("Property `embed` must contain a `from` property.", field_op.error());
}

TEST_F(CollectionAllFieldsTest, EmbedFromBasicValid) {
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
    nlohmann::json schema = R"({
        "name": "obj_coll",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"],
                "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    auto obj_coll_op = collectionManager.create_collection(schema);

    ASSERT_TRUE(obj_coll_op.ok());
    Collection* obj_coll = obj_coll_op.get();

    nlohmann::json doc1;
    doc1["name"] = "One Two Three";

    auto add_res = obj_coll->add(doc1.dump());

    ASSERT_TRUE(add_res.ok());
    ASSERT_TRUE(add_res.get()["name"].is_string());
    ASSERT_TRUE(add_res.get()["embedding"].is_array());
    ASSERT_EQ(384, add_res.get()["embedding"].size());

}

TEST_F(CollectionAllFieldsTest, WrongDataTypeForEmbedFrom) {
    nlohmann::json schema = R"({
        "name": "obj_coll",
        "fields": [
            {"name": "age", "type": "int32"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["age"],
                "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    auto obj_coll_op = collectionManager.create_collection(schema);

    ASSERT_FALSE(obj_coll_op.ok());
    ASSERT_EQ("Property `embed.from` can only refer to string, string array or image (for supported models) fields.", obj_coll_op.error());
}

TEST_F(CollectionAllFieldsTest, StoreInvalidInput) {
        nlohmann::json schema = R"({
        "name": "obj_coll",
        "fields": [
            {"name": "age", "type": "int32", "store": "qwerty"}
        ]
    })"_json;


    auto obj_coll_op = collectionManager.create_collection(schema);

    ASSERT_FALSE(obj_coll_op.ok());
    ASSERT_EQ("The `store` property of the field `age` should be a boolean.", obj_coll_op.error());
}

TEST_F(CollectionAllFieldsTest, InvalidstemValue) {
    nlohmann::json schema = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string", "stem": "qwerty"}
        ]
    })"_json;
    
    auto obj_coll_op = collectionManager.create_collection(schema);
    ASSERT_FALSE(obj_coll_op.ok());
    ASSERT_EQ("The `stem` property of the field `name` should be a boolean.", obj_coll_op.error());

    schema = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "int32", "stem": true}
        ]
    })"_json;

    obj_coll_op = collectionManager.create_collection(schema);
    ASSERT_FALSE(obj_coll_op.ok());
    ASSERT_EQ("The `stem` property is only allowed for string and string[] fields.", obj_coll_op.error());
}

