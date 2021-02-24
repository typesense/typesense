#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionAllFieldsTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_all_fields";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
        collectionManager.load();
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

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        auto coll_op = collectionManager.create_collection("coll1", 1, fields, "", 0, schema_detect_types::AUTO);
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
    auto results = coll1->search("will", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("chris", {"cast"}, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

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

    results = coll1->search("300", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("300", results["hits"][0]["document"]["title"].get<std::string>().c_str());

    // with dirty values set to `COERCE_OR_DROP`
    // `cast` field should not be indexed into store
    doc_json = R"({"cast":"William Barnes","points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title":"With bad cast field."})";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("With bad cast field", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("With bad cast field.", results["hits"][0]["document"]["title"].get<std::string>().c_str());
    ASSERT_EQ(0, results["hits"][0]["document"].count("cast"));

    // with dirty values set to `DROP`
    // no coercion should happen, `title` field will just be dropped, but record indexed
    doc_json = R"({"cast": ["Jeremy Livingston"],"points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title": 1200 })";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("1200", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("Jeremy Livingston", {"cast"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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
    auto res_op = coll1->search("*", {}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `not-found` in the schema for sorting.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, HandleArrayTypes) {
    Collection *coll1;

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, schema_detect_types::AUTO).get();
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

    auto results = coll1->search("second", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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
    results = coll1->search("third", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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
        coll1 = collectionManager.create_collection("coll1", 4, fields, "", 0, schema_detect_types::AUTO).get();
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

    auto results = coll1->search("second", {"title"}, "", {}, {}, 0, 10, 1, FREQUENCY, false).get();

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

    results = coll1->search("updated", {"title"}, "", {}, {}, 0, 50, 1, FREQUENCY, false).get();
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
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, schema_detect_types::STRINGIFY).get();
    }

    nlohmann::json doc;
    doc["title"] = "FIRST";
    doc["int_values"] = {1, 2};

    Option<nlohmann::json> add_op = coll1->add(doc.dump(), CREATE, "0");
    ASSERT_TRUE(add_op.ok());
    auto added_doc = add_op.get();

    ASSERT_EQ("1", added_doc["int_values"][0].get<std::string>());
    ASSERT_EQ("2", added_doc["int_values"][1].get<std::string>());

    auto results = coll1->search("first", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
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

    results = coll1->search("second", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("SECOND", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(1, results["hits"][0]["document"].count("int_values"));
    ASSERT_EQ(0, results["hits"][0]["document"]["int_values"].size());  // since both array values are dropped

    // try with REJECT
    doc["title"] = "THIRD";
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());

    // singular field coercion
    doc["single_int"] = 100;
    doc["title"] = "FOURTH";

    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());

    // uncoercable field, e.g. nested dict
    doc["dict"] = nlohmann::json::object();
    doc["dict"]["one"] = 1;
    doc["dict"]["two"] = 2;

    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Type of field `dict` is invalid.", add_op.error());

    // try with coerce_or_reject
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Type of field `dict` is invalid.", add_op.error());

    // try with drop
    doc["title"] = "FIFTH";
    add_op = coll1->add(doc.dump(), CREATE, "", DIRTY_VALUES::DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("fifth", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("FIFTH", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(0, results["hits"][0]["document"].count("dict"));

    // try with coerce_or_drop
    doc["title"] = "SIXTH";
    add_op = coll1->add(doc.dump(), CREATE, "66", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("sixth", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("SIXTH", results["hits"][0]["document"]["title"].get<std::string>());
    ASSERT_EQ(0, results["hits"][0]["document"].count("dict"));

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionAllFieldsTest, UpdateOfDocumentsInAutoMode) {
    Collection *coll1;

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, {}, "", 0, schema_detect_types::AUTO).get();
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

TEST_F(CollectionAllFieldsTest, JsonFieldsToFieldsConversion) {
    nlohmann::json fields_json = nlohmann::json::array();
    nlohmann::json all_field;
    all_field[fields::name] = "*";
    all_field[fields::type] = "stringify";
    fields_json.emplace_back(all_field);

    std::string auto_detect_schema;
    std::vector<field> fields;

    auto parse_op = field::json_fields_to_fields(fields_json, auto_detect_schema, fields);

    ASSERT_TRUE(parse_op.ok());
    ASSERT_EQ(1, fields.size());
    ASSERT_EQ("stringify", auto_detect_schema);
    ASSERT_EQ(true, fields[0].optional);
    ASSERT_EQ(false, fields[0].facet);
    ASSERT_EQ("*", fields[0].name);
    ASSERT_EQ("stringify", fields[0].type);

    // reject when you try to set optional to false or facet to true
    fields_json[0][fields::optional] = false;
    parse_op = field::json_fields_to_fields(fields_json, auto_detect_schema, fields);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_EQ("Field `*` must be an optional field.", parse_op.error());

    fields_json[0][fields::optional] = true;
    fields_json[0][fields::facet] = true;
    parse_op = field::json_fields_to_fields(fields_json, auto_detect_schema, fields);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_EQ("Field `*` cannot be a facet field.", parse_op.error());

    fields_json[0][fields::facet] = false;

    // can have only one "*" field
    fields_json.emplace_back(all_field);

    parse_op = field::json_fields_to_fields(fields_json, auto_detect_schema, fields);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_EQ("There can be only one field named `*`.", parse_op.error());

    // try with the `auto` type
    fields_json.clear();
    fields.clear();
    all_field[fields::type] = "auto";
    fields_json.emplace_back(all_field);

    parse_op = field::json_fields_to_fields(fields_json, auto_detect_schema, fields);
    ASSERT_TRUE(parse_op.ok());
    ASSERT_EQ("auto", fields[0].type);
}
