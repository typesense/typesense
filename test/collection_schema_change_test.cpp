#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionSchemaChangeTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_schema_change";
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

TEST_F(CollectionSchemaChangeTest, AddNewFieldsToCollection) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The quick brown fox was too fast.";
    doc["tags"] = {"experimental", "news"};
    doc["category"] = "animals";
    doc["quantity"] = 100;
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("fox",
                                 {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    auto schema_changes = R"({
        "fields": [
            {"name": "tags", "type": "string[]", "infix": true},
            {"name": "category", "type": "string", "sort": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    results = coll1->search("eriment",
                            {"tags"}, "", {}, {}, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    sort_fields = { sort_by("category", "DESC") };
    results = coll1->search("*",
                            {}, "", {}, sort_fields, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    schema_changes = R"({
        "fields": [
            {"name": "quantity", "type": "int32", "facet": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    results = coll1->search("*",
                            {}, "quantity: 100", {"quantity"}, {}, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("100", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);

    // add a dynamic field
    schema_changes = R"({
        "fields": [
            {"name": ".*_bool", "type": "bool"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    doc["id"] = "1";
    doc["title"] = "The one";
    doc["tags"] = {"sports", "news"};
    doc["category"] = "things";
    doc["quantity"] = 200;
    doc["points"] = 100;
    doc["on_sale_bool"] = true;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    results = coll1->search("*",
                            {}, "on_sale_bool: true", {}, {}, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // add auto field
    schema_changes = R"({
        "fields": [
            {"name": ".*", "type": "auto"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    doc["id"] = "2";
    doc["title"] = "The two";
    doc["tags"] = {"sports", "news"};
    doc["category"] = "things";
    doc["quantity"] = 200;
    doc["points"] = 100;
    doc["on_sale_bool"] = false;
    doc["foobar"] = 123;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());
    results = coll1->search("*",
                            {}, "foobar: 123", {}, {}, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, true,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("2", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // try to add auto field again
    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("The schema already contains a `.*` field.", alter_op.error());

    // try to add a regular field with 2 auto fields
    schema_changes = R"({
        "fields": [
            {"name": "bar", "type": "auto"},
            {"name": ".*", "type": "auto"},
            {"name": ".*", "type": "auto"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("There can be only one field named `.*`.", alter_op.error());

    // add non-index field
    schema_changes = R"({
        "fields": [
            {"name": "raw", "type": "int32", "index": false, "optional": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    ASSERT_EQ(8, coll1->get_schema().size());
    ASSERT_EQ(9, coll1->get_fields().size());

    ASSERT_EQ(4, coll1->_get_index()->_get_numerical_index().size());

    // try restoring collection from disk: all fields should be preserved
    collectionManager.dispose();
    delete store;

    store = new Store("/tmp/typesense_test/collection_schema_change");
    collectionManager.init(store, 1.0, "auth_key", quit);
    collectionManager.load(8, 1000);
    coll1 = collectionManager.get_collection("coll1").get();

    ASSERT_EQ(8, coll1->get_schema().size());
    ASSERT_EQ(9, coll1->get_fields().size());

    ASSERT_EQ(4, coll1->_get_index()->_get_numerical_index().size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSchemaChangeTest, DropFieldsFromCollection) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false),
                                 field("title", field_types::STRING, false, false, true, "", 1, 1),
                                 field("location", field_types::GEOPOINT, false),
                                 field("locations", field_types::GEOPOINT_ARRAY, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points", 0, "auto").get();

    std::vector<std::vector<double>> lat_lngs;
    lat_lngs.push_back({48.85821022164442, 2.294239067890161});

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The quick brown fox was too fast.";
    doc["location"] = {48.85821022164442, 2.294239067890161};
    doc["locations"] = lat_lngs;
    doc["tags"] = {"experimental", "news"};
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("*",
                                 {}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    auto schema_changes = R"({
        "fields": [
            {"name": ".*", "drop": true},
            {"name": "title", "drop": true},
            {"name": "location", "drop": true},
            {"name": "locations", "drop": true},
            {"name": "tags", "drop": true},
            {"name": "points", "drop": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    results = coll1->search("*", {}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    auto res_op = coll1->search("quick", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `title` in the schema.", res_op.error());

    auto search_schema = coll1->get_schema();
    ASSERT_EQ(0, search_schema.size());

    auto coll_fields = coll1->get_fields();
    ASSERT_EQ(0, coll_fields.size());

    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().size());
    ASSERT_EQ(0, coll1->_get_index()->_get_numerical_index().size());
    ASSERT_EQ(0, coll1->_get_index()->_get_infix_index().size());
    ASSERT_EQ(1, coll1->_get_index()->num_seq_ids());
    ASSERT_EQ("", coll1->get_fallback_field_type());
    ASSERT_EQ("", coll1->get_default_sorting_field());

    // try restoring collection from disk: all fields should be deleted
    collectionManager.dispose();
    delete store;

    store = new Store("/tmp/typesense_test/collection_schema_change");
    collectionManager.init(store, 1.0, "auth_key", quit);
    collectionManager.load(8, 1000);
    coll1 = collectionManager.get_collection("coll1").get();

    search_schema = coll1->get_schema();
    ASSERT_EQ(0, search_schema.size());

    coll_fields = coll1->get_fields();
    ASSERT_EQ(0, coll_fields.size());

    ASSERT_EQ(0, coll1->_get_index()->_get_search_index().size());
    ASSERT_EQ(0, coll1->_get_index()->_get_numerical_index().size());
    ASSERT_EQ(0, coll1->_get_index()->_get_infix_index().size());
    ASSERT_EQ(1, coll1->_get_index()->num_seq_ids());
    ASSERT_EQ("", coll1->get_default_sorting_field());
    ASSERT_EQ("", coll1->get_fallback_field_type());

    results = coll1->search("*", {}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    res_op = coll1->search("quick", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `title` in the schema.", res_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSchemaChangeTest, AlterValidations) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", 1, 1),
                                 field("location", field_types::GEOPOINT, false),
                                 field("locations", field_types::GEOPOINT_ARRAY, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points", 0, "").get();

    std::vector<std::vector<double>> lat_lngs;
    lat_lngs.push_back({48.85821022164442, 2.294239067890161});

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The quick brown fox was too fast.";
    doc["location"] = {48.85821022164442, 2.294239067890161};
    doc["locations"] = lat_lngs;
    doc["tags"] = {"experimental", "news"};
    doc["desc"] = "Story about fox.";
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // 1. Modify existing field, which is not supported

    auto schema_changes = R"({
        "fields": [
            {"name": "title", "type": "string[]"}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Field `title` is already part of schema: only field additions and deletions are supported for now.",
              alter_op.error());

    // 2. Bad field format
    schema_changes = R"({
        "fields": [
            {"name": "age", "typezzz": "int32"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Wrong format for `fields`. It should be an array of objects containing `name`, `type`, "
              "`optional` and `facet` properties.",alter_op.error());

    // 3. Try to drop non-existing field
    schema_changes = R"({
        "fields": [
            {"name": "age", "drop": true}
        ]
    })"_json;
    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Field `age` is not part of collection schema.",alter_op.error());

    // 4. Bad value for `drop` parameter
    schema_changes = R"({
        "fields": [
            {"name": "title", "drop": 123}
        ]
    })"_json;
    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Field `title` must have a drop value of `true`.", alter_op.error());

    // 5. New field schema should match on-disk data
    schema_changes = R"({
        "fields": [
            {"name": "desc", "type": "int32"}
        ]
    })"_json;
    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Schema change does not match on-disk data, error: Field `desc` must be an int32.", alter_op.error());

    // 6. Prevent non-optional field when on-disk data has missing values
    doc.clear();
    doc["id"] = "1";
    doc["title"] = "The brown lion was too slow.";
    doc["location"] = {68.85821022164442, 4.294239067890161};
    doc["locations"] = lat_lngs;
    doc["tags"] = {"lion", "zoo"};
    doc["points"] = 200;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    schema_changes = R"({
        "fields": [
            {"name": "desc", "type": "string", "optional": false}
        ]
    })"_json;
    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Schema change does not match on-disk data, error: Field `desc` has been declared in the "
              "schema, but is not found in the document.", alter_op.error());

    collectionManager.drop_collection("coll1");
}
