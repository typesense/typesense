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
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    sort_fields = { sort_by("category", "DESC") };
    results = coll1->search("*",
                            {}, "", {}, sort_fields, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
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
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("100", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);

    // add a dynamic field
    schema_changes = R"({
        "fields": [
            {"name": ".*_bool", "type": "bool"},
            {"name": "age", "type": "auto", "optional": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    auto coll_fields = coll1->get_fields();
    ASSERT_EQ(7, coll_fields.size());
    ASSERT_EQ(".*_bool", coll_fields[5].name);
    ASSERT_EQ("age", coll_fields[6].name);

    doc["id"] = "1";
    doc["title"] = "The one";
    doc["tags"] = {"sports", "news"};
    doc["category"] = "things";
    doc["quantity"] = 200;
    doc["points"] = 100;
    doc["on_sale_bool"] = true;
    doc["age"] = 45;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    results = coll1->search("*",
                            {}, "on_sale_bool: true", {}, {}, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {always}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("*",
                            {}, "age: 45", {}, {}, {0}, 3, 1, FREQUENCY,
                            {true}, 5,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
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
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
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

    // try to add `id` field
    schema_changes = R"({
        "fields": [
            {"name": "id", "type": "int32"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Field `id` cannot be altered.", alter_op.error());

    ASSERT_EQ(9, coll1->get_schema().size());
    ASSERT_EQ(12, coll1->get_fields().size());
    ASSERT_EQ(5, coll1->_get_index()->_get_numerical_index().size());

    // fields should also be persisted properly on disk
    std::string collection_meta_json;
    store->get(Collection::get_meta_key("coll1"), collection_meta_json);
    nlohmann::json collection_meta = nlohmann::json::parse(collection_meta_json);
    ASSERT_EQ(12, collection_meta["fields"].size());

    // try restoring collection from disk: all fields should be preserved
    collectionManager.dispose();
    delete store;

    store = new Store("/tmp/typesense_test/collection_schema_change");
    collectionManager.init(store, 1.0, "auth_key", quit);
    collectionManager.load(8, 1000);
    coll1 = collectionManager.get_collection("coll1").get();

    ASSERT_EQ(9, coll1->get_schema().size());
    ASSERT_EQ(12, coll1->get_fields().size());
    ASSERT_EQ(5, coll1->_get_index()->_get_numerical_index().size());

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

    // try to drop `id` field
    schema_changes = R"({
        "fields": [
            {"name": "id", "drop": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Field `id` cannot be altered.", alter_op.error());

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
    ASSERT_EQ("Field `title` is already part of the schema: To change this field, drop it first before adding it "
              "back to the schema.",alter_op.error());

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
    ASSERT_EQ("Schema change is incompatible with the type of documents already stored in this collection. "
              "Existing data for field `desc` cannot be coerced into an int32.", alter_op.error());

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
    ASSERT_EQ("Field `desc` has been declared in the schema, but is not found in the documents already present "
              "in the collection. If you still want to add this field, set it as `optional: true`.", alter_op.error());

    // 7. schema JSON missing "fields" property
    schema_changes = R"({
        "foo": "bar"
    })"_json;
    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("The `fields` value should be an array of objects containing the field `name` "
              "and other properties.", alter_op.error());

    // 8. sending full collection schema, like creation body
    schema_changes = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Only `fields` can be updated at the moment.",alter_op.error());

    // 9. bad datatype in alter
    schema_changes = R"({
        "fields": [
            {"name": "title", "drop": true},
            {"name": "title", "type": "foobar"}
        ]
    })"_json;
    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Field `title` has an invalid data type `foobar`, see docs for supported data types.",alter_op.error());

    // add + drop `id` field
    schema_changes = R"({
        "fields": [
            {"name": "id", "drop": true},
            {"name": "id", "type": "string"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Field `id` cannot be altered.", alter_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSchemaChangeTest, DropPropertyShouldNotBeAllowedInSchemaCreation) {
    nlohmann::json req_json = R"({
        "name": "coll1",
        "fields": [{"name": "title", "type": "string", "drop": true}]
    })"_json;

    auto coll1_op = collectionManager.create_collection(req_json);
    ASSERT_FALSE(coll1_op.ok());
    ASSERT_EQ("Invalid property `drop` on field `title`: it is allowed only during schema update.", coll1_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSchemaChangeTest, AbilityToDropAndReAddIndexAtTheSameTime) {
    nlohmann::json req_json = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "timestamp", "type": "int32"}
        ]
    })"_json;

    auto coll1_op = collectionManager.create_collection(req_json);
    ASSERT_TRUE(coll1_op.ok());

    auto coll1 = coll1_op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Hello";
    doc["timestamp"] = 3433232;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // try to alter with a bad type

    auto schema_changes = R"({
        "fields": [
            {"name": "title", "drop": true},
            {"name": "title", "type": "int32"}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Schema change is incompatible with the type of documents already stored in this collection. "
              "Existing data for field `title` cannot be coerced into an int32.", alter_op.error());

    // existing data should not have been touched
    auto res = coll1->search("he", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, 10).get();
    ASSERT_EQ(1, res["hits"].size());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());

    // drop re-add with facet index
    schema_changes = R"({
        "fields": [
            {"name": "title", "drop": true},
            {"name": "title", "type": "string", "facet": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    res = coll1->search("*",
                        {}, "", {"title"}, {}, {0}, 3, 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, res["found"].get<size_t>());
    ASSERT_EQ("0", res["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ(1, res["facet_counts"].size());
    ASSERT_EQ(4, res["facet_counts"][0].size());
    ASSERT_EQ("title", res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(1, res["facet_counts"][0]["counts"].size());
    ASSERT_EQ("Hello", res["facet_counts"][0]["counts"][0]["value"].get<std::string>());

    // migrate int32 to int64
    schema_changes = R"({
        "fields": [
            {"name": "timestamp", "drop": true},
            {"name": "timestamp", "type": "int64"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    ASSERT_EQ("int64", coll1->get_schema()["timestamp"].type);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionSchemaChangeTest, AddAndDropFieldImmediately) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", 1, 1),
                                 field("points", field_types::INT32, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points", 0, "").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The quick brown fox was too fast.";
    doc["points"] = 100;
    doc["quantity_int"] = 1000;
    doc["some_txt"] = "foo";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());
    ASSERT_EQ(2, coll1->get_schema().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());

    auto results = coll1->search("*",
                                 {}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // add a field via alter which we will try dropping later
    auto schema_changes = R"({
        "fields": [
            {"name": ".*_int", "type": "int32", "optional": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());
    ASSERT_EQ(3, coll1->get_schema().size());
    ASSERT_EQ(4, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    results = coll1->search("*",
                            {}, "quantity_int: 1000", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // drop + re-add dynamic field
    schema_changes = R"({
        "fields": [
            {"name": ".*_int", "type": "int32", "facet": true},
            {"name": ".*_int", "drop": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    ASSERT_EQ(3, coll1->get_schema().size());
    ASSERT_EQ(4, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    results = coll1->search("*",
                            {}, "", {"quantity_int"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("quantity_int", results["facet_counts"][0]["field_name"].get<std::string>());

    schema_changes = R"({
        "fields": [
            {"name": ".*_int", "drop": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    ASSERT_EQ(2, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());

    // with bad on-disk data
    schema_changes = R"({
        "fields": [
            {"name": ".*_txt", "type": "int32"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Schema change is incompatible with the type of documents already stored in this collection. "
              "Existing data for field `some_txt` cannot be coerced into an int32.", alter_op.error());

    ASSERT_EQ(2, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());
}

TEST_F(CollectionSchemaChangeTest, AddDynamicFieldMatchingMultipleFields) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", 1, 1),
                                 field("points", field_types::INT32, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points", 0, "").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The quick brown fox was too fast.";
    doc["points"] = 100;
    doc["quantity_int"] = 1000;
    doc["year_int"] = 2020;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());
    ASSERT_EQ(2, coll1->get_schema().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());

    // add a dynamic field via alter that will target both _int fields
    auto schema_changes = R"({
        "fields": [
            {"name": ".*_int", "type": "int32", "optional": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());
    ASSERT_EQ(4, coll1->get_schema().size());
    ASSERT_EQ(5, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    auto results = coll1->search("*",
                            {}, "quantity_int: 1000", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*",
                            {}, "year_int: 2020", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // drop + re-add dynamic field that targets 2 underlying fields
    schema_changes = R"({
        "fields": [
            {"name": ".*_int", "type": "int32", "facet": true},
            {"name": ".*_int", "drop": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    ASSERT_EQ(4, coll1->get_schema().size());
    ASSERT_EQ(5, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    results = coll1->search("*",
                            {}, "", {"quantity_int"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("quantity_int", results["facet_counts"][0]["field_name"].get<std::string>());

    results = coll1->search("*",
                            {}, "", {"year_int"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("year_int", results["facet_counts"][0]["field_name"].get<std::string>());

    schema_changes = R"({
        "fields": [
            {"name": ".*_int", "drop": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    ASSERT_EQ(2, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());
}

TEST_F(CollectionSchemaChangeTest, DropFieldNotExistingInDocuments) {
    // optional title field
    std::vector<field> fields = {field("title", field_types::STRING, false, true, true, "", 1, 1),
                                 field("points", field_types::INT32, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points", 0, "").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto schema_changes = R"({
        "fields": [
            {"name": "title", "drop": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());
}

TEST_F(CollectionSchemaChangeTest, ChangeFieldToCoercableTypeIsAllowed) {
    // optional title field
    std::vector<field> fields = {field("title", field_types::STRING, false, true, true, "", 1, 1),
                                 field("points", field_types::INT32, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points", 0, "").get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["points"] = 100;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // coerce field from int to string
    auto schema_changes = R"({
        "fields": [
            {"name": "points", "drop": true},
            {"name": "points", "type": "string"}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());
}

TEST_F(CollectionSchemaChangeTest, ChangeFromPrimitiveToDynamicField) {
    nlohmann::json req_json = R"({
        "name": "coll1",
        "fields": [
            {"name": "tags", "type": "string"}
        ]
    })"_json;

    auto coll1_op = collectionManager.create_collection(req_json);
    ASSERT_TRUE(coll1_op.ok());

    auto coll1 = coll1_op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["tags"] = "123";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(1, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());

    // try to alter to string* type

    auto schema_changes = R"({
        "fields": [
            {"name": "tags", "drop": true},
            {"name": "tags", "type": "string*", "facet": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    auto results = coll1->search("123", {"tags"}, "", {"tags"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    // go back to plain string type
    schema_changes = R"({
        "fields": [
            {"name": "tags", "drop": true},
            {"name": "tags", "type": "string", "facet": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    results = coll1->search("123", {"tags"}, "", {"tags"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(1, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());
}

TEST_F(CollectionSchemaChangeTest, ChangeFromPrimitiveToAutoField) {
    nlohmann::json req_json = R"({
        "name": "coll1",
        "fields": [
            {"name": "tags", "type": "string"}
        ]
    })"_json;

    auto coll1_op = collectionManager.create_collection(req_json);
    ASSERT_TRUE(coll1_op.ok());

    auto coll1 = coll1_op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["tags"] = "123";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(1, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());

    // try to alter to auto type

    auto schema_changes = R"({
        "fields": [
            {"name": "tags", "drop": true},
            {"name": "tags", "type": "auto", "facet": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    auto results = coll1->search("123", {"tags"}, "", {"tags"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    // go back to plain string type
    schema_changes = R"({
        "fields": [
            {"name": "tags", "drop": true},
            {"name": "tags", "type": "string", "facet": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    results = coll1->search("123", {"tags"}, "", {"tags"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(1, coll1->get_fields().size());
    ASSERT_EQ(0, coll1->get_dynamic_fields().size());
}

TEST_F(CollectionSchemaChangeTest, ChangeFromStringStarToAutoField) {
    nlohmann::json req_json = R"({
        "name": "coll1",
        "fields": [
            {"name": "tags", "type": "string*"}
        ]
    })"_json;

    auto coll1_op = collectionManager.create_collection(req_json);
    ASSERT_TRUE(coll1_op.ok());

    auto coll1 = coll1_op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["tags"] = "123";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    // try to alter to auto type

    auto schema_changes = R"({
        "fields": [
            {"name": "tags", "drop": true},
            {"name": "tags", "type": "auto", "facet": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    auto results = coll1->search("123", {"tags"}, "", {"tags"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());

    // go back to string* type
    schema_changes = R"({
        "fields": [
            {"name": "tags", "drop": true},
            {"name": "tags", "type": "string*", "facet": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    results = coll1->search("123", {"tags"}, "", {"tags"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    ASSERT_EQ(1, coll1->get_schema().size());
    ASSERT_EQ(2, coll1->get_fields().size());
    ASSERT_EQ(1, coll1->get_dynamic_fields().size());
}

TEST_F(CollectionSchemaChangeTest, OrderOfDropShouldNotMatter) {
    nlohmann::json req_json = R"({
        "name": "coll1",
        "fields": [
            {"name": "loc", "type": "geopoint"}
        ]
    })"_json;

    auto coll1_op = collectionManager.create_collection(req_json);
    ASSERT_TRUE(coll1_op.ok());

    auto coll1 = coll1_op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["loc"] = {1, 2};

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // try to alter to a bad type (int32)

    auto schema_changes = R"({
        "fields": [
            {"name": "loc", "type": "int32"},
            {"name": "loc", "drop": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());

    schema_changes = R"({
        "fields": [
            {"name": "loc", "drop": true},
            {"name": "loc", "type": "int32"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
}

TEST_F(CollectionSchemaChangeTest, IndexFalseToTrue) {
    nlohmann::json req_json = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string", "index": false, "facet": false, "optional": true}
        ]
    })"_json;

    auto coll1_op = collectionManager.create_collection(req_json);
    ASSERT_TRUE(coll1_op.ok());

    auto coll1 = coll1_op.get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Typesense";

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // make field indexable

    auto schema_changes = R"({
        "fields": [
            {"name": "title", "drop": true},
            {"name": "title", "type": "string", "index": true, "facet": true, "optional": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    auto res_op = coll1->search("type", {"title"}, "", {"title"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5);
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());
    ASSERT_EQ(1, res_op.get()["facet_counts"].size());
}

TEST_F(CollectionSchemaChangeTest, DropGeoPointArrayField) {
    // when a value is `null` initially, and is altered, subsequent updates should not fail
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "geoloc", "type": "geopoint[]"}
        ]
    })"_json;

    auto coll_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(coll_create_op.ok());
    Collection* coll1 = coll_create_op.get();

    nlohmann::json doc = R"({
        "geoloc": [[10, 20]]
    })"_json;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto schema_changes = R"({
        "fields": [
            {"name": "geoloc", "drop": true},
            {"name": "_geoloc", "type": "geopoint[]", "optional": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());
}

TEST_F(CollectionSchemaChangeTest, AddingFieldWithExistingNullValue) {
    // when a value is `null` initially, and is altered, subsequent updates should not fail
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
    doc["num"] = nullptr;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto schema_changes = R"({
        "fields": [
            {"name": "num", "type": "int32", "optional": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    // now try updating the doc
    doc["id"] = "0";
    doc["title"] = "Sample Title 1";
    doc["num"] = 100;
    ASSERT_TRUE(coll1->add(doc.dump(), UPSERT).ok());

    auto res = coll1->search("*", {}, "num:100", {}, {}, {2}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, res["hits"].size());
}

TEST_F(CollectionSchemaChangeTest, DropIntegerFieldAndAddStringValues) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": ".*", "type": "auto"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    // index a label field as integer

    nlohmann::json doc;
    doc["id"] = "0";
    doc["label"] = "hello";
    doc["title"] = "Foo";
    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // drop this field from schema
    auto schema_changes = R"({
        "fields": [
            {"name": "label", "drop": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    // add new document with a string label
    doc["id"] = "1";
    doc["label"] = 1000;
    doc["title"] = "Bar";
    add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // now we have documents which have both string and integer for the same field :BOOM:
    // schema change operation should not be allowed at this point
    schema_changes = R"({
        "fields": [
            {"name": "year", "type": "int32", "optional": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_FALSE(alter_op.ok());
    ASSERT_EQ("Schema change is incompatible with the type of documents already stored in this collection. "
              "Existing data for field `label` cannot be coerced into an int64.", alter_op.error());

    // but should allow the problematic field to be dropped
    schema_changes = R"({
        "fields": [
            {"name": "label", "drop": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    // add document with another field
    doc["id"] = "2";
    doc["label"] = "xyz";
    doc["year"] = 1947;
    add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // try searching for string label
    auto res_op = coll1->search("xyz", {"label"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5);
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());
}

TEST_F(CollectionSchemaChangeTest, NestedFieldExplicitSchemaDropping) {
    // Plain object field
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "person", "type": "object"},
            {"name": "school.city", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "Test";
    doc["person"] = nlohmann::json::object();
    doc["person"]["name"] = "Jack";
    doc["school"] = nlohmann::json::object();
    doc["school"]["city"] = "NYC";

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto fields = coll1->get_fields();
    auto schema_map = coll1->get_schema();

    ASSERT_EQ(4, fields.size());
    ASSERT_EQ(4, schema_map.size());
    ASSERT_EQ(2, coll1->get_nested_fields().size());

    // drop object field

    auto schema_changes = R"({
        "fields": [
            {"name": "person", "drop": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    fields = coll1->get_fields();
    schema_map = coll1->get_schema();

    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(2, schema_map.size());
    ASSERT_EQ(1, coll1->get_nested_fields().size());

    // drop primitive nested field

    schema_changes = R"({
        "fields": [
            {"name": "school.city", "drop": true}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    fields = coll1->get_fields();
    schema_map = coll1->get_schema();

    ASSERT_EQ(1, fields.size());
    ASSERT_EQ(1, schema_map.size());
    ASSERT_EQ(0, coll1->get_nested_fields().size());
}

TEST_F(CollectionSchemaChangeTest, NestedFieldSchemaAdditions) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "Test";
    doc["person"] = nlohmann::json::object();
    doc["person"]["name"] = "Jack";
    doc["school"] = nlohmann::json::object();
    doc["school"]["city"] = "NYC";
    doc["school"]["state"] = "NY";

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto fields = coll1->get_fields();
    auto schema_map = coll1->get_schema();

    ASSERT_EQ(1, fields.size());
    ASSERT_EQ(1, schema_map.size());
    ASSERT_EQ(0, coll1->get_nested_fields().size());

    // add plain object field

    auto schema_changes = R"({
        "fields": [
            {"name": "person", "type": "object"}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    fields = coll1->get_fields();
    schema_map = coll1->get_schema();

    ASSERT_EQ(3, fields.size());
    ASSERT_EQ(3, schema_map.size());
    ASSERT_EQ(1, coll1->get_nested_fields().size());

    // nested primitive field

    schema_changes = R"({
        "fields": [
            {"name": "school.city", "type": "string"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    fields = coll1->get_fields();
    schema_map = coll1->get_schema();

    ASSERT_EQ(4, fields.size());
    ASSERT_EQ(4, schema_map.size());
    ASSERT_EQ(2, coll1->get_nested_fields().size());

    // try searching on new fields
    auto res_op = coll1->search("jack", {"person.name"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5);
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    res_op = coll1->search("nyc", {"school.city"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5);
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());
}

TEST_F(CollectionSchemaChangeTest, DropAndReAddNestedObject) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "person", "type": "object"},
            {"name": "school.city", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "Test";
    doc["person"] = nlohmann::json::object();
    doc["person"]["name"] = "Jack";
    doc["school"] = nlohmann::json::object();
    doc["school"]["city"] = "NYC";

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto fields = coll1->get_fields();
    auto schema_map = coll1->get_schema();

    ASSERT_EQ(4, fields.size());
    ASSERT_EQ(4, schema_map.size());

    // drop + re-add object field

    auto schema_changes = R"({
        "fields": [
            {"name": "person", "drop": true},
            {"name": "person", "type": "object"}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    fields = coll1->get_fields();
    schema_map = coll1->get_schema();

    auto res_op = coll1->search("jack", {"person.name"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5);
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    ASSERT_EQ(4, fields.size());
    ASSERT_EQ(4, schema_map.size());

    // drop + re-add school

    schema_changes = R"({
        "fields": [
            {"name": "school.city", "drop": true},
            {"name": "school.city", "type": "string"}
        ]
    })"_json;

    alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    fields = coll1->get_fields();
    schema_map = coll1->get_schema();

    ASSERT_EQ(4, fields.size());
    ASSERT_EQ(4, schema_map.size());
}

TEST_F(CollectionSchemaChangeTest, UpdateAfterNestedNullValue) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "lines", "optional": false, "type": "object[]"},
            {"name": "lines.name", "optional": true, "type": "string[]"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc = R"(
        {"id": "1", "lines": [{"name": null}]}
     )"_json;

    auto add_op = coll1->add(doc.dump(), CREATE, "1", DIRTY_VALUES::DROP);
    ASSERT_TRUE(add_op.ok());

    // add new field

    auto schema_changes = R"({
        "fields": [
            {"name": "title", "type": "string", "optional": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());
}

TEST_F(CollectionSchemaChangeTest, AlterShouldBeAbleToHandleFieldValueCoercion) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "product", "optional": false, "type": "object"},
            {"name": "product.price", "type": "int64"},
            {"name": "title", "type": "string"},
            {"name": "description", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc = R"(
        {"id": "0", "product": {"price": 56.45}, "title": "Title 1", "description": "Description 1"}
     )"_json;

    auto add_op = coll1->add(doc.dump(), CREATE, "0", DIRTY_VALUES::COERCE_OR_REJECT);
    ASSERT_TRUE(add_op.ok());

    // drop a field

    auto schema_changes = R"({
        "fields": [
            {"name": "description", "drop": true}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());
}

TEST_F(CollectionSchemaChangeTest, GeoFieldSchemaAddition) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["title"] = "Title 1";
    doc["location"] = {22.847641, 89.5405279};

    coll1->add(doc.dump());
    doc["title"] = "Title 2";
    doc["location"] = {22.8951791, 89.5125549};
    coll1->add(doc.dump());

    // add location field

    auto schema_changes = R"({
        "fields": [
            {"name": "location", "type": "geopoint"}
        ]
    })"_json;

    auto alter_op = coll1->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    // try searching on new fields
    auto res_op = coll1->search("*", {}, "location:(22.848641, 89.5406279, 50 km)", {}, {}, {0}, 3, 1, FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(2, res_op.get()["found"].get<size_t>());
}

TEST_F(CollectionSchemaChangeTest, UpdateSchemaWithNewEmbeddingField) {
    nlohmann::json schema = R"({
                "name": "objects",
                "fields": [
                {"name": "names", "type": "string[]"}
                ]
            })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");
    
    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();
    

    nlohmann::json update_schema = R"({
                "fields": [
                {"name": "embedding", "type":"float[]", "embed":{"from": ["names"], "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;
    
    auto res = coll->alter(update_schema);

    ASSERT_TRUE(res.ok());
    ASSERT_EQ(1, coll->get_embedding_fields().size());

    nlohmann::json doc;
    doc["names"] = {"hello", "world"};
    auto add_op = coll->add(doc.dump());

    ASSERT_TRUE(add_op.ok());
    auto added_doc = add_op.get();
    
    ASSERT_EQ(384, added_doc["embedding"].get<std::vector<float>>().size());
}

TEST_F(CollectionSchemaChangeTest, DropFieldUsedForEmbedding) {
    nlohmann::json schema = R"({
            "name": "objects",
            "fields": [
            {"name": "names", "type": "string[]"},
            {"name": "category", "type":"string"}, 
            {"name": "embedding", "type":"float[]", "embed":{"from": ["names","category"], "model_config": {"model_name": "ts/e5-small"}}}
            ]
        })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    LOG(INFO) << "Created collection";

    auto schema_changes = R"({
        "fields": [
            {"name": "names", "drop": true}
        ]
    })"_json;


    auto embedding_fields = coll->get_embedding_fields();
    ASSERT_EQ(2, embedding_fields["embedding"].embed[fields::from].get<std::vector<std::string>>().size());

    auto alter_op = coll->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    embedding_fields = coll->get_embedding_fields();
    ASSERT_EQ(1, embedding_fields["embedding"].embed[fields::from].get<std::vector<std::string>>().size());
    ASSERT_EQ("category", embedding_fields["embedding"].embed[fields::from].get<std::vector<std::string>>()[0]);

    schema_changes = R"({
        "fields": [
            {"name": "category", "drop": true}
        ]
    })"_json;

    alter_op = coll->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    embedding_fields = coll->get_embedding_fields();
    ASSERT_EQ(0, embedding_fields.size());
    ASSERT_EQ(0, coll->_get_index()->_get_vector_index().size());
}

TEST_F(CollectionSchemaChangeTest, EmbeddingFieldsMapTest) {
    nlohmann::json schema = R"({
                            "name": "objects",
                            "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
                            ]
                        })"_json;
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    auto embedding_fields_map = coll->get_embedding_fields();
    ASSERT_EQ(1, embedding_fields_map.size());
    auto embedding_field_it = embedding_fields_map.find("embedding");
    ASSERT_TRUE(embedding_field_it != embedding_fields_map.end());
    ASSERT_EQ("embedding", embedding_field_it.value().name);
    ASSERT_EQ(1, embedding_field_it.value().embed[fields::from].get<std::vector<std::string>>().size());
    ASSERT_EQ("name", embedding_field_it.value().embed[fields::from].get<std::vector<std::string>>()[0]);

    // drop the embedding field
    nlohmann::json schema_without_embedding = R"({
                            "fields": [
                            {"name": "embedding", "drop": true}
                            ]
                        })"_json;
    auto update_op = coll->alter(schema_without_embedding);

    ASSERT_TRUE(update_op.ok());

    embedding_fields_map = coll->get_embedding_fields();
    ASSERT_EQ(0, embedding_fields_map.size());
}

TEST_F(CollectionSchemaChangeTest, DropAndReindexEmbeddingField) {
    nlohmann::json schema = R"({
        "name": "objects",
        "fields": [
        {"name": "name", "type": "string"},
        {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(create_op.ok());
    auto coll = create_op.get();

    // drop the embedding field and reindex
    nlohmann::json alter_schema = R"({
        "fields": [
        {"name": "embedding", "drop": true},
        {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;
    
    auto update_op = coll->alter(alter_schema);
    ASSERT_TRUE(update_op.ok());

    auto embedding_fields_map = coll->get_embedding_fields();

    ASSERT_EQ(1, embedding_fields_map.size());

    // try adding a document
    nlohmann::json doc;
    doc["name"] = "hello";
    auto add_op = coll->add(doc.dump());
    ASSERT_TRUE(add_op.ok());
    auto added_doc = add_op.get();
    ASSERT_EQ(384, added_doc["embedding"].get<std::vector<float>>().size());

    // alter with bad schema
    alter_schema = R"({
        "fields": [
        {"name": "embedding", "drop": true},
        {"name": "embedding", "type":"float[]", "embed":{"from": ["namez"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    update_op = coll->alter(alter_schema);
    ASSERT_FALSE(update_op.ok());
    ASSERT_EQ("Property `embed.from` can only refer to string or string array fields.", update_op.error());

    // alter with bad model name
    alter_schema = R"({
        "fields": [
        {"name": "embedding", "drop": true},
        {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/x5-small"}}}
        ]
    })"_json;

    update_op = coll->alter(alter_schema);
    ASSERT_FALSE(update_op.ok());
    ASSERT_EQ("Model not found", update_op.error());

    // should still be able to add doc after aborted alter
    add_op = coll->add(doc.dump());
    ASSERT_TRUE(add_op.ok());
    added_doc = add_op.get();
    ASSERT_EQ(384, added_doc["embedding"].get<std::vector<float>>().size());
}

TEST_F(CollectionSchemaChangeTest, EmbeddingFieldAlterDropTest) {
    nlohmann::json schema = R"({
                "name": "objects",
                "fields": [
                {"name": "name", "type": "string"},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    auto& vec_index = coll->_get_index()->_get_vector_index();
    ASSERT_EQ(1, vec_index.size());
    ASSERT_EQ(1, vec_index.count("embedding"));


    nlohmann::json schema_change = R"({
                "fields": [
                {"name": "embedding", "drop": true}
                ]
            })"_json;

    auto schema_change_op = coll->alter(schema_change);

    ASSERT_TRUE(schema_change_op.ok());
    ASSERT_EQ(0, vec_index.size());
    ASSERT_EQ(0, vec_index.count("embedding"));
}
