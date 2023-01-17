#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "string_utils.h"
#include "collection.h"

class CollectionManagerTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    Collection *collection1;
    std::vector<sort_by> sort_fields;
    nlohmann::json schema;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/coll_manager_test_db";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        schema = R"({
            "name": "collection1",
            "enable_nested_fields": true,
            "fields": [
                {"name": "title", "type": "string", "locale": "en"},
                {"name": "starring", "type": "string", "infix": true},
                {"name": "cast", "type": "string[]", "facet": true, "optional": true},
                {"name": ".*_year", "type": "int32", "facet": true, "optional": true},
                {"name": "location", "type": "geopoint", "optional": true},
                {"name": "not_stored", "type": "string", "optional": true, "index": false},
                {"name": "points", "type": "int32"},
                {"name": "person", "type": "object", "optional": true},
                {"name": "vec", "type": "float[]", "num_dim": 128, "optional": true},
                {"name": "product_id", "type": "string", "reference": "Products.product_id", "optional": true}
            ],
            "default_sorting_field": "points",
            "symbols_to_index":["+"],
            "token_separators":["-"]
        })"_json;

        sort_fields = { sort_by("points", "DESC") };
        auto op = collectionManager.create_collection(schema);
        ASSERT_TRUE(op.ok());
        collection1 = op.get();
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        if(store != nullptr) {
            collectionManager.drop_collection("collection1");
            collectionManager.dispose();
            delete store;
        }
    }
};

TEST_F(CollectionManagerTest, CollectionCreation) {
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collection1 = collectionManager2.get_collection("collection1").get();
    ASSERT_NE(nullptr, collection1);

    tsl::htrie_map<char, field> schema = collection1->get_schema();
    std::vector<std::string> facet_fields_expected = {"cast"};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(0, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(2, collection1->get_sort_fields().size());
    ASSERT_EQ("location", collection1->get_sort_fields()[0].name);
    ASSERT_EQ("points", collection1->get_sort_fields()[1].name);
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_default_sorting_field());
    ASSERT_EQ(false, schema.at("not_stored").index);

    // check storage as well
    rocksdb::Iterator* it = store->get_iterator();
    size_t num_keys = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        num_keys += 1;
    }

    delete it;

    std::string collection_meta_json;
    nlohmann::json collection_meta;
    std::string next_seq_id;
    std::string next_collection_id;

    store->get(Collection::get_meta_key("collection1"), collection_meta_json);
    store->get(Collection::get_next_seq_id_key("collection1"), next_seq_id);
    store->get(CollectionManager::NEXT_COLLECTION_ID_KEY, next_collection_id);

    ASSERT_EQ(3, num_keys);
    // we already call `collection1->get_next_seq_id` above, which is side-effecting
    ASSERT_EQ(1, StringUtils::deserialize_uint32_t(next_seq_id));

    LOG(INFO) << collection_meta_json;

    nlohmann::json expected_meta_json = R"(
        {
          "created_at":1663234047,
          "default_sorting_field":"points",
          "enable_nested_fields":true,
          "fallback_field_type":"",
          "fields":[
            {
              "facet":false,
              "index":true,
              "infix":false,
              "locale":"en",
              "name":"title",
              "nested":false,
              "optional":false,
              "sort":false,
              "type":"string"
            },
            {
              "facet":false,
              "index":true,
              "infix":true,
              "locale":"",
              "name":"starring",
              "nested":false,
              "optional":false,
              "sort":false,
              "type":"string"
            },
            {
              "facet":true,
              "index":true,
              "infix":false,
              "locale":"",
              "name":"cast",
              "nested":false,
              "optional":true,
              "sort":false,
              "type":"string[]"
            },
            {
              "facet":true,
              "index":true,
              "infix":false,
              "locale":"",
              "name":".*_year",
              "nested":false,
              "optional":true,
              "sort":true,
              "type":"int32"
            },
            {
              "facet":false,
              "index":true,
              "infix":false,
              "locale":"",
              "name":"location",
              "nested":false,
              "optional":true,
              "sort":true,
              "type":"geopoint"
            },
            {
              "facet":false,
              "index":false,
              "infix":false,
              "locale":"",
              "name":"not_stored",
              "nested":false,
              "optional":true,
              "sort":false,
              "type":"string"
            },
            {
              "facet":false,
              "index":true,
              "infix":false,
              "locale":"",
              "name":"points",
              "nested":false,
              "optional":false,
              "sort":true,
              "type":"int32"
            },
            {
              "facet":false,
              "index":true,
              "infix":false,
              "locale":"",
              "name":"person",
              "nested":true,
              "nested_array":2,
              "optional":true,
              "sort":false,
              "type":"object"
            },
            {
              "facet":false,
              "index":true,
              "infix":false,
              "locale":"",
              "name":"vec",
              "nested":false,
              "num_dim":128,
              "optional":true,
              "sort":false,
              "type":"float[]",
              "vec_dist":"cosine"
            },
            {
              "facet":false,
              "index":true,
              "infix":false,
              "locale":"",
              "name":"product_id",
              "nested":false,
              "optional":false,
              "sort":false,
              "type":"string",
              "reference":"Products.product_id"
            }
          ],
          "id":0,
          "name":"collection1",
          "num_memory_shards":4,
          "symbols_to_index":[
            "+"
          ],
          "token_separators":[
            "-"
          ]
        }
    )"_json;

    auto actual_json = nlohmann::json::parse(collection_meta_json);
    expected_meta_json["created_at"] = actual_json["created_at"];

    ASSERT_EQ(expected_meta_json.dump(), actual_json.dump());
    ASSERT_EQ("1", next_collection_id);
}

TEST_F(CollectionManagerTest, ParallelCollectionCreation) {
    std::vector<std::thread> threads;
    for(size_t i = 0; i < 10; i++) {
        threads.emplace_back([i, &collectionManager = collectionManager]() {
            nlohmann::json coll_json = R"({
                "name": "parcoll",
                "fields": [
                    {"name": "title", "type": "string"}
                ]
            })"_json;
            coll_json["name"] = coll_json["name"].get<std::string>() + std::to_string(i+1);
            auto coll_op = collectionManager.create_collection(coll_json);
            ASSERT_TRUE(coll_op.ok());
        });
    }

    for(auto& thread : threads){
        thread.join();
    }

    int64_t prev_id = INT32_MAX;

    for(auto coll: collectionManager.get_collections()) {
        // collections are sorted by ID, in descending order
        ASSERT_TRUE(coll->get_collection_id() < prev_id);
        prev_id = coll->get_collection_id();
    }
}

TEST_F(CollectionManagerTest, ShouldInitCollection) {
    nlohmann::json collection_meta1 =
            nlohmann::json::parse("{\"name\": \"foobar\", \"id\": 100, \"fields\": [{\"name\": \"org\", \"type\": "
                                  "\"string\", \"facet\": false}], \"default_sorting_field\": \"foo\"}");

    Collection *collection = collectionManager.init_collection(collection_meta1, 100, store, 1.0f);
    ASSERT_EQ("foobar", collection->get_name());
    ASSERT_EQ(100, collection->get_collection_id());
    ASSERT_EQ(1, collection->get_fields().size());
    ASSERT_EQ("foo", collection->get_default_sorting_field());
    ASSERT_EQ(0, collection->get_created_at());

    ASSERT_FALSE(collection->get_fields().at(0).infix);
    ASSERT_FALSE(collection->get_fields().at(0).sort);
    ASSERT_EQ("", collection->get_fields().at(0).locale);

    delete collection;

    // with non-default values

    nlohmann::json collection_meta2 =
            nlohmann::json::parse("{\"name\": \"foobar\", \"id\": 100, \"fields\": [{\"name\": \"org\", \"type\": "
                                  "\"string\", \"facet\": false, \"infix\": true, \"sort\": true, \"locale\": \"en\"}], \"created_at\": 12345,"
                                  "\"default_sorting_field\": \"foo\","
                                  "\"symbols_to_index\": [\"+\"], \"token_separators\": [\"-\"]}");


    collection = collectionManager.init_collection(collection_meta2, 100, store, 1.0f);
    ASSERT_EQ(12345, collection->get_created_at());

    std::vector<char> expected_symbols = {'+'};
    std::vector<char> expected_separators = {'-'};

    ASSERT_EQ(1, collection->get_token_separators().size());
    ASSERT_EQ('-', collection->get_token_separators()[0]);

    ASSERT_EQ(1, collection->get_symbols_to_index().size());
    ASSERT_EQ('+', collection->get_symbols_to_index()[0]);

    ASSERT_TRUE(collection->get_fields().at(0).infix);
    ASSERT_TRUE(collection->get_fields().at(0).sort);
    ASSERT_EQ("en", collection->get_fields().at(0).locale);

    delete collection;
}

TEST_F(CollectionManagerTest, GetAllCollections) {
    std::vector<Collection*> collection_vec = collectionManager.get_collections();
    ASSERT_EQ(1, collection_vec.size());
    ASSERT_STREQ("collection1", collection_vec[0]->get_name().c_str());

    // try creating one more collection
    auto new_schema = R"({
        "name": "collection2",
        "fields": [
            {"name": "title", "type": "string", "locale": "en"},
            {"name": "points", "type": "int32"}
        ]
    })"_json;

    collectionManager.create_collection(new_schema);
    collection_vec = collectionManager.get_collections();
    ASSERT_EQ(2, collection_vec.size());

    // most recently created collection first
    ASSERT_STREQ("collection2", collection_vec[0]->get_name().c_str());
    ASSERT_STREQ("collection1", collection_vec[1]->get_name().c_str());

    collectionManager.drop_collection("collection2");
}

TEST_F(CollectionManagerTest, RestoreRecordsOnRestart) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    // add some overrides
    nlohmann::json override_json_include = {
        {"id", "include-rule"},
        {
         "rule", {
               {"query", "in"},
               {"match", override_t::MATCH_EXACT}
           }
        }
    };
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "0";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "3";
    override_json_include["includes"][1]["position"] = 2;

    override_t override_include;
    override_t::parse(override_json_include, "", override_include);

    nlohmann::json override_json = {
        {"id", "exclude-rule"},
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

    override_t override_exclude;
    override_t::parse(override_json, "", override_exclude);

    nlohmann::json override_json_deleted = {
        {"id", "deleted-rule"},
        {
         "rule", {
                   {"query", "of"},
                   {"match", override_t::MATCH_EXACT}
           }
        }
    };

    override_json_deleted["excludes"] = nlohmann::json::array();
    override_json_deleted["excludes"][0] = nlohmann::json::object();
    override_json_deleted["excludes"][0]["id"] = "11";

    override_t override_deleted;
    override_t::parse(override_json_deleted, "", override_deleted);

    collection1->add_override(override_include);
    collection1->add_override(override_exclude);
    collection1->add_override(override_deleted);

    collection1->remove_override("deleted-rule");

    // make some synonym operation
    ASSERT_TRUE(collection1->add_synonym(R"({"id": "id1", "root": "smart phone", "synonyms": ["iphone"]})"_json).ok());
    ASSERT_TRUE(collection1->add_synonym(R"({"id": "id2", "root": "mobile phone", "synonyms": ["samsung phone"]})"_json).ok());
    ASSERT_TRUE(collection1->add_synonym(R"({"id": "id3", "synonyms": ["football", "foot ball"]})"_json).ok());

    collection1->remove_synonym("id2");

    std::vector<std::string> search_fields = {"starring", "title"};
    std::vector<std::string> facets;

    nlohmann::json results = collection1->search("thomas", search_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    tsl::htrie_map<char, field> schema = collection1->get_schema();

    // recreate collection manager to ensure that it restores the records from the disk backed store
    collectionManager.dispose();
    delete store;

    store = new Store("/tmp/typesense_test/coll_manager_test_db");
    collectionManager.init(store, 1.0, "auth_key", quit);
    auto load_op = collectionManager.load(8, 1000);

    if(!load_op.ok()) {
        LOG(ERROR) << load_op.error();
    }

    ASSERT_TRUE(load_op.ok());

    collection1 = collectionManager.get_collection("collection1").get();
    ASSERT_NE(nullptr, collection1);

    std::vector<std::string> facet_fields_expected = {"cast"};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(18, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(2, collection1->get_sort_fields().size());
    ASSERT_EQ("location", collection1->get_sort_fields()[0].name);
    ASSERT_EQ("points", collection1->get_sort_fields()[1].name);
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_default_sorting_field());

    auto restored_schema = collection1->get_schema();
    ASSERT_EQ(true, restored_schema.at("cast").optional);
    ASSERT_EQ(true, restored_schema.at("cast").facet);
    ASSERT_EQ(false, restored_schema.at("title").facet);
    ASSERT_EQ(false, restored_schema.at("title").optional);
    ASSERT_EQ(false, restored_schema.at("not_stored").index);
    ASSERT_TRUE(restored_schema.at("person").nested);
    ASSERT_EQ(2, restored_schema.at("person").nested_array);
    ASSERT_EQ(128, restored_schema.at("vec").num_dim);

    ASSERT_TRUE(collection1->get_enable_nested_fields());

    ASSERT_EQ(2, collection1->get_overrides().size());
    ASSERT_STREQ("exclude-rule", collection1->get_overrides()["exclude-rule"].id.c_str());
    ASSERT_STREQ("include-rule", collection1->get_overrides()["include-rule"].id.c_str());

    const auto& synonyms = collection1->get_synonyms();
    ASSERT_EQ(2, synonyms.size());

    ASSERT_STREQ("id1", synonyms.at("id1").id.c_str());
    ASSERT_EQ(2, synonyms.at("id1").root.size());
    ASSERT_EQ(1, synonyms.at("id1").synonyms.size());

    ASSERT_STREQ("id3", synonyms.at("id3").id.c_str());
    ASSERT_EQ(0, synonyms.at("id3").root.size());
    ASSERT_EQ(2, synonyms.at("id3").synonyms.size());

    std::vector<char> expected_symbols = {'+'};
    std::vector<char> expected_separators = {'-'};

    ASSERT_EQ(1, collection1->get_token_separators().size());
    ASSERT_EQ('-', collection1->get_token_separators()[0]);

    ASSERT_EQ(1, collection1->get_symbols_to_index().size());
    ASSERT_EQ('+', collection1->get_symbols_to_index()[0]);

    results = collection1->search("thomas", search_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());
}

TEST_F(CollectionManagerTest, VerifyEmbeddedParametersOfScopedAPIKey) {
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", -1, 1),
                                 field("year", field_types::INT32, false),
                                 field("points", field_types::INT32, false),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Tom Sawyer";
    doc1["year"] = 1876;
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Tom Sawyer";
    doc2["year"] = 1922;
    doc2["points"] = 200;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    auto results = coll1->search("*", {"title"}, "", {}, {}, {0}, 3, 1, FREQUENCY, {true}, 5).get();
    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    std::map<std::string, std::string> req_params;
    req_params["collection"] = "coll1";
    req_params["q"] = "*";

    nlohmann::json embedded_params;
    embedded_params["filter_by"] = "points: 200";

    std::string json_res;
    auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    auto search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());

    nlohmann::json res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(1, res_obj["found"].get<size_t>());
    ASSERT_EQ(1, res_obj["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());

    // existing filter should be augmented
    req_params.clear();
    req_params["collection"] = "coll1";
    req_params["filter_by"] = "year: 1922";
    req_params["q"] = "*";

    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);

    ASSERT_EQ(1, res_obj["found"].get<size_t>());
    ASSERT_EQ(1, res_obj["hits"].size());
    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_EQ("(year: 1922) && (points: 200)", req_params["filter_by"]);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionManagerTest, RestoreAutoSchemaDocsOnRestart) {
    Collection *coll1;

    std::ifstream infile(std::string(ROOT_DIR)+"test/optional_fields.jsonl");
    std::vector<field> fields = {
        field("max", field_types::INT32, false)
    };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "max", 0, field_types::AUTO).get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        nlohmann::json document = nlohmann::json::parse(json_line);
        Option<nlohmann::json> add_op = coll1->add(document.dump());
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    ASSERT_EQ(1, coll1->get_collection_id());
    ASSERT_EQ(3, coll1->get_sort_fields().size());

    // index a document with a 2 bad field values with COERCE_OR_DROP setting
    // `title` is an integer and `average` is a string
    auto doc_json = R"({"title": 12345, "max": 25, "scores": [22, "how", 44],
                        "average": "bad data", "is_valid": true})";

    Option<nlohmann::json> add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::COERCE_OR_DROP);
    ASSERT_TRUE(add_op.ok());

    tsl::htrie_map<char, field> schema = collection1->get_schema();

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store, 1.0, "auth_key", quit);
    auto load_op = collectionManager2.load(8, 1000);

    if(!load_op.ok()) {
        LOG(ERROR) << load_op.error();
    }

    ASSERT_TRUE(load_op.ok());

    auto restored_coll = collectionManager2.get_collection("coll1").get();
    ASSERT_NE(nullptr, restored_coll);

    std::vector<std::string> facet_fields_expected = {};
    auto restored_schema = restored_coll->get_schema();

    ASSERT_EQ(1, restored_coll->get_collection_id());
    ASSERT_EQ(7, restored_coll->get_next_seq_id());
    ASSERT_EQ(7, restored_coll->get_num_documents());
    ASSERT_EQ(facet_fields_expected, restored_coll->get_facet_fields());
    ASSERT_EQ(3, restored_coll->get_sort_fields().size());
    ASSERT_EQ("average", restored_coll->get_sort_fields()[0].name);
    ASSERT_EQ("is_valid", restored_coll->get_sort_fields()[1].name);
    ASSERT_EQ("max", restored_coll->get_sort_fields()[2].name);

    // ensures that the "id" field is not added to the schema
    ASSERT_EQ(6, restored_schema.size());

    ASSERT_EQ("max", restored_coll->get_default_sorting_field());

    ASSERT_EQ(1, restored_schema.count("title"));
    ASSERT_EQ(1, restored_schema.count("max"));
    ASSERT_EQ(1, restored_schema.count("description"));
    ASSERT_EQ(1, restored_schema.count("scores"));
    ASSERT_EQ(1, restored_schema.count("average"));
    ASSERT_EQ(1, restored_schema.count("is_valid"));

    // all detected schema are optional fields, while defined schema is not

    for(const auto& a_field: restored_schema) {
        if(a_field.name == "max") {
            ASSERT_FALSE(a_field.optional);
        } else {
            ASSERT_TRUE(a_field.optional);
        }
    }

    // try searching for record with bad data
    auto results = restored_coll->search("12345", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["hits"].size());

    // int to string conversion should be done for `title` while `average` field must be dropped
    ASSERT_STREQ("12345", results["hits"][0]["document"]["title"].get<std::string>().c_str());
    ASSERT_EQ(0, results["hits"][0]["document"].count("average"));

    ASSERT_EQ(2, results["hits"][0]["document"]["scores"].size());
    ASSERT_EQ(22, results["hits"][0]["document"]["scores"][0]);
    ASSERT_EQ(44, results["hits"][0]["document"]["scores"][1]);

    // try sorting on `average`, a field that not all records have
    ASSERT_EQ(7, restored_coll->get_num_documents());

    sort_fields = { sort_by("average", "DESC") };
    results = restored_coll->search("*", {"title"}, "", {}, {sort_fields}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(7, results["hits"].size());

    collectionManager.drop_collection("coll1");
    collectionManager2.drop_collection("coll1");
}

TEST_F(CollectionManagerTest, RestorePresetsOnRestart) {
    auto preset_value = R"(
        {"q":"*", "per_page": "12"}
    )"_json;

    collectionManager.upsert_preset("single_preset", preset_value);

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager& collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store, 1.0, "auth_key", quit);
    auto load_op = collectionManager2.load(8, 1000);

    if(!load_op.ok()) {
        LOG(ERROR) << load_op.error();
    }

    ASSERT_TRUE(load_op.ok());

    nlohmann::json preset;
    collectionManager2.get_preset("single_preset", preset);
    ASSERT_EQ("*", preset["q"].get<std::string>());

    collectionManager.drop_collection("coll1");
    collectionManager2.drop_collection("coll1");
}

TEST_F(CollectionManagerTest, RestoreNestedDocsOnRestart) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object[]" },
          {"name": "company.name", "type": "string" },
          {"name": "person", "type": "object"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "details": [{"tags": ["foobar"]}],
        "company": {"name": "Foobar Corp"},
        "person": {"first_name": "Foobar"}
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto res_op = coll1->search("foobar", {"details"}, "", {}, {}, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    res_op = coll1->search("foobar", {"company.name"}, "", {}, {}, {0}, 10, 1,
                           token_ordering::FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    res_op = coll1->search("foobar", {"person"}, "", {}, {}, {0}, 10, 1,
                           token_ordering::FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager& collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store, 1.0, "auth_key", quit);
    auto load_op = collectionManager2.load(8, 1000);

    if(!load_op.ok()) {
        LOG(ERROR) << load_op.error();
    }

    ASSERT_TRUE(load_op.ok());

    auto restored_coll = collectionManager2.get_collection("coll1").get();
    ASSERT_NE(nullptr, restored_coll);

    res_op = restored_coll->search("foobar", {"details"}, "", {}, {}, {0}, 10, 1,
                           token_ordering::FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    res_op = restored_coll->search("foobar", {"company.name"}, "", {}, {}, {0}, 10, 1,
                           token_ordering::FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    res_op = restored_coll->search("foobar", {"person"}, "", {}, {}, {0}, 10, 1,
                           token_ordering::FREQUENCY, {true});
    ASSERT_TRUE(res_op.ok());
    ASSERT_EQ(1, res_op.get()["found"].get<size_t>());

    collectionManager.drop_collection("coll1");
    collectionManager2.drop_collection("coll1");
}

TEST_F(CollectionManagerTest, DropCollectionCleanly) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    ASSERT_FALSE(nullptr == collectionManager.get_collection_with_id(0).get());
    ASSERT_FALSE(nullptr == collectionManager.get_collection("collection1").get());

    collectionManager.drop_collection("collection1");

    rocksdb::Iterator* it = store->get_iterator();
    size_t num_keys = 0;

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_EQ(it->key().ToString(), "$CI");
        num_keys += 1;
    }

    ASSERT_EQ(1, num_keys);
    ASSERT_TRUE(it->status().ok());

    ASSERT_EQ(nullptr, collectionManager.get_collection("collection1").get());
    ASSERT_EQ(nullptr, collectionManager.get_collection_with_id(0).get());
    ASSERT_EQ(1, collectionManager.get_next_collection_id());

    delete it;
}

TEST_F(CollectionManagerTest, AuthWithMultiSearchKeys) {
    api_key_t key1("api_key", "some key", {"documents:create"}, {"foo"}, 64723363199);
    collectionManager.getAuthManager().create_key(key1);

    std::vector<collection_key_t> collection_keys = {
        collection_key_t("foo", "api_key")
    };

    std::vector<nlohmann::json> embedded_params_vec = { nlohmann::json::object() };
    std::map<std::string, std::string> params;

    // empty req auth key (present in header / GET param)
    ASSERT_TRUE(collectionManager.auth_key_matches("", "documents:create", collection_keys, params,
                                                   embedded_params_vec));

    // should work with bootstrap key
    collection_keys = {
        collection_key_t("foo", "auth_key")
    };

    ASSERT_TRUE(collectionManager.auth_key_matches("", "documents:create", collection_keys, params,
                                                   embedded_params_vec));

    // bad key

    collection_keys = {
        collection_key_t("foo", "")
    };

    ASSERT_FALSE(collectionManager.auth_key_matches("", "documents:create", collection_keys, params,
                                                   embedded_params_vec));
}

TEST_F(CollectionManagerTest, Symlinking) {
    CollectionManager & cmanager = CollectionManager::get_instance();
    std::string state_dir_path = "/tmp/typesense_test/cmanager_test_db";
    system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
    Store *new_store = new Store(state_dir_path);
    cmanager.init(new_store, 1.0, "auth_key", quit);
    cmanager.load(8, 1000);

    // try resolving on a blank slate
    Option<std::string> collection_option = cmanager.resolve_symlink("collection");

    ASSERT_FALSE(collection_option.ok());
    ASSERT_EQ(404, collection_option.code());

    ASSERT_EQ(0, cmanager.get_symlinks().size());

    // symlink name cannot be the same as an existing collection
    Option<bool> inserted = cmanager.upsert_symlink("collection1", "collection_2018");
    ASSERT_FALSE(inserted.ok());
    ASSERT_STREQ("Name `collection1` conflicts with an existing collection name.", inserted.error().c_str());

    // insert a symlink
    inserted = cmanager.upsert_symlink("collection_link", "collection_2018");
    ASSERT_TRUE(inserted.ok());

    collection_option = cmanager.resolve_symlink("collection_link");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("collection_2018", collection_option.get());

    // let's try inserting another symlink
    cmanager.upsert_symlink("company", "company_2018");
    collection_option = cmanager.resolve_symlink("company");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2018", collection_option.get());

    ASSERT_EQ(2, cmanager.get_symlinks().size());

    // update existing symlink
    inserted = cmanager.upsert_symlink("company", "company_2019");
    ASSERT_TRUE(inserted.ok());
    collection_option = cmanager.resolve_symlink("company");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2019", collection_option.get());

    // add and update a symlink against an existing collection
    inserted = cmanager.upsert_symlink("collection1_link", "collection1");
    ASSERT_TRUE(inserted.ok());
    collection_option = cmanager.resolve_symlink("collection1_link");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("collection1", collection_option.get());

    // try to drop a collection using the alias `collection1_link`
    auto drop_op = cmanager.drop_collection("collection1_link");
    ASSERT_TRUE(drop_op.ok());

    // try to list collections now
    nlohmann::json summaries = cmanager.get_collection_summaries();
    ASSERT_EQ(0, summaries.size());

    // remap alias to another non-existing collection
    inserted = cmanager.upsert_symlink("collection1_link", "collection2");
    ASSERT_TRUE(inserted.ok());
    collection_option = cmanager.resolve_symlink("collection1_link");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("collection2", collection_option.get());

    // remove link
    Option<bool> deleted = cmanager.delete_symlink("collection");
    ASSERT_TRUE(deleted.ok());
    collection_option = cmanager.resolve_symlink("collection");
    ASSERT_FALSE(collection_option.ok());
    ASSERT_EQ(404, collection_option.code());

    // try adding a few more symlinks
    cmanager.upsert_symlink("company_1", "company_2018");
    cmanager.upsert_symlink("company_2", "company_2019");
    cmanager.upsert_symlink("company_3", "company_2020");

    // should be able to restore state on init
    CollectionManager & cmanager2 = CollectionManager::get_instance();
    cmanager2.init(store, 1.0, "auth_key", quit);
    cmanager2.load(8, 1000);

    collection_option = cmanager2.resolve_symlink("company");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2019", collection_option.get());

    collection_option = cmanager2.resolve_symlink("company_1");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2018", collection_option.get());

    collection_option = cmanager2.resolve_symlink("company_3");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2020", collection_option.get());

    delete new_store;
}

TEST_F(CollectionManagerTest, LoadMultipleCollections) {
    // to prevent fixture tear down from running as we are fudging with CollectionManager singleton
    collectionManager.dispose();
    delete store;
    store = nullptr;

    CollectionManager & cmanager = CollectionManager::get_instance();
    std::string state_dir_path = "/tmp/typesense_test/cmanager_test_db";
    system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
    Store *new_store = new Store(state_dir_path);
    cmanager.init(new_store, 1.0, "auth_key", quit);
    cmanager.load(8, 1000);

    for(size_t i = 0; i < 100; i++) {
        auto schema = {
            field("title", field_types::STRING, false),
            field("starring", field_types::STRING, false),
            field("cast", field_types::STRING_ARRAY, true, true),
            field(".*_year", field_types::INT32, true, true),
            field("location", field_types::GEOPOINT, false, true, true),
            field("points", field_types::INT32, false)
        };

        cmanager.create_collection("collection" + std::to_string(i), 4, schema, "points").get();
    }

    ASSERT_EQ(100, cmanager.get_collections().size());

    cmanager.dispose();
    delete new_store;

    new_store = new Store(state_dir_path);
    cmanager.init(new_store, 1.0, "auth_key", quit);
    cmanager.load(8, 1000);

    ASSERT_EQ(100, cmanager.get_collections().size());

    for(size_t i = 0; i < 100; i++) {
        collectionManager.drop_collection("collection" + std::to_string(i));
    }

    collectionManager.dispose();
    delete new_store;
}

TEST_F(CollectionManagerTest, ParseSortByClause) {
    std::vector<sort_by> sort_fields;
    bool sort_by_parsed = CollectionManager::parse_sort_by_str("points:desc,loc(24.56,10.45):ASC", sort_fields);
    ASSERT_TRUE(sort_by_parsed);

    ASSERT_STREQ("points", sort_fields[0].name.c_str());
    ASSERT_STREQ("DESC", sort_fields[0].order.c_str());

    ASSERT_STREQ("loc(24.56,10.45)", sort_fields[1].name.c_str());
    ASSERT_STREQ("ASC", sort_fields[1].order.c_str());

    sort_fields.clear();

    sort_by_parsed = CollectionManager::parse_sort_by_str(" points:desc , loc(24.56,10.45):ASC", sort_fields);
    ASSERT_TRUE(sort_by_parsed);

    ASSERT_STREQ("points", sort_fields[0].name.c_str());
    ASSERT_STREQ("DESC", sort_fields[0].order.c_str());

    ASSERT_STREQ("loc(24.56,10.45)", sort_fields[1].name.c_str());
    ASSERT_STREQ("ASC", sort_fields[1].order.c_str());

    sort_fields.clear();

    sort_by_parsed = CollectionManager::parse_sort_by_str(" loc(24.56,10.45):ASC, points: desc ", sort_fields);
    ASSERT_TRUE(sort_by_parsed);

    ASSERT_STREQ("loc(24.56,10.45)", sort_fields[0].name.c_str());
    ASSERT_STREQ("ASC", sort_fields[0].order.c_str());

    ASSERT_STREQ("points", sort_fields[1].name.c_str());
    ASSERT_STREQ("DESC", sort_fields[1].order.c_str());

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str(" location(48.853, 2.344, exclude_radius: 2mi):asc,popularity:desc", sort_fields);
    ASSERT_TRUE(sort_by_parsed);
    ASSERT_EQ("location(48.853, 2.344, exclude_radius: 2mi)", sort_fields[0].name);
    ASSERT_STREQ("ASC", sort_fields[0].order.c_str());

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str(" location(48.853, 2.344, precision: 2mi):asc,popularity:desc", sort_fields);
    ASSERT_TRUE(sort_by_parsed);
    ASSERT_EQ("location(48.853, 2.344, precision: 2mi)", sort_fields[0].name);
    ASSERT_STREQ("ASC", sort_fields[0].order.c_str());

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str(" _text_match(buckets: 10):ASC, points:desc ", sort_fields);
    ASSERT_TRUE(sort_by_parsed);
    ASSERT_EQ("_text_match(buckets: 10)", sort_fields[0].name);
    ASSERT_EQ("ASC", sort_fields[0].order);

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str("_eval(brand:nike && foo:bar):DESC,points:desc ", sort_fields);
    ASSERT_TRUE(sort_by_parsed);
    ASSERT_EQ("_eval(brand:nike && foo:bar)", sort_fields[0].name);
    ASSERT_EQ("DESC", sort_fields[0].order);
    ASSERT_EQ("points", sort_fields[1].name);
    ASSERT_EQ("DESC", sort_fields[1].order);

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str("", sort_fields);
    ASSERT_TRUE(sort_by_parsed);
    ASSERT_EQ(0, sort_fields.size());

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str("foobar:", sort_fields);
    ASSERT_FALSE(sort_by_parsed);

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str("foobar:,bar:desc", sort_fields);
    ASSERT_FALSE(sort_by_parsed);

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str(",", sort_fields);
    ASSERT_FALSE(sort_by_parsed);

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str(",,", sort_fields);
    ASSERT_FALSE(sort_by_parsed);
}

TEST_F(CollectionManagerTest, Presets) {
    // try getting on a blank slate
    auto presets = collectionManager.get_presets();
    ASSERT_TRUE(presets.empty());

    // insert some presets
    nlohmann::json preset_obj;

    preset_obj["query_by"] = "foo";
    collectionManager.upsert_preset("preset1", preset_obj);

    preset_obj["query_by"] = "bar";
    collectionManager.upsert_preset("preset2", preset_obj);

    ASSERT_EQ(2, collectionManager.get_presets().size());

    // try fetching individual presets
    nlohmann::json preset;
    auto preset_op = collectionManager.get_preset("preset1", preset);
    ASSERT_TRUE(preset_op.ok());
    ASSERT_EQ(1, preset.size());
    ASSERT_EQ("foo", preset["query_by"]);

    preset.clear();
    preset_op = collectionManager.get_preset("preset2", preset);
    ASSERT_TRUE(preset_op.ok());
    ASSERT_EQ(1, preset.size());
    ASSERT_EQ("bar", preset["query_by"]);

    // delete a preset
    auto del_op = collectionManager.delete_preset("preset2");
    ASSERT_TRUE(del_op.ok());

    std::string val;
    auto status = store->get(CollectionManager::get_preset_key("preset2"), val);
    ASSERT_EQ(StoreStatus::NOT_FOUND, status);

    ASSERT_EQ(1, collectionManager.get_presets().size());
    preset.clear();
    preset_op = collectionManager.get_preset("preset2", preset);
    ASSERT_FALSE(preset_op.ok());
    ASSERT_EQ(404, preset_op.code());

    // should be able to restore state on init
    collectionManager.dispose();
    delete store;

    store = new Store("/tmp/typesense_test/coll_manager_test_db");
    collectionManager.init(store, 1.0, "auth_key", quit);
    collectionManager.load(8, 1000);

    ASSERT_EQ(1, collectionManager.get_presets().size());
    preset.clear();
    preset_op = collectionManager.get_preset("preset1", preset);
    ASSERT_TRUE(preset_op.ok());
}

TEST_F(CollectionManagerTest, CloneCollection) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"}
        ],
        "symbols_to_index":["+"],
        "token_separators":["-", "?"]
    })"_json;

    auto create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(create_op.ok());
    auto coll1 = create_op.get();

    nlohmann::json synonym1 = R"({
        "id": "ipod-synonyms",
        "synonyms": ["ipod", "i pod", "pod"]
    })"_json;

    ASSERT_TRUE(coll1->add_synonym(synonym1).ok());

    nlohmann::json override_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                             {"query", "{categories}"},
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {categories}"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    nlohmann::json req = R"({"name": "coll2"})"_json;
    collectionManager.clone_collection("coll1", req);

    auto coll2 = collectionManager.get_collection_unsafe("coll2");
    ASSERT_FALSE(coll2 == nullptr);
    ASSERT_EQ("coll2", coll2->get_name());
    ASSERT_EQ(1, coll2->get_fields().size());
    ASSERT_EQ(1, coll2->get_synonyms().size());
    ASSERT_EQ(1, coll2->get_overrides().size());
    ASSERT_EQ("", coll2->get_fallback_field_type());

    ASSERT_EQ(1, coll2->get_symbols_to_index().size());
    ASSERT_EQ(2, coll2->get_token_separators().size());

    ASSERT_EQ('+', coll2->get_symbols_to_index().at(0));
    ASSERT_EQ('-', coll2->get_token_separators().at(0));
    ASSERT_EQ('?', coll2->get_token_separators().at(1));
}
