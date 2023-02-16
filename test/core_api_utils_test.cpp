#include <gtest/gtest.h>
#include "collection.h"
#include <vector>
#include <collection_manager.h>
#include <core_api.h>
#include "core_api_utils.h"

class CoreAPIUtilsTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/core_api_utils";
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

TEST_F(CoreAPIUtilsTest, StatefulRemoveDocs) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 2, fields, "points").get();
    }

    for(size_t i=0; i<100; i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        coll1->add(doc.dump());
    }

    bool done;
    deletion_state_t deletion_state;
    deletion_state.collection = coll1;
    deletion_state.num_removed = 0;

    // single document match

    filter_result_t filter_results;
    coll1->get_filter_ids("points: 99", filter_results);
    deletion_state.index_ids.emplace_back(filter_results.count, filter_results.docs);
    filter_results.docs = nullptr;
    for(size_t i=0; i<deletion_state.index_ids.size(); i++) {
        deletion_state.offsets.push_back(0);
    }

    stateful_remove_docs(&deletion_state, 5, done);
    ASSERT_EQ(1, deletion_state.num_removed);
    ASSERT_TRUE(done);

    // match 12 documents (multiple batches)
    for(auto& kv: deletion_state.index_ids) {
        delete [] kv.second;
    }
    deletion_state.index_ids.clear();
    deletion_state.offsets.clear();
    deletion_state.num_removed = 0;

    coll1->get_filter_ids("points:< 11", filter_results);
    deletion_state.index_ids.emplace_back(filter_results.count, filter_results.docs);
    filter_results.docs = nullptr;
    for(size_t i=0; i<deletion_state.index_ids.size(); i++) {
        deletion_state.offsets.push_back(0);
    }

    stateful_remove_docs(&deletion_state, 4, done);
    ASSERT_EQ(4, deletion_state.num_removed);
    ASSERT_FALSE(done);

    stateful_remove_docs(&deletion_state, 4, done);
    ASSERT_EQ(8, deletion_state.num_removed);
    ASSERT_FALSE(done);

    stateful_remove_docs(&deletion_state, 4, done);
    ASSERT_EQ(11, deletion_state.num_removed);
    ASSERT_TRUE(done);

    // match 9 documents (multiple batches)
    for(auto& kv: deletion_state.index_ids) {
        delete [] kv.second;
    }
    deletion_state.index_ids.clear();
    deletion_state.offsets.clear();
    deletion_state.num_removed = 0;

    coll1->get_filter_ids("points:< 20", filter_results);
    deletion_state.index_ids.emplace_back(filter_results.count, filter_results.docs);
    filter_results.docs = nullptr;
    for(size_t i=0; i<deletion_state.index_ids.size(); i++) {
        deletion_state.offsets.push_back(0);
    }

    stateful_remove_docs(&deletion_state, 7, done);
    ASSERT_EQ(7, deletion_state.num_removed);
    ASSERT_FALSE(done);

    stateful_remove_docs(&deletion_state, 7, done);
    ASSERT_EQ(9, deletion_state.num_removed);
    ASSERT_TRUE(done);

    // fetch raw document IDs
    for(size_t i=0; i<100; i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        coll1->add(doc.dump());
    }

    for(auto& kv: deletion_state.index_ids) {
        delete [] kv.second;
    }
    deletion_state.index_ids.clear();
    deletion_state.offsets.clear();
    deletion_state.num_removed = 0;

    coll1->get_filter_ids("id:[0, 1, 2]", filter_results);
    deletion_state.index_ids.emplace_back(filter_results.count, filter_results.docs);
    filter_results.docs = nullptr;
    for(size_t i=0; i<deletion_state.index_ids.size(); i++) {
        deletion_state.offsets.push_back(0);
    }

    stateful_remove_docs(&deletion_state, 5, done);
    ASSERT_EQ(3, deletion_state.num_removed);
    ASSERT_TRUE(done);

    // delete single doc

    for(auto& kv: deletion_state.index_ids) {
        delete [] kv.second;
    }
    deletion_state.index_ids.clear();
    deletion_state.offsets.clear();
    deletion_state.num_removed = 0;

    coll1->get_filter_ids("id :10", filter_results);
    deletion_state.index_ids.emplace_back(filter_results.count, filter_results.docs);
    filter_results.docs = nullptr;
    for(size_t i=0; i<deletion_state.index_ids.size(); i++) {
        deletion_state.offsets.push_back(0);
    }

    stateful_remove_docs(&deletion_state, 5, done);
    ASSERT_EQ(1, deletion_state.num_removed);
    ASSERT_TRUE(done);

    for(auto& kv: deletion_state.index_ids) {
        delete [] kv.second;
    }
    deletion_state.index_ids.clear();
    deletion_state.offsets.clear();
    deletion_state.num_removed = 0;

    // bad filter query
    auto op = coll1->get_filter_ids("bad filter", filter_results);
    ASSERT_FALSE(op.ok());
    ASSERT_STREQ("Could not parse the filter query.", op.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CoreAPIUtilsTest, MultiSearchEmbeddedKeys) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["filter_by"] = "user_id: 100";
    nlohmann::json body;
    body["searches"] = nlohmann::json::array();
    nlohmann::json search;
    search["collection"] = "users";
    search["filter_by"] = "age: > 100";
    body["searches"].push_back(search);

    req->body = body.dump();
    nlohmann::json embedded_params;
    embedded_params["filter_by"] = "foo: bar";
    req->embedded_params_vec.push_back(embedded_params);

    post_multi_search(req, res);

    // ensure that req params are appended to (embedded params are also rolled into req params)
    ASSERT_EQ("((user_id: 100) && (age: > 100)) && (foo: bar)", req->params["filter_by"]);

    // when empty filter_by is present in req params, don't add ()
    req->params["filter_by"] = "";
    post_multi_search(req, res);
    ASSERT_EQ("((age: > 100)) && (foo: bar)", req->params["filter_by"]);

    // when empty filter_by in collection search params, don't add ()
    req->params["filter_by"] = "user_id: 100";
    search["filter_by"] = "";
    body["searches"].clear();
    body["searches"].push_back(search);
    req->body = body.dump();
    post_multi_search(req, res);
    ASSERT_EQ("((user_id: 100)) && (foo: bar)", req->params["filter_by"]);

    // when both are empty, don't add ()
    req->params["filter_by"] = "";
    search["filter_by"] = "";
    body["searches"].clear();
    body["searches"].push_back(search);
    req->body = body.dump();
    post_multi_search(req, res);
    ASSERT_EQ("(foo: bar)", req->params["filter_by"]);

    // try setting max search limit
    req->embedded_params_vec[0]["limit_multi_searches"] = 0;
    ASSERT_FALSE(post_multi_search(req, res));
    ASSERT_EQ("{\"message\": \"Number of multi searches exceeds `limit_multi_searches` parameter.\"}", res->body);

    req->embedded_params_vec[0]["limit_multi_searches"] = 1;
    ASSERT_TRUE(post_multi_search(req, res));

    // req params must be overridden by embedded param
    req->embedded_params_vec[0]["limit_multi_searches"] = 0;
    req->params["limit_multi_searches"] = "100";
    ASSERT_FALSE(post_multi_search(req, res));
    ASSERT_EQ("{\"message\": \"Number of multi searches exceeds `limit_multi_searches` parameter.\"}", res->body);

    // use req params if embedded param not present
    req->embedded_params_vec[0].erase("limit_multi_searches");
    ASSERT_TRUE(post_multi_search(req, res));

}

TEST_F(CoreAPIUtilsTest, ExtractCollectionsFromRequestBody) {
    std::map<std::string, std::string> req_params;
    std::string body = R"(
      {
        "name": "coll1",
        "fields": [
          {"name": "title", "type": "string" },
          {"name": "points", "type": "int32" }
        ],
        "default_sorting_field": "points"
      }
    )";

    route_path rpath("POST", {"collections"}, post_create_collection, false, false);
    std::vector<collection_key_t> collections;
    std::vector<nlohmann::json> embedded_params_vec;

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("coll1", collections[0].collection);
    ASSERT_EQ("foo", collections[0].api_key);

    // badly constructed collection schema body
    collections.clear();
    embedded_params_vec.clear();
    body = R"(
      {
        "name": "coll1
        "fields": [
          {"name": "title", "type": "string" },
          {"name": "points", "type": "int32" }
        ],
        "default_sorting_field": "points"
      }
    )";

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("", collections[0].collection);
    ASSERT_EQ("foo", collections[0].api_key);
    ASSERT_EQ(1, embedded_params_vec.size());

    collections.clear();
    embedded_params_vec.clear();

    // missing collection name
    body = R"(
      {
        "fields": [
          {"name": "title", "type": "string" },
          {"name": "points", "type": "int32" }
        ],
        "default_sorting_field": "points"
      }
    )";

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("", collections[0].collection);
    ASSERT_EQ("foo", collections[0].api_key);

    // check for multi_search
    collections.clear();
    embedded_params_vec.clear();
    rpath = route_path("POST", {"collections"}, post_multi_search, false, false);
    body = R"(
        {"searches":[
              {
                "query_by": "concat",
                "collection": "products",
                "q": "battery",
                "x-typesense-api-key": "bar"
              }
          ]
        }
    )";

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("products", collections[0].collection);
    ASSERT_EQ("bar", collections[0].api_key);

    // when api key type is bad
    collections.clear();
    embedded_params_vec.clear();
    rpath = route_path("POST", {"collections"}, post_multi_search, false, false);
    body = R"(
        {"searches":[
              {
                "query_by": "concat",
                "collection": "products",
                "q": "battery",
                "x-typesense-api-key": 123
              }
          ]
        }
    )";

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ("foo", collections[0].api_key);

    // when collection name is bad
    collections.clear();
    embedded_params_vec.clear();
    rpath = route_path("POST", {"collections"}, post_multi_search, false, false);
    body = R"(
            {"searches":[
                  {
                    "query_by": "concat",
                    "collection": 123,
                    "q": "battery"
                  }
              ]
            }
        )";

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ("", collections[0].collection);

    // get collection for multi-search
    collections.clear();
    embedded_params_vec.clear();
    body = R"(
        {"searches":
              {
                "query_by": "concat",
                "collection": "products",
                "q": "battery"
              }
          ]
        }
    )";

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("", collections[0].collection);
    ASSERT_EQ("foo", collections[0].api_key);

    collections.clear();
    embedded_params_vec.clear();
    body = R"(
        {"searches":[
              {
                "query_by": "concat",
                "q": "battery",
                "x-typesense-api-key": "bar"
              }
          ]
        }
    )";

    get_collections_for_auth(req_params, body, rpath, "foo", collections, embedded_params_vec);
    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("", collections[0].collection);
    ASSERT_EQ("bar", collections[0].api_key);
}

TEST_F(CoreAPIUtilsTest, ExtractCollectionsFromRequestBodyExtended) {
    route_path rpath_multi_search = route_path("POST", {"multi_search"}, post_multi_search, false, false);
    std::map<std::string, std::string> req_params;

    std::vector<collection_key_t> collections;
    std::vector<nlohmann::json> embedded_params_vec;

    get_collections_for_auth(req_params, "{]", rpath_multi_search, "", collections, embedded_params_vec);

    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("", collections[0].collection);
    ASSERT_EQ(1, embedded_params_vec.size());

    nlohmann::json sample_search_body;
    sample_search_body["searches"] = nlohmann::json::array();
    nlohmann::json search_query;
    search_query["q"] = "aaa";
    search_query["collection"] = "company1";

    sample_search_body["searches"].push_back(search_query);

    search_query["collection"] = "company2";
    sample_search_body["searches"].push_back(search_query);

    collections.clear();
    embedded_params_vec.clear();
    get_collections_for_auth(req_params, sample_search_body.dump(), rpath_multi_search, "", collections, embedded_params_vec);

    ASSERT_EQ(2, collections.size());
    ASSERT_EQ("company1", collections[0].collection);
    ASSERT_EQ("company2", collections[1].collection);

    collections.clear();
    req_params["collection"] = "foo";

    get_collections_for_auth(req_params, sample_search_body.dump(), rpath_multi_search, "", collections, embedded_params_vec);

    ASSERT_EQ(2, collections.size());
    ASSERT_EQ("company1", collections[0].collection);
    ASSERT_EQ("company2", collections[1].collection);

    collections.clear();
    embedded_params_vec.clear();

    // when one of the search arrays don't have an explicit collection, use the collection name from req param
    sample_search_body["searches"][1].erase("collection");

    get_collections_for_auth(req_params, sample_search_body.dump(), rpath_multi_search, "", collections, embedded_params_vec);

    ASSERT_EQ(2, collections.size());
    ASSERT_EQ("company1", collections[0].collection);
    ASSERT_EQ("foo", collections[1].collection);

    collections.clear();
    embedded_params_vec.clear();
    req_params.clear();

    route_path rpath_search = route_path("GET", {"collections", ":collection", "documents", "search"}, get_search, false, false);
    get_collections_for_auth(req_params, sample_search_body.dump(), rpath_search, "", collections, embedded_params_vec);

    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("", collections[0].collection);
    ASSERT_EQ(1, embedded_params_vec.size());

    collections.clear();
    embedded_params_vec.clear();
    req_params.clear();
    req_params["collection"] = "foo";

    get_collections_for_auth(req_params, sample_search_body.dump(), rpath_search, "", collections, embedded_params_vec);

    ASSERT_EQ(1, collections.size());
    ASSERT_EQ("foo", collections[0].collection);
    ASSERT_EQ(1, embedded_params_vec.size());
}

TEST_F(CoreAPIUtilsTest, MultiSearchWithPresetShouldUsePresetForAuth) {
    nlohmann::json preset_value = R"(
        {"searches":[
            {"collection":"foo","q":"apple", "query_by": "title"},
            {"collection":"bar","q":"apple", "query_by": "title"}
        ]}
    )"_json;

    Option<bool> success_op = collectionManager.upsert_preset("apple", preset_value);

    route_path rpath_multi_search = route_path("POST", {"multi_search"}, post_multi_search, false, false);
    std::map<std::string, std::string> req_params;

    std::vector<collection_key_t> collections;
    std::vector<nlohmann::json> embedded_params_vec;

    std::string search_body = R"(
        {"searches":[
            {"collection":"foo1","q":"apple", "query_by": "title"},
            {"collection":"bar1","q":"apple", "query_by": "title"}
        ]}
    )";

    // without preset parameter, use collections from request body

    get_collections_for_auth(req_params, search_body, rpath_multi_search, "", collections, embedded_params_vec);
    
    ASSERT_EQ(2, collections.size());
    ASSERT_EQ("foo1", collections[0].collection);
    ASSERT_EQ("bar1", collections[1].collection);
    ASSERT_EQ(2, embedded_params_vec.size());

    // with preset parameter, use collections from preset configuration
    collections.clear();
    embedded_params_vec.clear();

    req_params["preset"] = "apple";
    get_collections_for_auth(req_params, search_body, rpath_multi_search, "", collections, embedded_params_vec);
    
    ASSERT_EQ(2, collections.size());
    ASSERT_EQ("foo", collections[0].collection);
    ASSERT_EQ("bar", collections[1].collection);
    ASSERT_EQ(2, embedded_params_vec.size());

    // try using multi_search preset within individual search param

    preset_value = R"(
        {"collection":"preset_coll"}
    )"_json;

    collectionManager.upsert_preset("single_preset", preset_value);

    req_params.clear();
    collections.clear();
    embedded_params_vec.clear();

    search_body = R"(
        {"searches":[
            {"collection":"foo1","q":"apple", "query_by": "title", "preset": "single_preset"},
            {"collection":"bar1","q":"apple", "query_by": "title", "preset": "single_preset"}
        ]}
    )";

    get_collections_for_auth(req_params, search_body, rpath_multi_search, "", collections, embedded_params_vec);

    ASSERT_EQ(2, collections.size());
    ASSERT_EQ("foo1", collections[0].collection);
    ASSERT_EQ("bar1", collections[1].collection);
    ASSERT_EQ(2, embedded_params_vec.size());

    // without collection in search array
    req_params.clear();
    collections.clear();
    embedded_params_vec.clear();

    search_body = R"(
        {"searches":[
            {"q":"apple", "query_by": "title", "preset": "single_preset"},
            {"q":"apple", "query_by": "title", "preset": "single_preset"}
        ]}
    )";

    get_collections_for_auth(req_params, search_body, rpath_multi_search, "", collections, embedded_params_vec);

    ASSERT_EQ(2, collections.size());
    ASSERT_EQ("preset_coll", collections[0].collection);
    ASSERT_EQ("preset_coll", collections[1].collection);
    ASSERT_EQ(2, embedded_params_vec.size());
}

TEST_F(CoreAPIUtilsTest, PresetSingleSearch) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "name", "type": "string" },
          {"name": "points", "type": "int32" }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto preset_value = R"(
        {"collection":"preset_coll", "per_page": "12"}
    )"_json;

    collectionManager.upsert_preset("single_preset", preset_value);

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    req->params["collection"] = "coll1";

    auto search_body = R"(
        {"searches":[
            {"collection":"coll1","q":"apple", "query_by": "title", "preset": "single_preset"}
        ]}
    )";

    req->body = search_body;
    nlohmann::json embedded_params;
    req->embedded_params_vec.push_back(embedded_params);

    post_multi_search(req, res);

    ASSERT_EQ("12", req->params["per_page"]);
    ASSERT_EQ("coll1", req->params["collection"]);

    collectionManager.drop_collection("coll1");
}

TEST_F(CoreAPIUtilsTest, ExportWithFilter) {
    Collection *coll1;
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 2, fields, "points").get();
    }

    for(size_t i=0; i<4; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;
        coll1->add(doc.dump());
    }

    bool done;
    std::string res_body;

    export_state_t export_state;
    filter_result_t filter_result;
    coll1->get_filter_ids("points:>=0", filter_result);
    export_state.index_ids.emplace_back(filter_result.count, filter_result.docs);
    filter_result.docs = nullptr;
    for(size_t i=0; i<export_state.index_ids.size(); i++) {
        export_state.offsets.push_back(0);
    }

    export_state.collection = coll1;
    export_state.res_body = &res_body;

    stateful_export_docs(&export_state, 2, done);
    ASSERT_FALSE(done);
    ASSERT_EQ('\n', export_state.res_body->back());

    // should not have trailing newline character for the last line
    stateful_export_docs(&export_state, 2, done);
    ASSERT_TRUE(done);
    ASSERT_EQ('}', export_state.res_body->back());
}

TEST_F(CoreAPIUtilsTest, TestParseAPIKeyIPFromMetadata) {
    // format <length of api key>:<api key><ip address>
    std::string valid_metadata = "4:abcd127.0.0.1";
    std::string invalid_ip = "4:abcd127.0.0.1:1234";
    std::string invalid_api_key = "3:abcd127.0.0.1";
    std::string no_length = "abcd127.0.0.1";
    std::string no_colon = "4abcd127.0.0.1";
    std::string no_ip = "4:abcd";
    std::string only_length = "4:";
    std::string only_colon = ":";
    std::string only_ip = "127.0.0.1";

    Option<std::pair<std::string, std::string>> res = get_api_key_and_ip(valid_metadata);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ("abcd", res.get().first);
    EXPECT_EQ("127.0.0.1", res.get().second);

    res = get_api_key_and_ip(invalid_ip);
    EXPECT_FALSE(res.ok());

    res = get_api_key_and_ip(invalid_api_key);
    EXPECT_FALSE(res.ok());

    res = get_api_key_and_ip(no_length);
    EXPECT_FALSE(res.ok());

    res = get_api_key_and_ip(no_colon);
    EXPECT_FALSE(res.ok());

    res = get_api_key_and_ip(no_ip);
    EXPECT_FALSE(res.ok());

    res = get_api_key_and_ip(only_length);
    EXPECT_FALSE(res.ok());

    res = get_api_key_and_ip(only_colon);
    EXPECT_FALSE(res.ok());

    res = get_api_key_and_ip(only_ip);
    EXPECT_FALSE(res.ok());
}
TEST_F(CoreAPIUtilsTest, ExportIncludeExcludeFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "name", "type": "object" },
          {"name": "points", "type": "int32" }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "name": {"first": "John", "last": "Smith"},
        "points": 100
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    req->params["collection"] = "coll1";

    // include fields

    req->params["include_fields"] = "name.last";

    get_export_documents(req, res);

    std::vector<std::string> res_strs;
    StringUtils::split(res->body, res_strs, "\n");
    nlohmann::json doc = nlohmann::json::parse(res_strs[0]);
    ASSERT_EQ(1, doc.size());
    ASSERT_EQ(1, doc.count("name"));
    ASSERT_EQ(1, doc["name"].count("last"));

    // exclude fields

    delete dynamic_cast<deletion_state_t*>(req->data);
    req->data = nullptr;
    res->body.clear();
    req->params.erase("include_fields");
    req->params["exclude_fields"] = "name.last";
    get_export_documents(req, res);

    res_strs.clear();
    StringUtils::split(res->body, res_strs, "\n");
    doc = nlohmann::json::parse(res_strs[0]);
    ASSERT_EQ(3, doc.size());
    ASSERT_EQ(1, doc.count("id"));
    ASSERT_EQ(1, doc.count("points"));
    ASSERT_EQ(1, doc.count("name"));
    ASSERT_EQ(1, doc["name"].count("first"));

    collectionManager.drop_collection("coll1");
}

TEST_F(CoreAPIUtilsTest, ExportIncludeExcludeFieldsWithFilter) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "name", "type": "object" },
          {"name": "points", "type": "int32" }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "name": {"first": "John", "last": "Smith"},
        "points": 100
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    req->params["collection"] = "coll1";

    // include fields

    req->params["include_fields"] = "name.last";
    req->params["filter_by"] = "points:>=0";

    get_export_documents(req, res);

    std::vector<std::string> res_strs;
    StringUtils::split(res->body, res_strs, "\n");
    nlohmann::json doc = nlohmann::json::parse(res_strs[0]);
    ASSERT_EQ(1, doc.size());
    ASSERT_EQ(1, doc.count("name"));
    ASSERT_EQ(1, doc["name"].count("last"));

    // exclude fields

    delete dynamic_cast<deletion_state_t*>(req->data);
    req->data = nullptr;
    res->body.clear();
    req->params.erase("include_fields");
    req->params["exclude_fields"] = "name.last";
    get_export_documents(req, res);

    res_strs.clear();
    StringUtils::split(res->body, res_strs, "\n");
    doc = nlohmann::json::parse(res_strs[0]);
    ASSERT_EQ(3, doc.size());
    ASSERT_EQ(1, doc.count("id"));
    ASSERT_EQ(1, doc.count("points"));
    ASSERT_EQ(1, doc.count("name"));
    ASSERT_EQ(1, doc["name"].count("first"));

    collectionManager.drop_collection("coll1");
}
