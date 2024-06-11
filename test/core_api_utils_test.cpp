#include <gtest/gtest.h>
#include "collection.h"
#include <vector>
#include <collection_manager.h>
#include <core_api.h>
#include <analytics_manager.h>
#include "core_api_utils.h"
#include "raft_server.h"
#include "conversation_model_manager.h"
#include "conversation_manager.h"

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

        ConversationModelManager::init(store);
        nlohmann::json schema_json = R"({
            "name": "conversation_store",
            "fields": [
                {
                    "name": "conversation_id",
                    "type": "string",
                    "facet": true
                },
                {
                    "name": "role",
                    "type": "string"
                },
                {
                    "name": "message",
                    "type": "string"
                },
                {
                    "name": "timestamp",
                    "type": "int32",
                    "sort": true
                }
            ]
        })"_json;

        collectionManager.create_collection(schema_json);
        ConversationManager::get_instance().add_conversation_collection("conversation_store");
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

TEST_F(CoreAPIUtilsTest, SearchEmbeddedPresetKey) {
    nlohmann::json preset_value = R"(
        {"per_page": 100}
    )"_json;

    Option<bool> success_op = collectionManager.upsert_preset("apple", preset_value);
    ASSERT_TRUE(success_op.ok());

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json embedded_params;
    embedded_params["preset"] = "apple";
    req->embedded_params_vec.push_back(embedded_params);
    req->params["collection"] = "foo";

    get_search(req, res);
    ASSERT_EQ("100", req->params["per_page"]);

    // with multi search

    req->params.clear();
    nlohmann::json body;
    body["searches"] = nlohmann::json::array();
    nlohmann::json search;
    search["collection"] = "users";
    search["filter_by"] = "age: > 100";
    body["searches"].push_back(search);
    req->body = body.dump();

    post_multi_search(req, res);
    ASSERT_EQ("100", req->params["per_page"]);
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

TEST_F(CoreAPIUtilsTest, PresetMultiSearch) {
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
            {"collection":"coll1","q":"apple", "query_by": "name", "preset": "single_preset"}
        ]}
    )";

    req->body = search_body;
    nlohmann::json embedded_params;
    req->embedded_params_vec.push_back(embedded_params);

    post_multi_search(req, res);

    auto res_json = nlohmann::json::parse(res->body);
    ASSERT_EQ(1, res_json["results"].size());
    ASSERT_EQ(0, res_json["results"][0]["found"].get<size_t>());

    // with multiple "searches" preset configuration
    preset_value = R"(
        {"searches":[
            {"collection":"coll1", "q": "*", "per_page": "8"},
            {"collection":"coll1", "q": "*", "per_page": "11"}
        ]}
    )"_json;

    collectionManager.upsert_preset("multi_preset", preset_value);
    embedded_params.clear();
    req->params.clear();
    req->params["preset"] = "multi_preset";
    req->embedded_params_vec.clear();
    req->embedded_params_vec.push_back(embedded_params);
    req->embedded_params_vec.push_back(embedded_params);

    //  "preset": "multi_preset"
    search_body = R"(
        {"searches":[
            {"collection":"coll1","q":"apple", "query_by": "title"}
        ]}
    )";

    req->body = search_body;

    post_multi_search(req, res);
    res_json = nlohmann::json::parse(res->body);
    ASSERT_EQ(2, res_json["results"].size());
    ASSERT_EQ(0, res_json["results"][0]["found"].get<size_t>());
    ASSERT_EQ(0, res_json["results"][1]["found"].get<size_t>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CoreAPIUtilsTest, SearchPagination) {
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

    for(size_t i = 0; i < 20; i++) {
        nlohmann::json doc;
        doc["name"] = "Title " + std::to_string(i);
        doc["points"] = i;
        coll1->add(doc.dump(), CREATE);
    }

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json body;

    // without any pagination params, default is top 10 records by sort order

    body["searches"] = nlohmann::json::array();
    nlohmann::json search;
    search["collection"] = "coll1";
    search["q"] = "title";
    search["query_by"] = "name";
    search["sort_by"] = "points:desc";
    body["searches"].push_back(search);

    req->body = body.dump();
    nlohmann::json embedded_params;
    req->embedded_params_vec.push_back(embedded_params);

    post_multi_search(req, res);
    nlohmann::json results = nlohmann::json::parse(res->body)["results"][0];
    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(19, results["hits"][0]["document"]["points"].get<size_t>());
    ASSERT_EQ(1, results["page"].get<size_t>());

    // when offset is used we should expect the same but "offset" should be returned in response
    search.clear();
    req->params.clear();
    body["searches"] = nlohmann::json::array();
    search["collection"] = "coll1";
    search["q"] = "title";
    search["offset"] = "1";
    search["query_by"] = "name";
    search["sort_by"] = "points:desc";
    body["searches"].push_back(search);
    req->body = body.dump();

    post_multi_search(req, res);
    results = nlohmann::json::parse(res->body)["results"][0];
    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(18, results["hits"][0]["document"]["points"].get<size_t>());
    ASSERT_EQ(1, results["offset"].get<size_t>());

    // use limit to restrict page size
    search.clear();
    req->params.clear();
    body["searches"] = nlohmann::json::array();
    search["collection"] = "coll1";
    search["q"] = "title";
    search["offset"] = "1";
    search["limit"] = "5";
    search["query_by"] = "name";
    search["sort_by"] = "points:desc";
    body["searches"].push_back(search);
    req->body = body.dump();

    post_multi_search(req, res);
    results = nlohmann::json::parse(res->body)["results"][0];
    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(18, results["hits"][0]["document"]["points"].get<size_t>());
    ASSERT_EQ(1, results["offset"].get<size_t>());

    // when page is -1
    search.clear();
    req->params.clear();
    body["searches"] = nlohmann::json::array();
    search["collection"] = "coll1";
    search["q"] = "title";
    search["page"] = "-1";
    search["limit"] = "5";
    search["query_by"] = "name";
    search["sort_by"] = "points:desc";
    body["searches"].push_back(search);
    req->body = body.dump();

    post_multi_search(req, res);
    results = nlohmann::json::parse(res->body)["results"][0];
    ASSERT_EQ(400, results["code"].get<size_t>());
    ASSERT_EQ("Parameter `page` must be an unsigned integer.", results["error"].get<std::string>());

    // when offset is -1
    search.clear();
    req->params.clear();
    body["searches"] = nlohmann::json::array();
    search["collection"] = "coll1";
    search["q"] = "title";
    search["offset"] = "-1";
    search["query_by"] = "name";
    search["sort_by"] = "points:desc";
    body["searches"].push_back(search);
    req->body = body.dump();

    post_multi_search(req, res);
    results = nlohmann::json::parse(res->body)["results"][0];
    ASSERT_EQ(400, results["code"].get<size_t>());
    ASSERT_EQ("Parameter `offset` must be an unsigned integer.", results["error"].get<std::string>());

    // when page is 0 and offset is NOT sent, we will treat as page=1
    search.clear();
    req->params.clear();
    body["searches"] = nlohmann::json::array();
    search["collection"] = "coll1";
    search["q"] = "title";
    search["page"] = "0";
    search["query_by"] = "name";
    search["sort_by"] = "points:desc";
    body["searches"].push_back(search);
    req->body = body.dump();

    post_multi_search(req, res);
    results = nlohmann::json::parse(res->body)["results"][0];
    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(1, results["page"].get<size_t>());
    ASSERT_EQ(0, results.count("offset"));

    // when both page and offset are sent, use page
    search.clear();
    req->params.clear();
    body["searches"] = nlohmann::json::array();
    search["collection"] = "coll1";
    search["q"] = "title";
    search["page"] = "2";
    search["offset"] = "30";
    search["query_by"] = "name";
    search["sort_by"] = "points:desc";
    body["searches"].push_back(search);
    req->body = body.dump();

    post_multi_search(req, res);
    results = nlohmann::json::parse(res->body)["results"][0];
    ASSERT_EQ(10, results["hits"].size());
    ASSERT_EQ(2, results["page"].get<size_t>());
    ASSERT_EQ(0, results.count("offset"));

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

    delete dynamic_cast<export_state_t*>(req->data);
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

    delete dynamic_cast<export_state_t*>(req->data);
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

TEST_F(CoreAPIUtilsTest, TestProxy) {
    std::string res;
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;

    std::string url = "https://typesense.org";

    long expected_status_code = HttpClient::get_instance().get_response(url, res, res_headers, headers);

    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    nlohmann::json body;
    body["url"] = url;
    body["method"] = "GET";
    body["headers"] = headers;

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(expected_status_code, resp->status_code);
    ASSERT_EQ(res, resp->body);
}


TEST_F(CoreAPIUtilsTest, TestProxyInvalid) {
    nlohmann::json body;
    


    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    // test with url as empty string
    body["url"] = "";
    body["method"] = "GET";
    body["headers"] = nlohmann::json::object();

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("URL and method must be non-empty strings.", nlohmann::json::parse(resp->body)["message"]);

    // test with url as integer
    body["url"] = 123;
    body["method"] = "GET";

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("URL and method must be non-empty strings.", nlohmann::json::parse(resp->body)["message"]);

    // test with no url parameter
    body.erase("url");
    body["method"] = "GET";

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Missing required fields.", nlohmann::json::parse(resp->body)["message"]);


    // test with invalid method
    body["url"] = "https://typesense.org";
    body["method"] = "INVALID";

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Parameter `method` must be one of GET, POST, POST_STREAM, PUT, DELETE.", nlohmann::json::parse(resp->body)["message"]);

    // test with method as integer
    body["method"] = 123;

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("URL and method must be non-empty strings.", nlohmann::json::parse(resp->body)["message"]);

    // test with no method parameter
    body.erase("method");

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Missing required fields.", nlohmann::json::parse(resp->body)["message"]);


    // test with body as integer
    body["method"] = "POST";
    body["body"] = 123;

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Body must be a string.", nlohmann::json::parse(resp->body)["message"]);


    // test with headers as integer
    body["body"] = "";
    body["headers"] = 123;

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Headers must be a JSON object.", nlohmann::json::parse(resp->body)["message"]);
}

TEST_F(CoreAPIUtilsTest, TestProxyTimeout) {
    nlohmann::json body;

    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    // test with url as empty string
    body["url"] = "https://typesense.org/docs/";
    body["method"] = "GET";
    body["headers"] = nlohmann::json::object();
    body["headers"]["timeout_ms"] = "1";
    body["headers"]["num_retry"] = "1";

    req->body = body.dump();

    post_proxy(req, resp);

    ASSERT_EQ(408, resp->status_code);
    ASSERT_EQ("Server error on remote server. Please try again later.", nlohmann::json::parse(resp->body)["message"]);
}

TEST_F(CoreAPIUtilsTest, TestGetConversations) {
    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    req->params["id"] = "0";

    get_conversations(req, resp);

    ASSERT_EQ(200, resp->status_code);

    nlohmann::json res_json = nlohmann::json::parse(resp->body);
    ASSERT_TRUE(res_json.is_array());
    ASSERT_EQ(0, res_json.size());

    get_conversation(req, resp);

    ASSERT_EQ(404, resp->status_code);

    auto schema_json =
        R"({
        "name": "Products",
        "fields": [
            {"name": "product_name", "type": "string", "infix": true},
            {"name": "category", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["product_name", "category"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    if (std::getenv("api_key") == nullptr) {
        LOG(INFO) << "Skipping test as api_key is not set.";
        return;
    }

    auto api_key = std::string(std::getenv("api_key"));

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "product_name": "moisturizer",
        "category": "beauty"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "product_name": "shampoo",
        "category": "beauty"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "product_name": "shirt",
        "category": "clothing"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "product_name": "pants",
        "category": "clothing"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    nlohmann::json model_config = R"({
        "model_name": "openai/gpt-3.5-turbo"
    })"_json;

    model_config["api_key"] = api_key;

    auto add_model_op = ConversationModelManager::add_model(model_config);

    ASSERT_TRUE(add_model_op.ok());

    LOG(INFO) << "Model id: " << add_model_op.get();

    auto model_id = add_model_op.get()["id"].get<std::string>();

    auto results_op = coll->search("how many products are there for clothing category?", {"embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>(), {},
                                 10, "", 30, 4, "", 1, "", "", {}, 3, "<mark>", "</mark>", {}, 4294967295UL, true, false,
                                 true, "", false, 6000000UL, 4, 7, fallback, 4, {off}, 32767UL, 32767UL, 2, 2, false, "",
                                 true, 0, max_score, 100, 0, 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true, true, true, model_id);
    
    ASSERT_TRUE(results_op.ok());

    get_conversations(req, resp);

    ASSERT_EQ(200, resp->status_code);

    res_json = nlohmann::json::parse(resp->body);

    ASSERT_TRUE(res_json.is_array());
    ASSERT_EQ(1, res_json.size());

    ASSERT_TRUE(res_json[0]["conversation"].is_array());
    ASSERT_EQ(2, res_json[0]["conversation"].size());
    ASSERT_EQ("how many products are there for clothing category?", res_json[0]["conversation"][0]["user"].get<std::string>());

    req->params["id"] = res_json[0]["id"].get<std::string>();
    get_conversation(req, resp);

    ASSERT_EQ(200, resp->status_code);

    res_json = nlohmann::json::parse(resp->body);

    ASSERT_TRUE(res_json.is_object());
    ASSERT_TRUE(res_json["conversation"].is_array());
    ASSERT_EQ(2, res_json["conversation"].size());
    ASSERT_EQ("how many products are there for clothing category?", res_json["conversation"][0]["user"].get<std::string>());

    del_conversation(req, resp);
    ASSERT_EQ(200, resp->status_code);

    get_conversations(req, resp);
    ASSERT_EQ(200, resp->status_code);

    res_json = nlohmann::json::parse(resp->body);
    ASSERT_TRUE(res_json.is_array());
    ASSERT_EQ(0, res_json.size());

    auto del_res = ConversationModelManager::delete_model(model_id);
}

TEST_F(CoreAPIUtilsTest, SampleGzipIndexTest) {
    Collection *coll_hnstories;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll_hnstories = collectionManager.get_collection("coll_hnstories").get();
    if(coll_hnstories == nullptr) {
        coll_hnstories = collectionManager.create_collection("coll_hnstories", 4, fields, "title").get();
    }

    auto req = std::make_shared<http_req>();
    std::ifstream infile(std::string(ROOT_DIR)+"test/resources/hnstories.jsonl.gz");
    std::stringstream outbuffer;

    infile.seekg (0, infile.end);
    int length = infile.tellg();
    infile.seekg (0, infile.beg);

    req->body.resize(length);
    infile.read(&req->body[0], length);

    auto res = ReplicationState::handle_gzip(req);
    if (!res.error().empty()) {
        LOG(ERROR) << res.error();
        FAIL();
    } else {
        outbuffer << req->body;
    }

    std::vector<std::string> doc_lines;
    std::string line;
    while(std::getline(outbuffer, line)) {
        doc_lines.push_back(line);
    }

    ASSERT_EQ(14, doc_lines.size());
    ASSERT_EQ("{\"points\":1,\"title\":\"DuckDuckGo Settings\"}", doc_lines[0]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Making Twitter Easier to Use\"}", doc_lines[1]);
    ASSERT_EQ("{\"points\":2,\"title\":\"London refers Uber app row to High Court\"}", doc_lines[2]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Young Global Leaders, who should be nominated? (World Economic Forum)\"}", doc_lines[3]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Blooki.st goes BETA in a few hours\"}", doc_lines[4]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Unicode Security Data: Beta Review\"}", doc_lines[5]);
    ASSERT_EQ("{\"points\":2,\"title\":\"FileMap: MapReduce on the CLI\"}", doc_lines[6]);
    ASSERT_EQ("{\"points\":1,\"title\":\"[Full Video] NBC News Interview with Edward Snowden\"}", doc_lines[7]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Hybrid App Monetization Example with Mobile Ads and In-App Purchases\"}", doc_lines[8]);
    ASSERT_EQ("{\"points\":1,\"title\":\"We need oppinion from Android Developers\"}", doc_lines[9]);
    ASSERT_EQ("{\"points\":1,\"title\":\"\\\\t Why Mobile Developers Should Care About Deep Linking\"}", doc_lines[10]);
    ASSERT_EQ("{\"points\":2,\"title\":\"Are we getting too Sassy? Weighing up micro-optimisation vs. maintainability\"}", doc_lines[11]);
    ASSERT_EQ("{\"points\":2,\"title\":\"Google's XSS game\"}", doc_lines[12]);
    ASSERT_EQ("{\"points\":1,\"title\":\"Telemba Turns Your Old Roomba and Tablet Into a Telepresence Robot\"}", doc_lines[13]);

    infile.close();
}

TEST_F(CoreAPIUtilsTest, TestConversationModels) {
    nlohmann::json model_config = R"({
        "model_name": "openai/gpt-3.5-turbo",
        "max_bytes": 10000,
        "conversation_collection": "conversation_store"
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    if (std::getenv("api_key") == nullptr) {
        LOG(INFO) << "Skipping test as api_key is not set.";
        return;
    }

    model_config["api_key"] = std::string(std::getenv("api_key"));

    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    req->body = model_config.dump();
    post_conversation_model(req, resp);
    ASSERT_EQ(200, resp->status_code);

    auto id = nlohmann::json::parse(resp->body)["id"].get<std::string>();
    req->params["id"] = id;
    get_conversation_model(req, resp);

    ASSERT_EQ(200, resp->status_code);
    ASSERT_EQ(id, nlohmann::json::parse(resp->body)["id"].get<std::string>());

    get_conversation_models(req, resp);
    ASSERT_EQ(200, resp->status_code);
    ASSERT_EQ(1, nlohmann::json::parse(resp->body).size());

    del_conversation_model(req, resp);
    ASSERT_EQ(200, resp->status_code);

    get_conversation_models(req, resp);
    ASSERT_EQ(200, resp->status_code);
    ASSERT_EQ(0, nlohmann::json::parse(resp->body).size());
}

TEST_F(CoreAPIUtilsTest, TestInvalidConversationModels) {
    // test with no model_name
    nlohmann::json model_config = R"({
        "conversation_collection": "conversation_store"
    })"_json;

    if (std::getenv("api_key") == nullptr) {
        LOG(INFO) << "Skipping test as api_key is not set.";
        return;
    }

    model_config["api_key"] = std::string(std::getenv("api_key"));

    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    req->body = model_config.dump();

    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Property `model_name` is not provided or not a string.", nlohmann::json::parse(resp->body)["message"]);

    // test with invalid model_name
    model_config["model_name"] = "invalid_model_name";

    req->body = model_config.dump();

    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Model namespace `` is not supported.", nlohmann::json::parse(resp->body)["message"]);

    // test with no api_key
    model_config["model_name"] = "openai/gpt-3.5-turbo";
    model_config.erase("api_key");

    req->body = model_config.dump();

    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("API key is not provided", nlohmann::json::parse(resp->body)["message"]);

    // test with api_key as integer
    model_config["api_key"] = 123;

    req->body = model_config.dump();

    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("API key is not a string", nlohmann::json::parse(resp->body)["message"]);

    // test with model_name as integer

    model_config["api_key"] = std::string(std::getenv("api_key"));
    model_config["model_name"] = 123;

    req->body = model_config.dump();

    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Property `model_name` is not provided or not a string.", nlohmann::json::parse(resp->body)["message"]);

    model_config["model_name"] = "openai/gpt-3.5-turbo";

    // test without max_bytes
    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Property `max_bytes` is not provided or not a number.", nlohmann::json::parse(resp->body)["message"]);

    // test with max_bytes as string
    model_config["max_bytes"] = "10000";

    req->body = model_config.dump();
    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Property `max_bytes` is not provided or not a number.", nlohmann::json::parse(resp->body)["message"]);

    // test with max_bytes as negative number
    model_config["max_bytes"] = -10000;

    req->body = model_config.dump();
    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Property `max_bytes` must be a positive number.", nlohmann::json::parse(resp->body)["message"]);

    model_config["max_bytes"] = 10000;
    model_config["conversation_collection"] = 123;

    // test with conversation_collection as integer
    req->body = model_config.dump();
    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Property `conversation_collection` is not provided or not a string.", nlohmann::json::parse(resp->body)["message"]);

    // test with conversation_collection as empty string
    model_config["conversation_collection"] = "";

    req->body = model_config.dump();
    post_conversation_model(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("Collection not found", nlohmann::json::parse(resp->body)["message"]);
}

TEST_F(CoreAPIUtilsTest, DeleteNonExistingDoc) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 2, fields, "points").get();
    }

    for(size_t i=0; i<10; i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        coll1->add(doc.dump());
    }

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["collection"] = "coll1";
    req->params["id"] = "9";
    del_remove_document(req, res);
    ASSERT_EQ(200, res->status_code);

    req->params["id"] = "10";
    del_remove_document(req, res);
    ASSERT_EQ(404, res->status_code);

    req->params["ignore_not_found"] = "true";
    del_remove_document(req, res);
    ASSERT_EQ(200, res->status_code);
}

TEST_F(CoreAPIUtilsTest, CollectionsPagination) {
    //remove all collections first
    auto collections = collectionManager.get_collections().get();
    for(auto collection : collections) {
        collectionManager.drop_collection(collection->get_name());
    }

    //create few collections
    for(size_t i = 0; i < 5; i++) {
        nlohmann::json coll_json = R"({
                "name": "cp",
                "fields": [
                    {"name": "title", "type": "string"}
                ]
            })"_json;
        coll_json["name"] = coll_json["name"].get<std::string>() + std::to_string(i + 1);
        auto coll_op = collectionManager.create_collection(coll_json);
        ASSERT_TRUE(coll_op.ok());
    }

    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    req->params["offset"] = "0";
    req->params["limit"] = "1";

    nlohmann::json expected_meta_json = R"(
        {
          "created_at":1663234047,
          "default_sorting_field":"",
          "enable_nested_fields":false,
          "fields":[
            {
              "facet":false,
              "index":true,
              "infix":false,
              "locale":"",
              "name":"title",
              "optional":false,
              "sort":false,
              "stem":false,
              "type":"string"
            }
          ],
          "name":"cp2",
          "num_documents":0,
          "symbols_to_index":[],
          "token_separators":[]
        }
    )"_json;

    get_collections(req, resp);

    auto actual_json = nlohmann::json::parse(resp->body);
    expected_meta_json["created_at"] = actual_json[0]["created_at"];

    ASSERT_EQ(expected_meta_json.dump(), actual_json[0].dump());

    //invalid offset string
    req->params["offset"] = "0a";
    get_collections(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("{\"message\": \"Offset param should be unsigned integer.\"}", resp->body);

    //invalid limit string
    req->params["offset"] = "0";
    req->params["limit"] = "-1";
    get_collections(req, resp);
    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("{\"message\": \"Limit param should be unsigned integer.\"}", resp->body);
}

TEST_F(CoreAPIUtilsTest, OverridesPagination) {
    Collection *coll2;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields, "points").get();
    }

    for(int i = 0; i < 5; ++i) {
        nlohmann::json override_json = {
                {"id",       "override"},
                {
                 "rule",     {
                                     {"query", "not-found"},
                                     {"match", override_t::MATCH_EXACT}
                             }
                },
                {"metadata", {       {"foo",   "bar"}}},
        };

        override_json["id"] = override_json["id"].get<std::string>() + std::to_string(i + 1);
        override_t override;
        override_t::parse(override_json, "", override);

        coll2->add_override(override);
    }

    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    req->params["collection"] = "coll2";
    req->params["offset"] = "0";
    req->params["limit"] = "1";

    get_overrides(req, resp);
    nlohmann::json expected_json = R"({
        "overrides":[
                    {
                        "excludes":[],
                        "filter_curated_hits":false,
                        "id":"override1",
                        "includes":[],
                        "metadata":{"foo":"bar"},
                        "remove_matched_tokens":false,
                        "rule":{
                                "match":"exact",
                                "query":"not-found"
                        },
                        "stop_processing":true
                    }]
    })"_json;

    ASSERT_EQ(expected_json.dump(), resp->body);

    //invalid offset string
    req->params["offset"] = "0a";
    get_collections(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("{\"message\": \"Offset param should be unsigned integer.\"}", resp->body);

    //invalid limit string
    req->params["offset"] = "0";
    req->params["limit"] = "-1";
    get_collections(req, resp);
    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("{\"message\": \"Limit param should be unsigned integer.\"}", resp->body);
}

TEST_F(CoreAPIUtilsTest, SynonymsPagination) {
    Collection *coll3;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll3 = collectionManager.get_collection("coll3").get();
    if (coll3 == nullptr) {
        coll3 = collectionManager.create_collection("coll3", 1, fields, "points").get();
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

    auto req = std::make_shared<http_req>();
    auto resp = std::make_shared<http_res>(nullptr);

    req->params["collection"] = "coll3";
    req->params["offset"] = "0";
    req->params["limit"] = "1";

    get_synonyms(req, resp);

    nlohmann::json expected_json = R"({
        "synonyms":[
                    {
                        "id":"foobar1",
                        "root":"",
                        "synonyms":["blazer","suit"]
                    }]
    })"_json;

    ASSERT_EQ(expected_json.dump(), resp->body);

    //invalid offset string
    req->params["offset"] = "0a";
    get_collections(req, resp);

    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("{\"message\": \"Offset param should be unsigned integer.\"}", resp->body);

    //invalid limit string
    req->params["offset"] = "0";
    req->params["limit"] = "-1";
    get_collections(req, resp);
    ASSERT_EQ(400, resp->status_code);
    ASSERT_EQ("{\"message\": \"Limit param should be unsigned integer.\"}", resp->body);
}


TEST_F(CoreAPIUtilsTest, CollectionMetadataUpdate) {
    CollectionManager & collectionManager3 = CollectionManager::get_instance();

    nlohmann::json schema = R"({
        "name": "collection_meta",
        "enable_nested_fields": true,
        "fields": [
          {"name": "value.color", "type": "string", "optional": false, "facet": true },
          {"name": "value.r", "type": "int32", "optional": false, "facet": true },
          {"name": "value.g", "type": "int32", "optional": false, "facet": true },
          {"name": "value.b", "type": "int32", "optional": false, "facet": true }
        ],
        "metadata": {
            "batch_job":"",
            "indexed_from":"2023-04-20T00:00:00.000Z",
            "total_docs": 0
        }
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    std::string collection_meta_json;
    nlohmann::json collection_meta;
    std::string next_seq_id;
    std::string next_collection_id;

    store->get(Collection::get_meta_key("collection_meta"), collection_meta_json);

    nlohmann::json expected_meta_json = R"(
        {
            "created_at":1705482381,
            "default_sorting_field":"",
            "enable_nested_fields":true,
            "fallback_field_type":"",
            "fields":[
                {
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.color",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":false,
                    "store":true,
                    "type":"string"
                },
                {
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.r",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":true,
                    "store":true,
                    "type":"int32"
                },{
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.g",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":true,
                    "store":true,
                    "type":"int32"
                },{
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.b",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":true,
                    "store":true,
                    "type":"int32"
                }
            ],
            "id":1,
            "metadata":{
                "batch_job":"",
                "indexed_from":"2023-04-20T00:00:00.000Z",
                "total_docs":0
            },
            "name":"collection_meta",
            "num_memory_shards":4,
            "symbols_to_index":[],
            "token_separators":[]
    })"_json;

    auto actual_json = nlohmann::json::parse(collection_meta_json);
    expected_meta_json["created_at"] = actual_json["created_at"];

    ASSERT_EQ(expected_meta_json.dump(), actual_json.dump());

    //try setting empty metadata
    auto metadata = R"({
        "metadata": {}
    })"_json;

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["collection"] = "collection_meta";
    req->body = metadata.dump();
    patch_update_collection(req, res);

    expected_meta_json = R"(
        {
            "created_at":1705482381,
            "default_sorting_field":"",
            "enable_nested_fields":true,
            "fallback_field_type":"",
            "fields":[
                {
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.color",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":false,
                    "store":true,
                    "type":"string"
                },
                {
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.r",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":true,
                    "store":true,
                    "type":"int32"
                },{
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.g",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":true,
                    "store":true,
                    "type":"int32"
                },{
                    "facet":true,
                    "index":true,
                    "infix":false,
                    "locale":"",
                    "name":"value.b",
                    "nested":true,
                    "nested_array":2,
                    "optional":false,
                    "sort":true,
                    "store":true,
                    "type":"int32"
                }
            ],
            "id":1,
            "metadata":{
            },
            "name":"collection_meta",
            "num_memory_shards":4,
            "symbols_to_index":[],
            "token_separators":[]
    })"_json;

    store->get(Collection::get_meta_key("collection_meta"), collection_meta_json);
    actual_json = nlohmann::json::parse(collection_meta_json);
    expected_meta_json["created_at"] = actual_json["created_at"];
    ASSERT_EQ(expected_meta_json.dump(), actual_json.dump());
}

TEST_F(CoreAPIUtilsTest, CollectionUpdateValidation) {
    CollectionManager & collectionManager3 = CollectionManager::get_instance();

    nlohmann::json schema = R"({
        "name": "collection_meta",
        "enable_nested_fields": true,
        "fields": [
          {"name": "value.color", "type": "string", "optional": false, "facet": true },
          {"name": "value.r", "type": "int32", "optional": false, "facet": true },
          {"name": "value.g", "type": "int32", "optional": false, "facet": true },
          {"name": "value.b", "type": "int32", "optional": false, "facet": true }
        ],
        "metadata": {
            "batch_job":"",
            "indexed_from":"2023-04-20T00:00:00.000Z",
            "total_docs": 0
        }
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto alter_schema = R"({
        "metadata": {},
        "fields":[
            {"name": "value.color", "drop": true },
           {"name": "value.color", "type": "string", "facet": true }
        ]
    })"_json;

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["collection"] = "collection_meta";
    req->body = alter_schema.dump();

    ASSERT_TRUE(patch_update_collection(req, res));

    alter_schema = R"({
        "metadata": {},
        "symbols_to_index":[]
    })"_json;

    req->body = alter_schema.dump();
    ASSERT_FALSE(patch_update_collection(req, res));
    ASSERT_EQ("{\"message\": \"Only `fields` and `metadata` can be updated at the moment.\"}", res->body);

    alter_schema = R"({
        "symbols_to_index":[]
    })"_json;

    req->body = alter_schema.dump();
    ASSERT_FALSE(patch_update_collection(req, res));
    ASSERT_EQ("{\"message\": \"Only `fields` and `metadata` can be updated at the moment.\"}", res->body);

    alter_schema = R"({
        "name": "collection_meta2",
        "metadata": {},
        "fields":[
            {"name": "value.hue", "type": "int32", "optional": false, "facet": true }
        ]
    })"_json;

    req->body = alter_schema.dump();
    ASSERT_FALSE(patch_update_collection(req, res));
    ASSERT_EQ("{\"message\": \"Only `fields` and `metadata` can be updated at the moment.\"}", res->body);

    alter_schema = R"({
    })"_json;

    req->body = alter_schema.dump();
    ASSERT_FALSE(patch_update_collection(req, res));
    ASSERT_EQ("{\"message\": \"Alter payload is empty.\"}", res->body);
}

TEST_F(CoreAPIUtilsTest, DocumentGetIncludeExcludeFields) {
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("brand", field_types::STRING, true, true),
            field("size", field_types::INT32, true, false),
            field("colors", field_types::STRING_ARRAY, true, false),
            field("rating", field_types::FLOAT, true, false)
    };

    auto coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "rating").get();
    }

    nlohmann::json doc;
    doc["id"] = "1";
    doc["title"] = "Denim jeans";
    doc["brand"] = "Spykar";
    doc["size"] = 40;
    doc["colors"] = {"blue", "black", "grey"};
    doc["rating"] = 4.5;
    coll1->add(doc.dump());

    doc["id"] = "2";
    doc["title"] = "Denim jeans";
    doc["brand"] = "Levis";
    doc["size"] = 42;
    doc["colors"] = {"blue", "black"};
    doc["rating"] = 4.4;
    coll1->add(doc.dump());

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["collection"] = "coll1";
    req->params["id"] = "1";

    //normal doc fetch
    ASSERT_TRUE(get_fetch_document(req, res));
    auto resp = nlohmann::json::parse(res->body);
    ASSERT_EQ(6, resp.size());
    ASSERT_TRUE(resp.contains("brand"));
    ASSERT_TRUE(resp.contains("size"));
    ASSERT_TRUE(resp.contains("colors"));
    ASSERT_TRUE(resp.contains("rating"));
    ASSERT_TRUE(resp.contains("id"));
    ASSERT_TRUE(resp.contains("title"));

    //include fields
    req->params["include_fields"] = "brand,size,colors";

    ASSERT_TRUE(get_fetch_document(req, res));
    resp = nlohmann::json::parse(res->body);
    ASSERT_EQ(3, resp.size());
    ASSERT_TRUE(resp.contains("brand"));
    ASSERT_TRUE(resp.contains("size"));
    ASSERT_TRUE(resp.contains("colors"));
    ASSERT_FALSE(resp.contains("rating"));

    //exclude fields
    req->params.erase("include_fields");
    req->params["exclude_fields"] = "brand,size,colors";
    ASSERT_TRUE(get_fetch_document(req, res));
    resp = nlohmann::json::parse(res->body);
    ASSERT_EQ(3, resp.size());
    ASSERT_TRUE(resp.contains("id"));
    ASSERT_TRUE(resp.contains("title"));
    ASSERT_TRUE(resp.contains("rating"));
    ASSERT_FALSE(resp.contains("brand"));

    //both include and exclude fields
    req->params["include_fields"] = "title,rating";
    req->params["exclude_fields"] = "brand,size,colors";
    ASSERT_TRUE(get_fetch_document(req, res));
    resp = nlohmann::json::parse(res->body);
    ASSERT_EQ(2, resp.size());
    ASSERT_TRUE(resp.contains("title"));
    ASSERT_TRUE(resp.contains("rating"));
    ASSERT_FALSE(resp.contains("id"));
}