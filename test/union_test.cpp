#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <cstdlib>
#include <atomic>
#include <chrono>
#include <thread>
#include <sstream>
#include <collection_manager.h>
#include "curation_index_manager.h"

class UnionTest : public ::testing::Test {
protected:
    Store *store = nullptr;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::string state_dir_path = "/tmp/typesense_test/union";

    std::map<std::string, std::string> req_params{};
    std::vector<nlohmann::json> embedded_params;
    nlohmann::json searches;
    nlohmann::json json_res;
    long now_ts = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    void setupCollection() {
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        EmbedderManager::set_model_dir("/tmp/typesense_test/models");
    }

    void setupProductsCollection() {
        auto schema_json =
                R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "product_name", "type": "string", "infix": true},
                    {"name": "product_description", "type": "string"},
                    {"name": "embedding", "type":"float[]", "embed":{"from": ["product_description"], "model_config": {"model_name": "ts/e5-small"}}},
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

        auto collection_create_op = collectionManager.create_collection(schema_json);
        ASSERT_TRUE(collection_create_op.ok());

        auto products = collection_create_op.get();
        for (auto const &json: documents) {
            auto add_op = products->add(json.dump());
            if (!add_op.ok()) {
                LOG(INFO) << add_op.error();
            }
            ASSERT_TRUE(add_op.ok());
        }
    }

    void setupFoodsAndMealsCollection() {
        auto schema_json =
                R"({
                "name": "Portions",
                "fields": [
                    {"name": "portion_id", "type": "string"},
                    {"name": "quantity", "type": "int32"},
                    {"name": "unit", "type": "string"}
                ]
            })"_json;
        std::vector<nlohmann::json> documents = {
                R"({
                "portion_id": "portion_a",
                "quantity": 500,
                "unit": "g"
            })"_json,
                R"({
                "portion_id": "portion_b",
                "quantity": 1,
                "unit": "lt"
            })"_json,
                R"({
                "portion_id": "portion_c",
                "quantity": 500,
                "unit": "ml"
            })"_json
        };

        auto collection_create_op = collectionManager.create_collection(schema_json);
        ASSERT_TRUE(collection_create_op.ok());
        for (auto const &json: documents) {
            auto add_op = collection_create_op.get()->add(json.dump());
            if (!add_op.ok()) {
                LOG(INFO) << add_op.error();
            }
            ASSERT_TRUE(add_op.ok());
        }

        schema_json =
                R"({
                "name": "Foods",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "portions", "type": "object[]"},
                    {"name": "portions.portion_id", "type": "string[]", "reference": "Portions.portion_id", "optional": true}
                ],
                "enable_nested_fields": true
            })"_json;
        documents = {
                R"({
                    "name": "Bread",
                    "portions": [
                        {
                            "portion_id": "portion_a",
                            "count": 10
                        }
                    ]
                })"_json,
                R"({
                    "name": "Milk",
                    "portions": [
                        {
                            "portion_id": "portion_b",
                            "count": 3
                        },
                        {
                            "count": 3
                        },
                        {
                            "portion_id": "portion_c",
                            "count": 1
                        }
                    ]
                })"_json
        };

        collection_create_op = collectionManager.create_collection(schema_json);
        ASSERT_TRUE(collection_create_op.ok());
        for (auto const &json: documents) {
            auto add_op = collection_create_op.get()->add(json.dump());
            if (!add_op.ok()) {
                LOG(INFO) << add_op.error();
            }
            ASSERT_TRUE(add_op.ok());
        }

        schema_json =
                R"({
                "name": "UserFavoriteFoods",
                "fields": [
                    {"name": "user_id", "type": "string"},
                    {"name": "food_id", "type": "string", "reference": "Foods.id"}
                ],
                "enable_nested_fields": true
            })"_json;
        documents = {
                R"({
                "user_id": "user_a",
                "food_id": "0"
            })"_json
        };

        collection_create_op = collectionManager.create_collection(schema_json);
        ASSERT_TRUE(collection_create_op.ok());
        for (auto const &json: documents) {
            auto add_op = collection_create_op.get()->add(json.dump());
            if (!add_op.ok()) {
                LOG(INFO) << add_op.error();
            }
            ASSERT_TRUE(add_op.ok());
        }

        schema_json =
                R"({
                "name": "Meals",
                "fields": [
                    {"name": "title", "type": "string"},
                    {"name": "foods", "type": "string[]", "reference": "Foods.id"},
                    {"name": "calories", "type": "int32"}
                ],
                "enable_nested_fields": true
            })"_json;
        documents = {
                R"({
                "title": "Light",
                "foods": ["1"],
                "calories": 1000
            })"_json,
                R"({
                "title": "Heavy",
                "foods": ["0", "1"],
                "calories": 1500
            })"_json
        };

        collection_create_op = collectionManager.create_collection(schema_json);
        ASSERT_TRUE(collection_create_op.ok());
        for (auto const &json: documents) {
            auto add_op = collection_create_op.get()->add(json.dump());
            if (!add_op.ok()) {
                LOG(INFO) << add_op.error();
            }
            ASSERT_TRUE(add_op.ok());
        }

        schema_json =
                R"({
                "name": "UserFavoriteMeals",
                "fields": [
                    {"name": "user_id", "type": "string"},
                    {"name": "meal_id", "type": "string", "reference": "Meals.id"}
                ],
                "enable_nested_fields": true
            })"_json;
        documents = {
                R"({
                "user_id": "user_a",
                "meal_id": "1"
            })"_json
        };

        collection_create_op = collectionManager.create_collection(schema_json);
        ASSERT_TRUE(collection_create_op.ok());
        for (auto const &json: documents) {
            auto add_op = collection_create_op.get()->add(json.dump());
            if (!add_op.ok()) {
                LOG(INFO) << add_op.error();
            }
            ASSERT_TRUE(add_op.ok());
        }
    }

    void setupNumericArrayCollectionWithDefaultSortingField() {
        Collection *coll_array_fields;

        std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
        std::vector<field> fields = {
                field("name", field_types::STRING, false),
                field("age", field_types::INT32, false),
                field("years", field_types::INT32_ARRAY, false),
                field("tags", field_types::STRING_ARRAY, true),
                field("rating", field_types::FLOAT, true)
        };

        coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
        if(coll_array_fields == nullptr) {
            coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields, "age").get();
        }

        std::string json_line;

        while (std::getline(infile, json_line)) {
            coll_array_fields->add(json_line);
        }

        infile.close();
    }

    void setupBoolCollectionWithDefaultSortingField() {
        Collection *coll_bool;

        std::ifstream infile(std::string(ROOT_DIR)+"test/bool_documents.jsonl");
        std::vector<field> fields = {
                field("popular", field_types::BOOL, false),
                field("title", field_types::STRING, false),
                field("rating", field_types::FLOAT, false),
                field("bool_array", field_types::BOOL_ARRAY, false),
        };

        coll_bool = collectionManager.get_collection("coll_bool").get();
        if(coll_bool == nullptr) {
            coll_bool = collectionManager.create_collection("coll_bool", 1, fields, "rating").get();
        }

        std::string json_line;

        while (std::getline(infile, json_line)) {
            coll_bool->add(json_line);
        }

        infile.close();
    }

    void setupNumericArrayCollection() {
        Collection *coll_array_fields;

        std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
        std::vector<field> fields = {
                field("name", field_types::STRING, false),
                field("age", field_types::INT32, false),
                field("years", field_types::INT32_ARRAY, false),
                field("tags", field_types::STRING_ARRAY, true),
                field("rating", field_types::FLOAT, true)
        };

        coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
        if(coll_array_fields == nullptr) {
            coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields).get();
        }

        std::string json_line;

        while (std::getline(infile, json_line)) {
            coll_array_fields->add(json_line);
        }

        infile.close();
    }

    void setupBoolCollection() {
        Collection *coll_bool;

        std::ifstream infile(std::string(ROOT_DIR)+"test/bool_documents.jsonl");
        std::vector<field> fields = {
                field("popular", field_types::BOOL, false),
                field("title", field_types::STRING, false),
                field("rating", field_types::FLOAT, false),
                field("bool_array", field_types::BOOL_ARRAY, false),
        };

        coll_bool = collectionManager.get_collection("coll_bool").get();
        if(coll_bool == nullptr) {
            coll_bool = collectionManager.create_collection("coll_bool", 1, fields).get();
        }

        std::string json_line;

        while (std::getline(infile, json_line)) {
            coll_bool->add(json_line);
        }

        infile.close();
    }

    void setupFiveHundredCollection() {
        auto schema_json =
                R"({
                "name": "FiveHundred",
                "fields": [
                    {"name": "title", "type": "string"}
                ]
            })"_json;

        auto collection_create_op = collectionManager.create_collection(schema_json);
        ASSERT_TRUE(collection_create_op.ok());

        auto products = collection_create_op.get();
        for (auto i = 0; i < 500; i++) {
            nlohmann::json json = {
                    {"title", "title_" + std::to_string(i)}
            };
            auto add_op = products->add(json.dump());
            if (!add_op.ok()) {
                LOG(INFO) << add_op.error();
            }
            ASSERT_TRUE(add_op.ok());
        }
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        EmbedderManager::get_instance().delete_all_text_embedders();
        delete store;
    }
};

TEST_F(UnionTest, ErrorHandling) {
    embedded_params = std::vector<nlohmann::json>(1, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "Products",
                        "q": "*"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(404, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("`Products` collection not found.", json_res["error"]);
    json_res.clear();
    req_params.clear();

    setupProductsCollection();

    searches = R"([
                    {
                        "collection": "Products",
                        "q": "foo"
                    }
                ])"_json;
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("No search fields specified for the query.", json_res["error"]);
    json_res.clear();
    req_params.clear();

    req_params = {
            {"page", "1"},
            {"per_page", "foo"}
    };
    searches = R"([
                    {
                        "collection": "Products",
                        "q": "*"
                    },
                    {
                        "collection": "Orders",
                        "q": "*"
                    }
                ])"_json;
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("Error while initializing global parameters of union: Parameter `per_page` must be an unsigned"
              " integer.", json_res["error"]);
    json_res.clear();
    req_params.clear();

    setupBoolCollectionWithDefaultSortingField();
    setupNumericArrayCollectionWithDefaultSortingField();

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name"
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("Expected type of `age` sort_by (int32_field) at search index `1` to be the same as the type of `rating` "
              "sort_by (float_field) at search index `0`. Both `coll_array_fields` and `coll_bool` collections have "
              "declared a default sorting field of different type. Since union expects the searches to sort_by on the "
              "same type of fields, default sorting fields of the collections should be removed.", json_res["error"]);
    json_res.clear();
    req_params.clear();

    collectionManager.drop_collection("coll_array_fields");
    setupNumericArrayCollection();

    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name"
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("Expected size of `sort_by` parameter of all searches to be equal. The first union search sorts on "
              "{`_text_match: text_match`, `rating: float_field`} but the search at index `1` sorts on "
              "{`_text_match: text_match`, `_union_search_index: union_query_order`, `_seq_id: insertion_order`}.",
              json_res["error"]);
    json_res.clear();
    req_params.clear();

    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "*",
                        "query_by": "title"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name",
                        "sort_by": "rating:desc"
                    }
                ])"_json;
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("Expected size of `sort_by` parameter of all searches to be equal. The first union search sorts on "
              "{`rating: float_field`, `_union_search_index: union_query_order`, `_seq_id: insertion_order`} "
              "but the search at index `1` sorts on {`rating: float_field`, `_text_match: text_match`}.", json_res["error"]);
    json_res.clear();
    req_params.clear();

    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title",
                        "sort_by": "popular:asc"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name",
                        "sort_by": "rating:desc"
                    }
                ])"_json;
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("Expected type of `rating` sort_by (float_field) at search index `1` to be the same as the type of "
              "`popular` sort_by (bool_field) at search index `0`.", json_res["error"]);
    json_res.clear();
    req_params.clear();

    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title",
                        "sort_by": "rating:asc"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name",
                        "sort_by": "rating:desc"
                    }
                ])"_json;
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("Expected order of `rating` sort_by (DESC) at search index `1` to be the same as the order of `rating` "
              "sort_by (ASC) at search index `0`.", json_res["error"]);
    json_res.clear();
    req_params.clear();
}

TEST_F(UnionTest, SameCollection) {
    setupProductsCollection();

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "Products",
                        "q": "soap",
                        "query_by": "product_name"
                    },
                    {
                        "collection": "Products",
                        "q": "shampoo",
                        "query_by": "product_name"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(2, json_res["found"].get<size_t>());
    ASSERT_EQ(2, json_res["out_of"]);
    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ(6, json_res["hits"][0]["document"].size());
    ASSERT_EQ(1, json_res["hits"][0]["document"].count("product_name"));
    ASSERT_EQ("soap", json_res["hits"][0]["document"]["product_name"]);

    ASSERT_EQ(6, json_res["hits"][1]["document"].size());
    ASSERT_EQ(1, json_res["hits"][1]["document"].count("product_name"));
    ASSERT_EQ("shampoo", json_res["hits"][1]["document"]["product_name"]);

    ASSERT_EQ(json_res["hits"][0]["text_match"], json_res["hits"][1]["text_match"]);
    json_res.clear();
    req_params.clear();

    searches = R"([
                    {
                        "collection": "Products",
                        "q": "soap",
                        "query_by": "product_name",
                        "exclude_fields": "embedding"
                    },
                    {
                        "collection": "Products",
                        "q": "shampoo",
                        "query_by": "product_name",
                        "include_fields": "product_name"
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(2, json_res["found"].get<size_t>());
    ASSERT_EQ(2, json_res["out_of"]);
    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ(5, json_res["hits"][0]["document"].size());
    ASSERT_EQ(1, json_res["hits"][0]["document"].count("product_name"));
    ASSERT_EQ("soap", json_res["hits"][0]["document"]["product_name"]);
    ASSERT_EQ(0, json_res["hits"][0]["document"].count("embedding"));

    ASSERT_EQ(1, json_res["hits"][1]["document"].size());
    ASSERT_EQ(1, json_res["hits"][1]["document"].count("product_name"));
    ASSERT_EQ("shampoo", json_res["hits"][1]["document"]["product_name"]);

    ASSERT_EQ(json_res["hits"][0]["text_match"], json_res["hits"][1]["text_match"]);
    json_res.clear();
    req_params.clear();

    searches = R"([
                    {
                        "collection": "Products",
                        "q": "so",
                        "query_by": "product_name",
                        "exclude_fields": "embedding"
                    },
                    {
                        "collection": "Products",
                        "q": "shampoo",
                        "query_by": "product_name",
                        "include_fields": "product_name"
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(2, json_res["found"].get<size_t>());
    ASSERT_EQ(2, json_res["out_of"]);
    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ(1, json_res.count("search_time_ms"));
    ASSERT_EQ(1, json_res.count("page"));
    ASSERT_EQ(1, json_res["hits"][0]["document"].size());
    ASSERT_EQ(1, json_res["hits"][0]["document"].count("product_name"));
    ASSERT_EQ("shampoo", json_res["hits"][0]["document"]["product_name"]);

    ASSERT_EQ(5, json_res["hits"][1]["document"].size());
    ASSERT_EQ(1, json_res["hits"][1]["document"].count("product_name"));
    ASSERT_EQ("soap", json_res["hits"][1]["document"]["product_name"]);
    ASSERT_EQ(0, json_res["hits"][1]["document"].count("embedding"));

    // Exact match gets better score.
    ASSERT_GT(json_res["hits"][0]["text_match"], json_res["hits"][1]["text_match"]);
    json_res.clear();
    req_params.clear();
}

TEST_F(UnionTest, DifferentCollections) {
    setupFoodsAndMealsCollection();

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "Meals",
                        "q": "he",
                        "query_by": "title",
                        "filter_by": "$UserFavoriteMeals(user_id: user_a) ",
                        "include_fields": "$Foods($Portions(*,strategy:merge)) "
                    },
                    {
                        "collection": "Foods",
                        "q": "bread",
                        "query_by": "name",
                        "filter_by": "$UserFavoriteFoods(user_id: user_a) ",
                        "include_fields": "$Portions(*,strategy:merge) "
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(2, json_res["found"].get<size_t>());
    ASSERT_EQ(2, json_res["hits"].size());

    ASSERT_EQ(1, json_res["hits"][0]["search_index"]);
    ASSERT_EQ(4, json_res["hits"][0]["document"].size());
    ASSERT_EQ(1, json_res["hits"][0]["document"].count("name"));
    ASSERT_EQ("Bread", json_res["hits"][0]["document"]["name"]);
    ASSERT_EQ(1, json_res["hits"][0]["document"].count("UserFavoriteFoods"));
    ASSERT_EQ(1, json_res["hits"][0]["document"].count("portions"));
    ASSERT_EQ(1, json_res["hits"][0]["document"]["portions"].size());
    ASSERT_EQ(1, json_res["hits"][0]["document"]["portions"][0].count("unit"));

    ASSERT_EQ(0, json_res["hits"][1]["search_index"]);
    ASSERT_EQ(6, json_res["hits"][1]["document"].size());
    ASSERT_EQ(1, json_res["hits"][1]["document"].count("title"));
    ASSERT_EQ("Heavy", json_res["hits"][1]["document"]["title"]);
    ASSERT_EQ(1, json_res["hits"][1]["document"].count("Foods"));
    ASSERT_EQ(2, json_res["hits"][1]["document"]["Foods"].size());

    ASSERT_EQ("Bread", json_res["hits"][1]["document"]["Foods"][0]["name"]);
    ASSERT_EQ(1, json_res["hits"][1]["document"]["Foods"][0].count("portions"));
    ASSERT_EQ(1, json_res["hits"][1]["document"]["Foods"][0]["portions"].size());
    ASSERT_EQ(1, json_res["hits"][1]["document"]["Foods"][0]["portions"][0].count("unit"));

    ASSERT_EQ("Milk", json_res["hits"][1]["document"]["Foods"][1]["name"]);
    ASSERT_EQ(1, json_res["hits"][1]["document"]["Foods"][1].count("portions"));
    ASSERT_EQ(3, json_res["hits"][1]["document"]["Foods"][1]["portions"].size());
    ASSERT_EQ(1, json_res["hits"][1]["document"]["Foods"][1]["portions"][0].count("unit"));
    ASSERT_EQ(0, json_res["hits"][1]["document"]["Foods"][1]["portions"][1].count("unit"));
    ASSERT_EQ(1, json_res["hits"][1]["document"]["Foods"][1]["portions"][2].count("unit"));
    json_res.clear();
    req_params.clear();

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "Meals",
                        "q": "*",
                        "filter_by": "$UserFavoriteMeals(user_id: user_a) ",
                        "include_fields": "$Foods($Portions(*,strategy:merge)) ",
                        "sort_by": "calories:desc"
                    },
                    {
                        "collection": "Foods",
                        "q": "*",
                        "filter_by": "$UserFavoriteFoods(user_id: user_a) && $Portions(id:*) ",
                        "include_fields": "$Portions(*,strategy:merge) ",
                        "sort_by": "$Portions(quantity:desc) "
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(2, json_res["found"].get<size_t>());
    ASSERT_EQ(2, json_res["hits"].size());

    ASSERT_EQ(0, json_res["hits"][0]["search_index"]);
    ASSERT_EQ(1, json_res["hits"][0]["document"].count("calories"));
    ASSERT_EQ(1500, json_res["hits"][0]["document"]["calories"]);

    ASSERT_EQ(1, json_res["hits"][1]["search_index"]);
    ASSERT_EQ(1, json_res["hits"][1]["document"].count("quantity"));
    ASSERT_EQ(500, json_res["hits"][1]["document"]["quantity"]);
    json_res.clear();
    req_params.clear();
}

TEST_F(UnionTest, Pagination) {
    setupNumericArrayCollection();
    setupBoolCollection();

    // Since no sort_by is mentioned, the documents are returned based on seq_id (insertion order).
    // search   seq_id
    //    0        9
    //    0        4
    //    0        3
    //    0        2
    //    0        1
    //    1        4
    //    1        3
    //    1        2
    //    1        1
    //    1        0
    req_params = {
            {"page", "1"},
            {"per_page", "2"}
    };
    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"]); // 5 documents from `coll_array_fields` and 5 documents from `coll_bool`.
    ASSERT_EQ(15, json_res["out_of"]);
    ASSERT_EQ(1, json_res["page"]);
    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ(0, json_res["hits"][0]["search_index"]);
    ASSERT_EQ("coll_bool", json_res["hits"][0]["collection"]);
    ASSERT_EQ("9", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("The Legend of the Titanic", json_res["hits"][0]["document"]["title"]);
    ASSERT_EQ(578730123365189753, json_res["hits"][0]["text_match"]);

    ASSERT_EQ(0, json_res["hits"][1]["search_index"]);
    ASSERT_EQ("coll_bool", json_res["hits"][1]["collection"]);
    ASSERT_EQ("4", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("The Wizard of Oz", json_res["hits"][1]["document"]["title"]);
    ASSERT_EQ(578730123365189753, json_res["hits"][1]["text_match"]);

    ASSERT_EQ(5, json_res["union_request_params"][0]["found"]);
    ASSERT_EQ("coll_bool", json_res["union_request_params"][0]["collection_name"]);
    ASSERT_EQ(5, json_res["union_request_params"][1]["found"]);
    ASSERT_EQ("coll_array_fields", json_res["union_request_params"][1]["collection_name"]);
    json_res.clear();
    req_params.clear();

    req_params = {
            {"page", "3"},
            {"per_page", "2"}
    };
    // Pagination parameters of individual searches should have no effect.
    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title",
                        "page": 10,
                        "per_page": 10
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name"
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"]); // 5 documents from `coll_array_fields` and 5 documents from `coll_bool`.
    ASSERT_EQ(15, json_res["out_of"]);
    ASSERT_EQ(3, json_res["page"]);
    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ("coll_bool", json_res["hits"][0]["collection"]);
    ASSERT_EQ("1", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("The Godfather", json_res["hits"][0]["document"]["title"]);
    ASSERT_EQ(578730123365189753, json_res["hits"][0]["text_match"]);

    ASSERT_EQ("coll_array_fields", json_res["hits"][1]["collection"]);
    ASSERT_EQ("4", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][1]["document"]["name"]);
    ASSERT_EQ(578730123365189753, json_res["hits"][1]["text_match"]);

    ASSERT_EQ(2, json_res["union_request_params"][0]["per_page"]);
    ASSERT_EQ("coll_bool", json_res["union_request_params"][0]["collection_name"]);
    ASSERT_EQ(2, json_res["union_request_params"][1]["per_page"]);
    ASSERT_EQ("coll_array_fields", json_res["union_request_params"][1]["collection_name"]);
    json_res.clear();
    req_params.clear();

    req_params = {
            {"page", "4"},
            {"per_page", "2"}
    };
    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name"
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"]); // 5 documents from `coll_array_fields` and 5 documents from `coll_bool`.
    ASSERT_EQ(15, json_res["out_of"]);
    ASSERT_EQ(4, json_res["page"]);
    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ("coll_array_fields", json_res["hits"][0]["collection"]);
    ASSERT_EQ("3", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][0]["document"]["name"]);
    ASSERT_EQ(578730123365189753, json_res["hits"][0]["text_match"]);

    ASSERT_EQ("coll_array_fields", json_res["hits"][1]["collection"]);
    ASSERT_EQ("2", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][1]["document"]["name"]);
    ASSERT_EQ(578730123365189753, json_res["hits"][1]["text_match"]);
    json_res.clear();
    req_params.clear();

    setupFiveHundredCollection();

    req_params = {
            {"page", "4"},
            {"per_page", "100"}
    };
    searches = R"([
                    {
                        "collection": "FiveHundred",
                        "q": "*"
                    }
                ])"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(500, json_res["found"]);
    ASSERT_EQ(500, json_res["out_of"]);
    ASSERT_EQ(4, json_res["page"]);
    ASSERT_EQ(100, json_res["hits"].size());
    json_res.clear();
    req_params.clear();
}

TEST_F(UnionTest, Sorting) {
    setupNumericArrayCollection();
    setupBoolCollection();

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title",
                        "sort_by": "rating:desc"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name",
                        "sort_by": "rating:desc"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"]); // 5 documents from `coll_array_fields` and 5 documents from `coll_bool`.
    ASSERT_EQ(15, json_res["out_of"]);
    ASSERT_EQ(10, json_res["hits"].size());
    ASSERT_EQ("1", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][0]["document"]["name"]);
    ASSERT_EQ(9.999, json_res["hits"][0]["document"]["rating"]);

    ASSERT_EQ("1", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("The Godfather", json_res["hits"][1]["document"]["title"]);
    ASSERT_EQ(9.9, json_res["hits"][1]["document"]["rating"]);

    ASSERT_EQ("3", json_res["hits"][2]["document"]["id"]);
    ASSERT_EQ("The Schindler's List", json_res["hits"][2]["document"]["title"]);
    ASSERT_EQ(9.8, json_res["hits"][2]["document"]["rating"]);

    ASSERT_EQ("4", json_res["hits"][3]["document"]["id"]);
    ASSERT_EQ("The Wizard of Oz", json_res["hits"][3]["document"]["title"]);
    ASSERT_EQ(8.9, json_res["hits"][3]["document"]["rating"]);

    ASSERT_EQ("2", json_res["hits"][4]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][4]["document"]["name"]);
    ASSERT_EQ(7.812, json_res["hits"][4]["document"]["rating"]);

    ASSERT_EQ("4", json_res["hits"][5]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][5]["document"]["name"]);
    ASSERT_EQ(5.5, json_res["hits"][5]["document"]["rating"]);

    ASSERT_EQ("9", json_res["hits"][6]["document"]["id"]);
    ASSERT_EQ("The Legend of the Titanic", json_res["hits"][6]["document"]["title"]);
    ASSERT_EQ(2, json_res["hits"][6]["document"]["rating"]);

    ASSERT_EQ("2", json_res["hits"][7]["document"]["id"]);
    ASSERT_EQ("Daniel the Wizard", json_res["hits"][7]["document"]["title"]);
    ASSERT_EQ(1.6, json_res["hits"][7]["document"]["rating"]);

    ASSERT_EQ("0", json_res["hits"][8]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][8]["document"]["name"]);
    ASSERT_EQ(1.09, json_res["hits"][8]["document"]["rating"]);

    ASSERT_EQ("3", json_res["hits"][9]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][9]["document"]["name"]);
    ASSERT_EQ(0, json_res["hits"][9]["document"]["rating"]);
    json_res.clear();
    req_params.clear();

    searches = R"([
                    {
                        "collection": "coll_bool",
                        "q": "the",
                        "query_by": "title",
                        "sort_by": "rating:asc"
                    },
                    {
                        "collection": "coll_array_fields",
                        "q": "Jeremy",
                        "query_by": "name",
                        "sort_by": "rating:asc"
                    }
                ])"_json;
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"]); // 5 documents from `coll_array_fields` and 5 documents from `coll_bool`.
    ASSERT_EQ(15, json_res["out_of"]);
    ASSERT_EQ(10, json_res["hits"].size());
    ASSERT_EQ("3", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][0]["document"]["name"]);
    ASSERT_EQ(0, json_res["hits"][0]["document"]["rating"]);

    ASSERT_EQ("0", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][1]["document"]["name"]);
    ASSERT_EQ(1.09, json_res["hits"][1]["document"]["rating"]);

    ASSERT_EQ("2", json_res["hits"][2]["document"]["id"]);
    ASSERT_EQ("Daniel the Wizard", json_res["hits"][2]["document"]["title"]);
    ASSERT_EQ(1.6, json_res["hits"][2]["document"]["rating"]);

    ASSERT_EQ("9", json_res["hits"][3]["document"]["id"]);
    ASSERT_EQ("The Legend of the Titanic", json_res["hits"][3]["document"]["title"]);
    ASSERT_EQ(2, json_res["hits"][3]["document"]["rating"]);

    ASSERT_EQ("4", json_res["hits"][4]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][4]["document"]["name"]);
    ASSERT_EQ(5.5, json_res["hits"][4]["document"]["rating"]);

    ASSERT_EQ("2", json_res["hits"][5]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][5]["document"]["name"]);
    ASSERT_EQ(7.812, json_res["hits"][5]["document"]["rating"]);

    ASSERT_EQ("4", json_res["hits"][6]["document"]["id"]);
    ASSERT_EQ("The Wizard of Oz", json_res["hits"][6]["document"]["title"]);
    ASSERT_EQ(8.9, json_res["hits"][6]["document"]["rating"]);

    ASSERT_EQ("3", json_res["hits"][7]["document"]["id"]);
    ASSERT_EQ("The Schindler's List", json_res["hits"][7]["document"]["title"]);
    ASSERT_EQ(9.8, json_res["hits"][7]["document"]["rating"]);

    ASSERT_EQ("1", json_res["hits"][8]["document"]["id"]);
    ASSERT_EQ("The Godfather", json_res["hits"][8]["document"]["title"]);
    ASSERT_EQ(9.9, json_res["hits"][8]["document"]["rating"]);

    ASSERT_EQ("1", json_res["hits"][9]["document"]["id"]);
    ASSERT_EQ("Jeremy Howard", json_res["hits"][9]["document"]["name"]);
    ASSERT_EQ(9.999, json_res["hits"][9]["document"]["rating"]);
    json_res.clear();
    req_params.clear();
}

TEST_F(UnionTest, PinnedHits) {
    auto schema_json =
            R"({
                "name": "Cars",
                "fields": [
                    {"name": "name", "type": "string"}
                ]
            })"_json;

    auto schema_json2 =
            R"({
                "name": "Watches",
                "fields": [
                    {"name": "name", "type": "string"}
                ]
            })"_json;

    std::vector<nlohmann::json> documents = {
            R"({
                "name": "Black McLaren"
            })"_json,
            R"({
                "name": "Black Lamborghini"
            })"_json,
            R"({
                "name": "Black Buggati"
            })"_json,
            R"({
                "name": "Black Rolex"
            })"_json,
            R"({
                "name": "Black Tissot"
            })"_json,
            R"({
                "name": "Black Rado"
            })"_json
    };

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();
    for (auto i = 0; i < 3; ++i) {
        const auto& json = documents[i];
        auto add_op = coll->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    collection_create_op = collectionManager.create_collection(schema_json2);
    ASSERT_TRUE(collection_create_op.ok());

    coll = collection_create_op.get();
    for (auto i = 3; i < 6; ++i) {
        const auto& json = documents[i];
        auto add_op = coll->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    req_params = {{"pinned_hits", "1:1"}};
    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());

    searches = R"([
                    {
                        "collection": "Cars",
                        "q": "black",
                        "query_by": "name"
                    },
                    {
                        "collection": "Watches",
                        "q": "black",
                        "query_by": "name"
                    }
                ])"_json;
    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(6, json_res["found"]);
    ASSERT_EQ(6, json_res["out_of"]);
    ASSERT_EQ(6, json_res["hits"].size());
    ASSERT_EQ("1", json_res["hits"][0]["document"]["id"]); //any one id will be pinned incase of same ids across multiple collections
    ASSERT_EQ("2", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("0", json_res["hits"][2]["document"]["id"]);
    ASSERT_EQ("2", json_res["hits"][3]["document"]["id"]);
    ASSERT_EQ("0", json_res["hits"][4]["document"]["id"]);
    ASSERT_EQ("1", json_res["hits"][5]["document"]["id"]);

    //with different id across collections
    auto schema_json3 =
            R"({
                "name": "Cars2",
                "fields": [
                    {"name": "name", "type": "string"}
                ]
            })"_json;

    auto schema_json4 =
            R"({
                "name": "Watches2",
                "fields": [
                    {"name": "name", "type": "string"}
                ]
            })"_json;

    documents = {
            R"({
                "id": "C0",
                "name": "Black McLaren"
            })"_json,
            R"({
                "id": "C1",
                "name": "Black Lamborghini"
            })"_json,
            R"({
                "id": "C2",
                "name": "Black Buggati"
            })"_json,
            R"({
                "id": "W0",
                "name": "Black Rolex"
            })"_json,
            R"({
                "id": "W1",
                "name": "Black Tissot"
            })"_json,
            R"({
                "id": "W2",
                "name": "Black Rado"
            })"_json
    };

    collection_create_op = collectionManager.create_collection(schema_json3);
    ASSERT_TRUE(collection_create_op.ok());

    coll = collection_create_op.get();
    for (auto i = 0; i < 3; ++i) {
        const auto& json = documents[i];
        auto add_op = coll->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    collection_create_op = collectionManager.create_collection(schema_json4);
    ASSERT_TRUE(collection_create_op.ok());

    coll = collection_create_op.get();
    for (auto i = 3; i < 6; ++i) {
        const auto& json = documents[i];
        auto add_op = coll->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    req_params = {{"pinned_hits", "C1:1"}};

    searches = R"([
                    {
                        "collection": "Cars2",
                        "q": "black",
                        "query_by": "name"
                    },
                    {
                        "collection": "Watches2",
                        "q": "black",
                        "query_by": "name"
                    }
                ])"_json;
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(6, json_res["found"]);
    ASSERT_EQ(6, json_res["out_of"]);
    ASSERT_EQ(6, json_res["hits"].size());
    ASSERT_EQ("C1", json_res["hits"][0]["document"]["id"]);  //with unique ids, given ids will be pinned
    ASSERT_EQ("C2", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("C0", json_res["hits"][2]["document"]["id"]);
    ASSERT_EQ("W2", json_res["hits"][3]["document"]["id"]);
    ASSERT_EQ("W1", json_res["hits"][4]["document"]["id"]);
    ASSERT_EQ("W0", json_res["hits"][5]["document"]["id"]);
}

TEST_F(UnionTest, CurationIncludesShouldNotCollapseInUnion) {
    auto schema_json =
            R"({
                "name": "Events",
                "fields": [
                    {"name": "title", "type": "string"}
                ]
            })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll = collection_create_op.get();

    ASSERT_TRUE(coll->add(R"({"id":"0","title":"2026 NCAA Tournament Winner"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"1","title":"2026 Women's NCAA Tournament Winner"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"2","title":"March Madness Sweet 16"})").ok());

    auto& curation_manager = CurationIndexManager::get_instance();
    curation_manager.init_store(store);
    auto upsert_set = nlohmann::json::array({
        nlohmann::json{
            {"id", "march-madness"},
            {"rule", {{"query", "march madness"}, {"match", curation_t::MATCH_CONTAINS}}},
            {"includes", nlohmann::json::array({
                nlohmann::json{{"id", "0"}, {"position", 1}},
                nlohmann::json{{"id", "1"}, {"position", 2}}
            })}
        }
    });
    ASSERT_TRUE(curation_manager.upsert_curation_set("events_curations", upsert_set).ok());
    ASSERT_TRUE(coll->set_curation_sets({"events_curations"}).ok());

    req_params = {};
    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "Events",
                        "q": "march madness",
                        "query_by": "title"
                    },
                    {
                        "collection": "Events",
                        "q": "march madness",
                        "query_by": "title"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());

    ASSERT_EQ(3, json_res["found"].get<size_t>());
    ASSERT_EQ(3, json_res["hits"].size());
    ASSERT_TRUE(json_res["hits"][0]["curated"].get<bool>());
    ASSERT_TRUE(json_res["hits"][1]["curated"].get<bool>());
    ASSERT_EQ("0", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("1", json_res["hits"][1]["document"]["id"]);
}

TEST_F(UnionTest, RemoveDuplicatesShouldDeduplicateAcrossCuratedAndRawUnionHits) {
    auto schema_json =
            R"({
                "name": "Events",
                "fields": [
                    {"name": "title", "type": "string"}
                ]
            })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll = collection_create_op.get();

    ASSERT_TRUE(coll->add(R"({"id":"0","title":"march madness winner"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"1","title":"regular season recap"})").ok());

    auto& curation_manager = CurationIndexManager::get_instance();
    curation_manager.init_store(store);
    auto upsert_set = nlohmann::json::array({
        nlohmann::json{
            {"id", "march-madness"},
            {"rule", {{"query", "march madness"}, {"match", curation_t::MATCH_EXACT}}},
            {"includes", nlohmann::json::array({
                nlohmann::json{{"id", "0"}, {"position", 1}}
            })}
        }
    });
    ASSERT_TRUE(curation_manager.upsert_curation_set("events_curations", upsert_set).ok());
    ASSERT_TRUE(coll->set_curation_sets({"events_curations"}).ok());

    req_params = {{"remove_duplicates", "true"}};
    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "Events",
                        "q": "march madness",
                        "query_by": "title"
                    },
                    {
                        "collection": "Events",
                        "q": "winner",
                        "query_by": "title"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());

    ASSERT_EQ(1, json_res["found"].get<size_t>());
    ASSERT_EQ(1, json_res["hits"].size());
    ASSERT_EQ("0", json_res["hits"][0]["document"]["id"]);
}

TEST_F(UnionTest, RemoveDuplicatesShouldNotLeakCuratedRawDuplicateToLaterPages) {
    auto schema_json =
            R"({
                "name": "Events",
                "fields": [
                    {"name": "title", "type": "string"}
                ]
            })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll = collection_create_op.get();

    ASSERT_TRUE(coll->add(R"({"id":"0","title":"march madness winner"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"1","title":"april recap"})").ok());

    auto& curation_manager = CurationIndexManager::get_instance();
    curation_manager.init_store(store);
    auto upsert_set = nlohmann::json::array({
        nlohmann::json{
            {"id", "march-madness"},
            {"rule", {{"query", "march madness"}, {"match", curation_t::MATCH_EXACT}}},
            {"includes", nlohmann::json::array({
                nlohmann::json{{"id", "0"}, {"position", 1}}
            })}
        }
    });
    ASSERT_TRUE(curation_manager.upsert_curation_set("events_curations", upsert_set).ok());
    ASSERT_TRUE(coll->set_curation_sets({"events_curations"}).ok());

    req_params = {
        {"remove_duplicates", "true"},
        {"per_page", "1"},
        {"page", "1"}
    };
    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "Events",
                        "q": "march madness",
                        "query_by": "title"
                    },
                    {
                        "collection": "Events",
                        "q": "winner",
                        "query_by": "title"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res["found"].get<size_t>());
    ASSERT_EQ(1, json_res["hits"].size());
    ASSERT_EQ("0", json_res["hits"][0]["document"]["id"]);

    req_params["page"] = "2";
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res["found"].get<size_t>());
    ASSERT_TRUE(json_res["hits"].empty());
}

TEST_F(UnionTest, HybridSearchHasVectorDistance) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vec",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "ts/e5-small"
                    }
                }
            }
        ]
    })"_json;

    auto schema2 = schema;
    schema2["name"] = "coll2";
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto collection_create_op2 = collectionManager.create_collection(schema2);
    ASSERT_TRUE(collection_create_op2.ok());

    // index docs
    nlohmann::json doc1 = R"({"name": "hello" })"_json;
    auto coll1 = collection_create_op.get();
    auto add_op1 = coll1->add(doc1.dump());
    ASSERT_TRUE(add_op1.ok());

    nlohmann::json doc2 = R"({"name": "world" })"_json;
    auto coll2 = collection_create_op2.get();
    auto add_op2 = coll2->add(doc2.dump());
    ASSERT_TRUE(add_op2.ok());

    // Do union search with hybrid search
    req_params = {{"q", "hello"}};
    auto embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    
    auto searches = R"([
        {
            "collection": "coll1",
            "query_by": "name, vec"
        },
        {
            "collection": "coll2",
            "query_by": "name, vec"
        }
    ])"_json;
    nlohmann::json json_res;
    
    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());

    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ("coll1", json_res["hits"][0]["collection"]);
    ASSERT_EQ("coll2", json_res["hits"][1]["collection"]);
    ASSERT_TRUE(json_res["hits"][0].contains("vector_distance"));
    ASSERT_TRUE(json_res["hits"][1].contains("vector_distance"));
}

TEST_F(UnionTest, RemoveDuplicatesWithUnion) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string"}
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();

    nlohmann::json doc = R"({"name": "anti dandruff shampoo" })"_json;
    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    doc = R"({"name": "sliky hair shampoo" })"_json;
    add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    req_params = {{"remove_duplicates", "true"}};
    auto embedded_params = std::vector<nlohmann::json>(4, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "coll1",
                        "q": "shampoo",
                        "query_by": "name"
                    },
                    {
                        "collection": "coll1",
                        "q": "dandruff",
                        "query_by": "name"
                    },
                    {
                        "collection": "coll1",
                        "q": "silky",
                        "query_by": "name"
                    },
                    {
                        "collection": "coll1",
                        "q": "hair",
                        "query_by": "name"
                    }
                ])"_json;

    //default to remove duplicates
    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(2, json_res["found"].get<size_t>());
    ASSERT_EQ(2, json_res["hits"].size());
    ASSERT_EQ("1", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("0", json_res["hits"][1]["document"]["id"]);

    //should explicitly set to false if not intending to remove duplicates
    req_params = {{"remove_duplicates", "false"}};
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts, false);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(5, json_res["found"].get<size_t>());
    ASSERT_EQ(5, json_res["hits"].size());
    ASSERT_EQ("1", json_res["hits"][0]["document"]["id"]);
    ASSERT_EQ("0", json_res["hits"][1]["document"]["id"]);
    ASSERT_EQ("0", json_res["hits"][2]["document"]["id"]);
    ASSERT_EQ("1", json_res["hits"][3]["document"]["id"]);
    ASSERT_EQ("1", json_res["hits"][4]["document"]["id"]);
}

TEST_F(UnionTest, GroupingWithUnions) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "category", "type": "string", "facet": true},
            {"name": "fieldId", "type": "int32"}
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();

    nlohmann::json doc;
    doc["name"] = "Head & Shoulders";
    doc["category"] = "Shampoo";
    doc["fieldId"] = 0;
    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    doc["name"] = "Dove";
    doc["category"] = "Shampoo";
    doc["fieldId"] = 1;
    add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    doc["name"] = "Heads Up";
    doc["category"] = "Shampoo";
    doc["fieldId"] = 2;
    add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "coll1",
                        "q": "head",
                        "query_by": "name",
                        "group_by": "category"
                    },
                    {
                        "collection": "coll1",
                        "q": "do",
                        "query_by": "name",
                        "group_by": "category"
                    }
                ])"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(2, json_res["found"].get<size_t>());
    ASSERT_EQ(2, json_res["grouped_hits"].size());

    ASSERT_EQ(2, json_res["grouped_hits"][0]["found"].get<size_t>());
    ASSERT_EQ("Shampoo", json_res["grouped_hits"][0]["group_key"][0]);
    ASSERT_EQ("0", json_res["grouped_hits"][0]["hits"][0]["document"]["id"]);

    ASSERT_EQ(1, json_res["grouped_hits"][1]["found"].get<size_t>());
    ASSERT_EQ("Shampoo", json_res["grouped_hits"][1]["group_key"][0]);
    ASSERT_EQ("1", json_res["grouped_hits"][1]["hits"][0]["document"]["id"]);


    //uneven searches
    searches = R"([
                    {
                        "collection": "coll1",
                        "q": "heads",
                        "query_by": "name"
                    },
                    {
                        "collection": "coll1",
                        "q": "dov",
                        "query_by": "name",
                        "group_by": "category"
                    }
                ])"_json;

    req_params.clear();
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("Invalid group_by searches count. All searches with union search should be uniform.", json_res["error"]);
}

TEST_F(UnionTest, UnionRemoveDuplicatesFoundCountShouldBePageInvariant) {
    auto schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string"}
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();

    for(uint32_t i = 0; i < 300; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["name"] = "ghost monster item " + std::to_string(i);
        auto add_op = coll1->add(doc.dump());
        ASSERT_TRUE(add_op.ok());
    }

    auto embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    searches = R"([
                    {
                        "collection": "coll1",
                        "q": "ghost",
                        "query_by": "name"
                    },
                    {
                        "collection": "coll1",
                        "q": "monster",
                        "query_by": "name"
                    }
                ])"_json;

    req_params = {
        {"remove_duplicates", "true"},
        {"per_page", "10"},
        {"page", "1"}
    };

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(300, json_res["found"].get<size_t>());

    req_params["page"] = "2";
    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(300, json_res["found"].get<size_t>());
}

TEST_F(UnionTest, FacetingWithUnion) {
    auto schema_json_countries =
            R"({
                "name": "Countries",
                "fields": [
                    {"name": "country_id", "type": "string"},
                    {"name": "country_name", "type": "string", "facet": true}
                ]
            })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json_countries);
    ASSERT_TRUE(collection_create_op.ok());

    std::vector<nlohmann::json> countries = {
            R"({
                "country_id": "ENG",
                "country_name": "England"
            })"_json,
            R"({
                "country_id": "ITA",
                "country_name": "Italy"
            })"_json,
            R"({
                "country_id": "USA",
                "country_name": "United States"
            })"_json,
            R"({
                "country_id": "GER",
                "country_name": "Germany"
            })"_json,
            R"({
                "country_id": "SUI",
                "country_name": "Switzerland"
            })"_json,
            R"({
                "country_id": "FRA",
                "country_name": "France"
            })"_json
    };

    auto coll_countries = collection_create_op.get();
    for (auto i = 0; i < 6; ++i) {
        const auto& json = countries[i];
        auto add_op = coll_countries->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    auto schema_json =
            R"({
                "name": "Cars",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "country", "type": "string", "facet": true},
                    {"name": "rating", "type": "float", "facet": true},
                    {"name": "country_id", "type": "string", "facet": true, "reference": "Countries.country_id"}
                ]
            })"_json;

    auto schema_json2 =
            R"({
                "name": "Watches",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "country", "type": "string", "facet": true},
                    {"name": "rating", "type": "float", "facet":true},
                    {"name": "country_id", "type": "string", "facet": true, "reference": "Countries.country_id"}
                ]
            })"_json;

    std::vector<nlohmann::json> documents = {
            R"({
                "name": "McLaren",
                "country" : "England",
                "country_id": "ENG",
                "rating": 4.4
            })"_json,
            R"({
                "name": "Lamborghini",
                "country" : "Italy",
                "country_id": "ITA",
                "rating": 4.7
            })"_json,
            R"({
                "name": "Ford",
                "country" : "United States",
                "country_id": "USA",
                "rating": 4.1
            })"_json,
            R"({
                "name": "BMW",
                "country" : "Germany",
                "country_id": "GER",
                "rating": 4.8
            })"_json,
            R"({
                "name": "Audi",
                "country" : "Germany",
                "country_id": "GER",
                "rating": 4.5
            })"_json,
            R"({
                "name": "Rado",
                "country" : "Switzerland",
                "country_id": "SUI",
                "rating": 4.2
            })"_json,
            R"({
                "name": "Tissot",
                "country" : "Switzerland",
                "country_id": "SUI",
                "rating": 4.8
            })"_json,
            R"({
                "name": "Cartier",
                "country" : "France",
                "country_id": "FRA",
                "rating": 4.1
            })"_json,
            R"({
                "name": "Panerai",
                "country" : "Italy",
                "country_id": "ITA",
                "rating": 4.4
            })"_json,
            R"({
                "name": "A. Lange & Sohne",
                "country" : "Germany",
                "country_id": "GER",
                "rating": 4.7
            })"_json
    };

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();
    for (auto i = 0; i < 5; ++i) {
        const auto& json = documents[i];
        auto add_op = coll->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    collection_create_op = collectionManager.create_collection(schema_json2);
    ASSERT_TRUE(collection_create_op.ok());

    coll = collection_create_op.get();
    for (auto i = 5; i < 10; ++i) {
        const auto& json = documents[i];
        auto add_op = coll->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());

    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country"
                    }
                ])OVR"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"].get<size_t>());
    ASSERT_EQ(10, json_res["hits"].size());

    ASSERT_EQ(1, json_res["facet_counts"].size());
    ASSERT_EQ("country", json_res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(6, json_res["facet_counts"][0]["counts"].size());
    ASSERT_EQ(6, json_res["facet_counts"][0]["stats"]["total_values"]);

    ASSERT_EQ("Germany", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(3, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("Switzerland", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("Italy", json_res["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("United States", json_res["facet_counts"][0]["counts"][3]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("France", json_res["facet_counts"][0]["counts"][4]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("England", json_res["facet_counts"][0]["counts"][5]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][5]["count"].get<size_t>());



    //multple facet fields
    req_params.clear();
    json_res.clear();

    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country, rating"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country, rating"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"].get<size_t>());
    ASSERT_EQ(10, json_res["hits"].size());
    ASSERT_EQ(2, json_res["facet_counts"].size());

    ASSERT_EQ("rating", json_res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(6, json_res["facet_counts"][0]["counts"].size());
    ASSERT_EQ(6, json_res["facet_counts"][0]["stats"]["total_values"]);
    ASSERT_EQ("4.8", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("4.7", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("4.4", json_res["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("4.1", json_res["facet_counts"][0]["counts"][3]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("4.5", json_res["facet_counts"][0]["counts"][4]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("4.2", json_res["facet_counts"][0]["counts"][5]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][5]["count"].get<size_t>());

    ASSERT_EQ("country", json_res["facet_counts"][1]["field_name"]);
    ASSERT_EQ(6, json_res["facet_counts"][1]["counts"].size());
    ASSERT_EQ(6, json_res["facet_counts"][1]["stats"]["total_values"]);
    ASSERT_EQ("Germany", json_res["facet_counts"][1]["counts"][0]["value"]);
    ASSERT_EQ(3, json_res["facet_counts"][1]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("Switzerland", json_res["facet_counts"][1]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][1]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("Italy", json_res["facet_counts"][1]["counts"][2]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][1]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("United States", json_res["facet_counts"][1]["counts"][3]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][1]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("France", json_res["facet_counts"][1]["counts"][4]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][1]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("England", json_res["facet_counts"][1]["counts"][5]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][1]["counts"][5]["count"].get<size_t>());

    //range facets
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "rating(great:[4, 4.5], exceptional:[4.5, 5])"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "rating(great:[4, 4.5], exceptional:[4.5, 5])"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"].get<size_t>());
    ASSERT_EQ(10, json_res["hits"].size());

    ASSERT_EQ(1, json_res["facet_counts"].size());
    ASSERT_EQ("rating", json_res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"].size());
    ASSERT_EQ(2, json_res["facet_counts"][0]["stats"]["total_values"]);

    ASSERT_EQ("great", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(5, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("exceptional", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(5, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());

    //facet sorting by alpha asc
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:asc)"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:asc)"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res["facet_counts"].size());
    ASSERT_EQ("country", json_res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(6, json_res["facet_counts"][0]["counts"].size());
    ASSERT_EQ(6, json_res["facet_counts"][0]["stats"]["total_values"]);

    ASSERT_EQ("England", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("France", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("Germany", json_res["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(3, json_res["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("Italy", json_res["facet_counts"][0]["counts"][3]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("Switzerland", json_res["facet_counts"][0]["counts"][4]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("United States", json_res["facet_counts"][0]["counts"][5]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][5]["count"].get<size_t>());

    //facet sorting by alpha desc
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:desc)"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:desc)"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res["facet_counts"].size());
    ASSERT_EQ("country", json_res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(6, json_res["facet_counts"][0]["counts"].size());
    ASSERT_EQ(6, json_res["facet_counts"][0]["stats"]["total_values"]);

    ASSERT_EQ("United States", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("Switzerland", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("Italy", json_res["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("Germany", json_res["facet_counts"][0]["counts"][3]["value"]);
    ASSERT_EQ(3, json_res["facet_counts"][0]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("France", json_res["facet_counts"][0]["counts"][4]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("England", json_res["facet_counts"][0]["counts"][5]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][5]["count"].get<size_t>());

    // facet with reference - join on faceted fields and get response
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "filter_by": "$Countries(id:*)",
                        "facet_by": "$Countries(country_name)"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "filter_by": "$Countries(id:*)",
                        "facet_by": "$Countries(country_name)"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"].get<size_t>());
    ASSERT_EQ(10, json_res["hits"].size());

    ASSERT_EQ(1, json_res["facet_counts"].size());
    ASSERT_EQ("$Countries(country_name)", json_res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(6, json_res["facet_counts"][0]["counts"].size());
    ASSERT_EQ(6, json_res["facet_counts"][0]["stats"]["total_values"]);

    ASSERT_EQ("Italy", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("Germany", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("United States", json_res["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("Switzerland", json_res["facet_counts"][0]["counts"][3]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("France", json_res["facet_counts"][0]["counts"][4]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("England", json_res["facet_counts"][0]["counts"][5]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][5]["count"].get<size_t>());
}

TEST_F(UnionTest, FacetingWithUnionsValidation) {
    auto schema_json =
            R"({
                "name": "Cars",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "country", "type": "string", "facet": true},
                    {"name": "rating", "type": "float", "facet": true},
                    {"name" : "country_id", "type": "string", "reference": "Countries.country_id"},
                    {"name" : "region_id", "type": "string", "reference": "Region.region_id"}
                ]
            })"_json;

    auto schema_json2 =
            R"({
                "name": "Watches",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "country", "type": "string", "facet": true},
                    {"name": "rating", "type": "float", "facet":true},
                    {"name" : "country_id", "type": "string", "reference": "Countries.country_id"}
                ]
            })"_json;

    auto schema_json3 =
            R"({
                "name": "Countries",
                "fields": [
                    {"name": "country_id", "type": "string"},
                    {"name": "name", "type": "string", "facet": true}
                ]
            })"_json;

    auto schema_json4 =
            R"({
                "name": "Region",
                "fields": [
                    {"name": "region_id", "type": "string", "facet": true},
                    {"name": "name", "type": "string", "facet": true}
                ]
            })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    collection_create_op = collectionManager.create_collection(schema_json2);
    ASSERT_TRUE(collection_create_op.ok());

    collection_create_op = collectionManager.create_collection(schema_json3);
    ASSERT_TRUE(collection_create_op.ok());

    collection_create_op = collectionManager.create_collection(schema_json4);
    ASSERT_TRUE(collection_create_op.ok());

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    //facet query should be uniform across all faceted searches
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country",
                        "facet_query" : "country: Switz"
                    }
                ])OVR"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("`facet_query` should be uniform across searches for faceting with union search.", json_res["error"]);

    // facet startegy should be uniform
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country",
                        "facet_strategy": "exhaustive"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country",
                        "facet_strategy": "top_values"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("`facet_strategy` should be uniform across searches for faceting with union search.", json_res["error"]);

    // facet field should be uniform
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "rating",
                        "facet_strategy": "top_values"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "rating(great:[4, 4.5], exceptional:[4.5, 5])",
                        "facet_strategy": "top_values"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("facet fields should be uniform across searches for faceting with union search.", json_res["error"]);

    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "rating(average:[4, 4.5], best:[4.5, 5])",
                        "facet_strategy": "top_values"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "rating(great:[4, 4.5], exceptional:[4.5, 5])",
                        "facet_strategy": "top_values"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("facet fields should be uniform across searches for faceting with union search.", json_res["error"]);

    // facet return parent should be consistent across searches
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "rating, country",
                        "facet_strategy": "top_values",
                        "facet_return_parent": "country"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "rating, country",
                        "facet_strategy": "top_values",
                        "facet_return_parent": "country, rating"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("`facet_return_parent` should be uniform across searches for faceting with union search.", json_res["error"]);

    // if facet fields are different then it's alright
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country",
                        "facet_strategy": "top_values"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "rating(great:[4, 4.5], exceptional:[4.5, 5])",
                        "facet_strategy": "top_values"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(0, json_res.count("code"));
    ASSERT_EQ(0, json_res.count("error"));

    //reference facets fails if not sharing common joined collection
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "filter_by": "$Region(id:*)",
                        "facet_by": "$Region(region_id)"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "filter_by": "$Region(id:*)",
                        "facet_by": "$Region(region_id)"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(1, json_res.count("error"));

    //fields different sort params
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:desc)"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:asc)"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(1, json_res.count("error"));
}

TEST_F(UnionTest, UnionHighlightingUAFRaceASAN) {
  nlohmann::json schema = R"({
        "name": "union_uaf_race",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "rank", "type": "int32"}
        ]
    })"_json;

  auto create_op = collectionManager.create_collection(schema);
  ASSERT_TRUE(create_op.ok());
  auto coll = create_op.get();

  constexpr size_t hot_docs = 8;
  constexpr size_t iterations = 80;

  const std::string volatile_token = "uafsentinelzzzz";
  std::stringstream dense_ss;
  for(size_t i = 0; i < 64; i++) {
    if(i != 0) {
      dense_ss << " ";
    }
    dense_ss << volatile_token;
  }
  const std::string dense_token_phrase = dense_ss.str();

  auto build_hot_doc = [&](size_t i, bool dense_doc0) {
    nlohmann::json doc;
    doc["id"] = "hot_" + std::to_string(i);
    doc["title"] = (dense_doc0 && i == 0) ? dense_token_phrase : volatile_token;
    doc["rank"] = int32_t(i + 1);
    return doc;
  };

  for (size_t i = 0; i < hot_docs; i++) {
    ASSERT_TRUE(coll->add(build_hot_doc(i, true).dump(), UPSERT).ok());
  }

  std::atomic<size_t> union_calls = 0;
  std::atomic<size_t> mutation_batches = 0;
  std::atomic<size_t> total_mutations = 0;
  std::atomic<size_t> total_mutations_during_search = 0;
  std::atomic<size_t> union_hit_count = 0;
  std::atomic<size_t> union_nonempty_highlight_count = 0;
  std::atomic<size_t> union_title_highlight_count = 0;

  for (size_t i = 0; i < iterations; i++) {
    // Start each round from low token-offset density.
    for (size_t j = 0; j < hot_docs; j++) {
      ASSERT_TRUE(coll->add(build_hot_doc(j, false).dump(), UPSERT).ok());
    }

    std::map<std::string, std::string> local_req_params = {
        {"page", "1"},
        {"per_page", std::to_string(hot_docs)}
    };
    std::vector<nlohmann::json> local_embedded_params(2, nlohmann::json::object());
    nlohmann::json local_searches = R"([
            {
                "collection": "union_uaf_race",
                "q": "uafsentinelzzzz",
                "query_by": "title",
                "highlight_fields": "title",
                "sort_by": "rank:desc"
            },
            {
                "collection": "union_uaf_race",
                "q": "missing_token_never_indexed",
                "query_by": "title",
                "highlight_fields": "title",
                "sort_by": "rank:desc"
            }
        ])"_json;
    auto req_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::system_clock::now().time_since_epoch()).count();

    std::atomic<bool> search_done = false;
    std::atomic<bool> search_ok = false;
    auto thread_req_params = local_req_params;
    auto thread_embedded_params = local_embedded_params;
    auto thread_searches = local_searches;
    auto search_thread = std::thread([&, thread_req_params, thread_embedded_params, thread_searches, req_ts]() mutable {
      nlohmann::json thread_res;
      auto op = collectionManager.do_union(thread_req_params, thread_embedded_params, thread_searches, thread_res, req_ts);
      if(op.ok() && thread_res.contains("hits") && thread_res["hits"].is_array()) {
        size_t local_hit_count = thread_res["hits"].size();
        size_t local_nonempty_highlight_count = 0;
        size_t local_title_highlight_count = 0;

        for(const auto& hit: thread_res["hits"]) {
          if(!hit.contains("highlight") || !hit["highlight"].is_object()) {
            continue;
          }

          const auto& highlight_obj = hit["highlight"];
          if(!highlight_obj.empty()) {
            local_nonempty_highlight_count++;
          }

          auto title_it = highlight_obj.find("title");
          if(title_it == highlight_obj.end()) {
            continue;
          }

          if(title_it->is_object()) {
            bool has_snippet = title_it->contains("snippet") && (*title_it)["snippet"].is_string() &&
                               !(*title_it)["snippet"].get<std::string>().empty();
            bool has_matched_tokens = title_it->contains("matched_tokens") &&
                                      (*title_it)["matched_tokens"].is_array() &&
                                      !(*title_it)["matched_tokens"].empty();
            if(has_snippet || has_matched_tokens) {
              local_title_highlight_count++;
            }
          } else if(title_it->is_array() && !title_it->empty()) {
            local_title_highlight_count++;
          }
        }

        union_hit_count.fetch_add(local_hit_count, std::memory_order_relaxed);
        union_nonempty_highlight_count.fetch_add(local_nonempty_highlight_count, std::memory_order_relaxed);
        union_title_highlight_count.fetch_add(local_title_highlight_count, std::memory_order_relaxed);
      }
      search_ok.store(op.ok(), std::memory_order_relaxed);
      search_done.store(true, std::memory_order_release);
    });

    // Give do_union a head-start to enter run_search/process_highlight_fields_with_lock.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    size_t local_mutations = 0;
    size_t local_mutations_during_search = 0;
    bool dense_doc0 = false;
    constexpr size_t max_mutations_while_searching = 512;
    for(size_t mutation_attempt = 0; mutation_attempt < max_mutations_while_searching; mutation_attempt++) {
      if(search_done.load(std::memory_order_acquire)) {
        break;
      }

      dense_doc0 = !dense_doc0;
      const bool search_running_before_add = !search_done.load(std::memory_order_acquire);
      ASSERT_TRUE(coll->add(build_hot_doc(0, dense_doc0).dump(), UPSERT).ok());
      local_mutations++;
      const bool search_running_after_add = !search_done.load(std::memory_order_acquire);
      if(search_running_before_add && search_running_after_add) {
        local_mutations_during_search++;
      }

      if(mutation_attempt % 16 == 15) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      } else {
        std::this_thread::yield();
      }
    }

    if(!search_done.load(std::memory_order_acquire)) {
      const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
      while(!search_done.load(std::memory_order_acquire) &&
             std::chrono::steady_clock::now() < wait_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    if(!search_done.load(std::memory_order_acquire)) {
      search_thread.detach();
      FAIL() << "Timed out waiting for union search to complete after pausing mutations. "
             << "local_mutations=" << local_mutations
             << ", local_mutations_during_search=" << local_mutations_during_search;
    }

    search_thread.join();
    ASSERT_TRUE(search_ok.load(std::memory_order_relaxed));

    // Restore compact low-offset form for next iteration.
    ASSERT_TRUE(coll->add(build_hot_doc(0, false).dump(), UPSERT).ok());
    local_mutations++;

    if (local_mutations > 0) {
      mutation_batches.fetch_add(1, std::memory_order_relaxed);
      total_mutations.fetch_add(local_mutations, std::memory_order_relaxed);
    }
    if(local_mutations_during_search > 0) {
      total_mutations_during_search.fetch_add(local_mutations_during_search, std::memory_order_relaxed);
    }
    union_calls.fetch_add(1, std::memory_order_relaxed);
  }

  ASSERT_GT(union_calls.load(std::memory_order_relaxed), 0);
  ASSERT_GT(mutation_batches.load(std::memory_order_relaxed), 0);
  ASSERT_GT(total_mutations.load(std::memory_order_relaxed), 0);
  ASSERT_GT(total_mutations_during_search.load(std::memory_order_relaxed), 0);
  ASSERT_GT(union_hit_count.load(std::memory_order_relaxed), 0);
  ASSERT_GT(union_nonempty_highlight_count.load(std::memory_order_relaxed), 0);
  ASSERT_GT(union_title_highlight_count.load(std::memory_order_relaxed), 0);
}

TEST_F(UnionTest, DynamicFacetMinOccurrenceRatioShouldApplyAfterUnionMerge) {
    auto schema_json =
            R"({
                "name": "UnionDynamicFacetsA",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "brand", "type": "string", "facet": true}
                ]
            })"_json;

    auto schema_json2 =
            R"({
                "name": "UnionDynamicFacetsB",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "brand", "type": "string", "facet": true}
                ]
            })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll = collection_create_op.get();

    ASSERT_TRUE(coll->add(R"({"id":"1","name":"A1","brand":"shared"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"2","name":"A2","brand":"shared"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"3","name":"A3","brand":"shared"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"4","name":"A4","brand":"left_only_1"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"5","name":"A5","brand":"left_only_2"})").ok());

    collection_create_op = collectionManager.create_collection(schema_json2);
    ASSERT_TRUE(collection_create_op.ok());
    coll = collection_create_op.get();

    ASSERT_TRUE(coll->add(R"({"id":"1","name":"B1","brand":"shared"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"2","name":"B2","brand":"shared"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"3","name":"B3","brand":"shared"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"4","name":"B4","brand":"right_only_1"})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"5","name":"B5","brand":"right_only_2"})").ok());

    embedded_params = std::vector<nlohmann::json>(2, nlohmann::json::object());
    req_params.clear();
    json_res.clear();

    searches = R"OVR([
                    {
                        "collection": "UnionDynamicFacetsA",
                        "q": "*",
                        "facet_by": "*",
                        "facet_min_occurrence_ratio": 0.5
                    },
                    {
                        "collection": "UnionDynamicFacetsB",
                        "q": "*",
                        "facet_by": "*",
                        "facet_min_occurrence_ratio": 0.5
                    }
                ])OVR"_json;

    auto search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(10, json_res["found"].get<size_t>());

    // "shared" is 3/10 in each individual search and would be filtered too early by the buggy code,
    // but it is 6/10 after union merge and should therefore be returned.
    ASSERT_EQ(1, json_res["facet_counts"].size());
    ASSERT_EQ("brand", json_res["facet_counts"][0]["field_name"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"].size());
    ASSERT_EQ("shared", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(6, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
}