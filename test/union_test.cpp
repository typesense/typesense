#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>

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

TEST_F(UnionTest, FacetingWithUnion) {
    auto schema_json =
            R"({
                "name": "Cars",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "country", "type": "string", "facet": true},
                    {"name": "rating", "type": "float", "facet": true}
                ]
            })"_json;

    auto schema_json2 =
            R"({
                "name": "Watches",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "country", "type": "string", "facet": true},
                    {"name": "rating", "type": "float", "facet":true}
                ]
            })"_json;

    std::vector<nlohmann::json> documents = {
            R"({
                "name": "McLaren",
                "country" : "England",
                "rating": 4.4
            })"_json,
            R"({
                "name": "Lamborghini",
                "country" : "Italy",
                "rating": 4.7
            })"_json,
            R"({
                "name": "Ford",
                "country" : "United States",
                "rating": 4.1
            })"_json,
            R"({
                "name": "BMW",
                "country" : "Germany",
                "rating": 4.8
            })"_json,
            R"({
                "name": "Audi",
                "country" : "Germany",
                "rating": 4.5
            })"_json,
            R"({
                "name": "Rado",
                "country" : "Switzerland",
                "rating": 4.2
            })"_json,
            R"({
                "name": "Tissot",
                "country" : "Switzerland",
                "rating": 4.8
            })"_json,
            R"({
                "name": "Cartier",
                "country" : "France",
                "rating": 4.1
            })"_json,
            R"({
                "name": "Panerai",
                "country" : "Italy",
                "rating": 4.4
            })"_json,
            R"({
                "name": "A. Lange & Sohne",
                "country" : "Germany",
                "rating": 4.7
            })"_json
    };

    auto collection_create_op = collectionManager.create_collection(schema_json);
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

    ASSERT_EQ("France", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("Switzerland", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("United States", json_res["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("England", json_res["facet_counts"][0]["counts"][3]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("Italy", json_res["facet_counts"][0]["counts"][4]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("Germany", json_res["facet_counts"][0]["counts"][5]["value"]);
    ASSERT_EQ(3, json_res["facet_counts"][0]["counts"][5]["count"].get<size_t>());

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
    ASSERT_EQ("4.2", json_res["facet_counts"][0]["counts"][0]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("4.1", json_res["facet_counts"][0]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("4.4", json_res["facet_counts"][0]["counts"][2]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("4.7", json_res["facet_counts"][0]["counts"][3]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("4.5", json_res["facet_counts"][0]["counts"][4]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][0]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("4.8", json_res["facet_counts"][0]["counts"][5]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][0]["counts"][5]["count"].get<size_t>());

    ASSERT_EQ("country", json_res["facet_counts"][1]["field_name"]);
    ASSERT_EQ(6, json_res["facet_counts"][1]["counts"].size());
    ASSERT_EQ(6, json_res["facet_counts"][1]["stats"]["total_values"]);
    ASSERT_EQ("France", json_res["facet_counts"][1]["counts"][0]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][1]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("Switzerland", json_res["facet_counts"][1]["counts"][1]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][1]["counts"][1]["count"].get<size_t>());
    ASSERT_EQ("United States", json_res["facet_counts"][1]["counts"][2]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][1]["counts"][2]["count"].get<size_t>());
    ASSERT_EQ("England", json_res["facet_counts"][1]["counts"][3]["value"]);
    ASSERT_EQ(1, json_res["facet_counts"][1]["counts"][3]["count"].get<size_t>());
    ASSERT_EQ("Italy", json_res["facet_counts"][1]["counts"][4]["value"]);
    ASSERT_EQ(2, json_res["facet_counts"][1]["counts"][4]["count"].get<size_t>());
    ASSERT_EQ("Germany", json_res["facet_counts"][1]["counts"][5]["value"]);
    ASSERT_EQ(3, json_res["facet_counts"][1]["counts"][5]["count"].get<size_t>());

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
}

TEST_F(UnionTest, FacetingWithUnionsValidation) {
    auto schema_json =
            R"({
                "name": "Cars",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "country", "type": "string", "facet": true},
                    {"name": "rating", "type": "float", "facet": true},
                    {"name" : "country_id", "type": "string", "reference": "Countries.country_id"}
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

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    collection_create_op = collectionManager.create_collection(schema_json2);
    ASSERT_TRUE(collection_create_op.ok());

    collection_create_op = collectionManager.create_collection(schema_json3);
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

    //sort_by is not supported with union search
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:desc)",
                        "facet_strategy": "top_values",
                        "facet_return_parent": "country"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country(sort_by:_alpha:desc)",
                        "facet_strategy": "top_values",
                        "facet_return_parent": "country, rating"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("`sort_by` is not supported for union search faceting.", json_res["error"]);

    //facet referencing is not supported with union search
    req_params.clear();
    json_res.clear();
    searches = R"OVR([
                    {
                        "collection": "Cars",
                        "q": "*",
                        "filter_by": "$Countries(id:= *)",
                        "facet_by": "$Countries(name)"
                    },
                    {
                        "collection": "Watches",
                        "q": "*",
                        "facet_by": "country"
                    }
                ])OVR"_json;

    search_op = collectionManager.do_union(req_params, embedded_params, searches, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    ASSERT_EQ(1, json_res.count("code"));
    ASSERT_EQ(400, json_res["code"]);
    ASSERT_EQ(1, json_res.count("error"));
    ASSERT_EQ("facet referencing is not supported for union search faceting.", json_res["error"]);

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
}