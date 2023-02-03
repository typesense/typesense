#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionJoinTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_join";
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

TEST_F(CollectionJoinTest, SchemaReferenceField) {
    nlohmann::json schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "product_.*", "type": "string", "reference": "Products.product_id"}
                ]
            })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Wildcard field cannot have a reference.", collection_create_op.error());

    schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": ".*", "type": "auto", "reference": "Products.product_id"}
                ]
            })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Field `.*` cannot be a reference field.", collection_create_op.error());

    schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "product_id", "type": "string", "reference": 123},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"}
                ]
            })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Reference should be a string.", collection_create_op.error());

    schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "product_id", "type": "string", "reference": "foo"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"}
                ]
            })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Invalid reference `foo`.", collection_create_op.error());

    schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "product_id", "type": "string", "reference": "Products.product_id"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto collection = collection_create_op.get();
    auto schema = collection->get_schema();

    ASSERT_EQ(schema.count("customer_name"), 1);
    ASSERT_TRUE(schema.at("customer_name").reference.empty());
    ASSERT_EQ(schema.count("product_id"), 1);
    ASSERT_FALSE(schema.at("product_id").reference.empty());

    auto reference_fields = collection->get_reference_fields();
    ASSERT_EQ(reference_fields.count("product_id"), 1);
    ASSERT_EQ(reference_fields.at("product_id").collection, "Products");
    ASSERT_EQ(reference_fields.at("product_id").field, "product_id");

    // Add a `foo_sequence_id` field in the schema for `foo` reference field.
    ASSERT_EQ(schema.count("product_id_sequence_id"), 1);
    ASSERT_TRUE(schema.at("product_id_sequence_id").index);

    collectionManager.drop_collection("Customers");
}

TEST_F(CollectionJoinTest, IndexDocumentHavingReferenceField) {
    auto customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "reference_id", "type": "string", "reference": "products.product_id"}
                ]
            })"_json;
    auto collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto customer_collection = collection_create_op.get();

    nlohmann::json customer_json = R"({
                                        "customer_id": "customer_a",
                                        "customer_name": "Joe",
                                        "product_price": 143,
                                        "product_id": "a"
                                    })"_json;
    auto add_doc_op = customer_collection->add(customer_json.dump());

    ASSERT_FALSE(add_doc_op.ok());
    ASSERT_EQ("Missing the required reference field `reference_id` in the document.", add_doc_op.error());

    customer_json = R"({
                        "customer_id": "customer_a",
                        "customer_name": "Joe",
                        "product_price": 143,
                        "reference_id": "a"
                    })"_json;
    add_doc_op = customer_collection->add(customer_json.dump());
    ASSERT_FALSE(add_doc_op.ok());
    ASSERT_EQ("Referenced collection `products` not found.", add_doc_op.error());
    collectionManager.drop_collection("Customers");

    customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "reference_id", "type": "string", "reference": "Products.id"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    customer_collection = collection_create_op.get();

    auto products_schema_json =
            R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string", "index": false, "optional": true},
                    {"name": "product_name", "type": "string"},
                    {"name": "product_description", "type": "string"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(products_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    add_doc_op = customer_collection->add(customer_json.dump());
    ASSERT_FALSE(add_doc_op.ok());
    ASSERT_EQ("Referenced field `id` not found in the collection `Products`.", add_doc_op.error());
    collectionManager.drop_collection("Customers");

    customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "reference_id", "type": "string", "reference": "Products.product_id"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    customer_collection = collection_create_op.get();
    add_doc_op = customer_collection->add(customer_json.dump());
    ASSERT_FALSE(add_doc_op.ok());
    ASSERT_EQ("Referenced field `product_id` in the collection `Products` must be indexed.", add_doc_op.error());

    collectionManager.drop_collection("Products");
    products_schema_json =
            R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "product_name", "type": "string"},
                    {"name": "product_description", "type": "string"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(products_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    add_doc_op = customer_collection->add(customer_json.dump());
    ASSERT_EQ("Referenced document having `product_id: a` not found in the collection `Products`.", add_doc_op.error());

    std::vector<nlohmann::json> products = {
            R"({
                "product_id": "product_a",
                "product_name": "shampoo",
                "product_description": "Our new moisturizing shampoo is perfect for those with dry or damaged hair."
            })"_json,
            R"({
                "product_id": "product_a",
                "product_name": "soap",
                "product_description": "Introducing our all-natural, organic soap bar made with essential oils and botanical ingredients."
            })"_json
    };
    for (auto const &json: products){
        auto add_op = collection_create_op.get()->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    customer_json["reference_id"] = "product_a";
    add_doc_op = customer_collection->add(customer_json.dump());
    ASSERT_EQ("Multiple documents having `product_id: product_a` found in the collection `Products`.", add_doc_op.error());

    collectionManager.drop_collection("Products");
    products[1]["product_id"] = "product_b";
    products_schema_json =
            R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "product_name", "type": "string"},
                    {"name": "product_description", "type": "string"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(products_schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    for (auto const &json: products){
        auto add_op = collection_create_op.get()->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    collectionManager.drop_collection("Customers");
    customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "reference_id", "type": "string", "reference": "Products.product_id"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    customer_collection = collection_create_op.get();
    add_doc_op = customer_collection->add(customer_json.dump());
    ASSERT_TRUE(add_doc_op.ok());
    ASSERT_EQ(customer_collection->get("0").get().count("reference_id_sequence_id"), 1);

    nlohmann::json document;
    // Referenced document's sequence_id must be valid.
    auto get_op = collectionManager.get_collection("Products")->get_document_from_store(
            customer_collection->get("0").get()["reference_id_sequence_id"].get<uint32_t>(),
                    document);
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(document.count("product_id"), 1);
    ASSERT_EQ(document["product_id"], "product_a");
    ASSERT_EQ(document["product_name"], "shampoo");

    collectionManager.drop_collection("Customers");
    collectionManager.drop_collection("Products");
}

TEST_F(CollectionJoinTest, FilterByReferenceField_SingleMatch) {
    auto schema_json =
            R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "product_name", "type": "string"},
                    {"name": "product_description", "type": "string"}
                ]
            })"_json;
    std::vector<nlohmann::json> documents = {
            R"({
                "product_id": "product_a",
                "product_name": "shampoo",
                "product_description": "Our new moisturizing shampoo is perfect for those with dry or damaged hair."
            })"_json,
            R"({
                "product_id": "product_b",
                "product_name": "soap",
                "product_description": "Introducing our all-natural, organic soap bar made with essential oils and botanical ingredients."
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
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "product_id", "type": "string", "reference": "Products.product_id"}
                ]
            })"_json;
    documents = {
            R"({
                "customer_id": "customer_a",
                "customer_name": "Joe",
                "product_price": 143,
                "product_id": "product_a"
            })"_json,
            R"({
                "customer_id": "customer_a",
                "customer_name": "Joe",
                "product_price": 73.5,
                "product_id": "product_b"
            })"_json,
            R"({
                "customer_id": "customer_b",
                "customer_name": "Dan",
                "product_price": 75,
                "product_id": "product_a"
            })"_json,
            R"({
                "customer_id": "customer_b",
                "customer_name": "Dan",
                "product_price": 140,
                "product_id": "product_b"
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

    auto coll = collectionManager.get_collection("Products");
    auto search_op = coll->search("s", {"product_name"}, "$foo:=customer_a", {}, {}, {0},
                               10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ(search_op.error(), "Could not parse the reference filter.");

    search_op = coll->search("s", {"product_name"}, "$foo(:=customer_a", {}, {}, {0},
                                  10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ(search_op.error(), "Could not parse the reference filter.");

    search_op = coll->search("s", {"product_name"}, "$foo(:=customer_a)", {}, {}, {0},
                                  10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ(search_op.error(), "Referenced collection `foo` not found.");

    search_op = coll->search("s", {"product_name"}, "$Customers(foo:=customer_a)", {}, {}, {0},
                             10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ(search_op.error(), "Failed to parse reference filter on `Customers` collection: Could not find a filter field named `foo` in the schema.");

    auto result = coll->search("s", {"product_name"}, "$Customers(customer_id:=customer_a && product_price:<100)", {}, {}, {0},
                             10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD).get();

    ASSERT_EQ(1, result["found"].get<size_t>());
    ASSERT_EQ(1, result["hits"].size());
    ASSERT_EQ("soap", result["hits"][0]["document"]["product_name"].get<std::string>());

//    collectionManager.drop_collection("Customers");
//    collectionManager.drop_collection("Products");
}

TEST_F(CollectionJoinTest, FilterByReferenceField_MultipleMatch) {
    auto schema_json =
            R"({
                "name": "Users",
                "fields": [
                    {"name": "user_id", "type": "string"},
                    {"name": "user_name", "type": "string"}
                ]
            })"_json;
    std::vector<nlohmann::json> documents = {
            R"({
                "user_id": "user_a",
                "user_name": "Roshan"
            })"_json,
            R"({
                "user_id": "user_b",
                "user_name": "Ruby"
            })"_json,
            R"({
                "user_id": "user_c",
                "user_name": "Joe"
            })"_json,
            R"({
                "user_id": "user_d",
                "user_name": "Aby"
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
                "name": "Repos",
                "fields": [
                    {"name": "repo_id", "type": "string"},
                    {"name": "repo_content", "type": "string"}
                ]
            })"_json;
    documents = {
            R"({
                "repo_id": "repo_a",
                "repo_content": "body1"
            })"_json,
            R"({
                "repo_id": "repo_b",
                "repo_content": "body2"
            })"_json,
            R"({
                "repo_id": "repo_c",
                "repo_content": "body3"
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
                "name": "Links",
                "fields": [
                    {"name": "repo_id", "type": "string", "reference": "Repos.repo_id"},
                    {"name": "user_id", "type": "string", "reference": "Users.user_id"}
                ]
            })"_json;
    documents = {
            R"({
                "repo_id": "repo_a",
                "user_id": "user_b"
            })"_json,
            R"({
                "repo_id": "repo_a",
                "user_id": "user_c"
            })"_json,
            R"({
                "repo_id": "repo_b",
                "user_id": "user_a"
            })"_json,
            R"({
                "repo_id": "repo_b",
                "user_id": "user_b"
            })"_json,
            R"({
                "repo_id": "repo_b",
                "user_id": "user_d"
            })"_json,
            R"({
                "repo_id": "repo_c",
                "user_id": "user_a"
            })"_json,
            R"({
                "repo_id": "repo_c",
                "user_id": "user_b"
            })"_json,
            R"({
                "repo_id": "repo_c",
                "user_id": "user_c"
            })"_json,
            R"({
                "repo_id": "repo_c",
                "user_id": "user_d"
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

    auto coll = collectionManager.get_collection("Users");

    // Search for users linked to repo_b
    auto result = coll->search("R", {"user_name"}, "$Links(repo_id:=repo_b)", {}, {}, {0},
                               10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD).get();

    ASSERT_EQ(2, result["found"].get<size_t>());
    ASSERT_EQ(2, result["hits"].size());
    ASSERT_EQ("user_b", result["hits"][0]["document"]["user_id"].get<std::string>());
    ASSERT_EQ("user_a", result["hits"][1]["document"]["user_id"].get<std::string>());

//    collectionManager.drop_collection("Users");
//    collectionManager.drop_collection("Repos");
//    collectionManager.drop_collection("Links");
}