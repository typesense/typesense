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
                    {"name": "product_id", "type": "string", "reference": "Products.product_id"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"}
                ]
            })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto collection = collection_create_op.get();
    auto schema = collection->get_schema();

    ASSERT_EQ(schema.at("customer_name").reference, "");
    ASSERT_EQ(schema.at("product_id").reference, "Products.product_id");

    // Index a `foo_sequence_id` field for `foo` reference field.
    ASSERT_EQ(schema.count("product_id_sequence_id"), 1);
    ASSERT_TRUE(schema.at("product_id_sequence_id").index);

    collectionManager.drop_collection("Customers");
}

TEST_F(CollectionJoinTest, IndexReferenceField) {
    auto products_schema_json =
            R"({
                "name": "Products",
                "fields": [
                    {"name": "product_id", "type": "string", "index": false, "optional": true},
                    {"name": "product_name", "type": "string"},
                    {"name": "product_description", "type": "string"}
                ]
            })"_json;
    auto collection_create_op = collectionManager.create_collection(products_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "product_id", "type": "string", "reference": "foo"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    nlohmann::json customer_json = R"({
                                        "customer_id": "customer_a",
                                        "customer_name": "Joe",
                                        "product_price": 143,
                                        "product_id": "a"
                                    })"_json;

    auto customer_collection = collection_create_op.get();
    auto add_op = customer_collection->add(customer_json.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Invalid reference `foo`.", add_op.error());
    collectionManager.drop_collection("Customers");

    customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "product_id", "type": "string", "reference": "products.product_id"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    customer_collection = collection_create_op.get();
    add_op = customer_collection->add(customer_json.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Referenced collection `products` not found.", add_op.error());
    collectionManager.drop_collection("Customers");

    customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "product_id", "type": "string", "reference": "Products.id"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    customer_collection = collection_create_op.get();
    add_op = customer_collection->add(customer_json.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Referenced field `id` not found in the collection `Products`.", add_op.error());
    collectionManager.drop_collection("Customers");

    customers_schema_json =
            R"({
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "product_price", "type": "float"},
                    {"name": "product_id", "type": "string", "reference": "Products.product_id"}
                ]
            })"_json;
    collection_create_op = collectionManager.create_collection(customers_schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    customer_collection = collection_create_op.get();
    add_op = customer_collection->add(customer_json.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Referenced field `product_id` in the collection `Products` must be indexed.", add_op.error());

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

    add_op = customer_collection->add(customer_json.dump());
    ASSERT_EQ("Referenced document having `product_id` = `a` not found in the collection `Products`.", add_op.error());

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

    customer_json["product_id"] = "product_a";
    add_op = customer_collection->add(customer_json.dump());
    ASSERT_EQ("Multiple documents having `product_id` = `product_a` found in the collection `Products`.", add_op.error());

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

    add_op = customer_collection->add(customer_json.dump());
    ASSERT_TRUE(add_op.ok());

    auto result = customer_collection->search("*", {"customer_id"}, "", {}, {}, {0}).get();
    ASSERT_EQ(result["hits"][0]["document"].count("product_id"), 1);
//    ASSERT_EQ(result["hits"][0]["document"].count("product_id_sequence_id"), 0);

    collectionManager.drop_collection("Customers");
    collectionManager.drop_collection("Products");
}