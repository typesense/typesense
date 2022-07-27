#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionNestedFieldsTest : public ::testing::Test {
protected:
    Store* store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector <std::string> query_fields;
    std::vector <sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_nested";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf " + state_dir_path + " && mkdir -p " + state_dir_path).c_str());

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

TEST_F(CollectionNestedFieldsTest, FlattenJSONObject) {
    auto json_str = R"({
        "company": {"name": "nike"},
        "employees": { "num": 1200 },
        "locations": [
            { "pincode": 100, "country": "USA",
              "address": { "street": "One Bowerman Drive", "city": "Beaverton", "products": ["shoes", "tshirts"] }
            },
            { "pincode": 200, "country": "Canada",
              "address": { "street": "175 Commerce Valley", "city": "Thornhill", "products": ["sneakers", "shoes"] }
            }
        ]}
    )";

    std::vector<field> nested_fields = {
        field("locations", field_types::OBJECT_ARRAY, false)
    };

    // array of objects
    std::vector<field> flattened_fields;
    nlohmann::json doc = nlohmann::json::parse(json_str);
    ASSERT_TRUE(field::flatten_doc(doc, nested_fields, flattened_fields).ok());
    ASSERT_EQ(5, flattened_fields.size());

    for(const auto& f: flattened_fields) {
        ASSERT_TRUE(f.is_array());
    }

    auto expected_json = R"(
        {
            ".flat": ["locations.address.city","locations.address.products","locations.address.street",
                      "locations.country","locations.pincode"],
            "company":{"name":"nike"},
            "employees":{"num":1200},
            "locations":[
                {"address":{"city":"Beaverton","products":["shoes","tshirts"],
                "street":"One Bowerman Drive"},"country":"USA","pincode":100},

                {"address":{"city":"Thornhill","products":["sneakers","shoes"],
                "street":"175 Commerce Valley"},"country":"Canada","pincode":200}
            ],

            "locations.address.city":["Beaverton","Thornhill"],
            "locations.address.products":["shoes","tshirts","sneakers","shoes"],
            "locations.address.street":["One Bowerman Drive","175 Commerce Valley"],
            "locations.country":["USA","Canada"],
            "locations.pincode":[100,200]
        }
    )";

    ASSERT_EQ(doc.dump(), nlohmann::json::parse(expected_json).dump());

    // plain object
    flattened_fields.clear();
    doc = nlohmann::json::parse(json_str);
    nested_fields = {
        field("company", field_types::OBJECT, false)
    };

    ASSERT_TRUE(field::flatten_doc(doc, nested_fields, flattened_fields).ok());

    expected_json = R"(
        {
          ".flat": ["company.name"],
          "company":{"name":"nike"},
          "company.name":"nike",
          "employees":{"num":1200},
          "company.name":"nike",
          "locations":[
                {"address":{"city":"Beaverton","products":["shoes","tshirts"],
                 "street":"One Bowerman Drive"},"country":"USA","pincode":100},
                {"address":{"city":"Thornhill","products":["sneakers","shoes"],"street":"175 Commerce Valley"},
                 "country":"Canada","pincode":200}
          ]
        }
    )";

    ASSERT_EQ(doc.dump(), nlohmann::json::parse(expected_json).dump());

    // plain object inside an array
    flattened_fields.clear();
    doc = nlohmann::json::parse(json_str);
    nested_fields = {
        field("locations.address", field_types::OBJECT, false)
    };

    ASSERT_FALSE(field::flatten_doc(doc, nested_fields, flattened_fields).ok()); // must be of type object_array

    nested_fields = {
        field("locations.address", field_types::OBJECT_ARRAY, false)
    };

    flattened_fields.clear();
    ASSERT_TRUE(field::flatten_doc(doc, nested_fields, flattened_fields).ok());

    expected_json = R"(
        {
          ".flat": ["locations.address.city", "locations.address.products", "locations.address.street"],
          "company":{"name":"nike"},
          "employees":{"num":1200},
          "locations":[
                {"address":{"city":"Beaverton","products":["shoes","tshirts"],
                 "street":"One Bowerman Drive"},"country":"USA","pincode":100},
                {"address":{"city":"Thornhill","products":["sneakers","shoes"],"street":"175 Commerce Valley"},
                 "country":"Canada","pincode":200}
          ],
          "locations.address.city":["Beaverton","Thornhill"],
          "locations.address.products":["shoes","tshirts","sneakers","shoes"],
          "locations.address.street":["One Bowerman Drive","175 Commerce Valley"]
        }
    )";

    ASSERT_EQ(doc.dump(), nlohmann::json::parse(expected_json).dump());

    // primitive inside nested object
    flattened_fields.clear();
    doc = nlohmann::json::parse(json_str);
    nested_fields = {
        field("company.name", field_types::STRING, false)
    };

    ASSERT_TRUE(field::flatten_doc(doc, nested_fields, flattened_fields).ok());

    expected_json = R"(
        {
          ".flat": ["company.name"],
          "company":{"name":"nike"},
          "company.name":"nike",
          "employees":{"num":1200},
          "locations":[
                {"address":{"city":"Beaverton","products":["shoes","tshirts"],
                 "street":"One Bowerman Drive"},"country":"USA","pincode":100},
                {"address":{"city":"Thornhill","products":["sneakers","shoes"],"street":"175 Commerce Valley"},
                 "country":"Canada","pincode":200}
          ]
        }
    )";

    ASSERT_EQ(doc.dump(), nlohmann::json::parse(expected_json).dump());
}

TEST_F(CollectionNestedFieldsTest, TestNestedArrayField) {
    auto json_str = R"({
        "company": {"name": "nike"},
        "employees": {
            "num": 1200,
            "detail": {
                "num_tags": 2,
                "tags": ["plumber", "electrician"]
            },
            "details": [{
                "num_tags": 2,
                "tags": ["plumber", "electrician"]
            }]
        },
        "locations": [
            { "pincode": 100, "country": "USA",
              "address": { "street": "One Bowerman Drive", "city": "Beaverton", "products": ["shoes", "tshirts"] }
            },
            { "pincode": 200, "country": "Canada",
              "address": { "street": "175 Commerce Valley", "city": "Thornhill", "products": ["sneakers", "shoes"] }
            }
        ]}
    )";

    std::vector<field> nested_fields = {
        field("locations", field_types::OBJECT_ARRAY, false)
    };

    // array of objects
    std::vector<field> flattened_fields;
    nlohmann::json doc = nlohmann::json::parse(json_str);
    ASSERT_TRUE(field::flatten_doc(doc, nested_fields, flattened_fields).ok());
    ASSERT_EQ(5, flattened_fields.size());

    for(const auto& f: flattened_fields) {
        ASSERT_TRUE(f.is_array());
        ASSERT_TRUE(f.nested_array);
    }

    flattened_fields.clear();

    // test against whole object

    nested_fields = {
        field("employees", field_types::OBJECT, false)
    };

    ASSERT_TRUE(field::flatten_doc(doc, nested_fields, flattened_fields).ok());
    ASSERT_EQ(5, flattened_fields.size());

    for(const auto& f: flattened_fields) {
        if(StringUtils::begins_with(f.name, "employees.details")) {
            ASSERT_TRUE(f.nested_array);
        } else {
            ASSERT_FALSE(f.nested_array);
        }
    }

    // test against deep paths
    flattened_fields.clear();
    nested_fields = {
        field("employees.details.num_tags", field_types::INT32_ARRAY, false),
        field("employees.details.tags", field_types::STRING_ARRAY, false),
        field("employees.detail.tags", field_types::STRING_ARRAY, false),
    };

    ASSERT_TRUE(field::flatten_doc(doc, nested_fields, flattened_fields).ok());
    ASSERT_EQ(3, flattened_fields.size());

    ASSERT_EQ("employees.detail.tags",flattened_fields[0].name);
    ASSERT_FALSE(flattened_fields[0].nested_array);

    ASSERT_EQ("employees.details.num_tags",flattened_fields[1].name);
    ASSERT_TRUE(flattened_fields[1].nested_array);

    ASSERT_EQ("employees.details.tags",flattened_fields[2].name);
    ASSERT_TRUE(flattened_fields[2].nested_array);
}

TEST_F(CollectionNestedFieldsTest, FlattenJSONObjectHandleErrors) {
    auto json_str = R"({
        "company": {"name": "nike"},
        "employees": { "num": 1200 }
    })";

    std::vector<field> nested_fields = {
        field("locations", field_types::OBJECT_ARRAY, false)
    };
    std::vector<field> flattened_fields;

    nlohmann::json doc = nlohmann::json::parse(json_str);
    auto flatten_op = field::flatten_doc(doc, nested_fields, flattened_fields);
    ASSERT_FALSE(flatten_op.ok());
    ASSERT_EQ("Field `locations` was not found or has an incorrect type.", flatten_op.error());

    nested_fields = {
        field("company", field_types::INT32, false)
    };

    flattened_fields.clear();
    flatten_op = field::flatten_doc(doc, nested_fields, flattened_fields);
    ASSERT_FALSE(flatten_op.ok());
    ASSERT_EQ("Field `company` was not found or has an incorrect type.", flatten_op.error());
}

TEST_F(CollectionNestedFieldsTest, SearchOnFieldsOnWildcardSchema) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true)};

    auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc = R"({
        "id": "0",
        "company": {"name": "Nike Inc."},
        "employees": {
            "num": 1200,
            "tags": ["senior plumber", "electrician"]
        },
        "locations": [
            { "pincode": 100, "country": "USA",
              "address": { "street": "One Bowerman Drive", "city": "Beaverton", "products": ["shoes", "tshirts"] }
            },
            { "pincode": 200, "country": "Canada",
              "address": { "street": "175 Commerce Valley", "city": "Thornhill", "products": ["sneakers", "shoes"] }
            }
        ]
    })"_json;

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());
    nlohmann::json create_res = add_op.get();
    ASSERT_EQ(doc.dump(), create_res.dump());

    // search both simply nested and deeply nested array-of-objects
    auto results = coll1->search("electrician commerce", {"employees", "locations"}, "", {}, sort_fields,
                                 {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(doc, results["hits"][0]["document"]);

    auto highlight_doc = R"({
      "employees":{
        "tags":[
          "senior plumber",
          "<mark>electrician</mark>"
        ]
      },
      "locations":[
        {
          "address":{
            "street":"One Bowerman Drive"
          }
        },
        {
          "address":{
            "street":"175 <mark>Commerce</mark> Valley"
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // search specific nested fields, only matching field is highlighted by default
    results = coll1->search("one shoe", {"locations.address.street", "employees.tags"}, "", {}, sort_fields,
                            {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(doc, results["hits"][0]["document"]);

    highlight_doc = R"({
      "locations":[
        {
          "address":{
            "street":"<mark>One</mark> Bowerman Drive"
          }
        },
        {
          "address":{
            "street":"175 Commerce Valley"
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // try to search nested fields that don't exist
    auto res_op = coll1->search("one shoe", {"locations.address.str"}, "", {}, sort_fields,
                                {0}, 10, 1, FREQUENCY, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `locations.address.str` in the schema.", res_op.error());

    res_op = coll1->search("one shoe", {"locations.address.foo"}, "", {}, sort_fields,
                           {0}, 10, 1, FREQUENCY, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `locations.address.foo` in the schema.", res_op.error());

    res_op = coll1->search("one shoe", {"locations.foo.street"}, "", {}, sort_fields,
                           {0}, 10, 1, FREQUENCY, {true});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `locations.foo.street` in the schema.", res_op.error());
}

TEST_F(CollectionNestedFieldsTest, IncludeExcludeFields) {
    auto doc_str = R"({
        "company": {"name": "Nike Inc."},
        "employees": {
            "num": 1200,
            "tags": ["senior plumber", "electrician"]
        },
        "employee": true,
        "locations": [
            { "pincode": 100, "country": "USA",
              "address": { "street": "One Bowerman Drive", "city": "Beaverton", "products": ["shoes", "tshirts"] }
            },
            { "pincode": 200, "country": "Canada",
              "address": { "street": "175 Commerce Valley", "city": "Thornhill", "products": ["sneakers", "shoes"] }
            }
        ],
        "one_obj_arr": [{"foo": "bar"}]
    })";

    auto doc = nlohmann::json::parse(doc_str);

    Collection::prune_doc(doc, tsl::htrie_set<char>(), {"one_obj_arr.foo"});
    ASSERT_EQ(0, doc.count("one_obj_arr"));

    // handle non-existing exclude field
    doc = nlohmann::json::parse(doc_str);
    Collection::prune_doc(doc, {"employees.num", "employees.tags"}, {"foobar"});
    ASSERT_EQ(1, doc.size());
    ASSERT_EQ(1, doc.count("employees"));
    ASSERT_EQ(2, doc["employees"].size());

    // select a specific field within nested array object
    doc = nlohmann::json::parse(doc_str);
    Collection::prune_doc(doc, {"locations.address.city"}, tsl::htrie_set<char>());
    ASSERT_EQ(R"({"locations":[{"address":{"city":"Beaverton"}},{"address":{"city":"Thornhill"}}]})", doc.dump());

    // select 2 fields within nested array object
    doc = nlohmann::json::parse(doc_str);
    Collection::prune_doc(doc, {"locations.address.city", "locations.address.products"}, tsl::htrie_set<char>());
    ASSERT_EQ(R"({"locations":[{"address":{"city":"Beaverton","products":["shoes","tshirts"]}},{"address":{"city":"Thornhill","products":["sneakers","shoes"]}}]})", doc.dump());

    // exclusion takes preference
    doc = nlohmann::json::parse(doc_str);
    Collection::prune_doc(doc, {"locations.address.city"}, {"locations.address.city"});
    ASSERT_EQ(R"({})", doc.dump());

    // include object, exclude sub-fields
    doc = nlohmann::json::parse(doc_str);
    Collection::prune_doc(doc, {"locations.address.city", "locations.address.products"}, {"locations.address.city"});
    ASSERT_EQ(R"({"locations":[{"address":{"products":["shoes","tshirts"]}},{"address":{"products":["sneakers","shoes"]}}]})", doc.dump());
}

TEST_F(CollectionNestedFieldsTest, HighlightNestedFieldFully) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true)};

    auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc = R"({
        "company_names": ["Space Corp. LLC", "Drive One Inc."],
        "company": {"names": ["Space Corp. LLC", "Drive One Inc."]},
        "locations": [
            { "pincode": 100, "country": "USA",
              "address": { "street": "One Bowerman Drive", "city": "Beaverton", "products": ["shoes", "tshirts"] }
            },
            { "pincode": 200, "country": "Canada",
              "address": { "street": "175 Commerce Drive", "city": "Thornhill", "products": ["sneakers", "shoes"] }
            }
        ]
    })"_json;

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // search both simply nested and deeply nested array-of-objects
    auto results = coll1->search("One", {"locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address").get();

    ASSERT_EQ(1, results["hits"].size());

    auto highlight_doc = R"({
      "locations":[
        {
          "address":{
            "street":"<mark>One</mark> Bowerman Drive"
          }
        },
        {
          "address":{
            "street":"175 Commerce Drive"
          }
        }
      ]
    })"_json;

    auto highlight_full_doc = R"({
        "locations":[
          {
            "address":{
              "city":"Beaverton",
              "products":[
                "shoes",
                "tshirts"
              ],
              "street":"<mark>One</mark> Bowerman Drive"
            }
          },
          {
            "address":{
              "city":"Thornhill",
              "products":[
                "sneakers",
                "shoes"
              ],
              "street":"175 Commerce Drive"
            }
          }
        ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());
    ASSERT_EQ(highlight_full_doc.dump(), results["hits"][0]["highlight"]["full"].dump());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // repeating token

    results = coll1->search("drive", {"locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address").get();

    ASSERT_EQ(1, results["hits"].size());

    highlight_doc = R"({
      "locations":[
        {
          "address":{
            "street":"One Bowerman <mark>Drive</mark>"
          }
        },
        {
          "address":{
            "street":"175 Commerce <mark>Drive</mark>"
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // nested array of array, highlighting parent of searched nested field
    results = coll1->search("shoes", {"locations.address.products"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address",
                            20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false, true,
                            "locations.address").get();

    ASSERT_EQ(1, results["hits"].size());
    highlight_full_doc = R"({
      "locations":[
        {
          "address":{
            "city":"Beaverton",
            "products":[
              "<mark>shoes</mark>",
              "tshirts"
            ],
            "street":"One Bowerman Drive"
          }
        },
        {
          "address":{
            "city":"Thornhill",
            "products":[
              "sneakers",
              "<mark>shoes</mark>"
            ],
            "street":"175 Commerce Drive"
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_full_doc.dump(), results["hits"][0]["highlight"]["full"].dump());
    ASSERT_EQ(highlight_full_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());

    // full highlighting only one of the 3 highlight fields
    results = coll1->search("drive", {"company.names", "company_names", "locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address",
                            20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false, true,
                            "company.names,company_names,locations.address").get();

    highlight_full_doc = R"({
        "locations":[
          {
            "address":{
              "city":"Beaverton",
              "products":[
                "shoes",
                "tshirts"
              ],
              "street":"One Bowerman <mark>Drive</mark>"
            }
          },
          {
            "address":{
              "city":"Thornhill",
              "products":[
                "sneakers",
                "shoes"
              ],
              "street":"175 Commerce <mark>Drive</mark>"
            }
          }
        ]
    })"_json;

    highlight_doc = R"({
        "company":{
          "names": ["Space Corp. LLC", "<mark>Drive</mark> One Inc."]
        },
        "company_names": ["Space Corp. LLC", "<mark>Drive</mark> One Inc."],
        "locations":[
          {
            "address":{
              "city":"Beaverton",
              "products":[
                "shoes",
                "tshirts"
              ],
              "street":"One Bowerman <mark>Drive</mark>"
            }
          },
          {
            "address":{
              "city":"Thornhill",
              "products":[
                "sneakers",
                "shoes"
              ],
              "street":"175 Commerce <mark>Drive</mark>"
            }
          }
        ]
    })"_json;

    ASSERT_EQ(highlight_full_doc.dump(), results["hits"][0]["highlight"]["full"].dump());
    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());

    // if highlight fields not provided, only matching sub-fields should appear in highlight

    results = coll1->search("space", {"company.names", "company_names", "locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    highlight_doc = R"({
        "company":{
          "names": ["<mark>Space</mark> Corp. LLC", "Drive One Inc."]
        },
        "company_names": ["<mark>Space</mark> Corp. LLC", "Drive One Inc."]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());
    ASSERT_EQ(0, results["hits"][0]["highlight"]["full"].size());

    // only a single highlight full field provided

    results = coll1->search("space", {"company.names", "company_names", "locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "company.names").get();

    highlight_full_doc = R"({
      "company":{
        "names":[
          "<mark>Space</mark> Corp. LLC",
          "Drive One Inc."
        ]
      }
    })"_json;

    highlight_doc = R"({
      "company":{
        "names":[
          "<mark>Space</mark> Corp. LLC",
          "Drive One Inc."
        ]
      },
      "company_names":[
        "<mark>Space</mark> Corp. LLC",
        "Drive One Inc."
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"]["snippet"].dump());
    ASSERT_EQ(highlight_full_doc.dump(), results["hits"][0]["highlight"]["full"].dump());
}

TEST_F(CollectionNestedFieldsTest, HighlightShouldHaveMeta) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true)};

    auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc = R"({
        "company_names": ["Quick brown fox jumped.", "The red fox was not fast."],
        "details": {
            "description": "Quick set, go.",
            "names": ["Quick brown fox jumped.", "The red fox was not fast."]
        },
        "locations": [
            {
              "address": { "street": "Brown Shade Avenue" }
            },
            {
                "address": { "street": "Graywolf Lane" }
            }
        ]
    })"_json;

    auto add_op = coll1->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // search both simply nested and deeply nested array-of-objects
    auto results = coll1->search("brown fox", {"company_names", "details", "locations"},
                                 "", {}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address").get();

    ASSERT_EQ(3, results["hits"][0]["highlight"]["meta"].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["meta"]["company_names"].size());

    ASSERT_EQ(2, results["hits"][0]["highlight"]["meta"]["company_names"]["matched_tokens"].size());
    ASSERT_EQ("brown", results["hits"][0]["highlight"]["meta"]["company_names"]["matched_tokens"][0]);
    ASSERT_EQ("fox", results["hits"][0]["highlight"]["meta"]["company_names"]["matched_tokens"][1]);

    ASSERT_EQ(2, results["hits"][0]["highlight"]["meta"]["details.names"]["matched_tokens"].size());
    ASSERT_EQ("brown", results["hits"][0]["highlight"]["meta"]["details.names"]["matched_tokens"][0]);
    ASSERT_EQ("fox", results["hits"][0]["highlight"]["meta"]["details.names"]["matched_tokens"][1]);

    ASSERT_EQ(1, results["hits"][0]["highlight"]["meta"]["locations.address.street"]["matched_tokens"].size());
    ASSERT_EQ("Brown", results["hits"][0]["highlight"]["meta"]["locations.address.street"]["matched_tokens"][0]);
}

TEST_F(CollectionNestedFieldsTest, GroupByOnNestedFieldsWithWildcardSchema) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true),
                                 field("education.name", field_types::STRING_ARRAY, true, true),
                                 field("employee.num", field_types::INT32, true, true)};

    auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "employee": {"num": 5000},
        "education": [
            {"name": "X High School", "type": "school"},
            {"name": "Y University", "type": "undergraduate"}
        ]
    })"_json;

    auto doc2 = R"({
        "employee": {"num": 1000},
        "education": [
            {"name": "X High School", "type": "school"},
            {"name": "Z University", "type": "undergraduate"}
        ]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());
    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());

    // group on a field inside array of objects
    auto results = coll1->search("school", {"education"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10,
                                 spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(), 10, "", 30,
                                 5, "", 10, {}, {}, {"education.name"}, 2).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["grouped_hits"].size());

    ASSERT_EQ(1, results["grouped_hits"][0]["group_key"].size());
    ASSERT_EQ(2, results["grouped_hits"][0]["group_key"][0].size());
    ASSERT_EQ("X High School", results["grouped_hits"][0]["group_key"][0][0].get<std::string>());
    ASSERT_EQ("Z University", results["grouped_hits"][0]["group_key"][0][1].get<std::string>());
    ASSERT_EQ(1, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("1", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ(1, results["grouped_hits"][1]["group_key"].size());
    ASSERT_EQ(2, results["grouped_hits"][1]["group_key"][0].size());
    ASSERT_EQ("X High School", results["grouped_hits"][1]["group_key"][0][0].get<std::string>());
    ASSERT_EQ("Y University", results["grouped_hits"][1]["group_key"][0][1].get<std::string>());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("0", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    // group on plain nested field
    results = coll1->search("school", {"education"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 10,
                            spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(), 10, "", 30,
                            5, "", 10, {}, {}, {"employee.num"}, 2).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["grouped_hits"].size());

    ASSERT_EQ(1, results["grouped_hits"][0]["group_key"].size());
    ASSERT_EQ(1, results["grouped_hits"][0]["group_key"][0].size());
    ASSERT_EQ(1000, results["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("1", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ(1, results["grouped_hits"][1]["group_key"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["group_key"][0].size());
    ASSERT_EQ(5000, results["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("0", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());
}