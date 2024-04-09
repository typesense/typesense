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

tsl::htrie_map<char, field> get_nested_map(const std::vector<field>& nested_fields) {
    tsl::htrie_map<char, field> map;
    for(const auto& f: nested_fields) {
        map.emplace(f.name, f);
    }

    return map;
}

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
    ASSERT_TRUE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok());
    ASSERT_EQ(5, flattened_fields.size());

    for(const auto& f: flattened_fields) {
        ASSERT_TRUE(f.is_array());
    }

    auto expected_json = R"(
        {
            ".flat": ["locations.address.city","locations.address.products","locations.address.street",
                      "locations.country", "locations.pincode"],
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

    // handle order of generation differences between compilers (due to iteration of unordered map)
    auto expected_flat_fields = doc[".flat"].get<std::vector<std::string>>();
    std::sort(expected_flat_fields.begin(), expected_flat_fields.end());
    doc[".flat"] = expected_flat_fields;

    ASSERT_EQ(doc.dump(), nlohmann::json::parse(expected_json).dump());

    // plain object
    flattened_fields.clear();
    doc = nlohmann::json::parse(json_str);
    nested_fields = {
        field("company", field_types::OBJECT, false)
    };

    ASSERT_TRUE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok());

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

    ASSERT_FALSE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok()); // must be of type object_array

    nested_fields = {
        field("locations.address", field_types::OBJECT_ARRAY, false)
    };

    flattened_fields.clear();
    ASSERT_TRUE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok());

    expected_json = R"(
        {
          ".flat": ["locations.address.city","locations.address.products","locations.address.street"],
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

    // handle order of generation differences between compilers (due to iteration of unordered map)
    expected_flat_fields = doc[".flat"].get<std::vector<std::string>>();
    std::sort(expected_flat_fields.begin(), expected_flat_fields.end());
    doc[".flat"] = expected_flat_fields;
    ASSERT_EQ(doc.dump(), nlohmann::json::parse(expected_json).dump());

    // primitive inside nested object
    flattened_fields.clear();
    doc = nlohmann::json::parse(json_str);
    nested_fields = {
        field("company.name", field_types::STRING, false)
    };

    ASSERT_TRUE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok());

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
    ASSERT_TRUE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok());
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

    ASSERT_TRUE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok());
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
    doc = nlohmann::json::parse(json_str);
    nested_fields = {
        field("employees.details.num_tags", field_types::INT32_ARRAY, false),
        field("employees.details.tags", field_types::STRING_ARRAY, false),
        field("employees.detail.tags", field_types::STRING_ARRAY, false),
    };

    ASSERT_TRUE(field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields).ok());
    ASSERT_EQ(3, flattened_fields.size());

    std::sort(flattened_fields.begin(), flattened_fields.end(), [](field& a, field& b) {
        return a.name < b.name;
    });

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
    auto flatten_op = field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields);
    ASSERT_FALSE(flatten_op.ok());
    ASSERT_EQ("Field `locations` not found.", flatten_op.error());

    nested_fields = {
        field("company", field_types::INT32, false)
    };

    flattened_fields.clear();
    flatten_op = field::flatten_doc(doc, get_nested_map(nested_fields), {}, false, flattened_fields);
    ASSERT_FALSE(flatten_op.ok());
    ASSERT_EQ("Field `company` has an incorrect type.", flatten_op.error());
}

TEST_F(CollectionNestedFieldsTest, FlattenStoredDoc) {
    auto stored_doc = R"({
        "employees": { "num": 1200 },
        "foo": "bar",
        "details": [{"name": "foo", "year": 2000}]
    })"_json;

    tsl::htrie_map<char, field> schema;
    schema.emplace("employees.num", field("employees.num", field_types::INT32, false));
    schema.emplace("details.name", field("details.name", field_types::STRING_ARRAY, false));
    schema.emplace("details.year", field("details.year", field_types::INT32_ARRAY, false));

    std::vector<field> flattened_fields;
    field::flatten_doc(stored_doc, schema, {}, true, flattened_fields);

    ASSERT_EQ(3, stored_doc[".flat"].size());
    ASSERT_EQ(7, stored_doc.size());

    ASSERT_EQ(1, stored_doc.count("employees.num"));
    ASSERT_EQ(1, stored_doc.count("details.name"));
    ASSERT_EQ(1, stored_doc.count("details.year"));
}

TEST_F(CollectionNestedFieldsTest, CompactNestedFields) {
    auto stored_doc = R"({
      "company_name": "Acme Corp",
      "display_address": {
        "city": "LA",
        "street": "Lumbard St"
      },
      "id": "314",
      "location_addresses": [
        {
          "city": "Columbus",
          "street": "Yale St"
        },
        {
          "city": "Soda Springs",
          "street": "5th St"
        }
      ],
      "num_employees": 10,
      "primary_address": {
        "city": "Los Angeles",
        "street": "123 Lumbard St"
      }
    })"_json;

    tsl::htrie_map<char, field> schema;
    schema.emplace("location_addresses.city", field("location_addresses.city", field_types::STRING_ARRAY, true));
    schema.emplace("location_addresses", field("location_addresses", field_types::OBJECT_ARRAY, true));
    schema.emplace("primary_address", field("primary_address", field_types::OBJECT, true));
    schema.emplace("primary_address.city", field("primary_address.city", field_types::STRING, true));
    schema.emplace("location_addresses.street", field("location_addresses.street", field_types::STRING_ARRAY, true));
    schema.emplace("primary_address.street", field("primary_address.street", field_types::STRING, true));

    field::compact_nested_fields(schema);
    ASSERT_EQ(2, schema.size());
    ASSERT_EQ(1, schema.count("primary_address"));
    ASSERT_EQ(1, schema.count("location_addresses"));

    std::vector<field> flattened_fields;
    field::flatten_doc(stored_doc, schema, {}, true, flattened_fields);

    ASSERT_EQ(2, stored_doc["location_addresses.city"].size());
    ASSERT_EQ(2, stored_doc["location_addresses.street"].size());
}

TEST_F(CollectionNestedFieldsTest, SearchOnFieldsOnWildcardSchema) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true)};

    auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO, {}, {}, true);
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

    auto results = coll1->search("electrician", {"employees"}, "", {}, sort_fields,
                                  {0}, 10, 1, FREQUENCY, {true}).get();

    auto highlight_doc = R"({
        "employees": {
          "num": {
            "matched_tokens": [],
            "snippet": "1200"
          },
          "tags": [
            {
              "matched_tokens": [],
              "snippet": "senior plumber"
            },
            {
              "matched_tokens": [
                "electrician"
              ],
              "snippet": "<mark>electrician</mark>"
            }
          ]
        }
      })"_json;

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());

    // search both simply nested and deeply nested array-of-objects
    results = coll1->search("electrician commerce", {"employees", "locations"}, "", {}, sort_fields,
                                 {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(doc, results["hits"][0]["document"]);

    highlight_doc = R"({
      "employees": {
        "num": {
          "matched_tokens": [],
          "snippet": "1200"
        },
        "tags": [
          {
            "matched_tokens": [],
            "snippet": "senior plumber"
          },
          {
            "matched_tokens": [
              "electrician"
            ],
            "snippet": "<mark>electrician</mark>"
          }
        ]
      },
      "locations": [
        {
          "address": {
            "city": {
              "matched_tokens": [],
              "snippet": "Beaverton"
            },
            "products": [
              {
                "matched_tokens": [],
                "snippet": "shoes"
              },
              {
                "matched_tokens": [],
                "snippet": "tshirts"
              }
            ],
            "street": {
              "matched_tokens": [],
              "snippet": "One Bowerman Drive"
            }
          },
          "country": {
            "matched_tokens": [],
            "snippet": "USA"
          },
          "pincode": {
            "matched_tokens": [],
            "snippet": "100"
          }
        },
        {
          "address": {
            "city": {
              "matched_tokens": [],
              "snippet": "Thornhill"
            },
            "products": [
              {
                "matched_tokens": [],
                "snippet": "sneakers"
              },
              {
                "matched_tokens": [],
                "snippet": "shoes"
              }
            ],
            "street": {
              "matched_tokens": [
                "Commerce"
              ],
              "snippet": "175 <mark>Commerce</mark> Valley"
            }
          },
          "country": {
            "matched_tokens": [],
            "snippet": "Canada"
          },
          "pincode": {
            "matched_tokens": [],
            "snippet": "200"
          }
        }
      ]
    })"_json;

    // ensure that flat fields are not returned in response
    ASSERT_EQ(0, results["hits"][0].count(".flat"));
    ASSERT_EQ(0, results["hits"][0].count("employees.tags"));

    // raw document in the store will not have the .flat meta key or actual flat fields
    nlohmann::json raw_doc;
    coll1->get_document_from_store(0, raw_doc, true);
    ASSERT_EQ(0, raw_doc.count(".flat"));
    ASSERT_EQ(0, raw_doc.count("employees.tags"));
    ASSERT_EQ(4, raw_doc.size());

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // after update also the flat fields or meta should not be present on disk
    doc["employees"]["tags"][0] = "senior plumber 2";
    auto update_op = coll1->add(doc.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());

    raw_doc.clear();
    coll1->get_document_from_store(0, raw_doc, true);
    ASSERT_EQ(0, raw_doc.count(".flat"));
    ASSERT_EQ(0, raw_doc.count("employees.tags"));
    ASSERT_EQ(4, raw_doc.size());

    // search specific nested fields, only matching field is highlighted by default
    results = coll1->search("one shoe", {"locations.address.street", "employees.tags"}, "", {}, sort_fields,
                            {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(doc, results["hits"][0]["document"]);

    highlight_doc = R"({
      "locations":[
        {
          "address":{
            "street":{
              "matched_tokens":[
                "One"
              ],
              "snippet":"<mark>One</mark> Bowerman Drive"
            }
          }
        },
        {
          "address":{
            "street":{
              "matched_tokens":[],
              "snippet":"175 Commerce Valley"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());
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

TEST_F(CollectionNestedFieldsTest, IncludeExcludeFieldsPruning) {
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
    ASSERT_EQ(1, doc.count("one_obj_arr"));
    ASSERT_EQ(1, doc["one_obj_arr"].size());

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
    ASSERT_EQ(R"({"locations":[{},{}]})", doc.dump());

    // include object, exclude sub-fields
    doc = nlohmann::json::parse(doc_str);
    Collection::prune_doc(doc, {"locations.address.city", "locations.address.products"}, {"locations.address.city"});
    ASSERT_EQ(R"({"locations":[{"address":{"products":["shoes","tshirts"]}},{"address":{"products":["sneakers","shoes"]}}]})", doc.dump());
}

TEST_F(CollectionNestedFieldsTest, ShouldNotPruneEmptyFields) {
    auto doc_str = R"({
        "name": "Foo",
        "obj": {},
        "obj_arr": [{}],
        "price": {
            "per_unit": {},
            "items": [{}]
        }
    })";

    auto doc = nlohmann::json::parse(doc_str);
    auto expected_doc = doc;
    Collection::prune_doc(doc, tsl::htrie_set<char>(), tsl::htrie_set<char>());
    ASSERT_EQ(expected_doc.dump(), doc.dump());
}

TEST_F(CollectionNestedFieldsTest, IncludeFieldsSearch) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "name", "type": "object" }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "name": {"first": "John", "last": "Smith"}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("*", {},
                                 "", {}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, {"name.first"},
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(1, results["hits"][0]["document"].size());
    ASSERT_EQ(1, results["hits"][0]["document"].count("name"));
    ASSERT_EQ(1, results["hits"][0]["document"]["name"].size());
}

TEST_F(CollectionNestedFieldsTest, HighlightNestedFieldFully) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true)};

    auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO, {}, {}, true);
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

    auto results = coll1->search("One", {"locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address").get();

    ASSERT_EQ(1, results["hits"].size());

    auto highlight_doc = R"({
      "locations":[
        {
          "address":{
            "city":{
              "matched_tokens":[

              ],
              "snippet":"Beaverton",
              "value":"Beaverton"
            },
            "products":[
              {
                "matched_tokens":[

                ],
                "snippet":"shoes",
                "value":"shoes"
              },
              {
                "matched_tokens":[

                ],
                "snippet":"tshirts",
                "value":"tshirts"
              }
            ],
            "street":{
              "matched_tokens":[
                "One"
              ],
              "snippet":"<mark>One</mark> Bowerman Drive",
              "value":"<mark>One</mark> Bowerman Drive"
            }
          }
        },
        {
          "address":{
            "city":{
              "matched_tokens":[

              ],
              "snippet":"Thornhill",
              "value":"Thornhill"
            },
            "products":[
              {
                "matched_tokens":[

                ],
                "snippet":"sneakers",
                "value":"sneakers"
              },
              {
                "matched_tokens":[

                ],
                "snippet":"shoes",
                "value":"shoes"
              }
            ],
            "street":{
              "matched_tokens":[

              ],
              "snippet":"175 Commerce Drive",
              "value":"175 Commerce Drive"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());
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
            "city":{
              "matched_tokens":[

              ],
              "snippet":"Beaverton",
              "value":"Beaverton"
            },
            "products":[
              {
                "matched_tokens":[

                ],
                "snippet":"shoes",
                "value":"shoes"
              },
              {
                "matched_tokens":[

                ],
                "snippet":"tshirts",
                "value":"tshirts"
              }
            ],
            "street":{
              "matched_tokens":[
                "Drive"
              ],
              "snippet":"One Bowerman <mark>Drive</mark>",
              "value":"One Bowerman <mark>Drive</mark>"
            }
          }
        },
        {
          "address":{
            "city":{
              "matched_tokens":[

              ],
              "snippet":"Thornhill",
              "value":"Thornhill"
            },
            "products":[
              {
                "matched_tokens":[

                ],
                "snippet":"sneakers",
                "value":"sneakers"
              },
              {
                "matched_tokens":[

                ],
                "snippet":"shoes",
                "value":"shoes"
              }
            ],
            "street":{
              "matched_tokens":[
                "Drive"
              ],
              "snippet":"175 Commerce <mark>Drive</mark>",
              "value":"175 Commerce <mark>Drive</mark>"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());
    ASSERT_EQ(0, results["hits"][0]["highlights"].size());

    // nested array of array, highlighting parent of searched nested field
    results = coll1->search("shoes", {"locations.address.products"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address",
                            20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false, true,
                            "locations.address").get();

    ASSERT_EQ(1, results["hits"].size());

    highlight_doc = R"({
      "locations":[
        {
          "address":{
            "city":{
              "matched_tokens":[

              ],
              "snippet":"Beaverton",
              "value":"Beaverton"
            },
            "products":[
              {
                "matched_tokens":[
                  "shoes"
                ],
                "snippet":"<mark>shoes</mark>",
                "value":"<mark>shoes</mark>"
              },
              {
                "matched_tokens":[

                ],
                "snippet":"tshirts",
                "value":"tshirts"
              }
            ],
            "street":{
              "matched_tokens":[

              ],
              "snippet":"One Bowerman Drive",
              "value":"One Bowerman Drive"
            }
          }
        },
        {
          "address":{
            "city":{
              "matched_tokens":[

              ],
              "snippet":"Thornhill",
              "value":"Thornhill"
            },
            "products":[
              {
                "matched_tokens":[

                ],
                "snippet":"sneakers",
                "value":"sneakers"
              },
              {
                "matched_tokens":[
                  "shoes"
                ],
                "snippet":"<mark>shoes</mark>",
                "value":"<mark>shoes</mark>"
              }
            ],
            "street":{
              "matched_tokens":[

              ],
              "snippet":"175 Commerce Drive",
              "value":"175 Commerce Drive"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());

    // full highlighting only one of the 3 highlight fields
    results = coll1->search("drive", {"company.names", "company_names", "locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "locations.address",
                            20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false, true,
                            "company.names,company_names,locations.address").get();

    highlight_doc = R"({
      "company": {
        "names": [
          {
            "matched_tokens": [],
            "snippet": "Space Corp. LLC"
          },
          {
            "matched_tokens": [
              "Drive"
            ],
            "snippet": "<mark>Drive</mark> One Inc."
          }
        ]
      },
      "company_names": [
        {
          "matched_tokens": [],
          "snippet": "Space Corp. LLC"
        },
        {
          "matched_tokens": [
            "Drive"
          ],
          "snippet": "<mark>Drive</mark> One Inc."
        }
      ],
      "locations": [
        {
          "address": {
            "city": {
              "matched_tokens": [],
              "snippet": "Beaverton",
              "value": "Beaverton"
            },
            "products": [
              {
                "matched_tokens": [],
                "snippet": "shoes",
                "value": "shoes"
              },
              {
                "matched_tokens": [],
                "snippet": "tshirts",
                "value": "tshirts"
              }
            ],
            "street": {
              "matched_tokens": [
                "Drive"
              ],
              "snippet": "One Bowerman <mark>Drive</mark>",
              "value": "One Bowerman <mark>Drive</mark>"
            }
          }
        },
        {
          "address": {
            "city": {
              "matched_tokens": [],
              "snippet": "Thornhill",
              "value": "Thornhill"
            },
            "products": [
              {
                "matched_tokens": [],
                "snippet": "sneakers",
                "value": "sneakers"
              },
              {
                "matched_tokens": [],
                "snippet": "shoes",
                "value": "shoes"
              }
            ],
            "street": {
              "matched_tokens": [
                "Drive"
              ],
              "snippet": "175 Commerce <mark>Drive</mark>",
              "value": "175 Commerce <mark>Drive</mark>"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());

    // if highlight fields not provided, only matching sub-fields should appear in highlight

    results = coll1->search("space", {"company.names", "company_names", "locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    highlight_doc = R"({
      "company":{
        "names":[
          {
            "matched_tokens":[
              "Space"
            ],
            "snippet":"<mark>Space</mark> Corp. LLC"
          },
          {
            "matched_tokens":[],
            "snippet":"Drive One Inc."
          }
        ]
      },
      "company_names":[
        {
          "matched_tokens":[
            "Space"
          ],
          "snippet":"<mark>Space</mark> Corp. LLC"
        },
        {
          "matched_tokens":[],
          "snippet":"Drive One Inc."
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());

    // only a single highlight full field provided

    results = coll1->search("space", {"company.names", "company_names", "locations.address"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "company.names").get();

    highlight_doc = R"({
      "company": {
        "names": [
          {
            "matched_tokens": [
              "Space"
            ],
            "snippet": "<mark>Space</mark> Corp. LLC",
            "value": "<mark>Space</mark> Corp. LLC"
          },
          {
            "matched_tokens": [],
            "snippet": "Drive One Inc.",
            "value": "Drive One Inc."
          }
        ]
      },
      "company_names": [
        {
          "matched_tokens": [
            "Space"
          ],
          "snippet": "<mark>Space</mark> Corp. LLC"
        },
        {
          "matched_tokens": [],
          "snippet": "Drive One Inc."
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());

    // try to highlight `id` field
    results = coll1->search("shoes", {"locations.address.products"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "id",
                            20, {}, {}, {}, 0, "<mark>", "</mark>", {}, 1000, true, false, true,
                            "id").get();

    ASSERT_TRUE(results["hits"][0]["highlight"].empty());
}

TEST_F(CollectionNestedFieldsTest, FieldsWithExplicitSchema) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object", "optional": false },
          {"name": "company.name", "type": "string", "optional": false, "facet": true },
          {"name": "locations", "type": "object[]", "optional": false }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    nlohmann::json coll_summary = coll1->get_summary_json();
    ASSERT_EQ(1, coll_summary.count("enable_nested_fields"));

    for(auto& f: coll_summary["fields"]) {
        ASSERT_EQ(0, f.count(fields::nested));
        ASSERT_EQ(0, f.count(fields::nested_array));
    }

    auto doc = R"({
        "company_names": ["Quick brown fox jumped.", "The red fox was not fast."],
        "details": {
            "description": "Quick set, go.",
            "names": ["Quick brown fox jumped.", "The red fox was not fast."]
        },
        "company": {"name": "Quick and easy fix."},
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

    ASSERT_TRUE(coll1->get_schema()["company.name"].facet);
    ASSERT_FALSE(coll1->get_schema()["company.name"].optional);

    // search both simply nested and deeply nested array-of-objects
    auto results = coll1->search("brown fox", {"details", "locations"},
                                 "", {}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    auto snippet_doc = R"({
      "details": {
        "description": {
          "matched_tokens": [],
          "snippet": "Quick set, go."
        },
        "names": [
          {
            "matched_tokens": [
              "brown",
              "fox"
            ],
            "snippet": "Quick <mark>brown</mark> <mark>fox</mark> jumped."
          },
          {
            "matched_tokens": [
              "fox"
            ],
            "snippet": "The red <mark>fox</mark> was not fast."
          }
        ]
      },
      "locations": [
        {
          "address": {
            "street": {
              "matched_tokens": [
                "Brown"
              ],
              "snippet": "<mark>Brown</mark> Shade Avenue"
            }
          }
        },
        {
          "address": {
            "street": {
              "matched_tokens": [],
              "snippet": "Graywolf Lane"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(snippet_doc.dump(), results["hits"][0]["highlight"].dump());

    results = coll1->search("fix", {"company.name"},
                            "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(1, results["hits"].size());

    // explicit nested array field (locations.address.street)
    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object", "optional": false },
          {"name": "company.name", "type": "string", "optional": false },
          {"name": "locations.address.street", "type": "string[]", "optional": false }
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();

    add_op = coll2->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    results = coll2->search("brown", {"locations.address.street"},
                            "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(1, results["hits"].size());

    snippet_doc = R"({
      "locations": [
        {
          "address": {
            "street": {
              "matched_tokens": [
                "Brown"
              ],
              "snippet": "<mark>Brown</mark> Shade Avenue"
            }
          }
        },
        {
          "address": {
            "street": {
              "matched_tokens": [],
              "snippet": "Graywolf Lane"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(snippet_doc.dump(), results["hits"][0]["highlight"].dump());

    // explicit partial array object field in the schema
    schema = R"({
        "name": "coll3",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object", "optional": false },
          {"name": "company.name", "type": "string", "optional": false },
          {"name": "locations.address", "type": "object[]", "optional": false }
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll3 = op.get();

    add_op = coll3->add(doc.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    results = coll3->search("brown", {"locations.address"},
                            "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(1, results["hits"].size());

    snippet_doc = R"({
      "locations": [
        {
          "address": {
            "street": {
              "matched_tokens": [
                "Brown"
              ],
              "snippet": "<mark>Brown</mark> Shade Avenue"
            }
          }
        },
        {
          "address": {
            "street": {
              "matched_tokens": [],
              "snippet": "Graywolf Lane"
            }
          }
        }
      ]
    })"_json;

    ASSERT_EQ(snippet_doc.dump(), results["hits"][0]["highlight"].dump());

    // non-optional object field validation (details)
    auto doc2 = R"({
        "company_names": ["Quick brown fox jumped.", "The red fox was not fast."],
        "company": {"name": "Quick and easy fix."},
        "locations": [
            {
                "address": { "street": "Foo bar street" }
            }
        ]
    })"_json;

    add_op = coll3->add(doc2.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `details` not found.", add_op.error());

    // check fields and their properties
    auto coll_fields = coll1->get_fields();
    ASSERT_EQ(6, coll_fields.size());

    for(size_t i = 0; i < coll_fields.size(); i++) {
        auto& coll_field = coll_fields[i];
        if(i <= 2) {
            // original 3 explicit fields will be non-optional, but the sub-properties will be optional
            ASSERT_FALSE(coll_field.optional);
        } else {
            ASSERT_TRUE(coll_field.optional);
        }
    }

    // deleting doc from coll1 and try querying again
    coll1->remove("0");
    results = coll1->search("brown", {"locations.address"},
                            "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // use remove_if_found API
    coll2->remove_if_found(0);
    results = coll2->search("brown", {"locations.address"},
                            "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, ExplicitSchemaOptionalFieldValidation) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object", "optional": true },
          {"name": "company.name", "type": "string", "optional": true },
          {"name": "locations", "type": "object[]", "optional": true },
          {"name": "blocks.text.description", "type": "string[]", "optional": true }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    // when a nested field is null it should be allowed
    auto doc1 = R"({
        "company": {"name": null}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // check the same with nested array type

    doc1 = R"({
        "blocks": {"text": [{"description": null}]}
    })"_json;

    add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // no optional field is present and that should be allowed
    doc1 = R"({
        "foo": "bar"
    })"_json;

    add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // some parts of an optional field is present in a subsequent doc indexed
    auto doc2 = R"({
        "details": {"name": "foo"}
    })"_json;
    add_op = coll1->add(doc2.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto doc3 = R"({
        "details": {"age": 30}
    })"_json;
    add_op = coll1->add(doc3.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // check fields and their properties
    auto coll_fields = coll1->get_fields();
    ASSERT_EQ(6, coll_fields.size());
    for(auto& coll_field : coll_fields) {
        ASSERT_TRUE(coll_field.optional);
    }
}

TEST_F(CollectionNestedFieldsTest, ExplicitSchemaForNestedArrayTypeValidation) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "blocks.text", "type": "object[]"},
            {"name": "blocks.text.description", "type": "string"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "blocks": {"text": [{"description": "Hello world."}]}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);

    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `blocks.text.description` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", add_op.error());
}

TEST_F(CollectionNestedFieldsTest, OptionalNestedOptionalOjectArrStringField) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "enable_nested_fields": true,
            "fields": [
              {"facet":true,"name":"data","optional":false,"type":"object"},
              {"facet":false,"name":"data.locations.stateShort","optional":true,"type":"string[]"}
            ]
        })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
          "data": {
            "locations": [
              {
                "stateShort": null
              }
            ]
          }
        })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    doc1 = R"({
          "data": {
            "locations": [
              {
                "stateShort": null
              },
              {
                "stateShort": "NY"
              }
            ]
          }
        })"_json;

    coll1->add(doc1.dump(), CREATE);

    auto results = coll1->search("ny", {"data.locations.stateShort"},
                                 "", {}, {}, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, OptionalNestedNonOptionalOjectArrStringField) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "enable_nested_fields": true,
            "fields": [
              {"facet":true,"name":"data","type":"object"},
              {"facet":false,"name":"data.locations.stateShort","type":"string[]"}
            ]
        })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
          "data": {
            "locations": [
              {
                "stateShort": null
              }
            ]
          }
        })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `data.locations.stateShort` has been declared in the schema, but is not found in the document.",
    add_op.error());

    doc1 = R"({
          "data": {
            "locations": [
              {
                "stateShort": null
              },
              {
                "stateShort": "NY"
              }
            ]
          }
        })"_json;

    coll1->add(doc1.dump(), CREATE);

    auto results = coll1->search("ny", {"data.locations.stateShort"},
                                 "", {}, {}, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}


TEST_F(CollectionNestedFieldsTest, UnindexedNestedFieldShouldNotClutterSchema) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "block", "type": "object", "optional": true, "index": false}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "block": {"text": "Hello world."}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // child fields should not become part of schema
    ASSERT_EQ(1, coll1->get_fields().size());
}

TEST_F(CollectionNestedFieldsTest, UnindexedNonOptionalFieldShouldBeAllowed) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name": "block", "type": "object", "index": false}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "block": {"text": "Hello world."}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // child fields should not become part of schema
    ASSERT_EQ(1, coll1->get_fields().size());
}

TEST_F(CollectionNestedFieldsTest, SortByNestedField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object", "optional": false },
          {"name": "company.num_employees", "type": "int32", "optional": false }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "details": {"count": 1000},
        "company": {"num_employees": 2000}
    })"_json;

    auto doc2 = R"({
        "details": {"count": 2000},
        "company": {"num_employees": 1000}
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());
    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());

    std::vector<sort_by> sort_fields = { sort_by("details.count", "ASC") };

    auto results = coll1->search("*", {},
                                 "", {}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    sort_fields = { sort_by("company.num_employees", "ASC") };
    results = coll1->search("*", {},
                            "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // with auto schema
    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();

    ASSERT_TRUE(coll2->add(doc1.dump(), CREATE).ok());
    ASSERT_TRUE(coll2->add(doc2.dump(), CREATE).ok());

    sort_fields = { sort_by("details.count", "ASC") };

    results = coll2->search("*", {},
                             "", {}, sort_fields, {0}, 10, 1,
                             token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    sort_fields = { sort_by("company.num_employees", "ASC") };
    results = coll2->search("*", {},
                            "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionNestedFieldsTest, OnlyExplcitSchemaFieldMustBeIndexedInADoc) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "company.num_employees", "type": "int32", "optional": false },
          {"name": "company.founded", "type": "int32", "optional": false }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "company": {"num_employees": 2000, "founded": 1976, "year": 2000}
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());
    auto fs = coll1->get_fields();
    ASSERT_EQ(2, coll1->get_fields().size());
}

TEST_F(CollectionNestedFieldsTest, VerifyDisableOfNestedFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": ".*", "type": "auto"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "company": {"num_employees": 2000, "founded": 1976, "year": 2000},
        "company_num_employees": 2000,
        "company_founded": 1976
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());
    auto fs = coll1->get_fields();
    ASSERT_EQ(3, coll1->get_fields().size());

    // explicit schema
    schema = R"({
        "name": "coll2",
        "fields": [
          {"name": "company_num_employees", "type": "int32"},
          {"name": "company_founded", "type": "int32"}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();

    ASSERT_TRUE(coll2->add(doc1.dump(), CREATE).ok());
    fs = coll2->get_fields();
    ASSERT_EQ(2, coll2->get_fields().size());
}

TEST_F(CollectionNestedFieldsTest, ExplicitDotSeparatedFieldsShouldHavePrecendence) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "company": {"num_employees": 1000, "ids": [1,2]},
        "details": [{"name": "bar"}],
        "company.num_employees": 2000,
        "company.ids": [10],
        "details.name": "foo"
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());
    auto fs = coll1->get_fields();
    ASSERT_EQ(6, coll1->get_fields().size());

    // simple nested object
    auto results = coll1->search("*", {}, "company.num_employees: 2000", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*", {}, "company.num_employees: 1000", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // nested array object
    results = coll1->search("foo", {"details.name"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("bar", {"details.name"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // nested simple array
    results = coll1->search("*", {}, "company.ids: 10", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*", {}, "company.ids: 1", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // WITH EXPLICIT SCHEMA

    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
        "fields": [
          {"name": "company.num_employees", "type": "int32"},
          {"name": "company.ids", "type": "int32[]"},
          {"name": "details.name", "type": "string[]"}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();

    auto doc2 = R"({
        "company": {"num_employees": 1000, "ids": [1,2]},
        "details": [{"name": "bar"}],
        "company.num_employees": 2000,
        "company.ids": [10],
        "details.name": ["foo"]
    })"_json;

    ASSERT_TRUE(coll2->add(doc2.dump(), CREATE).ok());

    // simple nested object
    results = coll2->search("*", {}, "company.num_employees: 2000", {}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll2->search("*", {}, "company.num_employees: 1000", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // nested array object
    results = coll2->search("foo", {"details.name"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll2->search("bar", {"details.name"}, "", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    // nested simple array
    results = coll2->search("*", {}, "company.ids: 10", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll2->search("*", {}, "company.ids: 1", {}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, NestedFieldWithExplicitWeight) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "company": {"num_employees": 2000, "founded": 1976},
        "studies": [{"name": "College 1", "location": "USA"}]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto results = coll1->search("college", {"studies"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "category", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {1}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, NestedFieldWithGeopointArray) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "addresses.geoPoint", "type": "geopoint[]"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "addresses": [{"geoPoint": [1.91, 23.5]}, {"geoPoint": [12.91, 23.5]}]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*", {}, "addresses.geoPoint: (12.911, 23.5, 1 mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // with nested geopoint array

    auto doc2 = R"({
        "addresses": [{"geoPoint": [[1.91, 23.5]]}, {"geoPoint": [[1.91, 23.5], [1.95, 24.5]]}]
    })"_json;

    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(2, results["found"].get<size_t>());

    // simply nested geopoint array

    auto doc3 = R"({
        "addresses": {"geoPoint": [[1.91, 23.5]]}
    })"_json;

    ASSERT_TRUE(coll1->add(doc3.dump(), CREATE).ok());
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(3, results["found"].get<size_t>());

    // simply nested geopoint
    // this technically cannot be allowed but it's really tricky to detect so we allow
    auto doc4 = R"({
        "addresses": {"geoPoint": [1.91, 23.5]}
    })"_json;

    auto simple_geopoint_op = coll1->add(doc4.dump(), CREATE);
    ASSERT_TRUE(simple_geopoint_op.ok());

    // data validation
    auto bad_doc = R"({
        "addresses": [{"geoPoint": [1.91, "x"]}]
    })"_json;

    auto create_op = coll1->add(bad_doc.dump(), CREATE);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Field `addresses.geoPoint` has an incorrect type.", create_op.error());

    bad_doc = R"({
        "addresses": [{"geoPoint": [[1.91, "x"]]}]
    })"_json;

    create_op = coll1->add(bad_doc.dump(), CREATE);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Field `addresses.geoPoint` must be an array of geopoint.", create_op.error());
}

TEST_F(CollectionNestedFieldsTest, NestedFieldWithGeopoint) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "enable_nested_fields": true,
            "fields": [
              {"name": "address.geoPoint", "type": "geopoint"}
            ]
        })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({"address": {"geoPoint": [19.07283, 72.88261]}})"_json;
    auto add_op = coll1->add(doc1.dump(), CREATE);
    LOG(INFO) << add_op.error();
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, 0).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*", {}, "address.geoPoint: (19.07, 72.882, 1 mi)",
                            {}, {}, {0}, 10, 1, FREQUENCY).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // data validation
    auto bad_doc = R"({
        "address": {"geoPoint": [1.91, "x"]}
    })"_json;

    auto create_op = coll1->add(bad_doc.dump(), CREATE);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Field `address.geoPoint` has an incorrect type.", create_op.error());

    bad_doc = R"({
        "address": {"geoPoint": [[1.91, "x"]]}
    })"_json;

    create_op = coll1->add(bad_doc.dump(), CREATE);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Field `address.geoPoint` must be a 2 element array: [lat, lng].", create_op.error());

    // with nested array field
    bad_doc = R"({
        "address": [
            {"geoPoint": [1.91, 2.56]},
            {"geoPoint": [2.91, 3.56]}
        ]
    })"_json;

    create_op = coll1->add(bad_doc.dump(), CREATE);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Field `address.geoPoint` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", create_op.error());
}

TEST_F(CollectionNestedFieldsTest, GroupByOnNestedFieldsWithWildcardSchema) {
    std::vector<field> fields = {field(".*", field_types::AUTO, false, true),
                                 field("education.name", field_types::STRING_ARRAY, true, true),
                                 field("employee.num", field_types::INT32, true, true)};

    auto op = collectionManager.create_collection("coll1", 1, fields, "", 0, field_types::AUTO, {}, {},
                                                  true);
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

TEST_F(CollectionNestedFieldsTest, WildcardWithExplicitSchema) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"},
          {"name": "company.id", "type": "int32"},
          {"name": "studies.year", "type": "int32[]"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "company": {"id": 1000, "name": "Foo"},
        "studies": [{"name": "College 1", "year": 1997}]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto results = coll1->search("*", {}, "company.id: 1000", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*", {}, "studies.year: 1997", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, DynamicFieldWithExplicitSchema) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "spec", "type": "object"},
          {"name": "spec\\..*\\.value", "type": "float"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "spec": {"number": {"value": 100}}
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto field_vec = coll1->get_fields();
    ASSERT_EQ(3, field_vec.size());
    ASSERT_EQ(field_types::FLOAT, field_vec[2].type);

    // with only explicit nested dynamic type
    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"},
          {"name": "spec\\..*\\.value", "type": "float"}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();
    ASSERT_TRUE(coll2->add(doc1.dump(), CREATE).ok());

    field_vec = coll2->get_fields();
    ASSERT_EQ(4, field_vec.size());
    ASSERT_EQ(field_types::FLOAT, field_vec[3].type);
}

TEST_F(CollectionNestedFieldsTest, UpdateOfNestFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name":"name", "type": "string", "index": false, "optional": true},
          {"name":"brand","type":"object","optional":true},
          {"name":"brand.id","type":"int32","sort":false},
          {"name":"brand.name","type":"string","index":false,"sort":false,"optional":true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    auto doc1 = R"({
        "id": "b4db5f0456a93320428365f92c2a54ce15df2d0a",
        "product_id": 63992305,
        "name": "Chips",
        "link": "http://wicked-uncle.biz",
        "meta": {
            "valid": true
        },
        "brand": {
            "id": 34002,
            "name": "Hodkiewicz - Rempel"
        }
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    // action=update - `name` changed, `id` not sent
    // `id` field should not be deleted

    auto doc_update = R"({
        "id": "b4db5f0456a93320428365f92c2a54ce15df2d0a",
        "brand": {
            "name": "Rempel"
        }
    })"_json;

    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"][0]["document"].size());
    ASSERT_EQ(2, results["hits"][0]["document"]["brand"].size());
    ASSERT_EQ("Rempel", results["hits"][0]["document"]["brand"]["name"].get<std::string>());

    // action=emplace
    doc_update = R"({
        "id": "b4db5f0456a93320428365f92c2a54ce15df2d0a",
        "brand": {
            "name": "The Rempel"
        }
    })"_json;

    ASSERT_TRUE(coll1->add(doc_update.dump(), EMPLACE).ok());

    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(6, results["hits"][0]["document"].size());
    ASSERT_EQ(2, results["hits"][0]["document"]["brand"].size());
    ASSERT_EQ("The Rempel", results["hits"][0]["document"]["brand"]["name"].get<std::string>());

    // action=upsert requires the full document
    doc_update = R"({
        "id": "b4db5f0456a93320428365f92c2a54ce15df2d0a",
        "brand": {
            "name": "Xomel"
        }
    })"_json;

    auto add_op = coll1->add(doc_update.dump(), UPSERT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `brand.id` has been declared in the schema, but is not found in the document.", add_op.error());

    doc_update = R"({
        "id": "b4db5f0456a93320428365f92c2a54ce15df2d0a",
        "name": "Chips",
        "brand": {
            "id": 34002,
            "name": "Xomel"
        }
    })"_json;

    add_op = coll1->add(doc_update.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());
    ASSERT_EQ(2, results["hits"][0]["document"]["brand"].size());
    ASSERT_EQ("Xomel", results["hits"][0]["document"]["brand"]["name"].get<std::string>());

    // upsert with brand.name missing is allowed because it's optional
    doc_update = R"({
        "id": "b4db5f0456a93320428365f92c2a54ce15df2d0a",
        "name": "Potato Chips",
        "brand": {
            "id": 34002
        }
    })"_json;

    add_op = coll1->add(doc_update.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());
    ASSERT_EQ(1, results["hits"][0]["document"]["brand"].size());
    ASSERT_EQ(34002, results["hits"][0]["document"]["brand"]["id"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, UpdateOfNestFieldsWithWildcardSchema) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "company": {"num_employees": 2000, "founded": 1976},
        "studies": [{"name": "College 1"}]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto doc_update = R"({
        "id": "0",
        "company": {"num_employees": 2000, "founded": 1976, "year": 2000},
        "studies": [{"name": "College Alpha", "year": 1967},{"name": "College Beta", "year": 1978}]
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    auto results = coll1->search("*", {}, "company.year: 2000", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*", {}, "studies.year: 1967", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("*", {}, "studies.year: 1978", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("alpha", {"studies.name"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("beta", {"studies.name"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // try removing fields via upsert, dropping "company.year"
    doc_update = R"({
        "id": "0",
        "company": {"num_employees": 4000, "founded": 1976},
        "studies": [{"name": "College Alpha"}]
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPSERT).ok());

    results = coll1->search("*", {}, "company.year: 2000", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll1->search("*", {}, "studies.year: 1967", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll1->search("*", {}, "studies.year: 1978", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());

    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());
    ASSERT_EQ(2, results["hits"][0]["document"]["company"].size());
    ASSERT_EQ(4000, results["hits"][0]["document"]["company"]["num_employees"].get<size_t>());
    ASSERT_EQ(1976, results["hits"][0]["document"]["company"]["founded"].get<size_t>());
    ASSERT_EQ(1, results["hits"][0]["document"]["studies"].size());
    ASSERT_EQ(1, results["hits"][0]["document"]["studies"][0].size());
    ASSERT_EQ("College Alpha", results["hits"][0]["document"]["studies"][0]["name"].get<std::string>());

    // via update (should not remove, since document can be partial)
    doc_update = R"({
        "id": "0",
        "company": {"num_employees": 2000},
        "studies": [{"name": "College Alpha"}]
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), UPDATE).ok());

    results = coll1->search("*", {}, "company.founded: 1976", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());
    ASSERT_EQ(2, results["hits"][0]["document"]["company"].size());
    ASSERT_EQ(2000, results["hits"][0]["document"]["company"]["num_employees"].get<size_t>());
    ASSERT_EQ(1976, results["hits"][0]["document"]["company"]["founded"].get<size_t>());
    ASSERT_EQ(1, results["hits"][0]["document"]["studies"].size());
    ASSERT_EQ(1, results["hits"][0]["document"]["studies"][0].size());
    ASSERT_EQ("College Alpha", results["hits"][0]["document"]["studies"][0]["name"].get<std::string>());

    // via emplace (should not remove, since document can be partial)
    doc_update = R"({
        "id": "0",
        "company": {},
        "studies": [{"name": "College Alpha", "year": 1977}]
    })"_json;
    ASSERT_TRUE(coll1->add(doc_update.dump(), EMPLACE).ok());

    results = coll1->search("*", {}, "company.num_employees: 2000", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());
    ASSERT_EQ(2, results["hits"][0]["document"]["company"].size());
    ASSERT_EQ(2000, results["hits"][0]["document"]["company"]["num_employees"].get<size_t>());
    ASSERT_EQ(1976, results["hits"][0]["document"]["company"]["founded"].get<size_t>());
    ASSERT_EQ(1, results["hits"][0]["document"]["studies"].size());
    ASSERT_EQ(2, results["hits"][0]["document"]["studies"][0].size());
    ASSERT_EQ("College Alpha", results["hits"][0]["document"]["studies"][0]["name"].get<std::string>());
    ASSERT_EQ(1977, results["hits"][0]["document"]["studies"][0]["year"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, NestedSchemaWithSingularType) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "studies.year", "type": "int32", "optional": false}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "studies": [{"name": "College 1", "year": 1997}]
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `studies.year` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", add_op.error());

    // even when field is optional, there should be an error
    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
        "fields": [
          {"name": "studies.year", "type": "int32", "optional": true}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();
    add_op = coll2->add(doc1.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `studies.year` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", add_op.error());

    // allow optional field to be missing when value is singular
    doc1 = R"({
        "id": "0",
        "studies": {"name": "College 1"}
    })"_json;

    add_op = coll2->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());
}

TEST_F(CollectionNestedFieldsTest, NestedSchemaAutoAndFacet) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "person.*", "type": "auto", "facet": true},
          {"name": "schools.*", "type": "auto", "facet": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "person": {"name": "Tony Stark"},
        "schools": [{"name": "Primary School"}]
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto fields = coll1->get_fields();
    for(const auto&f : fields) {
        ASSERT_TRUE(f.facet);
    }

    ASSERT_TRUE(coll1->get_schema()["schools.name"].optional);
}

TEST_F(CollectionNestedFieldsTest, NestedObjectOfObjectEnableFacet) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "variants", "type": "object"},
          {"name": "variants\\..*\\.price", "type": "int64", "facet": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "variants": {
            "store_1": {"price": 100},
            "store_2": {"price": 200}
        }
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    ASSERT_TRUE(coll1->get_schema()["variants.store_1.price"].facet);
    ASSERT_TRUE(coll1->get_schema()["variants.store_2.price"].facet);
}

TEST_F(CollectionNestedFieldsTest, ArrayOfObjectsFaceting) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "location_addresses", "type": "object[]", "facet": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "company_name": "Acme Corp",
        "display_address": {
            "city": "LA",
            "street": "Lumbard St"
        },
        "location_addresses": [
            {
                "city": "Columbus",
                "street": "Yale St"
            },
            {
                "city": "Soda Springs",
                "street": "5th St"
            }
        ],
        "num_employees": 10,
        "primary_address": {
            "city": "Los Angeles",
            "street": "123 Lumbard St"
        }
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("*", {}, "", {"location_addresses.city"}, {},
                                 {0}, 10, 1, FREQUENCY, {false}).get();

    // add same doc again
    doc1["id"] = "1";
    add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {}, "", {"location_addresses.city"}, {},
                            {0}, 10, 1, FREQUENCY, {false}).get();

    // facet count should be 2
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_EQ("Columbus", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    ASSERT_EQ("Soda Springs", results["facet_counts"][0]["counts"][1]["value"].get<std::string>());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"][1]["count"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, HighlightArrayInsideArrayOfObj) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "studies", "type": "auto"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "studies": [
            {"name": "College 1", "tags": ["foo", "bar"]},
            {"name": "College 1", "tags": ["alpha", "beta"]}
        ]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());

    auto results = coll1->search("beta", {"studies"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    nlohmann::json highlight_meta_doc = R"({
      "studies": [
        {
          "name": {
            "matched_tokens": [],
            "snippet": "College 1"
          },
          "tags": [
            {
              "matched_tokens": [],
              "snippet": "foo"
            },
            {
              "matched_tokens": [],
              "snippet": "bar"
            }
          ]
        },
        {
          "name": {
            "matched_tokens": [],
            "snippet": "College 1"
          },
          "tags": [
            {
              "matched_tokens": [],
              "snippet": "alpha"
            },
            {
              "matched_tokens": [
                "beta"
              ],
              "snippet": "<mark>beta</mark>"
            }
          ]
        }
      ]
    })"_json;

    ASSERT_EQ(highlight_meta_doc.dump(), results["hits"][0]["highlight"].dump());
}

TEST_F(CollectionNestedFieldsTest, ErrorWhenObjectTypeUsedWithoutEnablingNestedFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "details", "type": "object", "optional": false }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Type `object` or `object[]` can be used only when nested fields are enabled by setting` "
              "enable_nested_fields` to true.", op.error());

    schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "details", "type": "object[]", "optional": false }
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Type `object` or `object[]` can be used only when nested fields are enabled by setting` "
              "enable_nested_fields` to true.", op.error());
}

TEST_F(CollectionNestedFieldsTest, FieldsWithDotsButNotNested) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "name.first", "type": "string"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "name.first": "Alpha Beta Gamma"
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("beta", {"name.first"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ("Alpha <mark>Beta</mark> Gamma",
              results["hits"][0]["highlight"]["name.first"]["snippet"].get<std::string>());
}

TEST_F(CollectionNestedFieldsTest, NullValuesWithExplicitSchema) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "name", "type": "object"},
          {"name": "name.first", "type": "string"},
          {"name": "name.last", "type": "string", "optional": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "name": {"last": null, "first": "Jack"}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("jack", {"name.first"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"][0]["document"].size());  // id, name
    ASSERT_EQ(1, results["hits"][0]["document"]["name"].size());  // name.first
    ASSERT_EQ("Jack", results["hits"][0]["document"]["name"]["first"].get<std::string>());
}

TEST_F(CollectionNestedFieldsTest, EmplaceWithNullValueOnRequiredField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name":"currency", "type":"object"},
            {"name":"currency.eu", "type":"int32", "optional": false}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    auto doc_with_null = R"({
      "id": "0",
      "currency": {
        "eu": null
      }
    })"_json;

    auto add_op = coll1->add(doc_with_null.dump(), EMPLACE);
    ASSERT_FALSE(add_op.ok());

    add_op = coll1->add(doc_with_null.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());

    auto doc1 = R"({
      "id": "0",
      "currency": {
        "eu": 12000
      }
    })"_json;

    add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // now update with null value -- should not be allowed
    auto update_doc = R"({
      "id": "0",
      "currency": {
        "eu": null
      }
    })"_json;

    auto update_op = coll1->add(update_doc.dump(), EMPLACE);
    ASSERT_FALSE(update_op.ok());
    ASSERT_EQ("Field `currency.eu` must be an int32.", update_op.error());
}

TEST_F(CollectionNestedFieldsTest, EmplaceWithNullValueOnOptionalField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name":"currency", "type":"object"},
            {"name":"currency.eu", "type":"int32", "optional": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    auto doc1 = R"({
      "id": "0",
      "currency": {
        "eu": 12000
      }
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // now update with null value -- should be allowed since field is optional
    auto update_doc = R"({
      "id": "0",
      "currency": {
        "eu": null
      }
    })"_json;

    auto update_op = coll1->add(update_doc.dump(), EMPLACE);
    ASSERT_TRUE(update_op.ok());

    // try to fetch the document to see the stored value
    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"][0]["document"].size());  // id, currency
    ASSERT_EQ(0, results["hits"][0]["document"]["currency"].size());
}

TEST_F(CollectionNestedFieldsTest, UpsertWithNullValueOnOptionalField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "status", "type": "object"},
          {"name": "title", "type": "string"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "title": "Title Alpha",
        "status": {"name": "Foo"}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("alpha", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());  // id, title, status
    ASSERT_EQ(1, results["hits"][0]["document"]["status"].size());

    results = coll1->search("foo", {"status"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // upsert again with null value
    doc1 = R"({
        "id": "0",
        "title": "Title Alpha",
        "status": {"name": null}
    })"_json;

    add_op = coll1->add(doc1.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("alpha", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["document"].size());  // id, title, status
    ASSERT_EQ(0, results["hits"][0]["document"]["status"].size());

    results = coll1->search("foo", {"status"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, EmplaceWithMissingArrayValueOnOptionalField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name":"currency", "type":"object[]"},
            {"name":"currency.eu", "type":"int32[]", "optional": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    auto doc1 = R"({
      "id": "0",
      "currency": [
        {"eu": 12000},
        {"us": 10000}
      ]
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // now update with null value -- should be allowed since field is optional
    auto update_doc = R"({
      "id": "0",
      "currency": [
        {"us": 10000}
      ]
    })"_json;

    auto update_op = coll1->add(update_doc.dump(), EMPLACE);
    ASSERT_TRUE(update_op.ok());

    // try to fetch the document to see the stored value
    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"][0]["document"].size());  // id, currency
    ASSERT_EQ(1, results["hits"][0]["document"]["currency"].size());
    ASSERT_EQ(10000, results["hits"][0]["document"]["currency"][0]["us"].get<uint32_t>());
}

TEST_F(CollectionNestedFieldsTest, UpdateNestedDocument) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "contributors", "type": "object", "optional": false},
          {"name": "title", "type": "string", "optional": false}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "title": "Title Alpha",
        "contributors": {"first_name": "John", "last_name": "Galt"}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // update document partially

    doc1 = R"({
        "id": "0",
        "title": "Title Beta"
    })"_json;

    add_op = coll1->add(doc1.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("beta", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // emplace document partially

    doc1 = R"({
        "id": "0",
        "title": "Title Gamma"
    })"_json;

    add_op = coll1->add(doc1.dump(), EMPLACE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("gamma", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // update a sub-field of an object
    doc1 = R"({
        "id": "0",
        "contributors": {"last_name": "Shaw"}
    })"_json;

    add_op = coll1->add(doc1.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("shaw", {"contributors"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    results = coll1->search("john", {"contributors.first_name"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // should not be able to find the old name

    results = coll1->search("galt", {"contributors"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, UpdateNestedDocumentAutoSchema) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "price": {"now": 3000, "country": "US"}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // update document partially

    doc1 = R"({
        "id": "0",
        "price": {"now": 4000}
    })"_json;

    add_op = coll1->add(doc1.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("us", {"price.country"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, UpdateNestedDocumentWithOptionalNullValue) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "contributors", "type": "object", "optional": true},
          {"name": "title", "type": "string", "optional": false}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "id": "0",
        "title": "Title Alpha",
        "contributors": {"first_name": "John", "last_name": null}
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // update document partially

    doc1 = R"({
        "id": "0",
        "title": "Title Beta",
        "contributors": {"first_name": "Jack", "last_name": null}
    })"_json;

    add_op = coll1->add(doc1.dump(), UPDATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("beta", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // emplace document partially

    doc1 = R"({
        "id": "0",
        "title": "Title Gamma",
        "contributors": {"first_name": "Jim", "last_name": null}
    })"_json;

    add_op = coll1->add(doc1.dump(), EMPLACE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("gamma", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // remove field with null value
    auto del_op = coll1->remove("0");
    ASSERT_TRUE(del_op.ok());
    results = coll1->search("gamma", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, ImproveErrorMessageForNestedArrayNumericalFields) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
       "fields": [
            {"name": "variants", "type": "object[]", "facet": true, "index": true},
            {"name": "variants.sellingPrice", "type": "int32", "facet": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
       "variants": [
      {
        "sellingPrice": 2300,
        "timestamp": 10000,
        "is_deleted": false,
        "price": 50.50
      },
      {
        "sellingPrice": 1200,
        "timestamp": 10000,
        "is_deleted": false,
        "price": 150.50
      }
    ]

    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `variants.sellingPrice` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", add_op.error());

    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
       "fields": [
            {"name": "variants", "type": "object[]", "facet": true, "index": true},
            {"name": "variants.timestamp", "type": "int64", "facet": true}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll2 = op.get();

    add_op = coll2->add(doc1.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `variants.timestamp` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", add_op.error());

    schema = R"({
        "name": "coll3",
        "enable_nested_fields": true,
       "fields": [
            {"name": "variants", "type": "object[]", "facet": true, "index": true},
            {"name": "variants.is_deleted", "type": "bool", "facet": true}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll3 = op.get();

    add_op = coll3->add(doc1.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `variants.is_deleted` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", add_op.error());

    // float

    schema = R"({
        "name": "coll4",
        "enable_nested_fields": true,
       "fields": [
            {"name": "variants", "type": "object[]", "facet": true, "index": true},
            {"name": "variants.price", "type": "float", "facet": true}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll4 = op.get();

    add_op = coll4->add(doc1.dump(), CREATE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `variants.price` has an incorrect type. "
              "Hint: field inside an array of objects must be an array type as well.", add_op.error());
}

TEST_F(CollectionNestedFieldsTest, HighlightArrayOfObjects) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": ".*", "type": "auto"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "details": [
            {"foo": "John Smith"},
            {"name": "James Peterson"},
            {"bar": "John Galt"}
        ]
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("james", {"details.name"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                                 {true}, 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["highlight"]["details"].size());
    ASSERT_EQ(0, results["hits"][0]["highlight"]["details"][0].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["details"][1].size());
    ASSERT_EQ(0, results["hits"][0]["highlight"]["details"][2].size());

    results = coll1->search("james", {"details.name"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1}, 10000, true, false, true, "details.name").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(0, results["hits"][0]["highlight"]["details"][0].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["details"][1].size());
    ASSERT_EQ(0, results["hits"][0]["highlight"]["details"][2].size());

    results = coll1->search("james", {"details.name"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                            {true}, 1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1}, 10000, true, false, true, "details").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"][0]["highlight"]["details"].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["details"][0].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["details"][1].size());
    ASSERT_EQ(1, results["hits"][0]["highlight"]["details"][2].size());
}

TEST_F(CollectionNestedFieldsTest, DeepNestedOptionalArrayValue) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {
                "facet": false,
                "index": true,
                "infix": false,
                "locale": "",
                "name": "items.name",
                "optional": true,
                "sort": false,
                "type": "string[]"
            },
            {
                "facet": false,
                "index": true,
                "infix": false,
                "locale": "",
                "name": "items.description",
                "optional": true,
                "sort": false,
                "type": "string[]"
            },
            {
                "facet": false,
                "index": true,
                "infix": false,
                "locale": "",
                "name": "items.nested_items.name",
                "optional": true,
                "sort": false,
                "type": "string[]"
            }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "items": [
            {
                "description": "random description.",
                "name": "foobar",
                "nested_items": [
                    {
                        "isAvailable": true
                    },
                    {
                        "description": "nested description here",
                        "isAvailable": true,
                        "name": "naruto"
                    },
                    {
                        "description": "description again",
                        "isAvailable": true,
                        "name": "dragon ball"
                    }
                ]
            }
        ]
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("naruto", {"items.nested_items.name"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                                 {true}, 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, FloatInsideNestedObject) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "price.*", "type": "float"}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    auto doc1 = R"({
        "price": {
            "USD": 75.40
        }
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    // should also accept whole numbers
    schema = R"({
        "name": "coll2",
        "enable_nested_fields": true,
        "fields": [
          {"name": "price.*", "type": "float"}
        ]
    })"_json;

    op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll2 = op.get();

    auto doc2 = R"({
        "price": {
            "USD": 75
        }
    })"_json;

    add_op = coll2->add(doc2.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto fs = coll2->get_fields();
    ASSERT_EQ(3, fs.size());

    add_op = coll2->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());
}

TEST_F(CollectionNestedFieldsTest, NestedFieldWithRegexName) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
            {"name":"titles", "type":"object"},
            {"name": "titles\\..*", "type":"string"},
            {"name":"start_date", "type":"object"},
            {"name":"start_date\\..*", "type":"int32", "facet":true, "optional":true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    auto doc1 = R"({
      "titles": {
        "en": "Foobar baz"
      },
      "start_date": {
        "year": 2020,
        "month": 2,
        "day": 3
      }
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("foobar", {"titles.en"}, "start_date.year: 2020", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionNestedFieldsTest, HighlightOnFlatFieldWithSnippeting) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("body", field_types::STRING, false)};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "pimples keep popping up on chin";
    doc1["body"] = "on left side of chin under the corner of my mouth i keep getting huge pimples. they’ll go away for "
                   "a few days but come back every time and i don’t quit it. I have oily skin and acne prone. i "
                   "also just started using twice a week";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    auto results = coll1->search("pimples", {"title", "body"}, "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true}).get();

    auto highlight_doc = R"({
        "body": {
          "matched_tokens": [
            "pimples"
          ],
          "snippet": "i keep getting huge <mark>pimples</mark>. they’ll go away for"
        },
        "title": {
          "matched_tokens": [
            "pimples"
          ],
          "snippet": "<mark>pimples</mark> keep popping up on chin"
        }
    })"_json;

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());

    // with full highlighting

    highlight_doc = R"({
        "body": {
          "matched_tokens": [
            "pimples"
          ],
          "snippet": "i keep getting huge <mark>pimples</mark>. they’ll go away for",
          "value": ""
        },
        "title": {
          "matched_tokens": [
            "pimples"
          ],
          "snippet": "<mark>pimples</mark> keep popping up on chin",
          "value": "<mark>pimples</mark> keep popping up on chin"
        }
    })"_json;

    highlight_doc["body"]["value"] = "on left side of chin under the corner of my mouth i keep getting huge "
                                     "<mark>pimples</mark>. they’ll go away for a few days but come back every time "
                                     "and i don’t quit it. I have oily skin and acne prone. i also just started "
                                     "using twice a week";

    results = coll1->search("pimples", {"title", "body"}, "", {}, {}, {2}, 10,
                            1, FREQUENCY, {true}, 1, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title,body").get();

    ASSERT_EQ(highlight_doc.dump(), results["hits"][0]["highlight"].dump());
}

TEST_F(CollectionNestedFieldsTest, NestedObjecEnableSortOnString) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "status", "type": "object"},
          {"name": "status\\..*", "type": "string", "sort": true}
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "status": {
            "1": "ACCEPTED"
        }
    })"_json;

    auto add_op = coll1->add(doc1.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    ASSERT_TRUE(coll1->get_schema()["status.1"].sort);
}
