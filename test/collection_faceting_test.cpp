#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionFacetingTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_faceting";
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

TEST_F(CollectionFacetingTest, FacetCounts) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("name_facet", field_types::STRING, true),
                                 field("age", field_types::INT32, true),
                                 field("years", field_types::INT32_ARRAY, true),
                                 field("rating", field_types::FLOAT, true),
                                 field("timestamps", field_types::INT64_ARRAY, true),
                                 field("tags", field_types::STRING_ARRAY, true)};

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if(coll_array_fields == nullptr) {
        coll_array_fields = collectionManager.create_collection("coll_array_fields", 4, fields, "age").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        nlohmann::json document = nlohmann::json::parse(json_line);
        document["name_facet"] = document["name"];
        const std::string & patched_json_line = document.dump();
        coll_array_fields->add(patched_json_line);
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets = {"tags"};

    // single facet with no filters
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(3, results["facet_counts"][0].size());
    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ(4, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["stats"].size());
    ASSERT_EQ(4, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    ASSERT_STREQ("gold", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][1]["count"]);

    ASSERT_STREQ("bronze", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][2]["count"]);

    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][3]["count"]);

    // facet with facet count limit
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, {0}, 10, 1,
                                        FREQUENCY, {false}, 10, spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 2).get();
    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("gold", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][1]["count"]);

    // 2 facets, 1 text query with no filters
    facets.clear();
    facets.push_back("tags");
    facets.push_back("name_facet");
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(2, results["facet_counts"].size());

    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_STREQ("name_facet", results["facet_counts"][1]["field_name"].get<std::string>().c_str());

    // facet value must one that's stored, not indexed (i.e. no tokenization/standardization)
    ASSERT_STREQ("Jeremy Howard", results["facet_counts"][1]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(5, (int) results["facet_counts"][1]["counts"][0]["count"]);

    // facet with filters
    facets.clear();
    facets.push_back("tags");
    results = coll_array_fields->search("Jeremy", query_fields, "age: >24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());

    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][3]["count"]);

    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("gold", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("bronze", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());

    // facet with wildcard query
    facets.clear();
    facets.push_back("tags");
    results = coll_array_fields->search("*", query_fields, "age: >24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());

    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][3]["count"]);

    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("gold", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("bronze", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());

    // facet with facet filter query (allows typo correction!)
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, " tags : sliver").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching 2 tokens
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: fxne platim").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>FINE</mark> <mark>PLATIN</mark>UM", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    // facet with facet filter query matching first token of an array
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: fine").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching second token of an array
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: pltinum").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet query on an integer field
    results = coll_array_fields->search("*", query_fields, "", {"age"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "age: 2").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("age", results["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("21", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>2</mark>1", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("24", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>2</mark>4", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());

    // facet on a float field without query to check on stats
    results = coll_array_fields->search("*", query_fields, "", {"rating"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "").get();
    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(4.880199885368347, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(0.0, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(9.99899959564209, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(24.400999426841736, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(5, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    // check for "0" case
    ASSERT_STREQ("0", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    // facet query on a float field
    results = coll_array_fields->search("*", query_fields, "", {"rating"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "rating: 7").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("rating", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("7.812", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>7</mark>.812", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(4.880199885368347, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(0.0, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(9.99899959564209, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(24.400999426841736, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(1, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    // facet query on an array integer field

    results = coll_array_fields->search("*", query_fields, "", {"timestamps"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "timestamps: 142189002").get();
    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_STREQ("timestamps", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("1421890022", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>142189002</mark>2", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(348974822.0, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(1453426022.0, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(13275854664.0, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(1106321222.0, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(1, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    // facet query that does not match any indexed value
    results = coll_array_fields->search("*", query_fields, "", {facets}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, " tags : notfound").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(0, results["facet_counts"][0]["counts"].size());

    // empty facet query value should return all facets without any filtering of facets
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: ").get();

    ASSERT_EQ(5, results["hits"].size());

    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags:").get();

    ASSERT_EQ(5, results["hits"].size());

    // bad facet query syntax
    auto res_op = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                            {false}, Index::DROP_TOKENS_THRESHOLD,
                                            spp::sparse_hash_set<std::string>(),
                                            spp::sparse_hash_set<std::string>(), 10, "foobar");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query must be in the `facet_field: value` format.", res_op.error().c_str());

    // unknown facet field
    res_op = coll_array_fields->search("*", query_fields, "", {"foobar"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "foobar: baz");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a facet field named `foobar` in the schema.", res_op.error().c_str());

    // when facet query is given but no facet fields are specified, must return an error message
    res_op = coll_array_fields->search("*", query_fields, "", {}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: foo");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("The `facet_query` parameter is supplied without a `facet_by` parameter.", res_op.error().c_str());

    // given facet query field must be part of facet fields requested
    res_op = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "name_facet: jeremy");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query refers to a facet field `name_facet` that is not part of `facet_by` parameter.", res_op.error().c_str());

    // facet query with multiple colons should be fine (only first colon will be treate as separator)
    res_op = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags:foo:bar");

    ASSERT_TRUE(res_op.ok());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFacetingTest, FacetCountsBool) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),
                                 field("in_stock", field_types::BOOL, true)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "Ford Mustang";
    doc["points"] = 25;
    doc["in_stock"] = true;

    coll1->add(doc.dump());

    doc["id"] = "101";
    doc["title"] = "Tesla Model S";
    doc["points"] = 40;
    doc["in_stock"] = false;

    coll1->add(doc.dump());

    doc["id"] = "102";
    doc["title"] = "Chevrolet Beat";
    doc["points"] = 10;
    doc["in_stock"] = true;

    coll1->add(doc.dump());

    std::vector<std::string> facets = {"in_stock"};

    nlohmann::json results = coll1->search("*", {"title"}, "in_stock:true", facets, sort_fields, {0}, 10, 1,
                                           token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "in_stock:true").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(1, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    ASSERT_STREQ("in_stock", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("true", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>true</mark>",
                 results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetCountsFloatPrecision) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::FLOAT, true)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "Ford Mustang";
    doc["points"] = 113.4;

    coll1->add(doc.dump());

    std::vector<std::string> facets = {"points"};

    nlohmann::json results = coll1->search("*", {"title"}, "", facets, sort_fields, {0}, 10, 1,
                                           token_ordering::FREQUENCY, {true}).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("points", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("113.4", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("113.4",results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetCountsHighlighting) {
    Collection *coll1;

    std::vector<field> fields = {field("categories", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["categories"] = {"Cell Phones", "Cell Phone Accessories", "Cell Phone Cases & Clips"};
    doc["points"] = 25;

    coll1->add(doc.dump());

    std::vector<std::string> facets = {"categories"};

    nlohmann::json results = coll1->search("phone", {"categories"}, "", facets, sort_fields, {0}, 10, 1,
                                           token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "categories:cell").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(3, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("categories", results["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_STREQ("Cell Phones", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, results["facet_counts"][0]["counts"][1]["count"].get<size_t>());
    ASSERT_STREQ("Cell Phone Accessories", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phone Accessories", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, results["facet_counts"][0]["counts"][2]["count"].get<size_t>());
    ASSERT_STREQ("Cell Phone Cases & Clips", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phone Cases & Clips", results["facet_counts"][0]["counts"][2]["highlighted"].get<std::string>().c_str());

    coll1->remove("100");

    doc["categories"] = {"Cell Phones", "Unlocked Cell Phones", "All Unlocked Cell Phones" };
    coll1->add(doc.dump());

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:cell").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("categories", results["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Cell Phones", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Unlocked Cell Phones", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("Unlocked <mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("All Unlocked Cell Phones", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_STREQ("All Unlocked <mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][2]["highlighted"].get<std::string>().c_str());

    coll1->remove("100");
    doc["categories"] = {"Cell Phones", "Cell Phone Accessories", "Cell Phone Cases & Clips"};
    coll1->add(doc.dump());

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:acces").get();


    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_STREQ("categories", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Cell Phone Accessories", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("Cell Phone <mark>Acces</mark>sories", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    // ensure that query is NOT case sensitive

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:ACCES").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_STREQ("categories", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Cell Phone Accessories", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("Cell Phone <mark>Acces</mark>sories", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    // ensure that only the last token is treated as prefix search

    coll1->remove("100");
    doc["categories"] = {"Cell Phones", "Cell Phone Accessories", "Cellophanes"};
    coll1->add(doc.dump());

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:cell ph").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("Cell Phones", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("Cell Phone Accessories", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    // facet query longer than a token is correctly matched with typo tolerance
    // also ensure that setting per_page = 0 works fine

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, {0}, 0, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:cellx").get();

    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(1, results["found"].get<uint32_t>());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(3, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("<mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cello</mark>phanes", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phone Accessories", results["facet_counts"][0]["counts"][2]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetStatOnFloatFields) {
    Collection *coll_float_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/float_documents.jsonl");
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("score", field_types::FLOAT, false),
            field("average", field_types::FLOAT, true)
    };

    std::vector<sort_by> sort_fields_desc = { sort_by("average", "DESC") };

    coll_float_fields = collectionManager.get_collection("coll_float_fields").get();
    if(coll_float_fields == nullptr) {
        coll_float_fields = collectionManager.create_collection("coll_float_fields", 4, fields, "average").get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        coll_float_fields->add(json_line);
    }

    infile.close();

    query_fields = {"title"};
    std::vector<std::string> facets;
    auto res_op = coll_float_fields->search("Jeremy", query_fields, "", {"average"}, sort_fields_desc, {0}, 10,
                                            1, FREQUENCY, {false});

    auto results = res_op.get();

    ASSERT_EQ(7, results["hits"].size());

    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(-21.3799991607666, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(277.8160007725237, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(39.68800011036053, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(7, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    results = coll_float_fields->search("*", query_fields, "average:>100", {"average"}, sort_fields_desc, {0}, 10,
                                        1, FREQUENCY, {false}).get();

    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(1, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    // facet filter, though should not be considered when calculating facet stats (except total facet values)

    results = coll_float_fields->search("*", query_fields, "", {"average"}, sort_fields_desc, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(),
                                        10, "average: 11").get();

    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(39.68800011036053, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(-21.3799991607666, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(277.8160007725237, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(1, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    collectionManager.drop_collection("coll_float_fields");
}

TEST_F(CollectionFacetingTest, FacetCountOnSimilarStrings) {
    Collection *coll1;

    std::vector<field> fields = {field("categories", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, true)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["categories"] = {"England in India"};
    doc["points"] = 25;

    coll1->add(doc.dump());

    doc["id"] = "101";
    doc["categories"] = {"India in England"};
    doc["points"] = 50;

    coll1->add(doc.dump());

    std::vector<std::string> facets = {"categories"};

    nlohmann::json results = coll1->search("*", {"categories"}, "points:[25, 50]", facets, sort_fields, {0}, 10, 1,
                                           token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("India in England", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("England in India", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetQueryOnStringWithColon) {
    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    Collection* coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();

    nlohmann::json doc;
    doc["id"] = "100";
    doc["title"] = "foo:bar";
    doc["points"] = 25;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("*", {}, "", {"title"}, sort_fields, {0}, 10, 1,
                                           token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "title: foo:ba");

    ASSERT_TRUE(res_op.ok());

    auto results = res_op.get();

    ASSERT_STREQ("foo:bar", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>foo:b</mark>ar", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    results = coll1->search("*", {}, "", {"title"}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "title: ").get();

    ASSERT_STREQ("foo:bar", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("foo:bar", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    results = coll1->search("*", {}, "", {"title"}, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_STREQ("foo:bar", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("foo:bar", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetQueryOnStringArray) {
    Collection* coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("genres", field_types::STRING_ARRAY, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 2, fields, "").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["title"] = "Song 1";
    doc1["genres"] = {"Country Punk Rock", "Country", "Slow"};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["title"] = "Song 2";
    doc2["genres"] = {"Soft Rock", "Rock", "Electronic"};

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["title"] = "Song 3";
    doc3["genres"] = {"Rockabilly", "Metal"};

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["title"] = "Song 4";
    doc4["genres"] = {"Pop Rock", "Rock", "Fast"};

    nlohmann::json doc5;
    doc5["id"] = "4";
    doc5["title"] = "Song 5";
    doc5["genres"] = {"Pop", "Rockabilly", "Fast"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());
    ASSERT_TRUE(coll1->add(doc5.dump()).ok());

    auto results = coll1->search("*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "genres: roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(5, results["facet_counts"][0]["counts"].size());

    results = coll1->search("*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: soft roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());

    results = coll1->search("*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: punk roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("Country <mark>Punk</mark> <mark>Roc</mark>k", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    results = coll1->search("*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: country roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("<mark>Country</mark> Punk <mark>Roc</mark>k", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    // with facet query num typo parameter

    results = coll1->search("*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: eletronic",
                            30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 1).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("<mark>Electroni</mark>c", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    results = coll1->search("*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: eletronic",
                            30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 0).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(0, results["facet_counts"][0]["counts"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetQueryReturnAllCandidates) {
    std::vector<field> fields = {field("title", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    Collection* coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();

    std::vector<std::string> titles = {
        "everest", "evergreen", "everlast", "estrange", "energy", "extra"
    };

    for(size_t i=0; i < titles.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["points"] = i;
        doc["title"] = titles[i];
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto res_op = coll1->search("*", {}, "", {"title"}, sort_fields, {0}, 10, 1,
                                token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "title:e", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                10, {off}, 32767, 32767, 2,
                                false, false);

    ASSERT_TRUE(res_op.ok());

    auto results = res_op.get();
    ASSERT_EQ(6, results["facet_counts"][0]["counts"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetValuesShouldBeNormalized) {
    std::vector<field> fields = {field("brand", field_types::STRING, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::vector<std::vector<std::string>> records = {
        {"BUQU"},
        {"Buqu"},
        {"bu-qu"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["brand"] = records[i][0];
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {},
                                 "", {"brand"}, {}, {2}, 10, 1, FREQUENCY, {true}, 1).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());

    // any document is chosen as representative
    ASSERT_EQ("bu-qu", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetArrayValuesShouldBeNormalized) {
    std::vector<field> fields = {field("brands", field_types::STRING_ARRAY, true),};

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::vector<std::vector<std::string>> records = {
        {"BUQU", "Buqu", "bu-qu"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["brands"] = nlohmann::json::array();

        for(auto& str: records[i]) {
            doc["brands"].push_back(str);
        }

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {},
                                 "", {"brands"}, {}, {2}, 10, 1, FREQUENCY, {true}, 1).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());

    // any document is chosen as representative
    ASSERT_EQ("bu-qu", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetByNestedIntField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object", "optional": false },
          {"name": "company.num_employees", "type": "int32", "optional": false, "facet": true }
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
        "company": {"num_employees": 2000}
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());
    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());

    std::vector<sort_by> sort_fields = { sort_by("details.count", "ASC") };

    auto results = coll1->search("*", {}, "", {"company.num_employees"}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ("company.num_employees", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("2000", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
}

TEST_F(CollectionFacetingTest, FacetOnArrayFieldWithSpecialChars) {
    std::vector<field> fields = {
        field("tags", field_types::STRING_ARRAY, true),
        field("points", field_types::INT32, true),
    };

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["tags"] = {"gamma"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["tags"] = {"alpha", "| . |", "beta", "gamma"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("*", {},
                                 "", {"tags"}, {}, {2}, 10, 1, FREQUENCY, {true}, 1).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(4, results["facet_counts"][0]["counts"].size());

    for(size_t i = 0; i < results["facet_counts"][0]["counts"].size(); i++) {
        auto fvalue = results["facet_counts"][0]["counts"][i]["value"].get<std::string>();
        if(fvalue == "gamma") {
            ASSERT_EQ(2, results["facet_counts"][0]["counts"][i]["count"].get<size_t>());
        } else {
            ASSERT_EQ(1, results["facet_counts"][0]["counts"][i]["count"].get<size_t>());
        }
    }
}
