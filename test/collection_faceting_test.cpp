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

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_faceting";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
        collectionManager.load();
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

    coll_array_fields = collectionManager.get_collection("coll_array_fields");
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
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(3, results["facet_counts"][0].size());
    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ(4, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(0, results["facet_counts"][0]["stats"].size());

    ASSERT_STREQ("gold", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][1]["count"]);

    ASSERT_STREQ("bronze", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][2]["count"]);

    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][3]["count"]);

    // facet with facet count limit
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, 1,
                                        FREQUENCY, false, 10, spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 2).get();
    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);

    ASSERT_STREQ("gold", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][1]["count"]);

    // 2 facets, 1 text filter with no filters
    facets.clear();
    facets.push_back("tags");
    facets.push_back("name_facet");
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

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
    results = coll_array_fields->search("Jeremy", query_fields, "age: >24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

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
    results = coll_array_fields->search("*", query_fields, "age: >24", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

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
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, " tags : sliver").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching 2 tokens
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: fine pltinum").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching first token of an array
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: fine").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching second token of an array
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: pltinum").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet query on an integer field
    results = coll_array_fields->search("*", query_fields, "", {"age"}, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
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
    results = coll_array_fields->search("*", query_fields, "", {"rating"}, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "").get();
    ASSERT_EQ(4, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(4.880199885368347, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(0.0, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(9.99899959564209, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(24.400999426841736, results["facet_counts"][0]["stats"]["sum"].get<double>());

    // facet query on a float field
    results = coll_array_fields->search("*", query_fields, "", {"rating"}, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "rating: 7").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("rating", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("7.812", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>7</mark>.812", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(4, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(4.880199885368347, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(0.0, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(9.99899959564209, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(24.400999426841736, results["facet_counts"][0]["stats"]["sum"].get<double>());

    // facet query on an array integer field

    results = coll_array_fields->search("*", query_fields, "", {"timestamps"}, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "timestamps: 142189002").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_STREQ("timestamps", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("1421890022", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>142189002</mark>2", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(4, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(348974822.0, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(1453426022.0, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(13275854664.0, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(1106321222.0, results["facet_counts"][0]["stats"]["avg"].get<double>());

    // facet query that does not match any indexed value
    results = coll_array_fields->search("*", query_fields, "", {facets}, sort_fields, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, " tags : notfound").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(0, results["facet_counts"][0]["counts"].size());

    // bad facet query syntax
    auto res_op = coll_array_fields->search("*", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                            false, Index::DROP_TOKENS_THRESHOLD,
                                            spp::sparse_hash_set<std::string>(),
                                            spp::sparse_hash_set<std::string>(), 10, "foobar");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query must be in the `facet_field: value` format.", res_op.error().c_str());

    // unknown facet field
    res_op = coll_array_fields->search("*", query_fields, "", {"foobar"}, sort_fields, 0, 10, 1, FREQUENCY,
                                       false, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "foobar: baz");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a facet field named `foobar` in the schema.", res_op.error().c_str());

    // when facet query is given but no facet fields are specified, must return an error message
    res_op = coll_array_fields->search("*", query_fields, "", {}, sort_fields, 0, 10, 1, FREQUENCY,
                                       false, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: foo");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("The `facet_query` parameter is supplied without a `facet_by` parameter.", res_op.error().c_str());

    // given facet query field must be part of facet fields requested
    res_op = coll_array_fields->search("*", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY,
                                       false, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "name_facet: jeremy");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query refers to a facet field `name_facet` that is not part of `facet_by` parameter.", res_op.error().c_str());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionFacetingTest, FacetCountsBool) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),
                                 field("in_stock", field_types::BOOL, true)};

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    coll1 = collectionManager.get_collection("coll1");
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

    nlohmann::json results = coll1->search("*", {"title"}, "in_stock:true", facets, sort_fields, 0, 10, 1,
                                           token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "in_stock:true").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(0, results["facet_counts"][0]["stats"].size());

    ASSERT_STREQ("in_stock", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("true", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>true</mark>",
                 results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionFacetingTest, FacetCountsHighlighting) {
    Collection *coll1;

    std::vector<field> fields = {field("categories", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1");
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    nlohmann::json doc;
    doc["id"] = "100";
    doc["categories"] = {"Cell Phones", "Cell Phone Accessories", "Cell Phone Cases & Clips"};
    doc["points"] = 25;

    coll1->add(doc.dump());

    std::vector<std::string> facets = {"categories"};

    nlohmann::json results = coll1->search("phone", {"categories"}, "", facets, sort_fields, 0, 10, 1,
                                           token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "categories:cell").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(3, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("categories", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Cell Phones", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Cell Phone Accessories", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phone Accessories", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("Cell Phone Cases & Clips", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phone Cases & Clips", results["facet_counts"][0]["counts"][2]["highlighted"].get<std::string>().c_str());

    coll1->remove("100");

    doc["categories"] = {"Cell Phones", "Unlocked Cell Phones", "All Unlocked Cell Phones" };
    coll1->add(doc.dump());

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, 0, 10, 1,
                            token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:cell").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("categories", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Cell Phones", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("All Unlocked Cell Phones", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("All Unlocked <mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Unlocked Cell Phones", results["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());
    ASSERT_STREQ("Unlocked <mark>Cell</mark> Phones", results["facet_counts"][0]["counts"][2]["highlighted"].get<std::string>().c_str());

    coll1->remove("100");
    doc["categories"] = {"Cell Phones", "Cell Phone Accessories", "Cell Phone Cases & Clips"};
    coll1->add(doc.dump());

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, 0, 10, 1,
                            token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:acces").get();


    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_STREQ("categories", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Cell Phone Accessories", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("Cell Phone <mark>Acces</mark>sories", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    // ensure that query is NOT case sensitive

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, 0, 10, 1,
                            token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
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

    results = coll1->search("phone", {"categories"}, "", facets, sort_fields, 0, 10, 1,
                            token_ordering::FREQUENCY, true, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:cell ph").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("Cell Phones", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("Cell Phone Accessories", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

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

    coll_float_fields = collectionManager.get_collection("coll_float_fields");
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
    auto res_op = coll_float_fields->search("Jeremy", query_fields, "", {"average"}, sort_fields_desc, 0, 10,
                                            1, FREQUENCY, false);

    auto results = res_op.get();

    ASSERT_EQ(7, results["hits"].size());

    ASSERT_EQ(4, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(-21.3799991607666, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(277.8160007725237, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(39.68800011036053, results["facet_counts"][0]["stats"]["avg"].get<double>());

    results = coll_float_fields->search("*", query_fields, "average:>100", {"average"}, sort_fields_desc, 0, 10,
                                        1, FREQUENCY, false).get();

    ASSERT_EQ(4, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["sum"].get<double>());

    // facet filter, though should not be considered when calculating facet stats

    results = coll_float_fields->search("*", query_fields, "", {"average"}, sort_fields_desc, 0, 10, 1, FREQUENCY,
                                        false, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(),
                                        10, "average: 11").get();

    ASSERT_EQ(4, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(39.68800011036053, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(-21.3799991607666, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(277.8160007725237, results["facet_counts"][0]["stats"]["sum"].get<double>());

    collectionManager.drop_collection("coll_float_fields");
}
