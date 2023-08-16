#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"


class CollectionOptimizedFacetingTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_optimized_faceting";
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

TEST_F(CollectionOptimizedFacetingTest, FacetCounts) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("name_facet", field_types::STRING, true),
                                 field("age", field_types::INT32, true),
                                 field("years", field_types::INT32_ARRAY, true),
                                 field("rating", field_types::FLOAT, true),
                                 field("timestamps", field_types::INT64_ARRAY, true),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("optional_facet", field_types::INT64_ARRAY, true, true),};

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
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 
                                                        {0}, 10, 1, FREQUENCY, {false}, 1UL, 
                                                        spp::sparse_hash_set<std::string>(),
                                                        spp::sparse_hash_set<std::string>(), 
                                                        10UL, "", 30UL, 4UL, "", 1UL, "", "", {}, 
                                                        3UL, "<mark>", "</mark>", {}, 4294967295UL, true, 
                                                        false, true, "", false, 6000000UL, 4UL, 7UL, fallback,
                                                        4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false, "", true, 
                                                        0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();    

    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(4, results["facet_counts"][0].size());
    ASSERT_EQ("tags", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ(false, results["facet_counts"][0]["sampled"].get<bool>());
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
                                        spp::sparse_hash_set<std::string>(), 2, "", 30UL, 4UL, "", 1UL, 
                                        "", "", {}, 3UL, "<mark>", "</mark>", {}, 4294967295UL, true, 
                                        false, true, "", false, 6000000UL, 4UL, 7UL, fallback, 4UL, {off}, 
                                        32767UL, 32767UL, 2UL, 2UL, false, "", true, 0UL, max_score, 100UL,
                                        0UL, 4294967295UL, VALUE).get();  
    
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
    results = coll_array_fields->search("Jeremy", query_fields, "", facets, sort_fields, 
                                                        {0}, 10, 1, FREQUENCY, {false}, 1UL, 
                                                        spp::sparse_hash_set<std::string>(),
                                                        spp::sparse_hash_set<std::string>(), 
                                                        10UL, "", 30UL, 4UL, "", 1UL, "", "", {}, 
                                                        3UL, "<mark>", "</mark>", {}, 4294967295UL, true, 
                                                        false, true, "", false, 6000000UL, 4UL, 7UL, fallback,
                                                        4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false, "", true, 
                                                        0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();    

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(2, results["facet_counts"].size());

    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_STREQ("name_facet", results["facet_counts"][1]["field_name"].get<std::string>().c_str());

    // facet value must one that's stored, not indexed (i.e. no tokenization/standardization)
    ASSERT_STREQ("Jeremy Howard", results["facet_counts"][1]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(5, (int) results["facet_counts"][1]["counts"][0]["count"]);

    // facet with wildcard
    results = coll_array_fields->search("Jeremy", query_fields, "", {"ag*"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false},  1UL, spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 
                                        10UL, "", 30UL, 4UL, "", 1UL, "", "", {}, 
                                        3UL, "<mark>", "</mark>", {}, 4294967295UL, true, 
                                        false, true, "", false, 6000000UL, 4UL, 7UL, fallback,
                                        4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false, "", true, 
                                        0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("age", results["facet_counts"][0]["field_name"].get<std::string>().c_str());

    // facet on a float field without query to check on stats
    results = coll_array_fields->search("*", query_fields, "", {"rating"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(4.880199885368347, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(0.0, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(9.99899959564209, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(24.400999426841736, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(5, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    // check for "0" case
    ASSERT_STREQ("0", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    facets.clear();
    facets.push_back("tags");

    results = coll_array_fields->search("*", query_fields, "age: >24", facets, sort_fields, {0}, 10, 1, FREQUENCY, 
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

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
                                        spp::sparse_hash_set<std::string>(), 10, " tags : sliver", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching 2 tokens
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: fxne platim", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

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
                                        spp::sparse_hash_set<std::string>(), 10, "tags: fine", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching second token of an array
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: pltinum", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet query on an integer field
    results = coll_array_fields->search("*", query_fields, "", {"age"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "age: 2",30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("age", results["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("24", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>2</mark>4", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("21", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>2</mark>1", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());

     // facet query on a float field
    results = coll_array_fields->search("*", query_fields, "", {"rating"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "rating: 7",30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();
    
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

     // facet with wildcard
    results = coll_array_fields->search("Jeremy", query_fields, "", {"ag*"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();
    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("age", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    
    // empty facet query value should return all facets without any filtering of facets
    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: ", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());

    results = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags:", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());

    // Wildcard facet_by can have partial matches
    results = coll_array_fields->search("*", query_fields, "", {"nam*"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ("name_facet", results["facet_counts"][0]["field_name"].get<std::string>());

    // Wildcard facet_by having no counts should not be returned
    results = coll_array_fields->search("*", query_fields, "", {"optio*"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(0, results["facet_counts"].size());

    results = coll_array_fields->search("*", query_fields, "", {"optional_facet"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ("optional_facet", results["facet_counts"][0]["field_name"].get<std::string>());

    // bad facet query syntax
    auto res_op = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                            {false}, Index::DROP_TOKENS_THRESHOLD,
                                            spp::sparse_hash_set<std::string>(),
                                            spp::sparse_hash_set<std::string>(), 10, "foobar", 30UL, 4UL, 
                                            "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                            4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                            7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                            "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query must be in the `facet_field: value` format.", res_op.error().c_str());

    // unknown facet field
    res_op = coll_array_fields->search("*", query_fields, "", {"foobar"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "foobar: baz",  30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a facet field named `foobar` in the schema.", res_op.error().c_str());

    // only prefix matching is valid
    res_op = coll_array_fields->search("*", query_fields, "", {"*_facet"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "",  30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Only prefix matching with a wildcard is allowed.", res_op.error().c_str());

    // unknown wildcard facet field
    res_op = coll_array_fields->search("*", query_fields, "", {"foo*"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a facet field for `foo*` in the schema.", res_op.error().c_str());

    // when facet query is given but no facet fields are specified, must return an error message
    res_op = coll_array_fields->search("*", query_fields, "", {}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: foo", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("The `facet_query` parameter is supplied without a `facet_by` parameter.", res_op.error().c_str());

    res_op = coll_array_fields->search("*", query_fields, "", {""}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: foo",  30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a facet field named `` in the schema.", res_op.error().c_str());

    // given facet query field must be part of facet fields requested
    res_op = coll_array_fields->search("*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "name_facet: jeremy", 30UL, 4UL, 
                                        "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                        4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                        7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query refers to a facet field `name_facet` that is not part of `facet_by` parameter.", res_op.error().c_str());

    collectionManager.drop_collection("coll_array_fields");
}

TEST_F(CollectionOptimizedFacetingTest, FacetCountsBool) {
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
    doc["title"] = "Ford Mustang GT";
    doc["points"] = 10;
    doc["in_stock"] = true;

    coll1->add(doc.dump());

    std::vector<std::string> facets = {"in_stock"};

    nlohmann::json results = coll1->search("Ford", {"title"}, "", facets, sort_fields, {0}, 10, 1,
                                           token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10,"",  30UL, 4UL, 
                                           "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                           4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                           7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                           "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(1, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());

    ASSERT_STREQ("in_stock", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(2, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("true", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOptimizedFacetingTest, FacetCountsFloatPrecision) {
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
                                           token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10,"",  30UL, 4UL, 
                                           "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                           4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                           7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                           "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("points", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("113.4", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("113.4",results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOptimizedFacetingTest, FacetStatOnFloatFields) {
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
    auto res_op = coll_float_fields->search("Jeremy", query_fields, "", {"average"}, sort_fields_desc, {0}, 10,
                                            1, FREQUENCY, {false}, 10, spp::sparse_hash_set<std::string>(),
                                            spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                            "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                            4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                            7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                            "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    auto results = res_op.get();

    ASSERT_EQ(7, results["hits"].size());

    ASSERT_EQ(5, results["facet_counts"][0]["stats"].size());
    ASSERT_FLOAT_EQ(-21.3799991607666, results["facet_counts"][0]["stats"]["min"].get<double>());
    ASSERT_FLOAT_EQ(300, results["facet_counts"][0]["stats"]["max"].get<double>());
    ASSERT_FLOAT_EQ(277.8160007725237, results["facet_counts"][0]["stats"]["sum"].get<double>());
    ASSERT_FLOAT_EQ(39.68800011036053, results["facet_counts"][0]["stats"]["avg"].get<double>());
    ASSERT_FLOAT_EQ(7, results["facet_counts"][0]["stats"]["total_values"].get<size_t>());
}

TEST_F(CollectionOptimizedFacetingTest, FacetCountOnSimilarStrings) {
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
                                           spp::sparse_hash_set<std::string>(), 10, "", 30UL, 4UL, 
                                            "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {}, 
                                            4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                                            7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                            "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("India in England", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("England in India", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    // facet query
    results = coll1->search("*", {"categories"}, "points:[25, 50]", facets, sort_fields, {0}, 10, 1,
                            token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "categories:india eng", 30UL, 4UL,
                            "", 1UL, "", "", {}, 3UL, "<mark>", "</mark>", {},
                            4294967295UL, true, false, true, "", false, 6000000UL, 4UL,
                            7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                            "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("India in England", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>India</mark> in <mark>Eng</mark>land", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    ASSERT_STREQ("England in India", results["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Eng</mark>land in <mark>India</mark>", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOptimizedFacetingTest, FacetByNestedIntField) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "details", "type": "object", "optional": false },
          {"name": "company.num_employees", "type": "int32", "optional": false, "facet": true },
          {"name": "companyRank", "type": "int32", "optional": false, "facet": true }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    auto doc1 = R"({
        "details": {"count": 1000},
        "company": {"num_employees": 2000},
        "companyRank": 100
    })"_json;

    auto doc2 = R"({
        "details": {"count": 2000},
        "company": {"num_employees": 2000},
        "companyRank": 101
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump(), CREATE).ok());
    ASSERT_TRUE(coll1->add(doc2.dump(), CREATE).ok());

    std::vector<sort_by> sort_fields = { sort_by("details.count", "ASC") };

    auto results = coll1->search("*", {}, "", {"company.num_employees"}, sort_fields, {0}, 10, 1,
                                 token_ordering::FREQUENCY, {true}, 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4,  "", 1UL, "", "", {}, 3UL, 
                                 "<mark>", "</mark>", {}, 4294967295UL, true, false, true, "", false, 6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ("company.num_employees", results["facet_counts"][0]["field_name"]);
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(2, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());
    ASSERT_EQ("2000", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());

    // Nested wildcard faceting
    std::vector<facet> wildcard_facets;
    coll1->parse_facet("company.*", wildcard_facets);

    ASSERT_EQ(1, wildcard_facets.size());
    ASSERT_EQ("company.num_employees", wildcard_facets[0].field_name);

    wildcard_facets.clear();
    coll1->parse_facet("company*", wildcard_facets);

    ASSERT_EQ(2, wildcard_facets.size());
    ASSERT_EQ("company.num_employees", wildcard_facets[0].field_name);
    ASSERT_EQ("companyRank", wildcard_facets[1].field_name);
}

TEST_F(CollectionOptimizedFacetingTest, FacetParseTest){
    std::vector<field> fields = {
            field("score", field_types::INT32, true),
            field("grade", field_types::INT32, true),
            field("rank", field_types::INT32, true),
            field("range", field_types::INT32, true),
            field("scale", field_types::INT32, false),
    };

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    std::vector<std::string> range_facet_fields {
            "score(fail:[0, 40], pass:[40, 100])",
            "grade(A:[80, 100], B:[60, 80], C:[40, 60])"
    };
    std::vector<facet> range_facets;
    for(const std::string & facet_field: range_facet_fields) {
        coll1->parse_facet(facet_field, range_facets);
    }
    ASSERT_EQ(2, range_facets.size());

    ASSERT_STREQ("score", range_facets[0].field_name.c_str());
    ASSERT_TRUE(range_facets[0].is_range_query);
    ASSERT_GT(range_facets[0].facet_range_map.size(), 0);

    ASSERT_STREQ("grade", range_facets[1].field_name.c_str());
    ASSERT_TRUE(range_facets[1].is_range_query);
    ASSERT_GT(range_facets[1].facet_range_map.size(), 0);

    std::vector<std::string> normal_facet_fields {
            "score",
            "grade"
    };
    std::vector<facet> normal_facets;
    for(const std::string & facet_field: normal_facet_fields) {
        coll1->parse_facet(facet_field, normal_facets);
    }
    ASSERT_EQ(2, normal_facets.size());

    ASSERT_STREQ("score", normal_facets[0].field_name.c_str());
    ASSERT_STREQ("grade", normal_facets[1].field_name.c_str());

    std::vector<std::string> wildcard_facet_fields {
            "ran*",
            "sc*",
    };
    std::vector<facet> wildcard_facets;
    for(const std::string & facet_field: wildcard_facet_fields) {
        coll1->parse_facet(facet_field, wildcard_facets);
    }

    ASSERT_EQ(3, wildcard_facets.size());

    std::set<std::string> expected{"range", "rank", "score"};
    for (size_t i = 0; i < wildcard_facets.size(); i++) {
        ASSERT_TRUE(expected.count(wildcard_facets[i].field_name) == 1);
    }

    wildcard_facets.clear();
    coll1->parse_facet("*", wildcard_facets);

    // Last field is not a facet.
    ASSERT_EQ(fields.size() - 1, wildcard_facets.size());

    expected.clear();
    for (size_t i = 0; i < fields.size() - 1; i++) {
        expected.insert(fields[i].name);
    }

    for (size_t i = 0; i < wildcard_facets.size(); i++) {
        ASSERT_TRUE(expected.count(wildcard_facets[i].field_name) == 1);
    }

    std::vector<std::string> mixed_facet_fields {
            "score",
            "grade(A:[80, 100], B:[60, 80], C:[40, 60])",
            "ra*",
    };

    std::vector<facet> mixed_facets;
    for(const std::string & facet_field: mixed_facet_fields) {
        coll1->parse_facet(facet_field, mixed_facets);
    }
    ASSERT_EQ(4, mixed_facets.size());

    std::vector<facet*> mixed_facets_ptr;
    for(auto& f: mixed_facets) {
        mixed_facets_ptr.push_back(&f);
    }

    std::sort(mixed_facets_ptr.begin(), mixed_facets_ptr.end(), [](const facet* f1, const facet* f2) {
        return f1->field_name < f2->field_name;
    });

    ASSERT_EQ("score", mixed_facets_ptr[3]->field_name);

    ASSERT_EQ("grade", mixed_facets_ptr[0]->field_name);
    ASSERT_TRUE(mixed_facets_ptr[0]->is_range_query);
    ASSERT_GT(mixed_facets_ptr[0]->facet_range_map.size(), 0);

    ASSERT_EQ("rank", mixed_facets_ptr[2]->field_name);
    ASSERT_EQ("range", mixed_facets_ptr[1]->field_name);
}

TEST_F(CollectionOptimizedFacetingTest, RangeFacetTest) {
    std::vector<field> fields = {field("place", field_types::STRING, false),
                                 field("state", field_types::STRING, false),
                                 field("visitors", field_types::INT32, true),
                                 field("trackingFrom", field_types::INT32, true),};
    Collection* coll1 = collectionManager.create_collection(
            "coll1", 1, fields, "", 0, "", {}, {}
    ).get();
    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["place"] = "Mysore Palace";
    doc1["state"] = "Karnataka";
    doc1["visitors"] = 235486;
    doc1["trackingFrom"] = 1900;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["place"] = "Hampi";
    doc2["state"] = "Karnataka";
    doc2["visitors"] = 187654;
    doc2["trackingFrom"] = 1900;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["place"] = "Mahabalipuram";
    doc3["state"] = "TamilNadu";
    doc3["visitors"] = 174684;
    doc3["trackingFrom"] = 1900;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["place"] = "Meenakshi Amman Temple";
    doc4["state"] = "TamilNadu";
    doc4["visitors"] = 246676;
    doc4["trackingFrom"] = 2000;

    nlohmann::json doc5;
    doc5["id"] = "4";
    doc5["place"] = "Staue of Unity";
    doc5["state"] = "Gujarat";
    doc5["visitors"] = 345878;
    doc5["trackingFrom"] = 2000;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());
    ASSERT_TRUE(coll1->add(doc5.dump()).ok());

    auto results = coll1->search("Karnataka", {"state"},
                                 "", {"visitors(Busy:[0, 200000], VeryBusy:[200000, 500000])"},
                                 {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000,
                                 true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();
 
    ASSERT_EQ(2, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_EQ("Busy", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_EQ("VeryBusy", results["facet_counts"][0]["counts"][1]["value"].get<std::string>());

    auto results2 = coll1->search("Gujarat", {"state"},
                                  "", {"visitors(Busy:[0, 200000], VeryBusy:[200000, 500000])"},
                                  {}, {2}, 10,
                                  1, FREQUENCY, {true},
                                  10, spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                  "<mark>", "</mark>", {}, 1000,
                                  true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(1, results2["facet_counts"][0]["counts"].size());
    ASSERT_EQ(1, results2["facet_counts"][0]["counts"][0]["count"].get<std::size_t>());
    ASSERT_STREQ("VeryBusy", results2["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_TRUE(results2["facet_counts"][0]["counts"][1]["value"] == nullptr);

    // ensure that unknown facet field are handled

    auto results3 = coll1->search("Gujarat", {"state"},
                             "", {"visitorsz(Busy:[0, 200000], VeryBusy:[200000, 500000])"},
                             {}, {2}, 10,
                             1, FREQUENCY, {true},
                             10, spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                             "<mark>", "</mark>", {}, 1000, true, false, true, "", true,  6000000UL, 4UL,
                             7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false, "", true, 0UL, 
                             max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(results3.ok());
    ASSERT_EQ("Could not find a facet field named `visitorsz` in the schema.", results3.error());

    auto results4 = coll1->search("*", {"state"},
                                  "", {"trackingFrom(Old:[0, 1910], New:[1910, 2100])"},
                                  {}, {2}, 10,
                                  1, FREQUENCY, {true},
                                  10, spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                  "<mark>", "</mark>", {}, 1000, true, false, true, "", true,  6000000UL, 
                                  4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                  "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE).get();

    ASSERT_EQ(2, results4["facet_counts"][0]["counts"].size());
    ASSERT_EQ(3, results4["facet_counts"][0]["counts"][0]["count"].get<std::size_t>());
    ASSERT_EQ("Old", results4["facet_counts"][0]["counts"][0]["value"].get<std::string>());

    ASSERT_EQ(2, results4["facet_counts"][0]["counts"][1]["count"].get<std::size_t>());
    ASSERT_EQ("New", results4["facet_counts"][0]["counts"][1]["value"].get<std::string>());

    // ensure that only integer fields are allowed
    auto rop = coll1->search("Karnataka", {"state"},
                                 "", {"state(Busy:[0, 200000], VeryBusy:[200000, 500000])"},
                                 {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000,
                                 true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(rop.ok());
    ASSERT_EQ("Range facet is restricted to only integer and float fields.", rop.error());

    // ensure that bad facet range values are handled
    rop = coll1->search("Karnataka", {"state"},
                        "", {"visitors(Busy:[alpha, 200000], VeryBusy:[200000, beta])"},
                        {}, {2}, 10,
                        1, FREQUENCY, {true},
                        10, spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                        "<mark>", "</mark>", {}, 1000,
                        true, false, true, "", true,  6000000UL, 
                        4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                        "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_FALSE(rop.ok());
    ASSERT_EQ("Facet range value is not valid.", rop.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOptimizedFacetingTest, RangeFacetContinuity) {
    std::vector<field> fields = {field("place", field_types::STRING, false),
                                 field("state", field_types::STRING, false),
                                 field("visitors", field_types::INT32, true),};
    Collection* coll1 = collectionManager.create_collection(
            "coll1", 1, fields, "", 0, "", {}, {}
    ).get();
    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["place"] = "Mysore Palace";
    doc1["state"] = "Karnataka";
    doc1["visitors"] = 235486;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["place"] = "Hampi";
    doc2["state"] = "Karnataka";
    doc2["visitors"] = 187654;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["place"] = "Mahabalipuram";
    doc3["state"] = "TamilNadu";
    doc3["visitors"] = 174684;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["place"] = "Meenakshi Amman Temple";
    doc4["state"] = "TamilNadu";
    doc4["visitors"] = 246676;

    nlohmann::json doc5;
    doc5["id"] = "4";
    doc5["place"] = "Staue of Unity";
    doc5["state"] = "Gujarat";
    doc5["visitors"] = 345878;


    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());
    ASSERT_TRUE(coll1->add(doc5.dump()).ok());

    auto results = coll1->search("TamilNadu", {"state"},
                                 "", {"visitors(Busy:[0, 200000], VeryBusy:[200001, 500000])"},
                                 {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000,
                                 true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_STREQ("Ranges in range facet syntax should be continous.", results.error().c_str());

    auto results2 = coll1->search("TamilNadu", {"state"},
                                  "", {"visitors(Busy:[0, 200000], VeryBusy:[199999, 500000])"},
                                  {}, {2}, 10,
                                  1, FREQUENCY, {true},
                                  10, spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                  "<mark>", "</mark>", {}, 1000,
                                  true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_STREQ("Ranges in range facet syntax should be continous.", results2.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOptimizedFacetingTest, RangeFacetTypo) {
    std::vector<field> fields = {field("place", field_types::STRING, false),
                                 field("state", field_types::STRING, false),
                                 field("visitors", field_types::INT32, true),};
    Collection* coll1 = collectionManager.create_collection(
            "coll1", 1, fields, "", 0, "", {}, {}
    ).get();
    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["place"] = "Mysore Palace";
    doc1["state"] = "Karnataka";
    doc1["visitors"] = 235486;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["place"] = "Hampi";
    doc2["state"] = "Karnataka";
    doc2["visitors"] = 187654;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["place"] = "Mahabalipuram";
    doc3["state"] = "TamilNadu";
    doc3["visitors"] = 174684;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["place"] = "Meenakshi Amman Temple";
    doc4["state"] = "TamilNadu";
    doc4["visitors"] = 246676;

    nlohmann::json doc5;
    doc5["id"] = "4";
    doc5["place"] = "Staue of Unity";
    doc5["state"] = "Gujarat";
    doc5["visitors"] = 345878;


    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());
    ASSERT_TRUE(coll1->add(doc5.dump()).ok());

    auto results = coll1->search("TamilNadu", {"state"},
                                 "", {"visitors(Busy:[0, 200000], VeryBusy:[200000, 500000)"}, //missing ']' at end
                                 {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 10, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000,
                                 true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_STREQ("Error splitting the facet range values.", results.error().c_str());

    auto results2 = coll1->search("TamilNadu", {"state"},
                                  "", {"visitors(Busy:[0, 200000], VeryBusy:200000, 500000])"}, //missing '[' in second range
                                  {}, {2}, 10,
                                  1, FREQUENCY, {true},
                                  10, spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                  "<mark>", "</mark>", {}, 1000,
                                  true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_STREQ("Error splitting the facet range values.", results2.error().c_str());

    auto results3 = coll1->search("TamilNadu", {"state"},
                                  "", {"visitors(Busy:[0, 200000] VeryBusy:[200000, 500000])"}, //missing ',' between ranges
                                  {}, {2}, 10,
                                  1, FREQUENCY, {true},
                                  10, spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                  "<mark>", "</mark>", {}, 1000,
                                  true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_STREQ("Error splitting the facet range values.", results3.error().c_str());

    auto results4 = coll1->search("TamilNadu", {"state"},
                                  "", {"visitors(Busy:[0 200000], VeryBusy:[200000, 500000])"}, //missing ',' between first ranges values
                                  {}, {2}, 10,
                                  1, FREQUENCY, {true},
                                  10, spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                  "<mark>", "</mark>", {}, 1000,
                                  true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_STREQ("Facet range value is not valid.", results4.error().c_str());

    auto results5 = coll1->search("TamilNadu", {"state"},
                                  "", {"visitors(Busy:[0, 200000 VeryBusy:200000, 500000])"}, //missing '],' and '['
                                  {}, {2}, 10,
                                  1, FREQUENCY, {true},
                                  10, spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 10, {}, {}, {}, 0,
                                  "<mark>", "</mark>", {}, 1000,
                                  true, false, true, "", true,  6000000UL, 
                                 4UL, 7UL, fallback, 4UL, {off}, 32767UL, 32767UL, 2UL, 2UL, false,
                                 "", true, 0UL, max_score, 100UL, 0UL, 4294967295UL, VALUE);

    ASSERT_STREQ("Facet range value is not valid.", results5.error().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOptimizedFacetingTest, SampleFacetCounts) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "color", "type": "string", "facet": true}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::mt19937 gen(137723); // use constant seed to make sure that counts don't jump around
    std::uniform_int_distribution<> distr(1, 100); // 1 to 100 inclusive

    size_t count_blue = 0, count_red = 0;

    for(size_t i = 0; i < 1000; i++) {
        nlohmann::json doc;
        if(distr(gen) % 2 == 0) {
            doc["color"] = "blue";
            count_blue++;
        } else {
            doc["color"] = "red";
            count_red++;
        }

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto res = coll1->search("*", {}, "", {"color"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                             "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 3, 3, 2, 2, false, "", true, 0, max_score, 10, 0, 4294967295UL, VALUE).get();

    ASSERT_EQ(1000, res["found"].get<size_t>());
    ASSERT_EQ(1, res["facet_counts"].size());
    ASSERT_EQ(2, res["facet_counts"][0]["counts"].size());

    // verify approximate counts
    ASSERT_GE(res["facet_counts"][0]["counts"][0]["count"].get<size_t>(), 250);
    ASSERT_GE(res["facet_counts"][0]["counts"][1]["count"].get<size_t>(), 250);
    ASSERT_TRUE(res["facet_counts"][0]["sampled"].get<bool>());

    // when sample threshold is high, don't estimate
    res = coll1->search("*", {}, "", {"color"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                        "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                        4, {off}, 3, 3, 2, 2, false, "", true, 0, max_score, 10, 10000, 4294967295UL, VALUE).get();

    ASSERT_EQ(1000, res["found"].get<size_t>());
    ASSERT_EQ(1, res["facet_counts"].size());
    ASSERT_EQ(2, res["facet_counts"][0]["counts"].size());

    for(size_t i = 0; i < res["facet_counts"][0]["counts"].size(); i++) {
        if(res["facet_counts"][0]["counts"][i]["value"].get<std::string>() == "red") {
            ASSERT_EQ(count_red, res["facet_counts"][0]["counts"][i]["count"].get<size_t>());
        } else {
            ASSERT_EQ(count_blue, res["facet_counts"][0]["counts"][i]["count"].get<size_t>());
        }
    }

    ASSERT_FALSE(res["facet_counts"][0]["sampled"].get<bool>());

    // test for sample percent > 100

    auto res_op = coll1->search("*", {}, "", {"color"}, {}, {0}, 3, 1, FREQUENCY, {true}, 5,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                4, {off}, 3, 3, 2, 2, false, "", true, 0, max_score, 200, 0, 4294967295UL, VALUE);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Value of `facet_sample_percent` must be less than 100.", res_op.error());
}

TEST_F(CollectionOptimizedFacetingTest, FacetOnArrayFieldWithSpecialChars) {
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
                                 "", {"tags"}, {}, {2}, 10, 1, FREQUENCY, {true}, 1, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                4, {off}, 3, 3, 2, 2, false, "", true, 0, max_score, 100, 0, 4294967295UL, VALUE).get();

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

TEST_F(CollectionOptimizedFacetingTest, FacetTestWithDeletedDoc) {
    std::vector<field> fields = {
            field("tags", field_types::STRING_ARRAY, true),
            field("points", field_types::INT32, true),
    };

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;

    doc["id"] = "0";
    doc["tags"] = {"foobar"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["tags"] = {"gamma"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["tags"] = {"beta"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "3";
    doc["tags"] = {"alpha"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    coll1->remove("0");

    auto results = coll1->search("*", {},
                                 "", {"tags"}, {}, {2}, 10, 1, FREQUENCY, {true}, 1, spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 3, 3, 2, 2, false, "", true, 0, max_score, 100, 0, 4294967295UL, VALUE).get();


    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(3, results["facet_counts"][0]["counts"].size());
}

TEST_F(CollectionOptimizedFacetingTest, StringLengthTest) {
    std::vector<field> fields = {
            field("tags", field_types::STRING_ARRAY, true),
            field("points", field_types::INT32, true),
    };

    Collection* coll1 = collectionManager.create_collection("coll1", 1, fields).get();

    nlohmann::json doc;
    doc["tags"] = {"gamma"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["tags"] = {"beta"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["tags"] = {"alpha"};
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    std::string longStr = "";
    for(auto i = 0; i < 8; ++i) {
        longStr+="alphabetagamma";
    }

    ASSERT_TRUE(112 == longStr.size());
    
    std::vector<std::string> vec;
    vec.emplace_back(longStr);
    doc["tags"] = vec;
    doc["points"] = 10;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results = coll1->search("*", {},
                                 "", {"tags"}, {}, {2}, 10, 1, FREQUENCY, {true}, 1, spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 20, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                4, {off}, 3, 3, 2, 2, false, "", true, 0, max_score, 100, 0, 4294967295UL, VALUE).get();


    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(4, results["facet_counts"][0]["counts"].size());

    longStr = results["facet_counts"][0]["counts"][3]["value"];

    //string facet length is restricted to 100
    ASSERT_TRUE(100 == longStr.size());
}
