#include <gtest/gtest.h>
#include <fstream>
#include <vector>
#include <string>
#include "collection_manager.h"
#include "collection.h"
#include "logger.h"


class CollectionMultiSearchAggTest : public testing::Test {
 protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;
    
    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_multi_search_agg";
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


TEST_F(CollectionMultiSearchAggTest, FilteringTest) {
    Collection *coll_array_fields1,*coll_array_fields2;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("age", field_types::INT32, false),
            field("years", field_types::INT32_ARRAY, false),
            field("tags", field_types::STRING_ARRAY, true)
    };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields1 = collectionManager.get_collection("coll_array_fields1").get();
    if(coll_array_fields1 == nullptr) {
        coll_array_fields1 = collectionManager.create_collection("coll_array_fields1", 4, fields, "age").get();
    }

    coll_array_fields2 = collectionManager.get_collection("coll_array_fields2").get();
    if(coll_array_fields2 == nullptr) {
        coll_array_fields2 = collectionManager.create_collection("coll_array_fields2", 4, fields, "age").get();
    }

    std::string json_line;
    size_t counter = 0;

    while (std::getline(infile, json_line)) {
        if(counter % 2 == 0) {
            coll_array_fields1->add(json_line);
        } else {
            coll_array_fields2->add(json_line);
        }
        counter++;
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;
    nlohmann::json results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags: gold", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"2", "0", "1"};


    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags : fine PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

    
    ASSERT_EQ(1, results["hits"].size());

    // using just ":", filtering should return documents that contain ALL tokens in the filter expression
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags : PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(1, results["hits"].size());

    // no documents contain both "white" and "platinum", so
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags : WHITE PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    // with exact match operator (:=) partial matches are not allowed
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags:= PLATINUM", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(0, results["hits"].size());

    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags : bronze", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());



    ids = {"2", "1"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // search with a list of tags, also testing extra padding of space
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags: [bronze,   silver]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());


    ids = {"1", "2", "0", "1"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // need to be exact matches
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags: bronze", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    // when comparators are used, they should be ignored
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags:<bronze", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags:<=BRONZE", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags:>BRONZE", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(2, results["hits"].size());

    // bad filter value (empty)
    auto res_op = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "tags:=", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false});
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Error with filter field `tags`: Filter value cannot be empty.", res_op.error());

    collectionManager.drop_collection("coll_array_fields1");
    collectionManager.drop_collection("coll_array_fields2");
}







TEST_F(CollectionMultiSearchAggTest, FacetingTest) {
    Collection *coll_array_fields1, *coll_array_fields2;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("name_facet", field_types::STRING, true),
                                 field("age", field_types::INT32, true),
                                 field("years", field_types::INT32_ARRAY, true),
                                 field("rating", field_types::FLOAT, true),
                                 field("timestamps", field_types::INT64_ARRAY, true),
                                 field("tags", field_types::STRING_ARRAY, true)};
    
    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };


    coll_array_fields1 = collectionManager.get_collection("coll_array_fields1").get();
    if(coll_array_fields1 == nullptr) {
        coll_array_fields1 = collectionManager.create_collection("coll_array_fields1", 4, fields, "age").get();
    }

    coll_array_fields2 = collectionManager.get_collection("coll_array_fields2").get();
    if(coll_array_fields2 == nullptr) {
        coll_array_fields2 = collectionManager.create_collection("coll_array_fields2", 4, fields, "age").get();
    }

    std::string json_line;
    size_t counter = 0;

    while (std::getline(infile, json_line)) {
        nlohmann::json document = nlohmann::json::parse(json_line);
        document["name_facet"] = document["name"];
        const std::string & patched_json_line = document.dump();
        if(counter % 2 == 0) {
            coll_array_fields1->add(patched_json_line);
        } else {
            coll_array_fields2->add(patched_json_line);
        }
        counter++;
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets = {"tags"};

    // single facet with no filters
    nlohmann::json results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "", facets, sort_fields, {0}, 10, 1,
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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "Jeremy", query_fields, "age: >24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "age: >24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();

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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, " tags : sliver").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("silver", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching 2 tokens
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: fine").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet with facet filter query matching second token of an array
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "tags: pltinum").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(1, (int) results["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("FINE PLATINUM", results["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    // facet query on an integer field
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", {"age"}, sort_fields, {0}, 10, 1, FREQUENCY,
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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", {"rating"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, "").get();
    LOG (INFO) << "results: " << results.dump();
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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", {"rating"}, sort_fields, {0}, 10, 1, FREQUENCY,
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

    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", {"timestamps"}, sort_fields, {0}, 10, 1, FREQUENCY,
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
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", {facets}, sort_fields, {0}, 10, 1, FREQUENCY,
                                        {false}, Index::DROP_TOKENS_THRESHOLD,
                                        spp::sparse_hash_set<std::string>(),
                                        spp::sparse_hash_set<std::string>(), 10, " tags : notfound").get();

    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_STREQ("tags", results["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(0, results["facet_counts"][0]["counts"].size());

    // empty facet query value should return all facets without any filtering of facets
    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: ").get();

    ASSERT_EQ(5, results["hits"].size());

    results = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags:").get();

    ASSERT_EQ(5, results["hits"].size());

    // bad facet query syntax
    auto res_op = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                            {false}, Index::DROP_TOKENS_THRESHOLD,
                                            spp::sparse_hash_set<std::string>(),
                                            spp::sparse_hash_set<std::string>(), 10, "foobar");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query must be in the `facet_field: value` format.", res_op.error().c_str());

    // unknown facet field
    res_op = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", {"foobar"}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "foobar: baz");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Could not find a facet field named `foobar` in the schema.", res_op.error().c_str());

    // when facet query is given but no facet fields are specified, must return an error message
    res_op = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", {}, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags: foo");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("The `facet_query` parameter is supplied without a `facet_by` parameter.", res_op.error().c_str());

    // given facet query field must be part of facet fields requested
    res_op = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "name_facet: jeremy");

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Facet query refers to a facet field `name_facet` that is not part of `facet_by` parameter.", res_op.error().c_str());

    // facet query with multiple colons should be fine (only first colon will be treate as separator)
    res_op = collectionManager.search_multiple_collections({"coll_array_fields1","coll_array_fields2"}, "*", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "tags:foo:bar");

    ASSERT_TRUE(res_op.ok());

    collectionManager.drop_collection("coll_array_fields1");
    collectionManager.drop_collection("coll_array_fields2");
}

TEST_F(CollectionMultiSearchAggTest, VectorSearchTest) {
    nlohmann::json schema1 = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

        nlohmann::json schema2 = R"({
        "name": "coll2",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema1).get();
    Collection* coll2 = collectionManager.create_collection(schema2).get();

    std::vector<std::vector<float>> values = {
        {0.851758, 0.909671, 0.823431, 0.372063},
        {0.97826, 0.933157, 0.39557, 0.306488},
        {0.230606, 0.634397, 0.514009, 0.399594}
    };

    for (size_t i = 0; i < values.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = std::to_string(i) + " title";
        doc["points"] = i;
        doc["vec"] = values[i];
        if(i%2==0) {
            ASSERT_TRUE(coll1->add(doc.dump()).ok());
        } else {
            ASSERT_TRUE(coll2->add(doc.dump()).ok());
        }
    }

    auto results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    ASSERT_FLOAT_EQ(3.409385681152344e-05, results["hits"][0]["vector_distance"].get<float>());
    ASSERT_FLOAT_EQ(0.04329806566238403, results["hits"][1]["vector_distance"].get<float>());
    ASSERT_FLOAT_EQ(0.15141665935516357, results["hits"][2]["vector_distance"].get<float>());

    // with filtering
    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "points:[0,1]", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // validate wrong dimensions in query
    auto res_op = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                          spp::sparse_hash_set<std::string>(),
                                          spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                          "", 10, {}, {}, {}, 0,
                                          "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                          4, {off}, 32767, 32767, 2,
                                          false, true, "vec:([0.96826, 0.94, 0.39557])");

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Query field `vec` must have 4 dimensions.", res_op.error());

    // validate bad vector query field name
    res_op = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                          spp::sparse_hash_set<std::string>(),
                                          spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                          "", 10, {}, {}, {}, 0,
                                          "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                          4, {off}, 32767, 32767, 2,
                                          false, true, "zec:([0.96826, 0.94, 0.39557, 0.4542])");

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Field `zec` does not have a vector query index.", res_op.error());

    // only supported with wildcard queries
    res_op = collectionManager.search_multiple_collections({"coll1","coll2"}, "title", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                           4, {off}, 32767, 32767, 2,
                           false, true, "zec:([0.96826, 0.94, 0.39557, 0.4542])");

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Vector query is supported only on wildcard (q=*) searches.", res_op.error());

    // support num_dim on only float array fields
    schema1 = R"({
        "name": "coll3",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "vec", "type": "float", "num_dim": 4}
        ]
    })"_json;

    auto coll_op = collectionManager.create_collection(schema1);
    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("Property `num_dim` is only allowed on a float array field.", coll_op.error());

    // bad value for num_dim
    schema2 = R"({
        "name": "coll4",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "vec", "type": "float", "num_dim": -4}
        ]
    })"_json;

    coll_op = collectionManager.create_collection(schema2);
    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("Property `num_dim` must be a positive integer.", coll_op.error());

    collectionManager.drop_collection("coll1");
    collectionManager.drop_collection("coll2");
}

TEST_F(CollectionMultiSearchAggTest, FacetQueryOnStringArray) {
    Collection* coll1, *coll2;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("genres", field_types::STRING_ARRAY, true)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 2, fields, "").get();
    }

    coll2 = collectionManager.get_collection("coll2").get();
    if (coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 2, fields, "").get();
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
    ASSERT_TRUE(coll2->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll2->add(doc4.dump()).ok());
    ASSERT_TRUE(coll1->add(doc5.dump()).ok());

    auto results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "genres: roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(5, results["facet_counts"][0]["counts"].size());

    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: soft roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());

    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: punk roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("Country <mark>Punk</mark> <mark>Roc</mark>k", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: country roc").get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("<mark>Country</mark> Punk <mark>Roc</mark>k", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    // with facet query num typo parameter

    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: eletronic",
                            30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 1).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("<mark>Electroni</mark>c", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());

    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "*", {}, "", {"genres"}, sort_fields, {0}, 0, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "genres: eletronic",
                            30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 0).get();

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(0, results["facet_counts"][0]["counts"].size());

    collectionManager.drop_collection("coll1");
    collectionManager.drop_collection("coll2");
}


TEST_F(CollectionMultiSearchAggTest, SortingTest) {
    Collection *coll1, *coll2;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("starring", field_types::STRING, false),
                                 field("points", field_types::INT32, false),
                                 field("cast", field_types::STRING_ARRAY, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
    }

    coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 4, fields, "points").get();
    }

    std::string json_line;
    size_t counter = 0;

    while (std::getline(infile, json_line)) {
        if(counter % 2 == 0) {
            coll1->add(json_line);
        } else {
            coll2->add(json_line);
        }
    }

    infile.close();

    query_fields = {"title"};
    std::vector<std::string> facets;
    sort_fields = { sort_by("points", "ASC") };
    nlohmann::json results = collectionManager.search_multiple_collections({"coll1","coll2"}, "the", query_fields, "", facets, sort_fields, {0}, 15, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(10, results["hits"].size());

    std::vector<std::string> ids = {"17", "13", "10", "4", "0", "1", "8", "6", "16", "11"};


    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // limiting results to just 5, "ASC" keyword must be case insensitive
    sort_fields = { sort_by("points", "asc") };
    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "the", query_fields, "", facets, sort_fields, {0}, 5, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());

    ids = {"17", "13", "10", "4", "0"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // desc

    sort_fields = { sort_by("points", "dEsc") };
    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "the", query_fields, "", facets, sort_fields, {0}, 15, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(10, results["hits"].size());


    ids = {"11", "16", "6", "8", "1", "0", "10", "4", "13", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // With empty list of sort_by fields:
    // should be ordered desc on the default sorting field, since the match score will be the same for all records.
    sort_fields = { };
    results = collectionManager.search_multiple_collections({"coll1","coll2"}, "of", query_fields, "", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(5, results["hits"].size());


    ids = {"11", "12", "5", "4", "17"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    collectionManager.drop_collection("coll");
}