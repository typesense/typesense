#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"
#include "synonym_index.h"
#include "synonym_index_manager.h"
#include "curation_index_manager.h"

class CollectionCurationTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    Collection *coll_mul_fields;
    std::string state_dir_path = "/tmp/typesense_test/collection_curation";

    void setupCollection() {
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
        std::vector<field> fields = {
                field("title", field_types::STRING, false),
                field("starring", field_types::STRING, true),
                field("cast", field_types::STRING_ARRAY, true),
                field("points", field_types::INT32, false)
        };

        CurationIndexManager& curation_index_manager = CurationIndexManager::get_instance();
        curation_index_manager.init_store(store);

        CurationIndex curation_index1(store, "index");
        curation_index_manager.add_curation_index("index", std::move(curation_index1));

        coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
        if(coll_mul_fields == nullptr) {
            coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
            coll_mul_fields->set_curation_sets({"index"});
        }

        std::string json_line;

        while (std::getline(infile, json_line)) {
            coll_mul_fields->add(json_line);
        }

        infile.close();
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        SynonymIndexManager::get_instance().dispose();
        CurationIndexManager::get_instance().dispose();
        collectionManager.drop_collection("coll_mul_fields");
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionCurationTest, ExcludeIncludeExactQueryMatch) {
    Config::get_instance().set_enable_search_analytics(true);

    auto& ov_manager = CurationIndexManager::get_instance();

    nlohmann::json curation_json = {
            {"id",   "exclude-rule"},
            {
             "rule", {
                             {"query", "of"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            }
    };
    curation_json["excludes"] = nlohmann::json::array();
    curation_json["excludes"][0] = nlohmann::json::object();
    curation_json["excludes"][0]["id"] = "4";

    curation_json["excludes"][1] = nlohmann::json::object();
    curation_json["excludes"][1]["id"] = "11";

    curation_t curation;
    curation_t::parse(curation_json, "", curation);

    ov_manager.upsert_curation_item("index", curation_json);

    std::vector<std::string> facets = {"cast"};

    Option<nlohmann::json> res_op = coll_mul_fields->search("of", {"title"}, "", facets, {}, {0}, 10);
    ASSERT_TRUE(res_op.ok());
    nlohmann::json results = res_op.get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(3, results["found"].get<uint32_t>());
    ASSERT_EQ(6, results["facet_counts"][0]["counts"].size());

    ASSERT_STREQ("12", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("5", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("17", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // include
    nlohmann::json curation_json_include = {
            {"id",   "include-rule"},
            {
             "rule", {
                             {"query", "in"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            }
    };
    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "0";
    curation_json_include["includes"][0]["position"] = 1;

    curation_json_include["includes"][1] = nlohmann::json::object();
    curation_json_include["includes"][1]["id"] = "3";
    curation_json_include["includes"][1]["position"] = 2;

    ov_manager.upsert_curation_item("index", curation_json_include);

    res_op = coll_mul_fields->search("in", {"title"}, "", {}, {}, {0}, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(3, results["found"].get<uint32_t>());
    ASSERT_FALSE(results.contains("metadata"));

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("13", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // curated results should be marked as such
    ASSERT_EQ(true, results["hits"][0]["curated"].get<bool>());
    ASSERT_EQ(true, results["hits"][1]["curated"].get<bool>());
    ASSERT_EQ(0, results["hits"][2].count("curated"));

    ov_manager.delete_curation_item("index", "exclude-rule");
    ov_manager.delete_curation_item("index", "include-rule");

    // contains cases

    nlohmann::json curation_contains_inc = {
            {"id",   "include-rule"},
            {
             "rule", {
                             {"query", "will"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            }
    };
    curation_contains_inc["includes"] = nlohmann::json::array();
    curation_contains_inc["includes"][0] = nlohmann::json::object();
    curation_contains_inc["includes"][0]["id"] = "0";
    curation_contains_inc["includes"][0]["position"] = 1;

    curation_contains_inc["includes"][1] = nlohmann::json::object();
    curation_contains_inc["includes"][1]["id"] = "1";
    curation_contains_inc["includes"][1]["position"] = 7;  // purposely setting it way out

    ov_manager.upsert_curation_item("index", curation_contains_inc);

    res_op = coll_mul_fields->search("will smith", {"title"}, "", {}, {}, {0}, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(4, results["found"].get<uint32_t>());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][3]["document"]["id"].get<std::string>().c_str());

    // partial word should not match
    res_op = coll_mul_fields->search("dowillow", {"title"}, "", {}, {}, {0}, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(0, results["hits"].size());
    ASSERT_EQ(0, results["found"].get<uint32_t>());

    // ability to disable curations
    bool enable_curations = false;
    res_op = coll_mul_fields->search("will", {"title"}, "", {}, {}, {0}, 10,
                                     1, FREQUENCY, {false}, 0, spp::sparse_hash_set<std::string>(),
                                     spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 0, {}, {}, {}, 0,
                                     "<mark>", "</mark>", {1}, 10000, true, false, enable_curations);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<uint32_t>());

    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    enable_curations = true;
    res_op = coll_mul_fields->search("will", {"title"}, "", {}, {}, {0}, 10,
                                     1, FREQUENCY, {false}, 0, spp::sparse_hash_set<std::string>(),
                                     spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 0, {}, {}, {}, 0,
                                     "<mark>", "</mark>", {1}, 10000, true, false, enable_curations);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(4, results["found"].get<uint32_t>());

    ov_manager.delete_curation_item("index", "include-rule");
    Config::get_instance().set_enable_search_analytics(false);
}

TEST_F(CollectionCurationTest, OverrideJSONValidation) {
    nlohmann::json exclude_json = {
            {"id", "exclude-rule"},
            {
             "rule", {
                       {"query", "of"},
                       {"match", curation_t::MATCH_EXACT}
                   }
            }
    };

    auto& ov_manager = CurationIndexManager::get_instance();

    exclude_json["excludes"] = nlohmann::json::array();
    exclude_json["excludes"][0] = nlohmann::json::object();
    exclude_json["excludes"][0]["id"] = 11;

    curation_t curation1;
    auto parse_op = curation_t::parse(exclude_json, "", curation1);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Exclusion `id` must be a string.", parse_op.error().c_str());

    nlohmann::json include_json = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            }
    };

    include_json["includes"] = nlohmann::json::array();
    include_json["includes"][0] = nlohmann::json::object();
    include_json["includes"][0]["id"] = "11";

    curation_t curation2;
    parse_op = curation_t::parse(include_json, "", curation2);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Inclusion definition must define both `id` and `position` keys.", parse_op.error().c_str());

    include_json["includes"][0]["position"] = "1";

    parse_op = curation_t::parse(include_json, "", curation2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Inclusion `position` must be an integer.", parse_op.error().c_str());

    include_json["includes"][0]["position"] = 1;
    parse_op = curation_t::parse(include_json, "", curation2);
    ASSERT_TRUE(parse_op.ok());

    nlohmann::json include_json2 = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            }
    };

    parse_op = curation_t::parse(include_json2, "", curation2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Must contain one of: `includes`, `excludes`, `metadata`, `filter_by`, `sort_by`, "
                 "`remove_matched_tokens`, `replace_query`.", parse_op.error().c_str());

    include_json2["includes"] = nlohmann::json::array();
    include_json2["includes"][0] = 100;

    parse_op = curation_t::parse(include_json2, "", curation2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("The `includes` value must be an array of objects.", parse_op.error().c_str());

    nlohmann::json exclude_json2 = {
            {"id", "exclude-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            }
    };

    exclude_json2["excludes"] = nlohmann::json::array();
    exclude_json2["excludes"][0] = "100";

    parse_op = curation_t::parse(exclude_json2, "", curation2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("The `excludes` value must be an array of objects.", parse_op.error().c_str());
}

TEST_F(CollectionCurationTest, IncludeHitsFilterOverrides) {
    // Check facet field highlight for overridden results
    nlohmann::json curation_json_include = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "not-found"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            },
            {"metadata", {{"foo", "bar"}}},
    };
    auto& ov_manager = CurationIndexManager::get_instance();

    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "0";
    curation_json_include["includes"][0]["position"] = 1;

    curation_json_include["includes"][1] = nlohmann::json::object();
    curation_json_include["includes"][1]["id"] = "2";
    curation_json_include["includes"][1]["position"] = 2;

    curation_json_include["filter_curated_hits"] = true;

    ov_manager.upsert_curation_item("index", curation_json_include);

    auto curations = ov_manager.list_curation_items("index", 0, 0).get();
    ASSERT_EQ(1, curations.size());
    auto curation_json = curations[0];
    ASSERT_TRUE(curation_json.contains("filter_curated_hits"));
    ASSERT_TRUE(curation_json["filter_curated_hits"].get<bool>());

    auto results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("bar", results["metadata"]["foo"].get<std::string>());

    // disable filter curation option
    curation_json_include["filter_curated_hits"] = false;
    ov_manager.upsert_curation_item("index", curation_json_include);
    results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ(2, results["hits"].size());

    // remove filter curation option: by default no filtering should be done
    curation_json_include.erase("filter_curated_hits");
    ov_manager.upsert_curation_item("index", curation_json_include);
    results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ(2, results["hits"].size());

    // query param configuration should take precedence over curation level config
    results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "",
                                      30, 5,
                                      "", 10, {}, {}, {}, 0,
                                      "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                      4, {off}, 32767, 32767, 2, 1).get();

    ASSERT_EQ(1, results["hits"].size());

    // try disabling and overriding

    curation_json_include["filter_curated_hits"] = false;
    ov_manager.upsert_curation_item("index", curation_json_include);

    results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "",
                                      30, 5,
                                      "", 10, {}, {}, {}, 0,
                                      "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                      4, {off}, 32767, 32767, 2, 1).get();

    ASSERT_EQ(1, results["hits"].size());

    // try enabling and overriding
    curation_json_include["filter_curated_hits"] = true;
    ov_manager.upsert_curation_item("index", curation_json_include);

    results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "",
                                      30, 5,
                                      "", 10, {}, {}, {}, 0,
                                      "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                      4, {off}, 32767, 32767, 2, 0).get();

    ASSERT_EQ(1, results["hits"].size());

}

TEST_F(CollectionCurationTest, ExcludeIncludeFacetFilterQuery) {
    // Check facet field highlight for overridden results
    nlohmann::json curation_json_include = {
        {"id", "include-rule"},
        {
         "rule", {
                   {"query", "not-found"},
                   {"match", curation_t::MATCH_EXACT}
               }
        }
    };
    auto& ov_manager = CurationIndexManager::get_instance();

    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "0";
    curation_json_include["includes"][0]["position"] = 1;

    curation_json_include["includes"][1] = nlohmann::json::object();
    curation_json_include["includes"][1]["id"] = "2";
    curation_json_include["includes"][1]["position"] = 2;

    ov_manager.upsert_curation_item("index", curation_json_include);

    auto curations = ov_manager.list_curation_items("index", 0, 0).get();
    ASSERT_EQ(1, curations.size());
    auto curation_json = curations[0];
    ASSERT_FALSE(curation_json.contains("filter_by"));
    ASSERT_TRUE(curation_json.contains("remove_matched_tokens"));
    ASSERT_TRUE(curation_json.contains("filter_curated_hits"));
    ASSERT_FALSE(curation_json["remove_matched_tokens"].get<bool>());
    ASSERT_FALSE(curation_json["filter_curated_hits"].get<bool>());

    auto results = coll_mul_fields->search("not-found", {"title"}, "", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ("<mark>Will</mark> Ferrell", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());
    ASSERT_EQ("Will Ferrell", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    ov_manager.delete_curation_item("index", "include-rule");

    // facet count is okay when results are excluded
    nlohmann::json curation_json_exclude = {
        {"id",   "exclude-rule"},
        {
         "rule", {
                     {"query", "the"},
                     {"match", curation_t::MATCH_EXACT}
                 }
        }
    };
    curation_json_exclude["excludes"] = nlohmann::json::array();
    curation_json_exclude["excludes"][0] = nlohmann::json::object();
    curation_json_exclude["excludes"][0]["id"] = "10";


    ov_manager.upsert_curation_item("index", curation_json_exclude);

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: scott").get();

    ASSERT_EQ(9, results["found"].get<size_t>());

    // "count" would be `2` without exclusion
    ASSERT_EQ("<mark>Scott</mark> Glenn", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    ASSERT_EQ("Kristin <mark>Scott</mark> Thomas", results["facet_counts"][0]["counts"][1]["highlighted"].get<std::string>());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][1]["count"].get<size_t>());

    // ensure per_page is respected
    // first with per_page = 0
    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 0, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: scott").get();

    ASSERT_EQ(9, results["found"].get<size_t>());
    ASSERT_EQ(0, results["hits"].size());

    ov_manager.delete_curation_item("index", "exclude-rule");

    // now with per_page = 1, and an include query

    ov_manager.upsert_curation_item("index", curation_json_include);
    results = coll_mul_fields->search("not-found", {"title"}, "", {"starring"}, {}, {0}, 1, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // should be able to replace existing curation
    curation_json_include["rule"]["query"] = "found";
    ov_manager.upsert_curation_item("index", curation_json_include);
    ASSERT_STREQ("found", ov_manager.list_curation_items("index", 0, 0).get()[0]["rule"]["query"].get<std::string>().c_str());

    ov_manager.delete_curation_item("index", "include-rule");
}

TEST_F(CollectionCurationTest, FilterCuratedHitsSlideToCoverMissingSlots) {
    // when some of the curated hits are filtered away, lower ranked hits must be pulled up
    nlohmann::json curation_json_include = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "scott"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            }
    };

    auto& ov_manager = CurationIndexManager::get_instance();

    // first 2 hits won't match the filter, 3rd position should float up to position 1
    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "7";
    curation_json_include["includes"][0]["position"] = 1;

    curation_json_include["includes"][1] = nlohmann::json::object();
    curation_json_include["includes"][1]["id"] = "17";
    curation_json_include["includes"][1]["position"] = 2;

    curation_json_include["includes"][2] = nlohmann::json::object();
    curation_json_include["includes"][2]["id"] = "10";
    curation_json_include["includes"][2]["position"] = 3;

    curation_json_include["filter_curated_hits"] = true;

    ov_manager.upsert_curation_item("index", curation_json_include);

    auto results = coll_mul_fields->search("scott", {"starring"}, "points:>55", {}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("10", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("11", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("12", results["hits"][2]["document"]["id"].get<std::string>());

    // another curation where there is an ID missing in the middle
    curation_json_include = {
        {"id", "include-rule"},
        {
         "rule", {
                   {"query", "glenn"},
                   {"match", curation_t::MATCH_EXACT}
               }
        }
    };

    // middle hit ("10") will not satisfy filter, so "11" will move to position 2
    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "9";
    curation_json_include["includes"][0]["position"] = 1;

    curation_json_include["includes"][1] = nlohmann::json::object();
    curation_json_include["includes"][1]["id"] = "10";
    curation_json_include["includes"][1]["position"] = 2;

    curation_json_include["includes"][2] = nlohmann::json::object();
    curation_json_include["includes"][2]["id"] = "11";
    curation_json_include["includes"][2]["position"] = 3;

    curation_json_include["filter_curated_hits"] = true;

    ov_manager.upsert_curation_item("index", curation_json_include);

    results = coll_mul_fields->search("glenn", {"starring"}, "points:[43,86]", {}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("9", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("11", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, SimpleOverrideStopProcessing) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["price"] = 399.99;
    doc1["points"] = 30;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Fast Joggers";
    doc2["price"] = 49.99;
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Comfortable Sneakers";
    doc3["price"] = 19.99;
    doc3["points"] = 1;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json_include = {
            {"id", "include-rule-1"},
            {
             "rule", {
                           {"query", "shoes"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            },
            {"stop_processing", false}
    };

    // first 2 hits won't match the filter, 3rd position should float up to position 1
    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "2";
    curation_json_include["includes"][0]["position"] = 1;

    curation_t curation_include1;
    auto op = curation_t::parse(curation_json_include, "include-rule-1", curation_include1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json_include);

    curation_json_include["id"] = "include-rule-2";
    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "1";
    curation_json_include["includes"][0]["position"] = 2;

    curation_t curation_include2;
    op = curation_t::parse(curation_json_include, "include-rule-2", curation_include2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json_include);

    auto results = coll1->search("shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    // now with stop processing enabled for the first rule
    curation_json_include = {
        {"id", "include-rule-1"},
        {
            "rule", {
                               {"query", "shoes"},
                               {"match", curation_t::MATCH_EXACT}
            }
        },
        {"stop_processing", true}
    };
    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "2";
    curation_json_include["includes"][0]["position"] = 1;
    ov_manager.upsert_curation_item("index", curation_json_include);

    results = coll1->search("shoes", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // check that default value for stop_processing is true

    nlohmann::json curation_json_test = {
        {"id", "include-rule-test"},
        {
         "rule", {
                   {"query", "fast"},
                   {"match", curation_t::MATCH_CONTAINS}
               }
        },
    };

    curation_json_test["includes"] = nlohmann::json::array();
    curation_json_test["includes"][0] = nlohmann::json::object();
    curation_json_test["includes"][0]["id"] = "2";
    curation_json_test["includes"][0]["position"] = 1;

    curation_t curation_include_test;
    op = curation_t::parse(curation_json_test, "include-rule-test", curation_include_test);
    ASSERT_TRUE(op.ok());
    ASSERT_TRUE(curation_include_test.stop_processing);
}

TEST_F(CollectionCurationTest, IncludeOverrideWithFilterBy) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["price"] = 399.99;
    doc1["points"] = 30;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Fast Shoes";
    doc2["price"] = 49.99;
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Comfortable Shoes";
    doc3["price"] = 199.99;
    doc3["points"] = 1;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json_include = {
            {"id", "include-rule-1"},
            {
             "rule", {
                           {"query", "shoes"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            },
            {"filter_curated_hits", false},
            {"stop_processing", false},
            {"remove_matched_tokens", false},
            {"filter_by", "price: >55"}
    };

    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "2";
    curation_json_include["includes"][0]["position"] = 1;

    curation_t curation_include1;
    auto op = curation_t::parse(curation_json_include, "include-rule-1", curation_include1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json_include);

    auto results = coll1->search("shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // when filter by does not match any result, curated result should still show up
    // because `filter_curated_hits` is false
    results = coll1->search("shoes", {"name"}, "points:1000",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // when bad filter by clause is used in curation
    curation_json_include = {
            {"id", "include-rule-2"},
            {
             "rule", {
                           {"query", "test"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            },
            {"filter_curated_hits", false},
            {"stop_processing", false},
            {"remove_matched_tokens", false},
            {"filter_by", "price >55"}
    };

    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "2";
    curation_json_include["includes"][0]["position"] = 1;

    curation_t curation_include2;
    op = curation_t::parse(curation_json_include, "include-rule-2", curation_include2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json_include);

    results = coll1->search("random-name", {"name"}, "",
                             {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionCurationTest, ReplaceQuery) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["points"] = 30;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Fast Shoes";
    doc2["points"] = 50;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Comfortable Socks";
    doc3["points"] = 1;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "boots",
            "match": "exact"
        },
        "replace_query": "shoes"
    })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("boots", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // don't allow both remove_matched_tokens and replace_query
    curation_json["remove_matched_tokens"] = true;
    op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Only one of `replace_query` or `remove_matched_tokens` can be specified.", op.error());

    // it's okay when it's explicitly set to false
    curation_json["remove_matched_tokens"] = false;
    op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
}

TEST_F(CollectionCurationTest, ReplaceWildcardQueryWithKeyword) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["points"] = 30;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Fast Shoes";
    doc2["points"] = 50;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Comfortable Socks";
    doc3["points"] = 1;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "*",
            "match": "exact"
        },
        "replace_query": "shoes"
    })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("*", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

     // should return an error message when query_by is not sent
    auto res_op = coll1->search("*", {}, "", {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Missing `query_by` parameter.", res_op.error());
}

TEST_F(CollectionCurationTest, BothFilterByAndQueryMatch) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    auto schema = R"({
            "name": "coll1",
            "enable_nested_fields": true,
            "fields": [
                 {"name": "title", "type": "string"},
                 {"name": "storiesIds", "type": "object[]"}
            ]
        })"_json;

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection(schema).get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1 = R"({
       "id": "16b2e68b-b0a0-4b6f-aada-403277b5df7b",
       "title": "First document in curation",
       "storiesIds": [{"id": "a94f4198-c22d-4a67-9993-370f69243cc9"}]
    })"_json;

    nlohmann::json doc2 = R"({
       "id": "ff62dbec-7510-4688-9186-d89106e6566f",
       "title": "Second document in curation",
       "storiesIds": [{"id": "a94f4198-c22d-4a67-9993-370f69243cc9"}]
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    // additional documents with same story ID
    nlohmann::json docN;
    docN["title"] = "Additional document";
    docN["storiesIds"] = nlohmann::json::array();
    docN["storiesIds"][0] = nlohmann::json::object();
    docN["storiesIds"][0]["id"] = "a94f4198-c22d-4a67-9993-370f69243cc9";

    for(size_t i = 0; i < 5; i++) {
        docN["id"] = "id" + std::to_string(i);
        ASSERT_TRUE(coll1->add(docN.dump()).ok());
    }

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC") };

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
         "query": "*",
         "match": "exact",
         "filter_by": "storiesIds.id:=[a94f4198-c22d-4a67-9993-370f69243cc9]"
       },
       "includes": [
         {"id": "16b2e68b-b0a0-4b6f-aada-403277b5df7b", "position": 1},
         {"id": "ff62dbec-7510-4688-9186-d89106e6566f", "position": 2}
       ],
       "filter_curated_hits": true,
       "stop_processing": true
     })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("*", {}, "storiesIds.id:=[a94f4198-c22d-4a67-9993-370f69243cc9]",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(7, results["hits"].size());
    ASSERT_EQ("16b2e68b-b0a0-4b6f-aada-403277b5df7b", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("ff62dbec-7510-4688-9186-d89106e6566f", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, RuleQueryMustBeCaseInsensitive) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["points"] = 30;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Tennis Ball";
    doc2["points"] = 50;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Golf Ball";
    doc3["points"] = 1;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "GrEat",
            "match": "contains"
        },
        "replace_query": "amazing"
    })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    curation_json = R"({
       "id": "rule-2",
       "rule": {
            "query": "BaLL",
            "match": "contains"
        },
        "filter_by": "points: 1"
    })"_json;

    curation_t curation_rule2;
    op = curation_t::parse(curation_json, "rule-2", curation_rule2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("great shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("ball", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, RuleQueryWithAccentedChars) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("color", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Green";
    doc1["color"] = "Green";
    doc1["points"] = 30;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "Grün",
            "match": "contains"
        },
        "filter_by":"color:green",
        "filter_curated_hits":true
    })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("grün", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, WindowForRule) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["points"] = 30;
    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "boots",
            "match": "exact"
        },
        "replace_query": "shoes"
    })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("boots", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // rule must not match when window_start is set into the future
    curation_json["effective_from_ts"] = 35677971263;  // year 3100, here we come! ;)
    op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("boots", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // rule must not match when window_end is set into the past
    curation_json["effective_from_ts"] = -1;
    curation_json["effective_to_ts"] = 965388863;
    op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("boots", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // resetting both should bring the curation back in action
    curation_json["effective_from_ts"] = 965388863;
    curation_json["effective_to_ts"] = 35677971263;
    op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("boots", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionCurationTest, FilterRule) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["points"] = 30;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Fast Shoes";
    doc2["points"] = 50;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Comfortable Socks";
    doc3["points"] = 1;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "*",
            "match": "exact",
            "filter_by": "points: 50"
        },
        "includes": [{
            "id": "0",
            "position": 1
        }]
    })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("*", {}, "points: 50",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    // empty query should not trigger curation even though it will be deemed as wildcard search
    results = coll1->search("", {"name"}, "points: 50",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // check to_json
    nlohmann::json curation_json_ser = curation_rule.to_json();
    ASSERT_EQ("points: 50", curation_json_ser["rule"]["filter_by"]);

    // without q/match
    curation_json = R"({
       "id": "rule-2",
       "rule": {
            "filter_by": "points: 1"
        },
        "includes": [{
            "id": "0",
            "position": 1
        }]
    })"_json;

    curation_t curation_rule2;
    op = curation_t::parse(curation_json, "rule-2", curation_rule2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("socks", {"name"}, "points: 1",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    curation_json_ser = curation_rule2.to_json();
    ASSERT_EQ("points: 1", curation_json_ser["rule"]["filter_by"]);
    ASSERT_EQ(0, curation_json_ser["rule"].count("query"));
    ASSERT_EQ(0, curation_json_ser["rule"].count("match"));
}

TEST_F(CollectionCurationTest, CurationGroupingNonCuratedHitsShouldNotAppearOutside) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("group_id", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 3, fields).get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc;
    doc["id"] = "1";
    doc["title"] = "The Harry Potter 1";
    doc["group_id"] = "hp";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["title"] = "The Harry Potter 2";
    doc["group_id"] = "hp";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "3";
    doc["title"] = "Lord of the Rings";
    doc["group_id"] = "lotr";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    nlohmann::json curation_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "*",
            "match": "exact"
        },
        "includes": [{
            "id": "2",
            "position": 1
        }]
    })"_json;

    curation_t curation_rule;
    auto op = curation_t::parse(curation_json, "rule-1", curation_rule);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    curation_json = R"({
       "id": "rule-2",
       "rule": {
            "query": "the",
            "match": "exact"
        },
        "includes": [{
            "id": "2",
            "position": 1
        }]
    })"_json;

    curation_t curation_rule2;
    op = curation_t::parse(curation_json, "rule-2", curation_rule2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("*", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10,
                                 "", {}, {"group_id"}, 2).get();

    // when only one of the 2 records belonging to a group is used for curation, the other record
    // should also appear

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_EQ(2, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    // same for keyword search
    results = coll1->search("the", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10,
                            "", {}, {"group_id"}, 2).get();

    // when only one of the 2 records belonging to a group is used for curation, the other record
    // should also appear

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_EQ(2, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, PinnedAndHiddenHits) {
    auto pinned_hits = "13:1,4:2";
    auto& ov_manager = CurationIndexManager::get_instance();

    // basic pinning

    auto results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 50, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                           "", 10,
                                           pinned_hits, {}).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_STREQ("13", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("16", results["hits"][3]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("6", results["hits"][4]["document"]["id"].get<std::string>().c_str());

    // pinning + filtering
    results = coll_mul_fields->search("of", {"title"}, "points:>58", {}, {}, {0}, 50, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                      "", 10,
                                      pinned_hits, {}).get();

    ASSERT_EQ(5, results["found"].get<size_t>());
    ASSERT_STREQ("13", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("12", results["hits"][3]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("5", results["hits"][4]["document"]["id"].get<std::string>().c_str());

    // pinning + filtering with filter_curated_hits: true
    pinned_hits = "14:1,4:2";

    results = coll_mul_fields->search("of", {"title"}, "points:>58", {}, {}, {0}, 50, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                      "", 10, pinned_hits, {}, {}, 0,
                                      "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                      4, {off}, 32767, 32767, 2, 1).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_STREQ("14", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("12", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("5", results["hits"][3]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ("The Silence <mark>of</mark> the Lambs", results["hits"][1]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("Confessions <mark>of</mark> a Shopaholic", results["hits"][2]["highlights"][0]["snippet"].get<std::string>());
    ASSERT_EQ("Percy Jackson: Sea <mark>of</mark> Monsters", results["hits"][3]["highlights"][0]["snippet"].get<std::string>());

    // both pinning and hiding

    pinned_hits = "13:1,4:2";
    std::string hidden_hits="11,16";
    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 50, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                      "", 10,
                                      pinned_hits, hidden_hits).get();

    ASSERT_STREQ("13", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("6", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // paginating such that pinned hits appear on second page
    pinned_hits = "13:4,4:5";

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 2, 2, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                      "", 10,
                                      pinned_hits, hidden_hits).get();

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("13", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // take precedence over curation rules

    nlohmann::json curation_json_include = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "the"},
                           {"match", curation_t::MATCH_EXACT}
                   }
            }
    };

    // trying to include an ID that is also being hidden via `hidden_hits` query param will not work
    // as pinned and hidden hits will take precedence over curation rules
    curation_json_include["includes"] = nlohmann::json::array();
    curation_json_include["includes"][0] = nlohmann::json::object();
    curation_json_include["includes"][0]["id"] = "11";
    curation_json_include["includes"][0]["position"] = 2;

    curation_json_include["includes"][1] = nlohmann::json::object();
    curation_json_include["includes"][1]["id"] = "8";
    curation_json_include["includes"][1]["position"] = 1;

    curation_t curation_include;
    curation_t::parse(curation_json_include, "", curation_include);

    ov_manager.upsert_curation_item("index", curation_json_include);

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 50, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                      "", 10,
                                      {}, {hidden_hits}).get();

    ASSERT_EQ(8, results["found"].get<size_t>());
    ASSERT_STREQ("8", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("6", results["hits"][1]["document"]["id"].get<std::string>().c_str());
}

TEST_F(CollectionCurationTest, PinnedHitsSmallerThanPageSize) {
    auto pinned_hits = "17:1,13:4,11:3";
    auto& ov_manager = CurationIndexManager::get_instance();

    // pinned hits larger than page size: check that pagination works

    // without curations:
    // 11, 16, 6, 8, 1, 0, 10, 4, 13, 17

    auto results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 8, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                           "", 10,
                                           pinned_hits, {}).get();

    std::vector<size_t> expected_ids_p1 = {17, 16, 11, 13, 6, 8, 1, 0};

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(8, results["hits"].size());

    for(size_t i=0; i<8; i++) {
        ASSERT_EQ(expected_ids_p1[i], std::stoi(results["hits"][i]["document"]["id"].get<std::string>()));
    }

    std::vector<size_t> expected_ids_p2 = {10, 4};

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 8, 2, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                      "", 10,
                                      pinned_hits, {}).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    for(size_t i=0; i<2; i++) {
        ASSERT_EQ(expected_ids_p2[i], std::stoi(results["hits"][i]["document"]["id"].get<std::string>()));
    }
}

TEST_F(CollectionCurationTest, PinnedHitsLargerThanPageSize) {
    auto pinned_hits = "6:1,1:2,16:3,11:4";
    auto& ov_manager = CurationIndexManager::get_instance();

    // pinned hits larger than page size: check that pagination works

    auto results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 2, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                           "", 10,
                                           pinned_hits, {}).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 2, 2, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                      "", 10,
                                      pinned_hits, {}).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_STREQ("16", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 2, 3, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                      "", 10,
                                      pinned_hits, {}).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_STREQ("8", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());
}

TEST_F(CollectionCurationTest, PinnedHitsWhenThereAreNotEnoughResults) {
    auto pinned_hits = "6:1,1:2,11:5";
    auto& ov_manager = CurationIndexManager::get_instance();

    // multiple pinned hits specified, but query produces no result

    auto results = coll_mul_fields->search("not-foundquery", {"title"}, "", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                           "", 10,
                                           pinned_hits, {}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // multiple pinned hits but only single result
    results = coll_mul_fields->search("burgundy", {"title"}, "", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                      "", 10,
                                      pinned_hits, {}).get();

    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());

    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][3]["document"]["id"].get<std::string>().c_str());
}

TEST_F(CollectionCurationTest, HiddenHitsHidingSingleResult) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    std::vector<std::vector<std::string>> records = {
        {"Down There by the Train"}
    };

    for (size_t i = 0; i < records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    std::string hidden_hits="0";
    auto results = coll1->search("the train", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                      "", 10,
                                      "", hidden_hits).get();

    ASSERT_EQ(0, results["found"].get<size_t>());
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("the train", {"title"}, "points:0", {}, {}, {0}, 50, 1, FREQUENCY,
                           {false}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10,
                           "", hidden_hits).get();

    ASSERT_EQ(0, results["found"].get<size_t>());
    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, PinnedHitsGrouping) {
    auto pinned_hits = "6:1,8:1,1:2,13:3";
    auto& ov_manager = CurationIndexManager::get_instance();

    // without any grouping parameter, only the first ID in a position should be picked
    // and other IDs should appear in their original positions

    auto results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 50, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                                           "", 10,
                                           pinned_hits, {}).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("13", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][3]["document"]["id"].get<std::string>().c_str());

    // pinned hits should be marked as curated
    ASSERT_EQ(true, results["hits"][0]["curated"].get<bool>());
    ASSERT_EQ(true, results["hits"][1]["curated"].get<bool>());
    ASSERT_EQ(true, results["hits"][2]["curated"].get<bool>());
    ASSERT_EQ(0, results["hits"][3].count("curated"));

    // with grouping

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "starring: will", 30, 5,
                            "", 10,
                            pinned_hits, {}, {"cast"}, 2).get();

    ASSERT_EQ(9, results["found"].get<size_t>());

    ASSERT_EQ(1, results["grouped_hits"][0]["group_key"].size());
    ASSERT_EQ(2, results["grouped_hits"][0]["group_key"][0].size());
    ASSERT_STREQ("Chris Evans", results["grouped_hits"][0]["group_key"][0][0].get<std::string>().c_str());
    ASSERT_STREQ("Scarlett Johansson", results["grouped_hits"][0]["group_key"][0][1].get<std::string>().c_str());

    ASSERT_STREQ("6", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("8", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("1", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("13", results["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("11", results["grouped_hits"][3]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("16", results["grouped_hits"][4]["hits"][0]["document"]["id"].get<std::string>().c_str());
}

TEST_F(CollectionCurationTest, PinnedHitsGroupingNonPinnedHitsShouldNotAppearOutside) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("group_id", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 3, fields).get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc;
    doc["id"] = "1";
    doc["title"] = "The Harry Potter 1";
    doc["group_id"] = "hp";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["title"] = "The Harry Potter 2";
    doc["group_id"] = "hp";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "3";
    doc["title"] = "Lord of the Rings";
    doc["group_id"] = "lotr";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto pinned_hits = "2:1";

    auto results = coll1->search("*", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   pinned_hits, {}, {"group_id"}, 2).get();

    // when only one of the 2 records belonging to a group is used for curation, the other record
    // should appear at the back

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_EQ(2, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    // same for keyword search
    results = coll1->search("the", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10,
                            pinned_hits, {}, {"group_id"}, 2).get();

    // when only one of the 2 records belonging to a group is used for curation, the other record
    // should appear at the back

    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_EQ(2, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, PinnedHitsWithWildCardQuery) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 3, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    size_t num_indexed = 0;

    for(size_t i=0; i<311; i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
        num_indexed++;
    }

    auto pinned_hits = "7:1,4:2";

    auto results = coll1->search("*", {"title"}, "", {}, {}, {0}, 30, 11, FREQUENCY,
                                       {false}, Index::DROP_TOKENS_THRESHOLD,
                                       spp::sparse_hash_set<std::string>(),
                                       spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                       "", 10,
                                       pinned_hits, {}, {}, {0}, "", "", {}).get();

    ASSERT_EQ(311, results["found"].get<size_t>());
    ASSERT_EQ(11, results["hits"].size());

    std::vector<size_t> expected_ids = {12, 11, 10, 9, 8, 6, 5, 3, 2, 1, 0};  // 4 and 7 should be missing

    for(size_t i=0; i<11; i++) {
        ASSERT_EQ(expected_ids[i], std::stoi(results["hits"][i]["document"]["id"].get<std::string>()));
    }

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, HiddenHitsWithWildCardQuery) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 3, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    for(size_t i=0; i<5; i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = "Title " + std::to_string(i);
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto hidden_hits = "1";

    auto results = coll1->search("*", {"title"}, "", {}, {}, {0}, 30, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10,
                                 {}, hidden_hits, {}, {0}, "", "", {}).get();
    ASSERT_EQ(4, results["found"].get<size_t>());
    ASSERT_EQ(4, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, PinnedHitsIdsHavingColon) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("url", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    for(size_t i=1; i<=10; i++) {
        nlohmann::json doc;
        doc["id"] = std::string("https://example.com/") + std::to_string(i);
        doc["url"] = std::string("https://example.com/") + std::to_string(i);
        doc["points"] = i;

        coll1->add(doc.dump());
    }

    std::vector<std::string> query_fields = {"url"};
    std::vector<std::string> facets;

    std::string pinned_hits_str = "https://example.com/1:1, https://example.com/3:2";  // can have space

    auto res_op = coll1->search("*", {"url"}, "", {}, {}, {0}, 25, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                pinned_hits_str, {});

    ASSERT_TRUE(res_op.ok());

    auto res = res_op.get();

    ASSERT_EQ(10, res["found"].get<size_t>());
    ASSERT_STREQ("https://example.com/1", res["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("https://example.com/3", res["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("https://example.com/10", res["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("https://example.com/9", res["hits"][3]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("https://example.com/2", res["hits"][9]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringExactMatchBasics) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoes";
    doc1["brand"] = "Nike";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Track Gym";
    doc2["category"] = "shoes";
    doc2["brand"] = "Adidas";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Running Shoes";
    doc3["category"] = "sports";
    doc3["brand"] = "Nike";
    doc3["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    auto results = coll1->search("shoes", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][2]["document"]["id"].get<std::string>());

    // with curation, results will be different

    nlohmann::json curation_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                         {"query", "{category}"},
                         {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category}"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    curation_json = {
            {"id",   "dynamic-brand-cat-filter"},
            {
             "rule", {
                             {"query", "{brand} {category}"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category} && brand: {brand}"}
    };

    op = curation_t::parse(curation_json, "dynamic-brand-cat-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    curation_json = {
            {"id",   "dynamic-brand-filter"},
            {
             "rule", {
                     {"query", "{brand}"},
                     {"match", curation_t::MATCH_EXACT}
                 }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "brand: {brand}"}
    };

    curation_json["includes"] = nlohmann::json::array();
    curation_json["includes"][0] = nlohmann::json::object();
    curation_json["includes"][0]["id"] = "0";
    curation_json["includes"][0]["position"] = 1;

    op = curation_t::parse(curation_json, "dynamic-brand-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("shoes", {"name", "category", "brand"}, "",
                                       {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    ASSERT_EQ(0, results["hits"][0]["highlights"].size());
    ASSERT_EQ(0, results["hits"][1]["highlights"].size());

    // should not apply filter for non-exact case
    results = coll1->search("running shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(3, results["hits"].size());

    results = coll1->search("adidas shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // dynamic brand filter + explicit ID include
    results = coll1->search("adidas", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    // with bad curation

    nlohmann::json curation_json_bad1 = {
            {"id",   "dynamic-filters-bad1"},
            {
             "rule", {
                         {"query", "{brand}"},
                         {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", ""}
    };

    curation_t curation_bad1;
    op = curation_t::parse(curation_json_bad1, "dynamic-filters-bad1", curation_bad1);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `filter_by` must be a non-empty string.", op.error());

    nlohmann::json curation_json_bad2 = {
            {"id",   "dynamic-filters-bad2"},
            {
             "rule", {
                             {"query", "{brand}"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", {"foo", "bar"}}
    };

    curation_t curation_bad2;
    op = curation_t::parse(curation_json_bad2, "dynamic-filters-bad2", curation_bad2);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `filter_by` must be a string.", op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringPrefixMatchShouldNotWork) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoe";
    doc1["brand"] = "Nike";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Track Gym";
    doc2["category"] = "shoes";
    doc2["brand"] = "Adidas";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Running Shoe";
    doc3["category"] = "shoes";
    doc3["brand"] = "Nike";
    doc3["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    // with curation, results will be different

    nlohmann::json curation_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                             {"query", "{category}"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category}"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("shoe", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringMissingField) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoes";
    doc1["points"] = 3;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                             {"query", "{categories}"},             // this field does NOT exist
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {categories}"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("shoes", {"name", "category"}, "",
                            {}, sort_fields, {2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringBadFilterBy) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoes";
    doc1["points"] = 3;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                             {"query", "{category}"},             // this field does NOT exist
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category} && foo"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("shoes", {"name", "category"}, "",
                                 {}, sort_fields, {2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringMultiplePlaceholders) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Retro Shoes";
    doc1["category"] = "shoes";
    doc1["color"] = "yellow";
    doc1["brand"] = "Nike Air Jordan";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Baseball";
    doc2["category"] = "shoes";
    doc2["color"] = "white";
    doc2["brand"] = "Adidas";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Running Shoes";
    doc3["category"] = "sports";
    doc3["color"] = "grey";
    doc3["brand"] = "Nike";
    doc3["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC"), sort_by("points", "DESC")};

    nlohmann::json curation_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "{brand} {color} shoes"},
                                            {"match", curation_t::MATCH_CONTAINS}
                                    }
            },
            {"remove_matched_tokens", true},
            {"filter_by",           "brand: {brand} && color: {color}"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    // not an exact match of rule (because of "light") so all results will be fetched, not just Air Jordan brand
    auto results = coll1->search("Nike Air Jordan light yellow shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());

    // query with tokens at the start that preceding the placeholders in the rule
    results = coll1->search("New Nike Air Jordan yellow shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringTokensBetweenPlaceholders) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Retro Shoes";
    doc1["category"] = "shoes";
    doc1["color"] = "yellow";
    doc1["brand"] = "Nike Air Jordan";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Baseball";
    doc2["category"] = "shoes";
    doc2["color"] = "white";
    doc2["brand"] = "Adidas";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Running Shoes";
    doc3["category"] = "sports";
    doc3["color"] = "grey";
    doc3["brand"] = "Nike";
    doc3["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC"), sort_by("points", "DESC")};

    nlohmann::json curation_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "{brand} shoes {color}"},
                                            {"match", curation_t::MATCH_CONTAINS}
                                    }
            },
            {"remove_matched_tokens", true},
            {"filter_by",           "brand: {brand} && color: {color}"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("Nike Air Jordan shoes yellow", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringWithNumericalFilter) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Retro Shoes";
    doc1["category"] = "shoes";
    doc1["color"] = "yellow";
    doc1["brand"] = "Nike";
    doc1["points"] = 15;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Baseball Shoes";
    doc2["category"] = "shoes";
    doc2["color"] = "white";
    doc2["brand"] = "Nike";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Running Shoes";
    doc3["category"] = "sports";
    doc3["color"] = "grey";
    doc3["brand"] = "Nike";
    doc3["points"] = 5;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["name"] = "Running Shoes";
    doc4["category"] = "sports";
    doc4["color"] = "grey";
    doc4["brand"] = "Adidas";
    doc4["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC"), sort_by("points", "DESC")};

    nlohmann::json curation_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "popular {brand} shoes"},
                                            {"match", curation_t::MATCH_CONTAINS}
                                    }
            },
            {"remove_matched_tokens", false},
            {"filter_by",           "brand: {brand} && points:> 10"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());

    auto results = coll1->search("popular nike shoes", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(4, results["hits"].size());

    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("popular nike shoes", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // when curations are disabled

    bool enable_curations = false;
    results = coll1->search("popular nike shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false, false, false}, 10,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1, 1}, 10000, true, false, enable_curations).get();
    ASSERT_EQ(4, results["hits"].size());

    // should not match the defined curation

    results = coll1->search("running adidas shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][3]["document"]["id"].get<std::string>());

    results = coll1->search("adidas", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringExactMatch) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Retro Shoes";
    doc1["category"] = "shoes";
    doc1["color"] = "yellow";
    doc1["brand"] = "Nike";
    doc1["points"] = 15;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Baseball Shoes";
    doc2["category"] = "shoes";
    doc2["color"] = "white";
    doc2["brand"] = "Nike";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Running Shoes";
    doc3["category"] = "sports";
    doc3["color"] = "grey";
    doc3["brand"] = "Nike";
    doc3["points"] = 5;

    nlohmann::json doc4;
    doc4["id"] = "3";
    doc4["name"] = "Running Shoes";
    doc4["category"] = "sports";
    doc4["color"] = "grey";
    doc4["brand"] = "Adidas";
    doc4["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC"), sort_by("points", "DESC")};

    nlohmann::json curation_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "popular {brand} shoes"},
                                            {"match", curation_t::MATCH_EXACT}
                                    }
            },
            {"remove_matched_tokens", false},
            {"filter_by",           "brand: {brand} && points:> 10"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-cat-filter", curation);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_json);

    auto results = coll1->search("really popular nike shoes", {"name", "category", "brand"}, "",
                                  {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(4, results["hits"].size());

    results = coll1->search("popular nike running shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(4, results["hits"].size());

    results = coll1->search("popular nike shoes running", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringWithSynonyms) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        SynonymIndexManager& synonymIndexManager = SynonymIndexManager::get_instance();
        synonymIndexManager.init_store(store);
        synonymIndexManager.add_synonym_index("index");
        coll1->set_synonym_sets({"index"});
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoes";
    doc1["brand"] = "Nike";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Exciting Track Gym";
    doc2["category"] = "shoes";
    doc2["brand"] = "Adidas";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Amazing Sneakers";
    doc3["category"] = "sneakers";
    doc3["brand"] = "Adidas";
    doc3["points"] = 4;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    SynonymIndexManager::get_instance().upsert_synonym_item("index",R"({"id": "sneakers-shoes", "root": "sneakers", "synonyms": ["shoes"]})"_json);
    SynonymIndexManager::get_instance().upsert_synonym_item("index",R"({"id": "boots-shoes", "root": "boots", "synonyms": ["shoes"]})"_json);
    SynonymIndexManager::get_instance().upsert_synonym_item("index",R"({"id": "exciting-amazing", "root": "exciting", "synonyms": ["amazing"]})"_json);

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    // spaces around field name should still work e.g. "{ field }"
    nlohmann::json curation_json1 = {
        {"id",   "dynamic-filters"},
        {
         "rule", {
                     {"query", "{ category }"},
                     {"match", curation_t::MATCH_EXACT}
                 }
        },
        {"filter_by", "category: {category}"}
    };

    curation_t curation1;
    auto op = curation_t::parse(curation_json1, "dynamic-filters", curation1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json1);

    auto curations = ov_manager.list_curation_items("index", 0, 0).get();
    ASSERT_EQ(1, curations.size());
    auto curation_json = curations[0];
    ASSERT_EQ("category: {category}", curation_json["filter_by"].get<std::string>());
    ASSERT_EQ(true, curation_json["remove_matched_tokens"].get<bool>());  // must be true by default

    nlohmann::json curation_json2 = {
        {"id",   "static-filters"},
        {
         "rule", {
                     {"query", "exciting"},
                     {"match", curation_t::MATCH_CONTAINS}
                 }
        },
        {"remove_matched_tokens", true},
        {"filter_by", "points: [5, 4]"}
    };

    curation_t curation2;
    op = curation_t::parse(curation_json2, "static-filters", curation2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json2);

    auto results = coll1->search("sneakers", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // keyword does not exist but has a synonym with results

    results = coll1->search("boots", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    // keyword has no curation, but synonym's curation is used
    results = coll1->search("exciting", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, StaticFiltering) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        SynonymIndexManager& synonymIndexManager = SynonymIndexManager::get_instance();
        synonymIndexManager.init_store(store);
        synonymIndexManager.add_synonym_index("index");
        coll1->set_synonym_sets({"index"});
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["price"] = 399.99;
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Track Shoes";
    doc2["price"] = 49.99;
    doc2["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "expensive"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:> 100"}
    };

    curation_t curation_contains;
    auto op = curation_t::parse(curation_json_contains, "static-filters", curation_contains);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_json_contains);

    nlohmann::json curation_json_exact = {
            {"id",   "static-exact-filters"},
            {
             "rule", {
                             {"query", "cheap"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:< 100"}
    };

    curation_t curation_exact;
    op = curation_t::parse(curation_json_exact, "static-exact-filters", curation_exact);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_json_exact);

    auto results = coll1->search("expensive shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("expensive", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // partial word should not match
    results = coll1->search("inexpensive shoes", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 10).get();

    ASSERT_EQ(2, results["found"].get<uint32_t>());
    ASSERT_EQ(2, results["hits"].size());

    // with exact match

    results = coll1->search("cheap", {"name"}, "",
                            {}, sort_fields, {2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // should not work in match contains context

    results = coll1->search("cheap boots", {"name"}, "",
                            {}, sort_fields, {2}, 10).get();

    ASSERT_EQ(0, results["hits"].size());

    // with synonym for expensive: should NOT match as synonyms are resolved after curation substitution
    op = SynonymIndexManager::get_instance().upsert_synonym_item("index",
                                                            R"({"id": "costly-expensive", "root": "costly", "synonyms": ["expensive"]})"_json);
    ASSERT_TRUE(op.ok());

    results = coll1->search("costly", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, StaticFilteringMultipleRuleMatch) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["tags"] = {"twitter"};
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Track Shoes";
    doc2["tags"] = {"starred"};
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Track Shoes";
    doc3["tags"] = {"twitter", "starred"};
    doc3["points"] = 10;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_filter1_json = {
            {"id",   "static-filter-1"},
            {
             "rule", {
                             {"query", "twitter"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: twitter"}
    };

    curation_t curation_filter1;
    auto op = curation_t::parse(curation_filter1_json, "static-filter-1", curation_filter1);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_filter1_json);

    nlohmann::json curation_filter2_json = {
            {"id",   "static-filter-2"},
            {
             "rule", {
                             {"query", "starred"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: starred"}
    };

    curation_t curation_filter2;
    op = curation_t::parse(curation_filter2_json, "static-filter-2", curation_filter2);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_filter2_json);

    auto results = coll1->search("starred twitter", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // when stop_processing is enabled (default is true)
    curation_filter1_json.erase("stop_processing");
    curation_filter2_json.erase("stop_processing");

    curation_t curation_filter1_reset;
    op = curation_t::parse(curation_filter1_json, "static-filter-1", curation_filter1_reset);
    ASSERT_TRUE(op.ok());
    curation_t curation_filter2_reset;
    op = curation_t::parse(curation_filter2_json, "static-filter-2", curation_filter2_reset);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_filter1_json);
    ov_manager.upsert_curation_item("index", curation_filter2_json);

    results = coll1->search("starred twitter", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringMultipleRuleMatch) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["brand"] = "Nike";
    doc1["tags"] = {"twitter"};
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Track Shoes";
    doc2["brand"] = "Adidas";
    doc2["tags"] = {"starred"};
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Track Shoes";
    doc3["brand"] = "Nike";
    doc3["tags"] = {"twitter", "starred"};
    doc3["points"] = 10;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_filter1_json = {
            {"id",   "dynamic-filter-1"},
            {
             "rule", {
                             {"query", "{brand}"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: twitter"},
            {"metadata", {{"foo", "bar"}}},
    };

    curation_t curation_filter1;
    auto op = curation_t::parse(curation_filter1_json, "dynamic-filter-1", curation_filter1);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_filter1_json);

    ASSERT_EQ("bar", curation_filter1.to_json()["metadata"]["foo"].get<std::string>());

    nlohmann::json curation_filter2_json = {
            {"id",   "dynamic-filter-2"},
            {
             "rule", {
                             {"query", "{tags}"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: starred"}
    };

    curation_t curation_filter2;
    op = curation_t::parse(curation_filter2_json, "dynamic-filter-2", curation_filter2);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_filter2_json);

    auto results = coll1->search("starred nike", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("bar", results["metadata"]["foo"].get<std::string>());

    // when stop_processing is enabled (default is true)
    curation_filter1_json.erase("stop_processing");
    curation_filter2_json.erase("stop_processing");

    curation_t curation_filter1_reset;
    op = curation_t::parse(curation_filter1_json, "dynamic-filter-1", curation_filter1_reset);
    ASSERT_TRUE(op.ok());
    curation_t curation_filter2_reset;
    op = curation_t::parse(curation_filter2_json, "dynamic-filter-2", curation_filter2_reset);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_filter1_json);
    ov_manager.upsert_curation_item("index", curation_filter2_json);

    results = coll1->search("starred nike", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, SynonymsAppliedToOverridenQuery) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        SynonymIndexManager& synonymIndexManager = SynonymIndexManager::get_instance();
        synonymIndexManager.init_store(store);
        synonymIndexManager.add_synonym_index("index");
        coll1->set_synonym_sets({"index"});
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["price"] = 399.99;
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "White Sneakers";
    doc2["price"] = 149.99;
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Red Sneakers";
    doc3["price"] = 49.99;
    doc3["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "expensive"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:> 100"}
    };

    curation_t curation_contains;
    auto op = curation_t::parse(curation_json_contains, "static-filters", curation_contains);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_json_contains);

    SynonymIndexManager::get_instance().upsert_synonym_item("index", R"({"id": "", "root": "shoes", "synonyms": ["sneakers"]})"_json);

    auto results = coll1->search("expensive shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, StaticFilterWithAndWithoutQueryStringMutation) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Apple iPad";
    doc1["price"] = 399.99;
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Samsung Charger";
    doc2["price"] = 49.99;
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Samsung Phone";
    doc3["price"] = 249.99;
    doc3["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "apple"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", false},
            {"filter_by", "price:> 200"}
    };

    curation_t curation_contains;
    auto op = curation_t::parse(curation_json_contains, "static-filters", curation_contains);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_json_contains);

    // first without query string mutation

    auto results = coll1->search("apple", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // now, with query string mutation

    curation_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "apple"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:> 200"}
    };

    op = curation_t::parse(curation_json_contains, "static-filters", curation_contains);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json_contains);

    results = coll1->search("apple", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringWithJustRemoveTokens) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoes";
    doc1["brand"] = "Nike";
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Track Gym";
    doc2["category"] = "shoes";
    doc2["brand"] = "Adidas";
    doc2["points"] = 5;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Running Shoes";
    doc3["category"] = "sports";
    doc3["brand"] = "Nike";
    doc3["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC"), sort_by("points", "DESC")};

    auto results = coll1->search("all", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {0, 0, 0}, 10).get();

    ASSERT_EQ(0, results["hits"].size());

    // with curation, we return all records

    nlohmann::json curation_json = {
        {"id",                    "match-all"},
        {
         "rule",                  {
                                          {"query", "all"},
                                          {"match", curation_t::MATCH_EXACT}
                                  }
        },
        {"remove_matched_tokens", true}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "match-all", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("all", {"name", "category", "brand"}, "",
                            {}, sort_fields, {0, 0, 0}, 10).get();

    ASSERT_EQ(3, results["hits"].size());

    results = coll1->search("really amazing shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {0, 0, 0}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // with contains
    curation_json = {
            {"id",                    "remove-some-tokens"},
            {
             "rule",                  {
                                              {"query", "really"},
                                              {"match", curation_t::MATCH_CONTAINS}
                                      }
            },
            {"remove_matched_tokens", true}
    };

    curation_t curation2;
    op = curation_t::parse(curation_json, "remove-some-tokens", curation2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("really amazing shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {0, 0, 0}, 1).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, StaticSorting) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["price"] = 399.99;
    doc1["points"] = 3;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Track Shoes";
    doc2["price"] = 49.99;
    doc2["points"] = 5;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json curation_json_contains = {
            {"id",   "static-sort"},
            {
             "rule", {
                             {"query", "shoes"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"sort_by", "price:desc"}
    };

    curation_t curation_contains;
    auto op = curation_t::parse(curation_json_contains, "static-sort", curation_contains);
    ASSERT_TRUE(op.ok());

    // without curation kicking in
    auto results = coll1->search("shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // now add curation
    ov_manager.upsert_curation_item("index", curation_json_contains);

    results = coll1->search("shoes", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    // with curation we will sort on price
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    //unrelated queries should not get matched
    results = coll1->search("*", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicSorting) {
    Collection *coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("store", field_types::STRING_ARRAY, false),
                                 field("size", field_types::STRING_ARRAY, false),
                                 field("unitssold", field_types::OBJECT, false),
                                 field("unitssold.store01", field_types::INT32, true),
                                 field("unitssold.store02", field_types::INT32, true),
                                 field("unitssold.small", field_types::INT32, true),
                                 field("unitssold.medium", field_types::INT32, true),
                                 field("stockonhand", field_types::OBJECT, false),
                                 field("stockonhand.store01", field_types::INT32, true),
                                 field("stockonhand.store02", field_types::INT32, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Nike Shoes";
    doc1["store"] = {"store01", "store02"};
    doc1["size"] = {"small", "medium"};
    doc1["unitssold.store01"] = 399;
    doc1["unitssold.store02"] = 498;
    doc1["unitssold.small"] = 304;
    doc1["unitssold.medium"] = 593;
    doc1["stockonhand.store01"] = 129;
    doc1["stockonhand.store02"] = 227;
    doc1["points"] = 100;

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Asics Shoes";
    doc2["store"] = {"store01", "store02"};
    doc2["size"] = {"small", "medium"};
    doc2["unitssold.store01"] = 899;
    doc2["unitssold.store02"] = 408;
    doc2["unitssold.small"] = 507;
    doc2["unitssold.medium"] = 800;
    doc2["stockonhand.store01"] = 101;
    doc2["stockonhand.store02"] = 64;
    doc2["points"] = 100;

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Adidas Shoes Black";
    doc3["store"] = {"store01", "store02"};
    doc3["size"] = {"small", "medium"};
    doc3["unitssold.store01"] = 599;
    doc3["unitssold.store02"] = 501;
    doc3["unitssold.small"] = 607;
    doc3["unitssold.medium"] = 493;
    doc3["stockonhand.store01"] = 301;
    doc3["stockonhand.store02"] = 424;
    doc3["points"] = 100;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    //query based dynamic sorting
    nlohmann::json curation_json_contains = {
            {"id",   "dynamic-sort"},
            {
             "rule", {
                             {"query", "{store}"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"sort_by", "unitssold.{store}:desc, stockonhand.{store}:desc"}
    };

    curation_t curation_contains;
    auto op = curation_t::parse(curation_json_contains, "dynamic-sort", curation_contains);
    ASSERT_TRUE(op.ok());

    // now add curation
    ov_manager.upsert_curation_item("index", curation_json_contains);

    auto results = coll1->search("store01", {"store"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    results = coll1->search("store02", {"store"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());

    // filter based dynamic sorting
    curation_json_contains = {
            {"id",   "dynamic-sort2"},
            {
             "rule", {
                             {"filter_by", "store:={store}"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"sort_by", "unitssold.{store}:desc, stockonhand.{store}:desc"}
    };

    curation_t curation_contains2;
    op = curation_t::parse(curation_json_contains, "dynamic-sort", curation_contains2);
    ASSERT_TRUE(op.ok());

    // now add curation
    ov_manager.upsert_curation_item("index", curation_json_contains);

    results = coll1->search("*", {}, "store:=store01",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "store:=store02",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());

    //multiple place holder with dynamic filter
    curation_json_contains = {
            {"id",                  "dynamic-sort3"},
            {
             "rule",                {
                                            {"filter_by", "store:={store} && size:={size}"},
                                            {"match", curation_t::MATCH_CONTAINS},
                                            {"tags", {"size"}}
                                    }
            },
            {"remove_matched_tokens", true},
            {"sort_by", "unitssold.{store}:desc, unitssold.{size}:desc"}
    };

    curation_t curation_contains3;
    op = curation_t::parse(curation_json_contains, "dynamic-sort3", curation_contains3);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json_contains);

    results = coll1->search("*", {}, "store:=store02 && size:=small",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "size").get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "store:=store01 && size:=small",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "size").get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    //no curations matched, hence no sorting
    results = coll1->search("store", {"store"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilteringWithPartialTokenMatch) {
    // when query tokens do not match placeholder field value exactly, don't do filtering
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields).get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "Running Shoes";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "Magic Lamp";
    doc2["category"] = "Shoo";

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Shox and Us";
    doc3["category"] = "Socks";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    auto results = coll1->search("shoes", {"name"}, "",
                                 {}, sort_fields, {0}, 10).get();

    ASSERT_EQ(1, results["hits"].size());

    // with curation, we return all records

    nlohmann::json curation_json = {
            {"id",   "dynamic-filter"},
            {
             "rule", {
                             {"query", "{ category }"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"filter_by", "category:= {category}"},
            {"remove_matched_tokens", true}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "dynamic-filter", curation);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("shoes", {"name"}, "",
                            {}, sort_fields, {0}, 10).get();

    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("shox", {"name"}, "",
                            {}, sort_fields, {0}, 10).get();

    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, OverrideWithSymbolsToIndex) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", static_cast<uint64_t>(std::time(nullptr)),
                                                    "", {"-"}, {}).get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Non-Stick";
    doc1["category"] = "Cookware";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "NonStick";
    doc2["category"] = "Kitchen";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    auto results = coll1->search("non-stick", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

    ASSERT_EQ(2, results["hits"].size());

    // with curation, we return all records

    nlohmann::json curation_json = {
            {"id",   "ov-1"},
            {
             "rule", {
                             {"query", "non-stick"},
                             {"match", curation_t::MATCH_EXACT}
                     }
            },
            {"filter_by", "category:= Cookware"}
    };

    curation_t curation;
    auto op = curation_t::parse(curation_json, "ov-1", curation, "", {'-'}, {});
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    results = coll1->search("non-stick", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("nonstick", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10).get();

    ASSERT_EQ(2, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, OverrideWithTags) {
    /*

     If curation1 is tagged tagA, tagB, curation2 is tagged tagA, curation3 is tagged with nothing:

     Then if a search is tagged with tagA, we only consider curations that contain tagA (curation1 and curation2)
     with the usual logic - in alphabetic order of curation name and then process both if stop rule processing is false.

     If a search is tagged with tagA and tagB, we evaluate any rules that contain tagA and tagB first,
     then tag A or tag B, but not curations that contain no tags. Within each group, we evaluate in alphabetic order
     and process multiple if stop rule processing is false

     If a search has no tags, then we only consider rules that have no tags.
    */

    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = "kids";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "queryA";
    doc2["category"] = "kitchen";

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Clay Toy";
    doc3["category"] = "home";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    auto results = coll1->search("Clay", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10).get();

    ASSERT_EQ(1, results["hits"].size());

    // create curations containing 2 tags, single tag and no tags:
    nlohmann::json curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha", "beta"]
        },
        "filter_by": "category: kids"
    })"_json;

    curation_t curation1;
    auto op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json1);

    // single tag
    nlohmann::json curation_json2 = R"({
       "id": "ov-2",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha"]
        },
        "filter_by": "category: kitchen"
    })"_json;

    curation_t curation2;
    curation_t::parse(curation_json2, "ov-2", curation2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json2);

    // no tag
    nlohmann::json curation_json3 = R"({
       "id": "ov-3",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "filter_by": "category: home"
    })"_json;

    curation_t curation3;
    op = curation_t::parse(curation_json3, "ov-3", curation3);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json3);

    // when tag doesn't match any curation, no results will be found
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "foo").get();

    ASSERT_EQ(2, results["hits"].size());

    // when multiple curations match a given tag, return first matching record
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "alpha").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // single tag matching rule with multiple tags
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "beta").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // when multiple tags are passed, only consider rule with both tags
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "alpha,beta").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // query with no tags should only trigger curation with no tags
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, OverrideWithTagsPartialMatch) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = "kids";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "queryA";
    doc2["category"] = "kitchen";

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Clay Toy";
    doc3["category"] = "home";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    nlohmann::json curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha", "beta"]
        },
        "filter_by": "category: kids"
    })"_json;

    curation_t curation1;
    auto op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json1);

    //
    nlohmann::json curation_json2 = R"({
       "id": "ov-2",
       "rule": {
            "query": "queryB",
            "match": "exact",
            "tags": ["alpha"]
        },
        "filter_by": "category: kitchen"
    })"_json;

    curation_t curation2;
    curation_t::parse(curation_json2, "ov-2", curation2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json2);

    // when only one of the two tags are found, apply that rule
    auto results = coll1->search("queryB", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "alpha,zeta").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, OverrideWithTagsWithoutStopProcessing) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING_ARRAY, true),};

    auto& ov_manager = CurationIndexManager::get_instance();
    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = {"kids"};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "queryA";
    doc2["category"] = {"kids", "kitchen"};

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Clay Toy";
    doc3["category"] = {"home"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    nlohmann::json curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha", "beta"]
        },
        "stop_processing": false,
        "remove_matched_tokens": false,
        "filter_by": "category: kids"
    })"_json;

    curation_t curation1;
    auto op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json1);

    //
    nlohmann::json curation_json2 = R"({
       "id": "ov-2",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha"]
        },
        "stop_processing": false,
        "remove_matched_tokens": false,
        "filter_by": "category: kitchen",
        "metadata": {"foo": "bar"}
    })"_json;

    curation_t curation2;
    curation_t::parse(curation_json2, "ov-2", curation2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json2);

    //
    nlohmann::json curation_json3 = R"({
       "id": "ov-3",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "stop_processing": false,
        "remove_matched_tokens": false,
        "filter_by": "category: home"
    })"_json;

    curation_t curation3;
    op = curation_t::parse(curation_json3, "ov-3", curation3);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json3);

    auto results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "alpha").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("bar", results["metadata"]["foo"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, WildcardTagRuleThatMatchesAllQueries) {
    Collection* coll1;

    auto& ov_manager = CurationIndexManager::get_instance();
    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = "kids";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "queryA";
    doc2["category"] = "kitchen";

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Clay Toy";
    doc3["category"] = "home";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    nlohmann::json curation_json1 = R"({
       "id": "ov-1",
       "rule": {

        },
        "filter_by": "category: kids"
    })"_json;

    curation_t curation1;
    auto op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `rule` definition must contain either a `tags` or a `query` and `match`.", op.error());

    curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "tags": ["*"]
        },
        "filter_by": "category: kids"
    })"_json;

    op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json1);

    // should match all search queries, even without passing any tags
    std::string curation_tags = "";
    auto results = coll1->search("queryB", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                                 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", curation_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", curation_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // includes instead of filter_by
    ov_manager.delete_curation_item("index", "ov-1");
    auto curation_json2 = R"({
       "id": "ov-1",
       "rule": {
            "tags": ["*"]
        },
        "includes": [
            {"id": "1", "position": 1}
        ]
    })"_json;

    curation_t curation2;
    op = curation_t::parse(curation_json2, "ov-2", curation2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json2);

    results = coll1->search("foobar", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", curation_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, TagsOnlyRule) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING_ARRAY, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = {"kids"};

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "queryA";
    doc2["category"] = {"kitchen"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};
    curation_t curation1;
    auto curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "tags": ["listing"]
        },
        "filter_by": "category: kids"
    })"_json;

    auto op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json1);

    auto results = coll1->search("queryA", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                                 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", "listing").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // with include rule
    curation_t curation2;
    auto curation_json2 = R"({
       "id": "ov-2",
       "rule": {
            "tags": ["listing2"]
        },
        "includes": [
            {"id": "1", "position": 1}
        ]
    })"_json;

    op = curation_t::parse(curation_json2, "ov-2", curation2);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json2);

    results = coll1->search("foobar", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "listing2").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // no curation tag passed: rule should not match
    std::string curation_tag = "";
    results = coll1->search("foobar", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", curation_tag).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, MetadataValidation) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING_ARRAY, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = {"kids"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    nlohmann::json curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "filter_by": "category: kids",
        "metadata": "foo"
    })"_json;

    curation_t curation1;
    auto op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `metadata` must be a JSON object.", op.error());

    // don't allow empty rule without any action
    curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact"
        }
    })"_json;

    curation_t curation2;
    op = curation_t::parse(curation_json1, "ov-2", curation2);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Must contain one of: `includes`, `excludes`, `metadata`, `filter_by`, `sort_by`, "
              "`remove_matched_tokens`, `replace_query`.", op.error());

    // should allow only metadata to be present as action

    curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "metadata": {"foo": "bar"}
    })"_json;

    curation_t curation3;
    op = curation_t::parse(curation_json1, "ov-3", curation3);
    ASSERT_TRUE(op.ok());

    ov_manager.upsert_curation_item("index", curation_json1);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, WildcardSearchOverride) {
    Collection* coll1;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
        coll1->set_curation_sets({"index"});
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = "kids";

    nlohmann::json doc2;
    doc2["id"] = "1";
    doc2["name"] = "queryA";
    doc2["category"] = "kitchen";

    nlohmann::json doc3;
    doc3["id"] = "2";
    doc3["name"] = "Clay Toy";
    doc3["category"] = "home";

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    nlohmann::json curation_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "*",
            "match": "exact"
        },
        "filter_by": "category: kids"
    })"_json;

    curation_t curation1;
    auto op = curation_t::parse(curation_json1, "ov-1", curation1);
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", curation_json1);

    std::string curation_tags = "";
    auto results = coll1->search("*", {}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                                 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", curation_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // includes instead of filter_by
    ov_manager.delete_curation_item("index", "ov-1");

    auto curation_json2 = R"({
       "id": "ov-2",
       "rule": {
            "query": "*",
            "match": "exact"
        },
        "includes": [
            {"id": "1", "position": 1}
        ]
    })"_json;

    ov_manager.upsert_curation_item("index", curation_json2);

    results = coll1->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", curation_tags).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, OverridesPagination) {
    Collection *coll2;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};
    
    CurationIndexManager& ov_manager = CurationIndexManager::get_instance();

    coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields, "points").get();
    }

    for(int i = 0; i < 5; ++i) {
        nlohmann::json curation_json = {
                {"id",       "curation"},
                {
                 "rule",     {
                                     {"query", "not-found"},
                                     {"match", curation_t::MATCH_EXACT}
                             }
                },
                {"metadata", {       {"foo",   "bar"}}},
        };

        curation_json["id"] = curation_json["id"].get<std::string>() + std::to_string(i + 1);
        ov_manager.upsert_curation_item("index", curation_json);
    }

    uint32_t limit = 0, offset = 0, i = 0;

    //limit collections by 2
    limit=2;
    auto curation_op = ov_manager.list_curation_items("index", limit, offset);
    auto curation_map = curation_op.get();
    ASSERT_EQ(2, curation_map.size());
    i=offset;
    for(const auto &kv : curation_map) {
        ASSERT_EQ("curation" + std::to_string(i+1), kv["id"].get<std::string>().c_str());
        ++i;
    }

    //get 2 collection from offset 3
    offset=3;
    curation_op = ov_manager.list_curation_items("index", limit, offset);
    curation_map = curation_op.get();
    ASSERT_EQ(2, curation_map.size());
    i=offset;
    for(const auto &kv : curation_map) {
        ASSERT_EQ("curation" + std::to_string(i+1),  kv["id"].get<std::string>().c_str());
        ++i;
    }

    //get all collection except first
    offset=1; limit=0;
    curation_op = ov_manager.list_curation_items("index", limit, offset);
    curation_map = curation_op.get();
    ASSERT_EQ(4, curation_map.size());
    i=offset;
    for(const auto &kv : curation_map) {
        ASSERT_EQ("curation" + std::to_string(i+1),  kv["id"].get<std::string>().c_str());
        ++i;
    }

    //get last collection
    offset=4, limit=1;
    curation_op = ov_manager.list_curation_items("index", limit, offset);
    curation_map = curation_op.get();
    ASSERT_EQ(1, curation_map.size());
    ASSERT_EQ("curation5", curation_map[0]["id"].get<std::string>());

    //if limit is greater than number of collection then return all from offset
    offset=0; limit=8;
    curation_op = ov_manager.list_curation_items("index", limit, offset);
    curation_map = curation_op.get();
    ASSERT_EQ(5, curation_map.size());
    i=offset;
    for(const auto &kv : curation_map) {
        ASSERT_EQ("curation" + std::to_string(i+1),  kv["id"].get<std::string>());
        ++i;
    }

    offset=3; limit=4;
    curation_op = ov_manager.list_curation_items("index", limit, offset);
    curation_map = curation_op.get();
    ASSERT_EQ(2, curation_map.size());
    i=offset;
    for(const auto &kv : curation_map) {
        ASSERT_EQ("curation" + std::to_string(i+1),  kv["id"].get<std::string>().c_str());
        ++i;
    }

    //invalid offset
    offset=6; limit=0;
    curation_op = ov_manager.list_curation_items("index", limit, offset);
    ASSERT_FALSE(curation_op.ok());
    ASSERT_EQ("Invalid offset param.", curation_op.error());
}

TEST_F(CollectionCurationTest, RetrieveOverideByID) {
    Collection *coll2;
    auto& ov_manager = CurationIndexManager::get_instance();

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll2 = collectionManager.get_collection("coll2").get();
    if (coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields, "points").get();
    }

    nlohmann::json curation_json = {
            {"id",       "curation"},
            {
             "rule",     {
                                 {"query", "not-found"},
                                 {"match", curation_t::MATCH_EXACT}
                         }
            },
            {"metadata", {       {"foo",   "bar"}}},
    };

    curation_json["id"] = curation_json["id"].get<std::string>() + "1";
    ov_manager.upsert_curation_item("index", curation_json);

    auto op = ov_manager.get_curation_item("index", "curation1");
    ASSERT_TRUE(op.ok());
}


TEST_F(CollectionCurationTest, FilterPinnedHits) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    Collection* coll3 = collectionManager.get_collection("coll3").get();
    if(coll3 == nullptr) {
        coll3 = collectionManager.create_collection("coll3", 1, fields, "points").get();
    }

    nlohmann::json doc;

    doc["title"] = "Snapdragon 7 gen 2023";
    doc["points"] = 100;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Snapdragon 732G 2023";
    doc["points"] = 91;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Snapdragon 4 gen 2023";
    doc["points"] = 65;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Mediatek Dimensity 720G 2022";
    doc["points"] = 87;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Mediatek Dimensity 470G 2023";
    doc["points"] = 63;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    auto pinned_hits = "3:1, 4:2";

    bool filter_curated_hits = false;
    auto results = coll3->search("2023", {"title"}, "title: snapdragon", {}, {},
                                 {0}, 50, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10,
                                 "", 30, 5, "",
                                 10, pinned_hits, {}, {}, 3,
                                 "<mark>", "</mark>", {}, UINT_MAX,
                                 true, false, true, "",
                                 false, 6000 * 1000, 4, 7,
                                 fallback, 4, {off}, INT16_MAX,
                                 INT16_MAX, 2, filter_curated_hits ).get();


    ASSERT_EQ(5, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("4", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][3]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][4]["document"]["id"].get<std::string>());

    // when filter does not match, we should return only curated results
    results = coll3->search("2023", {"title"}, "title: foobarbaz", {}, {},
                            {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10,
                            "", 30, 5, "",
                            10, pinned_hits, {}, {}, 3,
                            "<mark>", "</mark>", {}, UINT_MAX,
                            true, false, true, "",
                            false, 6000 * 1000, 4, 7,
                            fallback, 4, {off}, INT16_MAX,
                            INT16_MAX, 2, filter_curated_hits ).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("4", results["hits"][1]["document"]["id"].get<std::string>());

    // Filter does not match but with filter_curated_hits = true
    filter_curated_hits = true;

    results = coll3->search("2023", {"title"}, "title: foobarbaz", {}, {},
                            {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10,
                            "", 30, 5, "",
                            10, pinned_hits, {}, {}, 3,
                            "<mark>", "</mark>", {}, UINT_MAX,
                            true, false, true, "",
                            false, 6000 * 1000, 4, 7,
                            fallback, 4, {off}, INT16_MAX,
                            INT16_MAX, 2, filter_curated_hits ).get();
    ASSERT_EQ(0, results["hits"].size());

    // Filter should apply on curated results
    results = coll3->search("2023", {"title"}, "points: >70", {}, {},
                            {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10,
                            "", 30, 5, "",
                            10, pinned_hits, {}, {}, 3,
                            "<mark>", "</mark>", {}, UINT_MAX,
                            true, false, true, "",
                            false, 6000 * 1000, 4, 7,
                            fallback, 4, {off}, INT16_MAX,
                            INT16_MAX, 2, filter_curated_hits ).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());

    results = coll3->search("2023", {"title"}, "title: snapdragon", {}, {},
                            {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10,
                            "", 30, 5, "",
                            10, pinned_hits, {}, {}, 3,
                            "<mark>", "</mark>", {}, UINT_MAX,
                            true, false, true, "",
                            false, 6000 * 1000, 4, 7,
                            fallback, 4, {off}, INT16_MAX,
                            INT16_MAX, 2, filter_curated_hits).get();


    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][2]["document"]["id"].get<std::string>());

    //partial filter out ids, remaining will take higher precedence than their assignment
    results = coll3->search("snapdragon", {"title"}, "title: 2023", {}, {},
                            {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10,
                            "", 30, 5, "",
                            10, pinned_hits, {}, {}, 3,
                            "<mark>", "</mark>", {}, UINT_MAX,
                            true, false, true, "",
                            false, 6000 * 1000, 4, 7,
                            fallback, 4, {off}, INT16_MAX,
                            INT16_MAX, 2, filter_curated_hits).get();


    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("4", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][3]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, AvoidTypoMatchingWhenOverlapWithCuratedData) {
    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    Collection* coll3 = collectionManager.get_collection("coll3").get();
    if (coll3 == nullptr) {
        coll3 = collectionManager.create_collection("coll3", 1, fields, "points").get();
    }

    nlohmann::json doc;

    doc["title"] = "Snapdragon 7 gen 2023";
    doc["points"] = 100;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Snapdragon 732G 2023";
    doc["points"] = 91;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Mediatak 4 gen 2023";
    doc["points"] = 65;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Mediatek Dimensity 720G 2022";
    doc["points"] = 87;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    doc["title"] = "Mediatek Dimensity 470G 2023";
    doc["points"] = 63;
    ASSERT_TRUE(coll3->add(doc.dump()).ok());

    auto pinned_hits = "3:1, 4:2";

    auto results = coll3->search("Mediatek", {"title"}, "", {}, {},
                                 {2}, 50, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10,
                                 "", 30, 5, "",
                                 1, pinned_hits, {}, {}, 3,
                                 "<mark>", "</mark>", {}, UINT_MAX,
                                 true, false, true, "",
                                 false, 6000 * 1000, 4, 7,
                                 fallback, 4, {off}, INT16_MAX,
                                 INT16_MAX, 2, false).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("4", results["hits"][1]["document"]["id"].get<std::string>());

    // only typo match found: we should return both curated and typo hits
    results = coll3->search("snapdragan", {"title"}, "", {}, {},
                            {2}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10,
                            "", 30, 5, "",
                            10, pinned_hits, {}, {}, 3,
                            "<mark>", "</mark>", {}, UINT_MAX,
                            true, false, true, "",
                            false, 6000 * 1000, 4, 7,
                            fallback, 4, {off}, INT16_MAX,
                            INT16_MAX, 2, false).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("4", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, PinnedHitsAndFilteredFaceting) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "enable_nested_fields": true,
        "fields": [
          {"name": "someprop", "index": true, "type": "string" },
          {"name": "somefacet", "index": true, "type": "string", "facet": true },
          {"name": "someotherfacet", "index": true, "type": "string", "facet": true }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    nlohmann::json doc1 = R"({"id": "4711", "someprop": "doc 4711", "somefacet": "sfa", "someotherfacet": "sofa"})"_json;
    nlohmann::json doc2 = R"({"id": "4712", "someprop": "doc 4712", "somefacet": "sfb", "someotherfacet": "sofb"})"_json;
    nlohmann::json doc3 = R"({"id": "4713", "someprop": "doc 4713", "somefacet": "sfc", "someotherfacet": "sofc"})"_json;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    auto pinned_hits = "4712:1";
    bool filter_curated_hits = true;

    auto results = coll1->search("*", {}, "somefacet:=sfa", {"somefacet"}, {},
                                 {2}, 50, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10,
                                 "", 30, 5, "",
                                 1, pinned_hits, {}, {}, 3,
                                 "<mark>", "</mark>", {}, UINT_MAX,
                                 true, false, true, "",
                                 false, 6000 * 1000, 4, 7,
                                 fallback, 4, {off}, INT16_MAX,
                                 INT16_MAX, 2, filter_curated_hits).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("4711", results["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("sfa", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<int>());
}

TEST_F(CollectionCurationTest, OverridesWithSemanticSearch) {
    auto& ov_manager = CurationIndexManager::get_instance();
    auto schema_json = R"({
            "name": "products",
            "fields":[
            {
                "name": "product_name",
                        "type": "string"
            },
            {
                "name": "embedding",
                        "type": "float[]",
                        "embed": {
                    "from": [
                    "product_name"
                    ],
                    "model_config": {
                        "model_name": "ts/clip-vit-b-p32"
                    }
                }
            }
            ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto coll_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(coll_op.ok());
    auto coll = coll_op.get();
    coll->set_curation_sets({"index"});

    std::vector<std::string> products = {"Cell Phone", "Laptop", "Desktop", "Printer", "Keyboard", "Monitor", "Mouse"};
    nlohmann::json doc;
    for (auto product: products) {
        doc["product_name"] = product;
        ASSERT_TRUE(coll->add(doc.dump()).ok());
    }

    auto results = coll->search("phone", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                {"embedding"}).get();

    ASSERT_EQ(results["found"], 7);

    nlohmann::json curation_json = {
            {"id",   "exclude-rule"},
            {
             "rule", {
                             {"query", "phone"},
                             {"match", curation_t::MATCH_CONTAINS}
                     }
            }
    };
    curation_json["excludes"] = nlohmann::json::array();
    curation_json["excludes"][0] = nlohmann::json::object();
    curation_json["excludes"][0]["id"] = "0";

    curation_t curation;
    curation_t::parse(curation_json, "", curation);

    ASSERT_TRUE(ov_manager.upsert_curation_item("index", curation_json).ok());

    results = coll->search("phone", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                {"embedding"}).get();

    ASSERT_EQ(results["found"], 6);

    ASSERT_EQ(results["hits"][0]["document"]["id"], "4");
    ASSERT_EQ(results["hits"][1]["document"]["id"], "6");
    ASSERT_EQ(results["hits"][2]["document"]["id"], "1");
    ASSERT_EQ(results["hits"][3]["document"]["id"], "5");
    ASSERT_EQ(results["hits"][4]["document"]["id"], "2");
    ASSERT_EQ(results["hits"][5]["document"]["id"], "3");
}

TEST_F(CollectionCurationTest, NestedObjectOverride) {
    auto& ov_manager = CurationIndexManager::get_instance();
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "nested", "type": "object", "facet": true},
            {"name": "nested.brand", "type": "string", "facet": true},
            {"name": "nested.category", "type": "string", "facet": true}
        ],
        "enable_nested_fields": true
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();
    coll1->set_curation_sets({"index"});

    // Add documents with nested objects
    nlohmann::json doc1 = R"({
        "id": "0",
        "name": "Amazing Shoes",
        "nested": {
            "brand": "Nike",
            "category": "shoes"
        }
    })"_json;

    nlohmann::json doc2 = R"({
        "id": "1",
        "name": "Track Shoes",
        "nested": {
            "brand": "Adidas",
            "category": "shoes"
        }
    })"_json;

    nlohmann::json doc3 = R"({
        "id": "2",
        "name": "Running Shoes",
        "nested": {
            "brand": "Nike",
            "category": "sports"
        }
    })"_json;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC") };

    // Test dynamic filtering with nested object fields
    nlohmann::json curation_json = {
        {"id", "nested-dynamic-filter"},
        {
            "rule", {
                {"query", "{nested.brand} shoes"},
                {"match", curation_t::MATCH_CONTAINS}
            }
        },
        {"remove_matched_tokens", true},
        {"filter_by", "nested.brand:{nested.brand} && nested.category: shoes"},
        {"metadata", {{"filtered", true}}}
    };

    curation_t curation;
    auto op_curation = curation_t::parse(curation_json, "nested-dynamic-filter", curation);
    ASSERT_TRUE(op_curation.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    // Search with brand name
    auto results = coll1->search("nike shoes", {"name", "nested.brand", "nested.category"}, "",
                                {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_TRUE(results.contains("metadata"));
    ASSERT_TRUE(results["metadata"]["filtered"].get<bool>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, CurationWithGroupBy) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "title", "index": true, "type": "string" },
          {"name": "category", "index": true, "type": "string", "facet": true },
          {"name": "brand", "index": true, "type": "string", "facet": true }
        ]
    })"_json;
    auto& ov_manager = CurationIndexManager::get_instance();

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();
    coll1->set_curation_sets({"index"});

    // Add test documents
    nlohmann::json doc1 = R"({"id": "1", "title": "winter dress", "category": "clothing", "brand": "brandA"})"_json;
    nlohmann::json doc2 = R"({"id": "2", "title": "winter shoes", "category": "footwear", "brand": "brandB"})"_json;
    nlohmann::json doc3 = R"({"id": "3", "title": "winter hat", "category": "accessories", "brand": "brandA"})"_json;
    nlohmann::json doc4 = R"({"id": "4", "title": "winter coat", "category": "clothing", "brand": "brandB"})"_json;
    nlohmann::json doc5 = R"({"id": "5", "title": "winter bag", "category": "something-else", "brand": "brandA"})"_json;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());
    ASSERT_TRUE(coll1->add(doc5.dump()).ok());

    // Create curation rule that pins documents for exact query "summer"
    nlohmann::json curation_json = R"({
       "id": "summer-curation",
       "rule": {
            "query": "summer",
            "match": "exact"
        },
        "includes": [
            {"id": "3", "position": 1},
            {"id": "5", "position": 2}
        ]
    })"_json;

    curation_t curation_rule;
    auto parse_op = curation_t::parse(curation_json, "summer-curation", curation_rule);
    ASSERT_TRUE(parse_op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    // Test 1: Search without group_by - should show curated results first
    auto results_no_group = coll1->search("summer", {"title"}, "", {}, {},
                                          {0}, 50, 1, FREQUENCY,
                                          {false}, Index::DROP_TOKENS_THRESHOLD,
                                          spp::sparse_hash_set<std::string>(),
                                          spp::sparse_hash_set<std::string>(), 10,
                                          "", 30, 5, "",
                                         10, {}, {}, {}, 0).get();

    ASSERT_EQ(2, results_no_group["hits"].size());
    // First two should be curated (pinned) documents
    ASSERT_EQ("3", results_no_group["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("5", results_no_group["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ(true, results_no_group["hits"][0]["curated"].get<bool>());
    ASSERT_EQ(true, results_no_group["hits"][1]["curated"].get<bool>());

    // Test 2: Search with group_by category - should still show curated results
    auto results_with_group = coll1->search("summer", {"title"}, "", {}, {},
                                            {0}, 50, 1, FREQUENCY,
                                            {false}, Index::DROP_TOKENS_THRESHOLD,
                                            spp::sparse_hash_set<std::string>(),
                                            spp::sparse_hash_set<std::string>(), 10,
                                            "", 30, 5, "",
                                            10, {}, {}, {"category"}, 2).get();

    // Should have grouped results
    ASSERT_TRUE(results_with_group.contains("grouped_hits"));
    ASSERT_GE(results_with_group["grouped_hits"].size(), 1);

    // Look for curated results in grouped hits
    bool found_curated_doc3 = false;
    bool found_curated_doc5 = false;
    // Debug: Print the grouped results structure
    
    for (const auto& group : results_with_group["grouped_hits"]) {
        for (const auto& hit : group["hits"]) {
            std::string doc_id = hit["document"]["id"].get<std::string>();
            bool is_curated = hit.contains("curated") && hit["curated"].get<bool>();
            
            if (doc_id == "3" && is_curated) {
                found_curated_doc3 = true;
            }
            if (doc_id == "5" && is_curated) {
                found_curated_doc5 = true;
            }
        }
    }
    
    // Verify that curated documents are present and marked as curated
    ASSERT_TRUE(found_curated_doc3) << "Document 3 should be marked as curated in grouped results";
    ASSERT_TRUE(found_curated_doc5) << "Document 5 should be marked as curated in grouped results";

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionCurationTest, DynamicFilterMatchingMultipleRules) {
    nlohmann::json schema = R"({
        "name": "products",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "category", "type": "string"},
            {"name": "region", "type": "string"},
            {"name": "popularity", "type": "int32", "sort": true}
     ]
    })"_json;
    auto& ov_manager = CurationIndexManager::get_instance();

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();
    coll1->set_curation_sets({"index"});

    // Add test documents
    nlohmann::json doc1 = R"({"id":"1","title":"USB-C Charger","category":"Electronics","region":"act","popularity":50})"_json;
    nlohmann::json doc2 = R"({"id":"2","title":"Office Stapler","category":"Office","region":"act","popularity":30})"_json;
    nlohmann::json doc3 = R"({"id":"3","title":"Notebook","category":"Office","region":"nsw","popularity":70})"_json;
    nlohmann::json doc4 = R"( {"id":"4","title":"Bluetooth Speaker","category":"Electronics","region":"act","popularity":90})"_json;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());
    ASSERT_TRUE(coll1->add(doc2.dump()).ok());
    ASSERT_TRUE(coll1->add(doc3.dump()).ok());
    ASSERT_TRUE(coll1->add(doc4.dump()).ok());

    //without any curation
    auto results = coll1->search("*", {}, "region:=act`", {}, {}, {0}).get();
    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ("4", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());

    //now add curations
    nlohmann::json curation_json = R"({
       "id": "001-electronics",
       "rule": { "filter_by": "region:={region} && category:=`Electronics`" },
       "includes": [{"id": "1", "position": 1}],
       "sort_by": "popularity:desc",
       "stop_processing": true
    })"_json;

    curation_t curation_rule, curation_rule2;
    auto parse_op = curation_t::parse(curation_json, "001-electronics", curation_rule);
    ASSERT_TRUE(parse_op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    nlohmann::json curation_json2 = R"({
       "id": "002-electronics-or-office",
       "rule": { "filter_by": "region:={region} && (category:=`Electronics` || category:= `Office`) " },
       "includes": [{"id": "2", "position": 1}],
       "sort_by": "popularity:desc",
       "stop_processing": true
    })"_json;

    parse_op = curation_t::parse(curation_json2, "002-electronics-or-office", curation_rule2);
    ASSERT_TRUE(parse_op.ok());
    ov_manager.upsert_curation_item("index", curation_json2);

    // should match with curation2 only even though curation1 can be matched with filter_query
    results = coll1->search("*", {}, "region:=act && (category:=`Electronics` || category:=`Office`) ", {}, {}, {0}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("4", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
    ASSERT_EQ(true, results["hits"][0]["curated"].get<bool>());

    //this should match with curation1 only
    results = coll1->search("*", {}, "region:=act && category:=`Electronics`", {}, {}, {0}).get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("4", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ(true, results["hits"][0]["curated"].get<bool>());

    //should not match any curation even though subset of both curations
    results = coll1->search("*", {}, "region:=act`", {}, {}, {0}).get();

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ("4", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][2]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, DynamicFilterStandaloneParenTokenDeath) {
    auto& ov_manager = CurationIndexManager::get_instance();
    nlohmann::json schema = R"({
          "name": "products",
          "fields": [
              {"name": "title", "type": "string"},
              {"name": "category", "type": "string"},
              {"name": "region", "type": "string"},
              {"name": "popularity", "type": "int32", "sort": true}
          ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();

    // Add test documents
    ASSERT_TRUE(coll1->add(R"({"id":"1","title":"USB-C Charger","category":"Electronics","region":"act","popularity":50})").ok());
    ASSERT_TRUE(coll1->add(R"({"id":"2","title":"Office Stapler","category":"Office","region":"act","popularity":30})").ok());
    ASSERT_TRUE(coll1->add(R"({"id":"3","title":"Notebook","category":"Office","region":"nsw","popularity":70})").ok());
    ASSERT_TRUE(coll1->add(R"({"id":"4","title":"Bluetooth Speaker","category":"Electronics","region":"act","popularity":90})").ok());

    // Curation with a space after "( to force "(" to be a standalone token.
    nlohmann::json curation_json = R"OVR(
        {
        "id": "crash-standalone-paren",
        "rule": { "filter_by": "region:={region} && ( category:=`Electronics` )" },
        "includes": [],
        "sort_by": "popularity:desc",
        "stop_processing": true
        }
    )OVR"_json;

    curation_t ov;
    auto parse_op = curation_t::parse(curation_json, "crash-standalone-paren", ov);
    ASSERT_TRUE(parse_op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto res_op = coll1->search("*", {}, "region:=act && ( category:=`Electronics` )", {}, {}, {0});
    ASSERT_TRUE(res_op.ok());
    auto results = res_op.get();
    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ("4", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionCurationTest, DynamicOverridePlaceHolderFieldNameTypo) {
    auto& ov_manager = CurationIndexManager::get_instance();
    nlohmann::json schema = R"({
          "name": "products",
          "fields": [
              {"name": "title", "type": "string"},
              {"name": "categoryType", "type": "string"},
              {"name": "region", "type": "string"},
              {"name": "popularity", "type": "int32", "sort": true}
          ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll1 = op.get();
    coll1->set_curation_sets({"index"});

    // Add test documents
    ASSERT_TRUE(coll1->add(R"({"id":"1","title":"Office Charger","categoryType":"Electronics","region":"act","popularity":50})").ok());
    ASSERT_TRUE(coll1->add(R"({"id":"2","title":"Office Stapler","categoryType":"Office","region":"act","popularity":30})").ok());
    ASSERT_TRUE(coll1->add(R"({"id":"3","title":"Notebook","categoryType":"Office","region":"nsw","popularity":70})").ok());
    ASSERT_TRUE(coll1->add(R"({"id":"4","title":"Bluetooth Speaker","categoryType":"Electronics","region":"act","popularity":90})").ok());

    nlohmann::json curation_json = R"OVR(
        {
        "id": "placeholder_field",
        "rule": {
            "query": "{categoryType}",
            "match": "contains"
          },
          "filter_by": "categoryType:={categoryType}",
          "filter_curated_hits": false,
          "stop_processing": false,
          "metadata": {
            "text": "placeholder_field filter triggered"
          }
        }
    )OVR"_json;

    curation_t ov;
    auto parse_op = curation_t::parse(curation_json, "placeholder_field", ov);
    ASSERT_TRUE(parse_op.ok());
    ov_manager.upsert_curation_item("index", curation_json);

    auto res_op = coll1->search("Office", {"title"}, "", {}, {}, {0});
    ASSERT_TRUE(res_op.ok());
    auto results = res_op.get();
    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ("3", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("placeholder_field filter triggered", results["metadata"]["text"].get<std::string>());
}

TEST_F(CollectionCurationTest, DiversityOverrideParsing) {
    Collection* tags_coll = nullptr;
    auto schema_json =
            R"({
                "name": "tags",
                "fields": [
                    {"name": "app_id", "type": "string"},
                    {"name": "ui_elements.group_id", "type": "string[]"}
                ]
            })"_json;
    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto& ov_manager = CurationIndexManager::get_instance();

    tags_coll = collection_create_op.get();
    tags_coll->set_curation_sets({"index"});

    auto json =
            R"({
                  "diversity": {
                    "similarity_metric": [
                      {
                        "field": "flow_id",
                        "method": "equality",
                        "weight": 0.6
                      },
                      {
                        "field": "app_id",
                        "method": "equality"
                      },
                      {
                        "field": "ui_elements.group_id",
                        "method": "jaccard",
                        "weight": 0.1
                      }
                    ]
                  }
                })"_json;

    diversity_t diversity;
    auto op = diversity_t::parse(json, diversity);
    ASSERT_TRUE(op.ok());

    ASSERT_EQ(3, diversity.similarity_equation.size());
    ASSERT_EQ("flow_id", diversity.similarity_equation[0].field);
    ASSERT_EQ(diversity_t::similarity_methods::equality, diversity.similarity_equation[0].method);
    ASSERT_FLOAT_EQ(0.6, diversity.similarity_equation[0].weight);

    ASSERT_EQ("app_id", diversity.similarity_equation[1].field);
    ASSERT_EQ(diversity_t::similarity_methods::equality, diversity.similarity_equation[1].method);
    ASSERT_FLOAT_EQ(1, diversity.similarity_equation[1].weight);

    ASSERT_EQ("ui_elements.group_id", diversity.similarity_equation[2].field);
    ASSERT_EQ(diversity_t::similarity_methods::jaccard, diversity.similarity_equation[2].method);
    ASSERT_FLOAT_EQ(0.1, diversity.similarity_equation[2].weight);

    json["id"] = "foo";
    json["rule"]["tags"] += "screen_pattern_rule";

    nlohmann::json embedded_params;
    std::string json_res;
    long now_ts = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    std::map<std::string, std::string> req_params = {
        {"collection", "tags"},
        {"q", "*"},
        {"curation_tags", "screen_pattern_rule"}, // Diversity re-ranking using MMR algorithm.
    };

    curation_t curation;
    op = curation_t::parse(json, "", curation, "", {}, {});
    ASSERT_TRUE(op.ok());
    auto create_op = ov_manager.upsert_curation_item("index", json);
    auto search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ("`flow_id` field not found in the schema.", search_op.error());

    auto schema_changes = R"({
        "fields": [
            {"name": "flow_id", "type": "string", "sort": true}
        ]
    })"_json;
    auto alter_op = tags_coll->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    op = curation_t::parse(json, "", curation, "", {}, {});
    ASSERT_TRUE(op.ok());
    create_op = ov_manager.upsert_curation_item("index", json);
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ("Enable sorting/faceting on `app_id` field to use in diversity.", search_op.error());

    schema_changes = R"({
        "fields": [
            {"name": "app_id", "drop": true},
            {"name": "app_id", "type": "string", "facet": true}
        ]
    })"_json;
    alter_op = tags_coll->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    op = curation_t::parse(json, "", curation, "", {}, {});
    ASSERT_TRUE(op.ok());
    create_op = ov_manager.upsert_curation_item("index", json);
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_FALSE(search_op.ok());
    ASSERT_EQ("Enable faceting on `ui_elements.group_id` array field to use in diversity.", search_op.error());

    schema_changes = R"({
        "fields": [
            {"name": "ui_elements.group_id", "drop": true},
            {"name": "ui_elements.group_id", "type": "string[]", "facet": true}
        ]
    })"_json;
    alter_op = tags_coll->alter(schema_changes);
    ASSERT_TRUE(alter_op.ok());

    op = curation_t::parse(json, "", curation, "", {}, {});
    ASSERT_TRUE(op.ok());
    ASSERT_EQ("foo", curation.id);
    ASSERT_EQ(1, curation.rule.tags.size());
    ASSERT_EQ("screen_pattern_rule", *curation.rule.tags.begin());
    ASSERT_EQ(3, curation.diversity.similarity_equation.size());

    create_op = ov_manager.upsert_curation_item("index", json);
    ASSERT_TRUE(create_op.ok());

    //emulate restart
    collectionManager.dispose();
    delete store;

    store = new Store(state_dir_path);
    collectionManager.init(store, 1.0, "auth_key", quit);
    auto load_op = collectionManager.load(8, 1000);
    ASSERT_TRUE(load_op.ok());

    tags_coll = collectionManager.get_collection("tags").get();
    auto get_op = ov_manager.get_curation_item("index", "foo");
    ASSERT_TRUE(get_op.ok());

    op = curation_t::parse(json, "", curation, "", {}, {});
    ASSERT_TRUE(op.ok());

    ASSERT_EQ("foo", curation.id);
    ASSERT_EQ(1, curation.rule.tags.size());
    ASSERT_EQ("screen_pattern_rule", *curation.rule.tags.begin());
    ASSERT_EQ(3, curation.diversity.similarity_equation.size());
}

TEST_F(CollectionCurationTest, DiversityOverride) {
    Collection* tags_coll = nullptr;
    auto schema_json =
            R"({
                "name": "tags",
                "fields": [
                    {"name": "tags", "type": "string[]", "facet": true}
                ]
            })"_json;
    std::vector<nlohmann::json> documents = {
            R"({"tags": ["gold", "silver"]})"_json,
            R"({"tags": ["FINE PLATINUM"]})"_json,
            R"({"tags": ["bronze", "gold"]})"_json,
            R"({"tags": ["silver"]})"_json,
            R"({"tags": ["silver", "gold", "bronze"]})"_json,
            R"({"tags": ["silver", "FINE PLATINUM"]})"_json
    };
    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto& ov_manager = CurationIndexManager::get_instance();

    tags_coll = collection_create_op.get();
    tags_coll->set_curation_sets({"index"});
    for (auto const &json: documents) {
        auto add_op = tags_coll->add(json.dump());
        ASSERT_TRUE(add_op.ok());
    }

    std::map<std::string, std::string> req_params = {
            {"collection", "tags"},
            {"q", "*"}
    };
    nlohmann::json embedded_params;
    std::string json_res;
    long now_ts = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    auto search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());

    nlohmann::json res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(6, res_obj["hits"].size());
    for (uint32_t i = 0; i < 6; i++) {
        ASSERT_EQ(std::to_string(5 - i), res_obj["hits"][i]["document"]["id"]);
    }

    auto json =
            R"({
                  "id": "foo",
                  "rule": {
                    "tags": [
                      "screen_pattern_rule"
                    ]
                  },
                  "diversity": {
                    "similarity_metric": [
                      {
                        "field": "tags",
                        "method": "jaccard"
                      }
                    ]
                  }
                })"_json;
    curation_t curation;
    auto op = curation_t::parse(json, "", curation, "", {}, {});
    ASSERT_TRUE(op.ok());
    ov_manager.upsert_curation_item("index", json);

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"}, // Diversity re-ranking using MMR algorithm.
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());

    res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(6, res_obj["hits"].size());
    ASSERT_EQ("5", res_obj["hits"][0]["document"]["id"]);
    ASSERT_EQ("2", res_obj["hits"][1]["document"]["id"]);
    ASSERT_EQ("4", res_obj["hits"][2]["document"]["id"]);
    ASSERT_EQ("3", res_obj["hits"][3]["document"]["id"]);
    ASSERT_EQ("1", res_obj["hits"][4]["document"]["id"]);
    ASSERT_EQ("0", res_obj["hits"][5]["document"]["id"]);

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"},
            {"diversity_lambda", "1"} // No diversity
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(6, res_obj["hits"].size());
    for (uint32_t i = 0; i < 6; i++) {
        ASSERT_EQ(std::to_string(5 - i), res_obj["hits"][i]["document"]["id"]);
    }

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"}, // Diversity re-ranking using MMR algorithm.
            {"page", "1"},
            {"per_page", "2"}
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(2, res_obj["hits"].size());
    ASSERT_EQ("5", res_obj["hits"][0]["document"]["id"]);
    ASSERT_EQ("2", res_obj["hits"][1]["document"]["id"]);

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"}, // Diversity re-ranking using MMR algorithm.
            {"page", "2"},
            {"per_page", "2"}
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(2, res_obj["hits"].size());
    ASSERT_EQ("4", res_obj["hits"][0]["document"]["id"]);
    ASSERT_EQ("3", res_obj["hits"][1]["document"]["id"]);

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"}, // Diversity re-ranking using MMR algorithm.
            {"page", "3"},
            {"per_page", "2"}
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(2, res_obj["hits"].size());
    ASSERT_EQ("1", res_obj["hits"][0]["document"]["id"]);
    ASSERT_EQ("0", res_obj["hits"][1]["document"]["id"]);

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"}, // Diversity re-ranking using MMR algorithm.
            {"page", "4"},
            {"per_page", "2"}
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(0, res_obj["hits"].size());

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"},
            {"diversity_limit", "3"} // Only diversify first n hits
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);LOG(INFO) << res_obj.dump(2);
    ASSERT_EQ(6, res_obj["found"].get<size_t>());
    ASSERT_EQ(6, res_obj["hits"].size());
    for (uint32_t i = 0; i < 6; i++) {
        ASSERT_EQ(std::to_string(5 - i), res_obj["hits"][i]["document"]["id"]);
    }

    req_params = {
            {"collection", "tags"},
            {"q", "*"},
            {"curation_tags", "screen_pattern_rule"},
            {"diversity_limit", "4"} // Only diversify first n hits
    };
    search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());
    res_obj = nlohmann::json::parse(json_res);
    ASSERT_EQ("5", res_obj["hits"][0]["document"]["id"]);
    ASSERT_EQ("2", res_obj["hits"][1]["document"]["id"]);
    ASSERT_EQ("4", res_obj["hits"][2]["document"]["id"]);
    ASSERT_EQ("3", res_obj["hits"][3]["document"]["id"]);
    ASSERT_EQ("1", res_obj["hits"][4]["document"]["id"]);
    ASSERT_EQ("0", res_obj["hits"][5]["document"]["id"]);
}
