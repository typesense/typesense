#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionOverrideTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    Collection *coll_mul_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_override";
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

        coll_mul_fields = collectionManager.get_collection("coll_mul_fields").get();
        if(coll_mul_fields == nullptr) {
            coll_mul_fields = collectionManager.create_collection("coll_mul_fields", 4, fields, "points").get();
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
        collectionManager.drop_collection("coll_mul_fields");
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionOverrideTest, ExcludeIncludeExactQueryMatch) {
    Config::get_instance().set_enable_search_analytics(true);

    nlohmann::json override_json = {
            {"id",   "exclude-rule"},
            {
             "rule", {
                             {"query", "of"},
                             {"match", override_t::MATCH_EXACT}
                     }
            }
    };
    override_json["excludes"] = nlohmann::json::array();
    override_json["excludes"][0] = nlohmann::json::object();
    override_json["excludes"][0]["id"] = "4";

    override_json["excludes"][1] = nlohmann::json::object();
    override_json["excludes"][1]["id"] = "11";

    override_t override;
    override_t::parse(override_json, "", override);

    coll_mul_fields->add_override(override);

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
    nlohmann::json override_json_include = {
            {"id",   "include-rule"},
            {
             "rule", {
                             {"query", "in"},
                             {"match", override_t::MATCH_EXACT}
                     }
            }
    };
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "0";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "3";
    override_json_include["includes"][1]["position"] = 2;

    override_t override_include;
    override_t::parse(override_json_include, "", override_include);

    coll_mul_fields->add_override(override_include);

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

    coll_mul_fields->remove_override("exclude-rule");
    coll_mul_fields->remove_override("include-rule");

    // contains cases

    nlohmann::json override_contains_inc = {
            {"id",   "include-rule"},
            {
             "rule", {
                             {"query", "will"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            }
    };
    override_contains_inc["includes"] = nlohmann::json::array();
    override_contains_inc["includes"][0] = nlohmann::json::object();
    override_contains_inc["includes"][0]["id"] = "0";
    override_contains_inc["includes"][0]["position"] = 1;

    override_contains_inc["includes"][1] = nlohmann::json::object();
    override_contains_inc["includes"][1]["id"] = "1";
    override_contains_inc["includes"][1]["position"] = 7;  // purposely setting it way out

    override_t override_inc_contains;
    override_t::parse(override_contains_inc, "", override_inc_contains);

    coll_mul_fields->add_override(override_inc_contains);

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

    // ability to disable overrides
    bool enable_overrides = false;
    res_op = coll_mul_fields->search("will", {"title"}, "", {}, {}, {0}, 10,
                                     1, FREQUENCY, {false}, 0, spp::sparse_hash_set<std::string>(),
                                     spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 0, {}, {}, {}, 0,
                                     "<mark>", "</mark>", {1}, 10000, true, false, enable_overrides);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<uint32_t>());

    ASSERT_STREQ("3", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    enable_overrides = true;
    res_op = coll_mul_fields->search("will", {"title"}, "", {}, {}, {0}, 10,
                                     1, FREQUENCY, {false}, 0, spp::sparse_hash_set<std::string>(),
                                     spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 0, {}, {}, {}, 0,
                                     "<mark>", "</mark>", {1}, 10000, true, false, enable_overrides);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(4, results["found"].get<uint32_t>());

    coll_mul_fields->remove_override("include-rule");
    Config::get_instance().set_enable_search_analytics(false);
}

TEST_F(CollectionOverrideTest, OverrideJSONValidation) {
    nlohmann::json exclude_json = {
            {"id", "exclude-rule"},
            {
             "rule", {
                       {"query", "of"},
                       {"match", override_t::MATCH_EXACT}
                   }
            }
    };

    exclude_json["excludes"] = nlohmann::json::array();
    exclude_json["excludes"][0] = nlohmann::json::object();
    exclude_json["excludes"][0]["id"] = 11;

    override_t override1;
    auto parse_op = override_t::parse(exclude_json, "", override1);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Exclusion `id` must be a string.", parse_op.error().c_str());

    nlohmann::json include_json = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", override_t::MATCH_EXACT}
                   }
            }
    };

    include_json["includes"] = nlohmann::json::array();
    include_json["includes"][0] = nlohmann::json::object();
    include_json["includes"][0]["id"] = "11";

    override_t override2;
    parse_op = override_t::parse(include_json, "", override2);

    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Inclusion definition must define both `id` and `position` keys.", parse_op.error().c_str());

    include_json["includes"][0]["position"] = "1";

    parse_op = override_t::parse(include_json, "", override2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Inclusion `position` must be an integer.", parse_op.error().c_str());

    include_json["includes"][0]["position"] = 1;
    parse_op = override_t::parse(include_json, "", override2);
    ASSERT_TRUE(parse_op.ok());

    nlohmann::json include_json2 = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", override_t::MATCH_EXACT}
                   }
            }
    };

    parse_op = override_t::parse(include_json2, "", override2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("Must contain one of: `includes`, `excludes`, `metadata`, `filter_by`, `sort_by`, "
                 "`remove_matched_tokens`, `replace_query`.", parse_op.error().c_str());

    include_json2["includes"] = nlohmann::json::array();
    include_json2["includes"][0] = 100;

    parse_op = override_t::parse(include_json2, "", override2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("The `includes` value must be an array of objects.", parse_op.error().c_str());

    nlohmann::json exclude_json2 = {
            {"id", "exclude-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", override_t::MATCH_EXACT}
                   }
            }
    };

    exclude_json2["excludes"] = nlohmann::json::array();
    exclude_json2["excludes"][0] = "100";

    parse_op = override_t::parse(exclude_json2, "", override2);
    ASSERT_FALSE(parse_op.ok());
    ASSERT_STREQ("The `excludes` value must be an array of objects.", parse_op.error().c_str());
}

TEST_F(CollectionOverrideTest, IncludeHitsFilterOverrides) {
    // Check facet field highlight for overridden results
    nlohmann::json override_json_include = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "not-found"},
                           {"match", override_t::MATCH_EXACT}
                   }
            },
            {"metadata", {{"foo", "bar"}}},
    };

    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "0";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "2";
    override_json_include["includes"][1]["position"] = 2;

    override_json_include["filter_curated_hits"] = true;

    override_t override_include;
    override_t::parse(override_json_include, "", override_include);
    coll_mul_fields->add_override(override_include);

    std::map<std::string, override_t*> overrides = coll_mul_fields->get_overrides().get();
    ASSERT_EQ(1, overrides.size());
    auto override_json = overrides.at("include-rule")->to_json();
    ASSERT_TRUE(override_json.contains("filter_curated_hits"));
    ASSERT_TRUE(override_json["filter_curated_hits"].get<bool>());

    auto results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("bar", results["metadata"]["foo"].get<std::string>());

    // disable filter curation option
    override_json_include["filter_curated_hits"] = false;
    override_t::parse(override_json_include, "", override_include);
    coll_mul_fields->add_override(override_include);
    results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ(2, results["hits"].size());

    // remove filter curation option: by default no filtering should be done
    override_json_include.erase("filter_curated_hits");
    override_t::parse(override_json_include, "", override_include);
    coll_mul_fields->add_override(override_include);
    results = coll_mul_fields->search("not-found", {"title"}, "points:>70", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ(2, results["hits"].size());

    // query param configuration should take precedence over override level config
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

    override_json_include["filter_curated_hits"] = false;
    override_t::parse(override_json_include, "", override_include);
    coll_mul_fields->add_override(override_include);

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
    override_json_include["filter_curated_hits"] = true;
    override_t::parse(override_json_include, "", override_include);
    coll_mul_fields->add_override(override_include);

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

TEST_F(CollectionOverrideTest, ExcludeIncludeFacetFilterQuery) {
    // Check facet field highlight for overridden results
    nlohmann::json override_json_include = {
        {"id", "include-rule"},
        {
         "rule", {
                   {"query", "not-found"},
                   {"match", override_t::MATCH_EXACT}
               }
        }
    };

    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "0";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "2";
    override_json_include["includes"][1]["position"] = 2;

    override_t override_include;
    override_t::parse(override_json_include, "", override_include);

    coll_mul_fields->add_override(override_include);

    std::map<std::string, override_t*> overrides = coll_mul_fields->get_overrides().get();
    ASSERT_EQ(1, overrides.size());
    auto override_json = overrides.at("include-rule")->to_json();
    ASSERT_FALSE(override_json.contains("filter_by"));
    ASSERT_TRUE(override_json.contains("remove_matched_tokens"));
    ASSERT_TRUE(override_json.contains("filter_curated_hits"));
    ASSERT_FALSE(override_json["remove_matched_tokens"].get<bool>());
    ASSERT_FALSE(override_json["filter_curated_hits"].get<bool>());

    auto results = coll_mul_fields->search("not-found", {"title"}, "", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will").get();

    ASSERT_EQ("<mark>Will</mark> Ferrell", results["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>());
    ASSERT_EQ("Will Ferrell", results["facet_counts"][0]["counts"][0]["value"].get<std::string>());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"][0]["count"].get<size_t>());

    coll_mul_fields->remove_override("include-rule");

    // facet count is okay when results are excluded
    nlohmann::json override_json_exclude = {
        {"id",   "exclude-rule"},
        {
         "rule", {
                     {"query", "the"},
                     {"match", override_t::MATCH_EXACT}
                 }
        }
    };
    override_json_exclude["excludes"] = nlohmann::json::array();
    override_json_exclude["excludes"][0] = nlohmann::json::object();
    override_json_exclude["excludes"][0]["id"] = "10";

    override_t override;
    override_t::parse(override_json_exclude, "", override);

    coll_mul_fields->add_override(override);

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

    coll_mul_fields->remove_override("exclude-rule");

    // now with per_page = 1, and an include query

    coll_mul_fields->add_override(override_include);
    results = coll_mul_fields->search("not-found", {"title"}, "", {"starring"}, {}, {0}, 1, 1, FREQUENCY,
                                      {false}, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // should be able to replace existing override
    override_include.rule.query = "found";
    coll_mul_fields->add_override(override_include);
    ASSERT_STREQ("found", coll_mul_fields->get_overrides().get()["include-rule"]->rule.query.c_str());

    coll_mul_fields->remove_override("include-rule");
}

TEST_F(CollectionOverrideTest, FilterCuratedHitsSlideToCoverMissingSlots) {
    // when some of the curated hits are filtered away, lower ranked hits must be pulled up
    nlohmann::json override_json_include = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "scott"},
                           {"match", override_t::MATCH_EXACT}
                   }
            }
    };

    // first 2 hits won't match the filter, 3rd position should float up to position 1
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "7";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "17";
    override_json_include["includes"][1]["position"] = 2;

    override_json_include["includes"][2] = nlohmann::json::object();
    override_json_include["includes"][2]["id"] = "10";
    override_json_include["includes"][2]["position"] = 3;

    override_json_include["filter_curated_hits"] = true;

    override_t override_include;
    override_t::parse(override_json_include, "", override_include);
    coll_mul_fields->add_override(override_include);

    auto results = coll_mul_fields->search("scott", {"starring"}, "points:>55", {}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("10", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("11", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("12", results["hits"][2]["document"]["id"].get<std::string>());

    // another curation where there is an ID missing in the middle
    override_json_include = {
        {"id", "include-rule"},
        {
         "rule", {
                   {"query", "glenn"},
                   {"match", override_t::MATCH_EXACT}
               }
        }
    };

    // middle hit ("10") will not satisfy filter, so "11" will move to position 2
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "9";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "10";
    override_json_include["includes"][1]["position"] = 2;

    override_json_include["includes"][2] = nlohmann::json::object();
    override_json_include["includes"][2]["id"] = "11";
    override_json_include["includes"][2]["position"] = 3;

    override_json_include["filter_curated_hits"] = true;

    override_t override_include2;
    override_t::parse(override_json_include, "", override_include2);
    coll_mul_fields->add_override(override_include2);

    results = coll_mul_fields->search("glenn", {"starring"}, "points:[43,86]", {}, {}, {0}, 10, 1, FREQUENCY,
                                           {false}, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("9", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("11", results["hits"][1]["document"]["id"].get<std::string>());
}

TEST_F(CollectionOverrideTest, SimpleOverrideStopProcessing) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json_include = {
            {"id", "include-rule-1"},
            {
             "rule", {
                           {"query", "shoes"},
                           {"match", override_t::MATCH_EXACT}
                   }
            },
            {"stop_processing", false}
    };

    // first 2 hits won't match the filter, 3rd position should float up to position 1
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "2";
    override_json_include["includes"][0]["position"] = 1;

    override_t override_include1;
    auto op = override_t::parse(override_json_include, "include-rule-1", override_include1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_include1);

    override_json_include["id"] = "include-rule-2";
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "1";
    override_json_include["includes"][0]["position"] = 2;

    override_t override_include2;
    op = override_t::parse(override_json_include, "include-rule-2", override_include2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_include2);

    auto results = coll1->search("shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][2]["document"]["id"].get<std::string>());

    // now with stop processing enabled for the first rule
    override_include1.stop_processing = true;
    coll1->add_override(override_include1);

    results = coll1->search("shoes", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // check that default value for stop_processing is true

    nlohmann::json override_json_test = {
        {"id", "include-rule-test"},
        {
         "rule", {
                   {"query", "fast"},
                   {"match", override_t::MATCH_CONTAINS}
               }
        },
    };

    override_json_test["includes"] = nlohmann::json::array();
    override_json_test["includes"][0] = nlohmann::json::object();
    override_json_test["includes"][0]["id"] = "2";
    override_json_test["includes"][0]["position"] = 1;

    override_t override_include_test;
    op = override_t::parse(override_json_test, "include-rule-test", override_include_test);
    ASSERT_TRUE(op.ok());
    ASSERT_TRUE(override_include_test.stop_processing);
}

TEST_F(CollectionOverrideTest, IncludeOverrideWithFilterBy) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json_include = {
            {"id", "include-rule-1"},
            {
             "rule", {
                           {"query", "shoes"},
                           {"match", override_t::MATCH_EXACT}
                   }
            },
            {"filter_curated_hits", false},
            {"stop_processing", false},
            {"remove_matched_tokens", false},
            {"filter_by", "price: >55"}
    };

    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "2";
    override_json_include["includes"][0]["position"] = 1;

    override_t override_include1;
    auto op = override_t::parse(override_json_include, "include-rule-1", override_include1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_include1);

    auto results = coll1->search("shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // when bad filter by clause is used in override
    override_json_include = {
            {"id", "include-rule-2"},
            {
             "rule", {
                           {"query", "test"},
                           {"match", override_t::MATCH_EXACT}
                   }
            },
            {"filter_curated_hits", false},
            {"stop_processing", false},
            {"remove_matched_tokens", false},
            {"filter_by", "price >55"}
    };

    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "2";
    override_json_include["includes"][0]["position"] = 1;

    override_t override_include2;
    op = override_t::parse(override_json_include, "include-rule-2", override_include2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_include2);

    results = coll1->search("random-name", {"name"}, "",
                             {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, results["hits"].size());
}

TEST_F(CollectionOverrideTest, ReplaceQuery) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "boots",
            "match": "exact"
        },
        "replace_query": "shoes"
    })"_json;

    override_t override_rule;
    auto op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    auto results = coll1->search("boots", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // don't allow both remove_matched_tokens and replace_query
    override_json["remove_matched_tokens"] = true;
    op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Only one of `replace_query` or `remove_matched_tokens` can be specified.", op.error());

    // it's okay when it's explicitly set to false
    override_json["remove_matched_tokens"] = false;
    op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
}

TEST_F(CollectionOverrideTest, RuleQueryMustBeCaseInsensitive) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "GrEat",
            "match": "contains"
        },
        "replace_query": "amazing"
    })"_json;

    override_t override_rule;
    auto op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    override_json = R"({
       "id": "rule-2",
       "rule": {
            "query": "BaLL",
            "match": "contains"
        },
        "filter_by": "points: 1"
    })"_json;

    override_t override_rule2;
    op = override_t::parse(override_json, "rule-2", override_rule2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule2);

    auto results = coll1->search("great shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("ball", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionOverrideTest, RuleQueryWithAccentedChars) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("color", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Green";
    doc1["color"] = "Green";
    doc1["points"] = 30;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json override_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "Grün",
            "match": "contains"
        },
        "filter_by":"color:green",
        "filter_curated_hits":true
    })"_json;

    override_t override_rule;
    auto op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    auto results = coll1->search("grün", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionOverrideTest, WindowForRule) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["points"] = 30;
    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json override_json = R"({
       "id": "rule-1",
       "rule": {
            "query": "boots",
            "match": "exact"
        },
        "replace_query": "shoes"
    })"_json;

    override_t override_rule;
    auto op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    auto results = coll1->search("boots", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // rule must not match when window_start is set into the future
    override_json["effective_from_ts"] = 35677971263;  // year 3100, here we come! ;)
    op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    results = coll1->search("boots", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // rule must not match when window_end is set into the past
    override_json["effective_from_ts"] = -1;
    override_json["effective_to_ts"] = 965388863;
    op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    results = coll1->search("boots", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // resetting both should bring the override back in action
    override_json["effective_from_ts"] = 965388863;
    override_json["effective_to_ts"] = 35677971263;
    op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    results = coll1->search("boots", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionOverrideTest, FilterRule) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json = R"({
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

    override_t override_rule;
    auto op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    auto results = coll1->search("*", {}, "points: 50",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    // empty query should not trigger override even though it will be deemed as wildcard search
    results = coll1->search("", {"name"}, "points: 50",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // check to_json
    nlohmann::json override_json_ser = override_rule.to_json();
    ASSERT_EQ("points: 50", override_json_ser["rule"]["filter_by"]);

    // without q/match
    override_json = R"({
       "id": "rule-2",
       "rule": {
            "filter_by": "points: 1"
        },
        "includes": [{
            "id": "0",
            "position": 1
        }]
    })"_json;

    override_t override_rule2;
    op = override_t::parse(override_json, "rule-2", override_rule2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule2);

    results = coll1->search("socks", {"name"}, "points: 1",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    override_json_ser = override_rule2.to_json();
    ASSERT_EQ("points: 1", override_json_ser["rule"]["filter_by"]);
    ASSERT_EQ(0, override_json_ser["rule"].count("query"));
    ASSERT_EQ(0, override_json_ser["rule"].count("match"));
}

TEST_F(CollectionOverrideTest, CurationGroupingNonCuratedHitsShouldNotAppearOutside) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("group_id", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 3, fields).get();
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

    nlohmann::json override_json = R"({
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

    override_t override_rule;
    auto op = override_t::parse(override_json, "rule-1", override_rule);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule);

    override_json = R"({
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

    override_t override_rule2;
    op = override_t::parse(override_json, "rule-2", override_rule2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_rule2);

    auto results = coll1->search("*", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10,
                                 "", {}, {"group_id"}, 2).get();

    // when only one of the 2 records belonging to a record is used for curation, the other record
    // should not appear back

    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_EQ(1, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    // same for keyword search
    results = coll1->search("the", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10,
                            "", {}, {"group_id"}, 2).get();

    // when only one of the 2 records belonging to a record is used for curation, the other record
    // should not appear back

    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_EQ(1, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionOverrideTest, PinnedAndHiddenHits) {
    auto pinned_hits = "13:1,4:2";

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

    // take precedence over override rules

    nlohmann::json override_json_include = {
            {"id", "include-rule"},
            {
             "rule", {
                           {"query", "the"},
                           {"match", override_t::MATCH_EXACT}
                   }
            }
    };

    // trying to include an ID that is also being hidden via `hidden_hits` query param will not work
    // as pinned and hidden hits will take precedence over override rules
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "11";
    override_json_include["includes"][0]["position"] = 2;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "8";
    override_json_include["includes"][1]["position"] = 1;

    override_t override_include;
    override_t::parse(override_json_include, "", override_include);

    coll_mul_fields->add_override(override_include);

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

TEST_F(CollectionOverrideTest, PinnedHitsSmallerThanPageSize) {
    auto pinned_hits = "17:1,13:4,11:3";

    // pinned hits larger than page size: check that pagination works

    // without overrides:
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

TEST_F(CollectionOverrideTest, PinnedHitsLargerThanPageSize) {
    auto pinned_hits = "6:1,1:2,16:3,11:4";

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

TEST_F(CollectionOverrideTest, PinnedHitsWhenThereAreNotEnoughResults) {
    auto pinned_hits = "6:1,1:2,11:5";

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

TEST_F(CollectionOverrideTest, HiddenHitsHidingSingleResult) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

TEST_F(CollectionOverrideTest, PinnedHitsGrouping) {
    auto pinned_hits = "6:1,8:1,1:2,13:3,4:3";

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

    ASSERT_EQ(8, results["found"].get<size_t>());

    ASSERT_EQ(1, results["grouped_hits"][0]["group_key"].size());
    ASSERT_EQ(2, results["grouped_hits"][0]["group_key"][0].size());
    ASSERT_STREQ("Chris Evans", results["grouped_hits"][0]["group_key"][0][0].get<std::string>().c_str());
    ASSERT_STREQ("Scarlett Johansson", results["grouped_hits"][0]["group_key"][0][1].get<std::string>().c_str());

    ASSERT_STREQ("6", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("8", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("1", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("13", results["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["grouped_hits"][2]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("11", results["grouped_hits"][3]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("16", results["grouped_hits"][4]["hits"][0]["document"]["id"].get<std::string>().c_str());
}

TEST_F(CollectionOverrideTest, PinnedHitsGroupingNonPinnedHitsShouldNotAppearOutside) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("group_id", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 3, fields).get();
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

    // when only one of the 2 records belonging to a record is used for curation, the other record
    // should not appear back

    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_EQ(1, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    // same for keyword search
    results = coll1->search("the", {"title"}, "", {}, {}, {0}, 50, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10,
                            pinned_hits, {}, {"group_id"}, 2).get();

    // when only one of the 2 records belonging to a record is used for curation, the other record
    // should not appear back

    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_EQ(1, results["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, results["grouped_hits"][1]["hits"].size());

    ASSERT_EQ("2", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("3", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionOverrideTest, PinnedHitsWithWildCardQuery) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 3, fields, "points").get();
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

TEST_F(CollectionOverrideTest, PinnedHitsIdsHavingColon) {
    Collection *coll1;

    std::vector<field> fields = {field("url", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 4, fields, "points").get();
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

TEST_F(CollectionOverrideTest, DynamicFilteringExactMatchBasics) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    // with override, results will be different

    nlohmann::json override_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                         {"query", "{category}"},
                         {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category}"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    override_json = {
            {"id",   "dynamic-brand-cat-filter"},
            {
             "rule", {
                             {"query", "{brand} {category}"},
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category} && brand: {brand}"}
    };

    op = override_t::parse(override_json, "dynamic-brand-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    override_json = {
            {"id",   "dynamic-brand-filter"},
            {
             "rule", {
                     {"query", "{brand}"},
                     {"match", override_t::MATCH_EXACT}
                 }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "brand: {brand}"}
    };

    override_json["includes"] = nlohmann::json::array();
    override_json["includes"][0] = nlohmann::json::object();
    override_json["includes"][0]["id"] = "0";
    override_json["includes"][0]["position"] = 1;

    op = override_t::parse(override_json, "dynamic-brand-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

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

    // with bad override

    nlohmann::json override_json_bad1 = {
            {"id",   "dynamic-filters-bad1"},
            {
             "rule", {
                         {"query", "{brand}"},
                         {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", ""}
    };

    override_t override_bad1;
    op = override_t::parse(override_json_bad1, "dynamic-filters-bad1", override_bad1);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `filter_by` must be a non-empty string.", op.error());

    nlohmann::json override_json_bad2 = {
            {"id",   "dynamic-filters-bad2"},
            {
             "rule", {
                             {"query", "{brand}"},
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", {"foo", "bar"}}
    };

    override_t override_bad2;
    op = override_t::parse(override_json_bad2, "dynamic-filters-bad2", override_bad2);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `filter_by` must be a string.", op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringPrefixMatchShouldNotWork) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    // with override, results will be different

    nlohmann::json override_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                             {"query", "{category}"},
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category}"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    auto results = coll1->search("shoe", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringMissingField) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoes";
    doc1["points"] = 3;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json override_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                             {"query", "{categories}"},             // this field does NOT exist
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {categories}"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    auto results = coll1->search("shoes", {"name", "category"}, "",
                            {}, sort_fields, {2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringBadFilterBy) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "Amazing Shoes";
    doc1["category"] = "shoes";
    doc1["points"] = 3;

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    nlohmann::json override_json = {
            {"id",   "dynamic-cat-filter"},
            {
             "rule", {
                             {"query", "{category}"},             // this field does NOT exist
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "category: {category} && foo"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    auto results = coll1->search("shoes", {"name", "category"}, "",
                                 {}, sort_fields, {2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringMultiplePlaceholders) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "{brand} {color} shoes"},
                                            {"match", override_t::MATCH_CONTAINS}
                                    }
            },
            {"remove_matched_tokens", true},
            {"filter_by",           "brand: {brand} && color: {color}"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

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

TEST_F(CollectionOverrideTest, DynamicFilteringTokensBetweenPlaceholders) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "{brand} shoes {color}"},
                                            {"match", override_t::MATCH_CONTAINS}
                                    }
            },
            {"remove_matched_tokens", true},
            {"filter_by",           "brand: {brand} && color: {color}"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    auto results = coll1->search("Nike Air Jordan shoes yellow", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringWithNumericalFilter) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "popular {brand} shoes"},
                                            {"match", override_t::MATCH_CONTAINS}
                                    }
            },
            {"remove_matched_tokens", false},
            {"filter_by",           "brand: {brand} && points:> 10"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());

    auto results = coll1->search("popular nike shoes", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();
    ASSERT_EQ(4, results["hits"].size());

    coll1->add_override(override);

    results = coll1->search("popular nike shoes", {"name", "category", "brand"}, "",
                                 {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false}, 10).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // when overrides are disabled

    bool enable_overrides = false;
    results = coll1->search("popular nike shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10, 1, FREQUENCY, {false, false, false}, 10,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, {}, {}, {}, 0,
                            "<mark>", "</mark>", {1, 1, 1}, 10000, true, false, enable_overrides).get();
    ASSERT_EQ(4, results["hits"].size());

    // should not match the defined override

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

TEST_F(CollectionOverrideTest, DynamicFilteringExactMatch) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("color", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json = {
            {"id",                  "dynamic-cat-filter"},
            {
             "rule",                {
                                            {"query", "popular {brand} shoes"},
                                            {"match", override_t::MATCH_EXACT}
                                    }
            },
            {"remove_matched_tokens", false},
            {"filter_by",           "brand: {brand} && points:> 10"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-cat-filter", override);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override);

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

TEST_F(CollectionOverrideTest, DynamicFilteringWithSynonyms) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    coll1->add_synonym(R"({"id": "sneakers-shoes", "root": "sneakers", "synonyms": ["shoes"]})"_json);
    coll1->add_synonym(R"({"id": "boots-shoes", "root": "boots", "synonyms": ["shoes"]})"_json);
    coll1->add_synonym(R"({"id": "exciting-amazing", "root": "exciting", "synonyms": ["amazing"]})"_json);

    std::vector<sort_by> sort_fields = { sort_by("_text_match", "DESC"), sort_by("points", "DESC") };

    // spaces around field name should still work e.g. "{ field }"
    nlohmann::json override_json1 = {
        {"id",   "dynamic-filters"},
        {
         "rule", {
                     {"query", "{ category }"},
                     {"match", override_t::MATCH_EXACT}
                 }
        },
        {"filter_by", "category: {category}"}
    };

    override_t override1;
    auto op = override_t::parse(override_json1, "dynamic-filters", override1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override1);

    std::map<std::string, override_t*> overrides = coll1->get_overrides().get();
    ASSERT_EQ(1, overrides.size());
    auto override_json = overrides.at("dynamic-filters")->to_json();
    ASSERT_EQ("category: {category}", override_json["filter_by"].get<std::string>());
    ASSERT_EQ(true, override_json["remove_matched_tokens"].get<bool>());  // must be true by default

    nlohmann::json override_json2 = {
        {"id",   "static-filters"},
        {
         "rule", {
                     {"query", "exciting"},
                     {"match", override_t::MATCH_CONTAINS}
                 }
        },
        {"remove_matched_tokens", true},
        {"filter_by", "points: [5, 4]"}
    };

    override_t override2;
    op = override_t::parse(override_json2, "static-filters", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

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

    // keyword has no override, but synonym's override is used
    results = coll1->search("exciting", {"name", "category", "brand"}, "",
                            {}, sort_fields, {2, 2, 2}, 10).get();

    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, StaticFiltering) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "expensive"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:> 100"}
    };

    override_t override_contains;
    auto op = override_t::parse(override_json_contains, "static-filters", override_contains);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_contains);

    nlohmann::json override_json_exact = {
            {"id",   "static-exact-filters"},
            {
             "rule", {
                             {"query", "cheap"},
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:< 100"}
    };

    override_t override_exact;
    op = override_t::parse(override_json_exact, "static-exact-filters", override_exact);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_exact);

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

    // with synonym for expensive: should NOT match as synonyms are resolved after override substitution
    coll1->add_synonym(R"({"id": "costly-expensive", "root": "costly", "synonyms": ["expensive"]})"_json);

    results = coll1->search("costly", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, StaticFilteringMultipleRuleMatch) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_filter1_json = {
            {"id",   "static-filter-1"},
            {
             "rule", {
                             {"query", "twitter"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: twitter"}
    };

    override_t override_filter1;
    auto op = override_t::parse(override_filter1_json, "static-filter-1", override_filter1);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_filter1);

    nlohmann::json override_filter2_json = {
            {"id",   "static-filter-2"},
            {
             "rule", {
                             {"query", "starred"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: starred"}
    };

    override_t override_filter2;
    op = override_t::parse(override_filter2_json, "static-filter-2", override_filter2);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_filter2);

    auto results = coll1->search("starred twitter", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    // when stop_processing is enabled (default is true)
    override_filter1_json.erase("stop_processing");
    override_filter2_json.erase("stop_processing");

    override_t override_filter1_reset;
    op = override_t::parse(override_filter1_json, "static-filter-1", override_filter1_reset);
    ASSERT_TRUE(op.ok());
    override_t override_filter2_reset;
    op = override_t::parse(override_filter2_json, "static-filter-2", override_filter2_reset);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_filter1_reset);
    coll1->add_override(override_filter2_reset);

    results = coll1->search("starred twitter", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringMultipleRuleMatch) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("brand", field_types::STRING, false),
                                 field("tags", field_types::STRING_ARRAY, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_filter1_json = {
            {"id",   "dynamic-filter-1"},
            {
             "rule", {
                             {"query", "{brand}"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: twitter"},
            {"metadata", {{"foo", "bar"}}},
    };

    override_t override_filter1;
    auto op = override_t::parse(override_filter1_json, "dynamic-filter-1", override_filter1);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_filter1);

    ASSERT_EQ("bar", override_filter1.to_json()["metadata"]["foo"].get<std::string>());

    nlohmann::json override_filter2_json = {
            {"id",   "dynamic-filter-2"},
            {
             "rule", {
                             {"query", "{tags}"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"stop_processing", false},
            {"filter_by", "tags: starred"}
    };

    override_t override_filter2;
    op = override_t::parse(override_filter2_json, "dynamic-filter-2", override_filter2);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_filter2);

    auto results = coll1->search("starred nike", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("bar", results["metadata"]["foo"].get<std::string>());

    // when stop_processing is enabled (default is true)
    override_filter1_json.erase("stop_processing");
    override_filter2_json.erase("stop_processing");

    override_t override_filter1_reset;
    op = override_t::parse(override_filter1_json, "dynamic-filter-1", override_filter1_reset);
    ASSERT_TRUE(op.ok());
    override_t override_filter2_reset;
    op = override_t::parse(override_filter2_json, "dynamic-filter-2", override_filter2_reset);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_filter1_reset);
    coll1->add_override(override_filter2_reset);

    results = coll1->search("starred nike", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, SynonymsAppliedToOverridenQuery) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "expensive"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:> 100"}
    };

    override_t override_contains;
    auto op = override_t::parse(override_json_contains, "static-filters", override_contains);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_contains);

    coll1->add_synonym(R"({"id": "", "root": "shoes", "synonyms": ["sneakers"]})"_json);

    auto results = coll1->search("expensive shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, StaticFilterWithAndWithoutQueryStringMutation) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "apple"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", false},
            {"filter_by", "price:> 200"}
    };

    override_t override_contains;
    auto op = override_t::parse(override_json_contains, "static-filters", override_contains);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override_contains);

    // first without query string mutation

    auto results = coll1->search("apple", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // now, with query string mutation

    override_json_contains = {
            {"id",   "static-filters"},
            {
             "rule", {
                             {"query", "apple"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"filter_by", "price:> 200"}
    };

    op = override_t::parse(override_json_contains, "static-filters", override_contains);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override_contains);

    results = coll1->search("apple", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringWithJustRemoveTokens) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),
                                 field("brand", field_types::STRING, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    // with override, we return all records

    nlohmann::json override_json = {
        {"id",                    "match-all"},
        {
         "rule",                  {
                                          {"query", "all"},
                                          {"match", override_t::MATCH_EXACT}
                                  }
        },
        {"remove_matched_tokens", true}
    };

    override_t override;
    auto op = override_t::parse(override_json, "match-all", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    results = coll1->search("all", {"name", "category", "brand"}, "",
                            {}, sort_fields, {0, 0, 0}, 10).get();

    ASSERT_EQ(3, results["hits"].size());

    results = coll1->search("really amazing shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {0, 0, 0}, 0).get();
    ASSERT_EQ(0, results["hits"].size());

    // with contains
    override_json = {
            {"id",                    "remove-some-tokens"},
            {
             "rule",                  {
                                              {"query", "really"},
                                              {"match", override_t::MATCH_CONTAINS}
                                      }
            },
            {"remove_matched_tokens", true}
    };

    override_t override2;
    op = override_t::parse(override_json, "remove-some-tokens", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

    results = coll1->search("really amazing shoes", {"name", "category", "brand"}, "",
                            {}, sort_fields, {0, 0, 0}, 1).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, StaticSorting) {
    Collection *coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("price", field_types::FLOAT, true),
                                 field("points", field_types::INT32, false)};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
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

    nlohmann::json override_json_contains = {
            {"id",   "static-sort"},
            {
             "rule", {
                             {"query", "shoes"},
                             {"match", override_t::MATCH_CONTAINS}
                     }
            },
            {"remove_matched_tokens", true},
            {"sort_by", "price:desc"}
    };

    override_t override_contains;
    auto op = override_t::parse(override_json_contains, "static-sort", override_contains);
    ASSERT_TRUE(op.ok());

    // without override kicking in
    auto results = coll1->search("shoes", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("0", results["hits"][1]["document"]["id"].get<std::string>());

    // now add override
    coll1->add_override(override_contains);

    results = coll1->search("shoes", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY, {true}, 0).get();

    // with override we will sort on price
    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results["hits"][1]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, DynamicFilteringWithPartialTokenMatch) {
    // when query tokens do not match placeholder field value exactly, don't do filtering
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields).get();
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

    // with override, we return all records

    nlohmann::json override_json = {
            {"id",   "dynamic-filter"},
            {
             "rule", {
                             {"query", "{ category }"},
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"filter_by", "category:= {category}"},
            {"remove_matched_tokens", true}
    };

    override_t override;
    auto op = override_t::parse(override_json, "dynamic-filter", override);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

    results = coll1->search("shoes", {"name"}, "",
                            {}, sort_fields, {0}, 10).get();

    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("shox", {"name"}, "",
                            {}, sort_fields, {0}, 10).get();

    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, OverrideWithSymbolsToIndex) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", static_cast<uint64_t>(std::time(nullptr)),
                                                    "", {"-"}, {}).get();
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

    // with override, we return all records

    nlohmann::json override_json = {
            {"id",   "ov-1"},
            {
             "rule", {
                             {"query", "non-stick"},
                             {"match", override_t::MATCH_EXACT}
                     }
            },
            {"filter_by", "category:= Cookware"}
    };

    override_t override;
    auto op = override_t::parse(override_json, "ov-1", override, "", {'-'}, {});
    ASSERT_TRUE(op.ok());
    coll1->add_override(override);

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

TEST_F(CollectionOverrideTest, OverrideWithTags) {
    /*

     If override1 is tagged tagA, tagB, override2 is tagged tagA, override3 is tagged with nothing:

     Then if a search is tagged with tagA, we only consider overrides that contain tagA (override1 and override2)
     with the usual logic - in alphabetic order of override name and then process both if stop rule processing is false.

     If a search is tagged with tagA and tagB, we evaluate any rules that contain tagA and tagB first,
     then tag A or tag B, but not overrides that contain no tags. Within each group, we evaluate in alphabetic order
     and process multiple if stop rule processing is false

     If a search has no tags, then we only consider rules that have no tags.
    */

    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
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

    // create overrides containing 2 tags, single tag and no tags:
    nlohmann::json override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha", "beta"]
        },
        "filter_by": "category: kids"
    })"_json;

    override_t override1;
    auto op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override1);

    // single tag
    nlohmann::json override_json2 = R"({
       "id": "ov-2",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha"]
        },
        "filter_by": "category: kitchen"
    })"_json;

    override_t override2;
    override_t::parse(override_json2, "ov-2", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

    // no tag
    nlohmann::json override_json3 = R"({
       "id": "ov-3",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "filter_by": "category: home"
    })"_json;

    override_t override3;
    op = override_t::parse(override_json3, "ov-3", override3);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override3);

    // when tag doesn't match any override, no results will be found
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "foo").get();

    ASSERT_EQ(2, results["hits"].size());

    // when multiple overrides match a given tag, return first matching record
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
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
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
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
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "alpha,beta").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // query with no tags should only trigger override with no tags
    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("2", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, OverrideWithTagsPartialMatch) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
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

    nlohmann::json override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact",
            "tags": ["alpha", "beta"]
        },
        "filter_by": "category: kids"
    })"_json;

    override_t override1;
    auto op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override1);

    //
    nlohmann::json override_json2 = R"({
       "id": "ov-2",
       "rule": {
            "query": "queryB",
            "match": "exact",
            "tags": ["alpha"]
        },
        "filter_by": "category: kitchen"
    })"_json;

    override_t override2;
    override_t::parse(override_json2, "ov-2", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

    // when only one of the two tags are found, apply that rule
    auto results = coll1->search("queryB", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "alpha,zeta").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, OverrideWithTagsWithoutStopProcessing) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING_ARRAY, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
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

    nlohmann::json override_json1 = R"({
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

    override_t override1;
    auto op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override1);

    //
    nlohmann::json override_json2 = R"({
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

    override_t override2;
    override_t::parse(override_json2, "ov-2", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

    //
    nlohmann::json override_json3 = R"({
       "id": "ov-3",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "stop_processing": false,
        "remove_matched_tokens": false,
        "filter_by": "category: home"
    })"_json;

    override_t override3;
    op = override_t::parse(override_json3, "ov-3", override3);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override3);

    auto results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "alpha").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("bar", results["metadata"]["foo"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, WildcardTagRuleThatMatchesAllQueries) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
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

    nlohmann::json override_json1 = R"({
       "id": "ov-1",
       "rule": {

        },
        "filter_by": "category: kids"
    })"_json;

    override_t override1;
    auto op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `rule` definition must contain either a `tags` or a `query` and `match`.", op.error());

    override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "tags": ["*"]
        },
        "filter_by": "category: kids"
    })"_json;

    op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override1);

    // should match all search queries, even without passing any tags
    std::string override_tags = "";
    auto results = coll1->search("queryB", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                                 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", override_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    results = coll1->search("queryA", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", override_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // includes instead of filter_by
    coll1->remove_override("ov-1");
    auto override_json2 = R"({
       "id": "ov-1",
       "rule": {
            "tags": ["*"]
        },
        "includes": [
            {"id": "1", "position": 1}
        ]
    })"_json;

    override_t override2;
    op = override_t::parse(override_json2, "ov-2", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

    results = coll1->search("foobar", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", override_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, TagsOnlyRule) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING_ARRAY, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
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
    override_t override1;
    auto override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "tags": ["listing"]
        },
        "filter_by": "category: kids"
    })"_json;

    auto op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override1);

    auto results = coll1->search("queryA", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                                 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", "listing").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // with include rule
    override_t override2;
    auto override_json2 = R"({
       "id": "ov-2",
       "rule": {
            "tags": ["listing2"]
        },
        "includes": [
            {"id": "1", "position": 1}
        ]
    })"_json;

    op = override_t::parse(override_json2, "ov-2", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

    results = coll1->search("foobar", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "listing2").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    // no override tag passed: rule should not match
    std::string override_tag = "";
    results = coll1->search("foobar", {"name"}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", override_tag).get();

    ASSERT_EQ(0, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, MetadataValidation) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING_ARRAY, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
    }

    nlohmann::json doc1;
    doc1["id"] = "0";
    doc1["name"] = "queryA";
    doc1["category"] = {"kids"};

    ASSERT_TRUE(coll1->add(doc1.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("_text_match", "DESC")};

    nlohmann::json override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "filter_by": "category: kids",
        "metadata": "foo"
    })"_json;

    override_t override1;
    auto op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("The `metadata` must be a JSON object.", op.error());

    // don't allow empty rule without any action
    override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact"
        }
    })"_json;

    override_t override2;
    op = override_t::parse(override_json1, "ov-2", override2);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ("Must contain one of: `includes`, `excludes`, `metadata`, `filter_by`, `sort_by`, "
              "`remove_matched_tokens`, `replace_query`.", op.error());

    // should allow only metadata to be present as action

    override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "queryA",
            "match": "exact"
        },
        "metadata": {"foo": "bar"}
    })"_json;

    override_t override3;
    op = override_t::parse(override_json1, "ov-3", override3);
    ASSERT_TRUE(op.ok());

    coll1->add_override(override3);

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, WildcardSearchOverride) {
    Collection* coll1;

    std::vector<field> fields = {field("name", field_types::STRING, false),
                                 field("category", field_types::STRING, true),};

    coll1 = collectionManager.get_collection("coll1").get();
    if (coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "").get();
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

    nlohmann::json override_json1 = R"({
       "id": "ov-1",
       "rule": {
            "query": "*",
            "match": "exact"
        },
        "filter_by": "category: kids"
    })"_json;

    override_t override1;
    auto op = override_t::parse(override_json1, "ov-1", override1);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override1);

    std::string override_tags = "";
    auto results = coll1->search("*", {}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                                 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", override_tags).get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    // includes instead of filter_by
    coll1->remove_override("ov-1");

    override_t override2;
    auto override_json2 = R"({
       "id": "ov-2",
       "rule": {
            "query": "*",
            "match": "exact"
        },
        "includes": [
            {"id": "1", "position": 1}
        ]
    })"_json;

    op = override_t::parse(override_json2, "ov-2", override2);
    ASSERT_TRUE(op.ok());
    coll1->add_override(override2);

    results = coll1->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", override_tags).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionOverrideTest, OverridesPagination) {
    Collection *coll2;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields, "points").get();
    }

    for(int i = 0; i < 5; ++i) {
        nlohmann::json override_json = {
                {"id",       "override"},
                {
                 "rule",     {
                                     {"query", "not-found"},
                                     {"match", override_t::MATCH_EXACT}
                             }
                },
                {"metadata", {       {"foo",   "bar"}}},
        };

        override_json["id"] = override_json["id"].get<std::string>() + std::to_string(i + 1);
        override_t override;
        override_t::parse(override_json, "", override);

        coll2->add_override(override);
    }

    uint32_t limit = 0, offset = 0, i = 0;

    //limit collections by 2
    limit=2;
    auto override_op = coll2->get_overrides(limit);
    auto override_map = override_op.get();
    ASSERT_EQ(2, override_map.size());
    i=offset;
    for(const auto &kv : override_map) {
        ASSERT_EQ("override" + std::to_string(i+1), kv.second->id);
        ++i;
    }

    //get 2 collection from offset 3
    offset=3;
    override_op = coll2->get_overrides(limit, offset);
    override_map = override_op.get();
    ASSERT_EQ(2, override_map.size());
    i=offset;
    for(const auto &kv : override_map) {
        ASSERT_EQ("override" + std::to_string(i+1), kv.second->id);
        ++i;
    }

    //get all collection except first
    offset=1; limit=0;
    override_op = coll2->get_overrides(limit, offset);
    override_map = override_op.get();
    ASSERT_EQ(4, override_map.size());
    i=offset;
    for(const auto &kv : override_map) {
        ASSERT_EQ("override" + std::to_string(i+1), kv.second->id);
        ++i;
    }

    //get last collection
    offset=4, limit=1;
    override_op = coll2->get_overrides(limit, offset);
    override_map = override_op.get();
    ASSERT_EQ(1, override_map.size());
    ASSERT_EQ("override5", override_map.begin()->second->id);

    //if limit is greater than number of collection then return all from offset
    offset=0; limit=8;
    override_op = coll2->get_overrides(limit, offset);
    override_map = override_op.get();
    ASSERT_EQ(5, override_map.size());
    i=offset;
    for(const auto &kv : override_map) {
        ASSERT_EQ("override" + std::to_string(i+1), kv.second->id);
        ++i;
    }

    offset=3; limit=4;
    override_op = coll2->get_overrides(limit, offset);
    override_map = override_op.get();
    ASSERT_EQ(2, override_map.size());
    i=offset;
    for(const auto &kv : override_map) {
        ASSERT_EQ("override" + std::to_string(i+1), kv.second->id);
        ++i;
    }

    //invalid offset
    offset=6; limit=0;
    override_op = coll2->get_overrides(limit, offset);
    ASSERT_FALSE(override_op.ok());
    ASSERT_EQ("Invalid offset param.", override_op.error());
}

TEST_F(CollectionOverrideTest, RetrieveOverideByID) {
    Collection *coll2;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("points", field_types::INT32, false)};

    coll2 = collectionManager.get_collection("coll2").get();
    if (coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields, "points").get();
    }

    nlohmann::json override_json = {
            {"id",       "override"},
            {
             "rule",     {
                                 {"query", "not-found"},
                                 {"match", override_t::MATCH_EXACT}
                         }
            },
            {"metadata", {       {"foo",   "bar"}}},
    };

    override_json["id"] = override_json["id"].get<std::string>() + "1";
    override_t override;
    override_t::parse(override_json, "", override);

    coll2->add_override(override);

    auto op = coll2->get_override("override1");
    ASSERT_TRUE(op.ok());
}


TEST_F(CollectionOverrideTest, FilterPinnedHits) {
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

    filter_curated_hits = true;
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