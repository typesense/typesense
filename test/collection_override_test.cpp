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
    Collection *coll_mul_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_override";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
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
    ASSERT_STREQ("Must contain either `includes` or `excludes`.", parse_op.error().c_str());

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
    ASSERT_STREQ("found", coll_mul_fields->get_overrides()["include-rule"].rule.query.c_str());

    coll_mul_fields->remove_override("include-rule");
}

TEST_F(CollectionOverrideTest, IncludeExcludeHitsQuery) {
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

    // both pinning and hiding

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

    auto results = coll_mul_fields->search("notfoundquery", {"title"}, "", {"starring"}, {}, {0}, 10, 1, FREQUENCY,
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