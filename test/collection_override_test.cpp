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
        collectionManager.init(store, 1, "auth_key");
        collectionManager.load();

        std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
        std::vector<field> fields = {
                field("title", field_types::STRING, false),
                field("starring", field_types::STRING, true),
                field("cast", field_types::STRING_ARRAY, true),
                field("points", field_types::INT32, false)
        };

        coll_mul_fields = collectionManager.get_collection("coll_mul_fields");
        if(coll_mul_fields == nullptr) {
            coll_mul_fields = collectionManager.create_collection("coll_mul_fields", fields, "points").get();
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

    override_t override(override_json);
    coll_mul_fields->add_override(override);

    std::vector<std::string> facets = {"cast"};

    Option<nlohmann::json> res_op = coll_mul_fields->search("of", {"title"}, "", facets, {}, 0, 10);
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

    override_t override_include(override_json_include);

    coll_mul_fields->add_override(override_include);

    res_op = coll_mul_fields->search("in", {"title"}, "", {}, {}, 0, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(3, results["found"].get<uint32_t>());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("13", results["hits"][2]["document"]["id"].get<std::string>().c_str());

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

    override_t override_inc_contains(override_contains_inc);
    coll_mul_fields->add_override(override_inc_contains);

    res_op = coll_mul_fields->search("will smith", {"title"}, "", {}, {}, 0, 10);
    ASSERT_TRUE(res_op.ok());
    results = res_op.get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_EQ(4, results["found"].get<uint32_t>());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][3]["document"]["id"].get<std::string>().c_str());

    coll_mul_fields->remove_override("include-rule");
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

    override_t override_include(override_json_include);
    coll_mul_fields->add_override(override_include);

    auto results = coll_mul_fields->search("not-found", {"title"}, "", {"starring"}, {}, 0, 10, 1, FREQUENCY,
                                           false, Index::DROP_TOKENS_THRESHOLD,
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

    override_t override(override_json_exclude);
    coll_mul_fields->add_override(override);

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, 0, 10, 1, FREQUENCY,
                                      false, Index::DROP_TOKENS_THRESHOLD,
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
    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, 0, 0, 1, FREQUENCY,
                                      false, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: scott").get();

    ASSERT_EQ(9, results["found"].get<size_t>());
    ASSERT_EQ(0, results["hits"].size());

    coll_mul_fields->remove_override("exclude-rule");

    // now with per_page = 1, and an include query

    coll_mul_fields->add_override(override_include);
    results = coll_mul_fields->search("not-found", {"title"}, "", {"starring"}, {}, 0, 1, 1, FREQUENCY,
                                      false, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ("0", results["hits"][0]["document"]["id"].get<std::string>());

    coll_mul_fields->remove_override("include-rule");
}

TEST_F(CollectionOverrideTest, IncludeExcludeHitsQuery) {
    std::map<std::string, size_t> pinned_hits;
    std::vector<std::string> hidden_hits;
    pinned_hits["13"] = 1;
    pinned_hits["4"] = 2;

    // basic pinning

    auto results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, 0, 50, 1, FREQUENCY,
                                           false, Index::DROP_TOKENS_THRESHOLD,
                                           spp::sparse_hash_set<std::string>(),
                                           spp::sparse_hash_set<std::string>(), 10, "starring: will", 30,
                                           "", 10,
                                           pinned_hits, {}).get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_STREQ("13", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", results["hits"][2]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("16", results["hits"][3]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("6", results["hits"][4]["document"]["id"].get<std::string>().c_str());

    // both pinning and hiding

    hidden_hits = {"11", "16"};
    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, 0, 50, 1, FREQUENCY,
                                      false, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30,
                                      "", 10,
                                      pinned_hits, hidden_hits).get();

    ASSERT_STREQ("13", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("6", results["hits"][2]["document"]["id"].get<std::string>().c_str());

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

    override_t override_include(override_json_include);
    coll_mul_fields->add_override(override_include);

    results = coll_mul_fields->search("the", {"title"}, "", {"starring"}, {}, 0, 50, 1, FREQUENCY,
                                      false, Index::DROP_TOKENS_THRESHOLD,
                                      spp::sparse_hash_set<std::string>(),
                                      spp::sparse_hash_set<std::string>(), 10, "starring: will", 30,
                                      "", 10,
                                      {}, {hidden_hits}).get();

    ASSERT_EQ(8, results["found"].get<size_t>());
    ASSERT_STREQ("8", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("6", results["hits"][1]["document"]["id"].get<std::string>().c_str());
}