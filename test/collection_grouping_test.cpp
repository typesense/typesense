#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"
#include "tsconfig.h"

class CollectionGroupingTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    Collection *coll_group;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_grouping";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("brand", field_types::STRING, true, true),
            field("size", field_types::INT32, true, false),
            field("colors", field_types::STRING_ARRAY, true, false),
            field("rating", field_types::FLOAT, true, false)
        };

        coll_group = collectionManager.get_collection("coll_group").get();
        if(coll_group == nullptr) {
            coll_group = collectionManager.create_collection("coll_group", 4, fields, "rating").get();
        }

        std::ifstream infile(std::string(ROOT_DIR)+"test/group_documents.jsonl");

        std::string json_line;

        while (std::getline(infile, json_line)) {
            auto add_op = coll_group->add(json_line);
            if(!add_op.ok()) {
                std::cout << add_op.error() << std::endl;
            }
            ASSERT_TRUE(add_op.ok());
        }

        infile.close();
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionGroupingTest, GroupingBasics) {
    // group by size (int32)
    auto res = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"size"}, 2).get();

    ASSERT_EQ(12, res["found_docs"].get<size_t>());
    ASSERT_EQ(3, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());
    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());

    ASSERT_EQ(2, res["grouped_hits"][0]["found"].get<int32_t>());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ(11, res["grouped_hits"][0]["hits"][0]["document"]["size"].get<size_t>());
    ASSERT_STREQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][0]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("1", res["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(7, res["grouped_hits"][1]["found"].get<int32_t>());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("4", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][1]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("3", res["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(3, res["grouped_hits"][2]["found"].get<int32_t>());
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][2]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("2", res["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][2]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("8", res["grouped_hits"][2]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("brand", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Beta", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][3]["count"]);
    ASSERT_STREQ("Zeta", res["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());

    // group by rating (float) and sort by size
    std::vector<sort_by> sort_size = {sort_by("size", "DESC")};
    res = coll_group->search("*", {}, "", {"brand"}, sort_size, {0}, 50, 1, FREQUENCY,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "brand: omeg", 30, 5,
                             "", 10,
                             {}, {}, {"rating"}, 2).get();

    // 7 unique ratings
    ASSERT_EQ(12, res["found_docs"].get<size_t>());
    ASSERT_EQ(7, res["found"].get<size_t>());
    ASSERT_EQ(7, res["grouped_hits"].size());
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][0]["group_key"][0].get<float>());

    ASSERT_EQ(1, res["grouped_hits"][0]["found"].get<int32_t>());
    ASSERT_EQ(12, res["grouped_hits"][0]["hits"][0]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("8", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());

    ASSERT_EQ(4, res["grouped_hits"][1]["found"].get<int32_t>());
    ASSERT_EQ(12, res["grouped_hits"][1]["hits"][0]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("6", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    
    ASSERT_EQ(11, res["grouped_hits"][1]["hits"][1]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("1", res["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][1]["hits"][1]["document"]["rating"].get<float>());

    ASSERT_EQ(1, res["grouped_hits"][5]["found"].get<int32_t>());
    ASSERT_EQ(10, res["grouped_hits"][5]["hits"][0]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("9", res["grouped_hits"][5]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.1, res["grouped_hits"][5]["hits"][0]["document"]["rating"].get<float>());

    ASSERT_EQ(1, res["grouped_hits"][6]["found"].get<int32_t>());
    ASSERT_EQ(10, res["grouped_hits"][6]["hits"][0]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("0", res["grouped_hits"][6]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.5, res["grouped_hits"][6]["hits"][0]["document"]["rating"].get<float>());

    ASSERT_STREQ("brand", res["facet_counts"][0]["field_name"].get<std::string>().c_str());
    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    ASSERT_STREQ("<mark>Omeg</mark>a", res["facet_counts"][0]["counts"][0]["highlighted"].get<std::string>().c_str());

    // Wildcard group_by is not allowed
    auto error = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"foo*"}, 2).error();
    ASSERT_EQ("Pattern `foo*` is not allowed.",  error);

    // typo_tokens_threshold should respect num_groups
    res = coll_group->search("beta", {"brand"}, "", {"brand"}, {}, {2}, 50, 1, FREQUENCY,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 2,
                             {}, {}, {"brand"}, 1).get();

    ASSERT_EQ(4, res["found_docs"].get<size_t>());
    ASSERT_EQ(2, res["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"].size());
    ASSERT_EQ("Beta", res["grouped_hits"][0]["group_key"][0]);
    ASSERT_EQ("Zeta", res["grouped_hits"][1]["group_key"][0]);

    res = coll_group->search("beta", {"brand"}, "", {"brand"}, {}, {2}, 50, 1, FREQUENCY,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 1,
                             {}, {}, {"brand"}, 1).get();

    ASSERT_EQ(3, res["found_docs"].get<size_t>());
    ASSERT_EQ(1, res["found"].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"].size());
    ASSERT_EQ("Beta", res["grouped_hits"][0]["group_key"][0]);
}

TEST_F(CollectionGroupingTest, GroupingCompoundKey) {
    // group by size+brand (int32, string)
    auto res = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"size", "brand"}, 2).get();
    ASSERT_EQ(12, res["found_docs"].get<size_t>());
    ASSERT_EQ(10, res["found"].get<size_t>());
    ASSERT_EQ(10, res["grouped_hits"].size());

    ASSERT_EQ(1, res["grouped_hits"][0]["found"].get<int32_t>());
    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_STREQ("Beta", res["grouped_hits"][0]["group_key"][1].get<std::string>().c_str());

    // optional field should have no value in the group key component
    ASSERT_EQ(1, res["grouped_hits"][5]["group_key"].size());
    ASSERT_STREQ("10", res["grouped_hits"][5]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", res["grouped_hits"][5]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(1, res["grouped_hits"][1]["found"].get<int32_t>());
    ASSERT_EQ(1, res["grouped_hits"][1]["hits"].size());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("4", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(2, res["grouped_hits"][2]["found"].get<int32_t>());
    ASSERT_EQ(2, res["grouped_hits"][2]["hits"].size());
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][2]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("3", res["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.5, res["grouped_hits"][2]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("0", res["grouped_hits"][2]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("brand", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Beta", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][3]["count"]);
    ASSERT_STREQ("Zeta", res["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());

    // pagination with page=2, per_page=2
    res = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 2, 2, FREQUENCY,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10,
                             {}, {}, {"size", "brand"}, 2).get();


    // 3rd result from previous assertion will be in the first position
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("3", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.5, res["grouped_hits"][0]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("0", res["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    // total count and facet counts should be the same
    ASSERT_EQ(12, res["found_docs"].get<size_t>());
    ASSERT_EQ(10, res["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"].size());
    ASSERT_EQ(10, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_STREQ("Omega", res["grouped_hits"][0]["group_key"][1].get<std::string>().c_str());

    ASSERT_STREQ("brand", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Beta", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][3]["count"]);
    ASSERT_STREQ("Zeta", res["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());

    // respect min and max grouping limit (greater than 0 and less than 99)
    auto res_op = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "brand: omeg", 30, 5,
                             "", 10,
                             {}, {}, {"rating"}, 100);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ(("Value of `group_limit` must be between 1 and " + std::to_string(Config::get_instance().get_max_group_limit()) + ".").c_str(), res_op.error().c_str());

    res_op = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                {false}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "brand: omeg", 30, 5,
                                "", 10,
                                {}, {}, {"rating"}, 0);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ(("Value of `group_limit` must be between 1 and " + std::to_string(Config::get_instance().get_max_group_limit()) + ".").c_str(), res_op.error().c_str());
}

TEST_F(CollectionGroupingTest, GroupingWithMultiFieldRelevance) {
    Collection *coll1;

    std::vector<field> fields = {field("title", field_types::STRING, false),
                                 field("artist", field_types::STRING, false),
                                 field("genre", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    std::vector<std::vector<std::string>> records = {
        {"Train or Highway", "Gord Downie", "rock"},
        {"Down There by the Train", "Dustin Kensrue", "pop"},
        {"In the Train", "Dustin Kensrue", "pop"},
        {"State Trooper", "Dustin Kensrue", "country"},
        {"Down There Somewhere", "Dustin Kensrue", "pop"},
        {"Down There by the Train", "Gord Downie", "rock"},
        {"Down and Outside", "Gord Downie", "rock"},
        {"Let it be", "Downie Kensrue", "country"},
        {"There was a Train", "Gord Kensrue", "country"},
    };

    for(size_t i=0; i<records.size(); i++) {
        nlohmann::json doc;

        doc["id"] = std::to_string(i);
        doc["title"] = records[i][0];
        doc["artist"] = records[i][1];
        doc["genre"] = records[i][2];
        doc["points"] = i;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("Dustin Kensrue Down There by the Train",
                                 {"title", "artist"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                                 {false}, 10,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10,
                                 {}, {}, {"genre"}, 2).get();

    ASSERT_EQ(7, results["found_docs"].get<size_t>());
    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["grouped_hits"].size());

    ASSERT_EQ(3, results["grouped_hits"][0]["found"].get<int32_t>());
    ASSERT_STREQ("pop", results["grouped_hits"][0]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, results["grouped_hits"][0]["hits"].size());
    ASSERT_STREQ("1", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(2, results["grouped_hits"][1]["found"].get<int32_t>());
    ASSERT_STREQ("rock", results["grouped_hits"][1]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, results["grouped_hits"][1]["hits"].size());
    ASSERT_STREQ("5", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(2, results["grouped_hits"][2]["found"].get<int32_t>());
    ASSERT_STREQ("country", results["grouped_hits"][2]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, results["grouped_hits"][2]["hits"].size());
    ASSERT_STREQ("8", results["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", results["grouped_hits"][2]["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionGroupingTest, GroupingWithGropLimitOfOne) {
    auto res = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"brand"}, 1).get();

    ASSERT_EQ(12, res["found_docs"].get<size_t>());
    ASSERT_EQ(5, res["found"].get<size_t>());
    ASSERT_EQ(5, res["grouped_hits"].size());

    // all hits array must be of size 1
    for(auto i=0; i<5; i++) {
        ASSERT_EQ(1, res["grouped_hits"][i]["hits"].size());
    }
    
    ASSERT_EQ(3, res["grouped_hits"][0]["found"].get<int32_t>());
    ASSERT_STREQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    
    ASSERT_EQ(4, res["grouped_hits"][1]["found"].get<int32_t>());
    ASSERT_STREQ("3", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    
    ASSERT_EQ(2, res["grouped_hits"][2]["found"].get<int32_t>());
    ASSERT_STREQ("8", res["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    
    ASSERT_EQ(2, res["grouped_hits"][3]["found"].get<int32_t>());
    ASSERT_STREQ("10", res["grouped_hits"][3]["hits"][0]["document"]["id"].get<std::string>().c_str()); // unbranded
    
    ASSERT_EQ(1, res["grouped_hits"][4]["found"].get<int32_t>());
    ASSERT_STREQ("9", res["grouped_hits"][4]["hits"][0]["document"]["id"].get<std::string>().c_str());

    // facet counts should each be 1, including unbranded
    ASSERT_STREQ("brand", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    for(size_t i=0; i < 4; i++) {
        ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][i]["count"]);
    }
}

TEST_F(CollectionGroupingTest, GroupingWithArrayFieldAndOverride) {
    nlohmann::json override_json_include = {
        {"id", "include-rule"},
        {
            "rule", {
               {"query", "shirt"},
               {"match", override_t::MATCH_EXACT}
            }
        },
        {"stop_processing", false}
    };

    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "11";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "10";
    override_json_include["includes"][1]["position"] = 1;

    nlohmann::json override_json_exclude = {
        {"id",   "exclude-rule"},
        {
            "rule", {
                 {"query", "shirt"},
                 {"match", override_t::MATCH_EXACT}
            }
        },
        {"stop_processing", false}
    };
    override_json_exclude["excludes"] = nlohmann::json::array();
    override_json_exclude["excludes"][0] = nlohmann::json::object();
    override_json_exclude["excludes"][0]["id"] = "2";

    override_t override1;
    override_t override2;

    override_t::parse(override_json_include, "", override1);
    override_t::parse(override_json_exclude, "", override2);

    Option<uint32_t> ov1_op = coll_group->add_override(override1);
    Option<uint32_t> ov2_op = coll_group->add_override(override2);

    ASSERT_TRUE(ov1_op.ok());
    ASSERT_TRUE(ov2_op.ok());

    auto res = coll_group->search("shirt", {"title"}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"colors"}, 2).get();

    ASSERT_EQ(9, res["found_docs"].get<size_t>());
    ASSERT_EQ(4, res["found"].get<size_t>());
    ASSERT_EQ(4, res["grouped_hits"].size());

    ASSERT_EQ(1, res["grouped_hits"][0]["group_key"][0].size());
    ASSERT_STREQ("white", res["grouped_hits"][0]["group_key"][0][0].get<std::string>().c_str());

    ASSERT_STREQ("11", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("10", res["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("5", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", res["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("4", res["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", res["grouped_hits"][2]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(1, res["grouped_hits"][3]["hits"].size());
    ASSERT_STREQ("8", res["grouped_hits"][3]["hits"][0]["document"]["id"].get<std::string>().c_str());

    // assert facet counts
    ASSERT_STREQ("brand", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());
    
    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Beta", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][3]["count"]);
    ASSERT_STREQ("Zeta", res["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());
}

TEST_F(CollectionGroupingTest, GroupOrderIndependence) {
    Collection *coll1;

    std::vector<field> fields = {field("group", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc;

    for(size_t i = 0; i < 256; i++) {
        int64_t points = 100 + i;
        doc["id"] = std::to_string(i);
        doc["group"] = std::to_string(i);
        doc["points"] = points;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // doc id "255" will have points of 255
    // try to insert doc id "256" with group "256" but having lesser points than all records

    doc["id"] = "256";
    doc["group"] = "256";
    doc["points"] = 50;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // insert doc id "257" of same group "256" with greatest point

    doc["id"] = "257";
    doc["group"] = "256";
    doc["points"] = 500;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // when we search by grouping records, sorting descending on points, both records of group "256" should show up

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    auto res = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"group"}, 10).get();

    ASSERT_EQ(1, res["grouped_hits"][0]["group_key"].size());
    ASSERT_STREQ("256", res["grouped_hits"][0]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());
}

TEST_F(CollectionGroupingTest, UseHighestValueInGroupForOrdering) {
    Collection *coll1;

    std::vector<field> fields = {field("group", field_types::STRING, true),
                                 field("points", field_types::INT32, false),};

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "points").get();
    }

    nlohmann::json doc;

    for(size_t i = 0; i < 250; i++) {
        int64_t points = 100 + i;
        doc["id"] = std::to_string(i);
        doc["group"] = std::to_string(i);
        doc["points"] = points;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    // points: 100 -> 349

    // group with highest point is "249" with 349 points
    // insert another document for that group with 50 points
    doc["id"] = "250";
    doc["group"] = "249";
    doc["points"] = 50;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // now insert another new group whose points is greater than 50
    doc["id"] = "251";
    doc["group"] = "1000";
    doc["points"] = 60;
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    std::vector<sort_by> sort_fields = {sort_by("points", "DESC")};

    auto res = coll1->search("*", {}, "", {}, sort_fields, {0}, 10, 1, FREQUENCY,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10,
                             {}, {}, {"group"}, 10).get();

    ASSERT_EQ(1, res["grouped_hits"][0]["group_key"].size());
    ASSERT_STREQ("249", res["grouped_hits"][0]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());
}


TEST_F(CollectionGroupingTest, RepeatedFieldNameGroupHitCount) {
    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("brand", field_types::STRING, true, true),
            field("colors", field_types::STRING, true, false),
    };

    Collection* coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields).get();
    }

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "foobar";
    doc["brand"] = "Omega";
    doc["colors"] = "foo";

    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    auto res = coll2->search("f", {"title", "colors"}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                                   {true}, 10,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"brand"}, 2).get();

    ASSERT_EQ(1, res["grouped_hits"].size());
    ASSERT_EQ(1, res["grouped_hits"][0]["found"].get<int32_t>());
}

TEST_F(CollectionGroupingTest, ControlMissingValues) {
    std::vector<field> fields = {
        field("brand", field_types::STRING, true, true),
    };

    Collection* coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields).get();
    }

    nlohmann::json doc;
    doc["id"] = "0";
    doc["brand"] = "Omega";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["brand"] = nullptr;
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["brand"] = nullptr;
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "3";
    doc["brand"] = "Omega";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    // disable null value aggregation

    auto res = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                             {true}, 10,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10,
                             {}, {}, {"brand"}, 2,
                             "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score,
                             100, 0, 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true, false).get();

    ASSERT_EQ(3, res["grouped_hits"].size());
    ASSERT_EQ("Omega", res["grouped_hits"][0]["group_key"][0].get<std::string>());
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());

    ASSERT_EQ(0, res["grouped_hits"][1]["group_key"].size());
    ASSERT_EQ(1, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("2", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ(0, res["grouped_hits"][2]["group_key"].size());
    ASSERT_EQ(1, res["grouped_hits"][2]["hits"].size());
    ASSERT_EQ("1", res["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>());

    // with null value aggregation (default)
    res = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                        {true}, 10,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                        "", 10,
                        {}, {}, {"brand"}, 2,
                        "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                        4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score,
                        100, 0, 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true, true).get();

    ASSERT_EQ(2, res["grouped_hits"].size());

    ASSERT_EQ("Omega", res["grouped_hits"][0]["group_key"][0].get<std::string>());
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());

    ASSERT_EQ(0, res["grouped_hits"][1]["group_key"].size());
    ASSERT_EQ(2, res["grouped_hits"][1]["hits"].size());
}

TEST_F(CollectionGroupingTest, SkipFieldValidation) {
    std::vector<field> fields = {
        field("brand", field_types::STRING, true, true),
        field("category", field_types::STRING, true, true),
    };

    Collection* coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields).get();
    }

    nlohmann::json doc;
    doc["id"] = "0";
    doc["brand"] = "Omega";
    doc["category"] = "Shoes";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["brand"] = "Alpha";
    doc["category"] = "Electronics";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["brand"] = "Omega";
    doc["category"] = "Shoes";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    bool validate_field_names = false;

    auto res_op = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                             {true}, 10,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10,
                             {}, {}, {"brandx"}, 2,
                             "<mark>", "</mark>",
                             {}, 1000,true,
                             false, true, "", false,
                             6000*1000, 4, 7, fallback, 4,
                             {off}, INT16_MAX, INT16_MAX,2,
                             2, false, "", true,
                             0, max_score, 100, 0, 0,
                             "exhaustive", 30000, 2, "",
                             {},{}, "right_to_left", true,
                             true, false, "", "", "",
                             "", true, true, false, 0, true,
                             true, 10, false, validate_field_names
                             );

    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();
    ASSERT_EQ(0, res["found_docs"].get<size_t>());
    ASSERT_EQ(0, res["found"].get<size_t>());
    ASSERT_EQ(0, res["grouped_hits"].size());

    // only one of the two group by fields are found
    res_op = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                           {true}, 10,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10,
                           {}, {}, {"brandx", "category"}, 2,
                           "<mark>", "</mark>",
                           {}, 1000,true,
                           false, true, "", false,
                           6000*1000, 4, 7, fallback, 4,
                           {off}, INT16_MAX, INT16_MAX,2,
                           2, false, "", true,
                           0, max_score, 100, 0, 0,
                           "exhaustive", 30000, 2, "",
                           {},{}, "right_to_left", true,
                           true, false, "", "", "",
                           "", true, true, false, 0, true,
                           true, 10, false, validate_field_names
    );

    ASSERT_TRUE(res_op.ok());
    res = res_op.get();

    ASSERT_EQ(3, res["found_docs"].get<size_t>());
    ASSERT_EQ(2, res["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"].size());

    // with validate_field_names
    validate_field_names = true;

    res_op = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                           {true}, 10,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10,
                           {}, {}, {"brandx", "category"}, 2,
                           "<mark>", "</mark>",
                           {}, 1000,true,
                           false, true, "", false,
                           6000*1000, 4, 7, fallback, 4,
                           {off}, INT16_MAX, INT16_MAX,2,
                           2, false, "", true,
                           0, max_score, 100, 0, 0,
                           "exhaustive", 30000, 2, "",
                           {},{}, "right_to_left", true,
                           true, false, "", "", "",
                           "", true, true, false, 0, true,
                           true, 10, false, validate_field_names);

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `brandx` in the schema.", res_op.error());
}

TEST_F(CollectionGroupingTest, SortingOnGroupCount) {

    std::vector<sort_by> sort_fields = {sort_by("_group_found", "DESC")};
    
    auto res = coll_group->search("*", {}, "", {"brand"}, sort_fields, {0}, 50, 1, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"size"}, 2).get();

    ASSERT_EQ(12, res["found_docs"].get<size_t>());
    ASSERT_EQ(3, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());

    ASSERT_EQ(10, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(7, res["grouped_hits"][0]["found"].get<int32_t>());

    ASSERT_EQ(12, res["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"][1]["found"].get<int32_t>());

    ASSERT_EQ(11, res["grouped_hits"][2]["group_key"][0].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][2]["found"].get<int32_t>());


    //search in asc order

    std::vector<sort_by> sort_fields2 = {sort_by("_group_found", "ASC")};
    
    auto res2 = coll_group->search("*", {}, "", {"brand"}, sort_fields2, {0}, 50, 1, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"size"}, 2).get();

    ASSERT_EQ(12, res2["found_docs"].get<size_t>());
    ASSERT_EQ(3, res2["found"].get<size_t>());
    ASSERT_EQ(3, res2["grouped_hits"].size());

    ASSERT_EQ(11, res2["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(2, res2["grouped_hits"][0]["found"].get<int32_t>());

    ASSERT_EQ(12, res2["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(3, res2["grouped_hits"][1]["found"].get<int32_t>());

    ASSERT_EQ(10, res2["grouped_hits"][2]["group_key"][0].get<size_t>());
    ASSERT_EQ(7, res2["grouped_hits"][2]["found"].get<int32_t>());
}

TEST_F(CollectionGroupingTest, SortingMoreThanMaxTopsterSize) {

    std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("brand", field_types::STRING, true, true),
            field("size", field_types::INT32, true, false),
            field("colors", field_types::STRING, true, false),
            field("rating", field_types::FLOAT, true, false)
    };

    Collection* coll3 = collectionManager.get_collection("coll3").get();
    if(coll3 == nullptr) {
        coll3 = collectionManager.create_collection("coll3", 4, fields, "rating").get();
    }

    for(auto i = 0; i < 150; i++) {
        auto group_id = i;
        for(auto j = 0; j < 4; j++) {
            nlohmann::json doc;
            doc["title"] = "Omega Casual Poplin Shirt";
            doc["brand"] = "Omega";
            doc["size"] = group_id;
            doc["colors"] = "blue";
            doc["rating"] = 4.5;

            ASSERT_TRUE(coll3->add(doc.dump()).ok());
        } 
    }

    for(auto i = 150; i < 250; i++) {
        auto group_id = i;
        for(auto j = 0; j < 3; j++) {
            nlohmann::json doc;
            doc["title"] = "Beta Casual Poplin Shirt";
            doc["brand"] = "Beta";
            doc["size"] = group_id;
            doc["colors"] = "white";
            doc["rating"] = 4.3;

            ASSERT_TRUE(coll3->add(doc.dump()).ok());
        } 
    }

    for(auto i = 250; i < 300; i++) {
        auto group_id = i;
        for(auto j = 0; j < 2; j++) {
            nlohmann::json doc;
            doc["title"] = "Zeta Casual Poplin Shirt";
            doc["brand"] = "Zeta";
            doc["size"] = group_id;
            doc["colors"] = "red";
            doc["rating"] = 4.6;

            ASSERT_TRUE(coll3->add(doc.dump()).ok());
        } 
    }

    //first search in desc order
    std::vector<sort_by> sort_fields = {sort_by("_group_found", "DESC")};

    auto res = coll3->search("*", {}, "", {"brand"}, sort_fields, {0}, 100, 2, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"size"}, 2).get();

    ASSERT_EQ(1000, res["found_docs"].get<size_t>());
    ASSERT_EQ(300, res["found"].get<size_t>());
    ASSERT_EQ(100, res["grouped_hits"].size());

    ASSERT_EQ(4, res["grouped_hits"][4]["found"].get<int32_t>());

    ASSERT_EQ(3, res["grouped_hits"][50]["found"].get<int32_t>());

    ASSERT_EQ(3, res["grouped_hits"][99]["found"].get<int32_t>());


    res = coll3->search("*", {}, "", {"brand"}, sort_fields, {0}, 100, 3, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"size"}, 2).get();

    ASSERT_EQ(1000, res["found_docs"].get<size_t>());
    ASSERT_EQ(300, res["found"].get<size_t>());
    ASSERT_EQ(100, res["grouped_hits"].size());

    ASSERT_EQ(3, res["grouped_hits"][4]["found"].get<int32_t>());

    ASSERT_EQ(3, res["grouped_hits"][4]["found"].get<int32_t>());

    ASSERT_EQ(2, res["grouped_hits"][50]["found"].get<int32_t>());

    ASSERT_EQ(2, res["grouped_hits"][99]["found"].get<int32_t>());


    //search in asc order

    std::vector<sort_by> sort_fields2 = {sort_by("_group_found", "ASC")};
    
    auto res2 = coll3->search("*", {}, "", {"brand"}, sort_fields2, {0}, 100, 1, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"size"}, 2).get();

    ASSERT_EQ(1000, res2["found_docs"].get<size_t>());
    ASSERT_EQ(300, res2["found"].get<size_t>());
    ASSERT_EQ(100, res2["grouped_hits"].size());

    ASSERT_EQ(2, res2["grouped_hits"][0]["found"].get<int32_t>());

    ASSERT_EQ(2, res2["grouped_hits"][1]["found"].get<int32_t>());

    ASSERT_EQ(3, res2["grouped_hits"][50]["found"].get<int32_t>());

    ASSERT_EQ(3, res2["grouped_hits"][99]["found"].get<int32_t>());

    res2 = coll3->search("*", {}, "", {"brand"}, sort_fields2, {0}, 100, 2, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {"size"}, 2).get();

    ASSERT_EQ(1000, res2["found_docs"].get<size_t>());
    ASSERT_EQ(300, res2["found"].get<size_t>());
    ASSERT_EQ(100, res2["grouped_hits"].size());

    ASSERT_EQ(3, res2["grouped_hits"][0]["found"].get<int32_t>());

    ASSERT_EQ(3, res2["grouped_hits"][1]["found"].get<int32_t>());

    ASSERT_EQ(4, res2["grouped_hits"][50]["found"].get<int32_t>());

    ASSERT_EQ(4, res2["grouped_hits"][99]["found"].get<int32_t>());
}

TEST_F(CollectionGroupingTest, GroupSortingWithoutGroupingFields) {
    
    std::vector<sort_by> sort_fields = {sort_by("_group_found", "DESC")};
    
    auto res = coll_group->search("*", {}, "", {"brand"}, sort_fields, {0}, 50, 1, FREQUENCY,
                                   {false}, Index::DROP_TOKENS_THRESHOLD,
                                   spp::sparse_hash_set<std::string>(),
                                   spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                   "", 10,
                                   {}, {}, {});

    ASSERT_EQ(res.ok(), false);
    ASSERT_EQ(res.error(), "group_by parameters should not be empty when using sort_by group_found");
}

TEST_F(CollectionGroupingTest, SkipToReverseGroupBy) {
    std::vector<field> fields = {
            field("brand", field_types::STRING, true, true),
    };

    Collection* coll2 = collectionManager.get_collection("coll2").get();
    if(coll2 == nullptr) {
        coll2 = collectionManager.create_collection("coll2", 1, fields).get();
    }

    nlohmann::json doc;

    doc["id"] = "0";
    doc["brand"] = nullptr;
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    auto res = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                             {true}, 10,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10,
                             {}, {}, {"brand"}, 2,
                             "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score,
                             100, 0, 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true, false).get();

    ASSERT_EQ(1, res["grouped_hits"].size());

    ASSERT_EQ(0, res["grouped_hits"][0]["group_key"].size());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("0", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>());

    doc["id"] = "1";
    doc["brand"] = "adidas";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["brand"] = "puma";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "3";
    doc["brand"] = nullptr;
    ASSERT_TRUE(coll2->add(doc.dump()).ok());

    doc["id"] = "4";
    doc["brand"] = "nike";
    ASSERT_TRUE(coll2->add(doc.dump()).ok());


    res = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                             {true}, 10,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10,
                             {}, {}, {"brand"}, 2,
                             "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score,
                             100, 0, 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true, false).get();

    ASSERT_EQ(5, res["grouped_hits"].size());

    ASSERT_EQ("nike", res["grouped_hits"][0]["group_key"][0].get<std::string>());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());

    ASSERT_EQ(0, res["grouped_hits"][1]["group_key"].size());
    ASSERT_EQ(1, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("3", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ("puma", res["grouped_hits"][2]["group_key"][0].get<std::string>());
    ASSERT_EQ(1, res["grouped_hits"][2]["hits"].size());

    ASSERT_EQ("adidas", res["grouped_hits"][3]["group_key"][0].get<std::string>());
    ASSERT_EQ(1, res["grouped_hits"][3]["hits"].size());

    ASSERT_EQ(0, res["grouped_hits"][4]["group_key"].size());
    ASSERT_EQ(1, res["grouped_hits"][4]["hits"].size());
    ASSERT_EQ("0", res["grouped_hits"][4]["hits"][0]["document"]["id"].get<std::string>());

    res = coll2->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY,
                             {true}, 10,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                             "", 10,
                             {}, {}, {"brand"}, 2,
                             "<mark>", "</mark>", {3,3}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                             4, {off}, 0, 0, 0, 2, false, "", true, 0, max_score,
                             100, 0, 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true, true).get();

    ASSERT_EQ(4, res["grouped_hits"].size());

    ASSERT_EQ("nike", res["grouped_hits"][0]["group_key"][0].get<std::string>());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());

    ASSERT_EQ(0, res["grouped_hits"][1]["group_key"].size());
    ASSERT_EQ(2, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("3", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_EQ("puma", res["grouped_hits"][2]["group_key"][0].get<std::string>());
    ASSERT_EQ(1, res["grouped_hits"][2]["hits"].size());
}

TEST_F(CollectionGroupingTest, GroupByMultipleFacetFields) {
    auto res = coll_group->search("*", {}, "", {"brand", "colors"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"size"}, 2).get();

    ASSERT_EQ(12, res["found_docs"].get<size_t>());
    ASSERT_EQ(3, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());
    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());

    ASSERT_EQ(2, res["grouped_hits"][0]["found"].get<int32_t>());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ(11, res["grouped_hits"][0]["hits"][0]["document"]["size"].get<size_t>());
    ASSERT_STREQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][0]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("1", res["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(7, res["grouped_hits"][1]["found"].get<int32_t>());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("4", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][1]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("3", res["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(3, res["grouped_hits"][2]["found"].get<int32_t>());
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][2]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("2", res["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][2]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("8", res["grouped_hits"][2]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("brand", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("Beta", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][3]["count"]);
    ASSERT_STREQ("Zeta", res["facet_counts"][0]["counts"][3]["value"].get<std::string>().c_str());


    ASSERT_STREQ("colors", res["facet_counts"][1]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][1]["counts"][0]["count"]);
    ASSERT_STREQ("blue", res["facet_counts"][1]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][1]["counts"][1]["count"]);
    ASSERT_STREQ("white", res["facet_counts"][1]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][1]["counts"][2]["count"]);
    ASSERT_STREQ("red", res["facet_counts"][1]["counts"][2]["value"].get<std::string>().c_str());
}

TEST_F(CollectionGroupingTest, GroupByMultipleFacetFieldsWithFilter) {
    auto res = coll_group->search("*", {}, "size:>10", {"colors", "brand"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"size"}, 2).get();

    ASSERT_EQ(5, res["found_docs"].get<size_t>());
    ASSERT_EQ(2, res["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"].size());

    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][0]["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ("1", res["grouped_hits"][0]["hits"][1]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][0]["hits"][1]["document"]["rating"].get<float>());

    ASSERT_EQ(12, res["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"][1]["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("2", res["grouped_hits"][1]["hits"][0]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ("8", res["grouped_hits"][1]["hits"][1]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][1]["hits"][1]["document"]["rating"].get<float>());

    ASSERT_STREQ("colors", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("blue", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("white", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("red", res["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());

    ASSERT_STREQ("brand", res["facet_counts"][1]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][1]["counts"][0]["count"]);
    ASSERT_STREQ("Beta", res["facet_counts"][1]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][1]["counts"][1]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][1]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][1]["counts"][2]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][1]["counts"][2]["value"].get<std::string>().c_str());
}

TEST_F(CollectionGroupingTest, GroupByMultipleFacetFieldsWithPinning) {
    auto res = coll_group->search("*", {}, "size:>10", {"colors", "brand"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {"3:1,4:2"}, {}, {"size"}, 2).get();

    ASSERT_EQ(5, res["found_docs"].get<size_t>());
    ASSERT_EQ(3, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());

    ASSERT_EQ(10, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("3", res["grouped_hits"][0]["hits"][0]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ("4", res["grouped_hits"][0]["hits"][1]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][0]["hits"][1]["document"]["rating"].get<float>());

    ASSERT_EQ(11, res["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("5", res["grouped_hits"][1]["hits"][0]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ("1", res["grouped_hits"][1]["hits"][1]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][1]["hits"][1]["document"]["rating"].get<float>());

    ASSERT_EQ(12, res["grouped_hits"][2]["group_key"][0].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"][2]["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][2]["hits"].size());
    ASSERT_EQ("2", res["grouped_hits"][2]["hits"][0]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][2]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ("8", res["grouped_hits"][2]["hits"][1]["document"]["id"]);
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][2]["hits"][1]["document"]["rating"].get<float>());

    ASSERT_STREQ("colors", res["facet_counts"][0]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_STREQ("blue", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("white", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][0]["counts"][2]["count"]);
    ASSERT_STREQ("red", res["facet_counts"][0]["counts"][2]["value"].get<std::string>().c_str());

    ASSERT_STREQ("brand", res["facet_counts"][1]["field_name"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][1]["counts"][0]["count"]);
    ASSERT_STREQ("Beta", res["facet_counts"][1]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(3, (int) res["facet_counts"][1]["counts"][1]["count"]);
    ASSERT_STREQ("Omega", res["facet_counts"][1]["counts"][1]["value"].get<std::string>().c_str());

    ASSERT_EQ(1, (int) res["facet_counts"][1]["counts"][2]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][1]["counts"][2]["value"].get<std::string>().c_str());
}

TEST_F(CollectionGroupingTest, GroupByPinnedHitsOrder) {
    auto res = coll_group->search("*", {"title"}, "size:=[12,11]", {}, {}, {0}, 50, 0, NOT_SET,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                                  "", 1,
                                  {"6:1,1:2"}, {}, {"size"}, 1).get();

    ASSERT_EQ(4, res["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"].size());

    ASSERT_EQ(12, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("6", res["grouped_hits"][0]["hits"][0]["document"]["id"]);

    ASSERT_EQ(11, res["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("1", res["grouped_hits"][1]["hits"][0]["document"]["id"]);

    // with group limit 2
    res = coll_group->search("*", {"title"}, "size:=[12,11]", {}, {}, {0}, 50, 0, NOT_SET,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                             "", 1,
                             {"6:1,1:2"}, {}, {"size"}, 2).get();

    ASSERT_EQ(4, res["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"].size());
    ASSERT_EQ(12, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("6", res["grouped_hits"][0]["hits"][0]["document"]["id"]);
    ASSERT_EQ("2", res["grouped_hits"][0]["hits"][1]["document"]["id"]);

    ASSERT_EQ(11, res["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("1", res["grouped_hits"][1]["hits"][0]["document"]["id"]);
    ASSERT_EQ("5", res["grouped_hits"][1]["hits"][1]["document"]["id"]);

    //try with pinned hits in other order
    res = coll_group->search("*", {"title"}, "size:=[12,11]", {}, {}, {0}, 50, 0, NOT_SET,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                                  "", 1,
                                  {"5:1,8:2"}, {}, {"size"}, 1).get();

    ASSERT_EQ(4, res["found"].get<size_t>());
    ASSERT_EQ(2, res["grouped_hits"].size());

    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"]);

    ASSERT_EQ(12, res["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("8", res["grouped_hits"][1]["hits"][0]["document"]["id"]);

    //random order
    res = coll_group->search("*", {"title"}, "size:=[12,11,10]", {}, {}, {0}, 50, 0, NOT_SET,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                             "", 1,
                             {"5:1,8:2,0:3"}, {}, {"size"}, 1).get();

    ASSERT_EQ(6, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());

    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"]);

    ASSERT_EQ(12, res["grouped_hits"][1]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"][1]["hits"].size());
    ASSERT_EQ("8", res["grouped_hits"][1]["hits"][0]["document"]["id"]);

    ASSERT_EQ(10, res["grouped_hits"][2]["group_key"][0].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"][2]["hits"].size());
    ASSERT_EQ("0", res["grouped_hits"][2]["hits"][0]["document"]["id"]);
}

TEST_F(CollectionGroupingTest, GroupByPerPage) {
    std::vector<field> fields = {
            field("name", field_types::STRING, false, false),
            field("id", field_types::STRING, true, true),
    };

    Collection* fabric = collectionManager.get_collection("fabric").get();
    if(fabric == nullptr) {
        fabric = collectionManager.create_collection("fabric", 1, fields).get();
    }


    nlohmann::json doc;

    doc["id"] = "1001";
    doc["name"] = "Cotton";
    ASSERT_TRUE(fabric->add(doc.dump()).ok());

    doc["id"] = "1002";
    doc["name"] = "Nylon";
    ASSERT_TRUE(fabric->add(doc.dump()).ok());

    doc["id"] = "1003";
    doc["name"] = "Polyester";
    ASSERT_TRUE(fabric->add(doc.dump()).ok());

    doc["id"] = "1004";
    doc["name"] = "Linen";
    ASSERT_TRUE(fabric->add(doc.dump()).ok());

    doc["id"] = "1005";
    doc["name"] = "Silk";
    ASSERT_TRUE(fabric->add(doc.dump()).ok());

    fields = {
            field("name", field_types::STRING, false, false),
            field("fabric_id", field_types::STRING, true, false, true,
            "", -1, -1, false, 0, 0, cosine, "fabric.id"),
            field("size", field_types::STRING, false, false),
    };

    Collection* garments = collectionManager.get_collection("garments").get();
    if(garments == nullptr) {
        garments = collectionManager.create_collection("garments", 1, fields).get();
    }

    nlohmann::json doc2;

    doc2["name"] = "Tshirt";
    doc2["fabric_id"] = "1001";
    doc2["size"] = "Medium";
    ASSERT_TRUE(garments->add(doc2.dump()).ok());

    doc2["name"] = "Tshirt";
    doc2["fabric_id"] = "1003";
    doc2["size"] = "Large";
    ASSERT_TRUE(garments->add(doc2.dump()).ok());

    doc2["name"] = "Shirt";
    doc2["fabric_id"] = "1004";
    doc2["size"] = "Xtra Large";
    ASSERT_TRUE(garments->add(doc2.dump()).ok());

    doc2["name"] = "Trouser";
    doc2["fabric_id"] = "1002";
    doc2["size"] = "Small";
    ASSERT_TRUE(garments->add(doc2.dump()).ok());

    doc2["name"] = "Veshti";
    doc2["fabric_id"] = "1005";
    doc2["size"] = "Free";
    ASSERT_TRUE(garments->add(doc2.dump()).ok());

    doc2["name"] = "Shorts";
    doc2["fabric_id"] = "1002";
    doc2["size"] = "Medium";
    ASSERT_TRUE(garments->add(doc2.dump()).ok());

    doc2["name"] = "Shirt";
    doc2["fabric_id"] = "1005";
    doc2["size"] = "Large";
    ASSERT_TRUE(garments->add(doc2.dump()).ok());

    //limit per page to 4
    auto res = garments->search("*", {"name"}, "", {}, {}, {0}, 4, 1, NOT_SET,
                             {false}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                             "", 1,
                             {}, {}, {"fabric_id"}, 1).get();

    ASSERT_EQ(5, res["found"].get<size_t>());
    ASSERT_EQ(4, res["grouped_hits"].size());
    ASSERT_EQ(7, res["found_docs"].get<size_t>());

    ASSERT_EQ("1005", res["grouped_hits"][0]["group_key"][0]);
    ASSERT_EQ("1002", res["grouped_hits"][1]["group_key"][0]);
    ASSERT_EQ("1004", res["grouped_hits"][2]["group_key"][0]);
    ASSERT_EQ("1003", res["grouped_hits"][3]["group_key"][0]);

    //per page 10
    res = garments->search("*", {"name"}, "", {}, {}, {0}, 10, 1, NOT_SET,
                                {false}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                                "", 1,
                                {}, {}, {"fabric_id"}, 1).get();

    ASSERT_EQ(5, res["found"].get<size_t>());
    ASSERT_EQ(5, res["grouped_hits"].size());
    ASSERT_EQ(7, res["found_docs"].get<size_t>());

    ASSERT_EQ("1005", res["grouped_hits"][0]["group_key"][0]);
    ASSERT_EQ("1002", res["grouped_hits"][1]["group_key"][0]);
    ASSERT_EQ("1004", res["grouped_hits"][2]["group_key"][0]);
    ASSERT_EQ("1003", res["grouped_hits"][3]["group_key"][0]);
    ASSERT_EQ("1001", res["grouped_hits"][4]["group_key"][0]);
}

TEST_F(CollectionGroupingTest, GroupByWithSplitJoinTokens) {
    std::vector<field> fields = {
        field("name", field_types::STRING, false, false),
        field("product_id", field_types::STRING, true, false),
    };

    Collection* coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields).get();
    }

    nlohmann::json doc;

    doc["product_id"] = "1001";
    doc["name"] = "Yes Correct";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1002";
    doc["name"] = "Yes Correcting";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1003";
    doc["name"] = "Yes Correcter";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1004";
    doc["name"] = "No Correctible";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1005";
    doc["name"] = "No Correction";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res = coll1->search("yescorrect", {"name"}, "", {"product_id"}, {}, {2}, 10, 1, NOT_SET,
                                {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                                "", 1,
                                {}, {}, {"product_id"}, 1).get();

    ASSERT_EQ(1, res["found"].get<size_t>());
    ASSERT_EQ(1, res["grouped_hits"].size());
    ASSERT_EQ(1, res["found_docs"].get<size_t>());
}

TEST_F(CollectionGroupingTest, GroupByWithEmptyValue) {
    std::vector<field> fields = {
            field("product_id", field_types::STRING, false, false),
            field("categories", field_types::STRING_ARRAY, true, true),
    };

    Collection* coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields).get();
    }

    nlohmann::json doc;

    doc["product_id"] = "1001";
    doc["categories"] = {"ALPHA1"};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1002";
    doc["categories"] = {"ALPHA2", ""};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1003";
    doc["categories"] = {"ALPHA3"};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("alpha", {"categories"}, "", {}, {}, {2}, 10, 1, NOT_SET,
                             {true}, Index::DROP_TOKENS_THRESHOLD,
                             spp::sparse_hash_set<std::string>(),
                             spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                             "", 1,
                             {}, {}, {"categories"}, 1);
    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();

    ASSERT_EQ(3, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());
    ASSERT_EQ(3, res["found_docs"].get<size_t>());
}

TEST_F(CollectionGroupingTest, GroupByWithCommaValue) {
    std::vector<field> fields = {
            field("product_id", field_types::STRING, false, false),
            field("categories", field_types::STRING_ARRAY, true, true),
    };

    Collection* coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields).get();
    }

    nlohmann::json doc;

    doc["product_id"] = "1000";
    doc["categories"] = {"ABC-DEF::4d03f::12.9519282285608,77.6062222810736"};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1000";
    doc["categories"] = {"DEF-GHI::4d03f::12.9519282285608,77.6062222810736"};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["product_id"] = "1000";
    doc["categories"] = {"GHI-JKL::4d03f::12.9519282285608,77.6062222810736"};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("1000", {"product_id"}, "", {}, {}, {2}, 10, 1, NOT_SET,
                                {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 4,
                                "", 1,
                                {}, {}, {"categories"}, 1);
    ASSERT_TRUE(res_op.ok());
    auto res = res_op.get();

    ASSERT_EQ(3, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());
    ASSERT_EQ(3, res["found_docs"].get<size_t>());
}

TEST_F(CollectionGroupingTest, SortByEval) {
    auto schema_json =
            R"({
                "name": "collection",
                "fields": [
                    {"name": "deduplicator", "type": "string", "facet": true, "optional": true},
                    {"name": "offer", "type": "object", "optional": true},
                    {"name": "offer._id", "type": "string", "optional": true},
                    {"name": "offer.price", "type": "float", "sort": true, "facet": true, "optional": true},
                    {"name": "category", "type": "string", "facet": true, "optional": true},
                    {"name": "subCategory", "type": "string", "facet": true, "optional": true},
                    {"name": "department", "type": "string", "facet": true, "optional": true},
                    {"name": "product", "type": "object", "optional": true},
                    {"name": "product.brand", "type": "string", "facet": true, "optional": true},
                    {"name": "collections", "type": "object", "optional": true},
                    {"name": "collections.names", "type": "string[]", "facet": true, "optional": true},
                    {"name": "offer.seller", "type": "object", "optional": true},
                    {"name": "offer.seller._id", "type": "string", "sort": true, "facet": true, "optional": true}
                ],
                "enable_nested_fields": true
            })"_json;
    std::vector<nlohmann::json> documents = {
            R"({
                "deduplicator": "r_0NRwMuoO",
                "offer": {
                    "_id": "offer_r_0NRwMuoO",
                    "seller": {
                        "_id": "r_0NRwMuoO"
                    },
                    "price": 100
                },
                "category": "Test Category",
                "subCategory": "Test SubCategory",
                "department": "Test Department",
                "product": {
                    "brand": "Test Brand"
                },
                "collections": {
                    "names": [
                        "top-200-produtos"
                    ]
                }
            })"_json,
            R"({
                "deduplicator": "ebhDVjRA2-",
                "offer": {
                    "_id": "offer_ebhDVjRA2-",
                    "seller": {
                        "_id": "ebhDVjRA2-"
                    },
                    "price": 100
                },
                "category": "Test Category",
                "subCategory": "Test SubCategory",
                "department": "Test Department",
                "product": {
                    "brand": "Test Brand"
                },
                "collections": {
                    "names": [
                        "top-200-produtos"
                    ]
                }
            })"_json,
            R"({
                "deduplicator": "OMDZGUlnMh",
                "offer": {
                    "_id": "offer_OMDZGUlnMh",
                    "seller": {
                        "_id": "OMDZGUlnMh"
                    },
                    "price": 100
                },
                "category": "Test Category",
                "subCategory": "Test SubCategory",
                "department": "Test Department",
                "product": {
                    "brand": "Test Brand"
                },
                "collections": {
                    "names": [
                        "top-200-produtos"
                    ]
                }
            })"_json,
            R"({
                "deduplicator": "iWYhaXHLuV",
                "offer": {
                    "_id": "offer_iWYhaXHLuV",
                    "seller": {
                        "_id": "iWYhaXHLuV"
                    },
                    "price": 100
                },
                "category": "Test Category",
                "subCategory": "Test SubCategory",
                "department": "Test Department",
                "product": {
                    "brand": "Test Brand"
                },
                "collections": {
                    "names": [
                        "top-200-produtos"
                    ]
                }
            })"_json,
            R"({
                "deduplicator": "GcVpUulgN-",
                "offer": {
                    "_id": "offer_GcVpUulgN-",
                    "seller": {
                        "_id": "GcVpUulgN-"
                    },
                    "price": 100
                },
                "category": "Test Category",
                "subCategory": "Test SubCategory",
                "department": "Test Department",
                "product": {
                    "brand": "Test Brand"
                },
                "collections": {
                    "names": [
                        "top-200-produtos"
                    ]
                }
            })"_json,
    };

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();
    for (auto const &json: documents) {
        auto add_op = coll->add(json.dump());
        if (!add_op.ok()) {
            LOG(INFO) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    std::map<std::string, std::string> req_params = {
            {"collection", "collection"},
            {"q", "*"},
            {"group_by", "deduplicator"},
            {"group_limit", "1"},
            {"filter_by", "offer.seller._id:[r_0NRwMuoO,ebhDVjRA2-,OMDZGUlnMh,iWYhaXHLuV,GcVpUulgN-,hLDQv6xYTq,5N1kxAEtEi,"
                          "wzJkWEpMQl,fHr7liahKk,HH8zOmY4Qg,Du4B3eYe-m,ZN-bR62Tdt,LisFdBlIPd,EB6Qp7Rzl0,LoVjFroRNF,cfemmOea2l,"
                          "fe9V0IPrGN,OXFnytzfC1,ZsvMOt1kF_,jVelifFXKT,TGc8CRZyvv,XCS3jRwrH5,Dc337159LV,qfEi-OeU8A,7e6J3qkQjM,"
                          "GailP9H-2y,WFjAhbf4gb,wEqE3L1vft,QcJF9-KXbp,-anlIuFtY-] && collections.names:=top-200-produtos"},
            {"facet_by", "offer.seller._id,category,subCategory,department,product.brand,collections.names"},
            {"sort_by", "_text_match:desc,_eval([(offer.seller._id:r_0NRwMuoO):30,(offer.seller._id:ebhDVjRA2-):30,"
                        "(offer.seller._id:OMDZGUlnMh):30,(offer.seller._id:iWYhaXHLuV):30,(offer.seller._id:GcVpUulgN-):30,"
                        "(offer.seller._id:hLDQv6xYTq):30,(offer.seller._id:5N1kxAEtEi):30,(offer.seller._id:wzJkWEpMQl):30,"
                        "(offer.seller._id:fHr7liahKk):30,(offer.seller._id:HH8zOmY4Qg):30,(offer.seller._id:Du4B3eYe-m):30,"
                        "(offer.seller._id:ZN-bR62Tdt):30,(offer.seller._id:LisFdBlIPd):30,(offer.seller._id:EB6Qp7Rzl0):30,"
                        "(offer.seller._id:LoVjFroRNF):30,(offer.seller._id:cfemmOea2l):30,(offer.seller._id:fe9V0IPrGN):30,"
                        "(offer.seller._id:OXFnytzfC1):30,(offer.seller._id:ZsvMOt1kF_):30,(offer.seller._id:jVelifFXKT):30,"
                        "(offer.seller._id:TGc8CRZyvv):30,(offer.seller._id:XCS3jRwrH5):30,(offer.seller._id:Dc337159LV):30,"
                        "(offer.seller._id:qfEi-OeU8A):30,(offer.seller._id:7e6J3qkQjM):30,(offer.seller._id:GailP9H-2y):30,"
                        "(offer.seller._id:WFjAhbf4gb):30,(offer.seller._id:wEqE3L1vft):30,(offer.seller._id:QcJF9-KXbp):30,"
                        "(offer.seller._id:-anlIuFtY-):30]):desc,offer.price:asc"}
    };

    nlohmann::json embedded_params;
    std::string json_res;
    long now_ts = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto search_op = collectionManager.do_search(req_params, embedded_params, json_res, now_ts);
    ASSERT_TRUE(search_op.ok());

    nlohmann::json res_obj = nlohmann::json::parse(json_res);
    // Validate top-level keys
    ASSERT_EQ(5, res_obj["found"].get<size_t>());
    ASSERT_EQ(5, res_obj["found_docs"].get<size_t>());
    ASSERT_EQ(5, res_obj["out_of"].get<size_t>());

    // Validate facet_counts
    ASSERT_TRUE(res_obj["facet_counts"].is_array());
    ASSERT_EQ(6, res_obj["facet_counts"].size());

    // Validate offer.seller._id facet
    ASSERT_EQ("offer.seller._id", res_obj["facet_counts"][0]["field_name"]);
    ASSERT_EQ(5, res_obj["facet_counts"][0]["counts"].size());
    ASSERT_EQ(1, res_obj["facet_counts"][0]["counts"][0]["count"]);
    ASSERT_EQ("r_0NRwMuoO", res_obj["facet_counts"][0]["counts"][0]["value"]);

    // Validate category facet
    ASSERT_EQ("category", res_obj["facet_counts"][1]["field_name"]);
    ASSERT_EQ(1, res_obj["facet_counts"][1]["counts"].size());
    ASSERT_EQ(5, res_obj["facet_counts"][1]["counts"][0]["count"]);
    ASSERT_EQ("Test Category", res_obj["facet_counts"][1]["counts"][0]["value"]);

    // Validate subCategory facet
    ASSERT_EQ("subCategory", res_obj["facet_counts"][2]["field_name"]);
    ASSERT_EQ(1, res_obj["facet_counts"][2]["counts"].size());
    ASSERT_EQ(5, res_obj["facet_counts"][2]["counts"][0]["count"]);
    ASSERT_EQ("Test SubCategory", res_obj["facet_counts"][2]["counts"][0]["value"]);

    // Validate department facet
    ASSERT_EQ("department", res_obj["facet_counts"][3]["field_name"]);
    ASSERT_EQ(1, res_obj["facet_counts"][3]["counts"].size());
    ASSERT_EQ(5, res_obj["facet_counts"][3]["counts"][0]["count"]);
    ASSERT_EQ("Test Department", res_obj["facet_counts"][3]["counts"][0]["value"]);

    // Validate product.brand facet
    ASSERT_EQ("product.brand", res_obj["facet_counts"][4]["field_name"]);
    ASSERT_EQ(1, res_obj["facet_counts"][4]["counts"].size());
    ASSERT_EQ(5, res_obj["facet_counts"][4]["counts"][0]["count"]);
    ASSERT_EQ("Test Brand", res_obj["facet_counts"][4]["counts"][0]["value"]);

    // Validate collections.names facet
    ASSERT_EQ("collections.names", res_obj["facet_counts"][5]["field_name"]);
    ASSERT_EQ(1, res_obj["facet_counts"][5]["counts"].size());
    ASSERT_EQ(5, res_obj["facet_counts"][5]["counts"][0]["count"]);
    ASSERT_EQ("top-200-produtos", res_obj["facet_counts"][5]["counts"][0]["value"]);

    // Validate grouped_hits
    ASSERT_EQ(5, res_obj["grouped_hits"].size());

    for (size_t i = 0; i < res_obj["grouped_hits"].size(); ++i) {
        auto& hit_group = res_obj["grouped_hits"][i];
        ASSERT_EQ(1, hit_group["found"].get<size_t>());
        ASSERT_EQ(1, hit_group["hits"].size());

        auto& document = hit_group["hits"][0]["document"];
        ASSERT_EQ("Test Category", document["category"].get<std::string>());
        ASSERT_EQ("Test SubCategory", document["subCategory"].get<std::string>());
        ASSERT_EQ("Test Department", document["department"].get<std::string>());
        ASSERT_EQ("Test Brand", document["product"]["brand"].get<std::string>());
        ASSERT_EQ("top-200-produtos", document["collections"]["names"][0].get<std::string>());

        // Validate offer
        auto& offer = document["offer"];
        ASSERT_EQ(100, offer["price"].get<int>());
        ASSERT_EQ(document["deduplicator"].get<std::string>(), offer["seller"]["_id"].get<std::string>());
    }
}