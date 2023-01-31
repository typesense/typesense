#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

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

    ASSERT_EQ(3, res["found"].get<size_t>());
    ASSERT_EQ(3, res["grouped_hits"].size());
    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());

    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_EQ(11, res["grouped_hits"][0]["hits"][0]["document"]["size"].get<size_t>());
    ASSERT_STREQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][0]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("1", res["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("4", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.6, res["grouped_hits"][1]["hits"][1]["document"]["rating"].get<float>());
    ASSERT_STREQ("3", res["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());

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
    ASSERT_EQ(7, res["found"].get<size_t>());
    ASSERT_EQ(7, res["grouped_hits"].size());
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][0]["group_key"][0].get<float>());

    ASSERT_EQ(12, res["grouped_hits"][0]["hits"][0]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("8", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.4, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());

    ASSERT_EQ(12, res["grouped_hits"][1]["hits"][0]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("6", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());

    ASSERT_EQ(11, res["grouped_hits"][1]["hits"][1]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("1", res["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.3, res["grouped_hits"][1]["hits"][1]["document"]["rating"].get<float>());

    ASSERT_EQ(10, res["grouped_hits"][5]["hits"][0]["document"]["size"].get<uint32_t>());
    ASSERT_STREQ("9", res["grouped_hits"][5]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_FLOAT_EQ(4.1, res["grouped_hits"][5]["hits"][0]["document"]["rating"].get<float>());

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
}

TEST_F(CollectionGroupingTest, GroupingCompoundKey) {
    // group by size+brand (int32, string)
    auto res = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"size", "brand"}, 2).get();

    ASSERT_EQ(10, res["found"].get<size_t>());
    ASSERT_EQ(10, res["grouped_hits"].size());

    ASSERT_EQ(11, res["grouped_hits"][0]["group_key"][0].get<size_t>());
    ASSERT_STREQ("Beta", res["grouped_hits"][0]["group_key"][1].get<std::string>().c_str());

    // optional field should have no value in the group key component
    ASSERT_EQ(1, res["grouped_hits"][5]["group_key"].size());
    ASSERT_STREQ("10", res["grouped_hits"][5]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("11", res["grouped_hits"][5]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][0]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());

    ASSERT_EQ(1, res["grouped_hits"][1]["hits"].size());
    ASSERT_FLOAT_EQ(4.8, res["grouped_hits"][1]["hits"][0]["document"]["rating"].get<float>());
    ASSERT_STREQ("4", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());

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
    ASSERT_STREQ("Value of `group_limit` must be between 1 and 99.", res_op.error().c_str());

    res_op = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                {false}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "brand: omeg", 30, 5,
                                "", 10,
                                {}, {}, {"rating"}, 0);

    ASSERT_FALSE(res_op.ok());
    ASSERT_STREQ("Value of `group_limit` must be between 1 and 99.", res_op.error().c_str());
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

    ASSERT_EQ(3, results["found"].get<size_t>());
    ASSERT_EQ(3, results["grouped_hits"].size());

    ASSERT_STREQ("pop", results["grouped_hits"][0]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, results["grouped_hits"][0]["hits"].size());
    ASSERT_STREQ("1", results["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("4", results["grouped_hits"][0]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("rock", results["grouped_hits"][1]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, results["grouped_hits"][1]["hits"].size());
    ASSERT_STREQ("5", results["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["grouped_hits"][1]["hits"][1]["document"]["id"].get<std::string>().c_str());

    ASSERT_STREQ("country", results["grouped_hits"][2]["group_key"][0].get<std::string>().c_str());
    ASSERT_EQ(2, results["grouped_hits"][2]["hits"].size());
    ASSERT_STREQ("3", results["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("8", results["grouped_hits"][2]["hits"][1]["document"]["id"].get<std::string>().c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionGroupingTest, GroupingWithGropLimitOfOne) {
    auto res = coll_group->search("*", {}, "", {"brand"}, {}, {0}, 50, 1, FREQUENCY,
                                  {false}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                  "", 10,
                                  {}, {}, {"brand"}, 1).get();

    ASSERT_EQ(5, res["found"].get<size_t>());
    ASSERT_EQ(5, res["grouped_hits"].size());

    // all hits array must be of size 1
    for(auto i=0; i<5; i++) {
        ASSERT_EQ(1, res["grouped_hits"][i]["hits"].size());
    }

    ASSERT_STREQ("5", res["grouped_hits"][0]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("3", res["grouped_hits"][1]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("8", res["grouped_hits"][2]["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("10", res["grouped_hits"][3]["hits"][0]["document"]["id"].get<std::string>().c_str()); // unbranded
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
    ASSERT_STREQ("Beta", res["facet_counts"][0]["counts"][0]["value"].get<std::string>().c_str());

    ASSERT_EQ(2, (int) res["facet_counts"][0]["counts"][1]["count"]);
    ASSERT_STREQ("Xorp", res["facet_counts"][0]["counts"][1]["value"].get<std::string>().c_str());

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
