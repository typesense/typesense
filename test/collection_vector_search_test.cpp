#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include "collection.h"
#include <cstdlib>
#include <ctime>

class CollectionVectorTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_vector_search";
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

TEST_F(CollectionVectorTest, BasicVectorQuerying) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    auto coll_summary = coll1->get_summary_json();
    ASSERT_EQ("cosine", coll_summary["fields"][2]["vec_dist"].get<std::string>());

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
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
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
    results = coll1->search("*", {}, "points:[0,1]", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 0)").get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // with filtering + flat search
    results = coll1->search("*", {}, "points:[0,1]", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 1000)").get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // must trim space after field name
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec :([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(3, results["found"].get<size_t>());

    // validate wrong dimensions in query
    auto res_op = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                          spp::sparse_hash_set<std::string>(),
                                          spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                          "", 10, {}, {}, {}, 0,
                                          "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                          4, {off}, 32767, 32767, 2,
                                          false, true, "vec:([0.96826, 0.94, 0.39557])");

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Query field `vec` must have 4 dimensions.", res_op.error());

    // validate bad vector query field name
    res_op = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                          spp::sparse_hash_set<std::string>(),
                                          spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                          "", 10, {}, {}, {}, 0,
                                          "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                          4, {off}, 32767, 32767, 2,
                                          false, true, "zec:([0.96826, 0.94, 0.39557, 0.4542])");

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Field `zec` does not have a vector query index.", res_op.error());

    // pass `id` of existing doc instead of vector, query doc should be omitted from results
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([], id: 1)").get();

    ASSERT_EQ(2, results["found"].get<size_t>());
    ASSERT_EQ(2, results["hits"].size());

    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("2", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    // `k` value should overrides per_page
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], k: 1)").get();

    ASSERT_EQ(1, results["hits"].size());

    // when k is not set, should use per_page
    results = coll1->search("*", {}, "", {}, {}, {0}, 2, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(2, results["hits"].size());

    // when `id` does not exist, return appropriate error
    res_op = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                           4, {off}, 32767, 32767, 2,
                           false, true, "vec:([], id: 100)");

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Document id referenced in vector query is not found.", res_op.error());

    // support num_dim on only float array fields
    schema = R"({
        "name": "coll2",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "vec", "type": "float", "num_dim": 4}
        ]
    })"_json;

    auto coll_op = collectionManager.create_collection(schema);
    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("Property `num_dim` is only allowed on a float array field.", coll_op.error());

    // bad value for num_dim
    schema = R"({
        "name": "coll2",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "vec", "type": "float", "num_dim": -4}
        ]
    })"_json;

    coll_op = collectionManager.create_collection(schema);
    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("Property `num_dim` must be a positive integer.", coll_op.error());

    collectionManager.drop_collection("coll1");
}

TEST_F(CollectionVectorTest, VectorDistanceConfig) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4, "vec_dist": "ip"}
        ]
    })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();

    auto coll_summary = coll1->get_summary_json();
    ASSERT_EQ("ip", coll_summary["fields"][2]["vec_dist"].get<std::string>());
}

TEST_F(CollectionVectorTest, VectorUnchangedUpsert) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "points", "type": "int32"},
                {"name": "vec", "type": "float[]", "num_dim": 3}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::vector<float> vec = {0.12, 0.45, 0.64};

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;
    doc["vec"] = vec;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.12, 0.44, 0.55])").get();

    ASSERT_EQ(1, results["found"].get<size_t>());


    // upsert unchanged doc
    add_op = coll1->add(doc.dump(), index_operation_t::UPSERT);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.12, 0.44, 0.55])").get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    // emplace unchanged doc
    add_op = coll1->add(doc.dump(), index_operation_t::EMPLACE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.12, 0.44, 0.55])").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionVectorTest, VectorPartialUpdate) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "points", "type": "int32"},
                {"name": "vec", "type": "float[]", "num_dim": 3}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::vector<float> vec = {0.12, 0.45, 0.64};

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;
    doc["vec"] = vec;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.12, 0.44, 0.55])").get();

    ASSERT_EQ(1, results["found"].get<size_t>());


    // emplace partial doc
    doc.erase("vec");
    doc["title"] = "Random";
    add_op = coll1->add(doc.dump(), index_operation_t::EMPLACE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("Random", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.12, 0.44, 0.55])").get();

    ASSERT_EQ(1, results["found"].get<size_t>());

    // update portial doc

    doc.erase("vec");
    doc["title"] = "Random";
    add_op = coll1->add(doc.dump(), index_operation_t::UPDATE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("Random", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.12, 0.44, 0.55])").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionVectorTest, NumVectorGreaterThanNumDim) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "points", "type": "int32"},
                {"name": "vec", "type": "float[]", "num_dim": 3}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    srand (static_cast <unsigned> (time(0)));

    for(size_t i = 0; i < 10; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = "Title";
        doc["points"] = 100;
        doc["vec"] = std::vector<float>();

        for(size_t j = 0; j < 100; j++) {
            float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
            doc["vec"].push_back(r);
        }

        auto add_op = coll1->add(doc.dump());
        ASSERT_FALSE(add_op.ok());
        ASSERT_EQ("Field `vec` must have 3 dimensions.", add_op.error());
    }
}

TEST_F(CollectionVectorTest, IndexGreaterThan1KVectors) {
    // tests the dynamic resizing of graph
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    size_t d = 4;
    size_t n = 1500;

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    for (size_t i = 0; i < n; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = std::to_string(i) + " title";
        doc["points"] = i;

        std::vector<float> values;
        for (size_t j = 0; j < d; j++) {
            values.push_back(distrib(rng));
        }
        doc["vec"] = values;

        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "").get();

    ASSERT_EQ(1500, results["found"].get<size_t>());
}

TEST_F(CollectionVectorTest, InsertDocWithEmptyVectorAndDelete) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "vec", "type": "float[]", "num_dim": 4, "optional": true}
        ]
    })"_json;

    Collection *coll1 = collectionManager.create_collection(schema).get();
    nlohmann::json doc;
    doc["id"] = "0";
    doc["vec"] = {};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());
    ASSERT_TRUE(coll1->remove("0").ok());
}

TEST_F(CollectionVectorTest, VecSearchWithFiltering) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    size_t num_docs = 20;

    for (size_t i = 0; i < num_docs; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = std::to_string(i) + " title";
        doc["points"] = i;

        std::vector<float> values;
        for(size_t j = 0; j < 4; j++) {
            values.push_back(distrib(rng));
        }

        doc["vec"] = values;
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(num_docs, results["found"].get<size_t>());
    ASSERT_EQ(num_docs, results["hits"].size());

    // with points:<10, non-flat-search

    results = coll1->search("*", {}, "points:<10", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 0)").get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(10, results["hits"].size());

    // with points:<10, flat-search
    results = coll1->search("*", {}, "points:<10", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 1000)").get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(10, results["hits"].size());

    // single point

    results = coll1->search("*", {}, "points:1", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 0)").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*", {}, "points:1", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 1000)").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());
}

TEST_F(CollectionVectorTest, VecSearchWithFilteringWithMissingVectorValues) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4, "optional": true}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    size_t num_docs = 20;
    std::vector<std::string> json_lines;

    for (size_t i = 0; i < num_docs; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = std::to_string(i) + " title";
        doc["points"] = i;

        std::vector<float> values;
        for(size_t j = 0; j < 4; j++) {
            values.push_back(distrib(rng));
        }

        if(i != 5 && i != 15) {
            doc["vec"] = values;
        }

        json_lines.push_back(doc.dump());
    }

    nlohmann::json insert_doc;
    auto res = coll1->add_many(json_lines, insert_doc, UPSERT);
    ASSERT_TRUE(res["success"].get<bool>());

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(18, results["found"].get<size_t>());
    ASSERT_EQ(18, results["hits"].size());

    // with points:<10, non-flat-search

    results = coll1->search("*", {}, "points:<10", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 0)").get();

    ASSERT_EQ(9, results["found"].get<size_t>());
    ASSERT_EQ(9, results["hits"].size());

    // with points:<10, flat-search
    results = coll1->search("*", {}, "points:<10", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 1000)").get();

    ASSERT_EQ(9, results["found"].get<size_t>());
    ASSERT_EQ(9, results["hits"].size());

    // single point

    results = coll1->search("*", {}, "points:1", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 0)").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*", {}, "points:1", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 1000)").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    ASSERT_EQ(1, coll1->_get_index()->_get_numerical_index().size());
    ASSERT_EQ(1, coll1->_get_index()->_get_numerical_index().count("points"));

    // should not be able to filter / sort / facet on vector fields
    auto res_op = coll1->search("*", {}, "vec:1", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                  spp::sparse_hash_set<std::string>(),
                                  spp::sparse_hash_set<std::string>());

    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Cannot filter on vector field `vec`.", res_op.error());

    schema = R"({
        "name": "coll2",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "vec", "type": "float[]", "num_dim": 4, "facet": true}
        ]
    })"_json;

    auto coll_op = collectionManager.create_collection(schema);
    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("Property `facet` is not allowed on a vector field.", coll_op.error());

    schema = R"({
        "name": "coll2",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "vec", "type": "float[]", "num_dim": 4, "sort": true}
        ]
    })"_json;

    coll_op = collectionManager.create_collection(schema);
    ASSERT_FALSE(coll_op.ok());
    ASSERT_EQ("Property `sort` cannot be enabled on a vector field.", coll_op.error());
}

TEST_F(CollectionVectorTest, VectorSearchTestDeletion) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    size_t num_docs = 20;

    for (size_t i = 0; i < num_docs; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = std::to_string(i) + " title";
        doc["points"] = i;

        std::vector<float> values;
        for(size_t j = 0; j < 4; j++) {
            values.push_back(distrib(rng));
        }

        doc["vec"] = values;
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    ASSERT_EQ(1024, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(20, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(0, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

    // now delete these docs

    for (size_t i = 0; i < num_docs; i++) {
        ASSERT_TRUE(coll1->remove(std::to_string(i)).ok());
    }

    ASSERT_EQ(1024, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(20, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(20, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

    for (size_t i = 0; i < num_docs; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i + num_docs);
        doc["title"] = std::to_string(i + num_docs) + " title";
        doc["points"] = i;

        std::vector<float> values;
        for(size_t j = 0; j < 4; j++) {
            values.push_back(distrib(rng));
        }

        doc["vec"] = values;
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    ASSERT_EQ(1024, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(20, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(0, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

    // delete those docs again and ensure that while reindexing till 1024 live docs, max count is not changed
    for (size_t i = 0; i < num_docs; i++) {
        ASSERT_TRUE(coll1->remove(std::to_string(i + num_docs)).ok());
    }

    ASSERT_EQ(1024, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(20, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(20, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

    for (size_t i = 0; i < 1014; i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(10000 + i);
        doc["title"] = std::to_string(10000 + i) + " title";
        doc["points"] = i;

        std::vector<float> values;
        for(size_t j = 0; j < 4; j++) {
            values.push_back(distrib(rng));
        }

        doc["vec"] = values;
        const Option<nlohmann::json>& add_op = coll1->add(doc.dump());
        if(!add_op.ok()) {
            LOG(ERROR) << add_op.error();
        }
        ASSERT_TRUE(add_op.ok());
    }

    ASSERT_EQ(1024, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(1014, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(0, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());
}

TEST_F(CollectionVectorTest, VectorWithNullValue) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::vector<std::string> json_lines;

    nlohmann::json doc;

    doc["id"] = "0";
    doc["vec"] = {0.1, 0.2, 0.3, 0.4};
    json_lines.push_back(doc.dump());


    doc["id"] = "1";
    doc["vec"] = nullptr;
    json_lines.push_back(doc.dump());

    auto res = coll1->add_many(json_lines, doc);

    ASSERT_FALSE(res["success"].get<bool>());
    ASSERT_EQ(1, res["num_imported"].get<size_t>());

    ASSERT_TRUE(nlohmann::json::parse(json_lines[0])["success"].get<bool>());
    ASSERT_FALSE(nlohmann::json::parse(json_lines[1])["success"].get<bool>());
    ASSERT_EQ("Field `vec` must be an array.",
              nlohmann::json::parse(json_lines[1])["error"].get<std::string>());
}

TEST_F(CollectionVectorTest, EmbeddedVectorUnchangedUpsert) {
    nlohmann::json schema = R"({
                "name": "coll1",
                "fields": [
                    {"name": "title", "type": "string"},
                    {"name": "points", "type": "int32"},
                    {"name": "embedding", "type":"float[]", "embed":{"from": ["title"],
                        "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("title", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    auto embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_EQ(384, embedding.size());

    // upsert unchanged doc
    doc.clear();
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;

    add_op = coll1->add(doc.dump(), index_operation_t::UPSERT);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("title", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>()).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_EQ(384, embedding.size());

    // update

    doc.clear();
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;

    add_op = coll1->add(doc.dump(), index_operation_t::UPDATE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("title", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>()).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_EQ(384, embedding.size());

    // emplace

    doc.clear();
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;

    add_op = coll1->add(doc.dump(), index_operation_t::EMPLACE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("title", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>()).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_EQ(384, embedding.size());
}

TEST_F(CollectionVectorTest, HybridSearchWithExplicitVector) {
    nlohmann::json schema = R"({
                            "name": "objects",
                            "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
                            ]
                        })"_json;
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();
    nlohmann::json object;
    object["name"] = "butter";
    auto add_op = coll->add(object.dump());
    ASSERT_TRUE(add_op.ok());

    object["name"] = "butterball";
    add_op = coll->add(object.dump());
    ASSERT_TRUE(add_op.ok());

    object["name"] = "butterfly";
    add_op = coll->add(object.dump());
    ASSERT_TRUE(add_op.ok());

    nlohmann::json model_config = R"({
        "model_name": "ts/e5-small"
    })"_json;

    auto query_embedding = TextEmbedderManager::get_instance().get_text_embedder(model_config).get()->Embed("butter");
    
    std::string vec_string = "[";
    for(size_t i = 0; i < query_embedding.embedding.size(); i++) {
        vec_string += std::to_string(query_embedding.embedding[i]);
        if(i != query_embedding.embedding.size() - 1) {
            vec_string += ",";
        }
    }
    vec_string += "]";  
    auto search_res_op = coll->search("butter", {"name"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:(" + vec_string + ")");
    
    ASSERT_TRUE(search_res_op.ok());
    auto search_res = search_res_op.get();
    ASSERT_EQ(3, search_res["found"].get<size_t>());
    ASSERT_EQ(3, search_res["hits"].size());
    // Hybrid search with rank fusion order:
    // 1. butter (1/1 * 0.7) + (1/1 * 0.3) = 1
    // 2. butterfly (1/2 * 0.7) + (1/3 * 0.3) = 0.45
    // 3. butterball (1/3 * 0.7) + (1/2 * 0.3) = 0.383
    ASSERT_EQ("butter", search_res["hits"][0]["document"]["name"].get<std::string>());
    ASSERT_EQ("butterfly", search_res["hits"][1]["document"]["name"].get<std::string>());
    ASSERT_EQ("butterball", search_res["hits"][2]["document"]["name"].get<std::string>());

    ASSERT_FLOAT_EQ((1.0/1.0 * 0.7) + (1.0/1.0 * 0.3), search_res["hits"][0]["hybrid_search_info"]["rank_fusion_score"].get<float>());
    ASSERT_FLOAT_EQ((1.0/2.0 * 0.7) + (1.0/3.0 * 0.3), search_res["hits"][1]["hybrid_search_info"]["rank_fusion_score"].get<float>());
    ASSERT_FLOAT_EQ((1.0/3.0 * 0.7) + (1.0/2.0 * 0.3), search_res["hits"][2]["hybrid_search_info"]["rank_fusion_score"].get<float>());

    // hybrid search with empty vector (to pass distance threshold param)
    std::string vec_query = "embedding:([], distance_threshold: 0.20)";

    search_res_op = coll->search("butter", {"embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, vec_query);
    ASSERT_TRUE(search_res_op.ok());
    search_res = search_res_op.get();

    ASSERT_EQ(2, search_res["found"].get<size_t>());
    ASSERT_EQ(2, search_res["hits"].size());

    ASSERT_NEAR(0.04620, search_res["hits"][0]["vector_distance"].get<float>(), 0.0001);
    ASSERT_NEAR(0.12133, search_res["hits"][1]["vector_distance"].get<float>(), 0.0001);

    // to pass k param
    vec_query = "embedding:([], k: 1)";
    search_res_op = coll->search("butter", {"embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, vec_query);
    ASSERT_TRUE(search_res_op.ok());
    search_res = search_res_op.get();
    ASSERT_EQ(1, search_res["found"].get<size_t>());
    ASSERT_EQ(1, search_res["hits"].size());

    // when no embedding field is passed, it should not be allowed
    search_res_op = coll->search("butter", {"name"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, vec_query);
    ASSERT_FALSE(search_res_op.ok());
    ASSERT_EQ("Vector query could not find any embedded fields.", search_res_op.error());

    // when no vector matches distance threshold, only text matches are entertained and distance score should be
    // 2 in those cases
    vec_query = "embedding:([], distance_threshold: 0.01)";
    search_res_op = coll->search("butter", {"name", "embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, vec_query);
    ASSERT_TRUE(search_res_op.ok());
    search_res = search_res_op.get();

    ASSERT_EQ(3, search_res["found"].get<size_t>());
    ASSERT_EQ(3, search_res["hits"].size());

    ASSERT_FLOAT_EQ(2.0f, search_res["hits"][0]["vector_distance"].get<float>());
    ASSERT_FLOAT_EQ(2.0f, search_res["hits"][1]["vector_distance"].get<float>());
    ASSERT_FLOAT_EQ(2.0f, search_res["hits"][2]["vector_distance"].get<float>());
}

TEST_F(CollectionVectorTest, HybridSearchOnlyVectorMatches) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string", "facet": true},
            {"name": "vec", "type": "float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");
    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["name"] = "john doe";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto results_op = coll1->search("zzz", {"name", "vec"}, "", {"name"}, {}, {0}, 20, 1, FREQUENCY, {true},
                                    Index::DROP_TOKENS_THRESHOLD,
                                    spp::sparse_hash_set<std::string>(),
                                    spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                    "", 10, {}, {}, {}, 0,
                                    "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                    fallback,
                                    4, {off}, 32767, 32767, 2);
    ASSERT_EQ(true, results_op.ok());
    ASSERT_EQ(1, results_op.get()["found"].get<size_t>());
    ASSERT_EQ(1, results_op.get()["hits"].size());
    ASSERT_EQ(1, results_op.get()["facet_counts"].size());
    ASSERT_EQ(4, results_op.get()["facet_counts"][0].size());
    ASSERT_EQ("name", results_op.get()["facet_counts"][0]["field_name"]);
}

TEST_F(CollectionVectorTest, DistanceThresholdTest) {
    nlohmann::json schema = R"({
        "name": "test",
        "fields": [
            {"name": "vec", "type": "float[]", "num_dim": 3}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["vec"] = {0.1, 0.2, 0.3};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    // write a vector which is 0.5 away from the first vector
    doc["vec"] = {0.6, 0.7, 0.8};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());


    auto results_op = coll1->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "vec:([0.3,0.4,0.5])");

    ASSERT_EQ(true, results_op.ok());
    ASSERT_EQ(2, results_op.get()["found"].get<size_t>());
    ASSERT_EQ(2, results_op.get()["hits"].size());

    ASSERT_FLOAT_EQ(0.6, results_op.get()["hits"][0]["document"]["vec"].get<std::vector<float>>()[0]);
    ASSERT_FLOAT_EQ(0.7, results_op.get()["hits"][0]["document"]["vec"].get<std::vector<float>>()[1]);
    ASSERT_FLOAT_EQ(0.8, results_op.get()["hits"][0]["document"]["vec"].get<std::vector<float>>()[2]);

    ASSERT_FLOAT_EQ(0.1, results_op.get()["hits"][1]["document"]["vec"].get<std::vector<float>>()[0]);
    ASSERT_FLOAT_EQ(0.2, results_op.get()["hits"][1]["document"]["vec"].get<std::vector<float>>()[1]);
    ASSERT_FLOAT_EQ(0.3, results_op.get()["hits"][1]["document"]["vec"].get<std::vector<float>>()[2]);

    results_op = coll1->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "vec:([0.3,0.4,0.5], distance_threshold:0.01)");
    
    ASSERT_EQ(true, results_op.ok());
    ASSERT_EQ(1, results_op.get()["found"].get<size_t>());
    ASSERT_EQ(1, results_op.get()["hits"].size());

    ASSERT_FLOAT_EQ(0.6, results_op.get()["hits"][0]["document"]["vec"].get<std::vector<float>>()[0]);
    ASSERT_FLOAT_EQ(0.7, results_op.get()["hits"][0]["document"]["vec"].get<std::vector<float>>()[1]);
    ASSERT_FLOAT_EQ(0.8, results_op.get()["hits"][0]["document"]["vec"].get<std::vector<float>>()[2]);

}


TEST_F(CollectionVectorTest, HybridSearchSortByGeopoint) {
    nlohmann::json schema = R"({
                "name": "objects",
                "fields": [
                {"name": "name", "type": "string"},
                {"name": "location", "type": "geopoint"},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;
    

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());

    auto coll = op.get();

    nlohmann::json doc;
    doc["name"] = "butter";
    doc["location"] = {80.0, 150.0};

    auto add_op = coll->add(doc.dump());

    ASSERT_TRUE(add_op.ok());

    doc["name"] = "butterball";
    doc["location"] = {40.0, 100.0};

    add_op = coll->add(doc.dump());

    ASSERT_TRUE(add_op.ok());

    doc["name"] = "butterfly";
    doc["location"] = {130.0, 200.0};

    add_op = coll->add(doc.dump());

    ASSERT_TRUE(add_op.ok());


    spp::sparse_hash_set<std::string> dummy_include_exclude;

    std::vector<sort_by> sort_by_list = {{"location(10.0, 10.0)", "asc"}};

    auto search_res_op = coll->search("butter", {"name", "embedding"}, "", {}, sort_by_list, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD, dummy_include_exclude, dummy_include_exclude, 10);

    ASSERT_TRUE(search_res_op.ok());

    auto search_res = search_res_op.get();

    ASSERT_EQ("butterfly", search_res["hits"][0]["document"]["name"].get<std::string>());
    ASSERT_EQ("butterball", search_res["hits"][1]["document"]["name"].get<std::string>());
    ASSERT_EQ("butter", search_res["hits"][2]["document"]["name"].get<std::string>());


    search_res_op = coll->search("butter", {"name", "embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {false}, Index::DROP_TOKENS_THRESHOLD, dummy_include_exclude, dummy_include_exclude, 10);

    ASSERT_TRUE(search_res_op.ok());

    search_res = search_res_op.get();


    ASSERT_EQ("butter", search_res["hits"][0]["document"]["name"].get<std::string>());
    ASSERT_EQ("butterball", search_res["hits"][1]["document"]["name"].get<std::string>());
    ASSERT_EQ("butterfly", search_res["hits"][2]["document"]["name"].get<std::string>());
}


TEST_F(CollectionVectorTest, EmbedFromOptionalNullField) {
    nlohmann::json schema = R"({
                "name": "objects",
                "fields": [
                {"name": "text", "type": "string", "optional": true},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["text"], "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);

    ASSERT_TRUE(op.ok());
    auto coll = op.get();

    nlohmann::json doc = R"({
    })"_json;

    auto add_op = coll->add(doc.dump());

    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("No valid fields found to create embedding for `embedding`, please provide at least one valid field or make the embedding field optional.", add_op.error());

    doc["text"] = "butter";
    add_op = coll->add(doc.dump());
    ASSERT_TRUE(add_op.ok());
    // drop the embedding field and reindex

    nlohmann::json alter_schema = R"({
        "fields": [
        {"name": "embedding", "drop": true},
        {"name": "embedding", "type":"float[]", "embed":{"from": ["text"], "model_config": {"model_name": "ts/e5-small"}}, "optional": true}
        ]
    })"_json;

    auto update_op = coll->alter(alter_schema);
    ASSERT_TRUE(update_op.ok());


    doc = R"({
    })"_json;
    add_op = coll->add(doc.dump());

    ASSERT_TRUE(add_op.ok());
}

TEST_F(CollectionVectorTest, HideCredential) {
    auto schema_json =
            R"({
            "name": "Products",
            "fields": [
                {"name": "product_name", "type": "string", "infix": true},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["product_name"],
                    "model_config": {
                        "model_name": "ts/e5-small",
                        "api_key": "ax-abcdef12345",
                        "access_token": "ax-abcdef12345",
                        "refresh_token": "ax-abcdef12345",
                        "client_id": "ax-abcdef12345",
                        "client_secret": "ax-abcdef12345",
                        "project_id": "ax-abcdef12345"
                    }}}
            ]
        })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();
    auto coll_summary = coll1->get_summary_json();

    ASSERT_EQ("ax-ab*********", coll_summary["fields"][1]["embed"]["model_config"]["api_key"].get<std::string>());
    ASSERT_EQ("ax-ab*********", coll_summary["fields"][1]["embed"]["model_config"]["access_token"].get<std::string>());
    ASSERT_EQ("ax-ab*********", coll_summary["fields"][1]["embed"]["model_config"]["refresh_token"].get<std::string>());
    ASSERT_EQ("ax-ab*********", coll_summary["fields"][1]["embed"]["model_config"]["client_id"].get<std::string>());
    ASSERT_EQ("ax-ab*********", coll_summary["fields"][1]["embed"]["model_config"]["client_secret"].get<std::string>());
    ASSERT_EQ("ax-ab*********", coll_summary["fields"][1]["embed"]["model_config"]["project_id"].get<std::string>());

    // small api key

    schema_json =
            R"({
            "name": "Products2",
            "fields": [
                {"name": "product_name", "type": "string", "infix": true},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["product_name"],
                    "model_config": {
                        "model_name": "ts/e5-small",
                        "api_key": "ax1",
                        "access_token": "ax1",
                        "refresh_token": "ax1",
                        "client_id": "ax1",
                        "client_secret": "ax1",
                        "project_id": "ax1"
                    }}}
            ]
        })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll2 = collection_create_op.get();
    coll_summary = coll2->get_summary_json();

    ASSERT_EQ("***********", coll_summary["fields"][1]["embed"]["model_config"]["api_key"].get<std::string>());
    ASSERT_EQ("***********", coll_summary["fields"][1]["embed"]["model_config"]["access_token"].get<std::string>());
    ASSERT_EQ("***********", coll_summary["fields"][1]["embed"]["model_config"]["refresh_token"].get<std::string>());
    ASSERT_EQ("***********", coll_summary["fields"][1]["embed"]["model_config"]["client_id"].get<std::string>());
    ASSERT_EQ("***********", coll_summary["fields"][1]["embed"]["model_config"]["client_secret"].get<std::string>());
    ASSERT_EQ("***********", coll_summary["fields"][1]["embed"]["model_config"]["project_id"].get<std::string>());
}

TEST_F(CollectionVectorTest, UpdateOfFieldReferencedByEmbedding) {
    nlohmann::json schema = R"({
        "name": "objects",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"],
                "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    nlohmann::json object;
    object["id"] = "0";
    object["name"] = "butter";

    auto add_op = coll->add(object.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("butter", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    auto original_embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();

    nlohmann::json update_object;
    update_object["id"] = "0";
    update_object["name"] = "ghee";
    auto update_op = coll->add(update_object.dump(), EMPLACE);
    ASSERT_TRUE(update_op.ok());

    results = coll->search("ghee", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    auto updated_embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_NE(original_embedding, updated_embedding);

    // action = update
    update_object["name"] = "milk";
    update_op = coll->add(update_object.dump(), UPDATE);
    ASSERT_TRUE(update_op.ok());

    results = coll->search("milk", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    updated_embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_NE(original_embedding, updated_embedding);

    // action = upsert
    update_object["name"] = "cheese";
    update_op = coll->add(update_object.dump(), UPSERT);
    ASSERT_TRUE(update_op.ok());

    results = coll->search("cheese", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
    updated_embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_NE(original_embedding, updated_embedding);
}

TEST_F(CollectionVectorTest, UpdateOfFieldNotReferencedByEmbedding) {
    // test updates to a field that's not referred by an embedding field
    nlohmann::json schema = R"({
        "name": "objects",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "about", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    nlohmann::json object;
    object["id"] = "0";
    object["name"] = "butter";
    object["about"] = "about butter";

    auto add_op = coll->add(object.dump(), CREATE);
    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("butter", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    nlohmann::json update_object;
    update_object["id"] = "0";
    update_object["about"] = "something about butter";
    auto update_op = coll->add(update_object.dump(), EMPLACE);
    ASSERT_TRUE(update_op.ok());

    results = coll->search("butter", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // action = update
    update_object["about"] = "something about butter 2";
    update_op = coll->add(update_object.dump(), UPDATE);
    ASSERT_TRUE(update_op.ok());

    results = coll->search("butter", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());

    // action = upsert
    update_object["name"] = "butter";
    update_object["about"] = "something about butter 3";
    update_op = coll->add(update_object.dump(), UPSERT);
    ASSERT_TRUE(update_op.ok());

    results = coll->search("butter", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}).get();
    ASSERT_EQ(1, results["found"].get<size_t>());
}

TEST_F(CollectionVectorTest, FreshEmplaceWithOptionalEmbeddingReferencedField) {
    auto schema = R"({
        "name": "objects",
        "fields": [
            {"name": "name", "type": "string", "optional": true},
            {"name": "about", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    nlohmann::json object;
    object["id"] = "0";
    object["about"] = "about butter";

    auto add_op = coll->add(object.dump(), EMPLACE);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("No valid fields found to create embedding for `embedding`, please provide at least one valid field "
              "or make the embedding field optional.", add_op.error());
}

TEST_F(CollectionVectorTest, EmbeddingFieldWithIdFieldPrecedingInSchema) {
    auto schema = R"({
        "name": "objects",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    auto fs = coll->get_fields();
    ASSERT_EQ(2, fs.size());
    ASSERT_EQ(384, fs[1].num_dim);
}

TEST_F(CollectionVectorTest, SkipEmbeddingOpWhenValueExists) {
    nlohmann::json schema = R"({
        "name": "objects",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    nlohmann::json model_config = R"({
        "model_name": "ts/e5-small"
    })"_json;

    // will be roughly 0.1110895648598671,-0.11710234731435776,-0.5319093465805054, ...

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection* coll = op.get();

    // document with explicit embedding vector
    nlohmann::json doc;
    doc["name"] = "FOO";

    std::vector<float> vec;
    for(size_t i = 0; i < 384; i++) {
        vec.push_back(0.345);
    }

    doc["embedding"] = vec;

    auto add_op = coll->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    // get the vector back
    auto res = coll->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true},
                                      Index::DROP_TOKENS_THRESHOLD).get();

    // let's check the first few vectors
    auto stored_vec = res["hits"][0]["document"]["embedding"];
    ASSERT_NEAR(0.345, stored_vec[0], 0.01);
    ASSERT_NEAR(0.345, stored_vec[1], 0.01);
    ASSERT_NEAR(0.345, stored_vec[2], 0.01);
    ASSERT_NEAR(0.345, stored_vec[3], 0.01);
    ASSERT_NEAR(0.345, stored_vec[4], 0.01);

    // what happens when vector contains invalid value, like string
    doc["embedding"] = "foo"; //{0.11, 0.11};
    add_op = coll->add(doc.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `embedding` contains an invalid embedding.", add_op.error());

    // when dims don't match
    doc["embedding"] = {0.11, 0.11};
    add_op = coll->add(doc.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `embedding` contains an invalid embedding.", add_op.error());

    // invalid array value
    doc["embedding"].clear();
    for(size_t i = 0; i < 384; i++) {
        doc["embedding"].push_back(0.01);
    }
    doc["embedding"][5] = "foo";
    add_op = coll->add(doc.dump());
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `embedding` contains invalid float values.", add_op.error());
}

TEST_F(CollectionVectorTest, SemanticSearchReturnOnlyVectorDistance) {
    auto schema_json =
        R"({
            "name": "Products",
            "fields": [
                {"name": "product_name", "type": "string", "infix": true},
                {"name": "category", "type": "string"},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["product_name", "category"], "model_config": {"model_name": "ts/e5-small"}}}
            ]
        })"_json;

    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();

    auto add_op = coll1->add(R"({
        "product_name": "moisturizer",
        "category": "beauty"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("moisturizer", {"embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>()).get();
    
    ASSERT_EQ(1, results["hits"].size());

    // Return only vector distance
    ASSERT_EQ(0, results["hits"][0].count("text_match_info"));
    ASSERT_EQ(0, results["hits"][0].count("hybrid_search_info"));
    ASSERT_EQ(1, results["hits"][0].count("vector_distance"));
}

TEST_F(CollectionVectorTest, KeywordSearchReturnOnlyTextMatchInfo) {
    auto schema_json =
            R"({
            "name": "Products",
            "fields": [
                {"name": "product_name", "type": "string", "infix": true},
                {"name": "category", "type": "string"},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["product_name", "category"], "model_config": {"model_name": "ts/e5-small"}}}
            ]
        })"_json;


    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();
    auto add_op = coll1->add(R"({
        "product_name": "moisturizer",
        "category": "beauty"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("moisturizer", {"product_name"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>()).get();

    
    ASSERT_EQ(1, results["hits"].size());

    // Return only text match info
    ASSERT_EQ(0, results["hits"][0].count("vector_distance"));
    ASSERT_EQ(0, results["hits"][0].count("hybrid_search_info"));
    ASSERT_EQ(1, results["hits"][0].count("text_match_info"));
}

TEST_F(CollectionVectorTest, GroupByWithVectorSearch) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "group", "type": "string", "facet": true},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    std::vector<std::vector<float>> values = {
        {0.851758, 0.909671, 0.823431, 0.372063},
        {0.97826, 0.933157, 0.39557, 0.306488},
        {0.230606, 0.634397, 0.514009, 0.399594}
    };

    for (size_t i = 0; i < values.size(); i++) {
        nlohmann::json doc;
        doc["id"] = std::to_string(i);
        doc["title"] = std::to_string(i) + " title";
        doc["group"] = "0";
        doc["vec"] = values[i];
        ASSERT_TRUE(coll1->add(doc.dump()).ok());
    }

    auto res = coll1->search("title", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                     spp::sparse_hash_set<std::string>(),
                     spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                     "", 10, {}, {}, {"group"}, 1,
                     "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                     4, {off}, 32767, 32767, 2,
                     false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(1, res["grouped_hits"].size());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"][0].count("vector_distance"));

    res = coll1->search("*", {"title"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                        spp::sparse_hash_set<std::string>(),
                        spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                        "", 10, {}, {}, {"group"}, 1,
                        "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                        4, {off}, 32767, 32767, 2,
                        false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(1, res["grouped_hits"].size());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"].size());
    ASSERT_EQ(1, res["grouped_hits"][0]["hits"][0].count("vector_distance"));
}

TEST_F(CollectionVectorTest, HybridSearchReturnAllInfo) {
    auto schema_json =
            R"({
            "name": "Products",
            "fields": [
                {"name": "product_name", "type": "string", "infix": true},
                {"name": "category", "type": "string"},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["product_name", "category"], "model_config": {"model_name": "ts/e5-small"}}}
            ]
        })"_json;
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();

    auto add_op = coll1->add(R"({
        "product_name": "moisturizer",
        "category": "beauty"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());


    auto results = coll1->search("moisturizer", {"product_name", "embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>()).get();
    
    ASSERT_EQ(1, results["hits"].size());

    // Return all info
    ASSERT_EQ(1, results["hits"][0].count("vector_distance"));
    ASSERT_EQ(1, results["hits"][0].count("text_match_info"));
    ASSERT_EQ(1, results["hits"][0].count("hybrid_search_info"));
}


TEST_F(CollectionVectorTest, DISABLED_HybridSortingTest) {
    auto schema_json =
            R"({
            "name": "TEST",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
            ]
    })"_json;
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");
    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();

    auto add_op = coll1->add(R"({
        "name": "john doe"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll1->add(R"({
        "name": "john legend"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll1->add(R"({
        "name": "john krasinski"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll1->add(R"({
        "name": "john abraham"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    // first do keyword search
    auto results = coll1->search("john", {"name"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>()).get();
    
    ASSERT_EQ(4, results["hits"].size());


    // now do hybrid search with sort_by: _text_match:desc,_vector_distance:asc
    std::vector<sort_by> sort_by_list = {{"_text_match", "desc"}, {"_vector_distance", "asc"}};

    auto hybrid_results = coll1->search("john", {"name", "embedding"},
                                 "", {}, sort_by_list, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>()).get();
    
    // first 4 results should be same as keyword search
    ASSERT_EQ(results["hits"][0]["document"]["name"].get<std::string>(), hybrid_results["hits"][0]["document"]["name"].get<std::string>());
    ASSERT_EQ(results["hits"][1]["document"]["name"].get<std::string>(), hybrid_results["hits"][1]["document"]["name"].get<std::string>());
    ASSERT_EQ(results["hits"][2]["document"]["name"].get<std::string>(), hybrid_results["hits"][2]["document"]["name"].get<std::string>());
    ASSERT_EQ(results["hits"][3]["document"]["name"].get<std::string>(), hybrid_results["hits"][3]["document"]["name"].get<std::string>());
}

TEST_F(CollectionVectorTest, TestDifferentOpenAIApiKeys) {
    if (std::getenv("api_key_1") == nullptr || std::getenv("api_key_2") == nullptr) {
        LOG(INFO) << "Skipping test as api_key_1 or api_key_2 is not set";
        return;
    }

    auto api_key1 = std::string(std::getenv("api_key_1"));
    auto api_key2 = std::string(std::getenv("api_key_2"));

    auto embedder_map = TextEmbedderManager::get_instance()._get_text_embedders();

    ASSERT_EQ(embedder_map.find("openai/text-embedding-ada-002:" + api_key1), embedder_map.end());
    ASSERT_EQ(embedder_map.find("openai/text-embedding-ada-002:" + api_key2), embedder_map.end());
    ASSERT_EQ(embedder_map.find("openai/text-embedding-ada-002"), embedder_map.end());

    nlohmann::json model_config1 = R"({
                "model_name": "openai/text-embedding-ada-002"
            })"_json;
    
    nlohmann::json model_config2 = model_config1;

    model_config1["api_key"] = api_key1;
    model_config2["api_key"] = api_key2;

    size_t num_dim;
    TextEmbedderManager::get_instance().validate_and_init_remote_model(model_config1, num_dim);
    TextEmbedderManager::get_instance().validate_and_init_remote_model(model_config2, num_dim);

    embedder_map = TextEmbedderManager::get_instance()._get_text_embedders();

    ASSERT_NE(embedder_map.find("openai/text-embedding-ada-002:" + api_key1), embedder_map.end());
    ASSERT_NE(embedder_map.find("openai/text-embedding-ada-002:" + api_key2), embedder_map.end());
    ASSERT_EQ(embedder_map.find("openai/text-embedding-ada-002"), embedder_map.end());
}


TEST_F(CollectionVectorTest, TestMultilingualE5) {
    auto schema_json =
            R"({
            "name": "TEST",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/multilingual-e5-small"}}}
            ]
    })"_json;

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());
    auto coll1 = collection_create_op.get();

    auto add_op = coll1->add(R"({
        "name": "john doe"
    })"_json.dump());

    auto hybrid_results = coll1->search("john", {"name", "embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>());
    
    ASSERT_TRUE(hybrid_results.ok());

    auto semantic_results = coll1->search("john", {"embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>());
    
    ASSERT_TRUE(semantic_results.ok());
}