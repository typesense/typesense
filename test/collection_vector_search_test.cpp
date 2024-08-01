#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include "collection.h"
#include <cstdlib>
#include <ctime>
#include "conversation_manager.h"
#include "conversation_model_manager.h"
#include "index.h"
#include "core_api.h"
#include "vq_model_manager.h"
#include "conversation_model.h"

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

        ConversationModelManager::init(store);
        nlohmann::json schema_json = R"({
            "name": "conversation_store",
            "fields": [
                {
                    "name": "conversation_id",
                    "type": "string",
                    "facet": true
                },
                {
                    "name": "role",
                    "type": "string"
                },
                {
                    "name": "message",
                    "type": "string"
                },
                {
                    "name": "timestamp",
                    "type": "int32",
                    "sort": true
                }
            ]
        })"_json;

        collectionManager.create_collection(schema_json);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        EmbedderManager::get_instance().delete_all_text_embedders();
        delete store;
    }
};

TEST_F(CollectionVectorTest, BasicVectorQuerying) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32", "facet": true},
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

    // when id does not match filter, don't return k+1 hits
    results = coll1->search("*", {}, "id:!=1", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([], id: 1, k:1)").get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    ASSERT_EQ(1, results["hits"].size());

    // `k` value should overrides per_page
    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], k: 1)").get();

    ASSERT_EQ(1, results["hits"].size());

    results = coll1->search("*", {}, "", {"points"}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], k: 1)",
                            true, 0, max_score, 100,
                            0, 0, "top_values").get();

    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(1, results["facet_counts"].size());
    ASSERT_EQ(1, results["facet_counts"][0]["counts"].size());
    ASSERT_EQ("1", results["facet_counts"][0]["counts"][0]["value"]);

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

TEST_F(CollectionVectorTest, VectorQueryByIDWithZeroValuedFloat) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "points", "type": "int32"},
            {"name": "vec", "type": "float[]", "num_dim": 3}
        ]
    })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    auto coll_summary = coll1->get_summary_json();
    ASSERT_EQ("cosine", coll_summary["fields"][2]["vec_dist"].get<std::string>());

    nlohmann::json doc = R"(
        {
            "title": "Title 1",
            "points": 100,
            "vec": [0, 0, 0]
        }
    )"_json;

    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    auto res_op = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                           spp::sparse_hash_set<std::string>(),
                           spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                           "", 10, {}, {}, {}, 0,
                           "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                           4, {off}, 32767, 32767, 2,
                           false, true, "vec:([], id: 0)");

    ASSERT_TRUE(res_op.ok());
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

TEST_F(CollectionVectorTest, VectorChangedUpsert) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "points", "type": "int32"},
                {"name": "vec", "type": "float[]", "num_dim": 2}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;
    doc["vec"] = {0.15, 0.25};

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.44, 0.44])").get();

    ASSERT_FLOAT_EQ(0.029857516288757324, results["hits"][0]["vector_distance"].get<float>());

    // upsert changed doc

    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;
    doc["vec"] = {0.75, 0.95};

    add_op = coll1->add(doc.dump(), index_operation_t::UPSERT);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.44, 0.44])").get();

    ASSERT_FLOAT_EQ(0.006849408149719238, results["hits"][0]["vector_distance"].get<float>());

    // put old doc back using update
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;
    doc["vec"] = {0.15, 0.25};

    add_op = coll1->add(doc.dump(), index_operation_t::UPDATE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.44, 0.44])").get();

    ASSERT_FLOAT_EQ(0.029857516288757324, results["hits"][0]["vector_distance"].get<float>());

    // revert using emplace

    doc["id"] = "0";
    doc["title"] = "Title";
    doc["points"] = 100;
    doc["vec"] = {0.75, 0.95};

    add_op = coll1->add(doc.dump(), index_operation_t::EMPLACE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.44, 0.44])").get();

    ASSERT_FLOAT_EQ(0.006849408149719238, results["hits"][0]["vector_distance"].get<float>());
}

TEST_F(CollectionVectorTest, VectorManyUpserts) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "points", "type": "int32"},
                {"name": "vec", "type": "float[]", "num_dim": 3}
            ]
        })"_json;

    Collection* coll1 = collectionManager.create_collection(schema).get();

    size_t d = 3;
    size_t n = 50;

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    std::vector<std::string> import_records;

    // first insert n docs
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
        import_records.push_back(doc.dump());
    }

    nlohmann::json document;
    nlohmann::json import_response = coll1->add_many(import_records, document);

    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(n, import_response["num_imported"].get<int>());
    import_records.clear();

    size_t num_new_records = 0;

    // upsert mix of old + new docs50
    for (size_t i = 0; i < n; i++) {
        nlohmann::json doc;
        auto id = i;
        if(i % 2 != 0) {
            id = (i + 1000);
            num_new_records++;
        }

        doc["id"] = std::to_string(id);
        doc["title"] = std::to_string(id) + " title";
        doc["points"] = id;

        std::vector<float> values;
        for (size_t j = 0; j < d; j++) {
            values.push_back(distrib(rng) + 0.01);
        }
        doc["vec"] = values;
        import_records.push_back(doc.dump());
    }

    import_response = coll1->add_many(import_records, document, UPSERT);
    ASSERT_TRUE(import_response["success"].get<bool>());
    ASSERT_EQ(n, import_response["num_imported"].get<int>());
    import_records.clear();

    /*for(size_t i = 0; i < 100; i++) {
        auto results = coll1->search("*", {}, "", {}, {}, {0}, 200, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                     spp::sparse_hash_set<std::string>(),
                                     spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                     "", 10, {}, {}, {}, 0,
                                     "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                     4, {off}, 32767, 32767, 2,
                                     false, true, "vec:([0.12, 0.44, 0.55])").get();

        if(results["found"].get<size_t>() != n+num_new_records) {
            LOG(INFO) << results["found"].get<size_t>();
        }
    }*/

    //LOG(INFO) << "Expected: " << n + num_new_records;
    //ASSERT_EQ(n + num_new_records, results["found"].get<size_t>());
    //ASSERT_EQ(n + num_new_records, results["hits"].size());
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
    results = coll1->search("*", {}, "points:<10", {}, {}, {0}, 3, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488], flat_search_cutoff: 1000)").get();

    ASSERT_EQ(10, results["found"].get<size_t>());
    ASSERT_EQ(3, results["hits"].size());
    ASSERT_FLOAT_EQ(3.409385e-05, results["hits"][0]["vector_distance"].get<float>());
    ASSERT_EQ("1", results["hits"][0]["document"]["id"].get<std::string>());

    ASSERT_FLOAT_EQ(0.016780376, results["hits"][1]["vector_distance"].get<float>());
    ASSERT_EQ("5", results["hits"][1]["document"]["id"].get<std::string>());

    results = coll1->search("*", {}, "points:<10", {}, {}, {0}, 3, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "vec:([], id: 3, flat_search_cutoff: 1000)").get();

    ASSERT_EQ(3, results["hits"].size());

    LOG(INFO) << results["hits"][0];
    LOG(INFO) << results["hits"][1];

    ASSERT_EQ("9", results["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_FLOAT_EQ(0.050603985, results["hits"][0]["vector_distance"].get<float>());

    ASSERT_EQ("5", results["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_FLOAT_EQ(0.100155532, results["hits"][1]["vector_distance"].get<float>());

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

    size_t num_docs = 10;

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

    ASSERT_EQ(16, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(10, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(0, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

    // now delete these docs

    for (size_t i = 0; i < num_docs; i++) {
        ASSERT_TRUE(coll1->remove(std::to_string(i)).ok());
    }

    ASSERT_EQ(16, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(10, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(10, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

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

    ASSERT_EQ(16, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(10, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(0, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

    // delete those docs again and ensure that while reindexing till 1024 live docs, max count is not changed
    for (size_t i = 0; i < num_docs; i++) {
        ASSERT_TRUE(coll1->remove(std::to_string(i + num_docs)).ok());
    }

    ASSERT_EQ(16, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
    ASSERT_EQ(10, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getCurrentElementCount());
    ASSERT_EQ(10, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getDeletedCount());

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

    ASSERT_EQ(1271, coll1->_get_index()->_get_vector_index().at("vec")->vecdex->getMaxElements());
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
    ASSERT_EQ("Field `vec` must have 4 dimensions.",
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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

TEST_F(CollectionVectorTest, EmbeddOptionalFieldNullValueUpsert) {
    nlohmann::json schema = R"({
                "name": "coll1",
                "fields": [
                    {"name": "title", "type": "string"},
                    {"name": "desc", "type": "string", "optional": true},
                    {"name": "tags", "type": "string[]", "optional": true},
                    {"name": "embedding", "type":"float[]", "embed":{"from": ["title", "desc", "tags"],
                        "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "Title";
    doc["desc"] = nullptr;
    doc["tags"] = {"foo", "bar"};

    auto add_op = coll1->add(doc.dump(), UPSERT);
    ASSERT_TRUE(add_op.ok());

    auto results = coll1->search("title", {"embedding"}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    auto embedding = results["hits"][0]["document"]["embedding"].get<std::vector<float>>();
    ASSERT_EQ(384, embedding.size());

    // upsert doc
    add_op = coll1->add(doc.dump(), index_operation_t::UPSERT);
    ASSERT_TRUE(add_op.ok());

    // try with null values in array: not allowed
    doc["tags"] = {"bar", nullptr};
    add_op = coll1->add(doc.dump(), index_operation_t::UPSERT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Field `tags` must be an array of string.", add_op.error());
}

TEST_F(CollectionVectorTest, SortKeywordSearchWithAutoEmbedVector) {
    nlohmann::json schema = R"({
                "name": "coll1",
                "fields": [
                    {"name": "title", "type": "string"},
                    {"name": "points", "type": "int32"},
                    {"name": "embedding", "type":"float[]", "embed":{"from": ["title"],
                        "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["title"] = "The Lord of the Rings";
    doc["points"] = 100;

    auto add_op = coll1->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    std::vector<sort_by> sort_by_list = {sort_by("_vector_query(embedding:([]))", "asc")};

    auto results = coll1->search("lord", {"title"}, "", {}, sort_by_list, {0}, 10, 1, FREQUENCY, {true},
                                 Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(1, results["found"].get<size_t>());
    auto actual_dist = results["hits"][0]["vector_distance"].get<float>();
    ASSERT_LE(0.173, actual_dist);
    ASSERT_GE(0.175, actual_dist);
}

TEST_F(CollectionVectorTest, HybridSearchWithExplicitVector) {
    nlohmann::json schema = R"({
                            "name": "objects",
                            "fields": [
                            {"name": "name", "type": "string"},
                            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
                            ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    auto query_embedding = EmbedderManager::get_instance().get_text_embedder(model_config).get()->Embed("butter");
    
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
    std::string vec_query = "embedding:([], distance_threshold: 0.13)";

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

    // allow wildcard with empty vector (for convenience)
    search_res_op = coll->search("*", {"embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
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

    ASSERT_TRUE(search_res["hits"][0].count("vector_distance") == 0);
    ASSERT_TRUE(search_res["hits"][1].count("vector_distance") == 0);
    ASSERT_TRUE(search_res["hits"][2].count("vector_distance") == 0);
}

TEST_F(CollectionVectorTest, HybridSearchOnlyVectorMatches) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string", "facet": true},
            {"name": "vec", "type": "float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
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

TEST_F(CollectionVectorTest, HybridSearchWithEvalSort) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string", "facet": true},
            {"name": "category", "type": "string", "facet": true},
            {"name": "vec", "type": "float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["name"] = "Apple Fruit";
    doc["category"] = "Fresh";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["name"] = "Apple";
    doc["category"] = "Phone";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["name"] = "Apple Pie";
    doc["category"] = "Notebook";
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    std::vector<sort_by> sort_fields;
    CollectionManager::parse_sort_by_str("_eval([(category:Fresh):3,(category:Notebook):2,(category:Phone):1]):desc", sort_fields);

    auto results_op = coll1->search("apple", {"name", "vec"}, "", {"name"}, sort_fields, {0}, 20, 1, FREQUENCY, {true},
                                    Index::DROP_TOKENS_THRESHOLD,
                                    spp::sparse_hash_set<std::string>(),
                                    spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                    "", 10, {}, {}, {}, 0,
                                    "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                    fallback,
                                    4, {off}, 32767, 32767, 2);
    ASSERT_EQ(true, results_op.ok());
    ASSERT_EQ(3, results_op.get()["found"].get<size_t>());
    ASSERT_EQ(3, results_op.get()["hits"].size());

    ASSERT_EQ("0", results_op.get()["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results_op.get()["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results_op.get()["hits"][2]["document"]["id"].get<std::string>());
}

TEST_F(CollectionVectorTest, VectorSearchWithEvalSort) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
            {"name": "name", "type": "string", "facet": true},
            {"name": "category", "type": "string", "facet": true},
            {"name": "vec", "type": "float[]", "num_dim": 4}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
    Collection* coll1 = collectionManager.create_collection(schema).get();

    nlohmann::json doc;
    doc["id"] = "0";
    doc["name"] = "Apple Fruit";
    doc["category"] = "Fresh";
    doc["vec"] = {0.1, 0.2, 0.3, 0.4};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["name"] = "Apple";
    doc["category"] = "Phone";
    doc["vec"] = {0.2, 0.3, 0.1, 0.1};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["name"] = "Apple Pie";
    doc["category"] = "Notebook";
    doc["vec"] = {0.1, 0.3, 0.2, 0.4};
    ASSERT_TRUE(coll1->add(doc.dump()).ok());

    std::vector<sort_by> sort_fields;
    CollectionManager::parse_sort_by_str("_eval([(category:Fresh):3,(category:Notebook):2,(category:Phone):1]):desc", sort_fields);

    auto results_op = coll1->search("*", {"vec"}, "", {"name"}, sort_fields, {0}, 20, 1, FREQUENCY, {true},
                                    Index::DROP_TOKENS_THRESHOLD,
                                    spp::sparse_hash_set<std::string>(),
                                    spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                    "", 10, {}, {}, {}, 0,
                                    "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                    fallback,
                                    4, {off}, 32767, 32767, 2,
                                    false, true, "vec:([0.1, 0.4, 0.2, 0.3])");
    ASSERT_EQ(true, results_op.ok());
    ASSERT_EQ(3, results_op.get()["found"].get<size_t>());
    ASSERT_EQ(3, results_op.get()["hits"].size());

    ASSERT_EQ("0", results_op.get()["hits"][0]["document"]["id"].get<std::string>());
    ASSERT_EQ("2", results_op.get()["hits"][1]["document"]["id"].get<std::string>());
    ASSERT_EQ("1", results_op.get()["hits"][2]["document"]["id"].get<std::string>());
}

TEST_F(CollectionVectorTest, EmbedFromOptionalNullField) {
    nlohmann::json schema = R"({
                "name": "objects",
                "fields": [
                {"name": "text", "type": "string", "optional": true},
                {"name": "embedding", "type":"float[]", "embed":{"from": ["text"], "model_config": {"model_name": "ts/e5-small"}}}
                ]
            })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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


    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
                     "", 10, {}, {}, {"group"}, 3,
                     "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                     4, {off}, 32767, 32767, 2,
                     false, true, "vec:([0.96826, 0.94, 0.39557, 0.306488])").get();

    ASSERT_EQ(1, res["grouped_hits"].size());
    ASSERT_EQ(3, res["grouped_hits"][0]["hits"].size());
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
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");
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

    auto embedder_map = EmbedderManager::get_instance()._get_text_embedders();

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
    EmbedderManager::get_instance().validate_and_init_remote_model(model_config1, num_dim);
    EmbedderManager::get_instance().validate_and_init_remote_model(model_config2, num_dim);

    embedder_map = EmbedderManager::get_instance()._get_text_embedders();

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

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

TEST_F(CollectionVectorTest, TestTwoEmbeddingFieldsSamePrefix) {
    nlohmann::json schema = R"({
                            "name": "docs",
                            "fields": [
                                {
                                "name": "title",
                                "type": "string"
                                },
                                {
                                "name": "embedding",
                                "type": "float[]",
                                "embed": {
                                    "from": [
                                    "title"
                                    ],
                                    "model_config": {
                                    "model_name": "ts/e5-small"
                                    }
                                }
                                },
                                {
                                "name": "embedding_en",
                                "type": "float[]",
                                "embed": {
                                    "from": [
                                    "title"
                                    ],
                                    "model_config": {
                                    "model_name": "ts/e5-small"
                                    }
                                }
                                }
                            ]
                            })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll1 = collection_create_op.get();

    auto add_op = coll1->add(R"({
        "title": "john doe"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto semantic_results = coll1->search("john", {"embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>());

    ASSERT_TRUE(semantic_results.ok());
}

TEST_F(CollectionVectorTest, TestOneEmbeddingOneKeywordFieldsHaveSamePrefix) {
    nlohmann::json schema = R"({
                        "name": "test",
                        "fields": [
                            {
                                "name": "title",
                                "type": "string"
                            },
                            {
                            "name": "title_vec",
                            "type": "float[]",
                            "embed": {
                                "from": [
                                    "title"
                                ],
                                "model_config": {
                                    "model_name": "ts/e5-small"
                                }
                            }
                            }
                        ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll1 = collection_create_op.get();

    auto add_op = coll1->add(R"({
        "title": "john doe"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto keyword_results = coll1->search("john", {"title"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>());

    ASSERT_TRUE(keyword_results.ok());
}

TEST_F(CollectionVectorTest, HybridSearchOnlyKeyworMatchDoNotHaveVectorDistance) {
    nlohmann::json schema = R"({
                        "name": "test",
                        "fields": [
                            {
                                "name": "title",
                                "type": "string"
                            },
                            {
                            "name": "embedding",
                            "type": "float[]",
                            "embed": {
                                "from": [
                                    "title"
                                ],
                                "model_config": {
                                    "model_name": "ts/e5-small"
                                }
                            }
                            }
                        ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll1 = collection_create_op.get();

    auto add_op = coll1->add(R"({
        "title": "john doe"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    // hybrid search with empty vector (to pass distance threshold param)
    std::string vec_query = "embedding:([], distance_threshold: 0.05)";

    auto hybrid_results = coll1->search("john", {"title", "embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, vec_query);

    ASSERT_TRUE(hybrid_results.ok());

    ASSERT_EQ(1, hybrid_results.get()["hits"].size());
    ASSERT_EQ(0, hybrid_results.get()["hits"][0].count("vector_distance"));
}

TEST_F(CollectionVectorTest, QueryByNotAutoEmbeddingVectorField) {
    nlohmann::json schema = R"({
                    "name": "test",
                    "fields": [
                        {
                            "name": "title",
                            "type": "string"
                        },
                        {
                        "name": "embedding",
                        "type": "float[]",
                        "num_dim": 384
                        }
                    ]
                    })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto search_res = coll->search("john", {"title", "embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "embedding:([0.96826, 0.94, 0.39557, 0.306488])");
    
    ASSERT_FALSE(search_res.ok());

    ASSERT_EQ("Vector field `embedding` is not an auto-embedding field, do not use `query_by` with it, use `vector_query` instead.", search_res.error());
}

TEST_F(CollectionVectorTest, TestUnloadingModelsOnCollectionDelete) {
    nlohmann::json actual_schema = R"({
                        "name": "test",
                        "fields": [
                            {
                                "name": "title",
                                "type": "string"
                            },
                            {
                            "name": "title_vec",
                            "type": "float[]",
                            "embed": {
                                "from": [
                                    "title"
                                ],
                                "model_config": {
                                    "model_name": "ts/e5-small"
                                }
                            }
                            }
                        ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto schema = actual_schema;
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();
 
    auto text_embedders = EmbedderManager::get_instance()._get_text_embedders();

    ASSERT_EQ(1, text_embedders.size());

    auto delete_op = collectionManager.drop_collection("test", true);

    ASSERT_TRUE(delete_op.ok());
    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());

    // create another collection
    schema = actual_schema;
    collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    coll = collection_create_op.get();

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    // create second collection
    schema = actual_schema;
    schema["name"] = "test2";
    collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll2 = collection_create_op.get();

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();

    ASSERT_EQ(1, text_embedders.size());

    delete_op = collectionManager.drop_collection("test", true);
    ASSERT_TRUE(delete_op.ok());

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    delete_op = collectionManager.drop_collection("test2", true);
    ASSERT_TRUE(delete_op.ok());

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());
}  

TEST_F(CollectionVectorTest, TestUnloadingModelsOnDrop) {
    nlohmann::json actual_schema = R"({
                        "name": "test",
                        "fields": [
                            {
                                "name": "title",
                                "type": "string"
                            },
                            {
                            "name": "title_vec",
                            "type": "float[]",
                            "embed": {
                                "from": [
                                    "title"
                                ],
                                "model_config": {
                                    "model_name": "ts/e5-small"
                                }
                            }
                            }
                        ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto schema = actual_schema;
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto text_embedders = EmbedderManager::get_instance()._get_text_embedders();

    ASSERT_EQ(1, text_embedders.size());

    nlohmann::json drop_schema = R"({
                        "fields": [
                            {
                                "name": "title_vec",
                                "drop": true
                            }
                        ]
                        })"_json;
    
    auto drop_op = coll->alter(drop_schema);
    ASSERT_TRUE(drop_op.ok());

    LOG(INFO) << "After alter";

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());

    // create another collection
    schema = actual_schema;
    schema["name"] = "test2";
    collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll2 = collection_create_op.get();

    nlohmann::json alter_schema = R"({
                        "fields": [
                            {
                                "name": "title_vec",
                                "type": "float[]",
                                "embed": {
                                    "from": [
                                        "title"
                                    ],
                                    "model_config": {
                                        "model_name": "ts/e5-small"
                                    }
                                }
                            }
                        ]
                        })"_json;

    auto alter_op = coll->alter(alter_schema);
    ASSERT_TRUE(alter_op.ok());

    LOG(INFO) << "After alter";

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    drop_op = coll2->alter(drop_schema);
    ASSERT_TRUE(drop_op.ok());

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    drop_op = coll->alter(drop_schema);
    ASSERT_TRUE(drop_op.ok());

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());
}

TEST_F(CollectionVectorTest, TestUnloadModelsCollectionHaveTwoEmbeddingField) {
        nlohmann::json actual_schema = R"({
                        "name": "test",
                        "fields": [
                            {
                                "name": "title",
                                "type": "string"
                            },
                            {
                                "name": "title_vec",
                                "type": "float[]",
                                "embed": {
                                    "from": [
                                        "title"
                                    ],
                                    "model_config": {
                                        "model_name": "ts/e5-small"
                                    }
                                }
                            },
                            {
                                "name": "title_vec2",
                                "type": "float[]",
                                "embed": {
                                    "from": [
                                        "title"
                                    ],
                                    "model_config": {
                                        "model_name": "ts/e5-small"
                                    }
                                }
                            }
                        ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto schema = actual_schema;
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();
    auto text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    nlohmann::json drop_schema = R"({
                        "fields": [
                            {
                                "name": "title_vec",
                                "drop": true
                            }
                        ]
                        })"_json;
    
    auto alter_op = coll->alter(drop_schema);
    ASSERT_TRUE(alter_op.ok());

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    drop_schema = R"({
                        "fields": [
                            {
                                "name": "title_vec2",
                                "drop": true
                            }
                        ]
                        })"_json;
    
    alter_op = coll->alter(drop_schema);
    ASSERT_TRUE(alter_op.ok());

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());

    // create another collection
    schema = actual_schema;
    schema["name"] = "test2";

    collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll2 = collection_create_op.get();

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    // drop collection
    auto drop_op = collectionManager.drop_collection("test2", true);

    ASSERT_TRUE(drop_op.ok());

    text_embedders = EmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());
}

TEST_F(CollectionVectorTest, TestHybridSearchAlphaParam) {
    nlohmann::json schema = R"({
                        "name": "test",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "embedding",
                                "type": "float[]",
                                "embed": {
                                    "from": [
                                        "name"
                                    ],
                                    "model_config": {
                                        "model_name": "ts/e5-small"
                                    }
                                }
                            }
                        ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "soccer"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "basketball"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "volleyball"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());


    // do hybrid search
    auto hybrid_results = coll->search("sports", {"name", "embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>()).get();
                                
    ASSERT_EQ(3, hybrid_results["hits"].size());

    // check scores
    ASSERT_FLOAT_EQ(0.3, hybrid_results["hits"][0]["hybrid_search_info"]["rank_fusion_score"].get<float>());
    ASSERT_FLOAT_EQ(0.15, hybrid_results["hits"][1]["hybrid_search_info"]["rank_fusion_score"].get<float>());
    ASSERT_FLOAT_EQ(0.10, hybrid_results["hits"][2]["hybrid_search_info"]["rank_fusion_score"].get<float>());

    // do hybrid search with alpha = 0.5
    hybrid_results = coll->search("sports", {"name", "embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "embedding:([], alpha:0.5)").get();
    ASSERT_EQ(3, hybrid_results["hits"].size());

    // check scores
    ASSERT_FLOAT_EQ(0.5, hybrid_results["hits"][0]["hybrid_search_info"]["rank_fusion_score"].get<float>());
    ASSERT_FLOAT_EQ(0.25, hybrid_results["hits"][1]["hybrid_search_info"]["rank_fusion_score"].get<float>());
    ASSERT_FLOAT_EQ(0.16666667, hybrid_results["hits"][2]["hybrid_search_info"]["rank_fusion_score"].get<float>());
}   

TEST_F(CollectionVectorTest, TestHybridSearchInvalidAlpha) {
        nlohmann::json schema = R"({
                        "name": "test",
                        "fields": [
                            {
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "name": "embedding",
                                "type": "float[]",
                                "embed": {
                                    "from": [
                                        "name"
                                    ],
                                    "model_config": {
                                        "model_name": "ts/e5-small"
                                    }
                                }
                            }
                        ]
                        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();


    // do hybrid search with alpha = 1.5
    auto hybrid_results = coll->search("sports", {"name", "embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "embedding:([], alpha:1.5)");
    
    ASSERT_FALSE(hybrid_results.ok());
    ASSERT_EQ("Malformed vector query string: "
              "`alpha` parameter must be a float between 0.0-1.0.", hybrid_results.error());
    
    // do hybrid search with alpha = -0.5
    hybrid_results = coll->search("sports", {"name", "embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "embedding:([], alpha:-0.5)");
    
    ASSERT_FALSE(hybrid_results.ok());
    ASSERT_EQ("Malformed vector query string: "
              "`alpha` parameter must be a float between 0.0-1.0.", hybrid_results.error());
    
    // do hybrid search with alpha as string
    hybrid_results = coll->search("sports", {"name", "embedding"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "embedding:([], alpha:\"0.5\")");
    
    ASSERT_FALSE(hybrid_results.ok());
    ASSERT_EQ("Malformed vector query string: "
              "`alpha` parameter must be a float between 0.0-1.0.", hybrid_results.error());
    
}

TEST_F(CollectionVectorTest, TestSearchNonIndexedEmbeddingField) {
    nlohmann::json schema = R"({
                    "name": "test",
                    "fields": [
                        {
                            "name": "name",
                            "type": "string"
                        },
                        {
                            "name": "embedding",
                            "type": "float[]",
                            "index": false,
                            "optional": true,
                            "embed": {
                                "from": [
                                    "name"
                                ],
                                "model_config": {
                                    "model_name": "ts/e5-small"
                                }
                            }
                        }
                    ]
                    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "soccer"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto search_res = coll->search("soccer", {"name", "embedding"}, "", {}, {}, {0});
    ASSERT_FALSE(search_res.ok());

    ASSERT_EQ("Field `embedding` is marked as a non-indexed field in the schema.", search_res.error());
}

TEST_F(CollectionVectorTest, TestSearchNonIndexedVectorField) {
        nlohmann::json schema = R"({
                    "name": "test",
                    "fields": [
                        {
                            "name": "vec",
                            "type": "float[]",
                            "index": false,
                            "optional": true,
                            "num_dim": 2
                        }
                    ]
                    })"_json;
    
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "vec": [0.1, 0.2]
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto search_result = coll->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "vec:([0.96826, 0.94])");
    
    ASSERT_FALSE(search_result.ok());
    ASSERT_EQ("Field `vec` is marked as a non-indexed field in the schema.", search_result.error());
}

TEST_F(CollectionVectorTest, TestSemanticSearchAfterUpdate) {
    nlohmann::json schema = R"({
                "name": "test",
                "fields": [
                    {
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "embedding",
                        "type": "float[]",
                        "embed": {
                            "from": [
                                "name"
                            ],
                            "model_config": {
                                "model_name": "ts/e5-small"
                            }
                        }
                    }
                ]
                })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "soccer",
        "id": "0"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "basketball",
        "id": "1"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "typesense",
        "id": "2"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "potato",
        "id": "3"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto result = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "embedding:([], id:0, k:1)");
    
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.get()["hits"].size());
    ASSERT_EQ("basketball", result.get()["hits"][0]["document"]["name"]);

    auto update_op = coll->add(R"({
        "name": "onion",
        "id": "0"
    })"_json.dump(), index_operation_t::UPDATE, "0");

    ASSERT_TRUE(update_op.ok());

    result = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                 fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "embedding:([], id:0, k:1)");

    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.get()["hits"].size());
    ASSERT_EQ("potato", result.get()["hits"][0]["document"]["name"]);   
}

TEST_F(CollectionVectorTest, TestQAConversation) {
    auto schema_json =
        R"({
        "name": "Products",
        "fields": [
            {"name": "product_name", "type": "string", "infix": true},
            {"name": "category", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["product_name", "category"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    if (std::getenv("api_key") == nullptr) {
        LOG(INFO) << "Skipping test as api_key is not set.";
        return;
    }

    auto api_key = std::string(std::getenv("api_key"));

    auto conversation_model_config = R"({
        "model_name": "openai/gpt-3.5-turbo",
        "max_bytes: 1000,
        "history_collection": "conversation_store",
    })"_json;

    conversation_model_config["api_key"] = api_key;

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto model_add_op = ConversationModelManager::add_model(conversation_model_config);

    ASSERT_TRUE(model_add_op.ok());

    auto add_op = coll->add(R"({
        "product_name": "moisturizer",
        "category": "beauty"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "product_name": "shampoo",
        "category": "beauty"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "product_name": "shirt",
        "category": "clothing"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "product_name": "pants",
        "category": "clothing"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());
    
    auto results_op = coll->search("how many products are there for clothing category?", {"embedding"},
                                 "", {}, {}, {2}, 10,
                                 1, FREQUENCY, {true},
                                 0, spp::sparse_hash_set<std::string>(), {},
                                 10, "", 30, 4, "", 1, "", "", {}, 3, "<mark>", "</mark>", {}, 4294967295UL, true, false,
                                 true, "", false, 6000000UL, 4, 7, fallback, 4, {off}, 32767UL, 32767UL, 2, 2, false, "",
                                 true, 0, max_score, 100, 0, 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left", true, true, true, model_add_op.get()["id"]);
    
    ASSERT_TRUE(results_op.ok());

    auto results = results_op.get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_TRUE(results.contains("conversation"));
    ASSERT_TRUE(results["conversation"].is_object());
    ASSERT_EQ("how many products are there for clothing category?", results["conversation"]["query"]);
    std::string conversation_id =  results["conversation"]["conversation_id"];

    
    // test getting conversation history
    auto history_op = ConversationManager::get_instance().get_conversation(conversation_id);

    ASSERT_TRUE(history_op.ok());

    auto history = history_op.get();

    ASSERT_TRUE(history.is_object());
    ASSERT_TRUE(history.contains("conversation"));
    ASSERT_TRUE(history["conversation"].is_array());

    ASSERT_EQ("how many products are there for clothing category?", history["conversation"][0]["user"]);
}

TEST_F(CollectionVectorTest, TestImageEmbeddingWithWrongModel) {
    auto schema_json =
        R"({
        "name": "Images",
        "fields": [
            {"name": "image", "type": "image"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["image"], "model_config": {"model_name": "ts/e5-small"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "image": "test"
    })"_json.dump());

    ASSERT_FALSE(add_op.ok());
    ASSERT_EQ("Could not find image embedder for model: ts/e5-small", add_op.error());
}

TEST_F(CollectionVectorTest, TestImageEmbedding) {
    auto schema_json =
        R"({
        "name": "Images",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "image", "type": "image", "store": false},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["image"], "model_config": {"model_name": "ts/clip-vit-b-p32"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();


    auto add_op = coll->add(R"({
        "name": "dog",
        "image": "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"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    LOG(INFO) << "Searching for image";

    add_op = coll->add(R"({
        "name": "teddy bear",
        "image": "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"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    LOG(INFO) << "Waiting for indexing to complete";

    auto results = coll->search("dog", {"embedding"},
                                    "", {}, {}, {2}, 10,
                                    1, FREQUENCY, {true},
                                    0, spp::sparse_hash_set<std::string>()).get();
    
    ASSERT_EQ(results["hits"].size(), 2);
    ASSERT_EQ(results["hits"][0]["document"]["id"], "0");
    ASSERT_EQ(results["hits"][1]["document"]["id"], "1");


    auto results2 = coll->search("teddy bear", {"embedding"},
                                    "", {}, {}, {2}, 10,
                                    1, FREQUENCY, {false},
                                    0, spp::sparse_hash_set<std::string>()).get();
    
    ASSERT_EQ(results2["hits"].size(), 2);
    ASSERT_EQ(results2["hits"][0]["document"]["id"], "1");
    ASSERT_EQ(results2["hits"][1]["document"]["id"], "0");
}

TEST_F(CollectionVectorTest, TestHybridSearchHiddenHits) {
    nlohmann::json schema = R"({
                "name": "test",
                "fields": [
                    {
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "embedding",
                        "type": "float[]",
                        "embed": {
                            "from": [
                                "name"
                            ],
                            "model_config": {
                                "model_name": "ts/e5-small"
                            }
                        }
                    }
                ]
                })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
            "name": "soccer",
            "id": "0"
        })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
            "name": "guitar",
            "id": "1"
        })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
            "name": "typesense",
            "id": "2"
        })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
            "name": "potato",
            "id": "3"
        })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("sports", {"name", "embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_STREQ("0", results["hits"][0]["document"]["id"].get<std::string>().c_str());


    // do hybrid search with hidden_hits
    auto hybrid_results = coll->search("sports", {"name", "embedding"},
                                       "", {}, {}, {2}, 10,
                                       1, FREQUENCY, {true},
                                       0, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "", 1, "", "0").get();

    ASSERT_EQ(3, hybrid_results["hits"].size());
    ASSERT_FALSE(hybrid_results["hits"][0]["document"]["id"] == 0);
}

TEST_F(CollectionVectorTest, TryAddingMultipleImageFieldToEmbedFrom) {
    auto schema_json =
        R"({
        "name": "Images",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "image", "type": "image", "store": false},
            {"name": "image2", "type": "image", "store": false},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["image", "image2"], "model_config": {"model_name": "ts/clip-vit-b-p32"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
}

TEST_F(CollectionVectorTest, TestLongTextForImageEmbedding) {
    auto schema_json = R"({
            "name": "images2",
            "fields": [
                {
                    "name": "name",
                    "type": "string"
                },
                {
                    "name": "embedding",
                    "type": "float[]",
                    "embed": {
                        "from": [
                            "name"
                        ],
                        "model_config": {
                            "model_name": "ts/clip-vit-b-p32"
                        }
                    }
                }
            ]
        })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll = collection_create_op.get();

    const std::string long_text = "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?";

    nlohmann::json doc;
    doc["name"] = long_text;

    auto add_op = coll->add(doc.dump());
    ASSERT_TRUE(add_op.ok());
}


TEST_F(CollectionVectorTest, TestMultipleFieldsForImageEmbedding) {
    auto schema_json = R"({
            "name": "images",
            "fields": [
                {
                "name": "name",
                "type": "string"
                },
                {
                "name": "image",
                "type": "image",
                "store": false
                },
                {
                "name": "embedding",
                "type": "float[]",
                "embed": {
                    "from": [
                    "image",
                    "name"
                    ],
                    "model_config": {
                    "model_name": "ts/clip-vit-b-p32"
                    }
                }
                }
            ]
            })"_json;
    
    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());
    auto coll = collection_create_op.get();

    const std::string kitten_image = "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";

    nlohmann::json doc_json;
    doc_json["image"] = kitten_image;   

    doc_json["name"] = "istanbul cat";
    auto add_op = coll->add(doc_json.dump());
    ASSERT_TRUE(add_op.ok());

    doc_json["name"] = "british shorthair";
    add_op = coll->add(doc_json.dump());

    doc_json["name"] = "persian cat";
    add_op = coll->add(doc_json.dump());


    auto results = coll->search("istanbul", {"embedding"},
                                       "", {}, {}, {2}, 10,
                                       1, FREQUENCY, {true},
                                       0, spp::sparse_hash_set<std::string>(), spp::sparse_hash_set<std::string>(), 10).get();
    ASSERT_EQ(results["hits"].size(), 3);
    ASSERT_EQ(results["hits"][0]["document"]["name"], "istanbul cat");
}

TEST_F(CollectionVectorTest, TestInvalidImage) {
    auto schema_json =
        R"({
        "name": "Images",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "image", "type": "image", "store": false},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["image"], "model_config": {"model_name": "ts/clip-vit-b-p32"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "teddy bear",
        "image": "invalid"
    })"_json.dump());

    ASSERT_FALSE(add_op.ok());

    ASSERT_EQ(add_op.error(), "Error while processing image");

}


TEST_F(CollectionVectorTest, TestCLIPTokenizerUnicode) {
    auto schema_json =
        R"({
        "name": "Images",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "image", "type": "image", "store": false},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["image"], "model_config": {"model_name": "ts/clip-vit-b-p32"}}}
        ]
    })"_json;


    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    // test english
    auto results = coll->search("dog", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();
    
    // test chinese
    results = coll->search("狗", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();
    
    // test japanese
    results = coll->search("犬", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();

    // test korean
    results = coll->search("개", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();
    
    // test russian
    results = coll->search("собака", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();
    
    // test arabic
    results = coll->search("كلب", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();
    
    // test turkish
    results = coll->search("kö", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();

    results = coll->search("öğ", {"embedding"},
                                "", {}, {}, {2}, 10,
                                1, FREQUENCY, {true},
                                0, spp::sparse_hash_set<std::string>()).get();
    
}
 
TEST_F(CollectionVectorTest, Test0VectorDistance) {
    auto schema_json =
        R"({
        "name": "colors",
        "fields": [
            {"name": "rgb", "type":"float[]", "num_dim": 3}
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "rgb": [0.9, 0.9, 0.9]
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("*", {}, "", {}, {}, {0}, 10, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                 "", 10, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, fallback,
                                 4, {off}, 32767, 32767, 2,
                                 false, true, "rgb:([0.5, 0.5, 0.5])").get();
    
    ASSERT_EQ(results["hits"].size(), 1);
    ASSERT_EQ(results["hits"][0].count("vector_distance"), 1);
    ASSERT_EQ(results["hits"][0]["vector_distance"], 0);
}


TEST_F(CollectionVectorTest, TestEmbeddingValues) {
        auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Elskovsbarnet"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    std::vector<float> embeddings = add_op.get()["embedding"];

    std::vector<float> normalized_embeddings(embeddings.size());

    hnsw_index_t::normalize_vector(embeddings, normalized_embeddings);

    ASSERT_EQ(embeddings.size(), 384);

    std::vector<float> actual_values{-0.07409533113241196, -0.02963513322174549, -0.018120333552360535, 0.012058400548994541, -0.07219868153333664, -0.09295058250427246, 0.018390782177448273, 0.007814675569534302, 0.026419874280691147, 0.037965331226587296, 0.020393727347254753, -0.04090584069490433, 0.03194206580519676, 0.025205004960298538, 0.02059922367334366, 0.026202859356999397, 0.009739107452332973, 0.07967381179332733, -0.006712059490382671, -0.045936256647109985, -0.0280868299305439, -0.028282660990953445, 0.00617704214528203, -0.0756121575832367, -0.009177971631288528, -0.0016412553377449512, -0.040854115039110184, -0.007597113959491253, -0.03225032240152359, -0.015282290056347847, -0.013507066294550896, -0.11270778626203537, 0.12383124977350235, 0.09607065469026566, -0.106889508664608, 0.02146402932703495, 0.061281926929950714, -0.04245373234152794, -0.05668728053569794, 0.02623145468533039, 0.016187654808163643, 0.05603780969977379, 0.0119243822991848, -0.004412775859236717, 0.040246933698654175, 0.07487507909536362, -0.05067175254225731, 0.030055716633796692, 0.014153759926557541, -0.04411328583955765, -0.010018891654908657, -0.08593358099460602, 0.037568483501672745, -0.10012772679328918, 0.029019853100180626, 0.019645709544420242, -0.0639389306306839, 0.02652929536998272, 0.015299974009394646, 0.07286490499973297, 0.029529787600040436, -0.044351380318403244, -0.041604846715927124, 0.06385225802659988, -0.007908550091087818, -0.003856210969388485, -0.03855051472783089, -0.0023078585509210825, -0.04141264036297798, -0.05051504448056221, -0.018076501786708832, -0.017384130507707596, 0.024294942617416382, 0.12094006687402725, 0.01351782027631998, 0.08950492739677429, 0.027889391407370567, -0.03165547922253609, -0.017131352797150612, -0.022714827209711075, 0.048935145139694214, -0.012115311808884144, -0.0575471930205822, -0.019780246540904045, 0.052039679139852524, 0.00199871021322906, -0.010556189343333244, -0.0176922008395195, -0.01899656467139721, -0.005256693810224533, -0.06929342448711395, -0.01906348578631878, 0.10669232159852982, -0.0058551388792693615, 0.011760520748794079, 0.0066625443287193775, 0.0019288291223347187, -0.08495593070983887, 0.03902851417660713, 0.1967391073703766, 0.007772537413984537, -0.04112537205219269, 0.08704622834920883, 0.007129311095923185, -0.07165598124265671, -0.06986088305711746, -0.028463803231716156, -0.02357759326696396, 0.015329649671912193, -0.01065903902053833, -0.09958454966545105, 0.020069725811481476, -0.04014518857002258, -0.0660862997174263, -0.055922750383615494, -0.032036129385232925, 0.01381504163146019, -0.0673903375864029, -0.025027597323060036, 0.021608922630548477, -0.0620601624250412, 0.03505481034517288, -0.054973628371953964, -0.0021920157596468925, -0.01736101694405079, -0.1220683753490448, -0.07779566198587418, 0.0008724227664060891, -0.046745795756578445, 0.06985874474048615, -0.06745105981826782, 0.052744727581739426, 0.03683020919561386, -0.03435657545924187, -0.06987597048282623, 0.00887364149093628, -0.04392600059509277, -0.03942466899752617, -0.057737983763217926, -0.00721937557682395, 0.010713488794863224, 0.03875933587551117, 0.15718387067317963, 0.008935746736824512, -0.06421459466218948, 0.02290276437997818, 0.034633539617061615, -0.06684417277574539, 0.0005746493698097765, -0.028561286628246307, 0.07741032540798187, -0.016047099605202675, 0.07573956996202469, -0.07167335599660873, -0.0015375938965007663, -0.019324950873851776, -0.033263999968767166, 0.014723926782608032, -0.0691518783569336, -0.06772343814373016, 0.0042124162428081036, 0.07307381927967072, 0.03486260399222374, 0.04603007435798645, 0.07130003720521927, -0.02456359565258026, -0.006673890631645918, -0.02338244579732418, 0.011230859905481339, 0.019877653568983078, -0.03518665209412575, 0.0206899493932724, 0.05910487845540047, 0.019732976332306862, 0.04096956551074982, 0.07400382310152054, -0.03024907223880291, -0.015541939064860344, -0.008652037009596825, 0.0935826525092125, -0.049539074301719666, -0.04189642146229744, -0.07915540784597397, 0.030161747708916664, 0.05217037349939346, 0.008498051203787327, -0.02225595712661743, 0.041023027151823044, -0.008676717057824135, 0.03920895606279373, 0.042901333421468735, -0.0509256087243557, 0.03418148308992386, 0.10294827818870544, -0.007491919212043285, -0.04547177255153656, -0.0013863483909517527, -0.016816288232803345, 0.0057535297237336636, 0.04133246839046478, -0.014831697568297386, 0.1096695065498352, -0.02640458010137081, 0.05342832952737808, -0.10505645722150803, -0.069507896900177, -0.04607844352722168, 0.030713962391018867, -0.047581497579813004, 0.07578378170728683, 0.02707124687731266, 0.05470479652285576, 0.01324087381362915, 0.005669544450938702, 0.07757364213466644, -0.027681969106197357, 0.015634633600711823, 0.011706131510436535, -0.11028207093477249, -0.03370887413620949, 0.0342826321721077, 0.052396781742572784, -0.03439828380942345, -9.332131367059089e-33, -0.003496044548228383, -0.0012644683010876179, 0.007245716638863087, 0.08308663219213486, -0.12923602759838104, 0.01113795768469572, -0.015030942857265472, 0.01813196949660778, -0.08993704617023468, 0.056248947978019714, 0.10432837903499603, 0.008380789309740067, 0.08054981380701065, -0.0016472548013553023, 0.0940462201833725, -0.002078677760437131, -0.040112320333719254, -0.022219669073820114, -0.08358576893806458, -0.022520577535033226, 0.026831910014152527, 0.020184528082609177, -0.019914891570806503, 0.11616221070289612, -0.08901996910572052, -0.016575688496232033, 0.027953164651989937, 0.07949092239141464, -0.03504502400755882, -0.04410504922270775, -0.012492713518440723, -0.06611645221710205, -0.020088162273168564, -0.019216760993003845, 0.08393155038356781, 0.11951949447393417, 0.06375068426132202, -0.061182133853435516, -0.09066124260425568, -0.046286359429359436, 0.02162717469036579, -0.02759421616792679, -0.09041713923215866, 0.008177299052476883, -0.006156154442578554, -0.0033287708647549152, -0.004311972297728062, -0.01960325799882412, -0.08414454013109207, -0.0034149065613746643, 0.015856321901082993, -0.0005123159498907626, -0.027074772864580154, 0.03869790956377983, 0.050786130130290985, -0.028933823108673096, -0.07446572184562683, 0.022279445081949234, 0.012226884253323078, -0.01748575083911419, -0.055989284068346024, -0.011646092869341373, -0.0002180236770072952, 0.10100196301937103, 0.02999500371515751, -0.021314362064003944, -0.04096762463450432, 0.05568964406847954, -0.004973178263753653, 0.013144302181899548, 0.022288570180535316, 0.09443598240613937, 0.0018029726343229413, -0.09654559940099716, -0.01457826979458332, 0.04508035257458687, 0.06526371091604233, -0.03033633343875408, 0.009471519850194454, -0.11114948242902756, -0.046912480145692825, -0.10612039268016815, 0.11780810356140137, -0.026177652180194855, 0.0320870615541935, -0.015745604410767555, 0.06458097696304321, 0.048562128096818924, -0.034073326736688614, -0.03065350651741028, 0.06918460875749588, 0.06126512959599495, 0.0058005815371870995, -0.03808598220348358, 0.03678971901535988, 4.168464892362657e-32, -0.0452132411301136, 0.051136620342731476, -0.09363184124231339, -0.032540980726480484, 0.08147275447845459, 0.03507697954773903, 0.04584404081106186, -0.00924444105476141, -0.012075415812432766, 0.0541100800037384, -0.015797585248947144, 0.05510234460234642, -0.04699498042464256, -0.018956895917654037, -0.04772498831152916, 0.05756324902176857, -0.0827300101518631, 0.004980154801160097, 0.024522915482521057, -0.019712436944246292, 0.009034484624862671, -0.012837578542530537, 0.026660654693841934, 0.06716003268957138, -0.05956435948610306, 0.0010818272130563855, -0.018492311239242554, 0.034606318920850754, 0.04679758474230766, -0.020694732666015625, 0.06055215373635292, -0.04266247898340225, 0.008420216850936413, -0.02698715589940548, -0.028203830122947693, 0.029279250651597977, -0.010966592468321323, -0.03348863869905472, -0.07982659339904785, -0.03935334458947182, -0.02174490876495838, -0.04081539437174797, 0.049022793769836426, -0.01604332961142063, -0.0032012134324759245, 0.0893029123544693, -0.0230527613312006, 0.01536057610064745, 0.027288464829325676, -0.01401998195797205, -0.057258568704128265, -0.07299835979938507, 0.032278336584568024, 0.040280167013406754, 0.060383908450603485, -0.0012196602765470743, 0.02501964196562767, -0.03808143362402916, -0.08765897154808044, 0.047424230724573135, -0.04527046158909798, -0.015525433234870434, -0.02020418457686901, -0.06228169426321983};

    for (int i = 0; i < 384; i++) {
        EXPECT_NEAR(normalized_embeddings[i], actual_values[i], 0.00001);
    }
}

TEST_F(CollectionVectorTest, InvalidMultiSearchConversation) {
    auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    if (std::getenv("api_key") == nullptr) {
        LOG(INFO) << "Skipping test as api_key is not set.";
        return;
    }

    auto api_key = std::string(std::getenv("api_key"));

    auto conversation_model_config = R"({
        "model_name": "openai/gpt-3.5-turbo",
        "max_bytes": 1000,
        "history_collection": "conversation_store"
    })"_json;

    conversation_model_config["api_key"] = api_key;

    auto model_add_op = ConversationModelManager::add_model(conversation_model_config);

    ASSERT_TRUE(model_add_op.ok());

    auto model_id = model_add_op.get()["id"];
    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    nlohmann::json search_body;
    search_body["searches"] = nlohmann::json::array();

    nlohmann::json search1;
    search1["collection"] = "test";
    search1["q"] = "dog";
    search1["query_by"] = "embedding";

    search_body["searches"].push_back(search1);

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    req->params["conversation"] = "true";
    req->params["conversation_model_id"] = model_id;
    req->params["q"] = "cat";

    req->body = search_body.dump();
    nlohmann::json embedded_params;
    req->embedded_params_vec.push_back(embedded_params);

    post_multi_search(req, res);
    auto res_json = nlohmann::json::parse(res->body);
    ASSERT_EQ(res->status_code, 400);
    ASSERT_EQ(res_json["message"], "`q` parameter cannot be used in POST body if `conversation` is enabled. Please set `q` as a query parameter in the request, instead of inside the POST body");

    search_body["searches"][0].erase("q");
    search_body["searches"][0]["conversation_model_id"] = to_string(model_id);

    req->body = search_body.dump();

    post_multi_search(req, res);

    res_json = nlohmann::json::parse(res->body);
    ASSERT_EQ(res->status_code, 400);
    ASSERT_EQ(res_json["message"], "`conversation_model_id` cannot be used in POST body. Please set `conversation_model_id` as a query parameter in the request, instead of inside the POST body");

    search_body["searches"][0].erase("conversation_model_id");
    search_body["searches"][0]["conversation_id"] = "123";

    req->body = search_body.dump();

    post_multi_search(req, res);

    res_json = nlohmann::json::parse(res->body);
    ASSERT_EQ(res->status_code, 400);

    ASSERT_EQ(res_json["message"], "`conversation_id` cannot be used in POST body. Please set `conversation_id` as a query parameter in the request, instead of inside the POST body");

    search_body["searches"][0].erase("conversation_id");
    search_body["searches"][0]["conversation"] = true;

    req->body = search_body.dump();

    post_multi_search(req, res);

    res_json = nlohmann::json::parse(res->body);
    ASSERT_EQ(res->status_code, 400);

    ASSERT_EQ(res_json["message"], "`conversation` cannot be used in POST body. Please set `conversation` as a query parameter in the request, instead of inside the POST body");
}

TEST_F(CollectionVectorTest, TestMigratingConversationModel) {
    auto conversation_model_config = R"({
        "model_name": "openai/gpt-3.5-turbo",
        "max_bytes": 1000,
        "history_collection": "conversation_store"
    })"_json;

    if (std::getenv("api_key") == nullptr) {
        LOG(INFO) << "Skipping test as api_key is not set.";
        return;
    }

    auto api_key = std::string(std::getenv("api_key"));

    auto migrate_res = ConversationModelManager::migrate_model(conversation_model_config);
    ASSERT_TRUE(migrate_res.ok());
    auto migrated_model = migrate_res.get();
    ASSERT_TRUE(migrated_model.count("history_collection") == 1);

    auto collection = CollectionManager::get_instance().get_collection("conversation_store").get();
    ASSERT_TRUE(collection != nullptr);
}


TEST_F(CollectionVectorTest, TestVectorQueryQs) {
    auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Stark Industries"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:[superhero, company])");
    
    ASSERT_TRUE(results.ok());
    ASSERT_EQ(results.get()["hits"].size(), 1);
}


TEST_F(CollectionVectorTest, TestVectorQueryInvalidQs) {
    auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Stark Industries"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:\"test\")");
    
    ASSERT_FALSE(results.ok());

    ASSERT_EQ(results.error(), "Malformed vector query string: "
                               "`queries` parameter must be a list of strings.");
    
    results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:11)");
    
    ASSERT_FALSE(results.ok());

    results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "embedding:([], queries:[superhero, company");
    
    ASSERT_FALSE(results.ok());

    results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                            "", 10, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                            fallback,
                            4, {off}, 32767, 32767, 2,
                            false, true, "embedding:([], queries:[superhero, company)");
    
    ASSERT_FALSE(results.ok());
    ASSERT_EQ(results.error(), "Malformed vector query string: "
                               "`queries` parameter must be a list of strings.");
}



TEST_F(CollectionVectorTest, TestVectorQueryQsWithHybridSearch) {
    auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Stark Industries"
    })"_json.dump());

    ASSERT_TRUE(add_op.ok());

    auto results = coll->search("stark", {"name"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:[superhero, company])");
    
    ASSERT_TRUE(results.ok());
    ASSERT_EQ(results.get()["hits"].size(), 1);
}

TEST_F(CollectionVectorTest, TestVectorQueryQsHybridSearchAlpha) {
    auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Apple iPhone"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "Samsung Galaxy"
    })"_json.dump());

    auto results = coll->search("apple", {"name"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:[samsung, phone])");
    
    ASSERT_TRUE(results.ok());
    ASSERT_EQ(results.get()["hits"].size(), 2);
    ASSERT_EQ(results.get()["hits"][0]["document"]["name"], "Apple iPhone");


    results = coll->search("apple", {"name"}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:[samsung, phone], alpha:0.9)");

    ASSERT_TRUE(results.ok());
    ASSERT_EQ(results.get()["hits"].size(), 2);
    ASSERT_EQ(results.get()["hits"][0]["document"]["name"], "Samsung Galaxy");
}

TEST_F(CollectionVectorTest, TestVectorQueryQsWeight) {
    auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Apple iPhone"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "Samsung Galaxy"
    })"_json.dump());

    auto results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:[samsung, apple], query_weights:[0.1, 0.9])");
    
    ASSERT_TRUE(results.ok());
    ASSERT_EQ(results.get()["hits"].size(), 2);
    ASSERT_EQ(results.get()["hits"][0]["document"]["name"], "Apple iPhone");


    results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0,
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7,
                                fallback,
                                4, {off}, 32767, 32767, 2,
                                false, true, "embedding:([], queries:[samsung, apple], query_weights:[0.9, 0.1])");

    ASSERT_TRUE(results.ok());
    ASSERT_EQ(results.get()["hits"].size(), 2);
    ASSERT_EQ(results.get()["hits"][0]["document"]["name"], "Samsung Galaxy");
}

TEST_F(CollectionVectorTest, TestVectorQueryQsWeightInvalid) {
    auto schema_json =
        R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["name"], "model_config": {"model_name": "ts/all-MiniLM-L12-v2"}}}
        ]
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);

    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Apple iPhone"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "Samsung Galaxy"
    })"_json.dump());

    auto results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0, 
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, 
                                fallback, 
                                4, {off}, 32767, 32767, 2, 
                                false, true, "embedding:([], queries:[samsung, apple], query_weights:[0.1, 0.9, 0.1])");
    
    ASSERT_FALSE(results.ok());
    ASSERT_EQ(results.error(), "Malformed vector query string: `queries` and `query_weights` must be of the same length.");

    results = coll->search("*", {}, "", {}, {}, {0}, 20, 1, FREQUENCY, {true}, Index::DROP_TOKENS_THRESHOLD,
                                spp::sparse_hash_set<std::string>(),
                                spp::sparse_hash_set<std::string>(), 10, "", 30, 5,
                                "", 10, {}, {}, {}, 0, 
                                "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 6000 * 1000, 4, 7, 
                                fallback, 
                                4, {off}, 32767, 32767, 2, 
                                false, true, "embedding:([], queries:[samsung, apple], query_weights:[0.4, 0.9])");
    
    ASSERT_FALSE(results.ok());
    ASSERT_EQ(results.error(), "Malformed vector query string: `query_weights` must sum to 1.0.");
}


TEST_F(CollectionVectorTest, TestInvalidVoiceQueryModel) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": "invalid-model"
        }
    })"_json;

    
    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Unknown model namespace", collection_create_op.error());

    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": "base.en"
        }
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Unknown model namespace", collection_create_op.error());

    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": "invalid"
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ(collection_create_op.error(), "Parameter `voice_query_model` must be an object.");

    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": 1
        }
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Parameter `voice_query_model.model_name` must be a non-empty string.", collection_create_op.error());

    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": ""
        }
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Parameter `voice_query_model.model_name` must be a non-empty string.", collection_create_op.error());
}

TEST_F(CollectionVectorTest, TestVoiceQuery) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": "ts/whisper/base.en"
        }
    })"_json;

    EmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "Zara shirt"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());

    add_op = coll->add(R"({
        "name": "Samsung Galaxy smartphone"
    })"_json.dump());
    ASSERT_TRUE(add_op.ok());
    auto results = coll->search("", {"name"}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                                 0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", "", "UklGRrSFAABXQVZFZm10IBAAAAABAAEAgD4AAAB9AAACABAATElTVDIAAABJTkZPSU5BTRAAAABDYXlpcmJhc2kgU2suIDQASVNGVA0AAABMYXZmNjAuMy4xMDAAAGRhdGFWhQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD//wAAAAAAAAAAAQAAAAAAAAAAAP///////wEAAQAAAAAA//8AAAEAAAD///////8AAAAAAQAAAAAAAQABAP///////wAAAAD//wAAAQABAAAAAQD//////v///wAA//8AAAAAAAACAAEAAAAAAP7///8BAAEAAgAAAAAAAAABAAEA///+////AQACAAEA/////wEAAAABAAAAAAAAAAEAAQD/////AQAAAAAAAQAAAAEAAAD/////AAAAAP////////3/AAAAAP//AAD+/////v///wAAAAD///////////////8AAAAAAAABAAEAAQABAAEAAQABAAIAAQABAAAAAAABAAEAAQABAAEAAQABAAEAAAAAAAAAAAABAAEAAgACAAIAAQABAAAAAAABAAIAAwADAAIAAgACAAEAAAAAAAEAAQAAAAAA///+//7//v/+//7//v/+//7//v/+//7///////////////////8AAAAAAQABAAMABAAGAAgACQAJAAUAAQD//////v8AAAAA/f/3/+3/7P/q/+3/9f/7////AQD///r/9v/1//P/8P/v/+3/9P8BAAgAEAAUABAACQD+//j/9P/z//b/9P/z//j/AgAGAAEA9v/t/+z/7v/2//n/AAABAAMACgAFAAMAAAD2//j//P///wwAEgAWABQABgD3/+n/5//s//L//P8CAP//9//2//b//f///wUAEgAYABUAEQAMAAgABgADAAAAAAALABYAGgAWAA8ABAD///n/9P/y//L/+P/+//j/9//5//b//f8EAAQABAAIAA4AEgASAA8AFQAZABsAHQAaABkAFgASAAUA/P8AAAYAEwAcABsAFQANAAYABQABAPr/7f/q//D/9/8KABYADwANAAIA+P/7/wAABwARABgAHQAbABcAGgAbABcAFgAPAAkAAAAEAAwAFgAoACwAJwAbAAwAAAD2//n/AwD///v/+f/9/wYACwALAAcAAQD//wAAAAAFAAkACwAFAPz/+f/1//H/7P/i/+L/4f/g/+b/7//8/wAA8f/r/+D/4P/b/9H/xv/I/9P/6f/0//r/9//s//X/9v/y/+7/3P/U/9P/1P/f/+7/+P/8//f/7v/m/97/4P/i/+f/7//2//T/6f/i/+L/4v/q/+7/6f/n/+f/7v8AAA4AEwANAAEA9P/m/9z/2//a/+f/AAAHAAkABQD+/+3/5v/k/93/5v/s//j/DAAVACAAHwAbABoAFwAgACYALQA2ADoAOQA5ADoAOwA/AEMAQgA4ADUAMQAwADAAJgAlACUAJgAkACQAHgAfABoAHAAfAB0AIwAnACUAGwAXABQAFgAfABwAIwAbAAsABQDs/93/1f/Y/+P/4//c/93/2P/V/9b/0f/V/9r/4P/d/9//4v/i/+v/9//9//////8BAPr/9v/0//L/+P/8/wMACAADAAIA+P/2//3//f8EAAkABAAAAPv/+f/2//P/+f8CAAkACwAOAAwABAAJABgAJQAsACgAIAAXABAADwAcAC0ANgAwAC4AIgAaAA4ABQD5//H/7P/u//P/9/8AAP3//f8BAAoA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v//G/8D/sf+2/6n/nf+k/6D/pP+r/6P/nP+d/5v/mv+Y/5n/l/+R/4X/kv+X/6j/wP/L/9D/2v/3/wMADgAZABcAJwAxADgAOgBDAEQATwBOAEIAQgA+AEIAOQA4AEUARgBKAEsARABFAEUASABeAGsAewCVAJEApACrALUAwgC3ALkAuwC3AKkAogCbAI8AmQCSAI4AjACAAH8AeAB3AG8AagBqAGAAWwBVAE4ARgBDACgAKgAdABUAIAAVACAAIgAjACkAJgAeABwADwD8//f/6P/i/+3/5f/h/97/5v/o/+X/3v/p/+X/4v/n/9//0v/R/9D/0v/P/9j/3v/R/9v/0//V/97/2f/g/+z/9/8EAP//CwAOAAYABgAHAAgADQAVABgAFQAfAB4AIgAfABIADwATABUACwAHAAUABgAEAAYA/v/7//3///////7//P8AAAAAAQDt//L/+f/6/woAGQAXACkAHwAdADMAJAAcAAwADAALAAEAFgAWABoAHgAhABkAEAAOAB4AJQAtADQAJQAZABsAGwAgACoALQAwADsAPwA6ACwAKQAxACAA+P/t/93/2f/Y/9L/vv+w/6j/q/+b/5f/pP+t/67/of+g/5//n/+W/4//if+H/37/dv96/4v/kf+T/4//g/+L/3//ev95/3X/bf9v/1//Xv9v/3T/gf+K/5T/nv+g/5//l/+m/6//xP/B/7r/wv+w/7v/w/+4/63/pv+j/5z/mv+U/5X/mf+x/7//uv+x/67/uP/B/8r/1f/Z/+r/9f/2/wQACwAEAPX/7P/v/+z/8//y//X/BwARABQAFAAZABwAHwAtACkALwA7AD8AQQA7ADsARABJAEQAWABaAFcAWwBWAEwASwBMAEUATABGADAAJAAfABEAFwAUAAcACgAGAAkADAAMAAcADgAJAAQAEwAVABAAGAAaACMAKgAmACcANAA1ADkASQBaAG0AaABhAFkAVABVAEwAVABcAFkAUgBLAEIAQAA5ADYANgAtABoAEgAVABEAIgAlACIAIAAnABwAGwAuADMANQA+ADoAMgAvACkANQAvACsAPQBFAFMAVgBRAFQAXwBhAGUAdAB5AIQAigCLAIgAlQCJAIMAfABoAF8AWgBWAFsAVQBZAGUAXgBqAGEAXgBnAFoAWABPAFAAYQBfAF8AYABlAGAAUgBDADkA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");
    
    ASSERT_TRUE(results.ok());
    auto results_json = results.get();
    ASSERT_EQ("Smartphone", results_json["request_params"]["voice_query"]["transcribed_query"].get<std::string>());
    ASSERT_EQ(1, results_json["hits"].size());
    ASSERT_EQ("1", results_json["hits"][0]["document"]["id"].get<std::string>());
}

TEST_F(CollectionVectorTest, TestInvalidVoiceQuery) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": "ts/whisper/base.en"
        }
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto collection = collection_create_op.get();

    auto results = collection->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                            0, "exhaustive", 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "", "test");
    
    ASSERT_FALSE(results.ok());
    ASSERT_EQ("Invalid audio format. Please provide a 16-bit 16kHz wav file.", results.error());
}

TEST_F(CollectionVectorTest, TestInvalidHNSWParams) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vector",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "ts/e5-small"
                    }
                },
                "hnsw_params": {
                    "ef_construction": "aaa",
                    "M": 16
                }
            }
        ]
    })"_json;



    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());

    ASSERT_EQ("Property `hnsw_params.ef_construction` must be a positive integer.", collection_create_op.error());

    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vector",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "ts/e5-small"
                    }
                },
                "hnsw_params": {
                    "ef_construction": -100,
                    "M": 16
                }
            }
        ]
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());

    ASSERT_EQ("Property `hnsw_params.ef_construction` must be a positive integer.", collection_create_op.error());


    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vector",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "ts/e5-small"
                    }
                },
                "hnsw_params": {
                    "ef_construction": 100,
                    "M": "aaa"
                }
            }
        ]
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Property `hnsw_params.M` must be a positive integer.", collection_create_op.error());


    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vector",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "ts/e5-small"
                    }
                },
                "hnsw_params": {
                    "ef_construction": 100,
                    "M": -100
                }
            }
        ]
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("Property `hnsw_params.M` must be a positive integer.", collection_create_op.error());


    schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vector",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "ts/e5-small"
                    }
                },
                "hnsw_params": {
                    "ef_construction": 100,
                    "M": 16
                }
            }
        ]
    })"_json;

    collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto collection = collection_create_op.get();


    auto results = collection->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "vector:([], ef:aaa)");
    
    ASSERT_FALSE(results.ok());
    ASSERT_EQ("Malformed vector query string: `ef` parameter must be a positive integer.", results.error());

    results = collection->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "vector:([], ef:-100)");

    ASSERT_FALSE(results.ok());
    ASSERT_EQ("Malformed vector query string: `ef` parameter must be a positive integer.", results.error());

    results = collection->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "vector:([], ef:0)");
    
    ASSERT_FALSE(results.ok());
    ASSERT_EQ("Malformed vector query string: `ef` parameter must be a positive integer.", results.error());

    results = collection->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "vector:([], ef:100)");
    
    ASSERT_TRUE(results.ok());
    
}

TEST_F(CollectionVectorTest, TestHNSWParamsSummaryJSON) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vector",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "ts/e5-small"
                    }
                },
                "hnsw_params": {
                    "ef_construction": 100,
                    "M": 16
                }
            }
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto collection = collection_create_op.get();

    auto summary = collection->get_summary_json();

    ASSERT_TRUE(summary["fields"][1]["hnsw_params"].is_object());
    ASSERT_EQ(100, summary["fields"][1]["hnsw_params"]["ef_construction"].get<uint32_t>());
    ASSERT_EQ(16, summary["fields"][1]["hnsw_params"]["M"].get<uint32_t>());
    ASSERT_EQ(0, summary["fields"][0].count("hnsw_params"));
}

TEST_F(CollectionVectorTest, TestUpdatingSameDocument){
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "vector", "type": "float[]", "num_dim": 10}
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_TRUE(collection_create_op.ok());

    auto collection = collection_create_op.get();

    std::mt19937 rng;
    std::uniform_real_distribution<float> dist;

    // generate 100 random documents
    for (int i = 0; i < 100; i++) {
        std::vector<float> vector(10);
        std::generate(vector.begin(), vector.end(), [&](){ return dist(rng); });

        nlohmann::json doc = {
            {"vector", vector}
        };
        auto op = collection->add(doc.dump());
        ASSERT_TRUE(op.ok());
    }

    std::vector<float> query_vector(10);
    std::generate(query_vector.begin(), query_vector.end(), [&](){ return dist(rng); });
    std::string query_vector_str = "vector:([";
    for (int i = 0; i < 10; i++) {
        query_vector_str += std::to_string(query_vector[i]);
        if (i != 9) {
            query_vector_str += ", ";
        }
    }
    query_vector_str += "], k:10)";

    auto results = collection->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, query_vector_str);
    ASSERT_TRUE(results.ok());
    auto results_json = results.get();
    ASSERT_EQ(results_json["found"].get<size_t>(), results_json["hits"].size());

    // delete half of the documents
    for (int i = 50; i < 99; i++) {
        auto op = collection->remove(std::to_string(i));
        ASSERT_TRUE(op.ok());
    }

    // update document with id 11 for 100 times
    for (int i = 0; i < 100; i++) {
        std::vector<float> vector(10);
        std::generate(vector.begin(), vector.end(), [&](){ return dist(rng); });

        nlohmann::json doc = {
            {"vector", vector}
        };
        auto op = collection->add(doc.dump(), index_operation_t::UPDATE, "11");
        ASSERT_TRUE(op.ok());
    }


    results = collection->search("*", {}, "",
                            {}, sort_fields, {2}, 10, 1, FREQUENCY,
                            {false}, Index::DROP_TOKENS_THRESHOLD,
                            spp::sparse_hash_set<std::string>(),
                            spp::sparse_hash_set<std::string>{"vector"}, 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                            "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                            4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, query_vector_str);
    ASSERT_TRUE(results.ok());

    results_json = results.get();
    ASSERT_EQ(results_json["found"].get<size_t>(), results_json["hits"].size());
}

TEST_F(CollectionVectorTest, TestCFModelResponseParsing) {
    std::string res = R"(
    {
        "response": [
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"publish\"}\n\n",
            "data: {\"response\":\"Date\"}\n\n",
            "data: {\"response\":\"Year\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \"}\n\n",
            "data: {\"response\":\"2\"}\n\n",
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"title\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \\\"\"}\n\n",
            "data: {\"response\":\"S\"}\n\n",
            "data: {\"response\":\"OP\"}\n\n",
            "data: {\"response\":\"A\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"top\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" [\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Links\"}\n\n",
            "data: {\"response\":\" to\"}\n\n",
            "data: {\"response\":\" x\"}\n\n",
            "data: {\"response\":\"k\"}\n\n",
            "data: {\"response\":\"cd\"}\n\n",
            "data: {\"response\":\".\"}\n\n",
            "data: {\"response\":\"com\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Apr\"}\n\n",
            "data: {\"response\":\"il\"}\n\n",
            "data: {\"response\":\" fool\"}\n\n",
            "data: {\"response\":\"s\"}\n\n",
            "data: {\"response\":\"'\"}\n\n",
            "data: {\"response\":\" com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Inter\"}\n\n",
            "data: {\"response\":\"active\"}\n\n",
            "data: {\"response\":\" com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\" with\"}\n\n",
            "data: {\"response\":\" animation\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Dynamic\"}\n\n",
            "data: {\"response\":\" com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\" with\"}\n\n",
            "data: {\"response\":\" audio\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\" ],\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"trans\"}\n\n",
            "data: {\"response\":\"cript\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \\\"\"}\n\n",
            "data: {\"response\":\" \\\"\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"},\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"{\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"alt\"}\n\n",
            "data: {\"response\":\"Title\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \\\"\"}\n\n",
            "data: {\"response\":\"I\"}\n\n",
            "data: {\"response\":\"'\"}\n\n",
            "data: {\"response\":\"m\"}\n\n",
            "data: {\"response\":\" currently\"}\n\n",
            "data: {\"response\":\" getting\"}\n\n",
            "data: {\"response\":\" totally\"}\n\n",
            "data: {\"response\":\" black\"}\n\n",
            "data: {\"response\":\"ed\"}\n\n",
            "data: {\"response\":\" out\"}\n\n",
            "data: {\"response\":\".\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"id\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \\\"\"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\"6\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"image\"}\n\n",
            "data: {\"response\":\"Url\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \\\"\"}\n\n",
            "data: {\"response\":\"https\"}\n\n",
            "data: {\"response\":\"://\"}\n\n",
            "data: {\"response\":\"im\"}\n\n",
            "data: {\"response\":\"gs\"}\n\n",
            "data: {\"response\":\".\"}\n\n",
            "data: {\"response\":\"x\"}\n\n",
            "data: {\"response\":\"k\"}\n\n",
            "data: {\"response\":\"cd\"}\n\n",
            "data: {\"response\":\".\"}\n\n",
            "data: {\"response\":\"com\"}\n\n",
            "data: {\"response\":\"/\"}\n\n",
            "data: {\"response\":\"com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"/\"}\n\n",
            "data: {\"response\":\"black\"}\n\n",
            "data: {\"response\":\"out\"}\n\n",
            "data: {\"response\":\".\"}\n\n",
            "data: {\"response\":\"png\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"publish\"}\n\n",
            "data: {\"response\":\"Date\"}\n\n",
            "data: {\"response\":\"Day\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\"8\"}\n\n",
            "data: {\"response\":\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"publish\"}\n\n",
            "data: {\"response\":\"Date\"}\n\n",
            "data: {\"response\":\"Month\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"publish\"}\n\n",
            "data: {\"response\":\"Date\"}\n\n",
            "data: {\"response\":\"Timestamp\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\"3\"}\n\n",
            "data: {\"response\":\"2\"}\n\n",
            "data: {\"response\":\"6\"}\n\n",
            "data: {\"response\":\"8\"}\n\n",
            "data: {\"response\":\"6\"}\n\n",
            "data: {\"response\":\"6\"}\n\n",
            "data: {\"response\":\"4\"}\n\n",
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"publish\"}\n\n",
            "data: {\"response\":\"Date\"}\n\n",
            "data: {\"response\":\"Year\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \"}\n\n",
            "data: {\"response\":\"2\"}\n\n",
            "data: {\"response\":\"0\"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\"1\"}\n\n",
            "data: {\"response\":\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"title\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" \\\"\"}\n\n",
            "data: {\"response\":\"Black\"}\n\n",
            "data: {\"response\":\"out\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"top\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\":\"}\n\n",
            "data: {\"response\":\" [\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Links\"}\n\n",
            "data: {\"response\":\" to\"}\n\n",
            "data: {\"response\":\" x\"}\n\n",
            "data: {\"response\":\"k\"}\n\n",
            "data: {\"response\":\"cd\"}\n\n",
            "data: {\"response\":\".\"}\n\n",
            "data: {\"response\":\"com\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Apr\"}\n\n",
            "data: {\"response\":\"il\"}\n\n",
            "data: {\"response\":\" fool\"}\n\n",
            "data: {\"response\":\"s\"}\n\n",
            "data: {\"response\":\"'\"}\n\n",
            "data: {\"response\":\" com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Inter\"}\n\n",
            "data: {\"response\":\"active\"}\n\n",
            "data: {\"response\":\" com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\" with\"}\n\n",
            "data: {\"response\":\" animation\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Dynamic\"}\n\n",
            "data: {\"response\":\" com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\"\\\",\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"Com\"}\n\n",
            "data: {\"response\":\"ics\"}\n\n",
            "data: {\"response\":\" with\"}\n\n",
            "data: {\"response\":\" audio\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\" ],\"}\n\n",
            "data: {\"response\":\"\\n\"}\n\n",
            "data: {\"response\":\"\\\"\"}\n\n",
            "data: {\"response\":\"\"}\n\ndata: [DONE]\n\n"
        ]
    })";
    auto parsed_string = CFConversationModel::parse_stream_response(res);
    ASSERT_TRUE(parsed_string.ok());
    ASSERT_EQ("00,\n\"publishDateYear\": 2011,\n\"title\": \"SOPA\",\n\"topics\": [\n\"Links to xkcd.com\",\n\"April fools' comics\",\n\"Interactive comics\",\n\"Comics with animation\",\n\"Dynamic comics\",\n\"Comics with audio\"\n ],\n\"transcript\": \" \"\n},\n{\n\"altTitle\": \"I'm currently getting totally blacked out.\",\n\"id\": \"1006\",\n\"imageUrl\": \"https://imgs.xkcd.com/comics/blackout.png\",\n\"publishDateDay\": 18,\n\"publishDateMonth\": 1,\n\"publishDateTimestamp\": 1326866400,\n\"publishDateYear\": 2011,\n\"title\": \"Blackout\",\n\"topics\": [\n\"Links to xkcd.com\",\n\"April fools' comics\",\n\"Interactive comics\",\n\"Comics with animation\",\n\"Dynamic comics\",\n\"Comics with audio\"\n ],\n\"", parsed_string.get());
}

TEST_F(CollectionVectorTest, TestInvalidOpenAIURL) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"},
            {
                "name": "vector",
                "type": "float[]",
                "embed": {
                    "from": ["name"],
                    "model_config": {
                        "model_name": "openai/text-embedding-3-small",
                        "api_key": "123",
                        "url": "invalid url"
                    }
                }
            }
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    ASSERT_FALSE(collection_create_op.ok());
    ASSERT_EQ("OpenAI API error: ", collection_create_op.error());
}

TEST_F(CollectionVectorTest, TestRestoringImages) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "image", "type": "image", "store": false},
            {"name": "embedding", "type":"float[]", "embed":{"from": ["image"], "model_config": {"model_name": "ts/clip-vit-b-p32"}}}
        ]
    })"_json;

    auto collection_create_op = collectionManager.create_collection(schema_json);
    auto coll = collection_create_op.get();

    auto add_op = coll->add(R"({
        "name": "dog",
        "image": "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"
    })"_json.dump());


    auto summary = coll->get_summary_json();
    ASSERT_EQ(1, summary["num_documents"]);

    collectionManager.dispose();
    delete store;

    store = new Store("/tmp/typesense_test/collection_vector_search");
    collectionManager.init(store, 1.0, "auth_key", quit);
    auto load_op = collectionManager.load(8, 1000);

    if(!load_op.ok()) {
        LOG(ERROR) << load_op.error();
    }

    ASSERT_TRUE(load_op.ok());

    coll = collectionManager.get_collection("test").get();

    ASSERT_EQ(1, coll->get_summary_json()["num_documents"]);
}