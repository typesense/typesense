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
        ConversationManager::get_instance().init(store);
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
        "model_name": "openai/gpt-3.5-turbo"
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
                                 true, 0, max_score, 100, 0, 0, HASH, 30000, 2, "", {}, {}, "right_to_left", true, true, true, model_add_op.get()["id"]);
    
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
        "image": "/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAoHCBYWFRgVFhYZGBgaHR8eHBwcHBwZHBwfHBwaHhoaGiEcIS4lHSErHx0dJzgmKy8xNTU1HCQ7QDs0Py40NTEBDAwMEA8QHxISHjQrJSs0NDQ3NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NP/AABEIAPcAzAMBIgACEQEDEQH/xAAbAAACAgMBAAAAAAAAAAAAAAAABQMEAQIGB//EAD0QAAECBAQEBAUEAAQFBQAAAAECEQADITEEEkFRBWFxgQYikaETMrHB8EJS0eEHFHLxI2KCkrIVM0Oiwv/EABkBAAMBAQEAAAAAAAAAAAAAAAABAgMEBf/EACIRAAMBAAICAgMBAQAAAAAAAAABAhEDIRIxBEEiMlETYf/aAAwDAQACEQMRAD8A9mggggAIIIIAMQQRDPxCUtmLPaE3gJaTQqxfGUIcCpBaFeO4kpVi3Q0/GhItTa1Nh9zHJyfIzqTq4+De6OiVx8vRNOd/aGmCx4XyP32jhsOskeYNyublvaLMnFFCndmqdG5xnHyKT7NL+PLXR30EK+H8QBT5i3M0/DE6+Jyh+r0BMdquWt043FJ5hdghWvjcoB3JtYb2v39I1PH5IuSG/wCUn6Qf6T/Q8K/g2ghfJ4zIVaYkf6vL/wCTRnieKyylKSQeYI1h+c5uh4vcKPGOMZAQg+ZJ81Lcg/5SOdTx6YP1F3s7kcm0pvvFPFTSpJNSpyHLnX5vr6Qrw86WlklZd8rOSQakq9hWPPvlqnqO2OKUsO4wXiOnnDjfWHmEx8uYHQoHlY+keZ4eZmNyHNtgPwlzvDFU0pTQ+YlgzludNg5i4+RU++yL4E/XR6NBCrguP+InKo+dIGbc84ax2zSpajkqXLxmYIIIoQQQQQAEEEEABBBBABiOd4/iPNl2H1joo4/jXzqfcxz/ACHkm3BO0KJk8uQP7/3jJwBLGYojZIv3OnSCQsZwVVao6i0ThZWfzWPPXZ6HojOGFGJAG9XjdaASzAudaiLQw4F6n6REhDRWYS60ygkXjRSrxIo0aNDTSkN0T4hLAAJUHffTK7N3J94rTVvQAfjt0/qDEVI61H36OfaBCQdLj6ke2veEqG5K6pjGg3rzDfnrEeJK2YKKRyvrvQ0+kTLQQd/z6UtbrGhLht/zvXVm5RRGEHwyA5XXYa3Fd6E6QnShMqbnW5QWYmpBa0NMTMYsBoGY8qjf3ctC6dhc6CAwewAq4H8gw8KTw6XDzJak+Rt41xcxSUpyhJzFg9nNnGvTpCTh2KShA+IACNR/MNpWJStLvTQHkb9YjML1Mv8AC8ZkKZj1FFUIf9wrpt/Ud2kuHEechbg/goatsaj8rHb8DnFclLhiKelo6vjV25OT5E+qGUEEEdhyhBBBAAQQQQAEEEEAGI5XxRhyCVAUI946qKmPwaZgYiuh2jLmjzlo04r8a082AUVAB3NNh+aw/wANICABy9dzArhmRdRUWq93FW/KxKp+keeoc+zvd+XoyUu7xhbCADtGF/n9QNiSIFrAiGauhY1iSdQ/xEKlat9IzbNEiMOahx1HRtWP53xIINLH+9N9PUPEcwGpYEXLDM40Zrm+zwYdZNTQW7Cn43K8NAwmMBXnU0cgtt7xXXl6mjj/APRpt9Cwiwvy5iRR9AAA9HLU/PWlMQEkgVJID/wQKks3WsWjNlaclLlTPXQnfViCbWislg5LMTSjij3J2+0W1uxBd20buX/LG1Iqy8yiQaNYD+YslkGJq+uaxpQ8os4HGWQsAEX2bfpEaZZKspActUU2/O8WRw5S8ik3eo3SoC/tA10NMZ4QZmSgGpper9dI9D4fh/hoCfXrCvgPBkywFqqo8mA6CHsdPBxufyf2cvNyKniNoIII6TAIIIIACCCCAAggggAxBBEOKm5UKVsKddPeE3i0EtOfxkx5ijzYdBSK4MRuamJwfxo86q8np6ErxRVmrMaGaEgqNKEk7ARKUpJtbk3+8LeK4RS5akJ1vp0B2H9xj9mq9GmH4xLmKyJWCqhA/Ux1a4HURcWAB1pend6x53wPwrjDiE+RKAiaFmeo1UmnkT+4EPQD9RfaPSlYJz5lqNbBgDys7PzjS4U5j0ibb9orFspBszA2Z7NV9XflC3G4hCCVHys5KiohLM9zcADkLVLw9VgkuFVBGzfm8cr4y8P4icgKkLQsIJORYAzeXKPm8pIuHYEs9hBEJvGF1i1DHhWITNTmlqSpJoClTg6NelfykazxRRo4FdxyJvb6Qj8B8PnYZEwzkKSV5UhFB8rutemoG5bpHS4xRIKVBnYAUYirvR9faHUqaxCltrWK5wdP+4LV7D/aKmDYKUf21oKOwrTXTvaJSCXINAsA130FNAN9DEaJYDpc1rptWjPUOPaGgYIWQSr7VZNq6VaL3Dp7rHVtaE6cwwHrFJbl0pAfmHuRQua2+oaL+FociQQABlJqUkMeps3Y9wk9I4Wt5ae49DFyKHBi8pJ3rF+PQj9UcVfszMEEEUSEEEEABBBBAAQQQQAYhbxonIBuofQn6wyhfxhJMumhBiOT9WVH7I55Jr+fmsZmEtTWA3jVTG5+0eaz0EazSEAl2A1jbC8OKwFroLhApf8Adz5RhPnmoQ1GzHtXvVvWHS7GLiE+2TVtdIV8V4jKw6M61hCBR9zoABUnkI4XFf4kyQrySlqD/MSlHcCvu0I/8RMWuZi1IUTkR5UJ2oCVdS99gI5FUnSOieKaWsxdtPEemSv8RcMospMxHMhKgOuVT+0dRhOJoWgLQsLQRQioO/cHvHhPw46bwNjFonFAqg1I0ez+n0ieThUrUVHI6eM9IxUwZSDq76ggwpM7zlCicqqJZzXblQDrFnHrcgClIVYkEgsWO40LXjBGzLqCS9NRm6s33HvvFfEqKXa53pTMKONS5NNo1w+JzhOhIDgkBilQswqLxvjTVL3uz6X/AJvDEVDNUrMbNl3NRduzaxc4Yr/i3LNT7A9Q5ikAKgfqFOqbs8a4AH4ictGbQXp7awCPWvDg/wCAjvz1MNYp8Ll5ZSEs1HbZ6t7xcj0JWSjgp7TMwQQRQgggggAIIIIACCCCADERYiXmSpO4IiaMQmtA46ahixuPtSIis6+t+XbpzhpxyQUH4n6TfkefX6wo+bdPM2+sebyS5po9DjpVOlvhjGYVf8jD1H9Q1KxaEGDm5FhTUNCeuvq3vDRU17Gv5WL4n+JHJP5HN+MfCaMQPiIIRMG75VjQFqgjQ9unl2O4DiJZZUlZqACBnSXIAYpflHuKl3EIuIzHTQjylKn/ANKgT9Iv/Ry8XoFCpHk+H8OYlZbIUDdflbt83tHX8D4OjD0NVak0JPIbQ9xIYFqmKOcOCak2G45bdbRFclV0y5iZ7La0P6RSnSrtFpC1MxZz7cu0RTlBCSo6D8aJGJsCQmYsV+a2lgSWi+skrJbWtLCoH2MK8JVZWoXrrT/baHKkkJoK09iNdQ1fSACumV5kXrQ6b2JttHQ+GMDmnCjpCnNNq+jNCjADMoDQVNq19rfTaPR/DvDEyZdBVTKPpQRpxT5UZcleMjiMwQR3HGEEEEABBBBAAQQQQAEEEYgAIqYniEuXRSgDtc+0LeK8aCfLLLq1NwOQ3McopRU5J1uX9BHNy/IU9T2zo4uB12+jrpvHJRdJBIIqWDfWOYMxOfIKJ0Cqlms4/LxFLkvZVu/11jZdTRV/Xsf5jkrmd+zqjimPRnGKQwALN1c3pyFP61jErFKQGPy7EuR+CKi8HMJdK20YgFPdqt3i1LwU3Kyig8wlvUPUcuV4UpvtDrF0y0MchVjXar+hrFTFSwUqH7htFXiHBpiw3xMm5SgAnup94RY3gs6WkqRiJoap8xq2n4DGvi37M/JL0OZ5WUtmFbsmv1aKoSEk5U9T9q/7RzC8ViQkkzVIFWzFJJ9qX3il8PErVlVOW7fuPrlBY3Gm3KBQPyOvOMCCcynP7RU/0IV43FldVEAD9P0PMwql8LngBImNzAqesSp4BNWXKyOoBt9IagXkXUz01U5oG6EuCx6RalYgFiBrSrvYC9XYGOfxWBnSTlUtICg4NNL168tY3lyVuVEhbWIJ6VbTTtyiXODT07rAcQw8ghcwZwSDlS3lp+qtmNq8717TA+KZExyMwALORva0eSOtUvKoJrsc3swa/PSGPDApDBhd9WLOHUDRwBDnkc+ia45r2ezS5gUHBBHKN44rgPFig+ZKgGqDlrzSx9o7GVMCkhQLghwY7OPkVo5LhyyWCCCNCAggggAIIIIAMQo45xISksn5lCnIbmGOJnBCSo2AeOAx+KKySSSX1q20c/Py+E4vbNuDj8q1+iGZmUGBIO7ezPSkYRIGhN2J26A840C/KADXXrYWaIULCEgqIGUqFSA4dQHJi1qCPNPRGKFaD1taIs4JNbHX86xYkVFGY21pzPOCZL5Ac4GCZmSvMWB++hi+cVLQPNMQDtmD+kJ5kgKTlIFb6RTRw5CfmN7pzKLDlUMGpaNY5PFYRfH5PRzM41IqM7kXCQpR9hSOX4vx9S3QgfDSaFawc3/SkW6k9odplpSXCWfSp+toxxJSPhk5ApZ8qARdRs/LU8gY38m1pj4pPDgJXDVrWChReWlJdRUoFVWoosPLlPeGKcLiVLSVJQoJ1AKVdiP9o6bDYJMshLA7qo6lF3f0fuWhj8OlGgVeXY2s6FGFl+QKUnLp52SX6uxHN4r47G5BZki6jryQB8yob4+Wn4agtyLs5AZwT7/WFZwiJiEpXLCdQAGIAZ6ipt7xFcmdMcxvYhEleJWpZATQBKXCsqRZwWuHJ7xeRwcpAOZiNN3ajAb8obEBDpSAE8gU7WvvpX7VMQgag75tKNyrE+TZWYRmWEqSlRFyH0LJfYbv94volABjehoWV1DdxCTMCk5jmPzAZQGYM9PTvDzB1KfJ5Muln1Hby1/uEwRiWjIpqjV0sKHrcGOw8M8Tf/hKc3YnfbvfqY5Weh00FQbhy439CYl4bPyroa+UvyNe9K23i4rxeonkjyWHpkEV8FPC0g+vXWLEegnq1HA1nRmCCCGIIIIjmLCQSSwAc9oAEfijFhKAh6qLkchb3+kcWVl62csa/wAflYvcTnFUxS3DE61IGgvtFK357x5XNflWnp8MeM4RIJKsgpY5tA1+W0YWiXmEtRCzmFDoFE5c3YH05xohQBWv9qRTZjUbVANoi4XJK5qS7VCzSpCTT3alKGIlGjZ0qMOAkAMANqD2gVKSbqN7+zf784nWQBUsBEKFBdQCwsW+lOUNolMXTpjGmZuQoAInwyXHXQ1Pev5SJVSQWYfTpv8AaNsMgipP0p6QRO0O6ySKZIVcV5GK0h1zjTyS3G5zqZ2/0incwwxOJShCl3yglt2FB3NO8LeHAhCUqUVVJLgVJLqPQEmvPpG3K8WGPH29JlDMsEJLJJ/TclgFAvT00i18Mm3qIrGYQEuDq9APlYuQ+wH4YsJxSkpHl9aEDR9bNC4X3g+T+mVyvL5jTXZjd4XTw4dCXVmZtKmuYtQByb2iycao0ZIHf+YXS5JXnK0ouf1E0BvlDM4D/Yw+We0xcdfRqsLzLdLgDZ9KClT/ANVniriZ6EfMq4HNqhgSFMKFte0WsVhWRlR5c4/ScxFCxcsAL/KLm8c7KQS+V0AkNmcl6AXoBQd9olFMixeJ+GpJIACydWLFLZS+5GuoG0P8FOKUBRUXUGZswJGWwFLBXr0hIpIUt1kKQKAKDZjX0r9Yjw2IEtaJYzPmJFSXFARzp7dBB7GdfhphUnMkEEmoJ7UpYe7xFKl5VUc5w50p02vbeJpSVEhhmGzU0BoKWEXGSQ7Cl+R+8SMYeFcbkJSqpURQG2gJf3rHZx5ciatCnQFElQINAxKvwetI9E4ZiviICiz6gR2fHrrDj5478i9BBBHSc5iFPiGfllZf3Fuwqft6w2jmfExJUlOyadzX6RlzPxhmnEttHNTR+PFOYSHOt7hLkNQva9OhhhNwu6i52FKddYqzJJTZXmFWblp71fkY8s9NEOJfJ8ocj5aNWr25GM8BkvOWuzJYC3zEOTr+mNcdLIQUu7VdtK2d/wAMHhmY65iaMhKPUlTA9MvvFyKjpFj6jtr/AHBMUB0+kYc9hz9vpGiTQ8uXpAyUbiW71NPzb8eKxVoLROpbJLC33+/8xXQd6RtxL7MuR/RT40jMhKKnMXUBcpQyj/8AbJ7xYlBOViObG16GIB55xLuEJSkilFLL82fy+kXvgpyerl7tqTraI5O6LjqSrNQynJ8zaEkhwAxA1LU6QLXqE1ZiAerPtrp9IkKQgmjlZ81auW0FaPpvGVpVSjA0IpSr5ga6vTYiFHT0K7WFdDm6QOtYhnTUIWcz+eoyk1oxDDkBp+qMqmuWjTHJCUpmKFEGrbKGU9RUR0Wtkxl4yPHraWoPlJBAcBVrA12cfffnFoWMoKzVIcJARRNU83B6GukPeIIzZEuAFqBYBgEhiLm7J+kKELICi5p6UD76xzm4uQgpNAwNW6sWHoekSTsMpTKaqS6Tcgh6/l4mXOZQDuUhIJYsWJcEbtryjdOKuFA/pZgNyTrsYWhh0HB8SFywqtQHataW7w2kIZRdm0J7sw736xzHh7FJWpaGYJUDc0dyLikdGpiCz6H6sN7j3gfQ0bTkhIe/50i54exikz8pAyKAANmdvuIXKXm2Zt6kl6Nvb3jSRPy3cMbGxc+wq7xXHePTPknVh6VBC3hGP+ImrZgzsX/P7hlHoy1S1HBScvGEcvxxTz22SPd46iOT4sGnrPT/AMRGPyP0NeD9yiZVtIoYqW1QA9g9vMQ4PV/YxfmGK0wJJCSQFPmZ9iQ57tHntHehZxCoIBcsX9SD3hd4KmvMxD2BQkdWL9qiGHECtwySQdg46UfX82R+FZi5c+clSVJdVHSQ9AKUtzi4+wr6O2XPD5RUva9t2tAhwncVf+oE6qapNd2Br3+ZoMpAOr8/zWJwDTETwwD6jlufsPSJLikUOLLZKQ3mKr9rRTViSlCiotlST6B42isRlU6yXhCM4mrbM805dvKyUn0B9odzCEjzWB/HhF4VU0tKQ+6jzNde3rDjE6XNQe5p9Ij/AKX/AMMBFnYlhXUlh/EZnruRWlPZz7CK60uol281W2AA7DV4rS8OotmWWBJpR60B5NprCDDASyiHeLa8MFy1JJooEezeoipiQoJKxeoTudi3aEfDJS0ElJWVK0FQ76jXqfaNlyJLszcNvotYecSlJUAVIDNzdm5faNf/AE0qoVB2dydW19eUbL4BPmLUsBKHLkFQrqaAFnNYvYbgExJJUtJBqzs5paoa3T75F6KFcIQls8wBRfytVtTUxFJRJQSHK1WcOwvYC/WOgHAE1oS9HcE+qq839DvRTwFCWzS5irgmqRS5LaaducIelXgKEImKIP8A7ihlBFWSLH0MdGpAYm7/AMNCHGYBCSlQWoZSCA4ykUoWDw+RNC0Am50F7O1IT7Q0V5c0ppTU9t2eg09Y1WlwywAFAkVq+nPewiSRI/U7VcgAF9a7394yvECwoxI05HTlCkKGfhSYZawjKSCGB2eo+jPHax59whZExKgKAi7OR5i5aosI9BEd/wAd/icPOvyCOW48cs4k2IS30+sdLMmBIJJYDWOT4zjxMLpozpBuTva0HyGvHGHAm60olQBrdqdHr9ogVVbpRnUKOmpY3fn/ADF7DcPoVLYk/KnQAb8ztDCSUoTlSnKL3udSdTHCp/p2us9CcYeZrLVU3GgO76xBiFnOEBC8ofMpSVNYNQByL/1r0MnFJOoPSo+sSLmPFKF/SXb/AIIxN8rmxF6MecRLWeRFrgH3LGGGP4bLmADKARUXZ+aQQDCubLMtkBJvuqpNyTYCv06QqloapMr46WF0B8yXID2KgAH7iFAwiwhctaiXBZRLmtCPv3h/iUkACjnflcXtb1hfNmaUNwzi/wBX7wtwog8MYpKEKQssxZ3LWe+lXh8taVlwRQ+lielD7xy8grlBWRlEn1Lk+b+RvEicYlEsZnBKDmKBRzmoRbQB6WhgPFCjmzDpZre8Kf8ANKK0JlhRbRPOuvMdqxHxCeRLSorJfK4NAXJckC3yimjc4acIwoQkLXRcxqGhCdAxN6110vAMMLgFqGacf+gVD7lgA/SGCChPlFBsBoB/y7CLGCQpacygUJJoLKKef7X9ekMMqUnygCjfm8LCWxbIC1NlQcv7leW3L5j6Rmfh1pIdJI5ZT7X9434nxqVISFTVhL2Fyd2Ar9oi4TxqViQoyl5stwQQQ7sa6FrjaK8OtJ8uzBKwklSFCtLFtictgIklrUS+a1/6GnvDEKox/qKmISEmhIBagYN0MPxzsN3oX4goW4UzKTUsHBZiVPQhmhchBkqKVEUSVUdiACQ1N4scSW3lAyguSSQC1aGjCtTyjVUxE5AQT8zpN3SrLcFtniRp4S4hByslTAN9vtSKyASk3qCzhiwNKXFPtBLngBkqzMAASKk6KPcbGJMOwKk2oAdy4r3c+0JIpvolwJAWz33rYOLltRb7x6Bhy6Um9BXePNpM7zhqbafho3aPQeFzs8tJ7ekdnx37RyfIXplDjc8k5Em1Te7UFPWFshAcmlKd9ekWcWofEXXUn0oYgSDkS1KOR1rp1jHke02zXjWSkbKUY5H/ABDmTBhk5CQkrAWR+0gs/LM3tHRcS4iiQgrmqypFBqSToBqYSYDxdhMSr4SgpJVQCYlOVT6UUR2MKU0/LC6pZh5jg8dNkrExCyFDnQsbKAuGpHtsucopSSClwk5XqHAJBhXJ8PYVC86JKcwqCSpQB0ISokA9qQyQXg5LVNYTMNeyyJlHjWbLdApUmmhHTUUhRxTjUjDBPxVfMaJSCpR3LCw5mncxc4fxmViU5pSwoC9wU9QaiDHmsWrSLGSw5R5SrYltKM1h2uLwl4hKdSVkEE0L2cAhi51A7sI6DFzEgMkZWq7Avv1pCzi8shTlqsA1SFVY8gzDmDEUjSWc6tRCiCxBtWxrSpseXPeFsviYSr4a0gJ1PJxS/URcxCyz0JIf+Lxz/E1HMlY2r0vBI6Oz4MEzpiyXyS1JIdmPk8qQf9Tlo6zh8pyZqqn5U8hdZHVVH2SIReGZGTDS6+ecQugslYFf+2vdo6lVABsPpCfsN6NSY1eEI8W4UzfhfE8zsFMQgnbNbvaHKpkJpr2Umn6PKPHYm/5pecKYtk1GVtO7w/8A8OuGrQFzlgpCwEofUO5V0oK61jtFEG9Y5Pxp4jXIyy5dFqGYrYEpDsMoNHJeps0bLkdLxSMXCl+TZ2JW0QqmHSpMeb+GvFOIViES5izMRMUEsQHSTQKBSBbV6MDHoBmU7Xialy8ZU0qWoh4jKCknLVYAFKPmYOaPeKeGmpQgM2YEGqQSKVPI3pFhU4BCypLmhfkmov8AjwvkIK1BNvM5OgA19IlDZXnYhf8AmFJBSEgvt8yQoE9E0prvpIicc4uA9SWrQnT8qYW8ewhRPM1C0rQWGUVIZITUa2uIlwk50EVqCB/Q3vFOcEnoxJ/YX0ezXr+bx6NwL/2JfT7mPO8BIUshIcs2ln1MemYNOVCU7AD2jf46etmHO+kjkuPyVZ1VDO6iHzM1gdL3i2tTHpaLfiTBZxmFiGV2sT6/SEvD55WChVFoorm1AodYx5Jc00a8dKpTEfjfhi8TIAlh1IL5dVBmLc+XWPMcLwfEKmBCJaytxTKoNW5ceUczHr/HOMowqAtYUXLJCQCSWfUgCKnBfGUiesS/OhZ+ULZlHYEEh+RaL46pT6JtS69jxCCAAouWDnctUxhQYxNGijGWGmnlfjzN/mlE/sQ3TzU9c3rEPgbGFGLQkHyzHQoaEEEjuFN7xB4t4imfiFLQXQAEoO6Uv5uhUpRHJoz4JkleMl7JdR6JST9WjrzI7/hzPuuj1pE0sWAT2ijiMy0KQ4qCAX8wyk6aDbWNJs9gVNGq8QpEpLFOZYKjQEuSSQ52BZ+UcTZ1pYcxj05AS9uccricW6wi7sCwtmZ+9YZcaxbrKU312H9xSwmCzH7xpEr7FTPY8FJaamoCUJKUJGjD+KQceUoSJ5T82RbNvlNucU+GYorQhRIMwJr/AKgNRsRtuYtKxIUSlmIDsXqCLjQxL66Ys30eAzFVrHsnhPHGdhJayXUBlV1SWrzZo8/8X+Hjh5mZAeWs+U3ym5Qdm03HeIfDPH5mEUzZ5ai6ka/6k7FtLFu8dPJPnOoxivCuz15JhJ4n8OIxWVQXkWkMC2YEXYhw1decM8DikTpaZktWZCg4P1B2INCOUTGjnlHInUPo6aSpHM8A8IIwyxNUv4i0vkYZUJcMTclRYkaCsPZw3pyEeV8V8R4mYsq+ItA0ShSkBI0HlIc2qY63wbxKdPlL+Kc+VQSlRFTRyC12pX/mje5eeTZjFTvijoEIBLGj2F31r6RUxCiiVMI+ZflHLODSuyR7xPNQAUi5JoB5Utq7aWeIONzUBQQBUHMWZqgNbW/rEQtoqniEcjhBX8yjDLD+HUaLWOhiTCzHLCOp4VgrE1MdShM56tos+HOBplpuo9Y6dCQA0RYZDCJ41mUl0Y1Tb7NVpBBBtHHY/hRkTM6HYlidCDdJ56iOwmKYQqxuNLENEcvGqRXHbl9HFcc4XLxaMilZFguk6g9P1A8o5CR4FxKZqRmRlCgc4VYA3Au/31juceou4l5gNO7xVXxLImpYftWC7f6mp1NI5fyjo6smux4FPCjxPLmLw01EtysiwuU5hnSNyU5g0Qo8RSLLWEbHMFJLXYp+4ETzOKSb/GR3UB9YhPOy83o8f+AtS8oSSp/lAJV6Xj0XwrwE4ZBWthMXcfsT+3qTU9ANIb/+qSr50nmlQUfYvGhx6T8iSvmGSB1zt7PFXzOlmCniUvWybFnyMKrNAL13YbRQ8YYwS0JQB5rBhd2Pt94kRishK5hAWKIQg5j3INXF3ZoU4nh+IxK86kKrYMQkDvGcw6KqkjlZWHJLmpMdDwjhxJtHR8N8HkMV+kdHhuFIQGAjpjjf2Y1yL6FGDwJAiwtCjYkLScw209aQ5EoDSIZuBCou+PyRE8mMWrkomoUlaRstBAIcXb+Y4riXgB1kyZgSn9q3Ldxf0jv18NXQDR6ihGx5xAiViK5kA7EeU9wzP0jnXnL9Gz8K+xZ4e4MMNJEvPnOZSiflFWDAHSn1i/Nd4JhWP/imHokH7xUxOJnhsuHWXH6noX1YHfeFSdd4NNLrShifDOGWvOqUAomrKUkE7sC0XQiXJQEhKUpFkpHc0HM3ihNGJWD5FIfQJKvVyIlwnD8QbSlAkCqyDVq2u0Hjb6DYXZvMm5XmKY/tRz07bmFmGwKlmOiw3hpZOZYcmHOG4GRG8cblGNciYjwHDgmwrHW8LwjBzEuG4alMMEpaN5nDCq0yBGYIIsg1UHitMwoMEEAERwY2ERr4ek3SDBBE4itZXXwSUboT6CK8zw1h1XlJPYQQQ/CR+dGE+GMOP/iR6CJkcDki0pH/AGiCCD/Of4HnRZl4BCaJQhPRIH0jf4BgghYhaZ/yxjIwsZggAx/lYyMLBBDFpuMLEicMIIIQazPwBGP8uIzBAGgMMnYRuJQjMEUI2yCMgQQQAZggggAIIIIAP//Z"
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
    ASSERT_FALSE(collection_create_op.ok());

    ASSERT_EQ(collection_create_op.error(), "Only one field can be used in the `embed.from` property of an embed field when embedding from an image field.");
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
        "model_name": "openai/gpt-3.5-turbo"
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


TEST_F(CollectionVectorTest, TestInvalidAudioQueryModel) {
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
    ASSERT_EQ(collection_create_op.error(), "Voice query model not found");

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
    ASSERT_EQ(collection_create_op.error(), "Voice query model not found");

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
    ASSERT_EQ(collection_create_op.error(), "Parameter `voice_query_model.model_name` must be a non-empty string.");

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
    ASSERT_EQ(collection_create_op.error(), "Parameter `voice_query_model.model_name` must be a non-empty string.");
}

TEST_F(CollectionVectorTest, TestAudioQuery) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": "whisper/base.en"
        }
    })"_json;

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
    ASSERT_TRUE(add_op.ok());
    auto results = coll->search("*", {}, "",
                                 {}, sort_fields, {2}, 10, 1, FREQUENCY,
                                 {false}, Index::DROP_TOKENS_THRESHOLD,
                                 spp::sparse_hash_set<std::string>(),
                                 spp::sparse_hash_set<std::string>(), 10, "", 30, 4, "title", 20, {}, {}, {}, 0,
                                 "<mark>", "</mark>", {}, 1000, true, false, true, "", false, 10000,
                                 4, 7, fallback, 4, {off}, 100, 100, 2, 2, false, "", true, 0, max_score, 100, 0,
                                 0, HASH, 30000, 2, "", {}, {}, "right_to_left",
                                 true, true, false, "", "", "", "UklGRrT9AQBXQVZFZm10IBAAAAABAAEAgD4AAAB9AAACABAATElTVDIAAABJTkZPSU5BTRAAAABOZXcgUmVjb3JkaW5nIDIASVNGVA4AAABMYXZmNTguNzYuMTAwAGRhdGFW/QEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAQAAAAAAAAAAAAEA//8BAAAAAAABAAAAAAD/////AQABAAEAAQAAAP//AQAAAP//AQD//wAAAAACAAAA/v8BAAAAAAD//wAAAAABAAEAAAD/////AAAAAAAAAQD//wAAAAAAAAIA//8AAAAA//8CAAEAAQAAAAIA/v/+/wEAAwAAAP7/AgAAAAEAAAACAAAA/f8BAAAAAAAAAAAAAAABAAAAAAABAP//AQAAAP////8BAAAAAAAAAAAAAAD/////AAAAAAIA////////AwAAAP7/AQD+/wEAAQACAP///v8BAAAAAgD/////AAD//wAAAAAAAAAAAAAAAP////8AAP//AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAEAAQAAAAEAAQABAAEAAQABAAEAAAABAAEAAQADAP3//f/9/wAAAQD4//n/9v/7//n/+f/6/+//8//0//X/8f/y//X/9P/y//H/9f/6//3//v/9/wQABgAFAAUADQAQAA0ADwAOABcAGAATAA4AEAAOABIADgAGABMAEgAIAPr/AAAMAAEA+f/3//7/8//s/+X/4//p//D/7P/e/+T/4v/h/+b/5f/0//H/7f/p/+n/8f/4//P/8f/3/wAAAwAHAAcACAAAAAUAAgD2//////8GAP7/BAAFAPj/BAADAAAA/P/+/wMACwASAA0AEgATAAsACwAEABEACQACAAwACgASAAoABwANAAgACQADAAcADAAQAA4ABgADAAEA/P/9//v/AQD5/wEADAAFABAA/f8FAAYAAgAXAAIACQAEAP3/EwAaABAABgACAAEA//8FAP//9/8CAPP/7v////n/9v8GAAYA+P8FAAAAAQALAAoABAAUABIACgAWAAAA+f8GAAUACAD//wIA+/8LABMAFAARABAAJAAbAB0AGQAYACYAHgAyABwAFQATABIAEAAOABMABQAAAP///v8IAP3/7//8/+//8f/9/+3/8f/n//D/AADv//z//P8BAP///P8IAPr//f/+/wYABwD7/wAA9P/t//b//P/u/+T/8f/0/97/5P/j/9L/2v/i/9//0P/W/+H/6f/b/9j/5f/t/wEA9//1//b/+P8HAAkA//8CAAMA+v/5/wYAAwD6//j//f8BAPz/8//d/9j/6f/n/+j/4//u/wAA9//0//b/6v/1//D/8f/4//H/8//2/+z/9P/7//X/2//v/wQA+P8BAAAA+//0/wQADQDt//T/CgADAPX///8BAAcAAgDy//3/BgAWAAsABgABAAQAEQAEAPv///8EAAcA/f/5/wUAAQACAPL/CQADAPn/DQAHABEA9v/7/xwAEAAMABAADQAAAPL/AwATABEAGQARAA8AGQAUABMAIQAiABMAGgAaABIAEAANABUADAD1/wQACAAAAAMABAAOAAMAAAD6//D/9P/2//r/DgAdABMADgD7//T/DAALAAoAEQARAAEAAAAJABcAEgAKAAsAAgAHAAsACgAGAAIA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EwEEAQsB8ADcAMEArwClALYAugCtAJ8AogCuAJsAowC6AL8ApQCSAKIAswCfAKYAswCrAJ8AhwBlAFEAXQBoAGsAZABWAEMASQBYAF4AVgAuACAAAgDo//f/6//y//b/EgAKAOj/xv+y/6n/mf98/3v/jP99/4//kv+B/4j/d/93/3//cv+H/5j/j/+W/5//qP+0/63/pf+x/6P/hf9n/3//if93/3r/m/+T/4X/g/9z/2n/a/9z/17/T/9G/zH/Pv9M/2H/ZP9o/1n/PP9H/1X/Yv9F/0z/Nf8T/yH/Nf9M/1v/hv+L/5r/s//G/9v/1f/d/+z/zf+0/7T/wf/E/6b/k/9+/2r/a/9d/1v/R/85/0n/Qv9D/zv/Jv8s/z3/Sf9H/1H/av97/3T/eP+U/5z/qf+z/9v/5v/m/w4ALQBNAGEAZgBYAFgAdwB8AIMAggBcACgAAQDH/7T/v/+f/5//lv+I/2j/Zf9y/4L/kP+u/8v/0f/h/wQAHgA9AFwAewCCAHoAdABtAHQAlACnAL0AyAB+AHMAiQB2AGwAlgClAJoAcABmAIEAkgCbALkAtACLAHgAWwA/ADQANwA9ACAAJQAqAB8AIgAUAB0AFwAdACcAIQAiADEANQAwAC0AVQBlAIwAtQDbANcAswC5AKoAogCKAHMAewBaACkADwD1/77/sP/C/6n/i/+X/4D/bP99/5X/rP+T/6X/s/+7/9v/6v8JAAsALwBEADMAJQA7AEMANAAhACMAIgD7//n/BADq/+j/9//4/+v/vv++/7T/i/+K/3D/Yv9j/1//U/9P/1L/W/9s/2f/bP9z/2T/af9M/zj/Qf87/1z/jv+u/8r/2v/v/xEATgBNAFIAYwBRAIAAngCYAJ4ArgCZAHoAYwBSAEkAUQB5AHYAWwAwACAABQDv/+f/2//b/8L/tP/G/97/+v8CABUAGAA7AHMAcwBiAIUAoACJAHwAeQCWAJwAiQCCAGYAXwBUAE4AYgCBAKcAkgBmAFcAPwAPABMA/v/+/+v/2v/u/+n/8P/9/wsAEgAqAEUANgA2ADMAJAASABQAJgANAPr/2//b/+n/5P/e/8z/r/+Z/4j/hf97/3X/bv+M/4//mv+4/97/6f/P/9r/0P/K/8D/qP+K/3T/g/+P/3L/Wv9q/3T/ZP9u/3j/ef+W/5z/h/+C/43/r//P/9T/6f8IAPr/AwAcAB0AAADe/+f/EAAmABUACwDo/9b/zf/E/8r/wP+8/8X/2f/V/9//+f/5/xoARgBoAEMARgBaAGQAiwCUAKAAmQB9AIoAngCpAL0AwwCvAJsAlQCkAJUAnQCoAI0AbgBaAEgAOwA3AF0AVAAVAPX/6P/b/+T//f/6//H/8//0/9r/zv/E/+r/9P/j//T/6f/q//P/+P8JABoAHwAoABoAHAAuADMARAA8AD4AOQA1AEwAMAAdAAIA9f8FABoAGQAZAAcA9f/T/8r/y//c/wQAAAAGAPn/4f/j/+v/2v+//7r/0//d/+T/7P/1/+7/BAAgAAUA////////CQAWAA8A7f/1//b/8v/s//H/4P/x/wcAFQAeAAcA8P/b//z/AAD4/wgAIgA/AEUAWgBVAFEASgBOAIMAmgCsAL4AsACmAJMAfQCKAKUArwCPAGMATAAXAB4ALQAXABEA/v/x/9b/nP+M/3//mv+O/27/e/+A/5T/mP+P/7T/uP+h/6P/uP/I/8D/0P/J/9j/6f/e/+3/1f/D/9X/1P/l/+X/2P+//6T/mP+U/4n/n/+r/7r/3v/i/+f/zf/E/7X/pf+s/4H/ef98/4r/j/9w/0//If8P/xT/D/8c/yr/Hv///gr/Cf/6/vT+9P7t/v/+Gv8c/zj/Xf+B/4D/hv97/1v/ZP96/4n/hP99/5r/oP+h/6n/r/+u/7v/rf+y/9j/4//z/w8ALQBCAEAASABeAFgAQAAnAPz/4//m/+D/7P8BAAcA9P/h/7b/vv/g//j/CgD1/+v/7/8ZACUAOQBnAGcAWwBpAIEAkwC8ANsADwEjAQ8BKQE3ASAB8QDxAA8BDAHxAPkACAHeAL0ArACmAIEAfQCFAHQAfQByAJ0AqQCaAJQAmQCeAJEAkQB1AGwAWgBVADYAKgBOAGcAfwBrAGIARgAzADMAOgBHAGUAnQCxAJwAtgDLANQA8QDWAM8AvgCuALkAqACcAIUAbgBCACQACQD1/8//mP+I/37/eP97/4L/bP92/4z/nv+m/8j/9/8fAC8AIQAkADEAPQBOAFcASgBRAE0AQwA7ACgAKAANAOv/y/+1/8v/nP+a/6b/e/96/43/kf94/3T/fP92/4T/hv+n/83/1v/Z/+b/9//0//b//v8HABQAJgApACgALwAVABMA/P/j/7v/nf+z/7D/mv+D/4X/dP9b/0H/L/9j/2n/Xv9j/2r/fv+D/3r/j/+S/4v/nP+p/7z/wf/C/7f/tP/T/+P/6f/X/8v/x//F/8H/vP+r/77/3f/e/97/4f8AAA8AHwBKAFEATgBNAEMANQAeACcARABQAE8AJAAfABEA6v/Z/8z/yv/J/8H/x//M//T/DQD2/wgABgAMABkAKQBVAFMAVABeAE0ATwBOAGMASwA8AEsAcgBwAGMAbgBtAG0AcACAAIcAfACCAHkAgQB4AEcAPwArAAEA6f/b/+b/1/+z/6D/n/+l/7L/0/+1/6v/tP+0/8f/1P/t/+v/CwAsACwATwBHAD4ARABLAEoAQwAhAB8AMwAdABgAAQD0//D/1f/M/7z/v/+w/5r/mv+R/4X/cv9g/2r/eP91/2j/Yv9i/0z/Nv85/0T/U/9t/43/iv+c/7H/zf/v/woAHgAtAD0AQgA5ACwANABDAE0AOAAaABEAEgALAPT/8v/i/7j/o/+P/3z/av9o/3T/bv9a/0T/Mv8r/0D/Tv9X/1j/Xf9u/3n/d/94/4L/mf+i/6v/p/+r/7f/vP/a/+j/AAACAP7//P/5//z/+//5//f/9//6//n/+//9//7///8BAAIAAgACAAEAAgADAAMAAgABAAEAAAD/////AAD///7//v/+//////////////////7//////////v/+//7//v/9//3//f/9//7//v/+//7//v/+//7//v/+//7//v///////////wAAAAAAAAEAAQABAAIAAgABAAEAAgACAAIAAQABAAAAAAAAAAAAAAAAAAAAAAABAAEAAQADAAMAAgACAAIABAADAAMAAgACAAEAAAD+//3//f/8//3//v//////AAACAAEABAABAP//AgACAAwACQALAAcABgAGAAQABAAFAAUABAACAAIA//8AAAEAAQACAP////8EAAQA//8CAAUABgABAAAABwACAAIABgAEAAEAAQAFAAQAAAAAAAEABQADAAEA//8CAAMAAwABAAMAAgAEAAYAAQABAAEAAAD+/wEAAgAAAAEAAgABAP7//f/////////+//3/AAD//////////////f/+//7//v////3///////3//f/9//3//v/8//v//v/9//3//f/8//z//P/7//r/+//8//v//P/8//3//f/9/w==");
    
    ASSERT_TRUE(results.ok());
    auto results_json = results.get();
    ASSERT_EQ(results_json["request_params"]["voice_query"]["transcribed_query"].get<std::string>(), " This is a test recording for audio search.");
}

TEST_F(CollectionVectorTest, TestInvalidAudioQuery) {
    nlohmann::json schema_json = R"({
        "name": "test",
        "fields": [
            {"name": "name", "type": "string"}
        ],
        "voice_query_model": {
            "model_name": "whisper/base.en"
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
                            0, HASH, 30000, 2, "", {}, {}, "right_to_left",
                            true, true, false, "", "", "", "test");
    
    ASSERT_FALSE(results.ok());
    ASSERT_EQ(results.error(), "Invalid audio format. Please provide a 16-bit 16kHz wav file.");
}