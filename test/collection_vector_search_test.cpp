#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include "collection.h"
#include <cstdlib>
#include <ctime>
#include "conversation_manager.h"
#include "conversation_model_manager.h"

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
        ConversationManager::init(store);
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
                                 true, 0, max_score, 100, 0, 0, HASH, 30000, 2, "", {}, {}, "right_to_left", true, true, true, 0);
    
    ASSERT_TRUE(results_op.ok());

    auto results = results_op.get();

    ASSERT_EQ(4, results["hits"].size());
    ASSERT_TRUE(results.contains("conversation"));
    ASSERT_TRUE(results["conversation"].is_object());
    ASSERT_EQ("how many products are there for clothing category?", results["conversation"]["query"]);
    std::string conversation_id =  results["conversation"]["conversation_id"];

    
    // test getting conversation history
    auto history_op = ConversationManager::get_conversation(conversation_id);

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

    LOG(INFO) << "Adding image to collection";

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