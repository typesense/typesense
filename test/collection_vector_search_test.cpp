#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include "collection.h"
#include <cstdlib>
#include <ctime>
#include "index.h"

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
        TextEmbedderManager::get_instance().delete_all_text_embedders();
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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto schema = actual_schema;
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();

    ASSERT_EQ(1, text_embedders.size());

    auto delete_op = collectionManager.drop_collection("test", true);

    ASSERT_TRUE(delete_op.ok());
    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());

    // create another collection
    schema = actual_schema;
    collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    coll = collection_create_op.get();

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    // create second collection
    schema = actual_schema;
    schema["name"] = "test2";
    collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll2 = collection_create_op.get();

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();

    ASSERT_EQ(1, text_embedders.size());

    delete_op = collectionManager.drop_collection("test", true);
    ASSERT_TRUE(delete_op.ok());

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    delete_op = collectionManager.drop_collection("test2", true);
    ASSERT_TRUE(delete_op.ok());

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto schema = actual_schema;
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();

    auto text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();

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

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
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

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    drop_op = coll2->alter(drop_schema);
    ASSERT_TRUE(drop_op.ok());

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    drop_op = coll->alter(drop_schema);
    ASSERT_TRUE(drop_op.ok());

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

    auto schema = actual_schema;
    auto collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll = collection_create_op.get();
    auto text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
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

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
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

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(0, text_embedders.size());

    // create another collection
    schema = actual_schema;
    schema["name"] = "test2";

    collection_create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(collection_create_op.ok());

    auto coll2 = collection_create_op.get();

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
    ASSERT_EQ(1, text_embedders.size());

    // drop collection
    auto drop_op = collectionManager.drop_collection("test2", true);

    ASSERT_TRUE(drop_op.ok());

    text_embedders = TextEmbedderManager::get_instance()._get_text_embedders();
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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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

    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
    
    TextEmbedderManager::set_model_dir("/tmp/typesense_test/models");

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
