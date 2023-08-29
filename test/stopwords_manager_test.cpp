#include <gtest/gtest.h>
#include "collection.h"
#include <vector>
#include <collection_manager.h>
#include <core_api.h>
#include "stopwords_manager.h"

class StopwordsManagerTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    StopwordsManager& stopwordsManager = StopwordsManager::get_instance();
    std::atomic<bool> quit = false;

    virtual void SetUp() {
        std::string state_dir_path = "/tmp/typesense_test/stopwords_manager";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
        store = new Store(state_dir_path);

        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);
        stopwordsManager.init(store);
    }

    virtual void TearDown() {
        delete store;
    }
};

TEST_F(StopwordsManagerTest, UpsertGetStopwords) {
    auto stopwords1 = R"(
            {"stopwords": ["america", "europe"], "locale": "en"}
        )"_json;

    auto upsert_op = stopwordsManager.upsert_stopword("continents", stopwords1);
    ASSERT_TRUE(upsert_op.ok());

    auto stopwords2 = R"(
                {"stopwords": ["a", "an", "the"], "locale": "en"}
            )"_json;

    upsert_op = stopwordsManager.upsert_stopword("articles", stopwords2);
    ASSERT_TRUE(upsert_op.ok());

    auto stopwords3 = R"(
                {"stopwords": ["India", "United States", "Japan", "China"], "locale": "en"}
            )"_json;

    upsert_op = stopwordsManager.upsert_stopword("countries", stopwords3);
    ASSERT_TRUE(upsert_op.ok());

    auto stopword_config = stopwordsManager.get_stopwords();
    ASSERT_EQ(3, stopword_config.size()); //total stopwords set
    ASSERT_TRUE(stopword_config.find("countries") != stopword_config.end());
    ASSERT_TRUE(stopword_config.find("articles") != stopword_config.end());
    ASSERT_TRUE(stopword_config.find("continents") != stopword_config.end());

    ASSERT_EQ(3, stopword_config["articles"].size());
    ASSERT_TRUE(stopword_config["articles"].find("a") != stopword_config["articles"].end());
    ASSERT_TRUE(stopword_config["articles"].find("an") != stopword_config["articles"].end());
    ASSERT_TRUE(stopword_config["articles"].find("the") != stopword_config["articles"].end());

    ASSERT_EQ(2, stopword_config["continents"].size());
    ASSERT_TRUE(stopword_config["continents"].find("america") != stopword_config["continents"].end());
    ASSERT_TRUE(stopword_config["continents"].find("europe") != stopword_config["continents"].end());

    ASSERT_EQ(5, stopword_config["countries"].size()); //with tokenization United States will be splited into two
    ASSERT_TRUE(stopword_config["countries"].find("india") != stopword_config["countries"].end());
    ASSERT_TRUE(stopword_config["countries"].find("united") != stopword_config["countries"].end());
    ASSERT_TRUE(stopword_config["countries"].find("states") != stopword_config["countries"].end());
    ASSERT_TRUE(stopword_config["countries"].find("china") != stopword_config["countries"].end());
    ASSERT_TRUE(stopword_config["countries"].find("japan") != stopword_config["countries"].end());
}

TEST_F(StopwordsManagerTest, GetStopword) {
    auto stopwords = R"({"stopwords": ["a", "an", "the"], "locale": "en"})"_json;

    auto upsert_op = stopwordsManager.upsert_stopword("articles", stopwords);
    ASSERT_TRUE(upsert_op.ok());

    spp::sparse_hash_set<std::string> stopwords_set;

    auto get_op = stopwordsManager.get_stopword("articles", stopwords_set);
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(3, stopwords_set.size());

    stopwords_set.clear();

    //try to fetch non-existing stopword
    get_op = stopwordsManager.get_stopword("country", stopwords_set);
    ASSERT_FALSE(get_op.ok());
    ASSERT_EQ(404, get_op.code());
    ASSERT_EQ("Stopword `country` not found.", get_op.error());

    //try fetching stopwords with token
    stopwords = R"({"stopwords": ["India", "United States", "Japan"], "locale": "en"})"_json;

    upsert_op = stopwordsManager.upsert_stopword("country", stopwords);
    ASSERT_TRUE(upsert_op.ok());

    get_op = stopwordsManager.get_stopword("country", stopwords_set);
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(4, stopwords_set.size()); //as United States will be tokenized and counted 2 stopwords
}

TEST_F(StopwordsManagerTest, DeleteStopword) {
    auto stopwords1 = R"(
                {"stopwords": ["america", "europe"], "locale": "en"}
            )"_json;

    auto upsert_op = stopwordsManager.upsert_stopword("continents", stopwords1);
    ASSERT_TRUE(upsert_op.ok());

    auto stopwords2 = R"(
                    {"stopwords": ["a", "an", "the"], "locale": "en"}
                )"_json;

    upsert_op = stopwordsManager.upsert_stopword("articles", stopwords2);
    ASSERT_TRUE(upsert_op.ok());

    spp::sparse_hash_set<std::string> stopwords_set;

    //delete a stopword
    auto del_op = stopwordsManager.delete_stopword("articles");
    ASSERT_TRUE(del_op.ok());

    auto get_op = stopwordsManager.get_stopword("articles", stopwords_set);
    ASSERT_FALSE(get_op.ok());
    ASSERT_EQ(404, get_op.code());
    ASSERT_EQ("Stopword `articles` not found.", get_op.error());

    //delete non-existing stopword
    del_op = stopwordsManager.delete_stopword("states");
    ASSERT_FALSE(del_op.ok());
    ASSERT_EQ(404, del_op.code());
    ASSERT_EQ("Stopword `states` not found.", del_op.error());
}

TEST_F(StopwordsManagerTest, UpdateStopword) {
    auto stopwords_json = R"(
                {"stopwords": ["america", "europe"], "locale": "en"}
            )"_json;

    auto upsert_op = stopwordsManager.upsert_stopword("continents", stopwords_json);
    ASSERT_TRUE(upsert_op.ok());

    auto stopword_config = stopwordsManager.get_stopwords();

    ASSERT_EQ(2, stopword_config["continents"].size());
    ASSERT_TRUE(stopword_config["continents"].find("america") != stopword_config["continents"].end());
    ASSERT_TRUE(stopword_config["continents"].find("europe") != stopword_config["continents"].end());

    //adding new words with same name should replace the stopwords set
    stopwords_json = R"(
                {"stopwords": ["india", "china", "japan"], "locale": "en"}
            )"_json;
    upsert_op = stopwordsManager.upsert_stopword("continents", stopwords_json);
    ASSERT_TRUE(upsert_op.ok());

    stopword_config = stopwordsManager.get_stopwords();

    ASSERT_EQ(3, stopword_config["continents"].size());
    ASSERT_TRUE(stopword_config["continents"].find("china") != stopword_config["continents"].end());
    ASSERT_TRUE(stopword_config["continents"].find("india") != stopword_config["continents"].end());
    ASSERT_TRUE(stopword_config["continents"].find("japan") != stopword_config["continents"].end());
}

TEST_F(StopwordsManagerTest, StopwordsBasics) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "title", "type": "string" },
          {"name": "points", "type": "int32" }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    nlohmann::json doc;
    doc["title"] = "The Dark Knight Europe";
    doc["points"] = 10;
    coll1->add(doc.dump(), CREATE);

    doc["title"] = "An American America";
    doc["points"] = 12;
    coll1->add(doc.dump(), CREATE);

    doc["title"] = "An the";
    doc["points"] = 17;
    coll1->add(doc.dump(), CREATE);

    doc["title"] = "A Deadman";
    doc["points"] = 13;
    coll1->add(doc.dump(), CREATE);

    doc["title"] = "A Village Of The Deadman";
    doc["points"] = 20;
    coll1->add(doc.dump(), CREATE);

    //when all words in query are stopwords
    auto stopword_value = R"(
        {"stopwords": ["the", "a", "an"], "locale": "en"}
    )"_json;

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    req->params["collection"] = "coll1";
    req->params["name"] = "articles";
    req->body = stopword_value.dump();

    auto result = put_upsert_stopword(req, res);
    if(!result) {
        LOG(ERROR) << res->body;
        FAIL();
    }

    req->params["collection"] = "coll1";
    req->params["q"] = "the";
    req->params["query_by"] = "title";
    req->params["stopwords"] = "articles";

    nlohmann::json embedded_params;
    auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    std::string json_results;

    auto search_op = collectionManager.do_search(req->params, embedded_params, json_results, now_ts);
    if(!search_op.error().empty()) {
        LOG(ERROR) << search_op.error();
    }
    ASSERT_TRUE(search_op.ok());
    nlohmann::json results = nlohmann::json::parse(json_results);
    ASSERT_EQ(0, results["hits"].size());

    req->params.clear();
    json_results.clear();

    //when not all words in query are stopwords then it should match the remaining words
    stopword_value = R"(
            {"stopwords": ["america", "europe"], "locale": "en"}
        )"_json;

    req->params["collection"] = "coll1";
    req->params["name"] = "continents";
    req->body = stopword_value.dump();

    result = put_upsert_stopword(req, res);
    if(!result) {
        LOG(ERROR) << res->body;
        FAIL();
    }

    req->params["q"] = "America Man";
    req->params["query_by"] = "title";
    req->params["stopwords"] = "continents";

    search_op = collectionManager.do_search(req->params, embedded_params, json_results, now_ts);
    ASSERT_TRUE(search_op.ok());
    results = nlohmann::json::parse(json_results);
    ASSERT_EQ(0, results["hits"].size());

    req->params.clear();
    json_results.clear();

    req->params["collection"] = "coll1";
    req->params["q"] = "a deadman";
    req->params["query_by"] = "title";
    req->params["stopwords"] = "articles";

    search_op = collectionManager.do_search(req->params, embedded_params, json_results, now_ts);
    ASSERT_TRUE(search_op.ok());
    results = nlohmann::json::parse(json_results);
    ASSERT_EQ(2, results["hits"].size());

    req->params.clear();
    json_results.clear();

    //try deteting nonexisting stopword
    req->params["collection"] = "coll1";
    req->params["name"] = "state";

    result = del_stopword(req, res);
    ASSERT_EQ(404, res->status_code);
    ASSERT_STREQ("{\"message\": \"Stopword `state` not found.\"}", res->body.c_str());

    req->params.clear();
    json_results.clear();

    //detete stopword and apply in search
    req->params["collection"] = "coll1";
    req->params["name"] = "continents";

    result = del_stopword(req, res);
    if(!result) {
        LOG(ERROR) << res->body;
        FAIL();
    }

    req->params["collection"] = "coll1";
    req->params["q"] = "America";
    req->params["query_by"] = "title";
    req->params["stopwords"] = "continents";

    search_op = collectionManager.do_search(req->params, embedded_params, json_results, now_ts);
    ASSERT_TRUE(search_op.ok());
    results = nlohmann::json::parse(json_results);
    ASSERT_EQ(1, results["hits"].size());

    collectionManager.drop_collection("coll1");
}

TEST_F(StopwordsManagerTest, StopwordsValidation) {
    nlohmann::json schema = R"({
            "name": "coll1",
            "fields": [
              {"name": "title", "type": "string" },
              {"name": "points", "type": "int32" }
            ]
        })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    //with a typo
    auto stopword_value = R"(
            {"stopword": ["america", "europe"], "locale": "en"}
        )"_json;

    req->params["collection"] = "coll1";
    req->params["name"] = "continents";
    req->body = stopword_value.dump();

    auto result = put_upsert_stopword(req, res);
    ASSERT_EQ(400, res->status_code);
    ASSERT_STREQ("{\"message\": \"Parameter `stopwords` is required\"}", res->body.c_str());

    //check for value types
    stopword_value = R"(
            {"stopwords": ["america", "europe"], "locale": 12}
        )"_json;

    req->params["collection"] = "coll1";
    req->params["name"] = "continents";
    req->body = stopword_value.dump();

    result = put_upsert_stopword(req, res);
    ASSERT_EQ(400, res->status_code);
    ASSERT_STREQ("{\"message\": \"Parameter `locale` is required as string value\"}", res->body.c_str());

    stopword_value = R"(
            {"stopwords": [1, 5, 2], "locale": "ko"}
        )"_json;

    req->params["collection"] = "coll1";
    req->params["name"] = "continents";
    req->body = stopword_value.dump();

    result = put_upsert_stopword(req, res);
    ASSERT_EQ(400, res->status_code);
    ASSERT_STREQ("{\"message\": \"Parameter `stopwords` is required as string array value\"}", res->body.c_str());

    collectionManager.drop_collection("coll1");
}

TEST_F(StopwordsManagerTest, ReloadStopwordsOnRestart) {
    nlohmann::json schema = R"({
        "name": "coll1",
        "fields": [
          {"name": "title", "type": "string" },
          {"name": "points", "type": "int32" }
        ]
    })"_json;

    auto op = collectionManager.create_collection(schema);
    ASSERT_TRUE(op.ok());
    Collection *coll1 = op.get();

    auto stopword_value = R"(
        {"stopwords": ["Pop", "Indie", "Rock", "Metal", "Folk"], "locale": "en"}
    )"_json;

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    req->params["collection"] = "coll1";
    req->params["name"] = "genre";
    req->body = stopword_value.dump();

    auto result = put_upsert_stopword(req, res);
    if(!result) {
        LOG(ERROR) << res->body;
        FAIL();
    }

    auto stopword_config = stopwordsManager.get_stopwords();
    ASSERT_TRUE(stopword_config.find("genre") != stopword_config.end());

    ASSERT_EQ(5, stopword_config["genre"].size());
    ASSERT_TRUE(stopword_config["genre"].find("pop") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("indie") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("rock") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("metal") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("folk") != stopword_config["genre"].end());

    //dispose collection manager and reload all stopwords
    collectionManager.dispose();
    stopwordsManager.dispose();
    delete store;
    stopword_config.clear();

    std::string state_dir_path = "/tmp/typesense_test/stopwords_manager";
    store = new Store(state_dir_path);

    stopwordsManager.init(store);
    collectionManager.init(store, 1.0, "auth_key", quit);
    collectionManager.load(8, 1000);

    stopword_config = stopwordsManager.get_stopwords();
    ASSERT_TRUE(stopword_config.find("genre") != stopword_config.end());

    ASSERT_EQ(5, stopword_config["genre"].size());
    ASSERT_TRUE(stopword_config["genre"].find("pop") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("indie") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("rock") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("metal") != stopword_config["genre"].end());
    ASSERT_TRUE(stopword_config["genre"].find("folk") != stopword_config["genre"].end());
}
