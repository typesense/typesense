#include <gtest/gtest.h>
#include "include/sparsepp.h"
#include "include/stopwords_manager.h"
#include "include/store.h"

class StopwordsManagerTest : public ::testing::Test {
protected:
    Store *store;

    virtual void SetUp() {
        std::string state_dir_path = "/tmp/typesense_test/stopwords_manager";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
        store = new Store(state_dir_path);
    }

    virtual void TearDown() {
        delete store;
    }
};

TEST_F(StopwordsManagerTest, UpsertGetStopwords) {
    StopwordsManager stopwordsManager;
    stopwordsManager.init(store);

    auto stopwords1 = R"(
            {"stopwords": ["america", "europe"]}
        )"_json;

    auto upsert_op = stopwordsManager.upsert_stopword("continents", stopwords1["stopwords"], "en");
    ASSERT_TRUE(upsert_op.ok());

    auto stopwords2 = R"(
                {"stopwords": ["a", "an", "the"]}
            )"_json;

    upsert_op = stopwordsManager.upsert_stopword("articles", stopwords2["stopwords"], "en");
    ASSERT_TRUE(upsert_op.ok());

    auto stopwords3 = R"(
                {"stopwords": ["India", "United States", "Japan", "China"]}
            )"_json;

    upsert_op = stopwordsManager.upsert_stopword("countries", stopwords3["stopwords"], "en");
    ASSERT_TRUE(upsert_op.ok());

    auto stopword_config = stopwordsManager.get_stopwords();
    ASSERT_EQ(3, stopword_config.size()); //total stopwords set
    ASSERT_EQ(3, stopword_config["articles"].size());
    ASSERT_EQ(2, stopword_config["continents"].size());
    ASSERT_EQ(5, stopword_config["countries"].size()); //with tokenization United States will be splited into two
}

TEST_F(StopwordsManagerTest, GetStopword) {
    StopwordsManager stopwordsManager;
    stopwordsManager.init(store);

    auto stopwords = R"({"stopwords": ["a", "an", "the"]})"_json;

    auto upsert_op = stopwordsManager.upsert_stopword("articles", stopwords["stopwords"], "en");
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
    ASSERT_EQ("stopword `country` not found.", get_op.error());

    //try fetching stopwords with token
    stopwords = R"({"stopwords": ["India", "United States", "Japan"]})"_json;

    upsert_op = stopwordsManager.upsert_stopword("country", stopwords["stopwords"], "en");
    ASSERT_TRUE(upsert_op.ok());

    get_op = stopwordsManager.get_stopword("country", stopwords_set);
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(4, stopwords_set.size()); //as United States will be tokenized and counted 2 stopwords
}

TEST_F(StopwordsManagerTest, DeleteStopword) {
    StopwordsManager stopwordsManager;
    stopwordsManager.init(store);

    auto stopwords1 = R"(
                {"stopwords": ["america", "europe"]}
            )"_json;

    auto upsert_op = stopwordsManager.upsert_stopword("continents", stopwords1["stopwords"], "en");
    ASSERT_TRUE(upsert_op.ok());

    auto stopwords2 = R"(
                    {"stopwords": ["a", "an", "the"]}
                )"_json;

    upsert_op = stopwordsManager.upsert_stopword("articles", stopwords2["stopwords"], "en");
    ASSERT_TRUE(upsert_op.ok());

    spp::sparse_hash_set<std::string> stopwords_set;

    //delete a stopword
    auto del_op = stopwordsManager.delete_stopword("articles");
    ASSERT_TRUE(del_op.ok());

    auto get_op = stopwordsManager.get_stopword("articles", stopwords_set);
    ASSERT_FALSE(get_op.ok());
    ASSERT_EQ(404, get_op.code());
    ASSERT_EQ("stopword `articles` not found.", get_op.error());

    //delete non-existing stopword
    del_op = stopwordsManager.delete_stopword("states");
    ASSERT_FALSE(del_op.ok());
    ASSERT_EQ(404, del_op.code());
    ASSERT_EQ("Stopword `states` not found.", del_op.error());
}