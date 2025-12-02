#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <algorithm>
#include "synonym_index_manager.h"
#include "store.h"

class SynonymIndexManagerTest : public ::testing::Test {
protected:
    Store* store = nullptr;
    SynonymIndexManager& mgr = SynonymIndexManager::get_instance();

    virtual void SetUp() {
        std::string state_dir_path = "/tmp/typesense_test/synonym_index_manager";
        system(("rm -rf " + state_dir_path + " && mkdir -p " + state_dir_path).c_str());
        store = new Store(state_dir_path);
        mgr.init_store(store);
    }

    virtual void TearDown() {
        mgr.remove_synonym_index("testset");
        mgr.remove_synonym_index("testset2");
        delete store;
    }
};

TEST_F(SynonymIndexManagerTest, UpsertSynonymSet) {
    nlohmann::json items = nlohmann::json::array();
    items.push_back(nlohmann::json{{"id", "syn-usa"}, {"root", "usa"}, {"synonyms", {"united states", "united states of america"}}});
    items.push_back(nlohmann::json{{"id", "syn-laptop"}, {"root", "laptop"}, {"synonyms", {"notebook", "ultrabook"}}});

    auto upsert_op = mgr.upsert_synonym_set("testset", items);
    ASSERT_TRUE(upsert_op.ok()) << upsert_op.error();
    auto created_json = upsert_op.get();
    ASSERT_TRUE(created_json.contains("items"));
    ASSERT_EQ(2, created_json["items"].size());
}

TEST_F(SynonymIndexManagerTest, ListSynonymItems) {
    nlohmann::json items = nlohmann::json::array({
        nlohmann::json{{"id", "a"}, {"root", "tv"}, {"synonyms", {"television"}}},
        nlohmann::json{{"id", "b"}, {"root", "laptop"}, {"synonyms", {"notebook"}}}
    });
    ASSERT_TRUE(mgr.upsert_synonym_set("testset", items).ok());

    auto list_all = mgr.list_synonym_items("testset", 0, 0);
    ASSERT_TRUE(list_all.ok()) << list_all.error();
    ASSERT_EQ(2, list_all.get().size());

    auto list_limited = mgr.list_synonym_items("testset", 1, 0);
    ASSERT_TRUE(list_limited.ok()) << list_limited.error();
    ASSERT_EQ(1, list_limited.get().size());

    auto list_from_offset = mgr.list_synonym_items("testset", 1, 1);
    ASSERT_TRUE(list_from_offset.ok()) << list_from_offset.error();
    ASSERT_EQ(1, list_from_offset.get().size());

    auto list_bad_offset = mgr.list_synonym_items("testset", 0, 5);
    ASSERT_FALSE(list_bad_offset.ok());
    ASSERT_EQ(400, list_bad_offset.code());
}

TEST_F(SynonymIndexManagerTest, GetSynonymItem) {
    nlohmann::json items = nlohmann::json::array({
        nlohmann::json{{"id", "syn-tv"}, {"root", "tv"}, {"synonyms", {"television", "smart tv"}}}
    });
    ASSERT_TRUE(mgr.upsert_synonym_set("testset", items).ok());

    auto get_item = mgr.get_synonym_item("testset", "syn-tv");
    ASSERT_TRUE(get_item.ok()) << get_item.error();
    ASSERT_EQ("syn-tv", get_item.get()["id"].get<std::string>());

    auto not_found = mgr.get_synonym_item("testset", "does-not-exist");
    ASSERT_FALSE(not_found.ok());
    ASSERT_EQ(404, not_found.code());
}

TEST_F(SynonymIndexManagerTest, UpsertSynonymItem) {
    ASSERT_TRUE(mgr.upsert_synonym_set("testset", nlohmann::json::array()).ok());

    nlohmann::json new_item = {{"id", "syn-phone"}, {"root", "phone"}, {"synonyms", {"cellphone", "mobile"}}};
    auto upsert_item = mgr.upsert_synonym_item("testset", new_item);
    ASSERT_TRUE(upsert_item.ok()) << upsert_item.error();

    auto get_new = mgr.get_synonym_item("testset", "syn-phone");
    ASSERT_TRUE(get_new.ok()) << get_new.error();
    ASSERT_EQ("syn-phone", get_new.get()["id"].get<std::string>());

    nlohmann::json updated_item = {{"id", "syn-phone"}, {"root", "smartphone"}, {"synonyms", {"cell", "mobile"}}};
    auto upsert_item_again = mgr.upsert_synonym_item("testset", updated_item);
    ASSERT_TRUE(upsert_item_again.ok()) << upsert_item_again.error();

    auto get_updated = mgr.get_synonym_item("testset", "syn-phone");
    ASSERT_TRUE(get_updated.ok());
    ASSERT_EQ("syn-phone", get_updated.get()["id"].get<std::string>());
}

TEST_F(SynonymIndexManagerTest, DeleteSynonymItem) {
    nlohmann::json items = nlohmann::json::array({
        nlohmann::json{{"id", "syn-phone"}, {"root", "phone"}, {"synonyms", {"cellphone", "mobile"}}}
    });
    ASSERT_TRUE(mgr.upsert_synonym_set("testset", items).ok());

    auto del_item = mgr.delete_synonym_item("testset", "syn-phone");
    ASSERT_TRUE(del_item.ok()) << del_item.error();

    auto get_deleted = mgr.get_synonym_item("testset", "syn-phone");
    ASSERT_FALSE(get_deleted.ok());
    ASSERT_EQ(404, get_deleted.code());
}
