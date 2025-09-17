#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <algorithm>
#include "override_index_manager.h"
#include "store.h"

class OverrideIndexManagerTest : public ::testing::Test {
protected:
    Store* store = nullptr;
    OverrideIndexManager& mgr = OverrideIndexManager::get_instance();

    virtual void SetUp() {
        std::string state_dir_path = "/tmp/typesense_test/override_index_manager";
        system(("rm -rf " + state_dir_path + " && mkdir -p " + state_dir_path).c_str());
        store = new Store(state_dir_path);
        mgr.init_store(store);
    }

    virtual void TearDown() {
        mgr.remove_override_index("testset");
        mgr.remove_override_index("testset2");
        delete store;
    }
};

TEST_F(OverrideIndexManagerTest, UpsertOverrideSet) {
    nlohmann::json items = nlohmann::json::array();
    items.push_back(nlohmann::json{
        {"id", "ov-a"},
        {"rule", {{"query", "foo"}, {"match", override_t::MATCH_EXACT}}},
        {"includes", {{{"id", "1"}, {"position", 1}}}}
    });

    auto upsert_op = mgr.upsert_override_set("testset", items);
    ASSERT_TRUE(upsert_op.ok()) << upsert_op.error();
    auto created_json = upsert_op.get();
    ASSERT_TRUE(created_json.contains("items"));
    ASSERT_EQ(1, created_json["items"].size());
}

TEST_F(OverrideIndexManagerTest, ListOverrideItems) {
    nlohmann::json items = nlohmann::json::array({
        nlohmann::json{{"id", "ov-a"}, {"rule", {{"query", "foo"}, {"match", override_t::MATCH_EXACT}}}, {"excludes", {{{"id", "1"}}}}},
        nlohmann::json{{"id", "ov-b"}, {"rule", {{"query", "bar"}, {"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id", "2"}, {"position", 1}}}}}
    });
    ASSERT_TRUE(mgr.upsert_override_set("testset", items).ok());

    auto list_all = mgr.list_override_items("testset", 0, 0);
    ASSERT_TRUE(list_all.ok()) << list_all.error();
    ASSERT_EQ(2, list_all.get().size());

    auto list_limited = mgr.list_override_items("testset", 1, 0);
    ASSERT_TRUE(list_limited.ok()) << list_limited.error();
    ASSERT_EQ(1, list_limited.get().size());
}

TEST_F(OverrideIndexManagerTest, GetUpsertDeleteOverrideItem) {
    ASSERT_TRUE(mgr.upsert_override_set("testset", nlohmann::json::array()).ok());

    nlohmann::json new_item = {
        {"id", "ov-x"},
        {"rule", {{"query", "baz"}, {"match", override_t::MATCH_CONTAINS}}},
        {"includes", {{{"id", "5"}, {"position", 1}}}}
    };
    auto upsert_item = mgr.upsert_override_item("testset", new_item);
    ASSERT_TRUE(upsert_item.ok()) << upsert_item.error();

    auto get_new = mgr.get_override_item("testset", "ov-x");
    ASSERT_TRUE(get_new.ok()) << get_new.error();
    ASSERT_EQ("ov-x", get_new.get()["id"].get<std::string>());

    auto del_item = mgr.delete_override_item("testset", "ov-x");
    ASSERT_TRUE(del_item.ok()) << del_item.error();
}


