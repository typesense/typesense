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

static bool contains_set_name(const nlohmann::json& sets_json, const std::string& name) {
    for(const auto& item : sets_json) {
        if(item.contains("name") && item["name"].is_string() && item["name"].get<std::string>() == name) {
            return true;
        }
    }
    return false;
}

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

    nlohmann::json not_array = nlohmann::json::object();
    auto op = mgr.upsert_override_set("testset", not_array);
    ASSERT_FALSE(op.ok());
    ASSERT_EQ(400, op.code());
    ASSERT_EQ(std::string("Invalid 'items' field; must be an array"), op.error());
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

    // pagination with offset
    auto list_offset = mgr.list_override_items("testset", 1, 1);
    ASSERT_TRUE(list_offset.ok()) << list_offset.error();
    ASSERT_EQ(1, list_offset.get().size());

    // invalid offset
    auto list_bad = mgr.list_override_items("testset", 1, 5);
    ASSERT_FALSE(list_bad.ok());
    ASSERT_EQ(400, list_bad.code());
    ASSERT_EQ(std::string("Invalid offset param."), list_bad.error());

    auto list_missing = mgr.list_override_items("does-not-exist", 0, 0);
    ASSERT_FALSE(list_missing.ok());
    ASSERT_EQ(404, list_missing.code());
    ASSERT_EQ(std::string("Override index not found"), list_missing.error());
}

TEST_F(OverrideIndexManagerTest, BasicSetItemOperations) {
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

    // deleting non-existent item
    auto del_missing = mgr.delete_override_item("testset", "does-not-exist");
    ASSERT_FALSE(del_missing.ok());
    ASSERT_EQ(404, del_missing.code());
    ASSERT_EQ(std::string("Could not find that `id`."), del_missing.error());
}


TEST_F(OverrideIndexManagerTest, ValidateOverrideIndex) {
    // not an object
    auto op1 = OverrideIndexManager::validate_override_index(nlohmann::json::array());
    ASSERT_FALSE(op1.ok());
    ASSERT_EQ(400, op1.code());
    ASSERT_EQ(std::string("Invalid override index format"), op1.error());

    // missing name
    auto op2 = OverrideIndexManager::validate_override_index(nlohmann::json{{"items", nlohmann::json::array()}});
    ASSERT_FALSE(op2.ok());
    ASSERT_EQ(400, op2.code());
    ASSERT_EQ(std::string("Missing or invalid 'name' field"), op2.error());

    // invalid name type
    auto op3 = OverrideIndexManager::validate_override_index(nlohmann::json{{"name", 123}, {"items", nlohmann::json::array()}});
    ASSERT_FALSE(op3.ok());
    ASSERT_EQ(400, op3.code());
    ASSERT_EQ(std::string("Missing or invalid 'name' field"), op3.error());

    // missing items
    auto op4 = OverrideIndexManager::validate_override_index(nlohmann::json{{"name", "s"}});
    ASSERT_FALSE(op4.ok());
    ASSERT_EQ(400, op4.code());
    ASSERT_EQ(std::string("Missing or invalid 'items' field"), op4.error());

    // items not array
    auto op5 = OverrideIndexManager::validate_override_index(nlohmann::json{{"name", "s"}, {"items", {}}});
    ASSERT_FALSE(op5.ok());
    ASSERT_EQ(400, op5.code());
    ASSERT_EQ(std::string("Missing or invalid 'items' field"), op5.error());

    // invalid item: missing rule
    nlohmann::json bad_items1 = nlohmann::json::array({ nlohmann::json{{"id", "x"}} });
    auto op6 = OverrideIndexManager::validate_override_index(nlohmann::json{{"name", "s"}, {"items", bad_items1}});
    ASSERT_FALSE(op6.ok());
    ASSERT_EQ(400, op6.code());
    ASSERT_EQ(std::string("Missing `rule` definition."), op6.error());

    // invalid item: rule missing triggers
    nlohmann::json bad_items2 = nlohmann::json::array({ nlohmann::json{{"id", "x"}, {"rule", nlohmann::json::object()}, {"includes", nlohmann::json::array({nlohmann::json{{"id","1"},{"position",1}}})}} });
    auto op7 = OverrideIndexManager::validate_override_index(nlohmann::json{{"name", "s"}, {"items", bad_items2}});
    ASSERT_FALSE(op7.ok());
    ASSERT_EQ(400, op7.code());
    ASSERT_EQ(std::string("The `rule` definition must contain either a `tags` or a `query` and `match`."), op7.error());

    // invalid item: includes wrong type
    nlohmann::json bad_items3 = nlohmann::json::array({ nlohmann::json{{"id", "x"}, {"rule", {{"query","q"},{"match", override_t::MATCH_EXACT}}}, {"includes", "bad"}} });
    auto op8 = OverrideIndexManager::validate_override_index(nlohmann::json{{"name", "s"}, {"items", bad_items3}});
    ASSERT_FALSE(op8.ok());
    ASSERT_EQ(400, op8.code());
    ASSERT_EQ(std::string("The `includes` value must be an array."), op8.error());

    nlohmann::json items = nlohmann::json::array({
        nlohmann::json{{"id","ok-1"}, {"rule", {{"query","hello"},{"match", override_t::MATCH_EXACT}}}, {"includes", nlohmann::json::array({ nlohmann::json{{"id","1"},{"position",1}} })}}
    });
    auto op = OverrideIndexManager::validate_override_index(nlohmann::json{{"name","testset"},{"items", items}});
    ASSERT_TRUE(op.ok()) << op.error();
}

TEST_F(OverrideIndexManagerTest, BasicSetOperations) {
    nlohmann::json items1 = nlohmann::json::array({ nlohmann::json{{"id","ov-a"}, {"rule", {{"query","foo"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","1"},{"position",1}}}}} });
    nlohmann::json items2 = nlohmann::json::array({ nlohmann::json{{"id","ov-b"}, {"rule", {{"query","bar"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","2"},{"position",1}}}}} });

    ASSERT_TRUE(mgr.upsert_override_set("testset", items1).ok());
    ASSERT_TRUE(mgr.upsert_override_set("testset2", items2).ok());

    auto all_sets = mgr.get_all_override_indices_json();
    ASSERT_EQ(2, all_sets.size());
    ASSERT_TRUE(contains_set_name(all_sets, "testset"));
    ASSERT_TRUE(contains_set_name(all_sets, "testset2"));

    auto rem_op = mgr.remove_override_index("testset");
    ASSERT_TRUE(rem_op.ok()) << rem_op.error();

    auto all_sets_after = mgr.get_all_override_indices_json();
    ASSERT_EQ(1, all_sets_after.size());
    ASSERT_FALSE(contains_set_name(all_sets_after, "testset"));
    ASSERT_TRUE(contains_set_name(all_sets_after, "testset2"));

    auto rem_missing = mgr.remove_override_index("does-not-exist");
    ASSERT_FALSE(rem_missing.ok());
    ASSERT_EQ(404, rem_missing.code());
    ASSERT_EQ(std::string("Override index not found"), rem_missing.error());
}

TEST_F(OverrideIndexManagerTest, UpsertSet) {
    nlohmann::json items1 = nlohmann::json::array({
        nlohmann::json{{"id","ov-a"}, {"rule", {{"query","foo"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","1"},{"position",1}}}}},
        nlohmann::json{{"id","ov-b"}, {"rule", {{"query","bar"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","2"},{"position",1}}}}}
    });
    ASSERT_TRUE(mgr.upsert_override_set("testset", items1).ok());
    auto list1 = mgr.list_override_items("testset", 0, 0);
    ASSERT_TRUE(list1.ok());
    ASSERT_EQ(2, list1.get().size());

    nlohmann::json items2 = nlohmann::json::array({
        nlohmann::json{{"id","ov-c"}, {"rule", {{"query","baz"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","3"},{"position",1}}}}}
    });
    ASSERT_TRUE(mgr.upsert_override_set("testset", items2).ok());
    auto list2 = mgr.list_override_items("testset", 0, 0);
    ASSERT_TRUE(list2.ok());
    ASSERT_EQ(1, list2.get().size());
    ASSERT_EQ("ov-c", list2.get()[0]["id"].get<std::string>());
}

TEST_F(OverrideIndexManagerTest, ListSetItems) {
    nlohmann::json items = nlohmann::json::array({
        nlohmann::json{{"id","ov-a"}, {"rule", {{"query","a"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","1"},{"position",1}}}}},
        nlohmann::json{{"id","ov-b"}, {"rule", {{"query","b"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","2"},{"position",1}}}}},
        nlohmann::json{{"id","ov-c"}, {"rule", {{"query","c"},{"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id","3"},{"position",1}}}}}
    });
    ASSERT_TRUE(mgr.upsert_override_set("testset", items).ok());

    auto list_mid = mgr.list_override_items("testset", 2, 1);
    ASSERT_TRUE(list_mid.ok()) << list_mid.error();
    ASSERT_EQ(2, list_mid.get().size());

    auto list_bad_offset = mgr.list_override_items("testset", 0, 10);
    ASSERT_FALSE(list_bad_offset.ok());
    ASSERT_EQ(400, list_bad_offset.code());
    ASSERT_EQ(std::string("Invalid offset param."), list_bad_offset.error());

    auto list_missing_set = mgr.list_override_items("missing", 0, 0);
    ASSERT_FALSE(list_missing_set.ok());
    ASSERT_EQ(404, list_missing_set.code());
    ASSERT_EQ(std::string("Override index not found"), list_missing_set.error());
}

TEST_F(OverrideIndexManagerTest, UpsertSetItem) {
    ASSERT_TRUE(mgr.upsert_override_set("testset", nlohmann::json::array()).ok());

    // missing id
    nlohmann::json bad_item = {
        {"rule", {{"query", "q"}, {"match", override_t::MATCH_EXACT}}},
        {"includes", {{{"id", "1"}, {"position", 1}}}}
    };
    auto up_bad = mgr.upsert_override_item("testset", bad_item);
    ASSERT_FALSE(up_bad.ok());
    ASSERT_EQ(400, up_bad.code());
    ASSERT_EQ(std::string("Override `id` not provided."), up_bad.error());

    // set not found
    nlohmann::json good_item = {
        {"id", "ok"},
        {"rule", {{"query", "q"}, {"match", override_t::MATCH_EXACT}}},
        {"includes", {{{"id", "1"}, {"position", 1}}}}
    };
    auto up_nf = mgr.upsert_override_item("missing", good_item);
    ASSERT_FALSE(up_nf.ok());
    ASSERT_EQ(404, up_nf.code());
    ASSERT_EQ(std::string("Override index not found"), up_nf.error());


    nlohmann::json item1 = {
        {"id", "same"},
        {"rule", {{"query", "x"}, {"match", override_t::MATCH_EXACT}}},
        {"includes", {{{"id", "1"}, {"position", 1}}}}
    };
    ASSERT_TRUE(mgr.upsert_override_item("testset", item1).ok());

    nlohmann::json item2 = {
        {"id", "same"},
        {"rule", {{"query", "x"}, {"match", override_t::MATCH_EXACT}}},
        {"includes", {{{"id", "1"}, {"position", 2}}}}
    };
    ASSERT_TRUE(mgr.upsert_override_item("testset", item2).ok());

    auto get_item = mgr.get_override_item("testset", "same");
    ASSERT_TRUE(get_item.ok()) << get_item.error();
    // Ensure the position was updated to 2
    ASSERT_TRUE(get_item.get().contains("includes"));
    ASSERT_EQ(1, get_item.get()["includes"].size());
    ASSERT_EQ(2, get_item.get()["includes"][0]["position"].get<int>());
}

TEST_F(OverrideIndexManagerTest, GetSetItem) {
    ASSERT_TRUE(mgr.upsert_override_set("testset", nlohmann::json::array()).ok());

    auto nf1 = mgr.get_override_item("testset", "absent");
    ASSERT_FALSE(nf1.ok());
    ASSERT_EQ(404, nf1.code());
    ASSERT_EQ(std::string("Not Found"), nf1.error());

    auto nf2 = mgr.get_override_item("missing", "anything");
    ASSERT_FALSE(nf2.ok());
    ASSERT_EQ(404, nf2.code());
    ASSERT_EQ(std::string("Override index not found"), nf2.error());
}
