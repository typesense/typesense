#include <gtest/gtest.h>
#include <stdlib.h>
#include <iostream>
#include <http_data.h>
#include "auth_manager.h"
#include "core_api.h"
#include <collection_manager.h>

static const size_t FUTURE_TS = 64723363199;

class AuthManagerTest : public ::testing::Test {
protected:
    Store *store;
    AuthManager auth_manager;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/auth_manager_test_db";
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        auth_manager.init(store, "bootstrap-key");
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        delete store;
    }
};

TEST_F(AuthManagerTest, CreateListDeleteAPIKeys) {
    auto list_op = auth_manager.list_keys();
    ASSERT_TRUE(list_op.ok());
    ASSERT_EQ(0, list_op.get().size());

    auto get_op = auth_manager.get_key(0);
    ASSERT_FALSE(get_op.ok());
    ASSERT_EQ(404, get_op.code());

    // test inserts

    api_key_t api_key1("abcd1", "test key 1", {"read", "write"}, {"collection1", "collection2"}, FUTURE_TS);
    api_key_t api_key2("abcd2", "test key 2", {"admin"}, {"*"}, FUTURE_TS);

    ASSERT_EQ("abcd1", api_key1.value);
    ASSERT_EQ("abcd2", api_key2.value);

    auto insert_op = auth_manager.create_key(api_key1);
    ASSERT_TRUE(insert_op.ok());
    ASSERT_EQ(5, insert_op.get().value.size());

    insert_op = auth_manager.create_key(api_key2);
    ASSERT_TRUE(insert_op.ok());
    ASSERT_EQ(5, insert_op.get().value.size());

    // reject on conflict
    insert_op = auth_manager.create_key(api_key2);
    ASSERT_FALSE(insert_op.ok());
    ASSERT_EQ(409, insert_op.code());
    ASSERT_EQ("API key generation conflict.", insert_op.error());

    api_key2.value = "bootstrap-key";
    insert_op = auth_manager.create_key(api_key2);
    ASSERT_FALSE(insert_op.ok());
    ASSERT_EQ(409, insert_op.code());
    ASSERT_EQ("API key generation conflict.", insert_op.error());

    // get an individual key

    get_op = auth_manager.get_key(0);
    ASSERT_TRUE(get_op.ok());
    const api_key_t &key1 = get_op.get();
    ASSERT_EQ(4, key1.value.size());
    ASSERT_EQ("test key 1", key1.description);
    ASSERT_EQ(2, key1.actions.size());
    EXPECT_STREQ("read", key1.actions[0].c_str());
    EXPECT_STREQ("write", key1.actions[1].c_str());
    ASSERT_EQ(2, key1.collections.size());
    EXPECT_STREQ("collection1", key1.collections[0].c_str());
    EXPECT_STREQ("collection2", key1.collections[1].c_str());

    get_op = auth_manager.get_key(1);
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(4, get_op.get().value.size());
    ASSERT_EQ("test key 2", get_op.get().description);

    get_op = auth_manager.get_key(1, false);
    ASSERT_TRUE(get_op.ok());
    ASSERT_NE(4, get_op.get().value.size());

    get_op = auth_manager.get_key(2, false);
    ASSERT_FALSE(get_op.ok());

    // listing keys
    list_op = auth_manager.list_keys();
    ASSERT_TRUE(list_op.ok());
    ASSERT_EQ(2, list_op.get().size());
    ASSERT_EQ("test key 1", list_op.get()[0].description);
    ASSERT_EQ("abcd", list_op.get()[0].value);
    ASSERT_EQ("test key 2", list_op.get()[1].description);
    ASSERT_EQ("abcd", list_op.get()[1].value);

    // delete key
    auto del_op = auth_manager.remove_key(1);
    ASSERT_TRUE(del_op.ok());

    del_op = auth_manager.remove_key(1000);
    ASSERT_FALSE(del_op.ok());
    ASSERT_EQ(404, del_op.code());
}

TEST_F(AuthManagerTest, CheckRestoreOfAPIKeys) {
    api_key_t api_key1("abcd1", "test key 1", {"read", "write"}, {"collection1", "collection2"}, FUTURE_TS);
    api_key_t api_key2("abcd2", "test key 2", {"admin"}, {"*"}, FUTURE_TS);

    std::string key_value1 = auth_manager.create_key(api_key1).get().value;
    std::string key_value2 = auth_manager.create_key(api_key2).get().value;

    AuthManager auth_manager2;
    auth_manager2.init(store, "bootstrap-key");

    // list keys

    auto list_op = auth_manager.list_keys();
    ASSERT_TRUE(list_op.ok());
    ASSERT_EQ(2, list_op.get().size());
    ASSERT_EQ("test key 1", list_op.get()[0].description);
    ASSERT_EQ("abcd", list_op.get()[0].value);
    ASSERT_STREQ(key_value1.substr(0, 4).c_str(), list_op.get()[0].value.c_str());
    ASSERT_EQ(FUTURE_TS, list_op.get()[0].expires_at);

    ASSERT_EQ("test key 2", list_op.get()[1].description);
    ASSERT_EQ("abcd", list_op.get()[1].value);
    ASSERT_STREQ(key_value2.substr(0, 4).c_str(), list_op.get()[1].value.c_str());
    ASSERT_EQ(FUTURE_TS, list_op.get()[1].expires_at);
}

TEST_F(AuthManagerTest, VerifyAuthentication) {
    std::map<std::string, std::string> sparams;
    std::vector<nlohmann::json> embedded_params(2);
    // when no keys are present at all
    ASSERT_FALSE(auth_manager.authenticate("", {collection_key_t("", "jdlaslasdasd")}, sparams, embedded_params));

    // wildcard permission
    api_key_t wildcard_all_key = api_key_t("abcd1", "wildcard all key", {"*"}, {"*"}, FUTURE_TS);
    auth_manager.create_key(wildcard_all_key);

    ASSERT_FALSE(auth_manager.authenticate("documents:create", {collection_key_t("collection1", "jdlaslasdasd")}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("metrics:get", {collection_key_t("", wildcard_all_key.value)}, sparams, embedded_params));

    // long API key
    std::string long_api_key_str = StringUtils::randstring(50);
    api_key_t long_api_key = api_key_t(long_api_key_str, "long api key", {"*"}, {"*"}, FUTURE_TS);
    auth_manager.create_key(long_api_key);

    ASSERT_TRUE(auth_manager.authenticate("metrics:get", {collection_key_t(long_api_key_str, wildcard_all_key.value)}, sparams, embedded_params));

    // wildcard on a collection
    api_key_t wildcard_coll_key = api_key_t("abcd2", "wildcard coll key", {"*"}, {"collection1"}, FUTURE_TS);
    auth_manager.create_key(wildcard_coll_key);

    ASSERT_FALSE(auth_manager.authenticate("documents:create", {collection_key_t("collection1", "adasda")}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("documents:get", {collection_key_t("collection1", wildcard_coll_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:get", {collection_key_t("collection2", wildcard_coll_key.value)}, sparams, embedded_params));

    // wildcard on multiple collections
    api_key_t wildcard_colls_key = api_key_t("abcd3", "wildcard coll key", {"*"}, {"collection1", "collection2", "collection3"}, FUTURE_TS);
    auth_manager.create_key(wildcard_colls_key);

    ASSERT_TRUE(auth_manager.authenticate("documents:get", {collection_key_t("collection1", wildcard_colls_key.value)}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("documents:search", {collection_key_t("collection2", wildcard_colls_key.value)}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("documents:create", {collection_key_t("collection3", wildcard_colls_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:get", {collection_key_t("collection4", wildcard_colls_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:get", {collection_key_t("*", wildcard_colls_key.value)}, sparams, embedded_params));

    // only 1 action on multiple collections
    api_key_t one_action_key = api_key_t("abcd4", "one action key", {"documents:search"}, {"collection1", "collection2"}, FUTURE_TS);
    auth_manager.create_key(one_action_key);

    ASSERT_TRUE(auth_manager.authenticate("documents:search", {collection_key_t("collection1", one_action_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:get", {collection_key_t("collection2", one_action_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("collection5", one_action_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("*", {collection_key_t("collection2", one_action_key.value)}, sparams, embedded_params));

    // multiple actions on multiple collections
    api_key_t mul_acoll_key = api_key_t("abcd5", "multiple action/collection key",
                                        {"documents:get", "collections:list"}, {"metacollection", "collection2"}, FUTURE_TS);
    auth_manager.create_key(mul_acoll_key);

    ASSERT_TRUE(auth_manager.authenticate("documents:get", {collection_key_t("metacollection", mul_acoll_key.value)}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("collections:list", {collection_key_t("collection2", mul_acoll_key.value)}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("collections:list", {collection_key_t("metacollection", mul_acoll_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("collection2", mul_acoll_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:get", {collection_key_t("collection5", mul_acoll_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("*", {collection_key_t("*", mul_acoll_key.value)}, sparams, embedded_params));

    // regexp match

    api_key_t regexp_colls_key1 = api_key_t("abcd6", "regexp coll key", {"*"}, {"coll.*"}, FUTURE_TS);
    auth_manager.create_key(regexp_colls_key1);
    ASSERT_TRUE(auth_manager.authenticate("collections:list", {collection_key_t("collection2", regexp_colls_key1.value)}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("documents:get", {collection_key_t("collection5", regexp_colls_key1.value)}, sparams, embedded_params));

    api_key_t regexp_colls_key2 = api_key_t("abcd7", "regexp coll key", {"*"}, {".*meta.*"}, FUTURE_TS);
    auth_manager.create_key(regexp_colls_key2);
    ASSERT_TRUE(auth_manager.authenticate("collections:list", {collection_key_t("metacollection", regexp_colls_key2.value)}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("collections:list", {collection_key_t("ametacollection", regexp_colls_key2.value)}, sparams, embedded_params));

    // check for expiry

    api_key_t expired_key1 = api_key_t("abcd8", "expiry key", {"*"}, {"*"}, 1606542716);
    auth_manager.create_key(expired_key1);
    ASSERT_FALSE(auth_manager.authenticate("collections:list", {collection_key_t("collection", expired_key1.value)}, sparams, embedded_params));

    api_key_t unexpired_key1 = api_key_t("abcd9", "expiry key", {"*"}, {"*"}, 2237712220);
    auth_manager.create_key(unexpired_key1);
    ASSERT_TRUE(auth_manager.authenticate("collections:list", {collection_key_t("collection", unexpired_key1.value)}, sparams, embedded_params));

    // wildcard action on any collection
    api_key_t wildcard_action_coll_key = api_key_t("abcd10", "wildcard coll action key", {"collections:*"}, {"*"}, FUTURE_TS);
    auth_manager.create_key(wildcard_action_coll_key);

    ASSERT_TRUE(auth_manager.authenticate("collections:create", {collection_key_t("collection1", wildcard_action_coll_key.value)}, sparams, embedded_params));
    ASSERT_TRUE(auth_manager.authenticate("collections:delete", {collection_key_t("collection1", wildcard_action_coll_key.value), collection_key_t("collection2", wildcard_action_coll_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:create", {collection_key_t("collection1", wildcard_action_coll_key.value)}, sparams, embedded_params));

    // create action on a specific collection
    api_key_t create_action_coll_key = api_key_t("abcd11", "create action+coll key", {"collections:create"}, {"collection1"}, FUTURE_TS);
    auth_manager.create_key(create_action_coll_key);

    ASSERT_TRUE(auth_manager.authenticate("collections:create", {collection_key_t("collection1", create_action_coll_key.value)}, sparams, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("collections:create", {collection_key_t("collection2", create_action_coll_key.value)}, sparams, embedded_params));

    // two keys against 2 different collections: both should be valid
    api_key_t coll_a_key = api_key_t("coll_a", "one action key", {"documents:search"}, {"collectionA"}, FUTURE_TS);
    api_key_t coll_b_key = api_key_t("coll_b", "one action key", {"documents:search"}, {"collectionB"}, FUTURE_TS);
    auth_manager.create_key(coll_a_key);
    auth_manager.create_key(coll_b_key);
    ASSERT_TRUE(auth_manager.authenticate("documents:search",
                                          {collection_key_t("collectionA", coll_a_key.value),
                                           collection_key_t("collectionB", coll_b_key.value)},
                                          sparams, embedded_params));

    ASSERT_FALSE(auth_manager.authenticate("documents:search",
                                          {collection_key_t("collectionA", coll_a_key.value),
                                           collection_key_t("collection1", coll_b_key.value)},
                                          sparams, embedded_params));

    ASSERT_FALSE(auth_manager.authenticate("documents:search",
                                           {collection_key_t("collection1", coll_a_key.value),
                                            collection_key_t("collectionB", coll_b_key.value)},
                                           sparams, embedded_params));

    // bad collection allow regexp
    api_key_t coll_c_key = api_key_t("coll_c", "one action key", {"documents:search"}, {"*coll_c"}, FUTURE_TS);
    auth_manager.create_key(coll_c_key);
    ASSERT_FALSE(auth_manager.authenticate("documents:search",
                                           {collection_key_t("coll_c", coll_c_key.value),},
                                           sparams, embedded_params));
}

TEST_F(AuthManagerTest, GenerationOfAPIAction) {
    route_path rpath_search = route_path("GET", {"collections", ":collection", "documents", "search"}, nullptr, false, false);
    route_path rpath_multi_search = route_path("POST", {"multi_search"}, nullptr, false, false);
    route_path rpath_coll_create = route_path("POST", {"collections"}, nullptr, false, false);
    route_path rpath_coll_get = route_path("GET", {"collections", ":collection"}, nullptr, false, false);
    route_path rpath_coll_list = route_path("GET", {"collections"}, nullptr, false, false);
    route_path rpath_coll_import = route_path("POST", {"collections", ":collection", "documents", "import"}, nullptr, false, false);
    route_path rpath_coll_export = route_path("GET", {"collections", ":collection", "documents", "export"}, nullptr, false, false);
    route_path rpath_keys_post = route_path("POST", {"keys"}, nullptr, false, false);
    route_path rpath_doc_delete = route_path("DELETE", {"collections", ":collection", "documents", ":id"}, nullptr, false, false);
    route_path rpath_override_upsert = route_path("PUT", {"collections", ":collection", "overrides", ":id"}, nullptr, false, false);
    route_path rpath_doc_patch = route_path("PATCH", {"collections", ":collection", "documents", ":id"}, nullptr, false, false);
    route_path rpath_analytics_rules_list = route_path("GET", {"analytics", "rules"}, nullptr, false, false);
    route_path rpath_analytics_rules_get = route_path("GET", {"analytics", "rules", ":id"}, nullptr, false, false);
    route_path rpath_analytics_rules_put = route_path("PUT", {"analytics", "rules", ":id"}, nullptr, false, false);
    route_path rpath_ops_cache_clear_post = route_path("POST", {"operations", "cache", "clear"}, nullptr, false, false);
    route_path rpath_conv_models_list = route_path("GET", {"conversations", "models"}, nullptr, false, false);

    ASSERT_STREQ("documents:search", rpath_search._get_action().c_str());
    ASSERT_STREQ("documents:search", rpath_multi_search._get_action().c_str());
    ASSERT_STREQ("collections:create", rpath_coll_create._get_action().c_str());
    ASSERT_STREQ("collections:get", rpath_coll_get._get_action().c_str());
    ASSERT_STREQ("documents:import", rpath_coll_import._get_action().c_str());
    ASSERT_STREQ("documents:export", rpath_coll_export._get_action().c_str());
    ASSERT_STREQ("collections:list", rpath_coll_list._get_action().c_str());
    ASSERT_STREQ("keys:create", rpath_keys_post._get_action().c_str());
    ASSERT_STREQ("documents:delete", rpath_doc_delete._get_action().c_str());
    ASSERT_STREQ("overrides:upsert", rpath_override_upsert._get_action().c_str());
    ASSERT_STREQ("documents:update", rpath_doc_patch._get_action().c_str());
    ASSERT_STREQ("analytics/rules:list", rpath_analytics_rules_list._get_action().c_str());
    ASSERT_STREQ("analytics/rules:get", rpath_analytics_rules_get._get_action().c_str());
    ASSERT_STREQ("analytics/rules:upsert", rpath_analytics_rules_put._get_action().c_str());
    ASSERT_STREQ("operations/cache/clear:create", rpath_ops_cache_clear_post._get_action().c_str());
    ASSERT_STREQ("conversations/models:list", rpath_conv_models_list._get_action().c_str());
}

TEST_F(AuthManagerTest, ScopedAPIKeys) {
    std::map<std::string, std::string> params;
    params["filter_by"] = "country:USA";
    std::vector<nlohmann::json> embedded_params(2);

    // create a API key bound to search scope and a given collection
    api_key_t key_search_coll1("KeyVal", "test key", {"documents:search"}, {"coll1"}, FUTURE_TS);
    auth_manager.create_key(key_search_coll1);

    std::string scoped_key = StringUtils::base64_encode(
      R"(IvjqWNZ5M5ElcvbMoXj45BxkQrZG4ZKEaNQoRioCx2s=KeyV{"filter_by": "user_id:1080"})"
    );

    ASSERT_TRUE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", scoped_key)}, params, embedded_params));
    ASSERT_EQ("user_id:1080", embedded_params[0]["filter_by"].get<std::string>());

    // should scope to collection bound by the parent key
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll2", scoped_key)}, params, embedded_params));

    // should scope to search action only
    ASSERT_FALSE(auth_manager.authenticate("documents:create", {collection_key_t("coll1", scoped_key)}, params, embedded_params));

    // check with corrupted key
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", "asdasasd")}, params, embedded_params));

    // with multiple collections, all should be authenticated
    ASSERT_FALSE(auth_manager.authenticate("documents:search",
                                          {collection_key_t("coll1", scoped_key),
                                           collection_key_t("coll2", scoped_key)},
                                          params, embedded_params));

    // send both regular key and scoped key
    ASSERT_TRUE(auth_manager.authenticate("documents:search",
                                           {collection_key_t("coll1", key_search_coll1.value),
                                            collection_key_t("coll1", scoped_key)},
                                           params, embedded_params));

    // when params is empty, embedded param should be set
    std::map<std::string, std::string> empty_params;
    embedded_params.clear();
    embedded_params.push_back(nlohmann::json::object());
    ASSERT_TRUE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", scoped_key)}, empty_params, embedded_params));
    ASSERT_EQ("user_id:1080", embedded_params[0]["filter_by"].get<std::string>());

    // when more than a single key prefix matches, must pick the correct underlying key
    embedded_params.clear();
    embedded_params.push_back(nlohmann::json::object());
    api_key_t key_search_coll2("KeyVal2", "test key", {"documents:search"}, {"coll2"}, FUTURE_TS);
    auth_manager.create_key(key_search_coll2);
    ASSERT_TRUE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", scoped_key)}, empty_params, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll2", scoped_key)}, empty_params, embedded_params));

    // scoped key generated from key_search_coll2
    embedded_params.clear();
    embedded_params.push_back(nlohmann::json::object());
    std::string scoped_key_prefix2 = "QmNlNXdkUThaeDJFZXNiOXB4VUFCT1BmN01GSEJnRUdiMng2aTJESjJqND1LZXlWeyJmaWx0ZXJfYnkiOiAidXNlcl9pZDoxMDgwIn0=";
    ASSERT_TRUE(auth_manager.authenticate("documents:search", {collection_key_t("coll2", scoped_key_prefix2)}, empty_params, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", scoped_key_prefix2)}, empty_params, embedded_params));

    // should only allow scoped API keys derived from parent key with documents:search action
    embedded_params.clear();
    embedded_params.push_back(nlohmann::json::object());
    api_key_t key_search_admin("AdminKey", "admin key", {"*"}, {"*"}, FUTURE_TS);
    auth_manager.create_key(key_search_admin);
    std::string scoped_key2 = StringUtils::base64_encode(
      "BXbsk+xLT1gxOjDyip6+PE4MtOzOm/H7kbkN1d/j/s4=Admi{\"filter_by\": \"user_id:1080\"}"
    );
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll2", scoped_key2)}, empty_params, embedded_params));

    // expiration of scoped api key

    // {"filter_by": "user_id:1080", "expires_at": 2237712220} (NOT expired)
    api_key_t key_expiry("ExpireKey", "expire key", {"documents:search"}, {"*"}, FUTURE_TS);
    auth_manager.create_key(key_expiry);

    embedded_params.clear();
    embedded_params.push_back(nlohmann::json::object());

    std::string scoped_key3 = "K1M2STRDelZYNHpxNGVWUTlBTGpOWUl4dk8wNU8xdnVEZi9aSUcvZE5tcz1FeHBpeyJmaWx0ZXJfYnkiOi"
                              "AidXNlcl9pZDoxMDgwIiwgImV4cGlyZXNfYXQiOiAyMjM3NzEyMjIwfQ==";

    ASSERT_TRUE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", scoped_key3)}, empty_params, embedded_params));
    ASSERT_EQ("user_id:1080", embedded_params[0]["filter_by"].get<std::string>());
    ASSERT_EQ(1, embedded_params.size());

    // {"filter_by": "user_id:1080", "expires_at": 1606563316} (expired)

    api_key_t key_expiry2("ExpireKey2", "expire key", {"documents:search"}, {"*"}, FUTURE_TS);
    auth_manager.create_key(key_expiry2);

    embedded_params.clear();
    embedded_params.push_back(nlohmann::json::object());

    std::string scoped_key4 = "SXFKNldZZWRiWkVKVmI2RCt3OTlKNHpBZ24yWlRUbEdJdERtTy9IZ2REZz1FeHBpeyJmaWx0ZXJfYnkiOiAidXN"
                              "lcl9pZDoxMDgwIiwgImV4cGlyZXNfYXQiOiAxNjA2NTYzMzE2fQ==";

    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", scoped_key4)}, empty_params, embedded_params));

    // {"filter_by": "user_id:1080", "expires_at": 64723363200} (greater than parent key expiry)
    // embedded key's param cannot exceed parent's expiry

    api_key_t key_expiry3("ExpireKey3", "expire key", {"documents:search"}, {"*"}, 1606563841);
    auth_manager.create_key(key_expiry3);

    embedded_params.clear();
    embedded_params.push_back(nlohmann::json::object());

    std::string scoped_key5 = "V3JMNFJlZHRMVStrZHphNFVGZDh4MWltSmx6Yzk2R3QvS2ZwSE8weGRWQT1FeHBpeyJmaWx0ZXJfYnkiOiAidX"
                              "Nlcl9pZDoxMDgwIiwgImV4cGlyZXNfYXQiOiA2NDcyMzM2MzIwMH0=";

    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", scoped_key5)}, empty_params, embedded_params));

    // bad scoped API key
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", " XhsdBdhehdDheruyhvbdhwjhHdhgyeHbfheR")}, empty_params, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", "cXYPvkNKRlQrBzVTEgY4a3FrZfZ2MEs4kFJ6all3eldwM GhKZnRId3Y3TT1RZmxZeYJmaWx0ZXJfYnkiOkJ1aWQ6OElVm1lUVm15SG9ZOHM4NUx2VFk4S2drNHJIMiJ9")}, empty_params, embedded_params));
    ASSERT_FALSE(auth_manager.authenticate("documents:search", {collection_key_t("coll1", "SXZqcVdOWjVNNUVsY3ZiTW9YajQ1QnhrUXJaRzRaS0VhTlFvUmlvQ3gycz1LZXlWeyJmaWx0ZXJfYnkiOiAidXNlcl9pZDoxMDgw In0=")}, empty_params, embedded_params));
}

TEST_F(AuthManagerTest, ValidateBadKeyProperties) {
    nlohmann::json key_obj1;
    key_obj1["description"] = "desc";
    key_obj1["actions"].push_back("*");
    key_obj1["collections"].push_back(1);

    Option<uint32_t> validate_op = api_key_t::validate(key_obj1);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Wrong format for `collections`. It should be an array of string.", validate_op.error().c_str());

    key_obj1["actions"].push_back(1);
    key_obj1["collections"].push_back("*");
    validate_op = api_key_t::validate(key_obj1);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Wrong format for `actions`. It should be an array of string.", validate_op.error().c_str());

    key_obj1["actions"] = 1;
    key_obj1["collections"] = {"*"};
    validate_op = api_key_t::validate(key_obj1);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Wrong format for `actions`. It should be an array of string.", validate_op.error().c_str());

    nlohmann::json key_obj2;
    key_obj2["description"] = "desc";
    key_obj2["actions"] = {"*"};
    key_obj2["collections"] = {"foobar"};
    key_obj2["expires_at"] = -100;

    validate_op = api_key_t::validate(key_obj2);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Wrong format for `expires_at`. It should be an unsigned integer.", validate_op.error().c_str());

    key_obj2["expires_at"] = "expiry_ts";

    validate_op = api_key_t::validate(key_obj2);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Wrong format for `expires_at`. It should be an unsigned integer.", validate_op.error().c_str());

    key_obj2["expires_at"] = 1606539880;

    validate_op = api_key_t::validate(key_obj2);
    ASSERT_TRUE(validate_op.ok());

    // check for valid value
    nlohmann::json key_obj3;
    key_obj3["description"] = "desc";
    key_obj3["actions"] = {"*"};
    key_obj3["collections"] = {"foobar"};
    key_obj3["value"] = 100;

    validate_op = api_key_t::validate(key_obj3);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Key value must be a string.", validate_op.error().c_str());

    // check for valid description
    nlohmann::json key_obj4;
    key_obj4["description"] = 42;
    key_obj4["actions"] = {"*"};
    key_obj4["collections"] = {"foobar"};
    key_obj4["value"] = "abcd";

    validate_op = api_key_t::validate(key_obj4);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Key description must be a string.", validate_op.error().c_str());
}

TEST_F(AuthManagerTest, AutoDeleteKeysOnExpiry) {
    auto list_op = auth_manager.list_keys();
    ASSERT_TRUE(list_op.ok());
    ASSERT_EQ(0, list_op.get().size());

    //regular key(future ts)
    api_key_t api_key1("abcd", "test key 1", {"read", "write"}, {"collection1", "collection2"}, FUTURE_TS);

    //key is expired (past ts)
    uint64_t PAST_TS = uint64_t(std::time(0)) - 100;
    api_key_t api_key2("wxyz", "test key 2", {"admin"}, {"*"}, PAST_TS, true);

    auto insert_op = auth_manager.create_key(api_key1);
    ASSERT_TRUE(insert_op.ok());
    ASSERT_EQ(4, insert_op.get().value.size());

    insert_op = auth_manager.create_key(api_key2);
    ASSERT_TRUE(insert_op.ok());
    ASSERT_EQ(4, insert_op.get().value.size());

    list_op = auth_manager.list_keys();
    ASSERT_TRUE(list_op.ok());
    auto keys = list_op.get();
    ASSERT_EQ(2, keys.size());
    ASSERT_EQ("abcd", keys[0].value);
    ASSERT_EQ("wxyz", keys[1].value);

    auth_manager.do_housekeeping();

    list_op = auth_manager.list_keys();
    ASSERT_TRUE(list_op.ok());
    keys = list_op.get();
    ASSERT_EQ(1, keys.size());
    ASSERT_EQ("abcd", keys[0].value);
}

TEST_F(AuthManagerTest, CollectionsByScope) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    auto scoped_key_json = R"({
        "description": "Write key",
        "actions": [ "collections:*", "documents:*", "synonyms:*" ],
        "collections": [ "collection_.*" ],
        "value": "3859c47b98"
    })"_json;

    req->body =scoped_key_json.dump();
    ASSERT_TRUE(post_create_key(req, res));

    auto schema1 = R"({
        "name": "collection_1",
        "fields": [
            {"name": "title", "type": "string", "locale": "en"},
            {"name": "points", "type": "int32"}
        ]
    })"_json;

    collectionManager.create_collection(schema1);

    auto schema2 = R"({
        "name": "collection2",
        "fields": [
            {"name": "title", "type": "string", "locale": "en"},
            {"name": "points", "type": "int32"}
        ]
    })"_json;

    collectionManager.create_collection(schema2);


    req->api_auth_key = "3859c47b98";
    get_collections(req, res);
    auto result_json = nlohmann::json::parse(res->body);

    ASSERT_EQ(1, result_json.size());
    ASSERT_EQ("collection_1", result_json[0]["name"]);

    req->api_auth_key.clear();
    get_collections(req, res);
    result_json = nlohmann::json::parse(res->body);

    ASSERT_EQ(2, result_json.size());
    ASSERT_EQ("collection2", result_json[0]["name"]);
    ASSERT_EQ("collection_1", result_json[1]["name"]);

    scoped_key_json = R"({
        "description": "Write key",
        "actions": [ "collections:*", "documents:*", "synonyms:*" ],
        "collections": [ "collection2" ],
        "value": "b78a573a1a"
    })"_json;

    req->body =scoped_key_json.dump();
    ASSERT_TRUE(post_create_key(req, res));

    req->api_auth_key = "b78a573a1a";
    get_collections(req, res);
    result_json = nlohmann::json::parse(res->body);

    ASSERT_EQ(1, result_json.size());
    ASSERT_EQ("collection2", result_json[0]["name"]);

    scoped_key_json = R"({
        "description": "Write key",
        "actions": [ "collections:*", "documents:*", "synonyms:*" ],
        "collections": [ "*" ],
        "value": "00071e2108"
    })"_json;

    req->body =scoped_key_json.dump();
    ASSERT_TRUE(post_create_key(req, res));

    req->api_auth_key = "00071e2108";
    get_collections(req, res);
    result_json = nlohmann::json::parse(res->body);

    ASSERT_EQ(2, result_json.size());
    ASSERT_EQ("collection2", result_json[0]["name"]);
    ASSERT_EQ("collection_1", result_json[1]["name"]);
}