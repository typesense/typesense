#include <gtest/gtest.h>
#include <stdlib.h>
#include <iostream>
#include <http_data.h>
#include "auth_manager.h"

class AuthManagerTest : public ::testing::Test {
protected:
    Store *store;
    AuthManager auth_manager;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/auth_manager_test_db";
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        auth_manager.init(store);
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

    api_key_t api_key1("abcd1", "test key 1", {"read", "write"}, {"collection1", "collection2"});
    api_key_t api_key2("abcd2", "test key 2", {"admin"}, {"*"});

    ASSERT_EQ("abcd1", api_key1.value);
    ASSERT_EQ("abcd2", api_key2.value);

    auto insert_op = auth_manager.create_key(api_key1);
    ASSERT_TRUE(insert_op.ok());
    ASSERT_EQ(5, insert_op.get().value.size());

    insert_op = auth_manager.create_key(api_key2);
    ASSERT_TRUE(insert_op.ok());
    ASSERT_EQ(5, insert_op.get().value.size());

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
}

TEST_F(AuthManagerTest, CheckRestoreOfAPIKeys) {
    api_key_t api_key1("abcd1", "test key 1", {"read", "write"}, {"collection1", "collection2"});
    api_key_t api_key2("abcd2", "test key 2", {"admin"}, {"*"});

    std::string key_value1 = auth_manager.create_key(api_key1).get().value;
    std::string key_value2 = auth_manager.create_key(api_key2).get().value;

    AuthManager auth_manager2;
    auth_manager2.init(store);

    // list keys

    auto list_op = auth_manager.list_keys();
    ASSERT_TRUE(list_op.ok());
    ASSERT_EQ(2, list_op.get().size());
    ASSERT_EQ("test key 1", list_op.get()[0].description);
    ASSERT_EQ("abcd", list_op.get()[0].value);
    ASSERT_STREQ(key_value1.substr(0, 4).c_str(), list_op.get()[0].value.c_str());
    ASSERT_EQ("test key 2", list_op.get()[1].description);
    ASSERT_EQ("abcd", list_op.get()[1].value);
    ASSERT_STREQ(key_value2.substr(0, 4).c_str(), list_op.get()[1].value.c_str());
}

TEST_F(AuthManagerTest, VerifyAuthentication) {
    std::map<std::string, std::string> sparams;
    // when no keys are present at all
    ASSERT_FALSE(auth_manager.authenticate("jdlaslasdasd", "", "", sparams));

    // wildcard permission
    api_key_t wildcard_all_key = api_key_t("abcd1", "wildcard all key", {"*"}, {"*"});
    auth_manager.create_key(wildcard_all_key);

    ASSERT_FALSE(auth_manager.authenticate("jdlaslasdasd", "documents:create", "collection1", sparams));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_all_key.value, "metrics:get", "", sparams));

    // wildcard on a collection
    api_key_t wildcard_coll_key = api_key_t("abcd2", "wildcard coll key", {"*"}, {"collection1"});
    auth_manager.create_key(wildcard_coll_key);

    ASSERT_FALSE(auth_manager.authenticate("adasda", "documents:create", "collection1", sparams));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_coll_key.value, "documents:get", "collection1", sparams));
    ASSERT_FALSE(auth_manager.authenticate(wildcard_coll_key.value, "documents:get", "collection2", sparams));

    // wildcard on multiple collections
    api_key_t wildcard_colls_key = api_key_t("abcd3", "wildcard coll key", {"*"}, {"collection1", "collection2", "collection3"});
    auth_manager.create_key(wildcard_colls_key);

    ASSERT_TRUE(auth_manager.authenticate(wildcard_colls_key.value, "documents:get", "collection1", sparams));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_colls_key.value, "documents:search", "collection2", sparams));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_colls_key.value, "documents:create", "collection3", sparams));
    ASSERT_FALSE(auth_manager.authenticate(wildcard_colls_key.value, "documents:get", "collection4", sparams));
    ASSERT_FALSE(auth_manager.authenticate(wildcard_colls_key.value, "documents:get", "*", sparams));

    // only 1 action on multiple collections
    api_key_t one_action_key = api_key_t("abcd4", "one action key", {"documents:search"}, {"collection1", "collection2"});
    auth_manager.create_key(one_action_key);

    ASSERT_TRUE(auth_manager.authenticate(one_action_key.value, "documents:search", "collection1", sparams));
    ASSERT_FALSE(auth_manager.authenticate(one_action_key.value, "documents:get", "collection2", sparams));
    ASSERT_FALSE(auth_manager.authenticate(one_action_key.value, "documents:search", "collection5", sparams));
    ASSERT_FALSE(auth_manager.authenticate(one_action_key.value, "*", "collection2", sparams));

    // multiple actions on multiple collections
    api_key_t mul_acoll_key = api_key_t("abcd5", "multiple action/collection key",
                                        {"documents:get", "collections:list"}, {"metacollection", "collection2"});
    auth_manager.create_key(mul_acoll_key);

    ASSERT_TRUE(auth_manager.authenticate(mul_acoll_key.value, "documents:get", "metacollection", sparams));
    ASSERT_TRUE(auth_manager.authenticate(mul_acoll_key.value, "collections:list", "collection2", sparams));
    ASSERT_TRUE(auth_manager.authenticate(mul_acoll_key.value, "collections:list", "metacollection", sparams));
    ASSERT_FALSE(auth_manager.authenticate(mul_acoll_key.value, "documents:search", "collection2", sparams));
    ASSERT_FALSE(auth_manager.authenticate(mul_acoll_key.value, "documents:get", "collection5", sparams));
    ASSERT_FALSE(auth_manager.authenticate(mul_acoll_key.value, "*", "*", sparams));
}

TEST_F(AuthManagerTest, GenerationOfAPIAction) {
    route_path rpath_search = route_path("GET", {"collections", ":collection", "documents", "search"}, nullptr, false, false);
    route_path rpath_coll_get = route_path("GET", {"collections", ":collection"}, nullptr, false, false);
    route_path rpath_coll_list = route_path("GET", {"collections"}, nullptr, false, false);
    route_path rpath_keys_post = route_path("POST", {"keys"}, nullptr, false, false);
    route_path rpath_doc_delete = route_path("DELETE", {"collections", ":collection", "documents", ":id"}, nullptr, false, false);
    route_path rpath_override_upsert = route_path("PUT", {"collections", ":collection", "overrides", ":id"}, nullptr, false, false);

    ASSERT_STREQ("documents:search", rpath_search._get_action().c_str());
    ASSERT_STREQ("collections:get", rpath_coll_get._get_action().c_str());
    ASSERT_STREQ("collections:list", rpath_coll_list._get_action().c_str());
    ASSERT_STREQ("keys:create", rpath_keys_post._get_action().c_str());
    ASSERT_STREQ("documents:delete", rpath_doc_delete._get_action().c_str());
    ASSERT_STREQ("overrides:upsert", rpath_override_upsert._get_action().c_str());
}

TEST_F(AuthManagerTest, ScopedAPIKeys) {
    std::map<std::string, std::string> params;
    params["filter_by"] = "country:USA";

    // create a API key bound to search scope and a given collection
    api_key_t key_search_coll1("KeyVal", "test key", {"documents:search"}, {"coll1"});
    auth_manager.create_key(key_search_coll1);

    std::string scoped_key = StringUtils::base64_encode(
      "IvjqWNZ5M5ElcvbMoXj45BxkQrZG4ZKEaNQoRioCx2s=KeyV{\"filter_by\": \"user_id:1080\"}"
    );

    ASSERT_TRUE(auth_manager.authenticate(scoped_key, "documents:search", "coll1", params));
    ASSERT_STREQ("country:USA&&user_id:1080", params["filter_by"].c_str());

    // should scope to collection bound by the parent key
    ASSERT_FALSE(auth_manager.authenticate(scoped_key, "documents:search", "coll2", params));

    // should scope to search action only
    ASSERT_FALSE(auth_manager.authenticate(scoped_key, "documents:create", "coll1", params));

    // check with corrupted key
    ASSERT_FALSE(auth_manager.authenticate("asdasasd", "documents:search", "coll1", params));

    // when params is empty, embedded param should be set
    std::map<std::string, std::string> empty_params;
    ASSERT_TRUE(auth_manager.authenticate(scoped_key, "documents:search", "coll1", empty_params));
    ASSERT_STREQ("user_id:1080", empty_params["filter_by"].c_str());

    // when more than a single key prefix matches, must pick the correct underlying key
    api_key_t key_search_coll2("KeyVal2", "test key", {"documents:search"}, {"coll2"});
    auth_manager.create_key(key_search_coll2);
    ASSERT_FALSE(auth_manager.authenticate(scoped_key, "documents:search", "coll2", empty_params));

    // should only allow scoped API keys derived from parent key with documents:search action
    api_key_t key_search_admin("AdminKey", "admin key", {"*"}, {"*"});
    auth_manager.create_key(key_search_admin);
    std::string scoped_key2 = StringUtils::base64_encode(
      "BXbsk+xLT1gxOjDyip6+PE4MtOzOm/H7kbkN1d/j/s4=Admi{\"filter_by\": \"user_id:1080\"}"
    );
    ASSERT_FALSE(auth_manager.authenticate(scoped_key2, "documents:search", "coll", empty_params));
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
    key_obj1["collections"].push_back("*");
    validate_op = api_key_t::validate(key_obj1);
    ASSERT_FALSE(validate_op.ok());
    ASSERT_STREQ("Wrong format for `actions`. It should be an array of string.", validate_op.error().c_str());
}