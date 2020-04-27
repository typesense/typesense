#include <gtest/gtest.h>
#include <stdlib.h>
#include <iostream>
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
    // when no keys are present at all
    ASSERT_FALSE(auth_manager.authenticate("jdlaslasdasd", "", ""));

    // wildcard permission
    api_key_t wildcard_all_key = api_key_t("abcd1", "wildcard all key", {"*"}, {"*"});
    auth_manager.create_key(wildcard_all_key);

    ASSERT_FALSE(auth_manager.authenticate("jdlaslasdasd", "documents:create", "collection1"));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_all_key.value, "metrics:get", ""));

    // wildcard on a collection
    api_key_t wildcard_coll_key = api_key_t("abcd2", "wildcard coll key", {"*"}, {"collection1"});
    auth_manager.create_key(wildcard_coll_key);

    ASSERT_FALSE(auth_manager.authenticate("adasda", "documents:create", "collection1"));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_coll_key.value, "documents:get", "collection1"));
    ASSERT_FALSE(auth_manager.authenticate(wildcard_coll_key.value, "documents:get", "collection2"));

    // wildcard on multiple collections
    api_key_t wildcard_colls_key = api_key_t("abcd3", "wildcard coll key", {"*"}, {"collection1", "collection2", "collection3"});
    auth_manager.create_key(wildcard_colls_key);

    ASSERT_TRUE(auth_manager.authenticate(wildcard_colls_key.value, "documents:get", "collection1"));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_colls_key.value, "documents:search", "collection2"));
    ASSERT_TRUE(auth_manager.authenticate(wildcard_colls_key.value, "documents:create", "collection3"));
    ASSERT_FALSE(auth_manager.authenticate(wildcard_colls_key.value, "documents:get", "collection4"));
    ASSERT_FALSE(auth_manager.authenticate(wildcard_colls_key.value, "documents:get", "*"));

    // only 1 action on multiple collections
    api_key_t one_action_key = api_key_t("abcd4", "one action key", {"documents:search"}, {"collection1", "collection2"});
    auth_manager.create_key(one_action_key);

    ASSERT_TRUE(auth_manager.authenticate(one_action_key.value, "documents:search", "collection1"));
    ASSERT_FALSE(auth_manager.authenticate(one_action_key.value, "documents:get", "collection2"));
    ASSERT_FALSE(auth_manager.authenticate(one_action_key.value, "documents:search", "collection5"));
    ASSERT_FALSE(auth_manager.authenticate(one_action_key.value, "*", "collection2"));

    // multiple actions on multiple collections
    api_key_t mul_acoll_key = api_key_t("abcd5", "multiple action/collection key",
                                        {"documents:get", "collections:list"}, {"metacollection", "collection2"});
    auth_manager.create_key(mul_acoll_key);

    ASSERT_TRUE(auth_manager.authenticate(mul_acoll_key.value, "documents:get", "metacollection"));
    ASSERT_TRUE(auth_manager.authenticate(mul_acoll_key.value, "collections:list", "collection2"));
    ASSERT_TRUE(auth_manager.authenticate(mul_acoll_key.value, "collections:list", "metacollection"));
    ASSERT_FALSE(auth_manager.authenticate(mul_acoll_key.value, "documents:search", "collection2"));
    ASSERT_FALSE(auth_manager.authenticate(mul_acoll_key.value, "documents:get", "collection5"));
    ASSERT_FALSE(auth_manager.authenticate(mul_acoll_key.value, "*", "*"));
}