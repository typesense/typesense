#include <gtest/gtest.h>
#include <string>
#include <thread>
#include "ratelimit_manager.h"

// Google test for RateLimitManager
class RateLimitManagerTest : public ::testing::Test
{
protected:
    RateLimitManager *manager = RateLimitManager::getInstance();

    RateLimitManagerTest()
    {
        // You can do set-up work for each test here.
    }

    virtual ~RateLimitManagerTest()
    {
        // You can do clean-up work that doesn't throw exceptions here.
        manager->clear_all();
    }

    // If the constructor and destructor are not enough for setting up
    // and cleaning up each test, you can define the following methods:

    virtual void SetUp()
    {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    virtual void TearDown()
    {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }

    // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(RateLimitManagerTest, TestAddRateLimitApiKey) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 1, 1, -1, -1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
}

TEST_F(RateLimitManagerTest, TestAddRateLimitIp) {
    manager->throttle_entities(RateLimitedEntityType::ip, {"test_api_key"}, 1, 1, -1, -1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
}

TEST_F(RateLimitManagerTest, TestRemoveRateLimitApiKey) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 1, 1, -1, -1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
    manager->remove_rule_entity(RateLimitedEntityType::api_key, "test_api_key");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestRemoveRateLimitIp) {
    manager->throttle_entities(RateLimitedEntityType::ip, {"test_api_key"}, 1, 1, -1, -1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
    manager->remove_rule_entity(RateLimitedEntityType::ip, "test_api_key");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestGetBannedIps) {
    manager->ban_entities_permanently(RateLimitedEntityType::ip, {"0.0.0.0"});
    EXPECT_EQ(manager->get_banned_entities(RateLimitedEntityType::ip).size(), 1);
}

TEST_F(RateLimitManagerTest, TestGetTrackedIps) {
    manager->throttle_entities(RateLimitedEntityType::ip, {"0.0.0.0"}, 1, 1, -1, -1);
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::throttle && entities[0].max_requests.minute_threshold == 1 && entities[0].max_requests.hour_threshold == 1;
    found = found && entities[0].entity_type == RateLimitedEntityType::ip && entities[0].entity_ids[0] == "0.0.0.0";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestGetTrackedApiKeys) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 1, 1, -1, -1);
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::throttle && entities[0].max_requests.minute_threshold == 1 && entities[0].max_requests.hour_threshold == 1;
    found = found && entities[0].entity_type == RateLimitedEntityType::api_key && entities[0].entity_ids[0] == "test_api_key";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestBanIpPermanently) {
    manager->ban_entities_permanently(RateLimitedEntityType::ip, {"0.0.0.1"});
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::block && entities[0].entity_type == RateLimitedEntityType::ip && entities[0].entity_ids[0] == "0.0.0.1";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestUnbanIp) {
    manager->ban_entities_permanently(RateLimitedEntityType::ip, {"0.0.0.1"});
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::block && entities[0].entity_type == RateLimitedEntityType::ip && entities[0].entity_ids[0] == "0.0.0.1";
    EXPECT_TRUE(found);
    manager->remove_rule_entity(RateLimitedEntityType::ip, "0.0.0.1");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestIsBannedIp) {
    manager->ban_entities_permanently(RateLimitedEntityType::ip, {"0.0.0.1"});
    EXPECT_TRUE(manager->get_banned_entities(RateLimitedEntityType::ip).size() == 1);
    auto entities = manager->get_banned_entities(RateLimitedEntityType::ip);
    bool found = entities[0].entity_type == RateLimitedEntityType::ip && entities[0].value == "0.0.0.1";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestIsBannedIpTemp) {
    manager->throttle_entities(RateLimitedEntityType::ip, {"0.0.0.1"}, 1, 1, 1, 1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyPermanently) {
    manager->ban_entities_permanently(RateLimitedEntityType::api_key, {"test_api_key"});
    EXPECT_TRUE(manager->get_banned_entities(RateLimitedEntityType::api_key).size() == 1);
    auto entities = manager->get_banned_entities(RateLimitedEntityType::api_key);
    bool found = entities[0].entity_type == RateLimitedEntityType::api_key && entities[0].value == "test_api_key";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyTemp) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 1, 1, 1, 1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestAllowAPIKey) {
    manager->allow_entities(RateLimitedEntityType::api_key, {{"test_api_key"}});
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestAllowIp) {
    manager->allow_entities(RateLimitedEntityType::ip, {{"0.0.0.1"}});
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestThrottleAPIKey) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 1, 1, -1, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestDeleteRuleByID) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 1, 1, -1, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    auto rules = manager->get_all_rules();
    manager->delete_rule_by_id(rules[0].id);
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestMinuteRateLimitAPIKey) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 5, -1, -1, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitAPIKey) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, -1, 5, -1, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestMinuteRateLimitIp) {
    manager->throttle_entities(RateLimitedEntityType::ip, {"0.0.0.1"}, 5, -1, -1, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitIp) {
    manager->throttle_entities(RateLimitedEntityType::ip, {"0.0.0.1"}, -1, 5, -1, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestGetAllRules) {
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 5, -1, -1, -1);
    manager->throttle_entities(RateLimitedEntityType::ip, {"0.0.0.1"}, 5, -1, -1, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 2);
}

TEST_F(RateLimitManagerTest, TestGetAllRulesEmpty) {
    auto rules = manager->get_all_rules();
    EXPECT_EQ(rules.size(), 0);
}

TEST_F(RateLimitManagerTest, TestGetAllRulesJSON)
{
    manager->throttle_entities(RateLimitedEntityType::api_key, {"test_api_key"}, 5, -1, -1, -1);
    nlohmann::json rules = manager->get_all_rules_json();
    EXPECT_EQ(rules.is_array(), true);
    EXPECT_EQ(rules.size(), 1);
    EXPECT_EQ(rules.at(0).is_object(), true);
    EXPECT_EQ(rules.at(0).at("id").is_number(), true);
    EXPECT_EQ(rules.at(0).at("entity_type").is_string(), true);
    EXPECT_EQ(rules.at(0).at("entity_ids").is_array(), true);
}
