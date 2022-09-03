#include <gtest/gtest.h>
#include <string>
#include <thread>
#include "ratelimit_manager.h"



// Google test for RateLimitManager
class RateLimitManagerTest : public ::testing::Test {
    protected:
        RateLimitManager *manager = RateLimitManager::getInstance();

        RateLimitManagerTest() {
        // You can do set-up work for each test here.
        }

        virtual ~RateLimitManagerTest() {
        // You can do clean-up work that doesn't throw exceptions here.
            manager->clear_all();
        }

        // If the constructor and destructor are not enough for setting up
        // and cleaning up each test, you can define the following methods:

        virtual void SetUp() {
        // Code here will be called immediately after the constructor (right
        // before each test).
        }

        virtual void TearDown() {
        // Code here will be called immediately after each test (right
        // before the destructor).
        }

        // Objects declared here can be used by all tests in the test case for Foo.
};


TEST_F(RateLimitManagerTest, TestAddRateLimitApiKey) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 1, 1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
}

TEST_F(RateLimitManagerTest, TestAddRateLimitIp) {
    manager->throttle_entries(RateLimitedResourceType::IP, {"test_api_key"}, 1, 1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
}


TEST_F(RateLimitManagerTest, TestRemoveRateLimitApiKey) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 1, 1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
    manager->remove_rule_entry(RateLimitedResourceType::API_KEY, "test_api_key");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestRemoveRateLimitIp) {
    manager->throttle_entries(RateLimitedResourceType::IP, {"test_api_key"}, 1, 1);
    EXPECT_EQ(manager->get_all_rules().size(), 1);
    manager->remove_rule_entry(RateLimitedResourceType::IP, "test_api_key");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestGetBannedIps) {
    manager->ban_entries_permanently(RateLimitedResourceType::IP, {"0.0.0.0"});
    EXPECT_EQ(manager->get_banned_entries(RateLimitedResourceType::IP).size(), 1);
}


TEST_F(RateLimitManagerTest, TestGetTrackedIps) {
    manager->throttle_entries(RateLimitedResourceType::IP, {"0.0.0.0"}, 1, 1);
    auto entries = manager->get_all_rules();
    bool found = entries[0].action == RateLimitAction::THROTTLE && entries[0].throttle.minute_rate_limit == 1 && entries[0].throttle.hour_rate_limit == 1;
    found = found && entries[0].resource_type == RateLimitedResourceType::IP && entries[0].values[0] == "0.0.0.0";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestGetTrackedApiKeys) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 1, 1);
    auto entries = manager->get_all_rules();
    bool found = entries[0].action == RateLimitAction::THROTTLE && entries[0].throttle.minute_rate_limit == 1 && entries[0].throttle.hour_rate_limit == 1;
    found = found && entries[0].resource_type == RateLimitedResourceType::API_KEY && entries[0].values[0] == "test_api_key";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestBanIpPermanently) {
    manager->ban_entries_permanently(RateLimitedResourceType::IP, {"0.0.0.1"});
    auto entries = manager->get_all_rules();
    bool found = entries[0].action == RateLimitAction::BLOCK && entries[0].resource_type == RateLimitedResourceType::IP && entries[0].values[0] == "0.0.0.1";
    EXPECT_TRUE(found);
}


TEST_F(RateLimitManagerTest, TestUnbanIp) {
    manager->ban_entries_permanently(RateLimitedResourceType::IP, {"0.0.0.1"});
    auto entries = manager->get_all_rules();
    bool found = entries[0].action == RateLimitAction::BLOCK && entries[0].resource_type == RateLimitedResourceType::IP && entries[0].values[0] == "0.0.0.1";
    EXPECT_TRUE(found);
    manager->remove_rule_entry(RateLimitedResourceType::IP, "0.0.0.1");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestIsBannedIp) {
    manager->ban_entries_permanently(RateLimitedResourceType::IP, {"0.0.0.1"});
    EXPECT_TRUE(manager->get_banned_entries(RateLimitedResourceType::IP).size() == 1);
    auto entries = manager->get_banned_entries(RateLimitedResourceType::IP);
    bool found = entries[0].is_banned  && entries[0].resource_type == RateLimitedResourceType::IP && entries[0].value == "0.0.0.1";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestIsBannedIpTemp) {
    manager->throttle_entries(RateLimitedResourceType::IP, {"0.0.0.1"}, 1, 1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
}


TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyPermanently) {
    manager->ban_entries_permanently(RateLimitedResourceType::API_KEY, {"test_api_key"});
    EXPECT_TRUE(manager->get_banned_entries(RateLimitedResourceType::API_KEY).size() == 1);
    auto entries = manager->get_banned_entries(RateLimitedResourceType::API_KEY);
    bool found = entries[0].is_banned  && entries[0].resource_type == RateLimitedResourceType::API_KEY && entries[0].value == "test_api_key";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyTemp) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 1, 1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
}


TEST_F(RateLimitManagerTest, TestAllowAPIKey) {
    manager->allow_entries(RateLimitedResourceType::API_KEY, {{"test_api_key"}});
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestAllowIp) {
    manager->allow_entries(RateLimitedResourceType::IP, {{"0.0.0.1"}});
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestThrottleAPIKey) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 1, 1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestDeleteRuleByID) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 1, 1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    auto rules = manager->get_all_rules();
    manager->delete_rule_by_id(rules[0].id);
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}


TEST_F(RateLimitManagerTest, TestMinuteRateLimitAPIKey) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 5, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitAPIKey) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, -1, 5);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::API_KEY, "test_api_key"}}));
}

TEST_F(RateLimitManagerTest, TestMinuteRateLimitIp) {
    manager->throttle_entries(RateLimitedResourceType::IP, {"0.0.0.1"}, 5, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitIp) {
    manager->throttle_entries(RateLimitedResourceType::IP, {"0.0.0.1"}, -1, 5);
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest,TestGetAllRules) {
    manager->throttle_entries(RateLimitedResourceType::API_KEY, {"test_api_key"}, 5, -1);
    manager->throttle_entries(RateLimitedResourceType::IP, {"0.0.0.1"}, 5, -1);
    EXPECT_TRUE(manager->get_all_rules().size() == 2);
}

TEST_F(RateLimitManagerTest,TestGetAllRulesEmpty) 
{
    auto rules = manager->get_all_rules();
    EXPECT_EQ(rules.size(), 0);
}

TEST_F(RateLimitManagerTest,TestUnbanTempIPBan)
{
    manager->throttle_entries(RateLimitedResourceType::IP, {"0.0.0.1"}, 5, -1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}});
    manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}});
    manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}});
    manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}});
    manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}});
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    auto banned_entries = manager->get_banned_entries(RateLimitedResourceType::IP);
    EXPECT_EQ(banned_entries[0].resource_type, RateLimitedResourceType::IP);
    EXPECT_EQ(banned_entries[0].value, "0.0.0.1");
    EXPECT_EQ(banned_entries[0].banDuration, RateLimitBanDuration::ONE_MINUTE);
    // Wait fore one minute
    std::this_thread::sleep_for(std::chrono::seconds(60));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedResourceType::IP, "0.0.0.1"}}));
    banned_entries = manager->get_banned_entries(RateLimitedResourceType::IP);
    EXPECT_EQ(banned_entries[0].is_banned, false);
    EXPECT_EQ(banned_entries[0].banDuration, RateLimitBanDuration::ONE_MINUTE);
    EXPECT_EQ(banned_entries[0].resource_type, RateLimitedResourceType::IP);
}
















