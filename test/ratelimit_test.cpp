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
    manager->add_rate_limit_api_key("test", 1, 1);
    EXPECT_EQ(manager->get_tracked_api_keys().size(), 1);
}

TEST_F(RateLimitManagerTest, TestAddRateLimitIp) {
    manager->add_rate_limit_ip("test", 1, 1);
    EXPECT_EQ(manager->get_tracked_ips().size(), 1);
}


TEST_F(RateLimitManagerTest, TestRemoveRateLimitApiKey) {
    manager->add_rate_limit_api_key("test", 1, 1);
    manager->remove_rate_limit_api_key("test");
    EXPECT_EQ(manager->get_tracked_api_keys().size(), 0);
}

TEST_F(RateLimitManagerTest, TestRemoveRateLimitIp) {
    manager->add_rate_limit_ip("test", 1, 1);
    manager->remove_rate_limit_ip("test");
    EXPECT_EQ(manager->get_tracked_ips().size(), 0);
}

TEST_F(RateLimitManagerTest, TestGetBannedIps) {
    manager->ban_ip_permanently("test");
    EXPECT_EQ(manager->get_banned_ips().size(), 1);
}


TEST_F(RateLimitManagerTest, TestGetTrackedIps) {
    manager->add_rate_limit_ip("test", 1, 1);
    EXPECT_EQ(manager->get_tracked_ips().size(), 1);
}

TEST_F(RateLimitManagerTest, TestGetTrackedApiKeys) {
    manager->add_rate_limit_api_key("test", 1, 1);
    EXPECT_EQ(manager->get_tracked_api_keys().size(), 1);
}

TEST_F(RateLimitManagerTest, TestIsRateLimitedAPIKey) {
    manager->add_rate_limit_api_key("test", 2, 2);
    EXPECT_EQ(manager->is_rate_limited("test", "ip"), false);
}

TEST_F(RateLimitManagerTest, TestIsRateLimitedIp) {
    manager->add_rate_limit_ip("test", 2, 2);
    EXPECT_EQ(manager->is_rate_limited("api_key", "test"), false);
}

TEST_F(RateLimitManagerTest, TestBanIpPermanently) {
    manager->ban_ip_permanently("test");
    EXPECT_EQ(manager->get_banned_ips().size(), 1);
}


TEST_F(RateLimitManagerTest, TestUnbanIp) {
    manager->ban_ip_permanently("test");
    manager->allow_ip("test");
    EXPECT_EQ(manager->get_banned_ips().size(), 0);
}

TEST_F(RateLimitManagerTest, TestIsBannedIp) {
    manager->ban_ip_permanently("test");
    auto banned_ips = manager->get_banned_ips();
    auto it = std::find_if(banned_ips.begin(), banned_ips.end(),
                           [](const ratelimit_ban_t &ban) {
                               return ban.ip == "test" && ban.api_key == "";
                           });
    EXPECT_EQ(it != banned_ips.end(), true);
}

TEST_F(RateLimitManagerTest, TestIsBannedIpTemp) {
    manager->add_rate_limit_ip("test", 0, 0);
    manager->is_rate_limited("api_key", "test");
    auto banned_ips = manager->get_banned_ips();
    auto it = std::find_if(banned_ips.begin(), banned_ips.end(),
                           [](const ratelimit_ban_t &ban) {
                               return ban.ip == "test" && ban.api_key == "api_key";
                           });
    EXPECT_EQ(it != banned_ips.end(), true);
}


TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyPermanently) {
    manager->ban_api_key_permanently("test");
    auto banned_api_keys = manager->get_banned_api_keys();
    auto it = std::find_if(banned_api_keys.begin(), banned_api_keys.end(), [&](const ratelimit_ban_t& banned_api_key) { return banned_api_key.api_key == "test" && banned_api_key.ip == ""; });
    EXPECT_EQ( it != banned_api_keys.end(), true);
}

TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyTemp) {
    manager->add_rate_limit_api_key("test", 0, 0);
    manager->is_rate_limited("test", "ip");
    auto banned_api_keys = manager->get_banned_api_keys();
    auto it = std::find_if(banned_api_keys.begin(), banned_api_keys.end(), [&](const ratelimit_ban_t& banned_api_key) { return banned_api_key.api_key == "test" && banned_api_key.ip == ""; });
    EXPECT_EQ( it != banned_api_keys.end(), true);
}


TEST_F(RateLimitManagerTest, TestAllowAPIKey) {
    manager->allow_api_key("test");
    auto rules = manager->get_all_rules();
    auto it = std::find_if(rules.begin(), rules.end(), [&](const ratelimit_tracker_t& rule) { return rule.api_key == "test" && rule.ip == "" && rule.is_allowed == true; });
    EXPECT_EQ( it != rules.end(), true);
}

TEST_F(RateLimitManagerTest, TestAllowIp) {
    manager->allow_ip("test");
    auto rules = manager->get_all_rules();
    auto it = std::find_if(rules.begin(), rules.end(), [&](const ratelimit_tracker_t& rule) { return rule.api_key == "" && rule.ip == "test" && rule.is_allowed == true; });
    EXPECT_EQ( it != rules.end(), true);
}

TEST_F(RateLimitManagerTest, TestThrottleAPIKey) {
    manager->add_rate_limit_api_key("test", 2, 2);
    auto rules = manager->get_all_rules();
    auto it = std::find_if(rules.begin(), rules.end(), [&](const ratelimit_tracker_t& rule) { return rule.api_key == "test" && rule.ip == "" && rule.is_allowed == false && rule.minute_rate_limit == 2 && rule.hour_rate_limit == 2; });
    EXPECT_EQ( it != rules.end(), true);
}

TEST_F(RateLimitManagerTest, TestThrottleIp) {
    manager->add_rate_limit_ip("test", 2, 2);
    auto rules = manager->get_all_rules();
    auto it = std::find_if(rules.begin(), rules.end(), [&](const ratelimit_tracker_t& rule) { return rule.api_key == "" && rule.ip == "test" && rule.is_allowed == false && rule.minute_rate_limit == 2 && rule.hour_rate_limit == 2; });
    EXPECT_EQ( it != rules.end(), true);
}

TEST_F(RateLimitManagerTest, TestDeleteRuleByID) {
    manager->add_rate_limit_api_key("test", 2, 2);
    auto rules = manager->get_all_rules();
    manager->delete_rule_by_id(rules[0].id);
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}


TEST_F(RateLimitManagerTest, TestMinuteRateLimitAPIKey) {
    manager->add_rate_limit_api_key("test", 3, 3);
    EXPECT_EQ(manager->is_rate_limited("test", "ip"), false);
    EXPECT_EQ(manager->is_rate_limited("test", "ip"), false);
    EXPECT_EQ(manager->is_rate_limited("test", "ip"), true);
}

TEST_F(RateLimitManagerTest, TestHourRateLimitAPIKey) {
    manager->add_rate_limit_api_key("test", -1, 3);
    EXPECT_EQ(manager->is_rate_limited("test", "ip"), false);
    EXPECT_EQ(manager->is_rate_limited("test", "ip"), false);
    EXPECT_EQ(manager->is_rate_limited("test", "ip"), true);
}

TEST_F(RateLimitManagerTest, TestMinuteRateLimitIp) {
    manager->add_rate_limit_ip("test", 3, 3);
    EXPECT_EQ(manager->is_rate_limited("api_key", "test"), false);
    EXPECT_EQ(manager->is_rate_limited("api_key", "test"), false);
    EXPECT_EQ(manager->is_rate_limited("api_key", "test"), true);
}

TEST_F(RateLimitManagerTest, TestHourRateLimitIp) {
    manager->add_rate_limit_ip("test", -1, 3);
    EXPECT_EQ(manager->is_rate_limited("api_key", "test"), false);
    EXPECT_EQ(manager->is_rate_limited("api_key", "test"), false);
    EXPECT_EQ(manager->is_rate_limited("api_key", "test"), true);
}

TEST_F(RateLimitManagerTest,TestGetAllRules) {
    manager->add_rate_limit_api_key("test", 3, 3);
    manager->add_rate_limit_ip("test", 3, 3);
    auto rules = manager->get_all_rules();
    EXPECT_EQ(rules.size(), 2);
}

TEST_F(RateLimitManagerTest,TestGetAllRulesEmpty) 
{
    auto rules = manager->get_all_rules();
    EXPECT_EQ(rules.size(), 0);
}

TEST_F(RateLimitManagerTest,TestUnbanTempIPBan)
{
    manager->add_rate_limit_ip("test", 2, 2);
    manager->is_rate_limited("api_key", "test");
    manager->is_rate_limited("api_key", "test");
    auto banned_ips = manager->get_banned_ips();
    auto it = std::find_if(banned_ips.begin(), banned_ips.end(), [&](const ratelimit_ban_t& banned_ip) { return banned_ip.ip == "test" && banned_ip.api_key == "api_key"; });
    EXPECT_EQ( it != banned_ips.end(), true);
    // Wait for a minute
    std::this_thread::sleep_for(std::chrono::seconds(60));
    banned_ips = manager->get_banned_ips();
    it = std::find_if(banned_ips.begin(), banned_ips.end(), [&](const ratelimit_ban_t& banned_ip) { return banned_ip.ip == "test" && banned_ip.api_key == "api_key"; });
    EXPECT_EQ( it != banned_ips.end(), false);

}

TEST_F(RateLimitManagerTest,TestUnbanTempAPIKeyBan)
{
    manager->add_rate_limit_api_key("test", 2, 2);
    manager->is_rate_limited("test", "ip");
    manager->is_rate_limited("test", "ip");
    auto banned_api_keys = manager->get_banned_api_keys();
    auto it = std::find_if(banned_api_keys.begin(), banned_api_keys.end(), [&](const ratelimit_ban_t& banned_api_key) { return banned_api_key.api_key == "test" && banned_api_key.ip == ""; });
    EXPECT_EQ( it != banned_api_keys.end(), true);
    // Wait for a minute
    std::this_thread::sleep_for(std::chrono::seconds(60));
    banned_api_keys = manager->get_banned_api_keys();
    it = std::find_if(banned_api_keys.begin(), banned_api_keys.end(), [&](const ratelimit_ban_t& banned_api_key) { return banned_api_key.api_key == "test" && banned_api_key.ip == ""; });
    EXPECT_EQ( it != banned_api_keys.end(), false);
}
















