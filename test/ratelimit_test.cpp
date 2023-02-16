#include <gtest/gtest.h>
#include <string>
#include <thread>
#include "ratelimit_manager.h"
#include "logger.h"
#include "core_api.h"

// Google test for RateLimitManager
class RateLimitManagerTest : public ::testing::Test
{
protected:
    RateLimitManager *manager = RateLimitManager::getInstance();
    Store *store;
    

    void changeBaseTimestamp(const uint64_t new_base_timestamp) {
        manager->_set_base_timestamp(new_base_timestamp);
    }



    RateLimitManagerTest() {

    }

    virtual ~RateLimitManagerTest() {
        // You can do clean-up work that doesn't throw exceptions here.
        manager->clear_all();

    }

    // If the constructor and destructor are not enough for setting up
    // and cleaning up each test, you can define the following methods:

    virtual void SetUp() {
        std::string state_dir_path = "/tmp/typesense_test/rate_limit_manager_test_db";
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        manager->init(store);
    }

    virtual void TearDown() {
        delete store;
    }

    // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(RateLimitManagerTest, TestAddRateLimitApiKey) {
    auto res = manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_1m_threshold", 10},
        {"auto_ban_1m_duration_hours", 1}
    });


    EXPECT_EQ(manager->get_all_rules().size(), 1);
}

TEST_F(RateLimitManagerTest, TestAddRateLimitIp) {
    auto res = manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_1m_threshold", 10},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_EQ(manager->get_all_rules().size(), 1);
}

TEST_F(RateLimitManagerTest, TestGetBannedIps) {
    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    EXPECT_EQ(manager->get_banned_entities(RateLimitedEntityType::ip).size(), 1);
}

TEST_F(RateLimitManagerTest, TestGetTrackedIps) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_1m_threshold", 10},
        {"auto_ban_1m_duration_hours", 1}
    });
    auto rules = manager->get_all_rules();
    bool found = rules[0].action == RateLimitAction::throttle && rules[0].max_requests.minute_threshold == 10 && rules[0].max_requests.hour_threshold == 100;
    found = found && rules[0].entities[0].entity_type == RateLimitedEntityType::ip && rules[0].entities[0].entity_id == "0.0.0.1";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestGetTrackedApiKeys) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_1m_threshold", 10},
        {"auto_ban_1m_duration_hours", 1}
    });
    auto rules = manager->get_all_rules();
    bool found = rules[0].action == RateLimitAction::throttle && rules[0].max_requests.minute_threshold == 10 && rules[0].max_requests.hour_threshold == 100;
    found = found && rules[0].entities[0].entity_type == RateLimitedEntityType::api_key && rules[0].entities[0].entity_id == "test";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestBanIpPermanently) {
    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    auto rules = manager->get_all_rules();
    bool found = rules[0].action == RateLimitAction::block && rules[0].entities[0].entity_type == RateLimitedEntityType::ip && rules[0].entities[0].entity_id == "0.0.0.1";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestIsBannedIp) {
    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    EXPECT_TRUE(manager->get_banned_entities(RateLimitedEntityType::ip).size() == 1);
    auto banned_entities = manager->get_banned_entities(RateLimitedEntityType::ip);
    bool found = banned_entities[0].entity.entity_type == RateLimitedEntityType::ip && banned_entities[0].entity.entity_id == "0.0.0.1";
    EXPECT_EQ(banned_entities[0].and_entity.ok(), false);
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestIsBannedIpTemp) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 1},
        {"max_requests_1h", 1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"},{RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"},{RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyPermanently) {
    manager->add_rule({
        {"action", "block"},
        {"api_keys", nlohmann::json::array({"test"})}
    });
    EXPECT_TRUE(manager->get_banned_entities(RateLimitedEntityType::api_key).size() == 1);
    auto banned_entities = manager->get_banned_entities(RateLimitedEntityType::api_key);
    bool found = banned_entities[0].entity.entity_type == RateLimitedEntityType::api_key && banned_entities[0].entity.entity_id == "test";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyTemp) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 1},
        {"max_requests_1h", 1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"},{RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"},{RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestAllowAPIKey) {
    manager->add_rule({
        {"action", "allow"},
        {"api_keys", nlohmann::json::array({"test"})}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test_"},{RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestAllowIp) {
    manager->add_rule({
        {"action", "allow"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"},{RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestThrottleAPIKey) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 1},
        {"max_requests_1h", 1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestDeleteRuleByID) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 1},
        {"max_requests_1h", 1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    auto rules = manager->get_all_rules();
    manager->delete_rule_by_id(rules[0].id);
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestMinuteRateLimitAPIKey) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitAPIKey) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", -1},
        {"max_requests_1h", 5}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestMinuteRateLimitIp) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitIp) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", -1},
        {"max_requests_1h", 5}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestGetAllRules) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", -1},
        {"max_requests_1h", 5}
    });
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 2);
}

TEST_F(RateLimitManagerTest, TestGetAllRulesEmpty) {
    auto rules = manager->get_all_rules();
    EXPECT_EQ(rules.size(), 0);
}

TEST_F(RateLimitManagerTest, TestGetAllRulesJSON) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1}
    });
    nlohmann::json rules = manager->get_all_rules_json();
    EXPECT_EQ(rules.is_array(), true);
    EXPECT_EQ(rules.size(), 1);
    EXPECT_EQ(rules.at(0).is_object(), true);
    EXPECT_EQ(rules.at(0).at("id").is_number(), true);
    EXPECT_EQ(rules.at(0).at("api_keys").is_array(), true);
    EXPECT_EQ(rules.at(0).at("api_keys").size(), 1);
    EXPECT_EQ(rules.at(0).at("api_keys").at(0).is_string(), true);
    EXPECT_EQ(rules.at(0).count("ip_addresses"), 0);
}

TEST_F(RateLimitManagerTest, TestAutoBan) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1},
        {"auto_ban_1m_threshold", 2},
        {"auto_ban_1m_duration_hours", 1}
    });
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    this->changeBaseTimestamp(120);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    this->changeBaseTimestamp(240);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    this->changeBaseTimestamp(60*59);
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    this->changeBaseTimestamp(60*60*2);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"})); 
}


TEST_F(RateLimitManagerTest, TestWildcardAPIKeyWithFlag) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({".*"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true}
    });
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
}

TEST_F(RateLimitManagerTest, TestWildcardAPIKeyWithoutFlag) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({".*"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1},
    });
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
}

TEST_F(RateLimitManagerTest, TestPriority) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({".*"})},
        {"max_requests_1m", 2},
        {"max_requests_1h", -1},
        {"priority", 3},
        {"apply_limit_per_entity", true}
    });
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1},
        {"priority", 1}
    });
    manager->add_rule({
        {"action", "block"},
        {"api_keys", nlohmann::json::array({"test1"})},
        {"priority", 4}
    });
    manager->add_rule({
        {"action", "allow"},
        {"api_keys", nlohmann::json::array({"test2"})},
        {"priority", 0}
    });

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test2"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test2"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test2"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
}

TEST_F(RateLimitManagerTest, TestAndRule) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1},
        {"priority", 3}
    });

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
}


TEST_F(RateLimitManagerTest, TestExceedCounter) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({".*"})},
        {"priority", 3},
        {"apply_limit_per_entity", true},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1}
    });

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));

    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));


    const auto exceeds = manager->get_exceeded_entities_json();
    EXPECT_EQ(exceeds.size(), 2);

    EXPECT_EQ(exceeds[0]["api_key"], ".*");
    EXPECT_EQ(exceeds[0]["ip"], "0.0.0.2");
    EXPECT_EQ(exceeds[0]["request_count"], 10);

    EXPECT_EQ(exceeds[1]["api_key"], ".*");
    EXPECT_EQ(exceeds[1]["ip"], "0.0.0.1");
    EXPECT_EQ(exceeds[1]["request_count"], 9);
}

TEST_F(RateLimitManagerTest, TestActiveThrottles) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    manager->_set_base_timestamp(120);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    const auto throttles = manager->get_throttled_entities_json();
    EXPECT_EQ(throttles.size(), 1);
    EXPECT_EQ(throttles[0]["ip_address"], "0.0.0.1");
    EXPECT_EQ(throttles[0].count("api_key"), 0);
    EXPECT_EQ(throttles[0].count("throttling_from"), 1);
    EXPECT_EQ(throttles[0].count("throttling_to"), 1);
}

TEST_F(RateLimitManagerTest, TestMultiSearchRateLimiting) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({".*"})},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true}
    });

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    nlohmann::json body;
    body["searches"] = nlohmann::json::array();
    nlohmann::json search;
    search["collection"] = "players";
    search["filter_by"] = "score: > 100";
    body["searches"].push_back(search);
    body["searches"].push_back(search);
    body["searches"].push_back(search);
    body["searches"].push_back(search);
    body["searches"].push_back(search);

    req->embedded_params_vec.push_back(nlohmann::json::object());
    req->embedded_params_vec.push_back(nlohmann::json::object());
    req->embedded_params_vec.push_back(nlohmann::json::object());
    req->embedded_params_vec.push_back(nlohmann::json::object());
    req->embedded_params_vec.push_back(nlohmann::json::object());
    req->body = body.dump();
    req->metadata = "4:test0.0.0.1";

    EXPECT_FALSE(post_multi_search(req, res));
    EXPECT_EQ(res->status_code, 429);
    EXPECT_EQ(res->body, "{\"message\": \"Rate limit exceeded or blocked\"}");

    body.erase("searches");
    body["searches"] = nlohmann::json::array();
    body["searches"].push_back(search);
    body["searches"].push_back(search);

    req->embedded_params_vec.pop_back();
    req->embedded_params_vec.pop_back();
    req->embedded_params_vec.pop_back();

    req->body = body.dump();
    req->metadata = "4:test0.0.0.2";

    EXPECT_TRUE(post_multi_search(req, res));
    EXPECT_EQ(res->status_code, 200);
}

TEST_F(RateLimitManagerTest, TestDeleteBanByID) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({".*"})},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    manager->_set_base_timestamp(120);
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    const auto throttles = manager->get_throttled_entities_json();
    EXPECT_EQ(throttles.size(), 1);
    EXPECT_EQ(throttles[0]["ip_address"], "0.0.0.1");
    EXPECT_EQ(throttles[0].count("api_key"), 0);
    EXPECT_EQ(throttles[0].count("throttling_from"), 1);
    EXPECT_EQ(throttles[0].count("throttling_to"), 1);

    EXPECT_TRUE(manager->delete_ban_by_id(throttles[0]["id"]));
    EXPECT_EQ(manager->get_throttled_entities_json().size(), 0);
}


TEST_F(RateLimitManagerTest, TestInvalidRules) {
    auto res = manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1", "0.0.0.2"})},
        {"api_keys", nlohmann::json::array({"test1", "test2"})},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(res.ok());
    EXPECT_EQ(400, res.code());
    EXPECT_EQ("Many to many rule is not supported.", res.error());

     res = manager->add_rule({
        {"action", "throttle"},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(res.ok());
    EXPECT_EQ(400, res.code());
    EXPECT_EQ("Parameter `ip_addresses` or `api_keys` is required.", res.error());


    res = manager->add_rule({
        {"ip_addresses", nlohmann::json::array({".*"})},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(res.ok());
    EXPECT_EQ(400, res.code());
    EXPECT_EQ("Parameter `action` is required.", res.error());

    res = manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", ".*"},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(res.ok());
    EXPECT_EQ(400, res.code());
    EXPECT_EQ("Parameter `ip_addresses` must be an array of strings.", res.error());

    res = manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({".*"})},
        {"priority", 3}
    });

    EXPECT_FALSE(res.ok());
    EXPECT_EQ(400, res.code());
    EXPECT_EQ("At least  one of `max_requests_1m` or `max_requests_1h` is required.", res.error());

    res = manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({".*"})},
        {"priority", 3},
        {"max_requests_1m", "aa"}
    });

    EXPECT_FALSE(res.ok());
    EXPECT_EQ(400, res.code());
    EXPECT_EQ("Parameter `max_requests_1m` must be an integer.", res.error());

    res = manager->add_rule({
        {"action", "invalid"},
        {"ip_addresses", nlohmann::json::array({".*"})},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", 3},
        {"apply_limit_per_entity", true},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(res.ok());
    EXPECT_EQ(400, res.code());
    EXPECT_EQ("Invalid action.", res.error()); 
}

TEST_F(RateLimitManagerTest, TestOneToManyRule) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1", "0.0.0.2"})},
        {"api_keys", nlohmann::json::array({"test"})},
        {"priority", 3},
        {"max_requests_1m", 2},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true},
        {"auto_ban_1m_threshold", 1},
        {"auto_ban_1m_duration_hours", 1}
    });

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.2"}));

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.2"}));
}

TEST_F(RateLimitManagerTest, TestDeleteThrottleByID) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({".*"})},
        {"priority", 3},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1},
        {"apply_limit_per_entity", true}
    });

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    auto exceeds = manager->get_exceeded_entities_json();
    EXPECT_EQ(1, exceeds.size());
    auto id = exceeds[0]["id"];
    auto res = manager->delete_throttle_by_id(id);
    EXPECT_TRUE(res);
    exceeds = manager->get_exceeded_entities_json();
    EXPECT_EQ(0, exceeds.size());
    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

}

TEST_F(RateLimitManagerTest, TestOneToManyFillTest) {
    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"api_keys", nlohmann::json::array({"test", "test1", "test2"})},
        {"priority", 3},
    });

    EXPECT_TRUE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));

    auto rules = manager->get_all_rules_json();
    EXPECT_EQ(1, rules.size());
    manager->delete_rule_by_id(rules[0]["id"]);
    EXPECT_EQ(0, manager->get_all_rules_json().size());

    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"api_keys", nlohmann::json::array({"test", "test2"})},
        {"priority", 3},
    });

    LOG(INFO) << manager->get_all_rules_json();

    EXPECT_FALSE(manager->is_rate_limited({RateLimitedEntityType::api_key, "test1"}, {RateLimitedEntityType::ip, "0.0.0.1"}));
}
