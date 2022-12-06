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
        {"auto_ban_threshold_num", 10},
        {"auto_ban_num_hours", 1}
    });

    LOG(INFO) << res.error();


    EXPECT_EQ(manager->get_all_rules().size(), 1);
}

TEST_F(RateLimitManagerTest, TestAddRateLimitIp) {
    auto res = manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_threshold_num", 10},
        {"auto_ban_num_hours", 1}
    });

    EXPECT_EQ(manager->get_all_rules().size(), 1);
}

TEST_F(RateLimitManagerTest, TestRemoveRateLimitApiKey) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_threshold_num", 10},
        {"auto_ban_num_hours", 1}
    });
    EXPECT_EQ(manager->get_all_rules().size(), 1);
    manager->remove_rule_entity(RateLimitedEntityType::api_key, "test");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestRemoveRateLimitIp) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_threshold_num", 10},
        {"auto_ban_num_hours", 1}
    });
    EXPECT_EQ(manager->get_all_rules().size(), 1);
    manager->remove_rule_entity(RateLimitedEntityType::ip, "0.0.0.1");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
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
        {"auto_ban_threshold_num", 10},
        {"auto_ban_num_hours", 1}
    });
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::throttle && entities[0].max_requests.minute_threshold == 10 && entities[0].max_requests.hour_threshold == 100;
    found = found && entities[0].entity_type == RateLimitedEntityType::ip && entities[0].entity_ids[0] == "0.0.0.1";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestGetTrackedApiKeys) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 10},
        {"max_requests_1h", 100},
        {"auto_ban_threshold_num", 10},
        {"auto_ban_num_hours", 1}
    });
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::throttle && entities[0].max_requests.minute_threshold == 10 && entities[0].max_requests.hour_threshold == 100;
    found = found && entities[0].entity_type == RateLimitedEntityType::api_key && entities[0].entity_ids[0] == "test";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestBanIpPermanently) {
    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::block && entities[0].entity_type == RateLimitedEntityType::ip && entities[0].entity_ids[0] == "0.0.0.1";
    EXPECT_TRUE(found);
}

TEST_F(RateLimitManagerTest, TestUnbanIp) {
    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    auto entities = manager->get_all_rules();
    bool found = entities[0].action == RateLimitAction::block && entities[0].entity_type == RateLimitedEntityType::ip && entities[0].entity_ids[0] == "0.0.0.1";
    EXPECT_TRUE(found);
    manager->remove_rule_entity(RateLimitedEntityType::ip, "0.0.0.1");
    EXPECT_EQ(manager->get_all_rules().size(), 0);
}

TEST_F(RateLimitManagerTest, TestIsBannedIp) {
    manager->add_rule({
        {"action", "block"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    EXPECT_TRUE(manager->get_banned_entities(RateLimitedEntityType::ip).size() == 1);
    auto entities = manager->get_banned_entities(RateLimitedEntityType::ip);
    bool found = entities[0].entity_type == RateLimitedEntityType::ip && entities[0].value == "0.0.0.1";
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
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestIsBannedAPIKeyPermanently) {
    manager->add_rule({
        {"action", "block"},
        {"api_keys", nlohmann::json::array({"test"})}
    });
    EXPECT_TRUE(manager->get_banned_entities(RateLimitedEntityType::api_key).size() == 1);
    auto entities = manager->get_banned_entities(RateLimitedEntityType::api_key);
    bool found = entities[0].entity_type == RateLimitedEntityType::api_key && entities[0].value == "test";
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
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
}

TEST_F(RateLimitManagerTest, TestAllowAPIKey) {
    manager->add_rule({
        {"action", "allow"},
        {"api_keys", nlohmann::json::array({"test"})}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test_"}}));
}

TEST_F(RateLimitManagerTest, TestAllowIp) {
    manager->add_rule({
        {"action", "allow"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestThrottleAPIKey) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 1},
        {"max_requests_1h", 1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
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
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitAPIKey) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", -1},
        {"max_requests_1h", 5}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
}

TEST_F(RateLimitManagerTest, TestMinuteRateLimitIp) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
}

TEST_F(RateLimitManagerTest, TestHourRateLimitIp) {
    manager->add_rule({
        {"action", "throttle"},
        {"ip_addresses", nlohmann::json::array({"0.0.0.1"})},
        {"max_requests_1m", -1},
        {"max_requests_1h", 5}
    });
    EXPECT_TRUE(manager->get_all_rules().size() == 1);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::ip, "0.0.0.1"}}));
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
    EXPECT_EQ(rules.at(0).at("entity_type").is_string(), true);
    EXPECT_EQ(rules.at(0).at("api_keys").is_array(), true);
}

TEST_F(RateLimitManagerTest, TestAutoBan) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1},
        {"auto_ban_threshold_num", 2},
        {"auto_ban_num_hours", 1}
    });
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    this->changeBaseTimestamp(120);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    this->changeBaseTimestamp(60*60);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}})); 
}


TEST_F(RateLimitManagerTest, TestWildcard) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({".*"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1}
    });
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test1"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test1"}}));
}

TEST_F(RateLimitManagerTest, TestCorrectOrderofRules) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({".*"})},
        {"max_requests_1m", 2},
        {"max_requests_1h", -1}
    });
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1}
    });
    manager->add_rule({
        {"action", "block"},
        {"api_keys", nlohmann::json::array({"test1"})},
    });
    manager->add_rule({
        {"action", "allow"},
        {"api_keys", nlohmann::json::array({"test2"})},
    });
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test3"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test3"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test3"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test1"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test2"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test2"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test2"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test2"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test2"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test2"}}));
}

TEST_F(RateLimitManagerTest, TestAutoBannedEntitiesList) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({"test"})},
        {"max_requests_1m", 5},
        {"max_requests_1h", -1},
        {"auto_ban_threshold_num", 1},
        {"auto_ban_num_hours", 3}
    });
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    this->changeBaseTimestamp(120);
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_FALSE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));
    EXPECT_TRUE(manager->is_rate_limited({{RateLimitedEntityType::api_key, "test"}}));

    auto throttled_entities = manager->get_all_throttled_entities_json();
    EXPECT_EQ(throttled_entities.size(), 1);
    EXPECT_EQ(throttled_entities["active"][0]["api_key"], "test");

    manager->delete_throttle_by_id(throttled_entities["active"][0]["id"]);

    throttled_entities = manager->get_all_throttled_entities_json();
    EXPECT_EQ(throttled_entities.size(), 0);
}

TEST_F(RateLimitManagerTest, TestMultiSearchRateLimit) {
    manager->add_rule({
        {"action", "throttle"},
        {"api_keys", nlohmann::json::array({".*"})},
        {"max_requests_1m", 3},
        {"max_requests_1h", -1}
    });
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json search,body;
    
    search["collection"] = "cars";
    search["query_by"] = "brand";
    search["q"] = "bmw";
    body["searches"] = nlohmann::json::array({search, search, search, search, search, search});
    req->embedded_params_vec.resize(6);
    req->metadata = "4:test0.0.0.0";
    req->body = body.dump();

    post_multi_search(req, res);

    EXPECT_EQ(res->status_code, 429);
    EXPECT_EQ(res->body, "{\"message\": \"Rate limit exceeded.\"}");
}