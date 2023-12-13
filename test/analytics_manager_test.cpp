#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include <analytics_manager.h>
#include "collection.h"
#include "core_api.h"

class AnalyticsManagerTest : public ::testing::Test {
protected:
    Store *store, *analytics_store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    AnalyticsManager& analyticsManager = AnalyticsManager::get_instance();

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/analytics_manager_test";
        std::string analytics_db_path = "/tmp/typesense_test/analytics_db";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
        system("mkdir -p /tmp/typesense_test/models");

        store = new Store(state_dir_path);
        analytics_store = new Store(analytics_db_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        analyticsManager.init(store, analytics_store);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
        delete analytics_store;
    }
};

TEST_F(AnalyticsManagerTest, AddSuggestion) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    nlohmann::json doc;
    doc["title"] = "Cool trousers";
    ASSERT_TRUE(titles_coll->add(doc.dump()).ok());

    // create a collection to store suggestions
    nlohmann::json suggestions_schema = R"({
        "name": "top_queries",
        "fields": [
          {"name": "q", "type": "string" },
          {"name": "count", "type": "int32" }
        ]
      })"_json;

    Collection* suggestions_coll = collectionManager.create_collection(suggestions_schema).get();

    nlohmann::json analytics_rule = R"({
        "name": "top_search_queries",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    std::string q = "foobar";
    analyticsManager.add_suggestion("titles", q, true, "1");

    auto popularQueries = analyticsManager.get_popular_queries();
    auto userQueries = popularQueries["top_queries"]->get_user_prefix_queries()["1"];
    ASSERT_EQ(1, userQueries.size());
    ASSERT_EQ("foobar", userQueries[0].query);

    // add another query which is more popular
    q = "buzzfoo";
    analyticsManager.add_suggestion("titles", q, true, "1");
    analyticsManager.add_suggestion("titles", q, true, "2");
    analyticsManager.add_suggestion("titles", q, true, "3");

    popularQueries = analyticsManager.get_popular_queries();
    userQueries = popularQueries["top_queries"]->get_user_prefix_queries()["1"];
    ASSERT_EQ(2, userQueries.size());
    ASSERT_EQ("foobar", userQueries[0].query);
    ASSERT_EQ("buzzfoo", userQueries[1].query);

    ASSERT_TRUE(analyticsManager.remove_rule("top_search_queries").ok());
}

TEST_F(AnalyticsManagerTest, GetAndDeleteSuggestions) {
    nlohmann::json analytics_rule = R"({
        "name": "top_search_queries",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    analytics_rule = R"({
        "name": "top_search_queries2",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("There's already another configuration for this destination collection.", create_op.error());

    analytics_rule = R"({
        "name": "top_search_queries2",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"]
            },
            "destination": {
                "collection": "top_queries2"
            }
        }
    })"_json;
    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    auto rules = analyticsManager.list_rules().get()["rules"];
    ASSERT_EQ(2, rules.size());

    ASSERT_TRUE(analyticsManager.get_rule("top_search_queries").ok());
    ASSERT_TRUE(analyticsManager.get_rule("top_search_queries2").ok());

    auto missing_rule_op = analyticsManager.get_rule("top_search_queriesX");
    ASSERT_FALSE(missing_rule_op.ok());
    ASSERT_EQ(404, missing_rule_op.code());
    ASSERT_EQ("Rule not found.", missing_rule_op.error());

    // upsert rule that already exists
    analytics_rule = R"({
        "name": "top_search_queries2",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"]
            },
            "destination": {
                "collection": "top_queriesUpdated"
            }
        }
    })"_json;
    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());
    auto existing_rule = analyticsManager.get_rule("top_search_queries2").get();
    ASSERT_EQ("top_queriesUpdated", existing_rule["params"]["destination"]["collection"].get<std::string>());

    // reject when upsert is not enabled
    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("There's already another configuration with the name `top_search_queries2`.", create_op.error());

    // try deleting both rules
    ASSERT_TRUE(analyticsManager.remove_rule("top_search_queries").ok());
    ASSERT_TRUE(analyticsManager.remove_rule("top_search_queries2").ok());

    missing_rule_op = analyticsManager.get_rule("top_search_queries");
    ASSERT_FALSE(missing_rule_op.ok());
    missing_rule_op = analyticsManager.get_rule("top_search_queries2");
    ASSERT_FALSE(missing_rule_op.ok());
}

TEST_F(AnalyticsManagerTest, ClickEventsValidation) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    //wrong type
    nlohmann::json event1 = R"({
        "type": "click",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "position": 2,
            "user_id": "13"
        }
    })"_json;

    req->body = event1.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event_type click not found.\"}", res->body);

    //missing query param
    nlohmann::json event2 = R"({
        "type": "query_click",
        "data": {
            "collection": "titles",
            "doc_id": "21",
            "position": 2,
            "user_id": "13"
        }
    })"_json;

    req->body = event2.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event json data fields should contain `q`, `doc_id`, `position`, `user_id`, and `collection`.\"}", res->body);

    //should be string type
    nlohmann::json event3 = R"({
        "type": "query_click",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": 21,
            "position": 2,
            "user_id": "13"
        }
    })"_json;

    req->body = event3.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"`doc_id` value should be string.\"}", res->body);

    //correct params
    nlohmann::json event4 = R"({
        "type": "query_click",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "position": 2,
            "user_id": "13"
        }
    })"_json;

    req->body = event4.dump();
    ASSERT_TRUE(post_create_event(req, res));
}

TEST_F(AnalyticsManagerTest, ClickEventsStoreRetrieveal) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json event1 = R"({
        "type": "query_click",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "position": 2,
            "user_id": "13"
        }
    })"_json;

    req->body = event1.dump();
    ASSERT_TRUE(post_create_event(req, res));

    nlohmann::json event2 = R"({
        "type": "query_click",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "position": 4,
            "user_id": "11"
        }
    })"_json;

    req->body = event2.dump();
    ASSERT_TRUE(post_create_event(req, res));

    event1["collection_id"] = "0";
    event1["timestamp"] = 1521512521;
    event1["event_type"] = "click_events";
    event2["collection_id"] = "0";
    event2["timestamp"] = 1521514354;
    event2["event_type"] = "click_events";

    nlohmann::json click_events = nlohmann::json::array();
    click_events.push_back(event1);
    click_events.push_back(event2);

    req->body = click_events.dump();
    ASSERT_TRUE(post_replicate_events(req, res));

    auto result = analyticsManager.get_click_events();

    ASSERT_EQ("0", result[0]["collection_id"]);
    ASSERT_EQ("13", result[0]["data"]["user_id"]);
    ASSERT_EQ("21", result[0]["data"]["doc_id"]);
    ASSERT_EQ(2, result[0]["data"]["position"]);
    ASSERT_EQ("technology", result[0]["data"]["q"]);

    ASSERT_EQ("0", result[1]["collection_id"]);
    ASSERT_EQ("11", result[1]["data"]["user_id"]);
    ASSERT_EQ("21", result[1]["data"]["doc_id"]);
    ASSERT_EQ(4, result[1]["data"]["position"]);
    ASSERT_EQ("technology", result[1]["data"]["q"]);
}

TEST_F(AnalyticsManagerTest, ClickEventsRateLimitTest) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json event = R"({
        "type": "query_click",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "position": 2,
            "user_id": "13"
        }
    })"_json;

    //reset the LRU cache to test the rate limit
    analyticsManager.resetRateLimit();

    req->body = event.dump();
    for(auto i = 0; i < 5; ++i) {
        ASSERT_TRUE(post_create_event(req, res));
    }

    //as rate limit is 5, adding one more event above that should trigger rate limit
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"click event rate limit reached.\"}", res->body);
}

TEST_F(AnalyticsManagerTest, NoresultsQueries) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    nlohmann::json doc;
    doc["title"] = "Cool trousers";
    ASSERT_TRUE(titles_coll->add(doc.dump()).ok());

    nlohmann::json suggestions_schema = R"({
        "name": "top_queries",
        "fields": [
          {"name": "q", "type": "string" },
          {"name": "count", "type": "int32" }
        ]
      })"_json;

    Collection* suggestions_coll = collectionManager.create_collection(suggestions_schema).get();

    nlohmann::json analytics_rule = R"({
        "name": "search_queries",
        "type": "nohits_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    std::string q = "foobar";
    analyticsManager.add_nohits_query("titles", q, true, "1");

    auto noresults_queries = analyticsManager.get_nohits_queries();
    auto userQueries = noresults_queries["top_queries"]->get_user_prefix_queries()["1"];

    ASSERT_EQ(1, userQueries.size());
    ASSERT_EQ("foobar", userQueries[0].query);

    //try deleting nohits_queries rule
    ASSERT_TRUE(analyticsManager.remove_rule("search_queries").ok());

    noresults_queries = analyticsManager.get_nohits_queries();
    ASSERT_EQ(0, noresults_queries.size());
}

TEST_F(AnalyticsManagerTest, QueryHitsCount) {
    //flush all events from analytics store
    analyticsManager.resetAnalyticsStore();

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection *titles_coll = collectionManager.create_collection(titles_schema).get();

    nlohmann::json doc;
    doc["title"] = "Cool trousers";
    ASSERT_TRUE(titles_coll->add(doc.dump()).ok());

    doc["title"] = "Cool pants";
    ASSERT_TRUE(titles_coll->add(doc.dump()).ok());

    doc["title"] = "Trendy sneakers";
    ASSERT_TRUE(titles_coll->add(doc.dump()).ok());

    doc["title"] = "Funky shorts";
    ASSERT_TRUE(titles_coll->add(doc.dump()).ok());

    nlohmann::json query_hits_array = nlohmann::json::array();
    nlohmann::json obj;

    obj["collection_id"] = "0";
    obj["event_type"] = "query_hits_counts";

    obj["query"] = "cool";
    obj["timestamp"] = 1625365612;
    obj["user_id"] = "1";
    obj["hits_count"] = 2;
    query_hits_array.push_back(obj);

    obj["query"] = "funky";
    obj["timestamp"] = 1625365616;
    obj["user_id"] = "1";
    obj["hits_count"] = 1;
    query_hits_array.push_back(obj);


    auto op = analyticsManager.write_events_to_store(query_hits_array);
    ASSERT_TRUE(op.ok());

    auto result = analyticsManager.get_query_hits_counts();

    ASSERT_EQ(2, result.size());
    ASSERT_EQ("cool", result[0]["query"]);
    ASSERT_EQ(2, result[0]["hits_count"]);
    ASSERT_EQ(1625365612, result[0]["timestamp"]);

    ASSERT_EQ("funky", result[1]["query"]);
    ASSERT_EQ(1, result[1]["hits_count"]);
    ASSERT_EQ(1625365616, result[1]["timestamp"]);
}

TEST_F(AnalyticsManagerTest, EventsExpiryBasic) {
    //flush all events from analytics store
    analyticsManager.resetAnalyticsStore();

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection *titles_coll = collectionManager.create_collection(titles_schema).get();

    nlohmann::json events = nlohmann::json::array();

    auto ts = 1701851341000000;
    nlohmann::json event;
    event["event_type"] = "click_events";
    event["q"] = "technology";
    event["collection_id"] = "0";
    event["doc_id"] = "21";
    event["position"] = 2;
    event["user_id"] = 13;
    event["timestamp"] = ts;
    events.push_back(event);

    event["doc_id"] = "12";
    event["position"] = 3;
    event["timestamp"] = ts+1000000; //1 sec later
    events.push_back(event);

    ASSERT_TRUE(analyticsManager.write_events_to_store(events).ok());

    nlohmann::json resp = analyticsManager.get_click_events();
    ASSERT_EQ(2, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ("21", resp[0]["doc_id"]);
    ASSERT_EQ(2, resp[0]["position"]);
    ASSERT_EQ(13, resp[0]["user_id"]);

    ASSERT_EQ("technology", resp[1]["q"]);
    ASSERT_EQ("12", resp[1]["doc_id"]);
    ASSERT_EQ(3, resp[1]["position"]);
    ASSERT_EQ(13, resp[1]["user_id"]);

    analyticsManager.checkEventsExpiry();

    resp = analyticsManager.get_click_events();
    ASSERT_EQ(0, resp.size());

    //add query hits events with click events
    nlohmann::json event2;
    event2["event_type"] = "query_hits_counts";
    event2["q"] = "technology";
    event2["collection_id"] = "0";
    event2["user_id"] = 13;
    event2["hits_count"] = 124;
    event2["timestamp"] = ts + 10000000; //after 10s

    events.push_back(event2);

    ASSERT_TRUE(analyticsManager.write_events_to_store(events).ok());

    resp = analyticsManager.get_query_hits_counts();
    ASSERT_EQ(1, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ(13, resp[0]["user_id"]);
    ASSERT_EQ(124, resp[0]["hits_count"]);

    //now old click events will be deleted on checking expiry but query hits events will be remaining
    // assumming ttl is ts + 5 sec
    analyticsManager.checkEventsExpiry();

    resp = analyticsManager.get_click_events();
    ASSERT_EQ(0, resp.size());

    resp = analyticsManager.get_query_hits_counts();
    ASSERT_EQ(1, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ(13, resp[0]["user_id"]);
    ASSERT_EQ(124, resp[0]["hits_count"]);
}

TEST_F(AnalyticsManagerTest, EventsExpiryAll) {
    //flush all events from analytics store
    analyticsManager.resetAnalyticsStore();

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection *titles_coll = collectionManager.create_collection(titles_schema).get();

    nlohmann::json events = nlohmann::json::array();

    auto ts = 1701851341000000;
    nlohmann::json event;
    event["event_type"] = "click_events";
    event["q"] = "technology";
    event["collection_id"] = "0";
    event["doc_id"] = "21";
    event["position"] = 2;
    event["user_id"] = 13;
    event["timestamp"] = ts;
    events.push_back(event);

    event["doc_id"] = "12";
    event["position"] = 3;
    event["timestamp"] = ts + 1000000; //after 1s
    events.push_back(event);

    nlohmann::json event2;
    event2["event_type"] = "query_hits_counts";
    event2["q"] = "technology";
    event2["collection_id"] = "0";
    event2["user_id"] = 13;
    event2["hits_count"] = 124;
    event2["timestamp"] = ts + 2000000; //after 2s

    events.push_back(event2);

    ASSERT_TRUE(analyticsManager.write_events_to_store(events).ok());

    nlohmann::json resp = analyticsManager.get_click_events();
    ASSERT_EQ(2, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ("21", resp[0]["doc_id"]);
    ASSERT_EQ(2, resp[0]["position"]);
    ASSERT_EQ(13, resp[0]["user_id"]);

    ASSERT_EQ("technology", resp[1]["q"]);
    ASSERT_EQ("12", resp[1]["doc_id"]);
    ASSERT_EQ(3, resp[1]["position"]);
    ASSERT_EQ(13, resp[1]["user_id"]);

    resp = analyticsManager.get_query_hits_counts();
    ASSERT_EQ(1, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ(13, resp[0]["user_id"]);
    ASSERT_EQ(124, resp[0]["hits_count"]);

    //check for events expiry
    // assuming ttl is ts + 5 sec
    analyticsManager.checkEventsExpiry();

    resp = analyticsManager.get_click_events();
    ASSERT_EQ(0, resp.size());

    resp = analyticsManager.get_query_hits_counts();
    ASSERT_EQ(0, resp.size());
}

TEST_F(AnalyticsManagerTest, EventsExpiryPartial) {
    //flush all events from analytics store
    analyticsManager.resetAnalyticsStore();

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection *titles_coll = collectionManager.create_collection(titles_schema).get();

    nlohmann::json events = nlohmann::json::array();

    auto ts = 1701851341000000;
    nlohmann::json event;
    event["event_type"] = "click_events";
    event["q"] = "technology";
    event["collection_id"] = "0";
    event["doc_id"] = "21";
    event["position"] = 2;
    event["user_id"] = 13;
    event["timestamp"] = ts;
    events.push_back(event);

    event["doc_id"] = "12";
    event["position"] = 3;
    event["timestamp"] = ts + 1000000; //after 1s
    events.push_back(event);

    event["doc_id"] = "19";
    event["position"] = 1;
    event["timestamp"] = ts + 6000000; //after 6s
    events.push_back(event);

    nlohmann::json event2;
    event2["event_type"] = "query_hits_counts";
    event2["q"] = "technology";
    event2["collection_id"] = "0";
    event2["user_id"] = 13;
    event2["hits_count"] = 124;
    event2["timestamp"] = ts;
    events.push_back(event2);

    event2["q"] = "industry";
    event2["user_id"] = 13;
    event2["hits_count"] = 214;
    event2["timestamp"] = ts + 2000000; //after 2s
    events.push_back(event2);

    event2["q"] = "management";
    event2["user_id"] = 13;
    event2["hits_count"] = 834;
    event2["timestamp"] = ts + 8000000; //after 8s
    events.push_back(event2);

    ASSERT_TRUE(analyticsManager.write_events_to_store(events).ok());

    nlohmann::json resp = analyticsManager.get_click_events();
    ASSERT_EQ(3, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ("21", resp[0]["doc_id"]);
    ASSERT_EQ(2, resp[0]["position"]);
    ASSERT_EQ(13, resp[0]["user_id"]);

    ASSERT_EQ("technology", resp[1]["q"]);
    ASSERT_EQ("12", resp[1]["doc_id"]);
    ASSERT_EQ(3, resp[1]["position"]);
    ASSERT_EQ(13, resp[1]["user_id"]);

    ASSERT_EQ("technology", resp[2]["q"]);
    ASSERT_EQ("19", resp[2]["doc_id"]);
    ASSERT_EQ(1, resp[2]["position"]);
    ASSERT_EQ(13, resp[2]["user_id"]);

    resp = analyticsManager.get_query_hits_counts();
    ASSERT_EQ(3, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ(13, resp[0]["user_id"]);
    ASSERT_EQ(124, resp[0]["hits_count"]);

    ASSERT_EQ("industry", resp[1]["q"]);
    ASSERT_EQ(13, resp[1]["user_id"]);
    ASSERT_EQ(214, resp[1]["hits_count"]);

    ASSERT_EQ("management", resp[2]["q"]);
    ASSERT_EQ(13, resp[2]["user_id"]);
    ASSERT_EQ(834, resp[2]["hits_count"]);

    //check for events expiry
    // assuming ttl is ts + 5 sec
    //only events in ttl interval will be removed
    analyticsManager.checkEventsExpiry();

    resp = analyticsManager.get_click_events();
    ASSERT_EQ(1, resp.size());
    ASSERT_EQ("technology", resp[0]["q"]);
    ASSERT_EQ("19", resp[0]["doc_id"]);
    ASSERT_EQ(1, resp[0]["position"]);
    ASSERT_EQ(13, resp[0]["user_id"]);

    resp = analyticsManager.get_query_hits_counts();
    ASSERT_EQ(1, resp.size());
    ASSERT_EQ("management", resp[0]["q"]);
    ASSERT_EQ(13, resp[0]["user_id"]);
    ASSERT_EQ(834, resp[0]["hits_count"]);
}