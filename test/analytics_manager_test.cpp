#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include <analytics_manager.h>
#include "collection.h"
#include "core_api.h"

class AnalyticsManagerTest : public ::testing::Test {
protected:
    Store *store;
    Store *analytic_store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    std::string state_dir_path, analytics_dir_path;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    AnalyticsManager& analyticsManager = AnalyticsManager::get_instance();
    uint32_t analytics_minute_rate_limit = 5;

    void setupCollection() {
        state_dir_path = "/tmp/typesense_test/analytics_manager_test";
        analytics_dir_path = "/tmp/typesense-test/analytics";

        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
        system("mkdir -p /tmp/typesense_test/models");

        store = new Store(state_dir_path);

        LOG(INFO) << "Truncating and creating: " << analytics_dir_path;
        system(("rm -rf "+ analytics_dir_path +" && mkdir -p "+analytics_dir_path).c_str());
        analytic_store = new Store(analytics_dir_path, 24*60*60, 1024, true, FOURWEEKS_SECS);

        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        analyticsManager.init(store, analytic_store, analytics_minute_rate_limit);
        analyticsManager.resetToggleRateLimit(false);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
        delete analytic_store;
        analyticsManager.stop();
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

    std::string q = "coo";
    analyticsManager.add_suggestion("titles", q, "cool", true, "1");

    auto popularQueries = analyticsManager.get_popular_queries();
    auto userQueries = popularQueries["top_queries"]->get_user_prefix_queries()["1"];
    ASSERT_EQ(1, userQueries.size());
    ASSERT_EQ("coo", userQueries[0].query);  // expanded query is NOT stored since it's not enabled

    // add another query which is more popular
    q = "buzzfoo";
    analyticsManager.add_suggestion("titles", q, q, true, "1");
    analyticsManager.add_suggestion("titles", q, q, true, "2");
    analyticsManager.add_suggestion("titles", q, q, true, "3");

    popularQueries = analyticsManager.get_popular_queries();
    userQueries = popularQueries["top_queries"]->get_user_prefix_queries()["1"];
    ASSERT_EQ(2, userQueries.size());
    ASSERT_EQ("coo", userQueries[0].query);
    ASSERT_EQ("buzzfoo", userQueries[1].query);

    ASSERT_TRUE(analyticsManager.remove_rule("top_search_queries").ok());
}

TEST_F(AnalyticsManagerTest, AddSuggestionWithExpandedQuery) {
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
            "expand_query": true,
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

    analyticsManager.add_suggestion("titles", "c", "cool", true, "1");

    auto popularQueries = analyticsManager.get_popular_queries();
    auto userQueries = popularQueries["top_queries"]->get_user_prefix_queries()["1"];
    ASSERT_EQ(1, userQueries.size());
    ASSERT_EQ("cool", userQueries[0].query);

    ASSERT_TRUE(analyticsManager.remove_rule("top_search_queries").ok());
}

TEST_F(AnalyticsManagerTest, GetAndDeleteSuggestions) {
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
        "name": "top_search_queries3",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": [241, 2353]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Source collections value should be a string.", create_op.error());

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

TEST_F(AnalyticsManagerTest, EventsValidation) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    nlohmann::json titles1_schema = R"({
            "name": "titles_1",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles1_coll = collectionManager.create_collection(titles1_schema).get();

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    auto analytics_rule = R"({
        "name": "product_events",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "AP"}, {"type": "visit", "name": "VP"}]
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    //wrong type
    nlohmann::json event1 = R"({
        "type": "query_click",
        "name": "AP",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event1.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event_type query_click not found.\"}", res->body);

    //missing name
    event1 = R"({
        "type": "click",
        "data": {
            "collection": "titles",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event1.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"key `name` not found.\"}", res->body);

    //should be string type
    nlohmann::json event3 = R"({
        "type": "conversion",
        "name": "AP",
        "data": {
            "q": "technology",
            "doc_id": 21,
            "user_id": "13"
        }
    })"_json;

    req->body = event3.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event should have 'doc_id' as string value.\"}", res->body);

    event3 = R"({
        "type": "conversion",
        "name": "AP",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": 12
        }
    })"_json;

    req->body = event3.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event should have 'user_id' as string value.\"}", res->body);

    event3 = R"({
        "type": "conversion",
        "name": "AP",
        "data": {
            "q": 1245,
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event3.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"'q' should be a string value.\"}", res->body);

    //event name should be unique
    analytics_rule = R"({
        "name": "product_click_events2",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "AP"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Events must contain a unique name.", create_op.error());

    //wrong event name
    nlohmann::json event4 = R"({
        "type": "visit",
        "name": "AB",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "11"
        }
    })"_json;

    req->body = event4.dump();
    ASSERT_FALSE(post_create_event(req, res));

    //correct params
    nlohmann::json event5 = R"({
        "type": "click",
        "name": "AP",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event5.dump();
    ASSERT_TRUE(post_create_event(req, res));

    nlohmann::json event6 = R"({
        "type": "visit",
        "name": "VP",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "11"
        }
    })"_json;

    req->body = event6.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //wrong event type
    nlohmann::json event7 = R"({
        "type": "conversion",
        "name": "VP",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "11"
        }
    })"_json;

    req->body = event7.dump();
    ASSERT_FALSE(post_create_event(req, res));

    //custom event
    analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "custom", "name": "CP"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json event8 = R"({
        "type": "custom",
        "name": "CP",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "11",
            "label1": "foo",
            "label2": "bar",
            "info": "xyz"
        }
    })"_json;
    req->body = event8.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //deleting rule should delete events associated with it
    req->params["name"] = "product_events2";
    ASSERT_TRUE(del_analytics_rules(req, res));
    analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "custom", "name": "CP"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    // log based event should be created with only doc_id and user_id
    event5 = R"({
        "type": "click",
        "name": "AP",
        "data": {
            "doc_id": "21",
            "user_id": "123"
        }
    })"_json;

    req->body = event5.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //search events validation

    nlohmann::json suggestions_schema = R"({
        "name": "top_queries",
        "fields": [
          {"name": "q", "type": "string" },
          {"name": "count", "type": "int32" }
        ]
      })"_json;

    Collection* suggestions_coll = collectionManager.create_collection(suggestions_schema).get();

    analytics_rule = R"({
        "name": "popular_searches",
        "type": "popular_queries",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "search", "name": "PS1"}]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    analytics_rule = R"({
        "name": "nohits_searches",
        "type": "nohits_queries",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "search", "name": "NH1"}]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    //missing query param
    auto event9 = R"({
        "type": "search",
        "name": "NH1",
        "data": {
            "user_id": "11"
        }
    })"_json;
    req->body = event9.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"search event json data fields should contain `q` as string value.\"}", res->body);

    event9 = R"({
        "type": "search",
        "name": "NH1",
        "data": {
            "q": "11"
        }
    })"_json;
    req->body = event9.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"search event json data fields should contain `user_id` as string value.\"}", res->body);

    //correct params
    event9 = R"({
        "type": "search",
        "name": "NH1",
        "data": {
            "q": "tech",
            "user_id": "11"
        }
    })"_json;
    req->body = event9.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //for log events source collections is not optional
    req->params["name"] = "product_events2";
    ASSERT_TRUE(del_analytics_rules(req, res));
    analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                 "events":  [{"type": "custom", "name": "CP"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Must contain a valid list of source collections.", create_op.error());

    //try adding removed events
    ASSERT_TRUE(analyticsManager.remove_rule("product_events").ok());

    analytics_rule = R"({
        "name": "product_events",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "AP"}, {"type": "visit", "name": "VP"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                 "collections": ["titles", "titles_1"],
                 "events":  [{"type": "click", "name": "CP"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    event9 = R"({
        "type": "click",
        "name": "CP",
        "data": {
            "doc_id": "12",
            "user_id": "11"
        }
    })"_json;
    req->body = event9.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"Multiple source collections. 'collection' should be specified\"}", res->body);

    event9 = R"({
        "type": "click",
        "name": "CP",
        "data": {
            "doc_id": "12",
            "user_id": "11",
            "collection": "titles"
        }
    })"_json;
    req->body = event9.dump();
    ASSERT_TRUE(post_create_event(req, res));
}

TEST_F(AnalyticsManagerTest, EventsPersist) {
    //remove all rules first
    analyticsManager.remove_all_rules();

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection *titles_coll = collectionManager.create_collection(titles_schema).get();

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    auto analytics_rule = R"({
        "name": "product_click_events",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "APC"}]
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json event = R"({
        "type": "click",
        "name": "APC",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //get events
    nlohmann::json payload = nlohmann::json::array();
    nlohmann::json event_data;
    auto collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;
        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    //manually trigger write to db
    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    std::vector<std::string> values;
    analyticsManager.get_last_N_events("13", "*", 5, values);
    ASSERT_EQ(1, values.size());

    auto parsed_json = nlohmann::json::parse(values[0]);

    ASSERT_EQ("APC", parsed_json["name"]);
    ASSERT_EQ("titles", parsed_json["collection"]);
    ASSERT_EQ("13", parsed_json["user_id"]);
    ASSERT_EQ("21", parsed_json["doc_id"]);
    ASSERT_EQ("technology", parsed_json["query"]);

    event = R"({
        "type": "click",
        "name": "APC",
        "data": {
            "q": "technology",
            "doc_id": "12",
            "user_id": "13"
        }
    })"_json;

    req->body = event.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //get events
    payload.clear();
    collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;
        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    //manually trigger write to db
    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    values.clear();
    analyticsManager.get_last_N_events("13", "*", 5, values);
    ASSERT_EQ(2, values.size());

    parsed_json = nlohmann::json::parse(values[0]);

    //events will be fetched in LIFO order
    ASSERT_EQ("APC", parsed_json["name"]);
    ASSERT_EQ("titles", parsed_json["collection"]);
    ASSERT_EQ("13", parsed_json["user_id"]);
    ASSERT_EQ("12", parsed_json["doc_id"]);
    ASSERT_EQ("technology", parsed_json["query"]);

    parsed_json = nlohmann::json::parse(values[1]);

    ASSERT_EQ("APC", parsed_json["name"]);
    ASSERT_EQ("titles", parsed_json["collection"]);
    ASSERT_EQ("13", parsed_json["user_id"]);
    ASSERT_EQ("21", parsed_json["doc_id"]);
    ASSERT_EQ("technology", parsed_json["query"]);
}

TEST_F(AnalyticsManagerTest, EventsRateLimitTest) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    auto analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "AB"}]
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json event1 = R"({
        "type": "click",
        "name": "AB",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    //reset the LRU cache to test the rate limit
    analyticsManager.resetToggleRateLimit(true);

    for(auto i = 0; i < 5; ++i) {
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    //as rate limit is 5, adding one more event above that should trigger rate limit
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event rate limit reached.\"}", res->body);

    analyticsManager.resetToggleRateLimit(false);


    //try with different limit
    //restart analytics manager as fresh
    analyticsManager.dispose();
    analyticsManager.stop();

    analytics_minute_rate_limit = 20;
    analyticsManager.init(store, analytic_store, analytics_minute_rate_limit);

    analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "AB"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    event1 = R"({
        "type": "click",
        "name": "AB",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    //reset the LRU cache to test the rate limit
    analyticsManager.resetToggleRateLimit(true);

    for(auto i = 0; i < 20; ++i) {
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    //as rate limit is 20, adding one more event above that should trigger rate limit
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event rate limit reached.\"}", res->body);

    analyticsManager.resetToggleRateLimit(false);
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

TEST_F(AnalyticsManagerTest, QueryLengthTruncation) {
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
        "name": "queries",
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
                "collection": "queries"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    std::string q1 = StringUtils::randstring(1050);
    std::string q2 = StringUtils::randstring(1000);
    analyticsManager.add_nohits_query("titles", q1, true, "1");
    analyticsManager.add_nohits_query("titles", q2, true, "2");

    auto noresults_queries = analyticsManager.get_nohits_queries();
    auto userQueries = noresults_queries["queries"]->get_user_prefix_queries()["1"];
    ASSERT_EQ(0, userQueries.size());

    userQueries = noresults_queries["queries"]->get_user_prefix_queries()["2"];
    ASSERT_EQ(1, userQueries.size());
    ASSERT_EQ(q2, userQueries[0].query);

    // delete nohits_queries rule
    ASSERT_TRUE(analyticsManager.remove_rule("search_queries").ok());

    noresults_queries = analyticsManager.get_nohits_queries();
    ASSERT_EQ(0, noresults_queries.size());

    // add popularity rule
    analytics_rule = R"({
        "name": "top_search_queries",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"]
            },
            "destination": {
                "collection": "queries"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    analyticsManager.add_suggestion("titles", q1, "cool", true, "1");
    analyticsManager.add_suggestion("titles", q2, "cool", true, "2");

    auto popular_queries = analyticsManager.get_popular_queries();
    userQueries = popular_queries["queries"]->get_user_prefix_queries()["1"];
    ASSERT_EQ(0, userQueries.size());

    userQueries = popular_queries["queries"]->get_user_prefix_queries()["2"];
    ASSERT_EQ(1, userQueries.size());
    ASSERT_EQ(q2, userQueries[0].query);
}

TEST_F(AnalyticsManagerTest, SuggestionConfigRule) {
    //clear all rules first
    analyticsManager.remove_all_rules();

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();


    // create a collection to store suggestions
    nlohmann::json suggestions_schema = R"({
        "name": "top_queries",
        "fields": [
          {"name": "q", "type": "string" },
          {"name": "count", "type": "int32" }
        ]
      })"_json;

    Collection* suggestions_coll = collectionManager.create_collection(suggestions_schema).get();

    //add popular quries rule
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

    //add nohits rule
    analytics_rule = R"({
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

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    auto rules = analyticsManager.list_rules().get()["rules"];
    ASSERT_EQ(2, rules.size());
    ASSERT_EQ("search_queries", rules[0]["name"]);
    ASSERT_EQ("nohits_queries", rules[0]["type"]);
    ASSERT_EQ("top_search_queries", rules[1]["name"]);
    ASSERT_EQ("popular_queries", rules[1]["type"]);

    //try deleting rules
    ASSERT_TRUE(analyticsManager.remove_rule("search_queries").ok());
    ASSERT_TRUE(analyticsManager.remove_rule("top_search_queries").ok());
    rules = analyticsManager.list_rules().get()["rules"];
    ASSERT_EQ(0, rules.size());
}

TEST_F(AnalyticsManagerTest, PopularityScore) {

    nlohmann::json products_schema = R"({
            "name": "products",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "popularity", "type": "int32"}
            ]
        })"_json;

    Collection* products_coll = collectionManager.create_collection(products_schema).get();

    nlohmann::json doc;
    doc["popularity"] = 0;

    doc["id"] = "0";
    doc["title"] = "Cool trousers";
    ASSERT_TRUE(products_coll->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "Funky trousers";
    ASSERT_TRUE(products_coll->add(doc.dump()).ok());

    doc["id"] = "2";
    doc["title"] = "Casual shorts";
    ASSERT_TRUE(products_coll->add(doc.dump()).ok());

    doc["id"] = "3";
    doc["title"] = "Trendy shorts";
    ASSERT_TRUE(products_coll->add(doc.dump()).ok());

    doc["id"] = "4";
    doc["title"] = "Formal pants";
    ASSERT_TRUE(products_coll->add(doc.dump()).ok());

    auto analytics_rule = R"({
        "name": "product_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["products"],
                "events":  [
                    {"type": "click", "weight": 1, "name": "CLK1", "log_to_store": true},
                    {"type": "conversion", "weight": 5, "name": "CNV1", "log_to_store": true}
                ]
            },
            "destination": {
                "collection": "products",
                "counter_field": "popularity"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json event1 = R"({
        "type": "conversion",
        "name": "CNV1",
        "data": {
            "q": "trousers",
            "doc_id": "1",
            "user_id": "13"
        }
    })"_json;

    req->body = event1.dump();
    ASSERT_TRUE(post_create_event(req, res));

    nlohmann::json event2 = R"({
        "type": "click",
        "name": "CLK1",
        "data": {
            "q": "shorts",
            "doc_id": "3",
            "user_id": "11"
        }
    })"_json;

    req->body = event2.dump();
    ASSERT_TRUE(post_create_event(req, res));

    ASSERT_TRUE(post_create_event(req, res));

    auto popular_clicks = analyticsManager.get_popular_clicks();
    ASSERT_EQ(1, popular_clicks.size());
    ASSERT_EQ("popularity", popular_clicks["products"].counter_field);
    ASSERT_EQ(2, popular_clicks["products"].docid_counts.size());
    ASSERT_EQ(5, popular_clicks["products"].docid_counts["1"]);
    ASSERT_EQ(2, popular_clicks["products"].docid_counts["3"]);

    nlohmann::json event3 = R"({
        "type": "click",
        "name": "CLK1",
        "data": {
            "q": "shorts",
            "doc_id": "1",
            "user_id": "11"
        }
    })"_json;

    req->body = event3.dump();
    ASSERT_TRUE(post_create_event(req, res));

    nlohmann::json event4 = R"({
        "type": "conversion",
        "name": "CNV1",
        "data": {
            "q": "shorts",
            "doc_id": "3",
            "user_id": "11"
        }
    })"_json;

    req->body = event4.dump();
    ASSERT_TRUE(post_create_event(req, res));

    popular_clicks = analyticsManager.get_popular_clicks();
    ASSERT_EQ(1, popular_clicks.size());
    ASSERT_EQ("popularity", popular_clicks["products"].counter_field);
    ASSERT_EQ(2, popular_clicks["products"].docid_counts.size());
    ASSERT_EQ(7, popular_clicks["products"].docid_counts["3"]);
    ASSERT_EQ(6, popular_clicks["products"].docid_counts["1"]);

    //trigger persistance event manually
    for(auto& popular_clicks_it : popular_clicks) {
        std::string docs;
        req->params["collection"] = popular_clicks_it.first;
        req->params["action"] = "update";
        popular_clicks_it.second.serialize_as_docs(docs);
        req->body = docs;
        post_import_documents(req, res);
    }

    sort_fields = {sort_by("popularity", "DESC")};
    auto results = products_coll->search("*", {}, "", {},
                                         sort_fields, {0}, 10, 1, FREQUENCY,{false},
                                         Index::DROP_TOKENS_THRESHOLD,spp::sparse_hash_set<std::string>(),
                                         spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(5, results["hits"].size());

    ASSERT_EQ("3", results["hits"][0]["document"]["id"]);
    ASSERT_EQ(7, results["hits"][0]["document"]["popularity"]);
    ASSERT_EQ("Trendy shorts", results["hits"][0]["document"]["title"]);

    ASSERT_EQ("1", results["hits"][1]["document"]["id"]);
    ASSERT_EQ(6, results["hits"][1]["document"]["popularity"]);
    ASSERT_EQ("Funky trousers", results["hits"][1]["document"]["title"]);

    //after persist should able to add new events
    analyticsManager.persist_popular_events(nullptr, 0);

    nlohmann::json event5 = R"({
        "type": "conversion",
        "name": "CNV1",
        "data": {
            "q": "shorts",
            "doc_id": "3",
            "user_id": "11"
        }
    })"_json;
    req->body = event5.dump();
    ASSERT_TRUE(post_create_event(req, res));

    popular_clicks = analyticsManager.get_popular_clicks();
    ASSERT_EQ(1, popular_clicks.size());
    ASSERT_EQ("popularity", popular_clicks["products"].counter_field);
    ASSERT_EQ(1, popular_clicks["products"].docid_counts.size());
}

TEST_F(AnalyticsManagerTest, PopularityScoreValidation) {
    //restart analytics manager as fresh
    analyticsManager.dispose();
    analyticsManager.stop();
    analyticsManager.init(store, analytic_store, analytics_minute_rate_limit);

    nlohmann::json products_schema = R"({
            "name": "books",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "popularity", "type": "int32"}
            ]
        })"_json;

    Collection *products_coll = collectionManager.create_collection(products_schema).get();

    nlohmann::json doc;
    doc["popularity"] = 0;

    doc["id"] = "0";
    doc["title"] = "Cool trousers";
    ASSERT_TRUE(products_coll->add(doc.dump()).ok());

    doc["id"] = "1";
    doc["title"] = "Funky trousers";
    ASSERT_TRUE(products_coll->add(doc.dump()).ok());

    nlohmann::json analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"],
                "events":  [{"type": "click", "weight": 1, "name": "CLK2"}, {"type": "conversion", "weight": 5, "name": "CNV2"} ]
            },
            "destination": {
                "collection": "popular_books",
                "counter_field": "popularity"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Collection `popular_books` not found.", create_op.error());

    analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"],
                "events":  [{"type": "click", "weight": 1, "name": "CLK3"}, {"type": "conversion", "weight": 5, "name": "CNV3"} ]
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity_score"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("counter_field `popularity_score` not found in destination collection.", create_op.error());

    analytics_rule = R"({
        "name": "books_popularity",
        "type": "popular_click",
        "params": {
            "source": {
                "collections": ["books"],
                "events":  [{"type": "query_click", "weight": 1}, {"type": "query_purchase", "weight": 5} ]
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity_score"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Invalid type.", create_op.error());

    analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"]
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity_score"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Bad or missing events.", create_op.error());

    analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"],
                "events":  []
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity_score"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Bad or missing events.", create_op.error());

    analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"],
                "events":  "query_click"
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity_score"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Bad or missing events.", create_op.error());

    analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"],
                "events":  [{"type": "click", "weight": 1}, {"type": "conversion", "weight": 5} ]
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Events must contain a unique name.", create_op.error());


    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                 "collections": ["books"],
                 "events":  [{"type": "click", "name" : "CLK4"}, {"type": "conversion", "name": "CNV4", "log_to_store" : true} ]
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity"
            }
        }
    })"_json;
    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Counter events must contain a weight value.", create_op.error());

    //correct params
    analytics_rule = R"({
        "name": "books_popularity2",
        "type": "counter",
        "params": {
            "source": {
                 "collections": ["books"],
                 "events":  [{"type": "click", "weight": 1, "name" : "CLK4"}, {"type": "conversion", "weight": 5, "name": "CNV4", "log_to_store" : true} ]
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    auto rule_op = analyticsManager.get_rule("books_popularity2");
    ASSERT_TRUE(rule_op.ok());
    auto rule = rule_op.get();
    ASSERT_EQ(analytics_rule["params"]["source"]["events"], rule["params"]["source"]["events"]);
    ASSERT_EQ(analytics_rule["params"]["destination"]["counter_field"], rule["params"]["destination"]["counter_field"]);

    nlohmann::json event = R"({
        "type": "conversion",
        "name": "CNV4",
        "data": {
            "q": "shorts",
            "doc_id": "1",
            "user_id": "11"
        }
    })"_json;
    req->body = event.dump();
    ASSERT_TRUE(post_create_event(req, res));

    auto popular_clicks = analyticsManager.get_popular_clicks();
    ASSERT_EQ("popularity", popular_clicks["books"].counter_field);
    ASSERT_EQ(1, popular_clicks["books"].docid_counts.size());
    ASSERT_EQ(5, popular_clicks["books"].docid_counts["1"]);

    //trigger persistence event manually
    for (auto &popular_clicks_it: popular_clicks) {
        std::string docs;
        req->params["collection"] = popular_clicks_it.first;
        req->params["action"] = "update";
        popular_clicks_it.second.serialize_as_docs(docs);
        req->body = docs;
        post_import_documents(req, res);
    }

    sort_fields = {sort_by("popularity", "DESC")};
    auto results = products_coll->search("*", {}, "", {},
                                         sort_fields, {0}, 10, 1, FREQUENCY, {false},
                                         Index::DROP_TOKENS_THRESHOLD, spp::sparse_hash_set<std::string>(),
                                         spp::sparse_hash_set<std::string>()).get();

    ASSERT_EQ(2, results["hits"].size());

    ASSERT_EQ("1", results["hits"][0]["document"]["id"]);
    ASSERT_EQ(5, results["hits"][0]["document"]["popularity"]);
    ASSERT_EQ("Funky trousers", results["hits"][0]["document"]["title"]);

    ASSERT_EQ("0", results["hits"][1]["document"]["id"]);
    ASSERT_EQ(0, results["hits"][1]["document"]["popularity"]);
    ASSERT_EQ("Cool trousers", results["hits"][1]["document"]["title"]);

    nlohmann::json payload = nlohmann::json::array();
    nlohmann::json event_data;
    auto collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;
        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    //manually trigger write to db
    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    std::vector<std::string> values;
    analyticsManager.get_last_N_events("11", "*", 5, values);
    ASSERT_EQ(1, values.size());

    auto parsed_json = nlohmann::json::parse(values[0]);
    ASSERT_EQ("CNV4", parsed_json["name"]);
    ASSERT_EQ("books", parsed_json["collection"]);
    ASSERT_EQ("11", parsed_json["user_id"]);
    ASSERT_EQ("1", parsed_json["doc_id"]);
    ASSERT_EQ("shorts", parsed_json["query"]);

    //now add click event rule
    analytics_rule = R"({
        "name": "click_event_rule",
        "type": "log",
        "params": {
            "source": {
                "collections": ["books"],
                 "events":  [{"type": "click", "name": "APC2"}]
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    event = R"({
        "type": "click",
        "name": "APC2",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //normal click event should not increment popularity score
    popular_clicks = analyticsManager.get_popular_clicks();
    ASSERT_EQ("popularity", popular_clicks["books"].counter_field);
    ASSERT_EQ(1, popular_clicks["books"].docid_counts.size());
    ASSERT_EQ(5, popular_clicks["books"].docid_counts["1"]);

    //add another counter event
    event = R"({
        "type": "conversion",
        "name": "CNV4",
        "data": {
            "q": "shorts",
            "doc_id": "1",
            "user_id": "11"
        }
    })"_json;
    req->body = event.dump();
    ASSERT_TRUE(post_create_event(req, res));

    popular_clicks = analyticsManager.get_popular_clicks();
    ASSERT_EQ(1, popular_clicks.size());
    ASSERT_EQ("popularity", popular_clicks["books"].counter_field);
    ASSERT_EQ(1, popular_clicks["books"].docid_counts.size());
    ASSERT_EQ(10, popular_clicks["books"].docid_counts["1"]);

    payload.clear();
    event_data.clear();
    collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;
        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    //manually trigger write to db
    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    values.clear();
    analyticsManager.get_last_N_events("11", "*", 5, values);
    ASSERT_EQ(2, values.size());

    parsed_json = nlohmann::json::parse(values[0]);
    ASSERT_EQ("CNV4", parsed_json["name"]);
    ASSERT_EQ("books", parsed_json["collection"]);
    ASSERT_EQ("11", parsed_json["user_id"]);
    ASSERT_EQ("1", parsed_json["doc_id"]);
    ASSERT_EQ("shorts", parsed_json["query"]);

    parsed_json = nlohmann::json::parse(values[1]);
    ASSERT_EQ("CNV4", parsed_json["name"]);
    ASSERT_EQ("books", parsed_json["collection"]);
    ASSERT_EQ("11", parsed_json["user_id"]);
    ASSERT_EQ("1", parsed_json["doc_id"]);
    ASSERT_EQ("shorts", parsed_json["query"]);

    values.clear();
    analyticsManager.get_last_N_events("13", "*", 5, values);
    ASSERT_EQ(1, values.size());

    parsed_json = nlohmann::json::parse(values[0]);
    ASSERT_EQ("APC2", parsed_json["name"]);
    ASSERT_EQ("books", parsed_json["collection"]);
    ASSERT_EQ("13", parsed_json["user_id"]);
    ASSERT_EQ("21", parsed_json["doc_id"]);
    ASSERT_EQ("technology", parsed_json["query"]);



    analyticsManager.dispose();
    analyticsManager.stop();
    analyticsManager.init(store, analytic_store, analytics_minute_rate_limit);

    analytics_rule = R"({
        "name": "books_popularity3",
        "type": "counter",
        "params": {
            "source": {
                 "collections": ["books"],
                 "events":  [{"type": "conversion", "weight": 5, "name": "CNV4"} ]
            },
            "destination": {
                "collection": "books",
                "counter_field": "popularity"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    event = R"({
        "type": "conversion",
        "name": "CNV4",
        "data": {
            "q": "shorts",
            "doc_id": "1",
            "user_id": "11"
        }
    })"_json;
    req->body = event.dump();
    ASSERT_TRUE(post_create_event(req, res));

    popular_clicks = analyticsManager.get_popular_clicks();
    ASSERT_EQ(1, popular_clicks.size());
    ASSERT_EQ("popularity", popular_clicks["books"].counter_field);
    ASSERT_EQ(1, popular_clicks["books"].docid_counts.size());
    ASSERT_EQ(5, popular_clicks["books"].docid_counts["1"]);
}

TEST_F(AnalyticsManagerTest, AnalyticsStoreTTL) {
    analyticsManager.dispose();
    analyticsManager.stop();
    delete analytic_store;

    //set TTL of an hour
    LOG(INFO) << "Truncating and creating: " << analytics_dir_path;
    system(("rm -rf "+ analytics_dir_path +" && mkdir -p "+analytics_dir_path).c_str());

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    analytic_store = new Store(analytics_dir_path, 24*60*60, 1024, true, FOURWEEKS_SECS);
    analyticsManager.init(store, analytic_store, analytics_minute_rate_limit);

    auto analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "AB"}]
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json event1 = R"({
        "type": "click",
        "name": "AB",
        "data": {
            "q": "technology",
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event1.dump();
    ASSERT_TRUE(post_create_event(req, res));

    //get events
    nlohmann::json payload = nlohmann::json::array();
    nlohmann::json event_data;
    auto collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;
        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
        }
    }
    payload.push_back(event_data);

    //manually trigger write to db
    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    //try fetching from db
    const std::string prefix_start = "13%";
    const std::string prefix_end = "13`";
    std::vector<std::string> events;

    analytic_store->scan_fill(prefix_start, prefix_end, events);
    ASSERT_EQ(1, events.size());
    ASSERT_EQ(events[0].c_str(), event_data.dump());

    //now set TTL to 1s and open analytics db
    events.clear();
    delete analytic_store;

    analytic_store = new Store(analytics_dir_path, 24*60*60, 1024, true, 1);

    sleep(2);
    analytic_store->compact_all();

    analytic_store->scan_fill(prefix_start, prefix_end, events);
    ASSERT_EQ(0, events.size());
}

TEST_F(AnalyticsManagerTest, AnalyticsStoreGetLastN) {
    analyticsManager.dispose();
    analyticsManager.stop();
    delete analytic_store;

    //set TTL of an hour
    LOG(INFO) << "Truncating and creating: " << analytics_dir_path;
    system(("rm -rf "+ analytics_dir_path +" && mkdir -p "+analytics_dir_path).c_str());

    analytic_store = new Store(analytics_dir_path, 24*60*60, 1024, true, FOURWEEKS_SECS);
    analyticsManager.init(store, analytic_store, analytics_minute_rate_limit);

    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    auto analytics_rule = R"({
        "name": "product_events2",
        "type": "log",
        "params": {
            "source": {
                "collections": ["titles"],
                 "events":  [{"type": "click", "name": "AB"}, {"type": "visit", "name": "AV"}]
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json event1;
    event1["type"] = "click";
    event1["name"] = "AB";
    event1["data"]["q"] = "technology";
    event1["data"]["user_id"] = "13";

    for(auto i = 0; i < 10; i++) {
        event1["data"]["doc_id"] = std::to_string(i);
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    //add more user events
    for(auto i = 0; i < 7; i++) {
        event1["data"]["user_id"] = "14";
        event1["data"]["doc_id"] = std::to_string(i);
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    for(auto i = 0; i < 5; i++) {
        event1["data"]["user_id"] = "15";
        event1["data"]["doc_id"] = std::to_string(i);
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    //get events
    nlohmann::json payload = nlohmann::json::array();
    nlohmann::json event_data;
    auto collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;
        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    //manually trigger write to db
    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    //basic test
    std::vector<std::string> values;
    analyticsManager.get_last_N_events("13", "*", 5, values);
    ASSERT_EQ(5, values.size());

    nlohmann::json parsed_json;
    uint32_t start_index = 9;
    for(auto i = 0; i < 5; i++) {
        parsed_json = nlohmann::json::parse(values[i]);
        ASSERT_EQ(std::to_string(start_index - i), parsed_json["doc_id"]);
    }

    //fetch events for middle user
    values.clear();
    analyticsManager.get_last_N_events("14", "*", 5, values);
    ASSERT_EQ(5, values.size());

    start_index = 6;
    for(auto i = 0; i < 5; i++) {
        parsed_json = nlohmann::json::parse(values[i]);
        ASSERT_EQ(std::to_string(start_index - i), parsed_json["doc_id"]);
    }

    //fetch more events than stored in db
    values.clear();
    analyticsManager.get_last_N_events("15", "*", 8, values);
    ASSERT_EQ(5, values.size());

    start_index = 4;
    for(auto i = 0; i < 5; i++) {
        parsed_json = nlohmann::json::parse(values[i]);
        ASSERT_EQ(std::to_string(start_index - i), parsed_json["doc_id"]);
    }


    //fetch events for non-existing user
    values.clear();
    analyticsManager.get_last_N_events("16", "*", 8, values);
    ASSERT_EQ(0, values.size());

    //get specific event type or user

    //add different type events
    event1["name"] = "AV";
    event1["type"] = "visit";
    event1["data"]["user_id"] = "14";

    for(auto i = 0; i < 5; i++) {
        event1["data"]["doc_id"] = std::to_string(i);
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    payload.clear();
    event_data.clear();
    collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;
        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    //manually trigger write to db
    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    //get last 5 visit events for user_id 14
    values.clear();
    analyticsManager.get_last_N_events("14", "AV", 5, values);
    ASSERT_EQ(5, values.size());
    for(int i = 0; i < 5; ++i) {
        parsed_json = nlohmann::json::parse(values[i]);
        ASSERT_EQ("AV", parsed_json["name"]);
        ASSERT_EQ(std::to_string(4-i), parsed_json["doc_id"]);
    }

    //get last 5 click events for user_id 14
    values.clear();
    analyticsManager.get_last_N_events("14", "AB", 5, values);
    ASSERT_EQ(5, values.size());
    for(int i = 0; i < 5; ++i) {
        parsed_json = nlohmann::json::parse(values[i]);
        ASSERT_EQ("AB", parsed_json["name"]);
        ASSERT_EQ(std::to_string(6-i), parsed_json["doc_id"]);
    }

    event1["name"] = "AB";
    event1["type"] = "click";
    event1["data"]["user_id"] = "14";

    for(auto i = 7; i < 10; i++) {
        event1["data"]["doc_id"] = std::to_string(i);
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    payload.clear();
    event_data.clear();
    collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;

        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    values.clear();
    analyticsManager.get_last_N_events("14", "AB", 10, values);
    ASSERT_EQ(10, values.size());
    for(int i = 0; i < 10; ++i) {
        parsed_json = nlohmann::json::parse(values[i]);
        ASSERT_EQ("AB", parsed_json["name"]);
        ASSERT_EQ(std::to_string(9-i), parsed_json["doc_id"]);
    }


    //try adding userid with _
    event1["name"] = "AB";
    event1["type"] = "click";
    event1["data"]["user_id"] = "14_U1";

    for(auto i = 0; i < 5; i++) {
        event1["data"]["doc_id"] = std::to_string(i);
        req->body = event1.dump();
        ASSERT_TRUE(post_create_event(req, res));
    }

    payload.clear();
    event_data.clear();
    collection_events_map = analyticsManager.get_log_events();
    for (auto &events_collection_it: collection_events_map) {
        const auto& collection = events_collection_it.first;

        for(const auto& event: events_collection_it.second) {
            event.to_json(event_data, collection);
            payload.push_back(event_data);
        }
    }

    ASSERT_TRUE(analyticsManager.write_to_db(payload));

    values.clear();
    analyticsManager.get_last_N_events("14_U1", "AB", 10, values);
    ASSERT_EQ(5, values.size());
    for(int i = 0; i < 5; ++i) {
        parsed_json = nlohmann::json::parse(values[i]);
        ASSERT_EQ("AB", parsed_json["name"]);
        ASSERT_EQ(std::to_string(4-i), parsed_json["doc_id"]);
    }
}

TEST_F(AnalyticsManagerTest, DISABLED_AnalyticsWithAliases) {
    nlohmann::json titles_schema = R"({
            "name": "titles",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "popularity", "type" : "int32"}
            ]
        })"_json;

    Collection* titles_coll = collectionManager.create_collection(titles_schema).get();

    //create alias
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    nlohmann::json alias_json = R"({
        "collection_name": "titles"
    })"_json;

    req->params["alias"] = "coll1";
    req->body = alias_json.dump();
    ASSERT_TRUE(put_upsert_alias(req, res));

    auto analytics_rule = R"({
        "name": "popular_titles",
        "type": "counter",
        "params": {
            "source": {
                "events":  [{"type": "click", "weight": 1, "name": "CLK1"}, {"type": "conversion", "weight": 5, "name": "CNV1"} ]
            },
            "destination": {
                "collection": "coll1",
                "counter_field": "popularity"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json event1;
    event1["type"] = "click";
    event1["name"] = "CLK1";
    event1["data"]["q"] = "technology";
    event1["data"]["user_id"] = "13";
    event1["data"]["doc_id"] = "1";

    req->body = event1.dump();
    ASSERT_TRUE(post_create_event(req, res));
}

TEST_F(AnalyticsManagerTest, AddSuggestionByEvent) {
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
                "collections": ["titles"],
                "events":  [{"type": "search", "name": "coll_search"}]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json event_data;
    event_data["q"] = "coo";
    event_data["user_id"] = "1";

    analyticsManager.add_event("127.0.0.1", "search", "coll_search", event_data);

    auto popularQueries = analyticsManager.get_popular_queries();
    auto localCounts = popularQueries["top_queries"]->get_local_counts();
    ASSERT_EQ(1, localCounts.size());
    ASSERT_EQ(1, localCounts.count("coo"));
    ASSERT_EQ(1, localCounts["coo"]);

    // add another query which is more popular
    event_data["q"] = "buzzfoo";
    analyticsManager.add_event("127.0.0.1", "search", "coll_search", event_data);

    event_data["user_id"] = "2";
    analyticsManager.add_event("127.0.0.1", "search", "coll_search", event_data);

    event_data["user_id"] = "3";
    analyticsManager.add_event("127.0.0.1", "search", "coll_search", event_data);


    popularQueries = analyticsManager.get_popular_queries();
    localCounts = popularQueries["top_queries"]->get_local_counts();
    ASSERT_EQ(2, localCounts.size());
    ASSERT_EQ(1, localCounts.count("coo"));
    ASSERT_EQ(1, localCounts["coo"]);
    ASSERT_EQ(1, localCounts.count("buzzfoo"));
    ASSERT_EQ(3, localCounts["buzzfoo"]);

    //try with nohits analytic rule
    analytics_rule = R"({
        "name": "noresults_queries",
        "type": "nohits_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"],
                "events":  [{"type": "search", "name": "nohits_search"}]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    event_data["q"] = "foobar";
    analyticsManager.add_event("127.0.0.1", "search", "nohits_search", event_data);

    auto noresults_queries = analyticsManager.get_nohits_queries();
    localCounts = noresults_queries["top_queries"]->get_local_counts();

    ASSERT_EQ(1, localCounts.size());
    ASSERT_EQ(1, localCounts.count("foobar"));
    ASSERT_EQ(1, localCounts["foobar"]);

    //try creating event with same name
    suggestions_schema = R"({
        "name": "top_queries2",
        "fields": [
          {"name": "q", "type": "string" },
          {"name": "count", "type": "int32" }
        ]
      })"_json;

    suggestions_coll = collectionManager.create_collection(suggestions_schema).get();

    analytics_rule = R"({
        "name": "noresults_queries2",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "source": {
                "collections": ["titles"],
                "events":  [{"type": "search", "name": "nohits_search"}]
            },
            "destination": {
                "collection": "top_queries2"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Events must contain a unique name.", create_op.error());
}

TEST_F(AnalyticsManagerTest, EventsOnlySearchTest) {
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

    //enable_auto_aggregation flag enables query aggregation via events only
    nlohmann::json analytics_rule = R"({
        "name": "top_search_queries",
        "type": "popular_queries",
        "params": {
            "limit": 100,
            "enable_auto_aggregation": false,
            "source": {
                "collections": ["titles"],
                "events":  [{"type": "search", "name": "coll_search"}]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    std::string q = "coo";
    analyticsManager.add_suggestion("titles", q, "cool", true, "1");

    auto popularQueries = analyticsManager.get_popular_queries();
    auto userQueries = popularQueries["top_queries"]->get_user_prefix_queries();
    ASSERT_EQ(0, userQueries.size());

    //try sending via events api
    nlohmann::json event_data;
    event_data["q"] = "coo";
    event_data["user_id"] = "1";

    analyticsManager.add_event("127.0.0.1", "search", "coll_search", event_data);

    popularQueries = analyticsManager.get_popular_queries();
    auto localCounts = popularQueries["top_queries"]->get_local_counts();
    ASSERT_EQ(1, localCounts.size());
    ASSERT_EQ(1, localCounts.count("coo"));
    ASSERT_EQ(1, localCounts["coo"]);

    //try with nohits analytic rule
    analytics_rule = R"({
        "name": "noresults_queries",
        "type": "nohits_queries",
        "params": {
            "limit": 100,
            "enable_auto_aggregation": false,
            "source": {
                "collections": ["titles"],
                "events":  [{"type": "search", "name": "nohits_search"}]
            },
            "destination": {
                "collection": "top_queries"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, false, true);
    ASSERT_TRUE(create_op.ok());

    q = "foobar";
    analyticsManager.add_nohits_query("titles", q, true, "1");

    auto noresults_queries = analyticsManager.get_nohits_queries();
    userQueries = noresults_queries["top_queries"]->get_user_prefix_queries();

    ASSERT_EQ(0, userQueries.size());

    //send events for same
    event_data["q"] = "foobar";
    analyticsManager.add_event("127.0.0.1", "search", "nohits_search", event_data);

    noresults_queries = analyticsManager.get_nohits_queries();
    localCounts = noresults_queries["top_queries"]->get_local_counts();

    ASSERT_EQ(1, localCounts.size());
    ASSERT_EQ(1, localCounts.count("foobar"));
    ASSERT_EQ(1, localCounts["foobar"]);
}