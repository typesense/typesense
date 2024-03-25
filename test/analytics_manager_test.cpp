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
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    AnalyticsManager& analyticsManager = AnalyticsManager::get_instance();

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/analytics_manager_test";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
        system("mkdir -p /tmp/typesense_test/models");

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        analyticsManager.init(store, state_dir_path);
        analyticsManager.resetToggleRateLimit(false);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
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
    ASSERT_EQ("Must contain a valid list of source collection names.", create_op.error());

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

    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);

    auto analytics_rule = R"({
        "name": "product_click_events",
        "type": "clicks",
        "params": {
            "name": "AP",
            "source": {
                "collection": "titles"
            }
        }
    })"_json;

    auto create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_TRUE(create_op.ok());

    analytics_rule = R"({
        "name": "product_visitors",
        "type": "visits",
        "params": {
            "name": "VP",
            "source": {
                "collection": "titles"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
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

    //missing query param
    nlohmann::json event2 = R"({
        "type": "click",
        "name": "AP",
        "data": {
            "doc_id": "21",
            "user_id": "13"
        }
    })"_json;

    req->body = event2.dump();
    ASSERT_FALSE(post_create_event(req, res));
    ASSERT_EQ("{\"message\": \"event json data fields should contain `q`.\"}", res->body);

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
    ASSERT_EQ("{\"message\": \"`doc_id` value should be string.\"}", res->body);

    //event name should be unique
    analytics_rule = R"({
        "name": "product_click_events2",
        "type": "clicks",
        "params": {
            "name": "AP",
            "source": {
                "collection": "titles"
            }
        }
    })"_json;

    create_op = analyticsManager.create_rule(analytics_rule, true, true);
    ASSERT_FALSE(create_op.ok());
    ASSERT_EQ("Event name already exists.", create_op.error());

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
        "name": "product_custom_events",
        "type": "custom_events",
        "params": {
            "name": "CP",
            "source": {
                "collection": "titles"
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
        "type": "clicks",
        "params": {
            "name": "APC",
            "source": {
                "collection": "titles"
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

    analyticsManager.persist_events();

    auto fileOutput = Config::fetch_file_contents("/tmp/typesense_test/analytics_manager_test/analytics_events.tsv");

    std::stringstream strbuff(fileOutput.get());
    std::string docid, userid, q, collection, name, timestamp;
    strbuff >> timestamp >> name >> collection >> userid >> docid >> q;
    ASSERT_EQ("APC", name);
    ASSERT_EQ("titles", collection);
    ASSERT_EQ("13", userid);
    ASSERT_EQ("21", docid);
    ASSERT_EQ("technology", q);

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

    analyticsManager.persist_events();

    fileOutput = Config::fetch_file_contents("/tmp/typesense_test/analytics_manager_test/analytics_events.tsv");

    std::stringstream strbuff2(fileOutput.get());
    timestamp.clear();name.clear();collection.clear();userid.clear();q.clear();
    strbuff2 >> timestamp >> name >> collection >> userid >> docid >> q;
    ASSERT_EQ("APC", name);
    ASSERT_EQ("titles", collection);
    ASSERT_EQ("13", userid);
    ASSERT_EQ("21", docid);
    ASSERT_EQ("technology", q);

    timestamp.clear();name.clear();collection.clear();userid.clear();q.clear();
    strbuff2 >> timestamp >> name >> collection >> userid >> docid >> q;
    ASSERT_EQ("APC", name);
    ASSERT_EQ("titles", collection);
    ASSERT_EQ("13", userid);
    ASSERT_EQ("12", docid);
    ASSERT_EQ("technology", q);
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
        "name": "rate_limit",
        "type": "clicks",
        "params": {
            "name": "AB",
            "source": {
                "collection": "titles"
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
                "events":  [{"type": "click", "weight": 1, "name": "CLK1"}, {"type": "conversion", "weight": 5, "name": "CNV1"} ],
                "log_to_file": true
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
    system("rm -rf /tmp/typesense_test/analytics_manager_test/analytics_events.tsv");
    analyticsManager.init(store, "/tmp/typesense_test/analytics_manager_test");

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

    //correct params
    analytics_rule = R"({
        "name": "books_popularity2",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"],
                 "events":  [{"type": "click", "weight": 1, "name" : "CLK4"}, {"type": "conversion", "weight": 5, "name": "CNV4", "log_to_file" : true} ]
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

    //trigger persistance event manually
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

    //verify log file
    analyticsManager.persist_events();

    auto fileOutput = Config::fetch_file_contents("/tmp/typesense_test/analytics_manager_test/analytics_events.tsv");

    std::stringstream strbuff(fileOutput.get());
    std::string timestamp, collection, userid, name, q, docid;
    strbuff >> timestamp >> name >> collection >> userid >> docid >> q;
    ASSERT_EQ("CNV4", name);
    ASSERT_EQ("books", collection);
    ASSERT_EQ("11", userid);
    ASSERT_EQ("1", docid);
    ASSERT_EQ("shorts", q);

    //now add click event rule
    analytics_rule = R"({
        "name": "book_click_events",
        "type": "clicks",
        "params": {
            "name": "APC2",
            "source": {
                "collection": "books"
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

    //check log file
    analyticsManager.persist_events();

    fileOutput = Config::fetch_file_contents("/tmp/typesense_test/analytics_manager_test/analytics_events.tsv");

    std::stringstream strbuff2(fileOutput.get());
    timestamp.clear(), collection.clear(), userid.clear(), name.clear(), q.clear(), docid.clear();
    strbuff2 >> timestamp >> name >> collection >> userid >> docid >> q;
    ASSERT_EQ("CNV4", name);
    ASSERT_EQ("books", collection);
    ASSERT_EQ("11", userid);
    ASSERT_EQ("1", docid);
    ASSERT_EQ("shorts", q);

    timestamp.clear(), collection.clear(), userid.clear(), name.clear(), q.clear(), docid.clear();
    strbuff2 >> timestamp >> name >> collection >> userid >> docid >> q;
    ASSERT_EQ("APC2", name);
    ASSERT_EQ("books", collection);
    ASSERT_EQ("13", userid);
    ASSERT_EQ("21", docid);
    ASSERT_EQ("technology", q);
}