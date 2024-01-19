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
        "type": "query_purchase",
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

    nlohmann::json event5 = R"({
        "type": "query_purchase",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "position": 4,
            "user_id": "11"
        }
    })"_json;

    req->body = event5.dump();
    ASSERT_TRUE(post_create_event(req, res));
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

    std::vector<nlohmann::json> events;

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

    nlohmann::json event2 = R"({
        "type": "query_purchase",
        "data": {
            "q": "technology",
            "collection": "titles",
            "doc_id": "21",
            "position": 4,
            "user_id": "11"
        }
    })"_json;

    events.push_back(event1);
    events.push_back(event2);

    //reset the LRU cache to test the rate limit
    analyticsManager.resetToggleRateLimit(true);

    for(auto i = 0; i < 5; ++i) {
        req->body = events[i%2].dump();
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

    nlohmann::json analytics_rule = R"({
        "name": "product_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["products"],
                "events":  [{"type": "query_click", "weight": 1}, {"type": "query_purchase", "weight": 5} ]
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
        "type": "query_purchase",
        "data": {
            "q": "trousers",
            "collection": "products",
            "doc_id": "1",
            "position": 2,
            "user_id": "13"
        }
    })"_json;

    req->body = event1.dump();
    ASSERT_TRUE(post_create_event(req, res));

    nlohmann::json event2 = R"({
        "type": "query_click",
        "data": {
            "q": "shorts",
            "collection": "products",
            "doc_id": "3",
            "position": 4,
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
        "type": "query_click",
        "data": {
            "q": "shorts",
            "collection": "products",
            "doc_id": "1",
            "position": 4,
            "user_id": "11"
        }
    })"_json;

    req->body = event3.dump();
    ASSERT_TRUE(post_create_event(req, res));

    nlohmann::json event4 = R"({
        "type": "query_purchase",
        "data": {
            "q": "shorts",
            "collection": "products",
            "doc_id": "3",
            "position": 4,
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

    //trigger persistance event
    for(const auto& popular_clicks_it : popular_clicks) {
        auto coll = popular_clicks_it.first;
        nlohmann::json doc;
        auto counter_field = popular_clicks_it.second.counter_field;
        req->params["collection"] = "products";
        req->params["action"] = "update";
        for(const auto& popular_click : popular_clicks_it.second.docid_counts) {
            doc["id"] = popular_click.first;
            doc[counter_field] = popular_click.second;
            req->body = doc.dump();
            post_import_documents(req, res);
        }
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
}

TEST_F(AnalyticsManagerTest, PopularityScoreValidation) {
    nlohmann::json products_schema = R"({
            "name": "books",
            "fields": [
                {"name": "title", "type": "string"},
                {"name": "popularity", "type": "int32"}
            ]
        })"_json;

    Collection* products_coll = collectionManager.create_collection(products_schema).get();

    nlohmann::json analytics_rule = R"({
        "name": "books_popularity",
        "type": "counter",
        "params": {
            "source": {
                "collections": ["books"],
                "events":  [{"type": "query_click", "weight": 1}, {"type": "query_purchase", "weight": 5} ]
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
}