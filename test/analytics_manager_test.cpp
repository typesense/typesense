#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include <analytics_manager.h>
#include <doc_analytics.h>
#include <search_analytics.h>
#include "collection.h"

class AnalyticsManagerTest : public ::testing::Test {
protected:
    Store *store;
    Store *analytic_store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    std::string state_dir_path, analytics_dir_path;

    AnalyticsManager& analyticsManager = AnalyticsManager::get_instance();
    DocAnalytics& doc_analytics = DocAnalytics::get_instance();
    SearchAnalytics& search_analytics = SearchAnalytics::get_instance();
    uint32_t analytics_minute_rate_limit = 5;

    void setupCollection() {
        Config::get_instance().set_enable_search_analytics(true);
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
        Config::get_instance().set_enable_search_analytics(false);
    }
};

TEST_F(AnalyticsManagerTest, CreateRule) {
    nlohmann::json products_schema = R"({
        "name": "products",
        "fields": [
          {"name": "company_name", "type": "string" },
          {"name": "num_employees", "type": "int32" },
          {"name": "country", "type": "string", "facet": true },
          {"name": "popularity", "type": "int32", "optional": true}
        ],
        "default_sorting_field": "num_employees"
    })"_json;

    auto coll_create_op = collectionManager.create_collection(products_schema);
    ASSERT_TRUE(coll_create_op.ok());

    nlohmann::json queries_schema = R"({
        "name": "queries",
        "fields": [
            {"name": "q", "type": "string"},
            {"name": "count", "type": "int32"}
        ]
    })"_json;

    auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
    ASSERT_TRUE(queries_coll_create_op.ok());

    nlohmann::json popular_queries_analytics_rule = R"({
        "name": "popular_queries_products",
        "type": "popular_queries",
        "collection": "products",
        "event_type": "search",
        "rule_tag": "popular_queries",
        "params": {
          "destination_collection": "queries",
          "capture_search_requests": false,
          "limit": 1000
        }
      })"_json;

    auto create_op = analyticsManager.create_rule(popular_queries_analytics_rule, false, true, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json counter_analytics_rule = R"({
      "name": "product_popularity",
      "type": "counter",
      "collection": "products",
      "event_type": "click",
      "rule_tag": "tag2",
      "params": {
        "counter_field": "popularity",
        "weight": 1
      }
    })"_json;

    create_op = analyticsManager.create_rule(counter_analytics_rule, false, true, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json nohits_analytics_rule = R"({
      "name": "no_hit_queries_products",
      "type": "nohits_queries",
      "collection": "products",
      "event_type": "search",
      "params": {
        "destination_collection": "queries",
        "capture_search_requests": false,
        "limit": 1000
      }
    })"_json;

    create_op = analyticsManager.create_rule(nohits_analytics_rule, false, true, true);
    ASSERT_TRUE(create_op.ok());

    nlohmann::json log_analytics_rule = R"({
      "name": "product_clicks",
      "type": "log",
      "collection": "products",
      "event_type": "click",
      "rule_tag": "tag1"
    })"_json;

    create_op = analyticsManager.create_rule(log_analytics_rule, false, true, true);
    ASSERT_TRUE(create_op.ok());
}

TEST_F(AnalyticsManagerTest, UpsertRule) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true },
        {"name": "popularity", "type": "int32", "optional": true}
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());

  nlohmann::json queries_schema = R"({
      "name": "queries",
      "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
      ]
  })"_json;

  auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
  ASSERT_TRUE(queries_coll_create_op.ok());

  queries_schema = R"({
      "name": "queries1",
      "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
      ]
  })"_json;

  queries_coll_create_op = collectionManager.create_collection(queries_schema);
  ASSERT_TRUE(queries_coll_create_op.ok());

  nlohmann::json popular_queries_analytics_rule = R"({
      "name": "popular_queries_products",
      "type": "popular_queries",
      "collection": "products",
      "event_type": "search",
      "rule_tag": "popular_queries",
      "params": {
        "destination_collection": "queries",
        "capture_search_requests": false,
        "limit": 1000
      }
    })"_json;

  auto create_op = analyticsManager.create_rule(popular_queries_analytics_rule, true, true, true);
  ASSERT_TRUE(create_op.ok());

  auto get_op = analyticsManager.get_rule("popular_queries_products");
  ASSERT_TRUE(get_op.ok());
  ASSERT_EQ(get_op.get()["name"], "popular_queries_products");
  ASSERT_EQ(get_op.get()["type"], "popular_queries");
  ASSERT_EQ(get_op.get()["collection"], "products");
  ASSERT_EQ(get_op.get()["event_type"], "search");
  ASSERT_EQ(get_op.get()["rule_tag"], "popular_queries");
  ASSERT_EQ(get_op.get()["params"]["destination_collection"], "queries");

  create_op = analyticsManager.create_rule(popular_queries_analytics_rule, true, true, true);
  ASSERT_TRUE(create_op.ok());

  get_op = analyticsManager.get_rule("popular_queries_products");
  ASSERT_TRUE(get_op.ok());
  ASSERT_EQ(get_op.get()["name"], "popular_queries_products");
  ASSERT_EQ(get_op.get()["type"], "popular_queries");
  ASSERT_EQ(get_op.get()["collection"], "products");
  ASSERT_EQ(get_op.get()["event_type"], "search");
  ASSERT_EQ(get_op.get()["rule_tag"], "popular_queries");
  ASSERT_EQ(get_op.get()["params"]["destination_collection"], "queries");

  popular_queries_analytics_rule["event_type"] = "click";
  create_op = analyticsManager.create_rule(popular_queries_analytics_rule, true, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Rule event type cannot be changed");

  popular_queries_analytics_rule["event_type"] = "search";
  popular_queries_analytics_rule["collection"] = "non_existent_collection";
  create_op = analyticsManager.create_rule(popular_queries_analytics_rule, true, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Rule collection cannot be changed");

  popular_queries_analytics_rule["event_type"] = "search";
  popular_queries_analytics_rule["collection"] = "products";
  popular_queries_analytics_rule["params"]["destination_collection"] = "queries1";
  create_op = analyticsManager.create_rule(popular_queries_analytics_rule, true, true, true);
  ASSERT_TRUE(create_op.ok());
  ASSERT_EQ(create_op.get()["name"], "popular_queries_products");
  ASSERT_EQ(create_op.get()["type"], "popular_queries");
  ASSERT_EQ(create_op.get()["collection"], "products");
  ASSERT_EQ(create_op.get()["event_type"], "search");
  ASSERT_EQ(create_op.get()["rule_tag"], "popular_queries");
  ASSERT_EQ(create_op.get()["params"]["destination_collection"], "queries1");

  get_op = analyticsManager.get_rule("popular_queries_products");
  ASSERT_TRUE(get_op.ok());
  ASSERT_EQ(get_op.get()["name"], "popular_queries_products");
  ASSERT_EQ(get_op.get()["type"], "popular_queries");
  ASSERT_EQ(get_op.get()["collection"], "products");
  ASSERT_EQ(get_op.get()["event_type"], "search");
  ASSERT_EQ(get_op.get()["rule_tag"], "popular_queries");
  ASSERT_EQ(get_op.get()["params"]["destination_collection"], "queries1");
}

TEST_F(AnalyticsManagerTest, GetRule) {
  nlohmann::json products_schema = R"({
        "name": "products",
        "fields": [
          {"name": "company_name", "type": "string" },
          {"name": "num_employees", "type": "int32" },
          {"name": "country", "type": "string", "facet": true },
          {"name": "popularity", "type": "int32", "optional": true}
        ],
        "default_sorting_field": "num_employees"
    })"_json;

    auto coll_create_op = collectionManager.create_collection(products_schema);
    ASSERT_TRUE(coll_create_op.ok());

    nlohmann::json queries_schema = R"({
        "name": "queries",
        "fields": [
            {"name": "q", "type": "string"},
            {"name": "count", "type": "int32"}
        ]
    })"_json;

    auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
    ASSERT_TRUE(queries_coll_create_op.ok());

    nlohmann::json popular_queries_analytics_rule = R"({
        "name": "popular_queries_products",
        "type": "popular_queries",
        "collection": "products",
        "event_type": "search",
        "rule_tag": "popular_queries",
        "params": {
          "destination_collection": "queries",
          "capture_search_requests": false,
          "limit": 1000
        }
      })"_json;

    auto create_op = analyticsManager.create_rule(popular_queries_analytics_rule, false, true, true);
    ASSERT_TRUE(create_op.ok());

    auto get_op = analyticsManager.get_rule("popular_queries_products");
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(get_op.get()["name"], "popular_queries_products");
    ASSERT_EQ(get_op.get()["type"], "popular_queries");
    ASSERT_EQ(get_op.get()["collection"], "products");
    ASSERT_EQ(get_op.get()["event_type"], "search");
    ASSERT_EQ(get_op.get()["rule_tag"], "popular_queries");
    ASSERT_EQ(get_op.get()["params"]["destination_collection"], "queries");
}

TEST_F(AnalyticsManagerTest, GetRules) {
  nlohmann::json products_schema = R"({
        "name": "products",
        "fields": [
          {"name": "company_name", "type": "string" },
          {"name": "num_employees", "type": "int32" },
          {"name": "country", "type": "string", "facet": true },
          {"name": "popularity", "type": "int32", "optional": true}
        ],
        "default_sorting_field": "num_employees"
    })"_json;

    auto coll_create_op = collectionManager.create_collection(products_schema);
    ASSERT_TRUE(coll_create_op.ok());

    nlohmann::json queries_schema = R"({
        "name": "queries",
        "fields": [
            {"name": "q", "type": "string"},
            {"name": "count", "type": "int32"}
        ]
    })"_json;

    auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
    ASSERT_TRUE(queries_coll_create_op.ok());

    nlohmann::json popular_queries_analytics_rule = R"({
        "name": "popular_queries_products",
        "type": "popular_queries",
        "collection": "products",
        "event_type": "search",
        "rule_tag": "popular_queries",
        "params": {
          "destination_collection": "queries",
          "capture_search_requests": false,
          "limit": 1000
        }
      })"_json;

    auto create_op = analyticsManager.create_rule(popular_queries_analytics_rule, false, true, true);
    ASSERT_TRUE(create_op.ok());

    auto get_op = analyticsManager.list_rules();
    ASSERT_TRUE(get_op.ok());
    ASSERT_EQ(get_op.get().size(), 1);
    ASSERT_EQ(get_op.get()[0]["name"], "popular_queries_products");
    ASSERT_EQ(get_op.get()[0]["type"], "popular_queries");
    ASSERT_EQ(get_op.get()[0]["collection"], "products");
    ASSERT_EQ(get_op.get()[0]["event_type"], "search");
    ASSERT_EQ(get_op.get()[0]["rule_tag"], "popular_queries");
    ASSERT_EQ(get_op.get()[0]["params"]["destination_collection"], "queries");


    auto get_op_by_tag = analyticsManager.list_rules("popular_queries");
    ASSERT_TRUE(get_op_by_tag.ok());
    ASSERT_EQ(get_op_by_tag.get().size(), 1);
    ASSERT_EQ(get_op_by_tag.get()[0]["name"], "popular_queries_products");
    ASSERT_EQ(get_op_by_tag.get()[0]["type"], "popular_queries");
    ASSERT_EQ(get_op_by_tag.get()[0]["collection"], "products");
    ASSERT_EQ(get_op_by_tag.get()[0]["event_type"], "search");
    ASSERT_EQ(get_op_by_tag.get()[0]["rule_tag"], "popular_queries");
    ASSERT_EQ(get_op_by_tag.get()[0]["params"]["destination_collection"], "queries");

    auto get_op_by_tag_and_name = analyticsManager.list_rules("non_existent_tag");
    ASSERT_TRUE(get_op_by_tag_and_name.ok());
    ASSERT_EQ(get_op_by_tag_and_name.get().size(), 0);
}

TEST_F(AnalyticsManagerTest, DeleteRule) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true },
        {"name": "popularity", "type": "int32", "optional": true}
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());

  nlohmann::json queries_schema = R"({
      "name": "queries",
      "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
      ]
  })"_json;

  auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
  ASSERT_TRUE(queries_coll_create_op.ok());

  nlohmann::json popular_queries_analytics_rule = R"({
      "name": "popular_queries_products",
      "type": "popular_queries",
      "collection": "products",
      "event_type": "search",
      "rule_tag": "popular_queries",
      "params": {
        "destination_collection": "queries",
        "capture_search_requests": false,
        "limit": 1000
      }
    })"_json;

  auto create_op = analyticsManager.create_rule(popular_queries_analytics_rule, false, true, true);
  ASSERT_TRUE(create_op.ok());

  auto delete_op = analyticsManager.remove_rule("popular_queries_products");
  ASSERT_TRUE(delete_op.ok());

  auto get_op = analyticsManager.get_rule("popular_queries_products");
  ASSERT_FALSE(get_op.ok());

  auto list_op = analyticsManager.list_rules();
  ASSERT_TRUE(list_op.ok());
  ASSERT_EQ(list_op.get().size(), 0);

  auto delete_non_existent_op = analyticsManager.remove_rule("non_existent_rule");
  ASSERT_FALSE(delete_non_existent_op.ok());
}

TEST_F(AnalyticsManagerTest, RuleValidation) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true },
        {"name": "popularity", "type": "int32", "optional": true}
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  nlohmann::json queries_schema = R"({
      "name": "queries",
      "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
      ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());

  auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
  ASSERT_TRUE(queries_coll_create_op.ok());

  nlohmann::json invalid_destination_collection_popular_queries_rule = R"({
    "name": "counter_products",
    "type": "counter",
    "collection": "products",
    "event_type": "click",
    "rule_tag": "tag1",
    "params": {
      "destination_collection": 1,
      "counter_field": "popularity",
      "weight": 1
    }
  })"_json;

  auto create_op = analyticsManager.create_rule(invalid_destination_collection_popular_queries_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Destination collection should be a string");

  nlohmann::json wrong_destination_collection_popular_queries_rule = R"({
    "name": "popular_queries_products",
    "type": "popular_queries",
    "collection": "products",
    "event_type": "search",
    "rule_tag": "popular_queries",
    "params": {
      "destination_collection": "non_existent_collection",
      "capture_search_requests": false,
      "limit": 1000
    }
  })"_json;

  create_op = analyticsManager.create_rule(wrong_destination_collection_popular_queries_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Destination collection does not exist");

  nlohmann::json collection_not_found_popular_queries_rule = R"({
    "name": "popular_queries_products",
    "type": "popular_queries",
    "collection": "non_existent_collection",
    "event_type": "search",
    "rule_tag": "popular_queries",
    "params": {
      "destination_collection": "queries",
      "capture_search_requests": false,
      "limit": 1000
    }
  })"_json;

  create_op = analyticsManager.create_rule(collection_not_found_popular_queries_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Collection non_existent_collection does not exist");

  nlohmann::json wrong_type_nohits_queries_rule = R"({
    "name": "nohits_queries_products",
    "type": "nohits_queries_wrong_type",
    "collection": "products",
    "event_type": "search",
    "rule_tag": "nohits_queries",
    "params": {
      "destination_collection": "queries"
    }
  })"_json;

  create_op = analyticsManager.create_rule(wrong_type_nohits_queries_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Event type or type is invalid (or) combination of both is invalid");

  nlohmann::json wrong_event_type_counter_rule = R"({
    "name": "product_popularity",
    "type": "counter",
    "collection": "products",
    "event_type": "click_wrong_event_type",
    "rule_tag": "tag2",
    "params": {
      "counter_field": "popularity",
      "weight": 1
    }
  })"_json;

  create_op = analyticsManager.create_rule(wrong_event_type_counter_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Event type or type is invalid (or) combination of both is invalid");

  nlohmann::json wrong_name_log_rule = R"({
    "name": 1,
    "type": "log",
    "collection": "products",
    "event_type": "click",
    "rule_tag": "tag1"
  })"_json;

  create_op = analyticsManager.create_rule(wrong_name_log_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Name is required when creating an analytics rule");

  nlohmann::json no_name_log_rule = R"({
    "name": "",
    "type": "log",
    "collection": "products",
    "event_type": "click"
  })"_json;

  create_op = analyticsManager.create_rule(no_name_log_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Name is required when creating an analytics rule");

  nlohmann::json wrong_event_type_log_rule = R"({
    "name": "product_clicks",
    "type": "log",
    "collection": "products",
    "event_type": "click_wrong_event_type"
  })"_json;

  create_op = analyticsManager.create_rule(wrong_event_type_log_rule, false, true, true);
  ASSERT_FALSE(create_op.ok());
  ASSERT_EQ(create_op.code(), 400);
  ASSERT_EQ(create_op.error(), "Event type or type is invalid (or) combination of both is invalid");
}

TEST_F(AnalyticsManagerTest, PopularQueries) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true },
        {"name": "popularity", "type": "int32", "optional": true}
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  nlohmann::json queries_schema = R"({
      "name": "queries",
      "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
      ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());
  auto coll = coll_create_op.get();

  auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
  ASSERT_TRUE(queries_coll_create_op.ok());

  nlohmann::json doc;
  doc["company_name"] = "Typesense";
  doc["num_employees"] = 10;
  doc["country"] = "US";
  doc["popularity"] = 10;
  
  auto add_op = coll->add(doc.dump());
  ASSERT_TRUE(add_op.ok());

  nlohmann::json with_no_capture = R"({
    "name": "with_no_capture",
    "type": "popular_queries",
    "collection": "products",
    "event_type": "search",
    "rule_tag": "popular_queries",
    "params": {
      "destination_collection": "queries",
      "capture_search_requests": false,
      "meta_fields": ["filter_by", "analytics_tag"],
      "limit": 1000
    }
  })"_json;

  auto create_op = analyticsManager.create_rule(with_no_capture, false, true, true);
  ASSERT_TRUE(create_op.ok());

  auto add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "with_no_capture",
    "data": {
      "q": "hola",
      "user_id": "user2",
      "analytics_tag": "tag1",
      "filter_by": "country:US"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

   add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "with_no_capture",
    "data": {
      "q": "hola",
      "user_id": "user3",
      "analytics_tag": "tag1",
      "filter_by": "country:US"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());


  auto future_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count() + uint64_t(SearchAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS) * 2;

  search_analytics.compact_all_user_queries(future_ts_us);

  auto get_counter_op = search_analytics.get_search_counter_events();
  ASSERT_EQ(get_counter_op.size(), 1);
  ASSERT_EQ(get_counter_op["with_no_capture"].query_counts.size(), 1);
  for(auto& [key, value] : get_counter_op["with_no_capture"].query_counts) {
    ASSERT_EQ(key.query, "hola");
    ASSERT_EQ(key.user_id, "user2");
    ASSERT_EQ(key.tag_str, "tag1");
    ASSERT_EQ(key.filter_str, "country:US");
    ASSERT_EQ(value, 2);
  }

  nlohmann::json with_capture = R"({
    "name": "with_capture",
    "type": "popular_queries",
    "collection": "products",
    "event_type": "search",
    "rule_tag": "popular_queries",
    "params": {
      "destination_collection": "queries",
      "capture_search_requests": true,
      "meta_fields": ["filter_by", "analytics_tag"],
      "limit": 1000
    }
  })"_json;

  create_op = analyticsManager.create_rule(with_capture, false, true, true);
  ASSERT_TRUE(create_op.ok());

  std::string results_json_str;
  std::map<std::string, std::string> req_params = {
    {"q", "type"},
    {"query_by", "company_name"},
    {"collection", "products"},
    {"x-typesense-user-id", "user2"},
    {"analytics_tag", "tag1"},
    {"filter_by", "country:US"}
  };
  nlohmann::json embedded_params;
  auto results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(1, nlohmann::json::parse(results_json_str)["hits"].size());
  req_params["q"] = "typesen";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(1, nlohmann::json::parse(results_json_str)["hits"].size());
  req_params["q"] = "typesense";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(1, nlohmann::json::parse(results_json_str)["hits"].size());

  req_params["x-typesense-user-id"] = "user3";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(1, nlohmann::json::parse(results_json_str)["hits"].size());

  search_analytics.compact_all_user_queries(future_ts_us);

  get_counter_op = search_analytics.get_search_counter_events();
  ASSERT_EQ(get_counter_op.size(), 2);
  ASSERT_EQ(get_counter_op["with_capture"].query_counts.size(), 1);
  for(auto& [key, value] : get_counter_op["with_capture"].query_counts) {
    ASSERT_EQ(key.query, "typesense");
    ASSERT_EQ(key.tag_str, "tag1");
    ASSERT_EQ(key.filter_str, "country:US");
    ASSERT_EQ(value, 2);
  }
}

TEST_F(AnalyticsManagerTest, NoHitsQueries) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true },
        {"name": "popularity", "type": "int32", "optional": true}
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  nlohmann::json queries_schema = R"({
      "name": "queries",
      "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
      ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());
  auto coll = coll_create_op.get();

  auto queries_coll_create_op = collectionManager.create_collection(queries_schema);
  ASSERT_TRUE(queries_coll_create_op.ok());

  nlohmann::json doc;
  doc["company_name"] = "Typesense";
  doc["num_employees"] = 10;
  doc["country"] = "US";
  doc["popularity"] = 10;

  auto add_op = coll->add(doc.dump());
  ASSERT_TRUE(add_op.ok());

  nlohmann::json with_no_capture = R"({
    "name": "with_no_capture_nohits",
    "type": "nohits_queries",
    "collection": "products",
    "event_type": "search",
    "params": {
      "destination_collection": "queries",
      "capture_search_requests": false,
      "meta_fields": ["filter_by", "analytics_tag"],
      "limit": 1000
    }
  })"_json;

  auto create_op = analyticsManager.create_rule(with_no_capture, false, true, true);
  ASSERT_TRUE(create_op.ok());

  auto add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "with_no_capture_nohits",
    "data": {
      "q": "nomatch",
      "user_id": "user2",
      "analytics_tag": "tag1",
      "filter_by": "country:US"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "with_no_capture_nohits",
    "data": {
      "q": "nomatch",
      "user_id": "user3",
      "analytics_tag": "tag1",
      "filter_by": "country:US"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  auto future_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count() + uint64_t(SearchAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS) * 2;

  search_analytics.compact_all_user_queries(future_ts_us);

  auto get_counter_op = search_analytics.get_search_counter_events();
  ASSERT_EQ(get_counter_op.size(), 1);
  ASSERT_EQ(get_counter_op["with_no_capture_nohits"].query_counts.size(), 1);
  for(auto& [key, value] : get_counter_op["with_no_capture_nohits"].query_counts) {
    ASSERT_EQ(key.query, "nomatch");
    ASSERT_EQ(key.user_id, "user2");
    ASSERT_EQ(key.tag_str, "tag1");
    ASSERT_EQ(key.filter_str, "country:US");
    ASSERT_EQ(value, 2);
  }

  nlohmann::json with_capture = R"({
    "name": "with_capture_nohits",
    "type": "nohits_queries",
    "collection": "products",
    "event_type": "search",
    "params": {
      "destination_collection": "queries",
      "capture_search_requests": true,
      "meta_fields": ["filter_by", "analytics_tag"],
      "limit": 1000
    }
  })"_json;

  create_op = analyticsManager.create_rule(with_capture, false, true, true);
  ASSERT_TRUE(create_op.ok());

  std::string results_json_str;
  std::map<std::string, std::string> req_params = {
    {"q", "non"},
    {"query_by", "company_name"},
    {"collection", "products"},
    {"x-typesense-user-id", "user2"},
    {"analytics_tag", "tag1"},
    {"filter_by", "country:US"}
  };
  nlohmann::json embedded_params;
  auto results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(0, nlohmann::json::parse(results_json_str)["hits"].size());
  req_params["q"] = "nonex";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(0, nlohmann::json::parse(results_json_str)["hits"].size());
  req_params["q"] = "nonexistent";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(0, nlohmann::json::parse(results_json_str)["hits"].size());

  req_params["x-typesense-user-id"] = "user3";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  ASSERT_EQ(0, nlohmann::json::parse(results_json_str)["hits"].size());

  search_analytics.compact_all_user_queries(future_ts_us);

  get_counter_op = search_analytics.get_search_counter_events();
  ASSERT_EQ(get_counter_op.size(), 2);
  ASSERT_EQ(get_counter_op["with_capture_nohits"].query_counts.size(), 1);
  for(auto& [key, value] : get_counter_op["with_capture_nohits"].query_counts) {
    ASSERT_EQ(key.query, "nonexistent");
    ASSERT_EQ(key.tag_str, "tag1");
    ASSERT_EQ(key.filter_str, "country:US");
    ASSERT_EQ(value, 2);
  }
}

TEST_F(AnalyticsManagerTest, MigrateOldPopularQueriesRule) {
  nlohmann::json products_schema = R"({
        "name": "products",
        "fields": [
          {"name": "company_name", "type": "string" },
          {"name": "num_employees", "type": "int32" },
          {"name": "country", "type": "string", "facet": true },
          {"name": "popularity", "type": "int32", "optional": true}
        ],
        "default_sorting_field": "num_employees"
    })"_json;
  ASSERT_TRUE(collectionManager.create_collection(products_schema).ok());

  nlohmann::json pq_schema = R"({
        "name": "product_queries",
        "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
        ]
    })"_json;
  ASSERT_TRUE(collectionManager.create_collection(pq_schema).ok());

  nlohmann::json old_rule = R"({
    "name": "product_queries_aggregation",
    "type": "popular_queries",
    "params": {
      "source": { "collections": ["products"] },
      "destination": { "collection": "product_queries" },
      "expand_query": false,
      "limit": 1000
    }
  })"_json;

  auto create_op = analyticsManager.create_old_rule(old_rule);
  ASSERT_TRUE(create_op.ok());

  auto get_op = analyticsManager.get_rule("product_queries_aggregation");
  ASSERT_TRUE(get_op.ok());
  ASSERT_EQ(get_op.get()["name"], "product_queries_aggregation");
  ASSERT_EQ(get_op.get()["type"], "popular_queries");
  ASSERT_EQ(get_op.get()["collection"], "products");
  ASSERT_EQ(get_op.get()["event_type"], "search");
  ASSERT_EQ(get_op.get()["rule_tag"], "product_queries_aggregation");
  ASSERT_EQ(get_op.get()["params"]["destination_collection"], "product_queries");
  ASSERT_EQ(get_op.get()["params"]["limit"], 1000);
  ASSERT_EQ(get_op.get()["params"]["expand_query"], false);
  ASSERT_EQ(get_op.get()["params"]["capture_search_requests"], true);
}

TEST_F(AnalyticsManagerTest, MigrateOldPopularQueriesEventRule) {
  nlohmann::json products_schema = R"({
        "name": "products",
        "fields": [
          {"name": "company_name", "type": "string" },
          {"name": "num_employees", "type": "int32" },
          {"name": "country", "type": "string", "facet": true },
          {"name": "popularity", "type": "int32", "optional": true}
        ],
        "default_sorting_field": "num_employees"
    })"_json;
  ASSERT_TRUE(collectionManager.create_collection(products_schema).ok());

  nlohmann::json pq_schema = R"({
        "name": "product_queries",
        "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
        ]
    })"_json;
  ASSERT_TRUE(collectionManager.create_collection(pq_schema).ok());

  nlohmann::json old_rule = R"({
    "name": "product_queries_aggregation",
    "type": "popular_queries",
    "params": {
      "source": {
        "collections": ["products"],
        "enable_auto_aggregation": false,
        "events": [{"type": "search", "name": "products_search_event"}]
      },
      "destination": { "collection": "product_queries" },
      "limit": 1000
    }
  })"_json;

  auto create_op = analyticsManager.create_old_rule(old_rule);
  ASSERT_TRUE(create_op.ok());

  auto get_op = analyticsManager.get_rule("products_search_event");
  ASSERT_TRUE(get_op.ok());
  ASSERT_EQ(get_op.get()["name"], "products_search_event");
  ASSERT_EQ(get_op.get()["type"], "popular_queries");
  ASSERT_EQ(get_op.get()["collection"], "products");
  ASSERT_EQ(get_op.get()["event_type"], "search");
  ASSERT_EQ(get_op.get()["rule_tag"], "product_queries_aggregation");
  ASSERT_EQ(get_op.get()["params"]["destination_collection"], "product_queries");
  ASSERT_EQ(get_op.get()["params"]["limit"], 1000);
  ASSERT_EQ(get_op.get()["params"]["capture_search_requests"], false);
}

TEST_F(AnalyticsManagerTest, MigrateOldNoHitsQueriesRule) {
  nlohmann::json products_schema = R"({
        "name": "products",
        "fields": [
          {"name": "company_name", "type": "string" },
          {"name": "num_employees", "type": "int32" },
          {"name": "country", "type": "string", "facet": true },
          {"name": "popularity", "type": "int32", "optional": true}
        ],
        "default_sorting_field": "num_employees"
    })"_json;
  ASSERT_TRUE(collectionManager.create_collection(products_schema).ok());

  nlohmann::json nh_schema = R"({
        "name": "no_hits_queries",
        "fields": [
          {"name": "q", "type": "string"},
          {"name": "count", "type": "int32"}
        ]
    })"_json;
  ASSERT_TRUE(collectionManager.create_collection(nh_schema).ok());

  nlohmann::json old_rule = R"({
    "name": "product_no_hits",
    "type": "nohits_queries",
    "params": {
      "source": { "collections": ["products"] },
      "destination": { "collection": "no_hits_queries" },
      "limit": 1000
    }
  })"_json;

  auto create_op = analyticsManager.create_old_rule(old_rule);
  ASSERT_TRUE(create_op.ok());

  auto get_op = analyticsManager.get_rule("product_no_hits");
  ASSERT_TRUE(get_op.ok());
  ASSERT_EQ(get_op.get()["name"], "product_no_hits");
  ASSERT_EQ(get_op.get()["type"], "nohits_queries");
  ASSERT_EQ(get_op.get()["collection"], "products");
  ASSERT_EQ(get_op.get()["event_type"], "search");
  ASSERT_EQ(get_op.get()["rule_tag"], "product_no_hits");
  ASSERT_EQ(get_op.get()["params"]["destination_collection"], "no_hits_queries");
  ASSERT_EQ(get_op.get()["params"]["limit"], 1000);
}

TEST_F(AnalyticsManagerTest, MigrateOldCounterRule) {
  nlohmann::json products_schema = R"({
        "name": "products",
        "fields": [
          {"name": "company_name", "type": "string" },
          {"name": "num_employees", "type": "int32" },
          {"name": "country", "type": "string", "facet": true },
          {"name": "popularity", "type": "int32", "optional": true}
        ],
        "default_sorting_field": "num_employees"
    })"_json;
  ASSERT_TRUE(collectionManager.create_collection(products_schema).ok());

  nlohmann::json old_rule = R"({
    "name": "product_clicks",
    "type": "counter",
    "params": {
      "source": {
        "collections": ["products"],
        "events": [{"type": "click", "weight": 1, "name": "products_click_event"}]
      },
      "destination": {
        "collection": "products",
        "counter_field": "popularity"
      }
    }
  })"_json;

  auto create_op = analyticsManager.create_old_rule(old_rule);
  ASSERT_TRUE(create_op.ok());

  auto get_op = analyticsManager.get_rule("products_click_event");
  ASSERT_TRUE(get_op.ok());
  ASSERT_EQ(get_op.get()["name"], "products_click_event");
  ASSERT_EQ(get_op.get()["type"], "counter");
  ASSERT_EQ(get_op.get()["collection"], "products");
  ASSERT_EQ(get_op.get()["event_type"], "click");
  ASSERT_EQ(get_op.get()["rule_tag"], "product_clicks");
  ASSERT_EQ(get_op.get()["params"]["destination_collection"], "products");
  ASSERT_EQ(get_op.get()["params"]["counter_field"], "popularity");
  ASSERT_EQ(get_op.get()["params"]["weight"], 1);

  auto missing_op = analyticsManager.get_rule("product_clicks");
  ASSERT_FALSE(missing_op.ok());
}

TEST_F(AnalyticsManagerTest, DocCounterEvents) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string" },
        {"name": "popularity", "type": "int32" }
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());
  auto coll = coll_create_op.get();

  nlohmann::json doc;
  doc["id"] = "1";
  doc["company_name"] = "Typesense";
  doc["num_employees"] = 10;
  doc["country"] = "US";
  doc["popularity"] = 5;
  ASSERT_TRUE(coll->add(doc.dump()).ok());

  doc["id"] = "2";
  doc["num_employees"] = 20;
  doc["popularity"] = 8;
  ASSERT_TRUE(coll->add(doc.dump()).ok());

  nlohmann::json counter_rule = R"({
    "name": "product_popularity",
    "type": "counter",
    "collection": "products",
    "event_type": "click",
    "params": {
      "counter_field": "popularity",
      "weight": 2
    }
  })"_json;

  auto create_op = analyticsManager.create_rule(counter_rule, false, true, true);
  ASSERT_TRUE(create_op.ok());

  auto add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "product_popularity",
    "data": {
      "doc_id": "1",
      "user_id": "user1"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "product_popularity",
    "data": {
      "doc_ids": ["1", "2"],
      "user_id": "user2"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  auto events = doc_analytics.get_doc_counter_events();
  ASSERT_EQ(events.size(), 1);
  ASSERT_TRUE(events.find("product_popularity") != events.end());
  const auto& counter = events["product_popularity"];
  ASSERT_EQ(counter.counter_field, "popularity");
  ASSERT_EQ(counter.destination_collection, "products");
  ASSERT_EQ(counter.docid_counts.size(), 2);
  ASSERT_EQ(counter.docid_counts.at("1"), 4);
  ASSERT_EQ(counter.docid_counts.at("2"), 2);
}

TEST_F(AnalyticsManagerTest, SearchWithNoRule) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true },
        {"name": "popularity", "type": "int32", "optional": true}
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());
  auto coll = coll_create_op.get();

  nlohmann::json doc;
  doc["company_name"] = "Typesense";
  doc["num_employees"] = 10;
  doc["country"] = "US";
  doc["popularity"] = 10;

  auto add_op = coll->add(doc.dump());
  ASSERT_TRUE(add_op.ok());

  std::map<std::string, std::string> req_params = {
    {"q", "type"},
    {"query_by", "company_name"},
    {"collection", "products"},
    {"x-typesense-user-id", "user2"},
    {"analytics_tag", "tag1"},
    {"filter_by", "country:US"}
  };
  nlohmann::json embedded_params;
  std::string results_json_str;
  auto results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
}

TEST_F(AnalyticsManagerTest, QueryLogEventsGetInMemory) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true }
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());

  nlohmann::json log_rule = R"({
    "name": "log_queries",
    "type": "log",
    "collection": "products",
    "event_type": "search",
    "rule_tag": "log_queries",
    "params": {
      "capture_search_requests": false,
      "meta_fields": ["filter_by", "analytics_tag"]
    }
  })"_json;

  auto create_op = analyticsManager.create_rule(log_rule, false, true, true);
  ASSERT_TRUE(create_op.ok());

  auto add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "log_queries",
    "data": {
      "q": "alpha",
      "user_id": "user2",
      "analytics_tag": "tag1",
      "filter_by": "country:US"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "log_queries",
    "data": {
      "q": "beta",
      "user_id": "user2",
      "analytics_tag": "tag1",
      "filter_by": "country:CA"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "log_queries",
    "data": {
      "q": "gamma",
      "user_id": "user3",
      "analytics_tag": "tag2",
      "filter_by": "country:US"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  auto get_events_op = analyticsManager.get_events("user2", "log_queries", 10);
  ASSERT_TRUE(get_events_op.ok());
  const auto& events = get_events_op.get()["events"].get<std::vector<nlohmann::json>>();
  ASSERT_EQ(events.size(), 2);

  ASSERT_EQ(events[0]["name"], "log_queries");
  ASSERT_EQ(events[0]["event_type"], "search");
  ASSERT_EQ(events[0]["collection"], "products");
  ASSERT_EQ(events[0]["user_id"], "user2");
  ASSERT_EQ(events[0]["query"], "beta");
  ASSERT_EQ(events[0]["filter_by"], "country:CA");
  ASSERT_EQ(events[0]["analytics_tag"], "tag1");

  ASSERT_EQ(events[1]["name"], "log_queries");
  ASSERT_EQ(events[1]["event_type"], "search");
  ASSERT_EQ(events[1]["collection"], "products");
  ASSERT_EQ(events[1]["user_id"], "user2");
  ASSERT_EQ(events[1]["query"], "alpha");
  ASSERT_EQ(events[1]["filter_by"], "country:US");
  ASSERT_EQ(events[1]["analytics_tag"], "tag1");
}

TEST_F(AnalyticsManagerTest, DocLogEventsGetInMemory) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "id", "type": "string" },
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" }
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());
  auto coll = coll_create_op.get();

  nlohmann::json doc;
  doc["id"] = "1";
  doc["company_name"] = "Typesense";
  doc["num_employees"] = 10;
  ASSERT_TRUE(coll->add(doc.dump()).ok());
  doc["id"] = "2";
  doc["num_employees"] = 20;
  ASSERT_TRUE(coll->add(doc.dump()).ok());

  nlohmann::json log_rule = R"({
    "name": "doc_click_logs",
    "type": "log",
    "collection": "products",
    "event_type": "click",
    "rule_tag": "doc_logs"
  })"_json;

  auto create_op = analyticsManager.create_rule(log_rule, false, true, true);
  ASSERT_TRUE(create_op.ok());

  auto add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "doc_click_logs",
    "data": {
      "user_id": "user2",
      "doc_id": "1",
      "query": "type"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "doc_click_logs",
    "data": {
      "user_id": "user2",
      "doc_ids": ["1", "2"],
      "query": "typesense"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  // Another user's event should not appear when fetching for user2
  add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "doc_click_logs",
    "data": {
      "user_id": "user3",
      "doc_id": "2",
      "query": "typesense"
    }
  })"_json);
  ASSERT_TRUE(add_event_op.ok());

  auto get_events_op = analyticsManager.get_events("user2", "doc_click_logs", 10);
  ASSERT_TRUE(get_events_op.ok());
  const auto& events = get_events_op.get()["events"].get<std::vector<nlohmann::json>>();
  ASSERT_EQ(events.size(), 2);

  // Reverse chronological order: multi-doc event first
  ASSERT_EQ(events[0]["name"], "doc_click_logs");
  ASSERT_EQ(events[0]["event_type"], "click");
  ASSERT_EQ(events[0]["collection"], "products");
  ASSERT_EQ(events[0]["user_id"], "user2");
  ASSERT_EQ(events[0]["query"], "typesense");
  ASSERT_TRUE(events[0].contains("doc_ids"));
  ASSERT_EQ(events[0]["doc_ids"].size(), 2);

  ASSERT_EQ(events[1]["name"], "doc_click_logs");
  ASSERT_EQ(events[1]["event_type"], "click");
  ASSERT_EQ(events[1]["collection"], "products");
  ASSERT_EQ(events[1]["user_id"], "user2");
  ASSERT_EQ(events[1]["query"], "type");
  ASSERT_TRUE(events[1].contains("doc_id"));
  ASSERT_EQ(events[1]["doc_id"], "1");
}

TEST_F(AnalyticsManagerTest, QueryLogEventsWithCaptureGetInMemory) {
  nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "company_name", "type": "string" },
        {"name": "num_employees", "type": "int32" },
        {"name": "country", "type": "string", "facet": true }
      ],
      "default_sorting_field": "num_employees"
  })"_json;

  auto coll_create_op = collectionManager.create_collection(products_schema);
  ASSERT_TRUE(coll_create_op.ok());
  auto coll = coll_create_op.get();

  nlohmann::json doc;
  doc["company_name"] = "Typesense";
  doc["num_employees"] = 10;
  doc["country"] = "US";
  ASSERT_TRUE(coll->add(doc.dump()).ok());

  nlohmann::json log_rule = R"({
    "name": "log_with_capture",
    "type": "log",
    "collection": "products",
    "event_type": "search",
    "rule_tag": "log_queries",
    "params": {
      "destination_collection": "ignored",
      "capture_search_requests": true,
      "meta_fields": ["filter_by", "analytics_tag"]
    }
  })"_json;

  auto create_op = analyticsManager.create_rule(log_rule, false, true, true);
  ASSERT_TRUE(create_op.ok());

  std::string results_json_str;
  std::map<std::string, std::string> req_params = {
    {"q", "type"},
    {"query_by", "company_name"},
    {"collection", "products"},
    {"x-typesense-user-id", "user2"},
    {"analytics_tag", "tag1"},
    {"filter_by", "country:US"}
  };
  nlohmann::json embedded_params;
  auto results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  req_params["q"] = "typesen";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());
  req_params["q"] = "typesense";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());

  // Another user
  req_params["x-typesense-user-id"] = "user3";
  req_params["analytics_tag"] = "tag2";
  req_params["q"] = "hello";
  results = CollectionManager::do_search(req_params, embedded_params, results_json_str, 0);
  ASSERT_TRUE(results.ok());

  auto future_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count() + uint64_t(SearchAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS) * 2;
  search_analytics.compact_all_user_queries(future_ts_us);

  auto get_events_op = analyticsManager.get_events("user2", "log_with_capture", 10);
  ASSERT_TRUE(get_events_op.ok());
  const auto& events = get_events_op.get()["events"].get<std::vector<nlohmann::json>>();
  ASSERT_EQ(events.size(), 1);
  ASSERT_EQ(events[0]["name"], "log_with_capture");
  ASSERT_EQ(events[0]["event_type"], "search");
  ASSERT_EQ(events[0]["collection"], "products");
  ASSERT_EQ(events[0]["user_id"], "user2");
  ASSERT_EQ(events[0]["query"], "typesense");
  ASSERT_EQ(events[0]["filter_by"], "country:US");
  ASSERT_EQ(events[0]["analytics_tag"], "tag1");
}

TEST_F(AnalyticsManagerTest, UninitializedAnalyticsStore) {
    //when analytics store is not uninitialized, it should return empty events

    system(("rm -rf " + analytics_dir_path + " && mkdir -p " + analytics_dir_path).c_str());

    collectionManager.init(store, 1.0, "auth_key", quit);
    collectionManager.load(8, 1000);

    analyticsManager.init(store, nullptr, analytics_minute_rate_limit);  //initialize with null anlaytics store
    analyticsManager.resetToggleRateLimit(false);

    nlohmann::json products_schema = R"({
      "name": "products",
      "fields": [
        {"name": "name", "type": "string" }
      ]
    })"_json;

    auto coll_create_op = collectionManager.create_collection(products_schema);
    ASSERT_TRUE(coll_create_op.ok());
    auto coll1 = coll_create_op.get();

    nlohmann::json products_queries_schema = R"({
      "name": "products_queries",
       "fields": [
          {"name": "q", "type": "string" },
          {"name": "count", "type": "int32" }
        ]
    })"_json;

    coll_create_op = collectionManager.create_collection(products_queries_schema);
    ASSERT_TRUE(coll_create_op.ok());
    auto coll2 = coll_create_op.get();

    nlohmann::json popular_queries_analytics_rule = R"({
        "name": "product_queries_aggregation",
        "type": "popular_queries",
        "collection": "products",
        "event_type": "search",
        "rule_tag": "popular_queries",
        "params": {
          "destination_collection": "products_queries",
          "capture_search_requests": false,
          "limit": 1000
        }
      })"_json;

    auto create_op = analyticsManager.create_rule(popular_queries_analytics_rule, false, true, true);
    ASSERT_TRUE(create_op.ok());

    auto add_event_op = analyticsManager.add_external_event("127.0.0.1", R"({
    "name": "product_queries_aggregation",
    "event_type": "search",
    "data": {
            "q": "running shoes",
            "user_id": "1234"
    }})"_json);
    ASSERT_TRUE(add_event_op.ok());

    auto get_events_op = analyticsManager.get_events("1234", "product_queries_aggregation", 10);
    ASSERT_TRUE(get_events_op.ok());
    const auto& events = get_events_op.get()["events"].get<std::vector<nlohmann::json>>();
    ASSERT_TRUE(events.empty());
}