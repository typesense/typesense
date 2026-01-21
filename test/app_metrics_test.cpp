#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <collection_manager.h>
#include "app_metrics.h"
#include "collection.h"

class AppMetricsTest : public ::testing::Test {
protected:
    AppMetrics& metrics = AppMetrics::get_instance();
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    virtual void SetUp() {
        std::string state_dir_path = "/tmp/typesense_test/app_metrics_test_db";
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);
        
        metrics.window_reset();
    }

    virtual void TearDown() {
        metrics.window_reset();
        
        if(store != nullptr) {
            collectionManager.dispose();
            delete store;
        }
    }
};


TEST_F(AppMetricsTest, StatefulRemoveDocs) {
    metrics.increment_count("GET /collections", 1);
    metrics.increment_count("GET /collections", 1);
    metrics.increment_count("GET /operations/vote", 1);

    metrics.increment_duration("GET /collections", 2);
    metrics.increment_duration("GET /collections", 4);
    metrics.increment_duration("GET /operations/vote", 5);

    metrics.increment_count(AppMetrics::SEARCH_LABEL, 1);
    metrics.increment_count(AppMetrics::SEARCH_LABEL, 1);
    metrics.increment_duration(AppMetrics::SEARCH_LABEL, 16);
    metrics.increment_duration(AppMetrics::SEARCH_LABEL, 12);

    metrics.window_reset();

    nlohmann::json result;
    metrics.get("rps", "latency", result);

    ASSERT_EQ(result["search_latency"].get<double>(), 14.0);
    ASSERT_EQ(result["search_rps"].get<double>(), 0.2);

    ASSERT_EQ(result["latency"]["GET /collections"].get<double>(), 3.0);
    ASSERT_EQ(result["latency"]["GET /operations/vote"].get<double>(), 5.0);

    ASSERT_EQ(result["rps"]["GET /collections"].get<double>(), 0.2);
    ASSERT_EQ(result["rps"]["GET /operations/vote"].get<double>(), 0.1);
}

TEST_F(AppMetricsTest, EstimateQuantileDuration) {
    //add 100 random durations
    std::mt19937 rng;
    std::uniform_int_distribution<uint32_t> distrib(0, 1000);
    rng.seed(1);

    std::vector<int> durations;
    for(auto i = 0; i < 10000; ++i) {
        durations.push_back(distrib(rng));
    }

    std::sort(durations.begin(), durations.end());

    // add to appmetrics to get approximate percentile
    for(auto i = 0; i < 10000; ++i) {
        metrics.increment_count(AppMetrics::SEARCH_LABEL, 1);
        metrics.increment_duration(AppMetrics::SEARCH_LABEL, durations[i]);
    }

    metrics.window_reset();

    nlohmann::json result;
    metrics.get("rps", "latency", result);
    ASSERT_EQ(result["search_70Percentile_latency"], 701.0);
    ASSERT_EQ(result["search_95Percentile_latency"], 950.0);
    ASSERT_EQ(result["search_99Percentile_latency"], 990.0);

    // compute accurate percentile
    auto computeNthPercentile = [&](int percentile) -> int {
        auto total_elems = durations.size();
        auto index = (percentile * total_elems)/100.f;
        index = lround(index) - 1; //array bounds
        return durations[index];
    };

    ASSERT_EQ(computeNthPercentile(70), 701);
    ASSERT_EQ(computeNthPercentile(95), 950);
    ASSERT_EQ(computeNthPercentile(99), 990);
}

TEST_F(AppMetricsTest, SearchLatencyStats) {
    metrics.window_reset();

    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", -1, 1),
                                 field("points", field_types::INT32, false)};
    Collection* coll1 = collectionManager.create_collection("latency_test", 1, fields, "points").get();

    nlohmann::json doc1;
    doc1["title"] = "Test Doc";
    doc1["points"] = 100;
    coll1->add(doc1.dump());

    std::map<std::string, std::string> req_params;
    req_params["collection"] = "latency_test";
    req_params["q"] = "*";
    nlohmann::json embedded_params;
    std::string json_res;

    // Simulate request start time using system_clock (as done in http_server)
    auto start_ts = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    // Sleep briefly to ensure non-zero latency
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto search_op = collectionManager.do_search(req_params, embedded_params, json_res, start_ts);
    ASSERT_TRUE(search_op.ok());

    nlohmann::json metrics_result;
    metrics.get("rps", "latency", metrics_result);

    // Verify search latency is reasonable
    ASSERT_TRUE(metrics_result.contains("search_latency"));
    double latency = metrics_result["search_latency"];
    
    // Check for overflow (huge values) or negative values (cast to double might preserve sign or look huge unsigned)
    // The bug produced values like 1.84e19
    ASSERT_GE(latency, 0.0);
    ASSERT_LT(latency, 100000.0); // 100s is plenty margin, well below 1.84e19

    collectionManager.drop_collection("latency_test");
}

TEST_F(AppMetricsTest, MultiSearchLatencyStats) {
    // Test both multi-search scenarios: without union and with union
    
    // Create two collections for multi search
    std::vector<field> fields = {field("title", field_types::STRING, false, false, true, "", -1, 1),
                                 field("points", field_types::INT32, false)};
    
    Collection* products_coll = collectionManager.create_collection("products_collection", 1, fields, "points").get();
    Collection* reviews_coll = collectionManager.create_collection("reviews_collection", 1, fields, "points").get();

    // Add test documents
    nlohmann::json product_doc;
    product_doc["title"] = "Test Product";
    product_doc["points"] = 100;
    products_coll->add(product_doc.dump());

    nlohmann::json review_doc;
    review_doc["title"] = "Test Review";
    review_doc["points"] = 200;
    reviews_coll->add(review_doc.dump());

    // Prepare multi search request
    std::map<std::string, std::string> req_params;
    nlohmann::json searches = nlohmann::json::array();
    
    nlohmann::json products_search;
    products_search["collection"] = "products_collection";
    products_search["q"] = "*";
    searches.push_back(products_search);

    nlohmann::json reviews_search;
    reviews_search["collection"] = "reviews_collection";
    reviews_search["q"] = "*";
    searches.push_back(reviews_search);

    std::vector<nlohmann::json> embedded_params_vec = {nlohmann::json::object(), nlohmann::json::object()};
    nlohmann::json response;

    // Test 1: Multi-search WITHOUT union
    {
        metrics.window_reset();

        auto start_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        auto multi_op = collectionManager.do_union(req_params, embedded_params_vec, searches, response, start_ts, false);
        ASSERT_TRUE(multi_op.ok());

        nlohmann::json metrics_result;
        metrics.get("rps", "latency", metrics_result);

        ASSERT_TRUE(metrics_result.contains("search_latency"));
        double latency = metrics_result["search_latency"];
        
        ASSERT_GE(latency, 0.0);
        ASSERT_LT(latency, 100000.0); // 100s is plenty margin, well below 1.84e19
    }

    // Test 2: Multi-search WITH union
    {
        metrics.window_reset();

        auto start_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        auto union_op = collectionManager.do_union(req_params, embedded_params_vec, searches, response, start_ts, true);
        ASSERT_TRUE(union_op.ok());

        nlohmann::json metrics_result;
        metrics.get("rps", "latency", metrics_result);

        ASSERT_TRUE(metrics_result.contains("search_latency"));
        double latency = metrics_result["search_latency"];
        
        ASSERT_GE(latency, 0.0);
        ASSERT_LT(latency, 100000.0); // 100s is plenty margin, well below 1.84e19
    }

    collectionManager.drop_collection("products_collection");
    collectionManager.drop_collection("reviews_collection");
}