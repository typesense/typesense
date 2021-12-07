#include <gtest/gtest.h>
#include "app_metrics.h"

class AppMetricsTest : public ::testing::Test {
protected:
    AppMetrics& metrics = AppMetrics::get_instance();

    virtual void SetUp() {

    }

    virtual void TearDown() {

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
