#include <gtest/gtest.h>
#include "query_analytics.h"
#include "logger.h"

class PopularQueriesTest : public ::testing::Test {
protected:
    virtual void SetUp() {

    }

    virtual void TearDown() {

    }
};

TEST_F(PopularQueriesTest, PrefixQueryCompaction) {
    QueryAnalytics pq(10);

    auto now_ts_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    // compaction when no queries have been entered
    pq.compact_user_queries(now_ts_us);
    auto queries = pq.get_user_prefix_queries();
    ASSERT_TRUE(queries.empty());

    // compaction after user has typed first prefix but before compaction interval has happened
    pq.add("f", "f", true, "0", now_ts_us+1);
    pq.compact_user_queries(now_ts_us+2);
    queries = pq.get_user_prefix_queries();
    ASSERT_EQ(1, queries.size());
    ASSERT_EQ(1, queries.count("0"));
    ASSERT_EQ(1, queries["0"].size());
    ASSERT_EQ("f", queries["0"][0].query);
    ASSERT_EQ(now_ts_us+1, queries["0"][0].timestamp);
    ASSERT_EQ(0, pq.get_local_counts().size());

    // compaction interval has happened
    pq.compact_user_queries(now_ts_us + QueryAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS + 100);
    queries = pq.get_user_prefix_queries();
    ASSERT_EQ(0, queries.size());
    auto local_counts = pq.get_local_counts();
    ASSERT_EQ(1, local_counts.size());
    ASSERT_EQ(1, local_counts.count("f"));
    ASSERT_EQ(1, local_counts["f"]);

    // 3 letter search
    pq.reset_local_counts();
    pq.add("f", "f", true, "0", now_ts_us+1);
    pq.add("fo", "fo", true, "0", now_ts_us+2);
    pq.add("foo", "foo", true, "0", now_ts_us+3);
    pq.compact_user_queries(now_ts_us + QueryAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS + 100);
    queries = pq.get_user_prefix_queries();
    ASSERT_EQ(0, queries.size());
    local_counts = pq.get_local_counts();
    ASSERT_EQ(1, local_counts.size());
    ASSERT_EQ(1, local_counts.count("foo"));
    ASSERT_EQ(1, local_counts["foo"]);

    // 3 letter search + start of next search
    pq.reset_local_counts();
    pq.add("f", "f", true, "0", now_ts_us+1);
    pq.add("fo", "fo", true, "0", now_ts_us+2);
    pq.add("foo", "foo", true, "0", now_ts_us+3);
    pq.add("b", "b", true, "0", now_ts_us + 3 + QueryAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS + 100);
    pq.compact_user_queries(now_ts_us + 3 + QueryAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS + 100 + 1);
    queries = pq.get_user_prefix_queries();
    ASSERT_EQ(1, queries.size());
    ASSERT_EQ(1, queries["0"].size());
    ASSERT_EQ("b", queries["0"][0].query);
    local_counts = pq.get_local_counts();
    ASSERT_EQ(1, local_counts.size());
    ASSERT_EQ(1, local_counts.count("foo"));
    ASSERT_EQ(1, local_counts["foo"]);

    // continue with that query
    auto prev_ts = now_ts_us + 3 + QueryAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS + 100 + 1;
    pq.add("ba", "ba", true, "0", prev_ts+1);
    pq.add("bar", "bar", true, "0", prev_ts+2);
    pq.compact_user_queries(prev_ts + 2 + QueryAnalytics::QUERY_FINALIZATION_INTERVAL_MICROS + 1);
    queries = pq.get_user_prefix_queries();
    ASSERT_EQ(0, queries.size());
    local_counts = pq.get_local_counts();
    ASSERT_EQ(2, local_counts.size());
    ASSERT_EQ(1, local_counts.count("bar"));
    ASSERT_EQ(1, local_counts["bar"]);
}
