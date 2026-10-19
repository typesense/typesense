#include <gtest/gtest.h>
#include <glog/logging.h>
#include <atomic>
#include <mutex>
#include <thread>
#include "housekeeper.h"

namespace {
class capture_log_sink_t: public google::LogSink {
    std::mutex mutex;
    std::vector<std::string> messages;

public:
    void send(google::LogSeverity severity, const char* full_filename, const char* base_filename, int line,
              const google::LogMessageTime& logmsgtime, const char* message, size_t message_len) override {
        std::unique_lock lock(mutex);
        messages.emplace_back(message, message_len);
    }

    bool contains(const std::string& text) {
        std::unique_lock lock(mutex);
        for(const auto& message: messages) {
            if(message.find(text) != std::string::npos) {
                return true;
            }
        }
        return false;
    }
};

std::shared_ptr<http_req> make_req(uint64_t start_ts, const std::string& q) {
    auto req = std::make_shared<http_req>();
    req->start_ts = start_ts;
    req->params["collection"] = "products";
    req->params["q"] = q;
    req->body = R"({"searches":[{"collection":"products","q":"*","per_page":250}]})";
    return req;
}
}

TEST(HouseKeeperTest, QueryLogIsSnapshotWhenRequestIsAdded) {
    auto req = make_req(1001, "original_query");
    HouseKeeper::get_instance().add_req(req);

    // search thread mutates the params of the in-flight request
    req->params["q"] = "mutated_query";

    capture_log_sink_t sink;
    google::AddLogSink(&sink);
    HouseKeeper::get_instance().log_running_queries();
    google::RemoveLogSink(&sink);

    HouseKeeper::get_instance().remove_req(req->start_ts);

    ASSERT_TRUE(sink.contains("q=original_query&"));
    ASSERT_FALSE(sink.contains("mutated_query"));
}

TEST(HouseKeeperTest, LogInFlightQueriesWhileParamsAreMutated) {
    // old enough to be logged as a long running query
    const uint64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    auto req = make_req(now_us - 600 * 1000000UL, "q0");
    HouseKeeper::get_instance().add_req(req);

    const auto orig_min_log_level = FLAGS_minloglevel;
    FLAGS_minloglevel = google::GLOG_WARNING;

    std::atomic<bool> done = false;

    // mimics multi_search, which resets and rebuilds `req->params` for every search in the request
    std::thread search_thread([&]() {
        const auto orig_params = req->params;
        size_t i = 0;
        while(!done) {
            req->params = orig_params;
            for(size_t j = 0; j < 32; j++) {
                req->params["param_" + std::to_string(j)] = std::to_string(i);
            }
            i++;
        }
    });

    for(size_t i = 0; i < 2000; i++) {
        HouseKeeper::get_instance().log_running_queries();
        HouseKeeper::get_instance().log_bad_queries();
    }

    done = true;
    search_thread.join();
    FLAGS_minloglevel = orig_min_log_level;

    ASSERT_EQ(1, HouseKeeper::get_instance().get_num_inflight_queries());
    HouseKeeper::get_instance().remove_req(req->start_ts);
    ASSERT_EQ(0, HouseKeeper::get_instance().get_num_inflight_queries());
}

TEST(HouseKeeperTest, QueryLogTruncatesLargeBody) {
    auto req = make_req(1002, "foo");
    req->body = std::string(HouseKeeper::MAX_QUERY_LOG_BODY_SIZE + 100, 'a');

    const std::string& query_log = HouseKeeper::get_instance().get_query_log(req);
    ASSERT_NE(std::string::npos, query_log.find("qs=?collection=products&q=foo&"));
    ASSERT_NE(std::string::npos, query_log.find(std::string(HouseKeeper::MAX_QUERY_LOG_BODY_SIZE, 'a') + "...[truncated]"));
    ASSERT_EQ(std::string::npos, query_log.find(std::string(HouseKeeper::MAX_QUERY_LOG_BODY_SIZE + 1, 'a')));
}
