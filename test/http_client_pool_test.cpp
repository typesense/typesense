#include <gtest/gtest.h>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include "http_client.h"

// a closed local port refuses instantly without leaving the machine. concurrent leases must
// never share a live handle and every handle must land back in the bounded idle pool
TEST(HttpClientPoolTest, ConcurrentLeasesReturnToBoundedPool) {
    HttpClient& client = HttpClient::get_instance();
    client.init("pool-test-key");

    constexpr size_t NUM_THREADS = 12;
    constexpr int CALLS_PER_THREAD = 3;

    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    for(size_t i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([]() {
            for(int j = 0; j < CALLS_PER_THREAD; j++) {
                std::string response;
                std::map<std::string, std::string> res_headers;
                HttpClient::get_response("http://127.0.0.1:1/", response, res_headers, {}, 300);
            }
        });
    }
    for(auto& thread : threads) {
        thread.join();
    }

    const size_t idle = HttpClient::get_idle_handle_count();
    ASSERT_GE(idle, 1);
    // CURL_POOL_MAX_IDLE
    ASSERT_LE(idle, 32);
}

TEST(HttpClientPoolTest, LeaseReusesTheWarmestHandleLifo) {
    CURL* first = HttpClient::lease_handle_for_test();
    ASSERT_NE(nullptr, first);
    HttpClient::release_handle_for_test(first);

    // lifo reuse hands the very handle just returned, a pool that always inits fresh fails here
    CURL* second = HttpClient::lease_handle_for_test();
    ASSERT_EQ(first, second);
    HttpClient::release_handle_for_test(second);
}

TEST(HttpClientPoolTest, ReleaseEvictsBeyondTheIdleCap) {
    constexpr size_t OVER_CAP = 40;

    std::vector<CURL*> leased;
    leased.reserve(OVER_CAP);
    for(size_t i = 0; i < OVER_CAP; i++) {
        CURL* curl = HttpClient::lease_handle_for_test();
        ASSERT_NE(nullptr, curl);
        leased.push_back(curl);
    }

    for(CURL* curl : leased) {
        HttpClient::release_handle_for_test(curl);
    }

    // CURL_POOL_MAX_IDLE, releases past the cap must clean their handle instead of pooling it
    ASSERT_EQ(32, HttpClient::get_idle_handle_count());
}

TEST(HttpClientPoolTest, TransferCountAndMetricsAdvancePerTransfer) {
    const uint64_t count_before = HttpClient::get_transfer_count();

    std::string response;
    std::map<std::string, std::string> res_headers;
    HttpClient::get_response("http://127.0.0.1:1/", response, res_headers, {}, 300);

    // even a refused connect is one completed transfer with a timing split behind it
    ASSERT_EQ(count_before + 1, HttpClient::get_transfer_count());
    const http_transfer_metrics_t metrics = HttpClient::get_last_transfer_metrics();
    ASSERT_GE(metrics.total_ms, 0.0);

    HttpClient::get_response("http://127.0.0.1:1/", response, res_headers, {}, 300);
    ASSERT_EQ(count_before + 2, HttpClient::get_transfer_count());
}
