#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "http_client.h"

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

struct recorded_request_t {
    std::string method;
    std::string path;
    std::string version;
    std::string body;
    std::map<std::string, std::string> headers;
};

// minimal keep-alive http/1.1 server on an ephemeral local port, optionally behind tls with an
// in-memory self-signed cert. records every request and counts accepted connections
class test_local_server_t {
public:
    explicit test_local_server_t(bool use_tls, std::shared_future<void> response_ready = {}):
        use_tls(use_tls), response_ready(std::move(response_ready)) {}

    ~test_local_server_t() {
        stop();
    }

    bool start() {
        if(use_tls && !init_tls()) {
            return false;
        }

        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if(listen_fd < 0) {
            return false;
        }

        int enable = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
           listen(listen_fd, 8) != 0) {
            close(listen_fd);
            listen_fd = -1;
            return false;
        }

        socklen_t addr_len = sizeof(addr);
        getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len);
        bound_port = ntohs(addr.sin_port);

        running = true;
        accept_thread = std::thread(accept_loop_thunk, this);
        return true;
    }

    void stop() {
        if(!running.exchange(false)) {
            return;
        }

        shutdown(listen_fd, SHUT_RDWR);
        close(listen_fd);
        listen_fd = -1;

        {
            std::lock_guard<std::mutex> lock(mutex);
            for(int fd : open_fds) {
                shutdown(fd, SHUT_RDWR);
            }
        }

        if(accept_thread.joinable()) {
            accept_thread.join();
        }
        for(auto& thread : conn_threads) {
            if(thread.joinable()) {
                thread.join();
            }
        }

        if(ssl_ctx != nullptr) {
            SSL_CTX_free(ssl_ctx);
            ssl_ctx = nullptr;
        }
    }

    int port() const {
        return bound_port;
    }

    std::string base_url() const {
        return (use_tls ? "https://127.0.0.1:" : "http://127.0.0.1:") + std::to_string(bound_port);
    }

    size_t connection_count() {
        return connections.load();
    }

    std::vector<recorded_request_t> requests() {
        std::lock_guard<std::mutex> lock(mutex);
        return recorded;
    }

private:
    bool init_tls() {
        EVP_PKEY_CTX* key_ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
        if(key_ctx == nullptr) {
            return false;
        }
        EVP_PKEY* pkey = nullptr;
        bool key_ok = EVP_PKEY_keygen_init(key_ctx) > 0 &&
                      EVP_PKEY_CTX_set_rsa_keygen_bits(key_ctx, 2048) > 0 &&
                      EVP_PKEY_keygen(key_ctx, &pkey) > 0;
        EVP_PKEY_CTX_free(key_ctx);
        if(!key_ok) {
            EVP_PKEY_free(pkey);
            return false;
        }

        X509* cert = X509_new();
        ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
        X509_gmtime_adj(X509_get_notBefore(cert), 0);
        X509_gmtime_adj(X509_get_notAfter(cert), 3600);
        X509_set_pubkey(cert, pkey);
        X509_NAME* name = X509_get_subject_name(cert);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   reinterpret_cast<const unsigned char*>("127.0.0.1"), -1, -1, 0);
        X509_set_issuer_name(cert, name);
        X509_sign(cert, pkey, EVP_sha256());

        ssl_ctx = SSL_CTX_new(TLS_server_method());
        bool ok = ssl_ctx != nullptr &&
                  SSL_CTX_use_certificate(ssl_ctx, cert) == 1 &&
                  SSL_CTX_use_PrivateKey(ssl_ctx, pkey) == 1;

        X509_free(cert);
        EVP_PKEY_free(pkey);
        return ok;
    }

    static void accept_loop_thunk(test_local_server_t* self) {
        self->accept_loop();
    }

    static void handle_connection_thunk(test_local_server_t* self, int fd) {
        self->handle_connection(fd);
    }

    void accept_loop() {
        while(running.load()) {
            int fd = accept(listen_fd, nullptr, nullptr);
            if(fd < 0) {
                if(!running.load()) {
                    break;
                }
                continue;
            }
            connections++;
            std::lock_guard<std::mutex> lock(mutex);
            open_fds.push_back(fd);
            conn_threads.emplace_back(handle_connection_thunk, this, fd);
        }
    }

    ssize_t conn_read(SSL* ssl, int fd, char* buf, size_t len) {
        return ssl != nullptr ? SSL_read(ssl, buf, len) : recv(fd, buf, len, 0);
    }

    void conn_write(SSL* ssl, int fd, const std::string& data) {
        if(ssl != nullptr) {
            SSL_write(ssl, data.c_str(), data.size());
        } else {
            send(fd, data.c_str(), data.size(), MSG_NOSIGNAL);
        }
    }

    void handle_connection(int fd) {
        SSL* ssl = nullptr;
        if(use_tls) {
            ssl = SSL_new(ssl_ctx);
            SSL_set_fd(ssl, fd);
            if(SSL_accept(ssl) != 1) {
                // a client rejecting the self-signed cert aborts the handshake here
                SSL_free(ssl);
                close(fd);
                return;
            }
        }

        std::string buffer;
        char chunk[4096];
        while(running.load()) {
            size_t header_end = buffer.find("\r\n\r\n");
            while(header_end == std::string::npos) {
                ssize_t n = conn_read(ssl, fd, chunk, sizeof(chunk));
                if(n <= 0) {
                    goto done;
                }
                buffer.append(chunk, n);
                header_end = buffer.find("\r\n\r\n");
            }

            recorded_request_t request;
            size_t content_length = 0;
            parse_headers(buffer.substr(0, header_end), request, content_length);

            size_t body_start = header_end + 4;
            while(buffer.size() < body_start + content_length) {
                ssize_t n = conn_read(ssl, fd, chunk, sizeof(chunk));
                if(n <= 0) {
                    goto done;
                }
                buffer.append(chunk, n);
            }
            request.body = buffer.substr(body_start, content_length);
            buffer.erase(0, body_start + content_length);

            {
                std::lock_guard<std::mutex> lock(mutex);
                recorded.push_back(request);
            }

            if(response_ready.valid()) {
                response_ready.wait();
            }

            conn_write(ssl, fd, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                                "Content-Length: 11\r\nConnection: keep-alive\r\n\r\n{\"ok\":true}");
        }

    done:
        if(ssl != nullptr) {
            SSL_shutdown(ssl);
            SSL_free(ssl);
        }
        close(fd);
    }

    static void parse_headers(const std::string& head, recorded_request_t& request, size_t& content_length) {
        size_t line_end = head.find("\r\n");
        std::string request_line = head.substr(0, line_end);
        size_t sp1 = request_line.find(' ');
        size_t sp2 = request_line.rfind(' ');
        request.method = request_line.substr(0, sp1);
        request.path = request_line.substr(sp1 + 1, sp2 - sp1 - 1);
        request.version = request_line.substr(sp2 + 1);

        size_t pos = line_end == std::string::npos ? head.size() : line_end + 2;
        while(pos < head.size()) {
            size_t next = head.find("\r\n", pos);
            if(next == std::string::npos) {
                next = head.size();
            }
            std::string line = head.substr(pos, next - pos);
            size_t colon = line.find(':');
            if(colon != std::string::npos) {
                std::string key = line.substr(0, colon);
                for(char& c : key) {
                    c = std::tolower(c);
                }
                size_t value_start = line.find_first_not_of(' ', colon + 1);
                request.headers[key] = value_start == std::string::npos ? "" : line.substr(value_start);
            }
            pos = next + 2;
        }

        auto it = request.headers.find("content-length");
        if(it != request.headers.end()) {
            content_length = std::stoul(it->second);
        }
    }

    const bool use_tls;
    const std::shared_future<void> response_ready;
    int listen_fd = -1;
    int bound_port = 0;
    SSL_CTX* ssl_ctx = nullptr;
    std::atomic<bool> running{false};
    std::atomic<size_t> connections{0};
    std::thread accept_thread;
    std::mutex mutex;
    std::vector<std::thread> conn_threads;
    std::vector<int> open_fds;
    std::vector<recorded_request_t> recorded;
};

// Shutdown is permanent, so run in a fresh process without changing other tests' pool state.
TEST(HttpClientPoolDeathTest, ShutdownWaitsForPooledAndUnpooledTransfers) {
    const auto previous_style = ::testing::GTEST_FLAG(death_test_style);
    ::testing::GTEST_FLAG(death_test_style) = "threadsafe";
    EXPECT_EXIT(([]() {
        alarm(20);
        HttpClient::get_instance().init("pool-test-key");
        std::promise<void> allow_response;
        test_local_server_t server(false, allow_response.get_future().share());
        if(!server.start()) {
            _exit(1);
        }
        auto pooled = std::async(std::launch::async, [&]() {
            std::string response;
            std::map<std::string, std::string> headers;
            return HttpClient::get_response(server.base_url() + "/pooled", response, headers, {}, 10000);
        });
        auto unpooled = std::async(std::launch::async, [&]() {
            async_stream_response_t response;
            std::map<std::string, std::string> headers;
            const long status = HttpClient::post_response_stream(server.base_url() + "/stream", "{}", response,
                                                                 headers, {}, 10000);
            // A successful stream signals ready from its socket cleanup callback.
            if(!response.ready) {
                _exit(4);
            }
            return status;
        });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(server.requests().size() != 2 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if(server.requests().size() != 2 || HttpClient::get_live_handle_count() != 2) {
            _exit(2);
        }
        std::promise<void> shutdown_started;
        auto started = shutdown_started.get_future();
        auto shutdown = std::async(std::launch::async, [&]() {
            shutdown_started.set_value();
            HttpClient::shutdown_curl_pool();
        });
        started.wait();
        const bool waited = shutdown.wait_for(std::chrono::milliseconds(2500)) == std::future_status::timeout;
        CURL* late_handle = HttpClient::lease_handle_for_test();
        const bool rejected_while_draining = late_handle == nullptr;
        HttpClient::release_handle_for_test(late_handle);
        allow_response.set_value();
        const bool succeeded = pooled.get() == 200 && unpooled.get() == 200;
        shutdown.get();
        const bool drained = HttpClient::get_live_handle_count() == 0 && HttpClient::get_idle_handle_count() == 0;
        const bool rejected = HttpClient::lease_handle_for_test() == nullptr;
        async_stream_response_t response;
        std::map<std::string, std::string> headers;
        const bool stream_rejected = HttpClient::post_response_stream(server.base_url(), "{}", response,
                                                                     headers, {}, 1000) == 500;
        server.stop();
        curl_global_cleanup();
        _exit(waited && rejected_while_draining && succeeded && drained && rejected && stream_rejected ? 0 : 3);
    }()), ::testing::ExitedWithCode(0), "");
    ::testing::GTEST_FLAG(death_test_style) = previous_style;
}

static long do_get(const std::string& url, HttpClient::SSLVerifyMode ssl_verify_mode) {
    std::string response;
    std::map<std::string, std::string> res_headers;
    return HttpClient::get_response(url, response, res_headers, {}, 5000, false, ssl_verify_mode);
}

static void do_get_in_thread(std::string url, long* status_out) {
    *status_out = do_get(url, HttpClient::SSLVerifyMode::NO_VERIFY);
}

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

// shutdown drains on this counter, so an unbalanced decrement wraps the size_t and makes every
// later shutdown wait out the full drain before logging a phantom in flight transfer. the pooled
// path and the unpooled download and stream paths each have to leave the count where they found it
TEST(HttpClientPoolTest, EveryHandlePathBalancesTheLiveCount) {
    test_local_server_t server(false);
    ASSERT_TRUE(server.start());

    const size_t live_before = HttpClient::get_live_handle_count();

    std::string response;
    std::map<std::string, std::string> res_headers;
    ASSERT_EQ(200, HttpClient::get_response(server.base_url() + "/pooled", response, res_headers, {}, 5000));
    ASSERT_EQ(live_before, HttpClient::get_live_handle_count());

    // download_file inits outside the pool, the drain has to see it all the same
    const std::string download_path = "/tmp/http_client_pool_live_count.txt";
    ASSERT_EQ(200, HttpClient::download_file(server.base_url() + "/download", download_path));
    ASSERT_EQ(live_before, HttpClient::get_live_handle_count());

    // a refused connect still owes the count a decrement
    ASSERT_EQ(-1, HttpClient::download_file("http://127.0.0.1:1/", download_path));
    ASSERT_EQ(live_before, HttpClient::get_live_handle_count());
    remove(download_path.c_str());

    async_stream_response_t stream_res;
    std::map<std::string, std::string> stream_headers;
    ASSERT_EQ(200, HttpClient::post_response_stream(server.base_url() + "/stream", "{}", stream_res,
                                                    stream_headers, {}, 5000));
    ASSERT_EQ(live_before, HttpClient::get_live_handle_count());

    // three of the four calls reached the server, the refused connect never did
    const std::vector<recorded_request_t> requests = server.requests();
    ASSERT_EQ(3, requests.size());

    server.stop();
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

TEST(HttpClientPoolTest, ReusesConnectionAcrossThreads) {
    // HTTPS negotiates HTTP/1.1; plain HTTP uses HTTP/2 prior knowledge.
    test_local_server_t server(true);
    ASSERT_TRUE(server.start());
    const std::string url = server.base_url() + "/reuse";

    long status1 = 0;
    long status2 = 0;
    std::thread thread1(do_get_in_thread, url, &status1);
    thread1.join();
    std::thread thread2(do_get_in_thread, url, &status2);
    thread2.join();

    ASSERT_EQ(200, status1);
    ASSERT_EQ(200, status2);
    ASSERT_EQ(2, server.requests().size());
    // lifo leasing hands the second thread the handle whose connection is still open
    ASSERT_EQ(1, server.connection_count());
}

TEST(HttpClientPoolTest, PostThenGetLeavesNoResidue) {
    test_local_server_t server(true);
    ASSERT_TRUE(server.start());
    const std::string url = server.base_url() + "/residue";
    const std::string post_body = "{\"probe\": true}";

    std::string response;
    std::map<std::string, std::string> res_headers;
    long post_status = HttpClient::post_response(url, post_body, response, res_headers,
                                                 {{"x-probe-header", "abc"}}, 5000);
    long get_status = do_get(url, HttpClient::SSLVerifyMode::NO_VERIFY);

    ASSERT_EQ(200, post_status);
    ASSERT_EQ(200, get_status);

    auto requests = server.requests();
    ASSERT_EQ(2, requests.size());
    ASSERT_EQ("POST", requests[0].method);
    ASSERT_EQ(post_body, requests[0].body);
    ASSERT_EQ("abc", requests[0].headers["x-probe-header"]);

    // the reset between leases must clear the method, body and custom headers
    ASSERT_EQ("GET", requests[1].method);
    ASSERT_TRUE(requests[1].body.empty());
    ASSERT_EQ(0, requests[1].headers.count("x-probe-header"));
    ASSERT_EQ(0, requests[1].headers.count("content-length"));
    ASSERT_EQ(1, server.connection_count());
}

TEST(HttpClientPoolTest, VerifyModeDoesNotStickToPooledHandles) {
    test_local_server_t server(true);
    ASSERT_TRUE(server.start());
    const std::string url = server.base_url() + "/verify";

    long relaxed = do_get(url, HttpClient::SSLVerifyMode::NO_VERIFY);
    ASSERT_EQ(200, relaxed);

    // the self-signed cert must fail verification even though a pooled handle holds a live
    // connection established without it
    std::string response;
    std::map<std::string, std::string> res_headers;
    long verified = HttpClient::get_response_verified(url, response, res_headers, {}, 5000);
    ASSERT_EQ(500, verified);

    long relaxed_again = do_get(url, HttpClient::SSLVerifyMode::NO_VERIFY);
    ASSERT_EQ(200, relaxed_again);
}

TEST(HttpClientPoolTest, HttpsFallsBackToHttp11) {
    test_local_server_t server(true);
    ASSERT_TRUE(server.start());

    // the server never offers h2, the client must negotiate down instead of forcing h2
    long status = do_get(server.base_url() + "/fallback", HttpClient::SSLVerifyMode::NO_VERIFY);
    ASSERT_EQ(200, status);

    auto requests = server.requests();
    ASSERT_EQ(1, requests.size());
    ASSERT_EQ("HTTP/1.1", requests[0].version);
}
