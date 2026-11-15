// llm-retry (vendored single header): https://github.com/Mattbusel/llm-retry
//
// MIT License
//
// Copyright (c) 2026 Mattbusel
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once

// llm-retry: single-header C++ retry + circuit breaker for LLM API calls
// Usage: #define LLM_RETRY_IMPLEMENTATION in ONE .cpp file before including.

#include <chrono>
#include <ctime>
#include <functional>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace llm {

// ─── Config ──────────────────────────────────────────────────────────────────

struct RetryConfig {
    int max_attempts          = 3;
    double base_delay_ms      = 500.0;
    double max_delay_ms       = 30000.0;
    double backoff_multiplier = 2.0;
    double jitter_factor      = 0.1;
    // 408 request timeout and 529 overloaded (Anthropic's overloaded_error)
    // are transient, so they are retried along with 429 and the 5xx codes.
    std::vector<int> retry_on_http_codes = {408, 429, 500, 502, 503, 504, 529};
};

struct CircuitBreakerConfig {
    int    failure_threshold  = 5;
    double success_threshold  = 0.8;   // unused since 0.2: one half-open trial decides
    double timeout_seconds    = 60.0;
};

// ─── Types ───────────────────────────────────────────────────────────────────

enum class CircuitState { Closed, Open, HalfOpen };

template<typename T>
struct RetryResult {
    T      value;
    int    attempts_used;
    double total_elapsed_ms;
    bool   from_fallback;
};

struct LLMError {
    int         http_code;     // 0 if not HTTP error
    std::string message;
    bool        is_retryable;
    // Seconds from the server's Retry-After header, or -1 if there was none.
    // When set, with_retry waits this long (plus a random spread) instead of
    // using its own backoff formula.
    double      retry_after_seconds = -1.0;
};

// ─── Helpers (declarations) ──────────────────────────────────────────────────

int parse_http_code(const std::string& response_header);

namespace detail {
    bool is_retryable_code(int code, const std::vector<int>& codes);
    double compute_delay_ms(int attempt, const RetryConfig& cfg);
    // A 4xx the caller caused (bad request, auth, too long): not 408 or 429.
    inline bool is_client_error(int code) {
        return code >= 400 && code < 500 && code != 408 && code != 429;
    }
} // namespace detail

// ─── with_retry ──────────────────────────────────────────────────────────────

template<typename T>
RetryResult<T> with_retry(
    std::function<T()> fn,
    const RetryConfig& config = {}
) {
    using clock = std::chrono::steady_clock;
    auto start = clock::now();

    std::mt19937 rng(std::random_device{}());

    for (int attempt = 1; attempt <= config.max_attempts; ++attempt) {
        try {
            T result = fn();
            double elapsed = std::chrono::duration<double, std::milli>(
                clock::now() - start).count();
            return RetryResult<T>{std::move(result), attempt, elapsed, false};
        } catch (const LLMError& err) {
            bool last = (attempt == config.max_attempts);
            bool retryable = err.is_retryable &&
                             detail::is_retryable_code(err.http_code, config.retry_on_http_codes);
            if (last || !retryable) throw;

            double delay = detail::compute_delay_ms(attempt, config);
            if (err.retry_after_seconds >= 0.0) {
                // The server said when to come back: obey it, plus a random
                // spread so every client does not return on the same second.
                std::uniform_real_distribution<double> spread(0.0, delay);
                delay = err.retry_after_seconds * 1000.0 + spread(rng);
            } else {
                std::uniform_real_distribution<double> jitter_dist(
                    1.0 - config.jitter_factor, 1.0 + config.jitter_factor);
                delay *= jitter_dist(rng);
            }
            std::this_thread::sleep_for(
                std::chrono::duration<double, std::milli>(delay));
        }
        // Any other exception came from the caller's own code (a JSON parse,
        // a logic error). Retrying a bug only delays it, so it propagates
        // immediately. Report transient network failures as LLMError{0, ...}.
    }
    throw LLMError{0, "with_retry: exhausted all attempts", false};
}

// ─── with_failover ───────────────────────────────────────────────────────────

template<typename T>
RetryResult<T> with_failover(
    std::function<T()> primary,
    std::function<T()> fallback,
    const RetryConfig& config = {}
) {
    try {
        return with_retry<T>(primary, config);
    } catch (const LLMError& err) {
        // A 4xx other than 408/429 means the request itself is broken; the
        // fallback model would hide the bug behind a quality drop. Surface it.
        if (detail::is_client_error(err.http_code)) throw;
        using clock = std::chrono::steady_clock;
        auto start = clock::now();
        T result = fallback();
        double elapsed = std::chrono::duration<double, std::milli>(
            clock::now() - start).count();
        return RetryResult<T>{std::move(result), 1, elapsed, true};
    } catch (...) {
        // Not an HTTP error (e.g. CircuitBreaker open): fail over.
        using clock = std::chrono::steady_clock;
        auto start = clock::now();
        T result = fallback();
        double elapsed = std::chrono::duration<double, std::milli>(
            clock::now() - start).count();
        return RetryResult<T>{std::move(result), 1, elapsed, true};
    }
}

// ─── CircuitBreaker ──────────────────────────────────────────────────────────

class CircuitBreaker {
public:
    struct Stats {
        CircuitState state;
        int          consecutive_failures;
        double       failure_rate;
        std::time_t  last_failure;
        std::time_t  last_success;
    };

    explicit CircuitBreaker(std::string name, CircuitBreakerConfig config = {})
        : name_(std::move(name)), config_(config) {}

    // After the cool-down the circuit goes HalfOpen and admits exactly ONE
    // trial call; concurrent callers fail fast until it finishes. Its success
    // closes the circuit and clears the counters, its failure reopens it.
    template<typename T>
    T call(std::function<T()> fn) {
        bool trial = false;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            if (state_ == CircuitState::Open) {
                auto now = std::chrono::steady_clock::now();
                double secs = std::chrono::duration<double>(
                    now - last_failure_time_).count();
                if (secs >= config_.timeout_seconds) {
                    state_ = CircuitState::HalfOpen;
                } else {
                    throw std::runtime_error(
                        "CircuitBreaker[" + name_ + "]: circuit is Open");
                }
            }
            if (state_ == CircuitState::HalfOpen) {
                if (trial_in_flight_) {
                    throw std::runtime_error(
                        "CircuitBreaker[" + name_ + "]: circuit is HalfOpen, trial in flight");
                }
                trial_in_flight_ = trial = true;
            }
        }

        try {
            T result = fn();
            record_success(trial);
            return result;
        } catch (...) {
            record_failure(trial);
            throw;
        }
    }

    CircuitState state() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return state_;
    }

    Stats stats() const {
        std::lock_guard<std::mutex> lk(mutex_);
        double rate = (total_calls_ == 0) ? 0.0 :
            static_cast<double>(failure_calls_) / total_calls_;
        return Stats{
            state_,
            consecutive_failures_,
            rate,
            last_failure_ts_,
            last_success_ts_
        };
    }

    void reset() {
        std::lock_guard<std::mutex> lk(mutex_);
        state_ = CircuitState::Closed;
        consecutive_failures_ = 0;
        total_calls_ = 0;
        failure_calls_ = 0;
        trial_in_flight_ = false;
    }

private:
    void record_success(bool trial) {
        std::lock_guard<std::mutex> lk(mutex_);
        ++total_calls_;
        consecutive_failures_ = 0;
        last_success_ts_ = std::time(nullptr);
        if (trial) {
            // The half-open trial worked: the provider is back.
            state_ = CircuitState::Closed;
            trial_in_flight_ = false;
            failure_calls_ = 0;
            total_calls_ = 0;
        }
    }

    void record_failure(bool trial) {
        std::lock_guard<std::mutex> lk(mutex_);
        ++total_calls_;
        ++failure_calls_;
        ++consecutive_failures_;
        last_failure_ts_  = std::time(nullptr);
        last_failure_time_ = std::chrono::steady_clock::now();
        if (trial || consecutive_failures_ >= config_.failure_threshold) {
            state_ = CircuitState::Open;
            trial_in_flight_ = false;
        }
    }

    std::string           name_;
    CircuitBreakerConfig  config_;
    mutable std::mutex    mutex_;
    CircuitState          state_             = CircuitState::Closed;
    int                   consecutive_failures_ = 0;
    int                   total_calls_       = 0;
    int                   failure_calls_     = 0;
    bool                  trial_in_flight_   = false;
    std::time_t           last_failure_ts_   = 0;
    std::time_t           last_success_ts_   = 0;
    std::chrono::steady_clock::time_point last_failure_time_;
};

} // namespace llm

// ─── Implementation ──────────────────────────────────────────────────────────

#ifdef LLM_RETRY_IMPLEMENTATION

namespace llm {

int parse_http_code(const std::string& header) {
    // Expects "HTTP/1.1 429 Too Many Requests" style
    auto pos = header.find("HTTP/");
    if (pos == std::string::npos) return 0;
    auto space = header.find(' ', pos);
    if (space == std::string::npos) return 0;
    try { return std::stoi(header.substr(space + 1, 3)); }
    catch (...) { return 0; }
}

namespace detail {

bool is_retryable_code(int code, const std::vector<int>& codes) {
    if (code == 0) return true; // network error, always retry
    for (int c : codes) if (c == code) return true;
    return false;
}

double compute_delay_ms(int attempt, const RetryConfig& cfg) {
    double delay = cfg.base_delay_ms;
    for (int i = 1; i < attempt; ++i) delay *= cfg.backoff_multiplier;
    if (delay > cfg.max_delay_ms) delay = cfg.max_delay_ms;
    return delay;
}

} // namespace detail
} // namespace llm

#endif // LLM_RETRY_IMPLEMENTATION
