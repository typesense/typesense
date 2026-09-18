#pragma once

#include <string>
#include <vector>
#include <map>
#include <tuple>
#include <unordered_map>
#include <option.h>
#include "json.hpp"

// knows nothing about NaturalLanguageSearchModel
class JevClient {
public:
    struct CapturedRequest {
        std::string url;
        std::string body;
        std::unordered_map<std::string, std::string> headers;
    };

private:
    // the search path fails fast by default, a hung connect must not read as a server hang
    static constexpr const size_t DEFAULT_TIMEOUT_MS = 5000;
    static constexpr const size_t VALIDATION_TIMEOUT_MS = 30000;

    static inline bool use_mock_response = false;
    static inline std::vector<std::tuple<std::string, long, std::map<std::string, std::string>>> mock_responses = {};
    static inline size_t mock_response_index = 0;

    static inline bool capture_request = false;
    static inline std::vector<CapturedRequest> captured_requests = {};

public:
    static constexpr const char* DEFAULT_API_URL = "https://api.typesafe.ai/v1/systemone";
    static constexpr const char* DEFAULT_MODELS_URL = "https://api.typesafe.ai/v1/models";

    static constexpr size_t MAX_CHOICE_OPTIONS = 255;

    // reserved none of the above key, jev needs an escape hatch or it is forced to pick
    static constexpr const char* NONE_OPTION = "__none__";

    static constexpr const size_t MIN_TIMEOUT_MS = 100;
    static constexpr const size_t MAX_TIMEOUT_MS = 60000;

    static bool is_jev_model(const nlohmann::json& model_config);

    // state. the timeout comes off the model config's `timeout_ms` knob
    static Option<nlohmann::json> ask(const nlohmann::json& state,
                                      const nlohmann::json& questions,
                                      const nlohmann::json& model_config);

    static Option<bool> verify_api_key(const nlohmann::json& model_config);

    static nlohmann::json noul_question(const std::string& instructions);
    static nlohmann::json noul_question(const std::string& instructions,
                                        const std::string& true_desc,
                                        const std::string& false_desc);
    static nlohmann::json choice_question(const std::string& instructions,
                                          const std::vector<std::pair<std::string, std::string>>& options);

    // a missing or malformed answer is an error, never a throw
    static Option<double> get_noul(const nlohmann::json& answers, const std::string& id);
    static Option<std::string> get_choice(const nlohmann::json& answers, const std::string& id);
    static Option<double> get_confidence(const nlohmann::json& answers, const std::string& id);

    static long post_response(const std::string& url, const std::string& body, std::string& response,
                              std::map<std::string, std::string>& res_headers,
                              const std::unordered_map<std::string, std::string>& headers = {},
                              long timeout_ms = DEFAULT_TIMEOUT_MS);

    // the models endpoint is a get, posting to it returns 405
    static long get_response(const std::string& url, std::string& response,
                             std::map<std::string, std::string>& res_headers,
                             const std::unordered_map<std::string, std::string>& headers = {},
                             long timeout_ms = DEFAULT_TIMEOUT_MS);

    static void add_mock_response(const std::string& response_body, long status_code = 200,
                                  const std::map<std::string, std::string>& response_headers = {});
    static void clear_mock_responses();

    static void enable_request_capture() { capture_request = true; }
    static void disable_request_capture() {
        capture_request = false;
        captured_requests.clear();
    }

    static const std::vector<CapturedRequest>& get_captured_requests() { return captured_requests; }
    static size_t get_num_captured_requests() { return captured_requests.size(); }

    static std::string get_last_request_url() {
        return captured_requests.empty() ? "" : captured_requests.back().url;
    }
    static std::string get_last_request_body() {
        return captured_requests.empty() ? "" : captured_requests.back().body;
    }

};
