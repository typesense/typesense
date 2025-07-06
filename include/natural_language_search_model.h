#pragma once

#include <string>
#include <option.h>
#include "json.hpp"

class NaturalLanguageSearchModel {
private:
    static inline bool use_mock_response = false;
    static constexpr const size_t DEFAULT_TIMEOUT_MS = 200000;
    static constexpr const size_t VALIDATION_TIMEOUT_MS = 30000;
    
    // Request capture for testing
    static inline bool capture_request = false;
    struct CapturedRequest {
        std::string url;
        std::string body;
        std::unordered_map<std::string, std::string> headers;
    };
    static inline std::vector<CapturedRequest> captured_requests = {};
    
    // Multiple mock responses for testing
    static inline std::vector<std::tuple<std::string, long, std::map<std::string, std::string>>> mock_responses = {};
    static inline size_t mock_response_index = 0;

public:
    static Option<nlohmann::json> extract_search_params_from_content(const std::string& content, const std::string& model_name_without_namespace);
    static Option<bool> validate_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> generate_search_params(const std::string& query, 
                                                        const std::string& collection_schema_prompt,
                                                        const nlohmann::json& model_config);

    static Option<bool> validate_openai_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> openai_vllm_generate_search_params(const std::string& query, 
                                                               const std::string& collection_schema_prompt,
                                                               const nlohmann::json& model_config);

    static Option<bool> validate_cloudflare_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> cloudflare_generate_search_params(const std::string& query, 
                                                                  const std::string& collection_schema_prompt,
                                                                  const nlohmann::json& model_config);

    static Option<bool> validate_vllm_model(const nlohmann::json& model_config);

    static Option<bool> validate_google_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> google_generate_search_params(const std::string& query, 
                                                              const std::string& collection_schema_prompt,
                                                              const nlohmann::json& model_config);

    static Option<bool> validate_gcp_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> gcp_generate_search_params(const std::string& query, 
                                                           const std::string& collection_schema_prompt,
                                                           const nlohmann::json& model_config);
    static Option<std::string> generate_gcp_access_token(const std::string& refresh_token, 
                                                        const std::string& client_id, 
                                                        const std::string& client_secret);

    static long post_response(const std::string& url, const std::string& body, std::string& response,
                                    std::map<std::string, std::string>& res_headers,
                                    const std::unordered_map<std::string, std::string>& headers = {},
                                    long timeout_ms = DEFAULT_TIMEOUT_MS,
                                    bool send_ts_api_header = false);

private:
    // Helper methods for making API calls to each provider
    static Option<nlohmann::json> call_openai_api(const nlohmann::json& request_body,
                                                  const nlohmann::json& model_config,
                                                  long timeout_ms = DEFAULT_TIMEOUT_MS);
    
    static Option<nlohmann::json> call_cloudflare_api(const nlohmann::json& request_body,
                                                      const nlohmann::json& model_config,
                                                      long timeout_ms = DEFAULT_TIMEOUT_MS);
    
    static Option<nlohmann::json> call_google_api(const nlohmann::json& request_body,
                                                  const nlohmann::json& model_config,
                                                  long timeout_ms = DEFAULT_TIMEOUT_MS);
    
    static Option<nlohmann::json> call_gcp_api(const nlohmann::json& request_body,
                                               const nlohmann::json& model_config,
                                               long timeout_ms = DEFAULT_TIMEOUT_MS);

public:

    // Mock responses support
    static void add_mock_response(const std::string& response_body, long status_code = 200, const std::map<std::string, std::string>& response_headers = {});
    static void clear_mock_responses();
    
    // Request capture methods for testing
    static void enable_request_capture() { capture_request = true; }
    static void disable_request_capture() { 
        capture_request = false; 
        captured_requests.clear();
    }
    
    // Access to captured requests
    static const std::vector<CapturedRequest>& get_captured_requests() { return captured_requests; }
    static size_t get_num_captured_requests() { return captured_requests.size(); }
    static const CapturedRequest& get_captured_request(size_t index) { 
        if (index >= captured_requests.size()) {
            static CapturedRequest empty;
            return empty;
        }
        return captured_requests[index]; 
    }
    
    // Convenience methods for accessing the last request
    static std::string get_last_request_url() { 
        return captured_requests.empty() ? "" : captured_requests.back().url; 
    }
    static std::string get_last_request_body() { 
        return captured_requests.empty() ? "" : captured_requests.back().body; 
    }
    static std::unordered_map<std::string, std::string> get_last_request_headers() { 
        return captured_requests.empty() ? std::unordered_map<std::string, std::string>{} : captured_requests.back().headers; 
    }
}; 