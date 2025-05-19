#pragma once

#include <string>
#include <option.h>
#include "json.hpp"

class NaturalLanguageSearchModel {
private:
    static inline bool use_mock_response = false;
    static inline std::string mock_response_body = "";
    static inline long mock_status_code = 200;
    static inline std::map<std::string, std::string> mock_response_headers = {};
    static constexpr const size_t DEFAULT_TIMEOUT_MS = 200000;

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

    static long post_response(const std::string& url, const std::string& body, std::string& response,
                                    std::map<std::string, std::string>& res_headers,
                                    const std::unordered_map<std::string, std::string>& headers = {},
                                    long timeout_ms = DEFAULT_TIMEOUT_MS,
                                    bool send_ts_api_header = false);

    static void set_mock_response(const std::string& response_body, long status_code = 200, const std::map<std::string, std::string>& response_headers = {});
    static void clear_mock_response();
}; 