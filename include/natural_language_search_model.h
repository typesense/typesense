#pragma once

#include <string>
#include <option.h>
#include "json.hpp"

class NaturalLanguageSearchModel {
public:
    static Option<bool> validate_model(const nlohmann::json& model_config);

    // Generate search parameters from a natural language query
    static Option<nlohmann::json> generate_search_params(const std::string& query, 
                                                        const std::string& collection_schema_prompt,
                                                        const nlohmann::json& model_config);

    // OpenAI implementation
    static Option<bool> validate_openai_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> openai_generate_search_params(const std::string& query, 
                                                               const std::string& collection_schema_prompt,
                                                               const nlohmann::json& model_config);

    // Cloudflare implementation
    static Option<bool> validate_cloudflare_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> cloudflare_generate_search_params(const std::string& query, 
                                                                  const std::string& collection_schema_prompt,
                                                                  const nlohmann::json& model_config);

    // vLLM implementation
    static Option<bool> validate_vllm_model(const nlohmann::json& model_config);
    static Option<nlohmann::json> vllm_generate_search_params(const std::string& query, 
                                                            const std::string& collection_schema_prompt,
                                                            const nlohmann::json& model_config);
}; 