#include <regex>
#include <iterator>
#include "natural_language_search_model.h"
#include "text_embedder_remote.h"
#include "string_utils.h"
#include "logger.h"

bool NaturalLanguageSearchModel::use_mock_response = false;
std::string NaturalLanguageSearchModel::mock_response_body;
long NaturalLanguageSearchModel::mock_status_code = 200;
std::map<std::string, std::string> NaturalLanguageSearchModel::mock_response_headers;

Option<nlohmann::json> NaturalLanguageSearchModel::extract_search_params_from_content(
    const std::string& content,
    const std::string& model_name_without_namespace) {
    try {
        nlohmann::json search_params = nlohmann::json::parse(content);
        search_params["llm_response"] = {{"content", content}, {"model", model_name_without_namespace}};
        return Option<nlohmann::json>(search_params);
    } catch(...) {
        std::regex json_regex("\\{[\\s\\S]*\\}");
        std::smatch m;
        if(std::regex_search(content, m, json_regex)) {
            try {
                nlohmann::json search_params = nlohmann::json::parse(m[0].str());
                search_params["llm_response"] = {
                    {"content", content},
                    {"model", model_name_without_namespace},
                    {"extraction_method", "regex"}
                };
                return Option<nlohmann::json>(search_params);
            } catch(const std::exception& e2) {
                return Option<nlohmann::json>(500, "Regex JSON parse failed on content");
            }
        }
        return Option<nlohmann::json>(500, "Could not extract search parameters");
    }
}

static const std::string get_model_namespace(const std::string& model_name) {
    if(model_name.find("/") != std::string::npos) {
        return model_name.substr(0, model_name.find("/"));
    } else {
        return "";
    }
}

Option<bool> NaturalLanguageSearchModel::validate_model(const nlohmann::json& model_config) {
    if(model_config.count("model_name") == 0 || !model_config["model_name"].is_string()) {
        return Option<bool>(400, "Property `model_name` is not provided or not a string.");
    }

    if(model_config.count("system_prompt") != 0 && !model_config["system_prompt"].is_string()) {
        return Option<bool>(400, "Property `system_prompt` is not a string.");
    }

    if(model_config.count("max_bytes") == 0 || !model_config["max_bytes"].is_number_unsigned() || 
       model_config["max_bytes"].get<size_t>() == 0) {
        return Option<bool>(400, "Property `max_bytes` is not provided or not a positive integer.");
    }

    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    if(model_namespace == "openai") {
        return validate_openai_model(model_config);
    } else if(model_namespace == "cloudflare") {
        return validate_cloudflare_model(model_config);
    } else if(model_namespace == "vllm") {
        return validate_vllm_model(model_config);
    }

    return Option<bool>(400, "Model namespace `" + model_namespace + "` is not supported.");
}

Option<nlohmann::json> NaturalLanguageSearchModel::generate_search_params(
    const std::string& query, 
    const std::string& collection_schema_prompt,
    const nlohmann::json& model_config) {
    
    const std::string& model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    
    std::string system_prompt = "";
    
    if(model_config.count("system_prompt") != 0 && model_config["system_prompt"].is_string()) {
        system_prompt = model_config["system_prompt"].get<std::string>();
    }
    
    std::string full_system_prompt = system_prompt;
    if (!system_prompt.empty()) {
        full_system_prompt += "\n\n";
    }
    full_system_prompt += collection_schema_prompt;

    if(model_namespace == "openai") {
        return openai_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "cloudflare") {
        return cloudflare_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "vllm") {
        return vllm_generate_search_params(query, full_system_prompt, model_config);
    }
    return Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<bool> NaturalLanguageSearchModel::validate_openai_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0 || !model_config["api_key"].is_string() || 
       model_config["api_key"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `api_key` is missing or is not a non-empty string.");
    }

    if(model_config.count("temperature") != 0 && 
       (!model_config["temperature"].is_number() || 
        model_config["temperature"].get<float>() < 0 || 
        model_config["temperature"].get<float>() > 2)) {
        return Option<bool>(400, "Property `temperature` must be a number between 0 and 2.");
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::openai_generate_search_params(
    const std::string& query,
    const std::string& system_prompt,
    const nlohmann::json& model_config) {

    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    const std::string& api_key = model_config["api_key"].get<std::string>();
    float temperature = model_config.value("temperature", 0.0f);
    size_t max_bytes = model_config["max_bytes"].get<size_t>();
    std::string api_url = model_config.value("api_url", std::string("https://api.openai.com/v1/chat/completions"));

    nlohmann::json request_body;
    request_body["model"] = model_name_without_namespace;
    request_body["temperature"] = temperature;
    request_body["max_tokens"] = max_bytes;
    request_body["messages"] = {
        {{"role", "system"}, {"content", system_prompt}},
        {{"role", "user"}, {"content", query}}
    };

    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer " + api_key}
    };

    std::string response;
    std::map<std::string, std::string> response_headers;
    long timeout_ms = 10000;
    nlohmann::json fallback{{"q", query}};

    long status_code = post_response(api_url, request_body.dump(), response, response_headers, headers, timeout_ms);

    if(status_code != 200) {
        return Option<nlohmann::json>(500, "Failed to get response from OpenAI: " + response);
    }

    nlohmann::json response_json;
    try {
        response_json = nlohmann::json::parse(response);
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Failed to parse OpenAI response: Invalid JSON");
    }

    auto& choices = response_json["choices"];
    if(!choices.is_array() || choices.empty()) {
        return Option<nlohmann::json>(500, "No valid response from OpenAI");
    }

    auto& choice = choices[0];
    if(!choice.contains("message") || !choice["message"].contains("content") || !choice["message"]["content"].is_string()) {
        return Option<nlohmann::json>(500, "No valid response content from OpenAI");
    }

    std::string content = choice["message"]["content"].get<std::string>();
    return extract_search_params_from_content(content, model_name_without_namespace);
}

Option<bool> NaturalLanguageSearchModel::validate_cloudflare_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0 || !model_config["api_key"].is_string() || 
       model_config["api_key"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `api_key` is missing or is not a non-empty string.");
    }

    if(model_config.count("account_id") == 0 || !model_config["account_id"].is_string() || 
       model_config["account_id"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `account_id` is missing or is not a non-empty string.");
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::cloudflare_generate_search_params(
    const std::string& query, 
    const std::string& system_prompt,
    const nlohmann::json& model_config) {
    
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find("/") + 1);
    const std::string& api_key = model_config["api_key"].get<std::string>();
    const std::string& account_id = model_config["account_id"].get<std::string>();
    
    size_t max_bytes = model_config["max_bytes"].get<size_t>();
    std::string api_url = "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/ai/run/" + model_name_without_namespace;

    nlohmann::json messages = nlohmann::json::array({
        {{"role", "system"}, {"content", system_prompt}},
        {{"role", "user"}, {"content", query}}
    });

    nlohmann::json request_body = {
        {"messages", messages},
        {"max_tokens", max_bytes}
    };

    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer " + api_key}
    };

    std::string response;
    std::map<std::string, std::string> response_headers;
    long status = post_response(api_url, request_body.dump(), response, response_headers, headers, max_bytes);

    if(status != 200) {
        return Option<nlohmann::json>(status, "Cloudflare API error: HTTP " + std::to_string(status) + ", response: " + response);
    }

    nlohmann::json response_json;
    try {
        response_json = nlohmann::json::parse(response);
    } catch(const std::exception& e) {
        LOG(ERROR) << "Failed to parse Cloudflare API JSON response: " << e.what()
                   << ", raw response: " << response;
        return Option<nlohmann::json>(500, "Cloudflare API response JSON parse error: " + std::string(e.what()) + ", response: " + response);
    }

    if(!response_json.contains("result") || !response_json["result"].is_object() ||
       !response_json["result"].contains("response") || !response_json["result"]["response"].is_string()) {
        LOG(ERROR) << "Invalid Cloudflare API response format. JSON: " << response_json.dump();
        return Option<nlohmann::json>(500, "Invalid format from Cloudflare API. JSON: " + response_json.dump());
    }

    std::string content = response_json["result"]["response"].get<std::string>();
    return extract_search_params_from_content(content, model_name_without_namespace);

}

Option<bool> NaturalLanguageSearchModel::validate_vllm_model(const nlohmann::json& model_config) {
    if(model_config.count("api_url") == 0 || !model_config["api_url"].is_string() || 
       model_config["api_url"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `api_url` is missing or is not a non-empty string.");
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::vllm_generate_search_params(
    const std::string& query, 
    const std::string& system_prompt,
    const nlohmann::json& model_config) {
    
    const std::string& api_url = model_config["api_url"].get<std::string>();
    size_t max_bytes = model_config["max_bytes"].get<size_t>();
    float temperature = 0.0;
    
    if(model_config.count("temperature") != 0 && model_config["temperature"].is_number()) {
        temperature = model_config["temperature"].get<float>();
    }

    // Create the prompt by combining system prompt and user query
    std::string full_prompt = "[SYSTEM]\n" + system_prompt + "\n\n[USER]\n" + query + "\n\n[ASSISTANT]\n";

    nlohmann::json request_body;
    request_body["prompt"] = full_prompt;
    request_body["temperature"] = temperature;
    request_body["max_tokens"] = max_bytes;

    // Set up headers for vLLM request
    std::unordered_map<std::string, std::string> headers;
    headers["Content-Type"] = "application/json";

    std::string response;
    std::map<std::string, std::string> response_headers;
    
    // Make HTTP request to vLLM
    long status_code = post_response(api_url, request_body.dump(), response,
                                           response_headers, headers, max_bytes);

    if(status_code != 200) {
        return Option<nlohmann::json>(status_code, "vLLM API request failed: " + response);
    }

    // Parse vLLM response
    nlohmann::json response_json;
    try {
        response_json = nlohmann::json::parse(response);
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Failed to parse vLLM response: " + std::string(e.what()));
    }

    if(response_json.count("text") == 0 || !response_json["text"].is_string()) {
        return Option<nlohmann::json>(500, "Invalid response from vLLM API");
    }

    std::string content = response_json["text"].get<std::string>();
    
    // Parse LLM response into search parameters
    return extract_search_params_from_content(content, "vllm");
}

long NaturalLanguageSearchModel::post_response(const std::string& url, const std::string& body, std::string& response,
                                    std::map<std::string, std::string>& res_headers,
                                    const std::unordered_map<std::string, std::string>& headers,
                                    long timeout_ms,
                                    bool send_ts_api_header) {
    if (use_mock_response) {
        response = mock_response_body;
        res_headers = mock_response_headers;
        return mock_status_code;
    }
    return HttpClient::post_response(url, body, response, res_headers, headers, timeout_ms, send_ts_api_header);
}

// Mock response functions
void NaturalLanguageSearchModel::set_mock_response(const std::string& response_body, long status_code, const std::map<std::string, std::string>& response_headers) {
    use_mock_response = true;
    mock_response_body = response_body;
    mock_status_code = status_code;
    mock_response_headers = response_headers;
}

void NaturalLanguageSearchModel::clear_mock_response() {
    use_mock_response = false;
    mock_response_body.clear();
    mock_status_code = 200;
    mock_response_headers.clear();
}
