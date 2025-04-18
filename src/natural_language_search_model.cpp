#include <regex>
#include <iterator>
#include "natural_language_search_model.h"
#include "text_embedder_remote.h"
#include "string_utils.h"
#include "logger.h"

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
    
    LOG(INFO) << "Beginning generate_search_params for query: " << query;
    
    const std::string& model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    LOG(INFO) << "Model namespace: " << model_namespace;
    
    std::string system_prompt = "";
    
    if(model_config.count("system_prompt") != 0 && model_config["system_prompt"].is_string()) {
        system_prompt = model_config["system_prompt"].get<std::string>();
        LOG(INFO) << "Using custom system prompt from model config";
    } else {
        LOG(INFO) << "No custom system prompt in model config";
    }
    
    // Add the collection schema to the system prompt
    std::string full_system_prompt = system_prompt;
    if (!system_prompt.empty()) {
        full_system_prompt += "\n\n";
    }
    full_system_prompt += collection_schema_prompt;
    LOG(INFO) << "Prepared full system prompt with collection schema";

    if(model_namespace == "openai") {
        LOG(INFO) << "Calling openai_generate_search_params";
        return openai_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "cloudflare") {
        LOG(INFO) << "Calling cloudflare_generate_search_params";
        return cloudflare_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "vllm") {
        LOG(INFO) << "Calling vllm_generate_search_params";
        return vllm_generate_search_params(query, full_system_prompt, model_config);
    }

    LOG(INFO) << "Unsupported model namespace: " << model_namespace;
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
    
    LOG(INFO) << "Starting openai_generate_search_params for query: " << query;
    
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find("/") + 1);
    const std::string& api_key = model_config["api_key"].get<std::string>();
    float temperature = 0.0;
    
    LOG(INFO) << "Using model: " << model_name_without_namespace;
    
    if(model_config.count("temperature") != 0 && model_config["temperature"].is_number()) {
        temperature = model_config["temperature"].get<float>();
        LOG(INFO) << "Using temperature: " << temperature;
    }

    size_t max_bytes = model_config["max_bytes"].get<size_t>();
    std::string api_url = "https://api.openai.com/v1/chat/completions";

    if(model_config.count("api_url") != 0 && model_config["api_url"].is_string()) {
        api_url = model_config["api_url"].get<std::string>();
    }
    
    LOG(INFO) << "Using API URL: " << api_url;

    nlohmann::json request_body;
    request_body["model"] = model_name_without_namespace;
    request_body["temperature"] = temperature;

    nlohmann::json messages = nlohmann::json::array();
    
    // System message
    nlohmann::json system_message;
    system_message["role"] = "system";
    system_message["content"] = system_prompt;
    messages.push_back(system_message);

    // User message
    nlohmann::json user_message;
    user_message["role"] = "user";
    user_message["content"] = query;
    messages.push_back(user_message);

    request_body["messages"] = messages;
    request_body["max_tokens"] = max_bytes;
    
    LOG(INFO) << "Prepared request for OpenAI API";
    LOG(INFO) << "Request body: " << request_body.dump().substr(0, 500) << "..."; // Log first 500 chars of request

    // Set up headers for OpenAI request
    std::unordered_map<std::string, std::string> headers;
    headers["Content-Type"] = "application/json";
    headers["Authorization"] = "Bearer " + api_key;
    
    LOG(INFO) << "Set up headers for OpenAI API request";
    LOG(INFO) << "About to make HTTP request to: " << api_url;
    LOG(INFO) << "Max tokens set to: " << max_bytes;

    std::string response;
    std::map<std::string, std::string> response_headers;
    
    LOG(INFO) << "Making HTTP request to OpenAI API - STARTING";
    
    // Set a shorter timeout for debugging purposes - 10 seconds
    long timeout_ms = 10000; 
    
    // Generate a fallback search parameter if the API call fails
    nlohmann::json fallback_search;
    fallback_search["q"] = query; // Use the original query as a simple fallback
    
    try {
        LOG(INFO) << "Entering HttpClient::post_response with timeout: " << timeout_ms << "ms";
        long status_code = HttpClient::post_response(api_url, request_body.dump(), response,
                                               response_headers, headers, timeout_ms);
        LOG(INFO) << "Completed HttpClient::post_response with status: " << status_code;
        
        if(status_code != 200) {
            LOG(ERROR) << "OpenAI API request failed with status code " << status_code << ": " << response;
            LOG(WARNING) << "Using fallback search parameters due to API failure";
            
            // Try to parse the error response as JSON
            nlohmann::json error_message;
            try {
                // If the response is valid JSON, use it directly
                error_message = nlohmann::json::parse(response);
                fallback_search["llm_response"] = {
                    {"message", error_message},
                    {"status_code", status_code}
                };
                LOG(INFO) << "Successfully parsed OpenAI error response as JSON";
            } catch(const std::exception& e) {
                // If not valid JSON, use it as a string
                fallback_search["llm_response"] = {
                    {"message", response},
                    {"status_code", status_code}
                };
                LOG(INFO) << "OpenAI error response is not valid JSON, keeping as string";
            }
            
            return Option<nlohmann::json>(fallback_search);
        }

        // Parse OpenAI response
        LOG(INFO) << "Parsing OpenAI response";
        nlohmann::json response_json;
        try {
            response_json = nlohmann::json::parse(response);
            LOG(INFO) << "Successfully parsed response: " << response_json.dump().substr(0, 500) << "...";
        } catch(const std::exception& e) {
            LOG(ERROR) << "Failed to parse OpenAI response: " << e.what();
            LOG(WARNING) << "Using fallback search parameters due to JSON parse failure";
            
            // Add error information to the fallback search parameters
            fallback_search["llm_response"] = {
                {"message", "Failed to parse OpenAI response: " + std::string(e.what())},
                {"status_code", 500}
            };
            
            return Option<nlohmann::json>(fallback_search);
        }

        if(response_json.count("choices") == 0 || !response_json["choices"].is_array() || 
           response_json["choices"].empty()) {
            LOG(ERROR) << "Invalid response from OpenAI API: no choices";
            LOG(WARNING) << "Using fallback search parameters due to invalid API response";
            
            // Add error information to the fallback search parameters
            fallback_search["llm_response"] = {
                {"message", "OpenAI API did not return a valid response"},
                {"status_code", 500}
            };
            
            return Option<nlohmann::json>(fallback_search);
        }

        nlohmann::json choice = response_json["choices"][0];
        
        if(choice.count("message") == 0 || !choice["message"].is_object() || 
           choice["message"].count("content") == 0 || !choice["message"]["content"].is_string()) {
            LOG(ERROR) << "Invalid response from OpenAI API: missing message content";
            LOG(WARNING) << "Using fallback search parameters due to invalid message content";
            
            // Add error information to the fallback search parameters
            fallback_search["llm_response"] = {
                {"message", "OpenAI API did not return a valid response"},
                {"status_code", 500}
            };
            
            return Option<nlohmann::json>(fallback_search);
        }

        std::string content = choice["message"]["content"].get<std::string>();
        LOG(INFO) << "Received content from OpenAI: " << content.substr(0, 500) << "...";
        
        // Parse LLM response into search parameters
        LOG(INFO) << "Attempting to parse search parameters from LLM response";
        try {
            nlohmann::json search_params = nlohmann::json::parse(content);
            LOG(INFO) << "Successfully parsed search parameters: " << search_params.dump();
            
            // Add raw LLM response for debugging
            search_params["llm_response"] = {
                {"content", content},
                {"model", model_name_without_namespace}
            };
            
            return Option<nlohmann::json>(search_params);
        } catch(const std::exception& e) {
            LOG(WARNING) << "Failed direct JSON parsing: " << e.what() << ", trying regex extraction";
            // Try to extract JSON from the response if it's wrapped in text
            std::regex json_regex("\\{[\\s\\S]*\\}");
            std::smatch matches;
            if(std::regex_search(content, matches, json_regex)) {
                try {
                    nlohmann::json search_params = nlohmann::json::parse(matches[0].str());
                    LOG(INFO) << "Successfully parsed search parameters after regex extraction: " << search_params.dump();
                    
                    // Add raw LLM response for debugging
                    search_params["llm_response"] = {
                        {"content", content},
                        {"model", model_name_without_namespace},
                        {"extraction_method", "regex"}
                    };
                    
                    return Option<nlohmann::json>(search_params);
                } catch(const std::exception& nested_e) {
                    LOG(ERROR) << "Failed to parse JSON after regex extraction: " << nested_e.what();
                    LOG(WARNING) << "Using fallback search parameters due to JSON regex extraction failure";
                    
                    // Add error information to the fallback search parameters
                    fallback_search["llm_response"] = {
                        {"message", "Failed to parse regex-extracted JSON from OpenAI API response: " + std::string(nested_e.what())},
                        {"content", content},
                        {"status_code", 500}
                    };
                    
                    return Option<nlohmann::json>(fallback_search);
                }
            }
            LOG(ERROR) << "Failed to extract search parameters from OpenAI response";
            fallback_search["llm_response"] = {
                {"message", "Failed to extract search parameters from OpenAI response"},
                {"content", content},
                {"status_code", 500}
            };
            return Option<nlohmann::json>(fallback_search);
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception during OpenAI API request: " << e.what();
        LOG(WARNING) << "Using fallback search parameters due to exception";
        
        // Add error information to the fallback search parameters
        fallback_search["llm_response"] = {
            {"message", "Exception during OpenAI API request: " + std::string(e.what())},
            {"status_code", 500}
        };
        
        return Option<nlohmann::json>(fallback_search);
    }
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

    nlohmann::json request_body;
    request_body["messages"] = nlohmann::json::array();
    
    // System message
    nlohmann::json system_message;
    system_message["role"] = "system";
    system_message["content"] = system_prompt;
    request_body["messages"].push_back(system_message);

    // User message
    nlohmann::json user_message;
    user_message["role"] = "user";
    user_message["content"] = query;
    request_body["messages"].push_back(user_message);

    request_body["max_tokens"] = max_bytes;

    // Set up headers for Cloudflare request
    std::unordered_map<std::string, std::string> headers;
    headers["Content-Type"] = "application/json";
    headers["Authorization"] = "Bearer " + api_key;

    std::string response;
    std::map<std::string, std::string> response_headers;
    
    // Make HTTP request to Cloudflare
    long status_code = HttpClient::post_response(api_url, request_body.dump(), response,
                                           response_headers, headers, max_bytes);

    if(status_code != 200) {
        return Option<nlohmann::json>(status_code, "Cloudflare API request failed: " + response);
    }

    // Parse Cloudflare response
    nlohmann::json response_json;
    try {
        response_json = nlohmann::json::parse(response);
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Failed to parse Cloudflare response: " + std::string(e.what()));
    }

    if(response_json.count("result") == 0 || !response_json["result"].is_object() || 
       response_json["result"].count("response") == 0 || !response_json["result"]["response"].is_string()) {
        return Option<nlohmann::json>(500, "Invalid response from Cloudflare API");
    }

    std::string content = response_json["result"]["response"].get<std::string>();
    
    // Parse LLM response into search parameters
    try {
        nlohmann::json search_params = nlohmann::json::parse(content);
        return Option<nlohmann::json>(search_params);
    } catch(const std::exception& e) {
        // Try to extract JSON from the response if it's wrapped in text
        std::regex json_regex("\\{[\\s\\S]*\\}");
        std::smatch matches;
        if(std::regex_search(content, matches, json_regex)) {
            try {
                nlohmann::json search_params = nlohmann::json::parse(matches[0].str());
                return Option<nlohmann::json>(search_params);
            } catch(const std::exception& nested_e) {
                return Option<nlohmann::json>(500, "Failed to parse search parameters from LLM response: " + 
                                           std::string(nested_e.what()) + ". Response: " + content);
            }
        }
        return Option<nlohmann::json>(500, "Failed to parse search parameters from LLM response: " + 
                                     std::string(e.what()) + ". Response: " + content);
    }
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
    long status_code = HttpClient::post_response(api_url, request_body.dump(), response,
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
    try {
        nlohmann::json search_params = nlohmann::json::parse(content);
        return Option<nlohmann::json>(search_params);
    } catch(const std::exception& e) {
        // Try to extract JSON from the response if it's wrapped in text
        std::regex json_regex("\\{[\\s\\S]*\\}");
        std::smatch matches;
        if(std::regex_search(content, matches, json_regex)) {
            try {
                nlohmann::json search_params = nlohmann::json::parse(matches[0].str());
                return Option<nlohmann::json>(search_params);
            } catch(const std::exception& nested_e) {
                return Option<nlohmann::json>(500, "Failed to parse search parameters from LLM response: " + 
                                           std::string(nested_e.what()) + ". Response: " + content);
            }
        }
        return Option<nlohmann::json>(500, "Failed to parse search parameters from LLM response: " + 
                                     std::string(e.what()) + ". Response: " + content);
    }
} 