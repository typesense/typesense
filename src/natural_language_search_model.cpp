#include <regex>
#include <iterator>
#include "natural_language_search_model.h"
#include "text_embedder_remote.h"
#include "string_utils.h"
#include "logger.h"
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <ctime>

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
    } else if(model_namespace == "google") {
        return validate_google_model(model_config);
    } else if(model_namespace == "gcp") {
        return validate_gcp_model(model_config);
    } else if(model_namespace == "azure") {
        return validate_azure_model(model_config);
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
        return openai_vllm_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "cloudflare") {
        return cloudflare_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "vllm") {
        return openai_vllm_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "google") {
        return google_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "gcp") {
        return gcp_generate_search_params(query, full_system_prompt, model_config);
    } else if(model_namespace == "azure") {
        return azure_generate_search_params(query, full_system_prompt, model_config);
    }
    return Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<bool> NaturalLanguageSearchModel::validate_openai_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0 || !model_config["api_key"].is_string() || 
       model_config["api_key"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `api_key` is missing or is not a non-empty string.");
    }
    // Validate API key by making a test API call
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    bool is_o_model = (model_name_without_namespace.size() >= 2 && model_name_without_namespace[0] == 'o' 
                        && isdigit(model_name_without_namespace[1]));
    bool is_gpt5_model = model_name_without_namespace.find("gpt-5") != std::string::npos;

    if(model_config.count("temperature") != 0) {
        if(is_o_model || is_gpt5_model) {
            return Option<bool>(400, "Property `temperature` is not supported for the o-series and gpt-5 models.");
        }
        if(!model_config["temperature"].is_number() || 
           model_config["temperature"].get<float>() < 0 || 
           model_config["temperature"].get<float>() > 2) {
            return Option<bool>(400, "Property `temperature` must be a number between 0 and 2.");
        }
    }


    nlohmann::json test_request;
    test_request["model"] = model_name_without_namespace;
    test_request["messages"] = R"([{"role":"user","content":"hello"}])"_json;
    if(is_o_model || is_gpt5_model) {
        test_request["max_completion_tokens"] = 10;
    } else {
        test_request["max_tokens"] = 10;
        test_request["temperature"] = 0;
    }
    
    auto result = call_openai_api(test_request, model_config, VALIDATION_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<bool>(400, result.error());
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::openai_vllm_generate_search_params(
    const std::string& query,
    const std::string& system_prompt,
    const nlohmann::json& model_config) {

    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    const std::string& api_key = model_config.contains("api_key") ? model_config["api_key"].get<std::string>() : "";
    float temperature = model_config.value("temperature", 0.0f);
    size_t max_bytes = model_config["max_bytes"].get<size_t>();
    std::string api_url = model_config.value("api_url", std::string("https://api.openai.com/v1/chat/completions"));

    bool is_o_model = (model_name_without_namespace.size() >= 2 && model_name_without_namespace[0] == 'o' 
                        && isdigit(model_name_without_namespace[1]));
    bool is_gpt5_model = model_name_without_namespace.find("gpt-5") != std::string::npos;

    nlohmann::json request_body;
    request_body["model"] = model_name_without_namespace;
    if(is_o_model || is_gpt5_model) {
        request_body["max_completion_tokens"] = max_bytes;
    } else {
        request_body["max_tokens"] = max_bytes;
        request_body["temperature"] = temperature;
    }
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
    nlohmann::json fallback{{"q", query}};

    long status_code = post_response(api_url, request_body.dump(), response, response_headers, headers, DEFAULT_TIMEOUT_MS);

    if(status_code != 200) {
        return Option<nlohmann::json>(500, "Failed to get response from OpenAI: " + std::to_string(status_code));
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

    // Validate API key and account ID by making a test API call
    nlohmann::json test_request = {
        {"messages", R"([{"role":"user","content":"hello"}])"_json},
        {"max_tokens", 10}
    };

    auto result = call_cloudflare_api(test_request, model_config, VALIDATION_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<bool>(400, result.error());
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::cloudflare_generate_search_params(
    const std::string& query, 
    const std::string& system_prompt,
    const nlohmann::json& model_config) {
    
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find("/") + 1);
    size_t max_bytes = model_config["max_bytes"].get<size_t>();

    nlohmann::json messages = nlohmann::json::array({
        {{"role", "system"}, {"content", system_prompt}},
        {{"role", "user"}, {"content", query}}
    });

    nlohmann::json request_body = {
        {"messages", messages},
        {"max_tokens", max_bytes}
    };

    auto result = call_cloudflare_api(request_body, model_config, DEFAULT_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<nlohmann::json>(500, result.error());
    }

    auto response_json = result.get();
    if(!response_json.contains("result") || !response_json["result"].is_object() ||
       !response_json["result"].contains("response") || !response_json["result"]["response"].is_string()) {
        return Option<nlohmann::json>(500, "Invalid format from Cloudflare API");
    }

    std::string content = response_json["result"]["response"].get<std::string>();
    return extract_search_params_from_content(content, model_name_without_namespace);

}

Option<bool> NaturalLanguageSearchModel::validate_vllm_model(const nlohmann::json& model_config) {
    if(model_config.count("api_url") == 0 || !model_config["api_url"].is_string() || 
       model_config["api_url"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `api_url` is missing or is not a non-empty string.");
    }

    if(model_config.count("api_key") != 0 && (!model_config["api_key"].is_string() || 
       model_config["api_key"].get<std::string>().empty())) {
        return Option<bool>(400, "Property `api_key` is not a string or is not a non-empty string.");
    }

    if(model_config.count("temperature") != 0 && 
       (!model_config["temperature"].is_number() || 
        model_config["temperature"].get<float>() < 0 || 
        model_config["temperature"].get<float>() > 2)) {
        return Option<bool>(400, "Property `temperature` must be a number between 0 and 2.");
    }

    // Validate API URL and model by making a test API call
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);

    nlohmann::json test_request;
    test_request["model"] = model_name_without_namespace;
    test_request["messages"] = R"([{"role":"user","content":"hello"}])"_json;
    test_request["max_tokens"] = 10;
    test_request["temperature"] = 0;

    auto result = call_openai_api(test_request, model_config, VALIDATION_TIMEOUT_MS);
    if(!result.ok()) {
        // Replace "OpenAI" with "vLLM" in error message
        std::string error_msg = result.error();
        size_t pos = error_msg.find("OpenAI");
        if(pos != std::string::npos) {
            error_msg.replace(pos, 6, "vLLM");
        }
        return Option<bool>(400, error_msg);
    }

    return Option<bool>(true);
}

Option<bool> NaturalLanguageSearchModel::validate_google_model(const nlohmann::json& model_config) {
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

    if(model_config.count("top_p") != 0 && 
       (!model_config["top_p"].is_number() || 
        model_config["top_p"].get<float>() < 0 || 
        model_config["top_p"].get<float>() > 1)) {
        return Option<bool>(400, "Property `top_p` must be a number between 0 and 1.");
    }

    if(model_config.count("top_k") != 0 && 
       (!model_config["top_k"].is_number_integer() || 
        model_config["top_k"].get<int>() < 0)) {
        return Option<bool>(400, "Property `top_k` must be a non-negative integer.");
    }

    if(model_config.count("stop_sequences") != 0 && !model_config["stop_sequences"].is_array()) {
        return Option<bool>(400, "Property `stop_sequences` must be an array of strings.");
    }

    if(model_config.count("api_version") != 0 && !model_config["api_version"].is_string()) {
        return Option<bool>(400, "Property `api_version` must be a string.");
    }

    // Validate API key by making a test API call
    nlohmann::json test_request;
    test_request["contents"] = {{
        {"parts", {{"text", "hello"}}}
    }};
    test_request["generationConfig"] = {
        {"temperature", 0},
        {"maxOutputTokens", 10}
    };

    auto result = call_google_api(test_request, model_config, VALIDATION_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<bool>(400, result.error());
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::google_generate_search_params(
    const std::string& query,
    const std::string& system_prompt,
    const nlohmann::json& model_config) {

    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    float temperature = model_config.value("temperature", 0.0f);
    size_t max_bytes = model_config["max_bytes"].get<size_t>();

    nlohmann::json request_body;
    
    // Add system instruction if present
    if(!system_prompt.empty()) {
        request_body["systemInstruction"] = {
            {"parts", {{{"text", system_prompt}}}}
        };
    }
    
    // Add user content
    request_body["contents"] = {{
        {"parts", {{{"text", query}}}}
    }};
    
    // Add generation config
    nlohmann::json generation_config;
    generation_config["temperature"] = temperature;
    generation_config["maxOutputTokens"] = max_bytes;
    
    if(model_config.count("top_p") != 0) {
        generation_config["topP"] = model_config["top_p"].get<float>();
    }
    
    if(model_config.count("top_k") != 0) {
        generation_config["topK"] = model_config["top_k"].get<int>();
    }
    
    if(model_config.count("stop_sequences") != 0) {
        generation_config["stopSequences"] = model_config["stop_sequences"];
    }
    
    request_body["generationConfig"] = generation_config;

    auto result = call_google_api(request_body, model_config, DEFAULT_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<nlohmann::json>(500, "Failed to get response from Google Gemini: " + result.error());
    }

    auto response_json = result.get();
    // Extract text from Gemini response format
    if(!response_json.contains("candidates") || !response_json["candidates"].is_array() || 
       response_json["candidates"].empty()) {
        return Option<nlohmann::json>(500, "No valid candidates in Google Gemini response");
    }

    auto& candidate = response_json["candidates"][0];
    if(!candidate.contains("content") || !candidate["content"].contains("parts") || 
       !candidate["content"]["parts"].is_array() || candidate["content"]["parts"].empty()) {
        return Option<nlohmann::json>(500, "No valid content in Google Gemini response");
    }

    auto& part = candidate["content"]["parts"][0];
    if(!part.contains("text") || !part["text"].is_string()) {
        return Option<nlohmann::json>(500, "No valid text in Google Gemini response");
    }

    std::string content = part["text"].get<std::string>();
    return extract_search_params_from_content(content, model_name_without_namespace);
}

// Helper methods for GCP service account authentication
static void normalize_pem_newlines(std::string& pem) {
    std::string::size_type pos = 0;
    while((pos = pem.find("\\n", pos)) != std::string::npos) {
        pem.replace(pos, 2, "\n");
        pos += 1;
    }
}

static std::string base64url_encode(const std::string& input) {
    std::string out = StringUtils::base64_encode(input);
    for(char& c : out) {
        if(c == '+') c = '-';
        else if(c == '/') c = '_';
    }
    // strip padding '='
    while(!out.empty() && out.back() == '=') out.pop_back();
    return out;
}

static Option<std::string> sign_jwt_rs256(const std::string& message, const std::string& private_key_pem) {
    BIO* bio = BIO_new_mem_buf(private_key_pem.data(), static_cast<int>(private_key_pem.size()));
    if(!bio) return Option<std::string>(500, "Internal error: BIO_new_mem_buf failed");
    EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if(!pkey) return Option<std::string>(400, "Invalid service_account.private_key format.");

    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if(!mdctx) { EVP_PKEY_free(pkey); return Option<std::string>(500, "Internal error: EVP_MD_CTX_new failed"); }
    if(EVP_DigestSignInit(mdctx, nullptr, EVP_sha256(), nullptr, pkey) != 1) {
        EVP_MD_CTX_free(mdctx); EVP_PKEY_free(pkey); return Option<std::string>(500, "Internal error: EVP_DigestSignInit failed");
    }
    if(EVP_DigestSignUpdate(mdctx, message.data(), message.size()) != 1) {
        EVP_MD_CTX_free(mdctx); EVP_PKEY_free(pkey); return Option<std::string>(500, "Internal error: EVP_DigestSignUpdate failed");
    }
    size_t siglen = 0;
    if(EVP_DigestSignFinal(mdctx, nullptr, &siglen) != 1) {
        EVP_MD_CTX_free(mdctx); EVP_PKEY_free(pkey); return Option<std::string>(500, "Internal error: EVP_DigestSignFinal size failed");
    }
    std::string signature;
    signature.resize(siglen);
    if(EVP_DigestSignFinal(mdctx, reinterpret_cast<unsigned char*>(&signature[0]), &siglen) != 1) {
        EVP_MD_CTX_free(mdctx); EVP_PKEY_free(pkey); return Option<std::string>(500, "Internal error: EVP_DigestSignFinal failed");
    }
    signature.resize(siglen);
    EVP_MD_CTX_free(mdctx);
    EVP_PKEY_free(pkey);
    return Option<std::string>(signature);
}

static Option<std::pair<std::string,long long>> mint_sa_access_token_once(const std::string& client_email, const std::string& private_key_pem, const std::string& token_uri) {
    const long long now = static_cast<long long>(std::time(nullptr));
    const long long exp = now + 3600; // 1 hour
    nlohmann::json header = {{"alg","RS256"},{"typ","JWT"}};
    nlohmann::json claims = {
        {"iss", client_email},
        {"scope", "https://www.googleapis.com/auth/cloud-platform"},
        {"aud", token_uri},
        {"exp", exp},
        {"iat", now}
    };
    const std::string signing_input = base64url_encode(header.dump()) + "." + base64url_encode(claims.dump());
    auto sig_op = sign_jwt_rs256(signing_input, private_key_pem);
    if(!sig_op.ok()) return Option<std::pair<std::string,long long>>(sig_op.code(), sig_op.error());
    const std::string assertion = signing_input + "." + base64url_encode(sig_op.get());

    std::unordered_map<std::string, std::string> headers;
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    std::map<std::string, std::string> res_headers;
    std::string res;
    std::string req_body = "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" + assertion;
    
    const std::string GCP_AUTH_TOKEN_URL = "https://oauth2.googleapis.com/token";
    auto res_code = NaturalLanguageSearchModel::post_response(token_uri, req_body, res, res_headers, headers, 30000);
    if(res_code != 200) {
        if(res_code == 408) return Option<std::pair<std::string,long long>>(408, "GCP API timeout.");
        nlohmann::json json_res;
        try { json_res = nlohmann::json::parse(res); } catch (...) {
            return Option<std::pair<std::string,long long>>(400, "Got malformed response from GCP API.");
        }
        std::string msg = json_res.count("error") ? json_res["error"].dump() : res;
        return Option<std::pair<std::string,long long>>(400, "GCP API error: " + msg);
    }

    nlohmann::json res_json;
    try { res_json = nlohmann::json::parse(res); } catch (...) {
        return Option<std::pair<std::string,long long>>(400, "Got malformed response from GCP API.");
    }

    if(res_json.count("access_token") == 0 || res_json.count("expires_in") == 0) {
        return Option<std::pair<std::string,long long>>(400, "GCP API error: access_token missing in response");
    }

    const std::string token = res_json["access_token"].get<std::string>();
    const long long expires_at = now + res_json["expires_in"].get<long long>();
    return Option<std::pair<std::string,long long>>(std::make_pair(token, expires_at));
}

static Option<std::string> ensure_gcp_access_token(const nlohmann::json& model_config, bool force_refresh = false) {
    // Check for service account authentication
    if(model_config.count("service_account") > 0 && model_config["service_account"].is_object()) {
        const nlohmann::json& sa = model_config["service_account"];
        const std::string client_email = sa["client_email"].get<std::string>();
        std::string private_key = sa["private_key"].get<std::string>();
        normalize_pem_newlines(private_key);
        const std::string token_uri = sa.count("token_uri") > 0 && sa["token_uri"].is_string() ? 
                                       sa["token_uri"].get<std::string>() : "https://oauth2.googleapis.com/token";
        

        auto mint_op = mint_sa_access_token_once(client_email, private_key, token_uri);
        if(!mint_op.ok()) {
            return Option<std::string>(mint_op.code(), mint_op.error());
        }
        return Option<std::string>(mint_op.get().first);
    }
    
    if(!force_refresh && model_config.count("access_token") > 0 && 
       model_config["access_token"].is_string() && 
       !model_config["access_token"].get<std::string>().empty()) {
        return Option<std::string>(model_config["access_token"].get<std::string>());
    }
    
    if(!model_config.count("refresh_token") || !model_config.count("client_id") || !model_config.count("client_secret")) {
        return Option<std::string>(400, "Missing OAuth credentials (refresh_token, client_id, client_secret)");
    }

    const std::string& refresh_token = model_config["refresh_token"].get<std::string>();
    const std::string& client_id = model_config["client_id"].get<std::string>();
    const std::string& client_secret = model_config["client_secret"].get<std::string>();

    return NaturalLanguageSearchModel::generate_gcp_access_token(refresh_token, client_id, client_secret);
}

Option<bool> NaturalLanguageSearchModel::validate_gcp_model(const nlohmann::json& model_config) {
    if(model_config.count("service_account") > 0 && model_config["service_account"].is_object()) {
        if(model_config.count("project_id") == 0 || !model_config["project_id"].is_string() || 
           model_config["project_id"].get<std::string>().empty()) {
            return Option<bool>(400, "Property `project_id` is missing or is not a non-empty string.");
        }

        const std::string& model_name = model_config["model_name"].get<std::string>();
        const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
        const std::string& project_id = model_config["project_id"].get<std::string>();
        std::string region = model_config.value("region", std::string("us-central1"));
        
        if(model_name.find("/") == std::string::npos || model_name.substr(0, model_name.find("/")) != "gcp") {
            return Option<bool>(400, "Invalid GCP model name");
        }

        const nlohmann::json& sa = model_config["service_account"];
        if(sa.count("client_email") == 0 || !sa["client_email"].is_string() || 
           sa.count("private_key") == 0 || !sa["private_key"].is_string()) {
            return Option<bool>(400, "Property `service_account.client_email/private_key` missing or not a string.");
        }
        
        const std::string client_email = sa["client_email"].get<std::string>();
        std::string private_key = sa["private_key"].get<std::string>();
        normalize_pem_newlines(private_key);
        const std::string token_uri = sa.count("token_uri") > 0 && sa["token_uri"].is_string() ? 
                                       sa["token_uri"].get<std::string>() : "https://oauth2.googleapis.com/token";
        
        auto mint_op = mint_sa_access_token_once(client_email, private_key, token_uri);
        if(!mint_op.ok()) {
            return Option<bool>(mint_op.code(), mint_op.error());
        }
        const std::string access_token_tmp = mint_op.get().first;

        std::string api_url = "https://" + region + "-aiplatform.googleapis.com/v1/projects/" + 
                             project_id + "/locations/" + region + "/publishers/google/models/" + 
                             model_name_without_namespace + ":generateContent";

        std::unordered_map<std::string, std::string> headers = {
            {"Content-Type", "application/json"},
            {"Authorization", "Bearer " + access_token_tmp}
        };

        nlohmann::json test_request;
        test_request["contents"] = {{
            {"role", "user"},
            {"parts", {{{"text", "hello"}}}}
        }};
        test_request["generationConfig"] = {
            {"temperature", 0},
            {"maxOutputTokens", 10}
        };

        std::string response;
        std::map<std::string, std::string> response_headers;
        long status_code = post_response(api_url, test_request.dump(), response, response_headers, headers, VALIDATION_TIMEOUT_MS);

        if(status_code != 200) {
            nlohmann::json json_res;
            try { json_res = nlohmann::json::parse(response); } catch (...) {
                return Option<bool>(400, "Got malformed response from GCP API.");
            }
            if(status_code == 408) return Option<bool>(408, "GCP API timeout.");
            if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
                return Option<bool>(400, "GCP API error: " + response);
            }
            return Option<bool>(400, "GCP API error: " + json_res["error"]["message"].get<std::string>());
        }

        return Option<bool>(true);
    }

    if(model_config.count("project_id") == 0 || !model_config["project_id"].is_string() || 
       model_config["project_id"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `project_id` is missing or is not a non-empty string.");
    }

    if(model_config.count("access_token") == 0 || !model_config["access_token"].is_string() || 
       model_config["access_token"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `access_token` is missing or is not a non-empty string.");
    }

    if(model_config.count("refresh_token") == 0 || !model_config["refresh_token"].is_string() || 
       model_config["refresh_token"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `refresh_token` is missing or is not a non-empty string.");
    }

    if(model_config.count("client_id") == 0 || !model_config["client_id"].is_string() || 
       model_config["client_id"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `client_id` is missing or is not a non-empty string.");
    }

    if(model_config.count("client_secret") == 0 || !model_config["client_secret"].is_string() || 
       model_config["client_secret"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `client_secret` is missing or is not a non-empty string.");
    }

    // Optional fields
    if(model_config.count("region") != 0 && !model_config["region"].is_string()) {
        return Option<bool>(400, "Property `region` must be a string.");
    }

    if(model_config.count("temperature") != 0 && 
       (!model_config["temperature"].is_number() || 
        model_config["temperature"].get<float>() < 0 || 
        model_config["temperature"].get<float>() > 2)) {
        return Option<bool>(400, "Property `temperature` must be a number between 0 and 2.");
    }

    if(model_config.count("top_p") != 0 && 
       (!model_config["top_p"].is_number() || 
        model_config["top_p"].get<float>() < 0 || 
        model_config["top_p"].get<float>() > 1)) {
        return Option<bool>(400, "Property `top_p` must be a number between 0 and 1.");
    }

    if(model_config.count("top_k") != 0 && 
       (!model_config["top_k"].is_number_integer() || 
        model_config["top_k"].get<int>() < 0)) {
        return Option<bool>(400, "Property `top_k` must be a non-negative integer.");
    }

    if(model_config.count("max_output_tokens") != 0 && 
       (!model_config["max_output_tokens"].is_number_integer() || 
        model_config["max_output_tokens"].get<int>() <= 0)) {
        return Option<bool>(400, "Property `max_output_tokens` must be a positive integer.");
    }

    // Validate credentials by making a test API call
    nlohmann::json test_request;
    test_request["contents"] = {{
        {"role", "user"},
        {"parts", {{{"text", "hello"}}}}
    }};
    test_request["generationConfig"] = {
        {"temperature", 0},
        {"maxOutputTokens", 10}
    };

    auto result = call_gcp_api(test_request, model_config, VALIDATION_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<bool>(400, result.error());
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::gcp_generate_search_params(
    const std::string& query,
    const std::string& system_prompt,
    const nlohmann::json& model_config) {

    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    float temperature = model_config.value("temperature", 0.0f);
    size_t max_bytes = model_config["max_bytes"].get<size_t>();

    // Build request body
    nlohmann::json request_body;
    
    // Combine system prompt and query
    std::string full_prompt = system_prompt;
    if (!full_prompt.empty()) {
        full_prompt += "\n\n";
    }
    full_prompt += query;
    
    // Add contents
    request_body["contents"] = {{
        {"role", "user"},
        {"parts", {{{"text", full_prompt}}}}
    }};
    
    // Add generation config
    nlohmann::json generation_config;
    generation_config["temperature"] = temperature;
    generation_config["maxOutputTokens"] = max_bytes;
    
    if(model_config.count("top_p") != 0) {
        generation_config["topP"] = model_config["top_p"].get<float>();
    }
    
    if(model_config.count("top_k") != 0) {
        generation_config["topK"] = model_config["top_k"].get<int>();
    }
    
    if(model_config.count("max_output_tokens") != 0) {
        generation_config["maxOutputTokens"] = model_config["max_output_tokens"].get<int>();
    }
    
    request_body["generationConfig"] = generation_config;

    auto result = call_gcp_api(request_body, model_config, DEFAULT_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<nlohmann::json>(500, "Failed to get response from GCP Vertex AI: " + result.error());
    }

    auto response_json = result.get();
    // Extract text from Vertex AI response format
    if(!response_json.contains("candidates") || !response_json["candidates"].is_array() || 
       response_json["candidates"].empty()) {
        return Option<nlohmann::json>(500, "No valid candidates in GCP Vertex AI response");
    }

    auto& candidate = response_json["candidates"][0];
    if(!candidate.contains("content") || !candidate["content"].contains("parts") || 
       !candidate["content"]["parts"].is_array() || candidate["content"]["parts"].empty()) {
        return Option<nlohmann::json>(500, "No valid content in GCP Vertex AI response");
    }

    auto& part = candidate["content"]["parts"][0];
    if(!part.contains("text") || !part["text"].is_string()) {
        return Option<nlohmann::json>(500, "No valid text in GCP Vertex AI response");
    }

    std::string content = part["text"].get<std::string>();
    return extract_search_params_from_content(content, model_name_without_namespace);
}

Option<std::string> NaturalLanguageSearchModel::generate_gcp_access_token(
    const std::string& refresh_token, 
    const std::string& client_id, 
    const std::string& client_secret) {
    
    const std::string GCP_AUTH_TOKEN_URL = "https://oauth2.googleapis.com/token";
    
    std::unordered_map<std::string, std::string> headers;
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    
    std::map<std::string, std::string> res_headers;
    std::string res;
    std::string req_body = "grant_type=refresh_token&client_id=" + client_id + 
                          "&client_secret=" + client_secret + "&refresh_token=" + refresh_token;

    auto res_code = post_response(GCP_AUTH_TOKEN_URL, req_body, res, res_headers, headers, DEFAULT_TIMEOUT_MS);
    
    if(res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "Got malformed response from GCP OAuth API.");
        }
        
        // Handle OAuth2 error response format
        if(json_res.count("error") != 0) {
            std::string error_msg = "GCP OAuth API error: ";
            
            // OAuth2 errors have "error" as a string and "error_description" as additional info
            if(json_res["error"].is_string()) {
                error_msg += json_res["error"].get<std::string>();
                if(json_res.count("error_description") != 0 && json_res["error_description"].is_string()) {
                    error_msg += " - " + json_res["error_description"].get<std::string>();
                }
            }
            // Some GCP errors have "error" as an object with "message" field
            else if(json_res["error"].is_object() && json_res["error"].count("message") != 0) {
                error_msg += json_res["error"]["message"].get<std::string>();
            }
            else {
                error_msg += "Unknown error format";
            }
            
            return Option<std::string>(400, error_msg);
        }
        
        return Option<std::string>(400, "GCP OAuth API error: HTTP " + std::to_string(res_code));
    }
    
    nlohmann::json res_json;
    try {
        res_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from GCP OAuth API.");
    }
    
    if(!res_json.contains("access_token") || !res_json["access_token"].is_string()) {
        return Option<std::string>(400, "No access token in GCP OAuth response");
    }
    
    std::string access_token = res_json["access_token"].get<std::string>();
    return Option<std::string>(access_token);
}

long NaturalLanguageSearchModel::post_response(const std::string& url, const std::string& body, std::string& response,
                                    std::map<std::string, std::string>& res_headers,
                                    const std::unordered_map<std::string, std::string>& headers,
                                    long timeout_ms,
                                    bool send_ts_api_header) {
    // Capture request if enabled
    if (capture_request) {
        captured_requests.push_back({url, body, headers});
    }
    
    if (use_mock_response && !mock_responses.empty() && mock_response_index < mock_responses.size()) {
        auto& [mock_body, status, mock_headers] = mock_responses[mock_response_index++];
        response = mock_body;
        res_headers = mock_headers;
        return status;
    }
    return HttpClient::post_response(url, body, response, res_headers, headers, timeout_ms, send_ts_api_header);
}

void NaturalLanguageSearchModel::add_mock_response(const std::string& response_body, long status_code, const std::map<std::string, std::string>& response_headers) {
    use_mock_response = true;
    mock_responses.push_back(std::make_tuple(response_body, status_code, response_headers));
}

void NaturalLanguageSearchModel::clear_mock_responses() {
    use_mock_response = false;
    mock_responses.clear();
    mock_response_index = 0;
    captured_requests.clear();
}

// Helper method for making OpenAI/vLLM API calls
Option<nlohmann::json> NaturalLanguageSearchModel::call_openai_api(
    const nlohmann::json& request_body,
    const nlohmann::json& model_config,
    long timeout_ms) {
    
    const std::string& api_key = model_config.contains("api_key") ? model_config["api_key"].get<std::string>() : "";
    std::string api_url = model_config.value("api_url", std::string("https://api.openai.com/v1/chat/completions"));

    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"}
    };
    
    if(!api_key.empty()) {
        headers["Authorization"] = "Bearer " + api_key;
    }

    std::string response;
    std::map<std::string, std::string> response_headers;
    long status_code = post_response(api_url, request_body.dump(), response, response_headers, headers, timeout_ms);

    if(status_code == 408) {
        return Option<nlohmann::json>(408, "OpenAI API timeout.");
    }

    if(status_code != 200) {
        std::string error_msg = "OpenAI API error: ";
        try {
            nlohmann::json response_json = nlohmann::json::parse(response);
            if(response_json.contains("error") && response_json["error"].contains("message")) {
                error_msg += response_json["error"]["message"].get<std::string>();
            } else {
                error_msg += "HTTP " + std::to_string(status_code);
            }
        } catch(...) {
            error_msg += "HTTP " + std::to_string(status_code);
        }
        return Option<nlohmann::json>(status_code, error_msg);
    }

    try {
        return Option<nlohmann::json>(nlohmann::json::parse(response));
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Failed to parse OpenAI response: Invalid JSON");
    }
}

// Helper method for making Cloudflare API calls
Option<nlohmann::json> NaturalLanguageSearchModel::call_cloudflare_api(
    const nlohmann::json& request_body,
    const nlohmann::json& model_config,
    long timeout_ms) {
    
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find("/") + 1);
    const std::string& api_key = model_config["api_key"].get<std::string>();
    const std::string& account_id = model_config["account_id"].get<std::string>();
    
    std::string api_url = "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/ai/run/" + model_name_without_namespace;

    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer " + api_key}
    };

    std::string response;
    std::map<std::string, std::string> response_headers;
    long status_code = post_response(api_url, request_body.dump(), response, response_headers, headers, timeout_ms);

    if(status_code == 408) {
        return Option<nlohmann::json>(408, "Cloudflare API timeout.");
    }

    if(status_code != 200) {
        std::string error_msg = "Cloudflare API error: ";
        try {
            nlohmann::json response_json = nlohmann::json::parse(response);
            if(response_json.contains("errors") && response_json["errors"].is_array() && !response_json["errors"].empty()) {
                error_msg += response_json["errors"][0]["message"].get<std::string>();
            } else {
                error_msg += "HTTP " + std::to_string(status_code);
            }
        } catch(...) {
            error_msg += "HTTP " + std::to_string(status_code);
        }
        return Option<nlohmann::json>(status_code, error_msg);
    }

    try {
        return Option<nlohmann::json>(nlohmann::json::parse(response));
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Cloudflare API response JSON parse error: Invalid JSON");
    }
}

// Helper method for making Google API calls
Option<nlohmann::json> NaturalLanguageSearchModel::call_google_api(
    const nlohmann::json& request_body,
    const nlohmann::json& model_config,
    long timeout_ms) {
    
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    const std::string& api_key = model_config["api_key"].get<std::string>();
    const std::string& api_version = model_config.value("api_version", std::string("v1beta"));
    
    std::string api_url = "https://generativelanguage.googleapis.com/" + api_version + 
                         "/models/" + model_name_without_namespace + ":generateContent?key=" + api_key;

    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"}
    };

    std::string response;
    std::map<std::string, std::string> response_headers;
    long status_code = post_response(api_url, request_body.dump(), response, response_headers, headers, timeout_ms);

    if(status_code == 408) {
        return Option<nlohmann::json>(408, "Google Gemini API timeout.");
    }

    if(status_code != 200) {
        std::string error_msg = "Google Gemini API error: ";
        try {
            nlohmann::json response_json = nlohmann::json::parse(response);
            if(response_json.contains("error") && response_json["error"].contains("message")) {
                error_msg += response_json["error"]["message"].get<std::string>();
            } else {
                error_msg += "HTTP " + std::to_string(status_code);
            }
        } catch(...) {
            error_msg += "HTTP " + std::to_string(status_code);
        }
        return Option<nlohmann::json>(status_code, error_msg);
    }

    try {
        return Option<nlohmann::json>(nlohmann::json::parse(response));
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Failed to parse Google Gemini response: Invalid JSON");
    }
}

// Helper method for making GCP Vertex AI API calls
Option<nlohmann::json> NaturalLanguageSearchModel::call_gcp_api(
    const nlohmann::json& request_body,
    const nlohmann::json& model_config,
    long timeout_ms) {
    
    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    const std::string& project_id = model_config["project_id"].get<std::string>();
    const std::string& region = model_config.value("region", std::string("us-central1"));
    
    auto token_op = ensure_gcp_access_token(model_config, false);
    if(!token_op.ok()) {
        return Option<nlohmann::json>(token_op.code(), "Failed to get GCP access token: " + token_op.error());
    }
    std::string access_token = token_op.get();
    
    std::string api_url = "https://" + region + "-aiplatform.googleapis.com/v1/projects/" + 
                         project_id + "/locations/" + region + "/publishers/google/models/" + 
                         model_name_without_namespace + ":generateContent";

    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer " + access_token}
    };

    std::string response;
    std::map<std::string, std::string> response_headers;
    long status_code = post_response(api_url, request_body.dump(), response, response_headers, headers, timeout_ms);

    // Handle 401 Unauthorized - refresh token and retry
    if(status_code == 401) {
        auto refresh_op = ensure_gcp_access_token(model_config, true);
        if(!refresh_op.ok()) {
            return Option<nlohmann::json>(401, "Failed to refresh GCP access token: " + refresh_op.error());
        }
        
        access_token = refresh_op.get();
        headers["Authorization"] = "Bearer " + access_token;
        
        // Retry with new token
        response.clear();
        status_code = post_response(api_url, request_body.dump(), response, response_headers, headers, timeout_ms);
    }

    if(status_code == 408) {
        return Option<nlohmann::json>(408, "GCP Vertex AI API timeout.");
    }

    if(status_code != 200) {
        std::string error_msg = "GCP Vertex AI API error: ";
        try {
            nlohmann::json response_json = nlohmann::json::parse(response);
            if(response_json.contains("error") && response_json["error"].contains("message")) {
                error_msg += response_json["error"]["message"].get<std::string>();
            } else {
                error_msg += "HTTP " + std::to_string(status_code);
            }
        } catch(...) {
            error_msg += "HTTP " + std::to_string(status_code);
        }
        return Option<nlohmann::json>(status_code, error_msg);
    }

    try {
        return Option<nlohmann::json>(nlohmann::json::parse(response));
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Failed to parse GCP Vertex AI response: Invalid JSON");
    }
}

Option<bool> NaturalLanguageSearchModel::validate_azure_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0 || !model_config["api_key"].is_string() || 
       model_config["api_key"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `api_key` is missing or is not a non-empty string.");
    }

    if(model_config.count("url") == 0 || !model_config["url"].is_string() || 
       model_config["url"].get<std::string>().empty()) {
        return Option<bool>(400, "Property `url` is missing or is not a non-empty string.");
    }

    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    bool is_o_model = (model_name_without_namespace.size() >= 2 && model_name_without_namespace[0] == 'o' 
                        && isdigit(model_name_without_namespace[1]));
    bool is_gpt5_model = model_name_without_namespace.find("gpt-5") != std::string::npos;

    if(model_config.count("temperature") != 0) {
        if(is_o_model || is_gpt5_model) {
            return Option<bool>(400, "Property `temperature` is not supported for the o-series and gpt-5 models.");
        }
        if(!model_config["temperature"].is_number() || 
           model_config["temperature"].get<float>() < 0 || 
           model_config["temperature"].get<float>() > 2) {
            return Option<bool>(400, "Property `temperature` must be a number between 0 and 2.");
        }
    }

    nlohmann::json test_request;
    test_request["model"] = model_name_without_namespace;
    test_request["messages"] = R"([{"role":"user","content":"hello"}])"_json;
    if(is_o_model || is_gpt5_model) {
        test_request["max_completion_tokens"] = 10;
    } else {
        test_request["max_tokens"] = 10;
        test_request["temperature"] = 0;
    }

    auto result = call_azure_api(test_request, model_config, VALIDATION_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<bool>(400, result.error());
    }

    return Option<bool>(true);
}

Option<nlohmann::json> NaturalLanguageSearchModel::azure_generate_search_params(
    const std::string& query,
    const std::string& system_prompt,
    const nlohmann::json& model_config) {

    const std::string& model_name = model_config["model_name"].get<std::string>();
    const std::string& model_name_without_namespace = model_name.substr(model_name.find('/') + 1);
    float temperature = model_config.value("temperature", 0.0f);
    size_t max_bytes = model_config["max_bytes"].get<size_t>();

    bool is_o_model = (model_name_without_namespace.size() >= 2 && model_name_without_namespace[0] == 'o' 
                        && isdigit(model_name_without_namespace[1]));
    bool is_gpt5_model = model_name_without_namespace.find("gpt-5") != std::string::npos;

    nlohmann::json request_body;
    request_body["model"] = model_name_without_namespace;
    if(is_o_model || is_gpt5_model) {
        request_body["max_completion_tokens"] = max_bytes;
    } else {
        request_body["max_tokens"] = max_bytes;
        request_body["temperature"] = temperature;
    }
    request_body["messages"] = {
        {{"role", "system"}, {"content", system_prompt}},
        {{"role", "user"}, {"content", query}}
    };

    auto result = call_azure_api(request_body, model_config, DEFAULT_TIMEOUT_MS);
    if(!result.ok()) {
        return Option<nlohmann::json>(500, "Failed to get response from Azure OpenAI: " + result.error());
    }

    auto response_json = result.get();
    if(!response_json.contains("choices") || !response_json["choices"].is_array() || 
       response_json["choices"].empty()) {
        return Option<nlohmann::json>(500, "No valid choices in Azure OpenAI response");
    }

    auto& choice = response_json["choices"][0];
    if(!choice.contains("message") || !choice["message"].contains("content") || 
       !choice["message"]["content"].is_string()) {
        return Option<nlohmann::json>(500, "No valid content in Azure OpenAI response");
    }

    std::string content = choice["message"]["content"].get<std::string>();
    return extract_search_params_from_content(content, model_name_without_namespace);
}

// Helper method for making Azure OpenAI API calls
Option<nlohmann::json> NaturalLanguageSearchModel::call_azure_api(
    const nlohmann::json& request_body,
    const nlohmann::json& model_config,
    long timeout_ms) {
    
    const std::string& api_key = model_config["api_key"].get<std::string>();
    const std::string& url = model_config["url"].get<std::string>();

    std::unordered_map<std::string, std::string> headers = {
        {"Content-Type", "application/json"},
        {"api-key", api_key}
    };

    std::string response;
    std::map<std::string, std::string> response_headers;
    long status_code = post_response(url, request_body.dump(), response, response_headers, headers, timeout_ms);

    if(status_code == 408) {
        return Option<nlohmann::json>(408, "Azure OpenAI API timeout.");
    }

    if(status_code != 200) {
        std::string error_msg = "Azure OpenAI API error: ";
        try {
            nlohmann::json response_json = nlohmann::json::parse(response);
            if(response_json.contains("error") && response_json["error"].contains("message")) {
                error_msg += response_json["error"]["message"].get<std::string>();
            } else {
                error_msg += "HTTP " + std::to_string(status_code);
            }
        } catch(...) {
            error_msg += "HTTP " + std::to_string(status_code);
        }
        return Option<nlohmann::json>(status_code, error_msg);
    }

    try {
        return Option<nlohmann::json>(nlohmann::json::parse(response));
    } catch(const std::exception& e) {
        return Option<nlohmann::json>(500, "Failed to parse Azure OpenAI response: Invalid JSON");
    }
}
