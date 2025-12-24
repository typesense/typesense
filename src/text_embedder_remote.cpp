#include <http_proxy.h>
#include "text_embedder_remote.h"
#include "embedder_manager.h"
#include "string_utils.h"
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <ctime>

Option<bool> RemoteEmbedder::validate_string_properties(const nlohmann::json& model_config, const std::vector<std::string>& properties) {
    for(auto& property : properties) {
        if(model_config.count(property) == 0 || !model_config[property].is_string()) {
            return Option<bool>(400, "Property `embed.model_config." + property + " is missing or is not a string.");
        }
    }
    return Option<bool>(true);
}

void GCPEmbedder::normalize_pem_newlines(std::string& pem) {
    std::string::size_type pos = 0;
    while((pos = pem.find("\\n", pos)) != std::string::npos) {
        pem.replace(pos, 2, "\n");
        pos += 1;
    }
}

long RemoteEmbedder::call_remote_api(const std::string& method, const std::string& url, const std::string& req_body,
                                     std::string& res_body,
                                     std::map<std::string, std::string>& res_headers,
                                     std::unordered_map<std::string, std::string>& req_headers) {

    if(raft_server == nullptr || raft_server->get_leader_url().empty()) {
        // call proxy's internal send() directly
        if(method == "GET" || method == "POST") {
            auto proxy_res = HttpProxy::get_instance().send(url, method, req_body, req_headers);
            res_body = std::move(proxy_res.body);
            res_headers = std::move(proxy_res.headers);
            return proxy_res.status_code;
        } else {
            return 400;
        }
    }

    auto proxy_url = raft_server->get_leader_url() + "proxy";
    nlohmann::json proxy_req_body;
    proxy_req_body["method"] = method;
    proxy_req_body["url"] = url;
    proxy_req_body["body"] = req_body;
    proxy_req_body["headers"] = req_headers;

    size_t per_call_timeout_ms = HttpProxy::default_timeout_ms;
    size_t num_try = HttpProxy::default_num_try;

    if(req_headers.find("timeout_ms") != req_headers.end()){
        per_call_timeout_ms = std::stoul(req_headers.at("timeout_ms"));
    }

    if(req_headers.find("num_try") != req_headers.end()){
        num_try = std::stoul(req_headers.at("num_try"));
    }

    size_t proxy_call_timeout_ms = (per_call_timeout_ms * num_try) + 1000;

    return HttpClient::get_instance().post_response(proxy_url, proxy_req_body.dump(), res_body, res_headers, {},
                                                    proxy_call_timeout_ms, true);
}


const std::string RemoteEmbedder::get_model_key(const nlohmann::json& model_config, size_t num_dims) {
    const std::string model_namespace = EmbedderManager::get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIEmbedder::get_model_key(model_config, num_dims);
    } else if(model_namespace == "google") {
        return GoogleEmbedder::get_model_key(model_config, num_dims);
    } else if(model_namespace == "gcp") {
        return GCPEmbedder::get_model_key(model_config, num_dims);
    } else if(model_namespace == "azure") {
        return AzureEmbedder::get_model_key(model_config, num_dims);
    } else {
        return "";
    }
}

OpenAIEmbedder::OpenAIEmbedder(const std::string& openai_model_path, const std::string& api_key, const size_t num_dims, 
                            const bool has_custom_dims, const nlohmann::json& model_config) : api_key(api_key), openai_model_path(openai_model_path), 
                                                                                                                         num_dims(num_dims), has_custom_dims(has_custom_dims){

    if(model_config.count("url") == 0) {
        this->openai_url = "https://api.openai.com";
    } else {
        this->openai_url = model_config["url"].get<std::string>();
    }

    if(model_config.count("path") == 0) {
        this->openai_create_embedding_suffix = OPENAI_CREATE_EMBEDDING;
    } else {
        this->openai_create_embedding_suffix = model_config["path"].get<std::string>();
    }
}

Option<bool> OpenAIEmbedder::is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims) {
    auto validate_properties = validate_string_properties(model_config, {"model_name", "api_key"});
    
    if (!validate_properties.ok()) {
        return validate_properties;
    }

    const std::string openai_url = model_config.count("url") > 0 ? model_config["url"].get<std::string>() : "https://api.openai.com";
    const std::string openai_path = model_config.count("path") > 0 ? model_config["path"].get<std::string>() : OPENAI_CREATE_EMBEDDING;
    auto model_name = model_config["model_name"].get<std::string>();
    auto api_key = model_config["api_key"].get<std::string>();

    if(EmbedderManager::get_model_namespace(model_name) != "openai") {
        return Option<bool>(400, "Property `embed.model_config.model_name` malformed.");
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    std::string res;

    nlohmann::json req_body;
    req_body["input"] = "typesense";
    // remove "openai/" prefix
    auto model_name_without_namespace = EmbedderManager::get_model_name_without_namespace(model_name);
    req_body["model"] = model_name_without_namespace;

    if(has_custom_dims && model_name != "openai/text-embedding-ada-002") {
        req_body["dimensions"] = num_dims;
    }

    std::string embedding_res;
    headers["Content-Type"] = "application/json";
    
    auto res_code = call_remote_api("POST", OpenAIEmbedder::get_embedding_url_for_config(openai_url, openai_path), req_body.dump(), embedding_res, res_headers, headers);

    if(res_code == 408) {
        return Option<bool>(408, "OpenAI API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(embedding_res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "OpenAI API error: " + embedding_res);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "OpenAI API error: " + embedding_res);
        }
        return Option<bool>(400, "OpenAI API error: " + json_res["error"]["message"].get<std::string>());
    }
    std::vector<float> embedding;
    try {
        embedding = nlohmann::json::parse(embedding_res)["data"][0]["embedding"].get<std::vector<float>>();
    } catch (const std::exception& e) {
        return Option<bool>(400, "Got malformed response from OpenAI API.");
    }
    num_dims = embedding.size();
    return Option<bool>(true);
}

embedding_res_t OpenAIEmbedder::embed_query(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return embed_query(get_embedding_url(), text, remote_embedder_timeout_ms, remote_embedding_num_tries, api_key, num_dims, has_custom_dims, openai_model_path.substr(7), OpenAIEmbedderType::OPENAI);
}

embedding_res_t OpenAIEmbedder::embed_query(const std::string url, const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries, 
                                      const std::string& api_key, const size_t num_dims, const bool has_custom_dims, 
                                      const std::string& model_name, const OpenAIEmbedderType embedder_type) {
    std::string cache_key = text + model_name;
    {
        std::shared_lock<std::shared_mutex> cache_read_lock(RemoteEmbedder::cache_mutex);
        auto key_it = RemoteEmbedder::cache.find(cache_key);
        if(key_it != RemoteEmbedder::cache.end()) {
            return key_it->second;
        }
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    headers["timeout_ms"] = std::to_string(remote_embedder_timeout_ms);
    headers["num_try"] = std::to_string(remote_embedding_num_tries);
    std::string res;
    nlohmann::json req_body;
    req_body["input"] = std::vector<std::string>{text};
    if(has_custom_dims && model_name != "openai/text-embedding-ada-002") {
        req_body["dimensions"] = num_dims;
    }
    // remove "openai/" prefix
    if(embedder_type == OpenAIEmbedderType::OPENAI) {
        req_body["model"] = model_name;
        headers["Authorization"] = "Bearer " + api_key;
    } else if(embedder_type == OpenAIEmbedderType::AZURE_OPENAI) {
        headers["api-key"] = api_key;
    }
    
    auto res_code = call_remote_api("POST", url, req_body.dump(), res, res_headers, headers);
    if (res_code != 200) {
        return embedding_res_t(res_code, get_error_json(req_body, res_code, res, url));
    }
    try {
        embedding_res_t embedding_res = embedding_res_t(nlohmann::json::parse(res)["data"][0]["embedding"].get<std::vector<float>>());
        {
            std::unique_lock<std::shared_mutex> cache_write_lock(RemoteEmbedder::cache_mutex);
            RemoteEmbedder::cache.insert(cache_key, embedding_res);
        }
        return embedding_res;
    } catch (const std::exception& e) {
        return embedding_res_t(500, get_error_json(req_body, res_code, res, url));
    }
}

std::vector<embedding_res_t> OpenAIEmbedder::embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size,
                                                         const size_t remote_embedding_timeout_ms, const size_t remote_embedding_num_tries) {
    // call recursively if inputs larger than remote_embedding_batch_size
    if(inputs.size() > remote_embedding_batch_size) {
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i += remote_embedding_batch_size) {
            auto batch = std::vector<std::string>(inputs.begin() + i, inputs.begin() + std::min(i + remote_embedding_batch_size, inputs.size()));
            auto batch_outputs = embed_documents(batch, remote_embedding_batch_size, remote_embedding_timeout_ms, remote_embedding_num_tries);
            outputs.insert(outputs.end(), batch_outputs.begin(), batch_outputs.end());
        }
        return outputs;
    }

    return embed_documents(get_embedding_url(), inputs, remote_embedding_timeout_ms, 
                       remote_embedding_num_tries, api_key, num_dims, has_custom_dims, openai_model_path.substr(7), 
                       OpenAIEmbedderType::OPENAI);
}

std::vector<embedding_res_t> OpenAIEmbedder::embed_documents(const std::string url, const std::vector<std::string>& inputs, const size_t remote_embedding_timeout_ms, const size_t remote_embedding_num_tries, 
                                                         const std::string& api_key, const size_t num_dims, const bool has_custom_dims, const std::string& model_name,
                                                         const OpenAIEmbedderType embedder_type) {
    nlohmann::json req_body;
    std::unordered_map<std::string, std::string> headers;
    req_body["input"] = inputs;
    if(has_custom_dims && model_name != "openai/text-embedding-ada-002") {
        req_body["dimensions"] = num_dims;
    }
    if(embedder_type == OpenAIEmbedderType::OPENAI) {
        req_body["model"] = model_name;
        headers["Authorization"] = "Bearer " + api_key;
    } else if(embedder_type == OpenAIEmbedderType::AZURE_OPENAI) {
        headers["api-key"] = api_key;
    }

    headers["Content-Type"] = "application/json";
    headers["timeout_ms"] = std::to_string(remote_embedding_timeout_ms);
    headers["num_try"] = std::to_string(remote_embedding_num_tries);
    std::map<std::string, std::string> res_headers;
    std::string res;
    auto res_code = call_remote_api("POST", url, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        std::vector<embedding_res_t> outputs;
        nlohmann::json embedding_res = get_error_json(req_body, res_code, res, url);
        for(size_t i = 0; i < inputs.size(); i++) {
            embedding_res["request"]["body"]["input"][0] = inputs[i];
            outputs.push_back(embedding_res_t(res_code, embedding_res));
        }
        return outputs;
    }

    nlohmann::json res_json;

    try {
        res_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        nlohmann::json embedding_res = get_error_json(req_body, res_code, res, url);
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            embedding_res["request"]["body"]["input"][0] = inputs[i];
            outputs.push_back(embedding_res_t(500, embedding_res));
        }
        return outputs;
    }

    if(res_json.count("data") == 0 || !res_json["data"].is_array() || res_json["data"].size() != inputs.size()) {
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            outputs.push_back(embedding_res_t(500, "Got malformed response from OpenAI API."));
        }
        return outputs;
    }

    std::vector<embedding_res_t> outputs;
    for(auto& data : res_json["data"]) {
        if(data.count("embedding") == 0 || !data["embedding"].is_array() || data["embedding"].size() == 0) {
            outputs.push_back(embedding_res_t(500, "Got malformed response from OpenAI API."));
            continue;
        }
        outputs.push_back(embedding_res_t(data["embedding"].get<std::vector<float>>()));
    }

    return outputs;
}


nlohmann::json OpenAIEmbedder::get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body, const std::string& url) {
    nlohmann::json json_res;
    try {
        json_res = nlohmann::json::parse(res_body);
    } catch (const std::exception& e) {
        json_res = nlohmann::json::object();
        json_res["error"] = "Malformed response from OpenAI API.";
    }
    nlohmann::json embedding_res = nlohmann::json::object();
    embedding_res["response"] = json_res;
    embedding_res["request"] = nlohmann::json::object();
    embedding_res["request"]["url"] = url;
    embedding_res["request"]["method"] = "POST";
    embedding_res["request"]["body"] = req_body;
    if(embedding_res["request"]["body"].count("input") > 0 && embedding_res["request"]["body"]["input"].get<std::vector<std::string>>().size() > 1) {
        auto vec = embedding_res["request"]["body"]["input"].get<std::vector<std::string>>();
        vec.resize(1);
        embedding_res["request"]["body"]["input"] = vec;
    }
    if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
        embedding_res["error"] = "API error: " + json_res["error"]["message"].get<std::string>();
    }
    if(res_code == 408) {
        embedding_res["error"] = "API timeout.";
    }

    return embedding_res;
}

nlohmann::json OpenAIEmbedder::get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) {
    return get_error_json(req_body, res_code, res_body, get_embedding_url());
}

std::string OpenAIEmbedder::get_model_key(const nlohmann::json& model_config, size_t num_dims) {
    std::string key = model_config["model_name"].get<std::string>() + ":" + model_config["api_key"].get<std::string>();
    if(num_dims > 0) {
        key += ":" + std::to_string(num_dims);
    }
    return key;
}

GoogleEmbedder::GoogleEmbedder(const std::string& google_api_key) : google_api_key(google_api_key) {

}

Option<bool> GoogleEmbedder::is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims) {
    auto validate_properties = validate_string_properties(model_config, {"model_name", "api_key"});
    
    if (!validate_properties.ok()) {
        return validate_properties;
    }

    auto model_name = model_config["model_name"].get<std::string>();
    auto api_key = model_config["api_key"].get<std::string>();

    if(EmbedderManager::get_model_namespace(model_name) != "google") {
        return Option<bool>(400, "Property `embed.model_config.model_name` malformed.");
    }

    if(EmbedderManager::get_model_name_without_namespace(model_name) != std::string(SUPPORTED_MODEL)) {
        return Option<bool>(400, "Property `embed.model_config.model_name` is not a supported Google model.");
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    std::string res;
    nlohmann::json req_body;
    req_body["text"] = "test";

    auto res_code = call_remote_api("POST", std::string(GOOGLE_CREATE_EMBEDDING) + api_key, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            json_res = nlohmann::json::object();
            json_res["error"] = "Malformed response from Google API.";
        }
        if(res_code == 408) {
            return Option<bool>(408, "Google API timeout.");
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "Google API error: " + res);
        }
        
        return Option<bool>(400, "Google API error: " + json_res["error"]["message"].get<std::string>());
    }

    try {
        num_dims = nlohmann::json::parse(res)["embedding"]["value"].get<std::vector<float>>().size();
    } catch (const std::exception& e) {
        return Option<bool>(500, "Got malformed response from Google API.");
    }

    return Option<bool>(true);
}

embedding_res_t GoogleEmbedder::embed_query(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    std::string cache_key = text + SUPPORTED_MODEL;
    {
        std::shared_lock<std::shared_mutex> cache_read_lock(RemoteEmbedder::cache_mutex);
        auto key_it = RemoteEmbedder::cache.find(cache_key);
        if(key_it != RemoteEmbedder::cache.end()) {
            return key_it->second;
        }
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    headers["timeout_ms"] = std::to_string(remote_embedder_timeout_ms);
    headers["num_try"] = std::to_string(remote_embedding_num_tries);
    std::string res;
    nlohmann::json req_body;
    req_body["text"] = text;

    auto res_code = call_remote_api("POST", std::string(GOOGLE_CREATE_EMBEDDING) + google_api_key, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        return embedding_res_t(res_code, get_error_json(req_body, res_code, res));
    }

    try {
        auto res_obj = embedding_res_t(nlohmann::json::parse(res)["embedding"]["value"].get<std::vector<float>>());
        {
            std::unique_lock<std::shared_mutex> cache_write_lock(RemoteEmbedder::cache_mutex);
            RemoteEmbedder::cache.insert(cache_key, res_obj);
        }
        return res_obj;
    } catch (const std::exception& e) {
        return embedding_res_t(500, get_error_json(req_body, res_code, res));
    }
}


std::vector<embedding_res_t> GoogleEmbedder::embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size,
                                                         const size_t remote_embedding_timeout_ms, const size_t remote_embedding_num_tries) {
    std::vector<embedding_res_t> outputs;
    bool timeout_prev = false;
    for(auto& input : inputs) {
        auto res = embed_query(input, remote_embedding_timeout_ms, remote_embedding_num_tries);
        if(res.status_code == 408) {
            if(timeout_prev) {
                // fail whole batch if two consecutive timeouts,
                nlohmann::json req_body;
                req_body["text"] = input;
                return std::vector<embedding_res_t>(inputs.size(), embedding_res_t(408, get_error_json(req_body, 408, "")));
            }
            timeout_prev = true;
        }
        timeout_prev = false;
        outputs.push_back(res);
    }

    return outputs;
}

nlohmann::json GoogleEmbedder::get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) {
    nlohmann::json json_res;
    try {
        nlohmann::json json_res = nlohmann::json::parse(res_body);
    } catch (const std::exception& e) {
        json_res = nlohmann::json::object();
        json_res["error"] = "Malformed response from Google API.";
    }
    nlohmann::json embedding_res = nlohmann::json::object();
    embedding_res["response"] = json_res;
    embedding_res["request"] = nlohmann::json::object();
    embedding_res["request"]["url"] = GOOGLE_CREATE_EMBEDDING;
    embedding_res["request"]["method"] = "POST";
    embedding_res["request"]["body"] = req_body;
    if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
        embedding_res["error"] = "Google API error: " + json_res["error"]["message"].get<std::string>();
    }
    if(res_code == 408) {
        embedding_res["error"] = "Google API timeout.";
    }

    return embedding_res;
}

std::string GoogleEmbedder::get_model_key(const nlohmann::json& model_config, size_t num_dims) {
    std::string key = model_config["model_name"].get<std::string>() + ":" + model_config["api_key"].get<std::string>();
    if(num_dims > 0) {
        key += ":" + std::to_string(num_dims);
    }
    return key;
}


GCPEmbedder::GCPEmbedder(const std::string& project_id, const std::string& model_name, const std::string& access_token, 
                         const std::string& refresh_token, const std::string& client_id, const std::string& client_secret, const bool has_custom_dims, const size_t num_dims,
                         const std::string& document_task, const std::string& query_task, const std::string& region) :
        project_id(project_id), access_token(access_token), refresh_token(refresh_token), client_id(client_id), client_secret(client_secret), has_custom_dims(has_custom_dims), num_dims(num_dims),
        document_task(document_task), query_task(query_task), region(region) {
    
    this->model_name = EmbedderManager::get_model_name_without_namespace(model_name);
}

GCPEmbedder::GCPEmbedder(const std::string& project_id, const std::string& model_name, const nlohmann::json& service_account,
                         const bool has_custom_dims, const size_t num_dims, const std::string& document_task, const std::string& query_task, const std::string& region)
        : project_id(project_id), has_custom_dims(has_custom_dims), num_dims(num_dims), document_task(document_task), query_task(query_task), region(region) {
    this->model_name = EmbedderManager::get_model_name_without_namespace(model_name);
    this->auth_type = AuthType::SERVICE_ACCOUNT_KEY;
    if(service_account.count("client_email") > 0 && service_account["client_email"].is_string()) {
        this->sa_client_email = service_account["client_email"].get<std::string>();
    }
    if(service_account.count("private_key") > 0 && service_account["private_key"].is_string()) {
        this->sa_private_key_pem = service_account["private_key"].get<std::string>();
        normalize_pem_newlines(this->sa_private_key_pem);
    }
    if(service_account.count("token_uri") > 0 && service_account["token_uri"].is_string()) {
        this->sa_token_uri = service_account["token_uri"].get<std::string>();
    } else {
        this->sa_token_uri = GCP_AUTH_TOKEN_URL;
    }
}

std::string GCPEmbedder::base64url_encode(const std::string& input) {
    std::string out = StringUtils::base64_encode(input);
    for(char& c : out) {
        if(c == '+') c = '-';
        else if(c == '/') c = '_';
    }
    // strip padding '='
    while(!out.empty() && out.back() == '=') out.pop_back();
    return out;
}

Option<std::string> GCPEmbedder::sign_jwt_rs256(const std::string& message, const std::string& private_key_pem) {
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

Option<std::pair<std::string,long long>> GCPEmbedder::mint_sa_access_token_once(const std::string& client_email, const std::string& private_key_pem, const std::string& token_uri) {
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
    auto res_code = call_remote_api("POST", token_uri, req_body, res, res_headers, headers);
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

Option<std::string> GCPEmbedder::ensure_access_token(bool force_refresh) {
    if (auth_type == AuthType::SERVICE_ACCOUNT_KEY) {
        const long long now = static_cast<long long>(std::time(nullptr));
        if (!force_refresh && !access_token.empty() && now + 60 < token_expires_at_epoch) {
            return Option<std::string>(access_token);
        }
        auto mint_op = mint_sa_access_token_once(sa_client_email, sa_private_key_pem, sa_token_uri);
        if (!mint_op.ok()) return Option<std::string>(mint_op.code(), mint_op.error());
        access_token = mint_op.get().first;
        token_expires_at_epoch = mint_op.get().second;
        return Option<std::string>(access_token);
    }

    if (!force_refresh && !access_token.empty()) return Option<std::string>(access_token);
    auto refresh_op = generate_access_token(refresh_token, client_id, client_secret);
    if (!refresh_op.ok()) return Option<std::string>(refresh_op.code(), refresh_op.error());
    access_token = refresh_op.get();
    return Option<std::string>(access_token);
}
 
Option<bool> GCPEmbedder::is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims)  {
    // service account path
    if(model_config.count("service_account") > 0 && model_config["service_account"].is_object()) {
        auto validate_properties = validate_string_properties(model_config, {"model_name", "project_id"});
        if(!validate_properties.ok()) return validate_properties;
        auto model_name = model_config["model_name"].get<std::string>();
        auto project_id = model_config["project_id"].get<std::string>();
        std::string region = GCP_DEFAULT_REGION;
        if(model_config.count("region") > 0 && model_config["region"].is_string()) region = model_config["region"].get<std::string>();
        if(EmbedderManager::get_model_namespace(model_name) != "gcp") return Option<bool>(400, "Invalid GCP model name");
        auto model_name_without_namespace = EmbedderManager::get_model_name_without_namespace(model_name);
        // Mint token once
        const nlohmann::json& sa = model_config["service_account"];
        if(sa.count("client_email") == 0 || !sa["client_email"].is_string() || sa.count("private_key") == 0 || !sa["private_key"].is_string()) {
            return Option<bool>(400, "Property `embed.model_config.service_account.client_email/private_key` missing or not a string.");
        }
        const std::string client_email = sa["client_email"].get<std::string>();
        std::string private_key = sa["private_key"].get<std::string>();
        normalize_pem_newlines(private_key);
        const std::string token_uri = sa.count("token_uri") > 0 && sa["token_uri"].is_string() ? sa["token_uri"].get<std::string>() : GCP_AUTH_TOKEN_URL;
        auto mint_op = mint_sa_access_token_once(client_email, private_key, token_uri);
        if(!mint_op.ok()) return Option<bool>(mint_op.code(), mint_op.error());
        const std::string access_token_tmp = mint_op.get().first;

        std::unordered_map<std::string, std::string> headers;
        std::map<std::string, std::string> res_headers;
        headers["Content-Type"] = "application/json";
        headers["Authorization"] = "Bearer " + access_token_tmp;
        std::string res;
        nlohmann::json req_body;
        req_body["instances"] = nlohmann::json::array();
        nlohmann::json instance; instance["content"] = "typesense"; req_body["instances"].push_back(instance);
        if(has_custom_dims) { nlohmann::json dimensions; dimensions["outputDimensionality"] = num_dims; req_body["parameters"] = dimensions; }
        auto res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name_without_namespace, region), req_body.dump(), res, res_headers, headers);
        if(res_code != 200) {
            nlohmann::json json_res; try { json_res = nlohmann::json::parse(res); } catch (...) { return Option<bool>(400, "Got malformed response from GCP API."); }
            if(res_code == 408) return Option<bool>(408, "GCP API timeout.");
            if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) return Option<bool>(400, "GCP API error: " + res);
            return Option<bool>(400, "GCP API error: " + json_res["error"]["message"].get<std::string>());
        }
        nlohmann::json res_json; try { res_json = nlohmann::json::parse(res); } catch (...) { return Option<bool>(400, "Got malformed response from GCP API."); }
        if(res_json.count("predictions") == 0 || res_json["predictions"].size() == 0 || res_json["predictions"][0].count("embeddings") == 0) return Option<bool>(400, "GCP API error: Invalid response");
        num_dims = res_json["predictions"][0]["embeddings"]["values"].size();
        return Option<bool>(true);
    }

    auto validate_properties = validate_string_properties(model_config, {"model_name", "project_id", "access_token", "refresh_token", "client_id", "client_secret"});

    if (!validate_properties.ok()) {
        return validate_properties;
    }

    if(model_config.count("document_task") > 0 && !model_config["document_task"].is_string()) {
        return Option<bool>(400, "Property `embed.model_config.document_task` is not a string.");
    }

    if(model_config.count("query_task") > 0 && !model_config["query_task"].is_string()) {
        return Option<bool>(400, "Property `embed.model_config.query_task` is not a string.");
    }

    if(model_config.count("region") > 0 && !model_config["region"].is_string()) {
        return Option<bool>(400, "Property `embed.model_config.region` is not a string.");
    }
    
    auto model_name = model_config["model_name"].get<std::string>();
    auto project_id = model_config["project_id"].get<std::string>();    
    auto access_token = model_config["access_token"].get<std::string>();
    auto refresh_token = model_config["refresh_token"].get<std::string>();
    auto client_id = model_config["client_id"].get<std::string>();
    auto client_secret = model_config["client_secret"].get<std::string>();
    
    std::string region = GCP_DEFAULT_REGION;
    if(model_config.count("region") > 0 && model_config["region"].is_string()) {
        region = model_config["region"].get<std::string>();
    }

    if(EmbedderManager::get_model_namespace(model_name) != "gcp") {
        return Option<bool>(400, "Invalid GCP model name");
    }

    auto model_name_without_namespace = EmbedderManager::get_model_name_without_namespace(model_name);

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    headers["Authorization"] = "Bearer " + access_token;
    std::string res;
    nlohmann::json req_body;
    req_body["instances"] = nlohmann::json::array();
    nlohmann::json instance;
    instance["content"] = "typesense";
    req_body["instances"].push_back(instance);
    if(has_custom_dims) {
        nlohmann::json dimensions;
        dimensions["outputDimensionality"] = num_dims;
        req_body["parameters"] = dimensions;
    }

    auto res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name_without_namespace, region), req_body.dump(), res, res_headers, headers);

    if(res_code == 401) {
        auto refresh_op = generate_access_token(refresh_token, client_id, client_secret);
        if(!refresh_op.ok()) {
            nlohmann::json embedding_res = nlohmann::json::object();
            embedding_res["error"] = refresh_op.error();
            return Option<bool>(400, "Invalid client_id, client_secret or refresh_token in `embed.model config'.");
        }
        access_token = refresh_op.get();
        headers["Authorization"] = "Bearer " + access_token;
        res.clear();
        res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name_without_namespace, region), req_body.dump(), res, res_headers, headers);
    }

    if(res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "Got malformed response from GCP API.");
        }
        if(json_res == 408) {
            return Option<bool>(408, "GCP API timeout.");
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "GCP API error: " + res);
        }
        return Option<bool>(400, "GCP API error: " + json_res["error"]["message"].get<std::string>());
    }
    nlohmann::json res_json;
    try {
        res_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return Option<bool>(400, "Got malformed response from GCP API.");
    }
    if(res_json.count("predictions") == 0 || res_json["predictions"].size() == 0 || res_json["predictions"][0].count("embeddings") == 0) {
        LOG(INFO) << "Invalid response from GCP API: " << res_json.dump();
        return Option<bool>(400, "GCP API error: Invalid response");
    }

    auto generate_access_token_res = generate_access_token(refresh_token, client_id, client_secret);
    if(!generate_access_token_res.ok()) {
        return Option<bool>(400, "Invalid client_id, client_secret or refresh_token in `embed.model config'.");
    }

    num_dims = res_json["predictions"][0]["embeddings"]["values"].size();

    return Option<bool>(true);
}

embedding_res_t GCPEmbedder::embed_query(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
    std::shared_lock<std::shared_mutex> lock(mutex);

    std::string cache_key = text + model_name;
    {
        std::shared_lock<std::shared_mutex> cache_read_lock(RemoteEmbedder::cache_mutex);
        auto key_it = RemoteEmbedder::cache.find(cache_key);
        if(key_it != RemoteEmbedder::cache.end()) {
            return key_it->second;
        }
    }

    nlohmann::json req_body;
    req_body["instances"] = nlohmann::json::array();
    nlohmann::json instance;
    if(!query_task.empty()) {
        instance["task_type"] = query_task;
    }
    instance["content"] = text;
    req_body["instances"].push_back(instance);
    if(has_custom_dims) {
        nlohmann::json dimensions;
        dimensions["outputDimensionality"] = num_dims;
        req_body["parameters"] = dimensions;
    }
    std::unordered_map<std::string, std::string> headers;
    auto token_op = ensure_access_token(false);
    if(!token_op.ok()) {
        return embedding_res_t(token_op.code(), nlohmann::json({{"error", token_op.error()}}));
    }
    headers["Authorization"] = "Bearer " + access_token;
    headers["Content-Type"] = "application/json";
    headers["timeout_ms"] = std::to_string(remote_embedder_timeout_ms);
    headers["num_try"] = std::to_string(remote_embedding_num_tries);
    std::map<std::string, std::string> res_headers;
    std::string res;

    auto res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name, region), req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        if(res_code == 401) {
            auto refresh_op = ensure_access_token(true);
            if(!refresh_op.ok()) {
                nlohmann::json embedding_res = nlohmann::json::object();
                embedding_res["error"] = refresh_op.error();
                return embedding_res_t(refresh_op.code(), embedding_res);
            }
            headers["Authorization"] = "Bearer " + access_token;
            res.clear();
            res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name, region), req_body.dump(), res, res_headers, headers);
        }
    }

    if(res_code != 200) {
        return embedding_res_t(res_code, get_error_json(req_body, res_code, res));
    }
    nlohmann::json res_json;
    try {
        res_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return embedding_res_t(500, get_error_json(req_body, res_code, res));
    }
    auto res_obj = embedding_res_t(res_json["predictions"][0]["embeddings"]["values"].get<std::vector<float>>());

    {
        std::unique_lock<std::shared_mutex> cache_write_lock(RemoteEmbedder::cache_mutex);
        RemoteEmbedder::cache.insert(cache_key, res_obj);
    }

    return res_obj;
}


std::vector<embedding_res_t> GCPEmbedder::embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size,
                                                      const size_t remote_embedding_timeout_ms, const size_t remote_embedding_num_tries) {
    // GCP API has a limit of 5 instances per request
    if(inputs.size() > 5) {
        std::vector<embedding_res_t> res;
        for(size_t i = 0; i < inputs.size(); i += 5) {
            auto batch_res = embed_documents(std::vector<std::string>(inputs.begin() + i, inputs.begin() + std::min(i + 5, inputs.size())));
            res.insert(res.end(), batch_res.begin(), batch_res.end());
        }
        return res;
    }
    nlohmann::json req_body;
    req_body["instances"] = nlohmann::json::array();
    for(const auto& input : inputs) {
        nlohmann::json instance;
        instance["content"] = input;
        if(!document_task.empty()) {
            instance["task_type"] = document_task;
        }
        req_body["instances"].push_back(instance);
    }
    if(has_custom_dims) {
        nlohmann::json dimensions;
        dimensions["outputDimensionality"] = num_dims;
        req_body["parameters"] = dimensions;
    }
    auto token_op = ensure_access_token(false);
    if(!token_op.ok()) {
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            outputs.push_back(embedding_res_t(token_op.code(), nlohmann::json({{"error", token_op.error()}})));
        }
        return outputs;
    }
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = "Bearer " + access_token;
    headers["Content-Type"] = "application/json";
    headers["timeout_ms"] = std::to_string(remote_embedding_timeout_ms);
    headers["num_try"] = std::to_string(remote_embedding_num_tries);
    std::map<std::string, std::string> res_headers;
    std::string res;
    auto res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name, region), req_body.dump(), res, res_headers, headers);
    if(res_code != 200) {
        if(res_code == 401) {
            auto refresh_op = ensure_access_token(true);
            if(!refresh_op.ok()) {
                nlohmann::json embedding_res = nlohmann::json::object();
                embedding_res["error"] = refresh_op.error();
                std::vector<embedding_res_t> outputs;
                for(size_t i = 0; i < inputs.size(); i++) {
                    outputs.push_back(embedding_res_t(refresh_op.code(), embedding_res));
                }
                return outputs;
            }
            headers["Authorization"] = "Bearer " + access_token;
            res.clear();
            res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name, region), req_body.dump(), res, res_headers, headers);
        }
    }

    if(res_code != 200) {
        auto embedding_res = get_error_json(req_body, res_code, res);
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            outputs.push_back(embedding_res_t(res_code, embedding_res));
        }
        return outputs;
    }

    nlohmann::json res_json;
    try {
        res_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        nlohmann::json embedding_res = get_error_json(req_body, res_code, res);
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            outputs.push_back(embedding_res_t(400, embedding_res));
        }
        return outputs;
    }

    if(res_json.count("predictions") == 0 || !res_json["predictions"].is_array() || res_json["predictions"].size() != inputs.size()) {
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            outputs.push_back(embedding_res_t(500, "Got malformed response from GCP API."));
        }
        return outputs;
    }

    std::vector<embedding_res_t> outputs;
    for(const auto& prediction : res_json["predictions"]) {
        if(prediction.count("embeddings") == 0 || !prediction["embeddings"].is_object() || prediction["embeddings"].count("values") == 0 || !prediction["embeddings"]["values"].is_array() || prediction["embeddings"]["values"].size() == 0) {
            outputs.push_back(embedding_res_t(500, "Got malformed response from GCP API."));
            continue;
        }
        outputs.push_back(embedding_res_t(prediction["embeddings"]["values"].get<std::vector<float>>()));
    }

    return outputs;
}


nlohmann::json GCPEmbedder::get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) {
    nlohmann::json json_res;
    try {
        json_res = nlohmann::json::parse(res_body);
    } catch (const std::exception& e) {
        json_res = nlohmann::json::object();
        json_res["error"] = "Malformed response from GCP API.";
    }
    nlohmann::json embedding_res = nlohmann::json::object();
    embedding_res["response"] = json_res;
    embedding_res["request"] = nlohmann::json::object();
    embedding_res["request"]["url"] = get_gcp_embedding_url(project_id, model_name, region);
    embedding_res["request"]["method"] = "POST";
    embedding_res["request"]["body"] = req_body;
    if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
        embedding_res["error"] = "GCP API error: " + json_res["error"]["message"].get<std::string>();
    } else {
        embedding_res["error"] = "Malformed response from GCP API.";
    }

    if(res_code == 408) {
        embedding_res["error"] = "GCP API timeout.";
    }

    return embedding_res;
}

Option<std::string> GCPEmbedder::generate_access_token(const std::string& refresh_token, const std::string& client_id, const std::string& client_secret) {
    std::unordered_map<std::string, std::string> headers;
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    std::map<std::string, std::string> res_headers;
    std::string res;
    std::string req_body;
    req_body = "grant_type=refresh_token&client_id=" + client_id + "&client_secret=" + client_secret + "&refresh_token=" + refresh_token;

    auto res_code = call_remote_api("POST", GCP_AUTH_TOKEN_URL, req_body, res, res_headers, headers);
    
    if(res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "Got malformed response from GCP API.");
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::string>(400, "GCP API error: " + res);
        }
        if(res_code == 408) {
            return Option<std::string>(408, "GCP API timeout.");
        }
        return Option<std::string>(400, "GCP API error: " + json_res["error"]["message"].get<std::string>());
    }
    nlohmann::json res_json;
    try {
        res_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from GCP API.");
    }
    std::string access_token = res_json["access_token"].get<std::string>();

    return Option<std::string>(access_token);
}

std::string GCPEmbedder::get_model_key(const nlohmann::json& model_config, size_t num_dims) {
    const std::string name = model_config["model_name"].get<std::string>();
    const std::string project = model_config["project_id"].get<std::string>();
    std::string key;
    if(model_config.count("service_account") > 0 && model_config["service_account"].is_object()) {
        const auto& sa = model_config["service_account"];
        const std::string email = sa.count("client_email") && sa["client_email"].is_string() ? sa["client_email"].get<std::string>() : std::string("unknown");
        key = name + ":" + project + ":sa:" + email;
    } else {
        key = name + ":" + project + ":" + model_config["client_secret"].get<std::string>();
    }
    if(num_dims > 0) {
        key += ":" + std::to_string(num_dims);
    }
    return key;
}


AzureEmbedder::AzureEmbedder(const std::string& azure_url, const std::string& api_key, const size_t num_dims, const bool has_custom_dims) : 
    azure_url(azure_url), api_key(api_key), num_dims(num_dims), has_custom_dims(has_custom_dims) {
}

Option<bool> AzureEmbedder::is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims) {
    auto validate_properties = validate_string_properties(model_config, {"model_name", "api_key", "url"});
    
    if (!validate_properties.ok()) {
        return validate_properties;
    }

    const std::string azure_url = model_config["url"].get<std::string>();
    auto api_key = model_config["api_key"].get<std::string>();
    const std::string model_name = model_config["model_name"].get<std::string>();

    if(EmbedderManager::get_model_namespace(model_name) != "azure") {
        return Option<bool>(400, "Property `embed.model_config.model_name` malformed.");
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["api-key"] = api_key;
    std::string res;

    nlohmann::json req_body;
    req_body["input"] = "typesense";

    if(has_custom_dims) {
        req_body["dimensions"] = num_dims;
    }

    std::string embedding_res;
    headers["Content-Type"] = "application/json";
    auto res_code = call_remote_api("POST", azure_url, req_body.dump(), embedding_res, res_headers, headers);  


    if(res_code == 408) {
        return Option<bool>(408, "Azure API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(embedding_res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "Azure API error: " + embedding_res);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "Azure API error: " + embedding_res);
        }
        return Option<bool>(400, "Azure API error: " + json_res["error"]["message"].get<std::string>());
    }
    std::vector<float> embedding;
    try {
        embedding = nlohmann::json::parse(embedding_res)["data"][0]["embedding"].get<std::vector<float>>();
    } catch (const std::exception& e) {
        return Option<bool>(400, "Got malformed response from Azure API.");
    }
    num_dims = embedding.size();
    return Option<bool>(true);
}

embedding_res_t AzureEmbedder::embed_query(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
    std::shared_lock<std::shared_mutex> lock(mutex);

    return OpenAIEmbedder::embed_query(azure_url, text, remote_embedder_timeout_ms, remote_embedding_num_tries, api_key, num_dims, has_custom_dims, "", OpenAIEmbedder::OpenAIEmbedderType::AZURE_OPENAI);
}

std::vector<embedding_res_t> AzureEmbedder::embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size,
                                                         const size_t remote_embedding_timeout_ms, const size_t remote_embedding_num_tries) {
    // call recursively if inputs larger than remote_embedding_batch_size
    if(inputs.size() > remote_embedding_batch_size) {
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i += remote_embedding_batch_size) {
            auto batch = std::vector<std::string>(inputs.begin() + i, inputs.begin() + std::min(i + remote_embedding_batch_size, inputs.size()));
            auto batch_outputs = embed_documents(batch, remote_embedding_batch_size, remote_embedding_timeout_ms, remote_embedding_num_tries);
            outputs.insert(outputs.end(), batch_outputs.begin(), batch_outputs.end());
        }
        return outputs;
    }
    
    return OpenAIEmbedder::embed_documents(azure_url, inputs, remote_embedding_timeout_ms, remote_embedding_num_tries, api_key, num_dims, has_custom_dims, "", OpenAIEmbedder::OpenAIEmbedderType::AZURE_OPENAI);
}

nlohmann::json AzureEmbedder::get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) {
    return OpenAIEmbedder::get_error_json(req_body, res_code, res_body, azure_url);
}

std::string AzureEmbedder::get_model_key(const nlohmann::json& model_config, size_t num_dims) {
    std::string key = model_config["model_name"].get<std::string>() + ":" + model_config["api_key"].get<std::string>();
    if(num_dims > 0) {
        key += ":" + std::to_string(num_dims);
    }
    return key;
}