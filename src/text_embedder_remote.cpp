#include <http_proxy.h>
#include "text_embedder_remote.h"
#include "text_embedder_manager.h"


Option<bool> RemoteEmbedder::validate_string_properties(const nlohmann::json& model_config, const std::vector<std::string>& properties) {
    for(auto& property : properties) {
        if(model_config.count(property) == 0 || !model_config[property].is_string()) {
            return Option<bool>(400, "Property `embed.model_config." + property + " is missing or is not a string.");
        }
    }
    return Option<bool>(true);
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


const std::string RemoteEmbedder::get_model_key(const nlohmann::json& model_config) {
    const std::string model_namespace = TextEmbedderManager::get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIEmbedder::get_model_key(model_config);
    } else if(model_namespace == "google") {
        return GoogleEmbedder::get_model_key(model_config);
    } else if(model_namespace == "gcp") {
        return GCPEmbedder::get_model_key(model_config);
    } else {
        return "";
    }
}

OpenAIEmbedder::OpenAIEmbedder(const std::string& openai_model_path, const std::string& api_key) : api_key(api_key), openai_model_path(openai_model_path) {

}

Option<bool> OpenAIEmbedder::is_model_valid(const nlohmann::json& model_config, size_t& num_dims) {
    auto validate_properties = validate_string_properties(model_config, {"model_name", "api_key"});
    
    if (!validate_properties.ok()) {
        return validate_properties;
    }

    auto model_name = model_config["model_name"].get<std::string>();
    auto api_key = model_config["api_key"].get<std::string>();

    if(TextEmbedderManager::get_model_namespace(model_name) != "openai") {
        return Option<bool>(400, "Property `embed.model_config.model_name` malformed.");
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    std::string res;
    auto res_code = call_remote_api("GET", OPENAI_LIST_MODELS, "", res, res_headers, headers);

    if(res_code == 408) {
        return Option<bool>(408, "OpenAI API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "OpenAI API error: " + res);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "OpenAI API error: " + res);
        }
        return Option<bool>(400, "OpenAI API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    nlohmann::json models_json;
    try {
        models_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return Option<bool>(400, "Got malformed response from OpenAI API.");
    }
    bool found = false;
    // extract model name by removing "openai/" prefix
    auto model_name_without_namespace = TextEmbedderManager::get_model_name_without_namespace(model_name);
    for (auto& model : models_json["data"]) {
        if (model["id"] == model_name_without_namespace) {
            found = true;
            break;
        }
    }

    if (!found) {
        return Option<bool>(400, "Property `embed.model_config.model_name` is not a valid OpenAI model.");
    }
    nlohmann::json req_body;
    req_body["input"] = "typesense";
    // remove "openai/" prefix
    req_body["model"] = model_name_without_namespace;

    std::string embedding_res;
    headers["Content-Type"] = "application/json";
    res_code = call_remote_api("POST", OPENAI_CREATE_EMBEDDING, req_body.dump(), embedding_res, res_headers, headers);  


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
        return Option<bool>(400, "OpenAI API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
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

embedding_res_t OpenAIEmbedder::Embed(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    headers["timeout_ms"] = std::to_string(remote_embedder_timeout_ms);
    headers["num_try"] = std::to_string(remote_embedding_num_tries);
    std::string res;
    nlohmann::json req_body;
    req_body["input"] = std::vector<std::string>{text};
    // remove "openai/" prefix
    req_body["model"] = TextEmbedderManager::get_model_name_without_namespace(openai_model_path);
    auto res_code = call_remote_api("POST", OPENAI_CREATE_EMBEDDING, req_body.dump(), res, res_headers, headers);
    if (res_code != 200) {
        return embedding_res_t(res_code, get_error_json(req_body, res_code, res));
    }
    try {
        embedding_res_t embedding_res = embedding_res_t(nlohmann::json::parse(res)["data"][0]["embedding"].get<std::vector<float>>());
        return embedding_res;
    } catch (const std::exception& e) {
        return embedding_res_t(500, get_error_json(req_body, res_code, res));
    }
}

std::vector<embedding_res_t> OpenAIEmbedder::batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size) {
    // call recursively if inputs larger than remote_embedding_batch_size
    if(inputs.size() > remote_embedding_batch_size) {
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i += remote_embedding_batch_size) {
            auto batch = std::vector<std::string>(inputs.begin() + i, inputs.begin() + std::min(i + remote_embedding_batch_size, inputs.size()));
            auto batch_outputs = batch_embed(batch, remote_embedding_batch_size);
            outputs.insert(outputs.end(), batch_outputs.begin(), batch_outputs.end());
        }
        return outputs;
    }
    nlohmann::json req_body;
    req_body["input"] = inputs;
    // remove "openai/" prefix
    req_body["model"] = openai_model_path.substr(7);
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    std::map<std::string, std::string> res_headers;
    std::string res;
    auto res_code = call_remote_api("POST", OPENAI_CREATE_EMBEDDING, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        std::vector<embedding_res_t> outputs;
        nlohmann::json embedding_res = get_error_json(req_body, res_code, res);
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
        nlohmann::json embedding_res = get_error_json(req_body, res_code, res);
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


nlohmann::json OpenAIEmbedder::get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) {
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
    embedding_res["request"]["url"] = OPENAI_CREATE_EMBEDDING;
    embedding_res["request"]["method"] = "POST";
    embedding_res["request"]["body"] = req_body;
    if(embedding_res["request"]["body"].count("input") > 0 && embedding_res["request"]["body"]["input"].get<std::vector<std::string>>().size() > 1) {
        auto vec = embedding_res["request"]["body"]["input"].get<std::vector<std::string>>();
        vec.resize(1);
        embedding_res["request"]["body"]["input"] = vec;
    }
    if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
        embedding_res["error"] = "OpenAI API error: " + json_res["error"]["message"].get<std::string>();
    }
    if(res_code == 408) {
        embedding_res["error"] = "OpenAI API timeout.";
    }

    return embedding_res;
}

std::string OpenAIEmbedder::get_model_key(const nlohmann::json& model_config) {
    return model_config["model_name"].get<std::string>() + ":" + model_config["api_key"].get<std::string>();
}

GoogleEmbedder::GoogleEmbedder(const std::string& google_api_key) : google_api_key(google_api_key) {

}

Option<bool> GoogleEmbedder::is_model_valid(const nlohmann::json& model_config, size_t& num_dims) {
    auto validate_properties = validate_string_properties(model_config, {"model_name", "api_key"});
    
    if (!validate_properties.ok()) {
        return validate_properties;
    }

    auto model_name = model_config["model_name"].get<std::string>();
    auto api_key = model_config["api_key"].get<std::string>();

    if(TextEmbedderManager::get_model_namespace(model_name) != "google") {
        return Option<bool>(400, "Property `embed.model_config.model_name` malformed.");
    }

    if(TextEmbedderManager::get_model_name_without_namespace(model_name) != std::string(SUPPORTED_MODEL)) {
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
        
        return Option<bool>(400, "Google API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    try {
        num_dims = nlohmann::json::parse(res)["embedding"]["value"].get<std::vector<float>>().size();
    } catch (const std::exception& e) {
        return Option<bool>(500, "Got malformed response from Google API.");
    }

    return Option<bool>(true);
}

embedding_res_t GoogleEmbedder::Embed(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
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
        return embedding_res_t(nlohmann::json::parse(res)["embedding"]["value"].get<std::vector<float>>());
    } catch (const std::exception& e) {
        return embedding_res_t(500, get_error_json(req_body, res_code, res));
    }
}


std::vector<embedding_res_t> GoogleEmbedder::batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size) {
    std::vector<embedding_res_t> outputs;
    for(auto& input : inputs) {
        auto res = Embed(input);
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

std::string GoogleEmbedder::get_model_key(const nlohmann::json& model_config) {
    return model_config["model_name"].get<std::string>() + ":" + model_config["api_key"].get<std::string>();
}


GCPEmbedder::GCPEmbedder(const std::string& project_id, const std::string& model_name, const std::string& access_token, 
                         const std::string& refresh_token, const std::string& client_id, const std::string& client_secret) : 
        project_id(project_id), access_token(access_token), refresh_token(refresh_token), client_id(client_id), client_secret(client_secret) {
    
    this->model_name = TextEmbedderManager::get_model_name_without_namespace(model_name);
}

Option<bool> GCPEmbedder::is_model_valid(const nlohmann::json& model_config, size_t& num_dims)  {
    auto validate_properties = validate_string_properties(model_config, {"model_name", "project_id", "access_token", "refresh_token", "client_id", "client_secret"});

    if (!validate_properties.ok()) {
        return validate_properties;
    }
    
    auto model_name = model_config["model_name"].get<std::string>();
    auto project_id = model_config["project_id"].get<std::string>();    
    auto access_token = model_config["access_token"].get<std::string>();
    auto refresh_token = model_config["refresh_token"].get<std::string>();
    auto client_id = model_config["client_id"].get<std::string>();
    auto client_secret = model_config["client_secret"].get<std::string>();

    if(TextEmbedderManager::get_model_namespace(model_name) != "gcp") {
        return Option<bool>(400, "Invalid GCP model name");
    }

    auto model_name_without_namespace = TextEmbedderManager::get_model_name_without_namespace(model_name);

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

    auto res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name_without_namespace), req_body.dump(), res, res_headers, headers);

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
        return Option<bool>(400, "GCP API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
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

embedding_res_t GCPEmbedder::Embed(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
    nlohmann::json req_body;
    req_body["instances"] = nlohmann::json::array();
    nlohmann::json instance;
    instance["content"] = text;
    req_body["instances"].push_back(instance);
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = "Bearer " + access_token;
    headers["Content-Type"] = "application/json";
    headers["timeout_ms"] = std::to_string(remote_embedder_timeout_ms);
    headers["num_try"] = std::to_string(remote_embedding_num_tries);
    std::map<std::string, std::string> res_headers;
    std::string res;

    auto res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name), req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        if(res_code == 401) {
            auto refresh_op = generate_access_token(refresh_token, client_id, client_secret);
            if(!refresh_op.ok()) {
                nlohmann::json embedding_res = nlohmann::json::object();
                embedding_res["error"] = refresh_op.error();
                return embedding_res_t(refresh_op.code(), embedding_res);
            }
            access_token = refresh_op.get();
            // retry
            headers["Authorization"] = "Bearer " + access_token;
            res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name), req_body.dump(), res, res_headers, headers);
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
    return embedding_res_t(res_json["predictions"][0]["embeddings"]["values"].get<std::vector<float>>());
}


std::vector<embedding_res_t> GCPEmbedder::batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size) {
    // GCP API has a limit of 5 instances per request
    if(inputs.size() > 5) {
        std::vector<embedding_res_t> res;
        for(size_t i = 0; i < inputs.size(); i += 5) {
            auto batch_res = batch_embed(std::vector<std::string>(inputs.begin() + i, inputs.begin() + std::min(i + 5, inputs.size())));
            res.insert(res.end(), batch_res.begin(), batch_res.end());
        }
        return res;
    }
    nlohmann::json req_body;
    req_body["instances"] = nlohmann::json::array();
    for(const auto& input : inputs) {
        nlohmann::json instance;
        instance["content"] = input;
        req_body["instances"].push_back(instance);
    }
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = "Bearer " + access_token;
    headers["Content-Type"] = "application/json";
    std::map<std::string, std::string> res_headers;
    std::string res;
    auto res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name), req_body.dump(), res, res_headers, headers);
    if(res_code != 200) {
        if(res_code == 401) {
            auto refresh_op = generate_access_token(refresh_token, client_id, client_secret);
            if(!refresh_op.ok()) {
                nlohmann::json embedding_res = nlohmann::json::object();
                embedding_res["error"] = refresh_op.error();
                std::vector<embedding_res_t> outputs;
                for(size_t i = 0; i < inputs.size(); i++) {
                    outputs.push_back(embedding_res_t(refresh_op.code(), embedding_res));
                }
                return outputs;
            }
            access_token = refresh_op.get();
            // retry
            headers["Authorization"] = "Bearer " + access_token;
            res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name), req_body.dump(), res, res_headers, headers);
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
    std::vector<embedding_res_t> outputs;

    if(res_json.count("predictions") == 0 || !res_json["predictions"].is_array() || res_json["predictions"].size() != inputs.size()) {
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            outputs.push_back(embedding_res_t(500, "Got malformed response from GCP API."));
        }
        return outputs;
    }

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
    embedding_res["request"]["url"] = get_gcp_embedding_url(project_id, model_name);
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
        return Option<std::string>(400, "GCP API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
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

std::string GCPEmbedder::get_model_key(const nlohmann::json& model_config) {
    return model_config["model_name"].get<std::string>() + ":" + model_config["project_id"].get<std::string>() + ":" + model_config["client_secret"].get<std::string>();
}