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

long RemoteEmbedder::call_remote_api(const std::string& method, const std::string& url, const std::string& body, std::string& res_body, 
                            std::map<std::string, std::string>& headers, const std::unordered_map<std::string, std::string>& req_headers) {
    if(raft_server == nullptr || raft_server->get_leader_url().empty()) {
        if(method == "GET") {
            return HttpClient::get_instance().get_response(url, res_body, headers, req_headers, 100000, true);
        } else if(method == "POST") {
            return HttpClient::get_instance().post_response(url, body, res_body, headers, req_headers, 100000, true);
        } else {
            return 400;
        }
    }

    auto leader_url = raft_server->get_leader_url();
    leader_url += "proxy";
    nlohmann::json req_body;
    req_body["method"] = method;
    req_body["url"] = url;
    req_body["body"] = body;
    req_body["headers"] = req_headers;
    return HttpClient::get_instance().post_response(leader_url, req_body.dump(), res_body, headers, {}, 10000, true);
}



OpenAIEmbedder::OpenAIEmbedder(const std::string& openai_model_path, const std::string& api_key) : api_key(api_key), openai_model_path(openai_model_path) {

}


Option<bool> OpenAIEmbedder::is_model_valid(const nlohmann::json& model_config, unsigned int& num_dims) {
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
    if (res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "OpenAI API error: " + res);
        }
        return Option<bool>(400, "OpenAI API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    auto models_json = nlohmann::json::parse(res);
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


    if (res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(embedding_res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "OpenAI API error: " + embedding_res);
        }
        return Option<bool>(400, "OpenAI API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    auto embedding = nlohmann::json::parse(embedding_res)["data"][0]["embedding"].get<std::vector<float>>();
    num_dims = embedding.size();
    return Option<bool>(true);
}

embedding_res_t OpenAIEmbedder::Embed(const std::string& text) {
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    std::string res;
    nlohmann::json req_body;
    req_body["input"] = text;
    // remove "openai/" prefix
    req_body["model"] = TextEmbedderManager::get_model_name_without_namespace(openai_model_path);
    auto res_code = call_remote_api("POST", OPENAI_CREATE_EMBEDDING, req_body.dump(), res, res_headers, headers);
    if (res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        nlohmann::json embedding_res = nlohmann::json::object();
        embedding_res["response"] = json_res;
        embedding_res["request"] = nlohmann::json::object();
        embedding_res["request"]["url"] = OPENAI_CREATE_EMBEDDING;
        embedding_res["request"]["method"] = "POST";
        embedding_res["request"]["body"] = req_body;

        if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
            embedding_res["error"] = "OpenAI API error: " + json_res["error"]["message"].get<std::string>();
        }
        return embedding_res_t(res_code, embedding_res);
    }

    return embedding_res_t(nlohmann::json::parse(res)["data"][0]["embedding"].get<std::vector<float>>());
}

std::vector<embedding_res_t> OpenAIEmbedder::batch_embed(const std::vector<std::string>& inputs) {
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

        nlohmann::json json_res = nlohmann::json::parse(res);
        LOG(INFO) << "OpenAI API error: " << json_res.dump();
        nlohmann::json embedding_res = nlohmann::json::object();
        embedding_res["response"] = json_res;
        embedding_res["request"] = nlohmann::json::object();
        embedding_res["request"]["url"] = OPENAI_CREATE_EMBEDDING;
        embedding_res["request"]["method"] = "POST";
        embedding_res["request"]["body"] = req_body;
        embedding_res["request"]["body"]["input"] = std::vector<std::string>{inputs[0]};
        if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
            embedding_res["error"] = "OpenAI API error: " + json_res["error"]["message"].get<std::string>();
        }

        for(size_t i = 0; i < inputs.size(); i++) {
            embedding_res["request"]["body"]["input"][0] = inputs[i];
            outputs.push_back(embedding_res_t(res_code, embedding_res));
        }
        return outputs;
    }

    nlohmann::json res_json = nlohmann::json::parse(res);
    std::vector<embedding_res_t> outputs;
    for(auto& data : res_json["data"]) {
        outputs.push_back(embedding_res_t(data["embedding"].get<std::vector<float>>()));
    }

    return outputs;
}


GoogleEmbedder::GoogleEmbedder(const std::string& google_api_key) : google_api_key(google_api_key) {

}

Option<bool> GoogleEmbedder::is_model_valid(const nlohmann::json& model_config, unsigned int& num_dims) {
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
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "Google API error: " + res);
        }
        return Option<bool>(400, "Google API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    num_dims = nlohmann::json::parse(res)["embedding"]["value"].get<std::vector<float>>().size();

    return Option<bool>(true);
}

embedding_res_t GoogleEmbedder::Embed(const std::string& text) {
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    std::string res;
    nlohmann::json req_body;
    req_body["text"] = text;

    auto res_code = call_remote_api("POST", std::string(GOOGLE_CREATE_EMBEDDING) + google_api_key, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        nlohmann::json embedding_res = nlohmann::json::object();
        embedding_res["response"] = json_res;
        embedding_res["request"] = nlohmann::json::object();
        embedding_res["request"]["url"] = GOOGLE_CREATE_EMBEDDING;
        embedding_res["request"]["method"] = "POST";
        embedding_res["request"]["body"] = req_body;
        if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
            embedding_res["error"] = "Google API error: " + json_res["error"]["message"].get<std::string>();
        }
        return embedding_res_t(res_code, embedding_res);
    }

    return embedding_res_t(nlohmann::json::parse(res)["embedding"]["value"].get<std::vector<float>>());
}


std::vector<embedding_res_t> GoogleEmbedder::batch_embed(const std::vector<std::string>& inputs) {
    std::vector<embedding_res_t> outputs;
    for(auto& input : inputs) {
        auto res = Embed(input);
        outputs.push_back(res);
    }

    return outputs;
}


GCPEmbedder::GCPEmbedder(const std::string& project_id, const std::string& model_name, const std::string& access_token, 
                         const std::string& refresh_token, const std::string& client_id, const std::string& client_secret) : 
        project_id(project_id), access_token(access_token), refresh_token(refresh_token), client_id(client_id), client_secret(client_secret) {
    
    this->model_name = TextEmbedderManager::get_model_name_without_namespace(model_name);
}

Option<bool> GCPEmbedder::is_model_valid(const nlohmann::json& model_config, unsigned int& num_dims)  {
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
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "GCP API error: " + res);
        }
        return Option<bool>(400, "GCP API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    auto res_json = nlohmann::json::parse(res);
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

embedding_res_t GCPEmbedder::Embed(const std::string& text) {
    nlohmann::json req_body;
    req_body["instances"] = nlohmann::json::array();
    nlohmann::json instance;
    instance["content"] = text;
    req_body["instances"].push_back(instance);
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
                return embedding_res_t(refresh_op.code(), embedding_res);
            }
            access_token = refresh_op.get();
            // retry
            headers["Authorization"] = "Bearer " + access_token;
            res_code = call_remote_api("POST", get_gcp_embedding_url(project_id, model_name), req_body.dump(), res, res_headers, headers);
        }
    }

    if(res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        nlohmann::json embedding_res = nlohmann::json::object();
        embedding_res["response"] = json_res;
        embedding_res["request"] = nlohmann::json::object();
        embedding_res["request"]["url"] = get_gcp_embedding_url(project_id, model_name);
        embedding_res["request"]["method"] = "POST";
        embedding_res["request"]["body"] = req_body;
        if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
            embedding_res["error"] = "GCP API error: " + json_res["error"]["message"].get<std::string>();
        }
        return embedding_res_t(res_code, embedding_res);
    }

    nlohmann::json res_json = nlohmann::json::parse(res);
    return embedding_res_t(res_json["predictions"][0]["embeddings"]["values"].get<std::vector<float>>());
}


std::vector<embedding_res_t> GCPEmbedder::batch_embed(const std::vector<std::string>& inputs) {
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
        nlohmann::json json_res = nlohmann::json::parse(res);
        nlohmann::json embedding_res = nlohmann::json::object();
        embedding_res["response"] = json_res;
        embedding_res["request"] = nlohmann::json::object();
        embedding_res["request"]["url"] = get_gcp_embedding_url(project_id, model_name);
        embedding_res["request"]["method"] = "POST";
        embedding_res["request"]["body"] = req_body;
        if(json_res.count("error") != 0 && json_res["error"].count("message") != 0) {
            embedding_res["error"] = "GCP API error: " + json_res["error"]["message"].get<std::string>();
        }
        std::vector<embedding_res_t> outputs;
        for(size_t i = 0; i < inputs.size(); i++) {
            outputs.push_back(embedding_res_t(res_code, embedding_res));
        }
        return outputs;
    }

    nlohmann::json res_json = nlohmann::json::parse(res);
    std::vector<embedding_res_t> outputs;
    for(const auto& prediction : res_json["predictions"]) {
        outputs.push_back(embedding_res_t(prediction["embeddings"]["values"].get<std::vector<float>>()));
    }

    return outputs;
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
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::string>(400, "GCP API error: " + res);
        }
        return Option<std::string>(400, "GCP API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    nlohmann::json res_json = nlohmann::json::parse(res);
    std::string access_token = res_json["access_token"].get<std::string>();

    return Option<std::string>(access_token);
}


