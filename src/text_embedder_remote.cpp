#include "text_embedder_remote.h"
#include "text_embedder_manager.h"



OpenAIEmbedder::OpenAIEmbedder(const std::string& openai_model_path, const std::string& api_key) : api_key(api_key), openai_model_path(openai_model_path) {

}


Option<bool> OpenAIEmbedder::is_model_valid(const std::string& model_name, const std::string& api_key, unsigned int& num_dims) {
    if (model_name.empty() || api_key.empty()) {
        return Option<bool>(400, "Invalid OpenAI model name or API key");
    }

    if(TextEmbedderManager::get_model_namespace(model_name) != "openai") {
        return Option<bool>(400, "Invalid OpenAI model name");
    }

    HttpClient& client = HttpClient::get_instance();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    std::string res;
    auto res_code = client.get_response(OPENAI_LIST_MODELS, res, res_headers, headers);
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
        return Option<bool>(400, "OpenAI model not found");
    }

    // This part is hard coded for now. Because OpenAI API does not provide a way to get the output dimensions of the model.
    if(model_name.find("-ada-") != std::string::npos) {
        if(model_name.substr(model_name.length() - 3) == "002") {
            num_dims = 1536;
        } else {
            num_dims = 1024;
        }
    }
    else if(model_name.find("-davinci-") != std::string::npos) {
        num_dims = 12288;
    } else if(model_name.find("-curie-") != std::string::npos) {
        num_dims = 4096;
    } else if(model_name.find("-babbage-") != std::string::npos) {
        num_dims = 2048;
    } else {
        num_dims = 768;
    }

    return Option<bool>(true);
}

Option<std::vector<float>> OpenAIEmbedder::Embed(const std::string& text) {
    HttpClient& client = HttpClient::get_instance();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    std::string res;
    nlohmann::json req_body;
    req_body["input"] = text;
    // remove "openai/" prefix
    req_body["model"] = openai_model_path.substr(7);
    auto res_code = client.post_response(OPENAI_CREATE_EMBEDDING, req_body.dump(), res, res_headers, headers);
    if (res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::vector<float>>(400, "OpenAI API error: " + res);
        }
        return Option<std::vector<float>>(400, "OpenAI API error: " + res);
    }
    return Option<std::vector<float>>(nlohmann::json::parse(res)["data"][0]["embedding"].get<std::vector<float>>());
}

Option<std::vector<std::vector<float>>> OpenAIEmbedder::batch_embed(const std::vector<std::string>& inputs) {
    nlohmann::json req_body;
    req_body["input"] = inputs;
    // remove "openai/" prefix
    req_body["model"] = openai_model_path.substr(7);
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    std::map<std::string, std::string> res_headers;
    std::string res;
    HttpClient& client = HttpClient::get_instance();

    auto res_code = client.post_response(OPENAI_CREATE_EMBEDDING, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::vector<std::vector<float>>>(400, "OpenAI API error: " + res);
        }
        return Option<std::vector<std::vector<float>>>(400, res);
    }

    nlohmann::json res_json = nlohmann::json::parse(res);
    std::vector<std::vector<float>> outputs;
    for(auto& data : res_json["data"]) {
        outputs.push_back(data["embedding"].get<std::vector<float>>());
    }

    return Option<std::vector<std::vector<float>>>(outputs);
}


GoogleEmbedder::GoogleEmbedder(const std::string& google_api_key) : google_api_key(google_api_key) {

}

Option<bool> GoogleEmbedder::is_model_valid(const std::string& model_name, const std::string& api_key, unsigned int& num_dims) {
    if(model_name.empty() || api_key.empty()) {
        return Option<bool>(400, "Invalid Google model name or API key");
    }

    if(TextEmbedderManager::get_model_namespace(model_name) != "google") {
        return Option<bool>(400, "Invalid Google model name");
    }

    if(TextEmbedderManager::get_model_name_without_namespace(model_name) != std::string(SUPPORTED_MODEL)) {
        return Option<bool>(400, "Invalid Google model name");
    }

    HttpClient& client = HttpClient::get_instance();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    std::string res;
    nlohmann::json req_body;
    req_body["text"] = "test";

    auto res_code = client.post_response(std::string(GOOGLE_CREATE_EMBEDDING) + api_key, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "Google API error: " + res);
        }
        return Option<bool>(400, "Google API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    num_dims = GOOGLE_EMBEDDING_DIM;

    return Option<bool>(true);
}

Option<std::vector<float>> GoogleEmbedder::Embed(const std::string& text) {
    HttpClient& client = HttpClient::get_instance();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    std::string res;
    nlohmann::json req_body;
    req_body["text"] = text;

    auto res_code = client.post_response(std::string(GOOGLE_CREATE_EMBEDDING) + google_api_key, req_body.dump(), res, res_headers, headers);

    if(res_code != 200) {
        nlohmann::json json_res = nlohmann::json::parse(res);
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::vector<float>>(400, "Google API error: " + res);
        }
        return Option<std::vector<float>>(400, "Google API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    return Option<std::vector<float>>(nlohmann::json::parse(res)["embedding"]["value"].get<std::vector<float>>());
}


Option<std::vector<std::vector<float>>> GoogleEmbedder::batch_embed(const std::vector<std::string>& inputs) {
    std::vector<std::vector<float>> outputs;
    for(auto& input : inputs) {
        auto res = Embed(input);
        if(!res.ok()) {
            return Option<std::vector<std::vector<float>>>(res.code(), res.error());
        }
        outputs.push_back(res.get());
    }

    return Option<std::vector<std::vector<float>>>(outputs);
}
