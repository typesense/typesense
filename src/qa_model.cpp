#include "qa_model.h"
#include "text_embedder_manager.h"
#include "text_embedder_remote.h"


Option<bool> QAModel::validate_model(const nlohmann::json& model_config) {
    const std::string model_namespace = TextEmbedderManager::get_model_namespace(model_config["model_name"].get<std::string>());
    if(model_namespace == "openai") {
        return OpenAIQAModel::validate_model(model_config);
    }

    return Option<bool>(400, "Model namespace `" + model_namespace + "` is not supported.");
}

Option<std::string> QAModel::get_answer(const std::string& context, const std::string& prompt, const nlohmann::json& model_config) {
    const std::string model_namespace = TextEmbedderManager::get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIQAModel::get_answer(context, prompt, model_config);
    }

    throw Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}


Option<bool> OpenAIQAModel::validate_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0) {
        return Option<bool>(400, "API key is not provided");
    }
    
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + model_config["api_key"].get<std::string>();
    headers["Content-Type"] = "application/json";
    std::string res;
    auto res_code = RemoteEmbedder::call_remote_api("GET", OPENAI_LIST_MODELS, "", res, res_headers, headers);

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
    auto model_name_without_namespace = TextEmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    for (auto& model : models_json["data"]) {
        if (model["id"] == model_name_without_namespace) {
            found = true;
            break;
        }
    }

    if(!found) {
        return Option<bool>(400, "Property `qa.model_name` is not a valid OpenAI model.");
    }

    nlohmann::json req_body;
    req_body["model"] = model_name_without_namespace;
    req_body["messages"] = R"([
        {
            "role":"user",
            "content":"hello"
        }
    ])"_json;
    std::string chat_res;

    res_code = RemoteEmbedder::call_remote_api("POST", OPENAI_CHAT_COMPLETION, req_body.dump(), chat_res, res_headers, headers);

    if(res_code == 408) {
        return Option<bool>(408, "OpenAI API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(chat_res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "OpenAI API error: " + chat_res);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<bool>(400, "OpenAI API error: " + chat_res);
        }
        return Option<bool>(400, "OpenAI API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    return Option<bool>(true);
}

Option<std::string> OpenAIQAModel::get_answer(const std::string& context, const std::string& prompt, const nlohmann::json& model_config) {
    const std::string model_name = TextEmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string api_key = model_config["api_key"].get<std::string>();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    nlohmann::json req_body;
    req_body["model"] = model_name;
    req_body["messages"] = nlohmann::json::array();
    static nlohmann::json system_message = nlohmann::json::object();
    system_message["role"] = "system";
    system_message["content"] = "You are an assistant, answer questions according to the data in JSON format. Do not mention the data content in your answer.";
    req_body["messages"].push_back(system_message);
    nlohmann::json message = nlohmann::json::object();
    message["role"] = "user";
    message["content"] = "Data:\n" + context + "\n\nQuestion:\n" + prompt;
    req_body["messages"].push_back(message);

    std::string res;
    auto res_code = RemoteEmbedder::call_remote_api("POST", OPENAI_CHAT_COMPLETION, req_body.dump(), res, res_headers, headers);

    if(res_code == 408) {
        throw Option<std::string>(400, "OpenAI API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            throw Option<std::string>(400, "OpenAI API error: " + res);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            throw Option<std::string>(400, "OpenAI API error: " + res);
        }
        throw Option<std::string>(400, "OpenAI API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    nlohmann::json json_res;
    try {
        json_res = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        throw Option<std::string>(400, "Got malformed response from OpenAI API.");
    }

    return Option<std::string>(json_res["choices"][0]["message"]["content"].get<std::string>());
}