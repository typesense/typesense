#include "qa_model.h"
#include "text_embedder_manager.h"
#include "text_embedder_remote.h"
#include "conversation_manager.h"


Option<bool> QAModel::validate_model(const nlohmann::json& model_config) {
    // check model_name is exists and it is a string
    if(model_config.count("model_name") == 0 || !model_config["model_name"].is_string()) {
        return Option<bool>(400, "Property `qa.model_name` is not provided or not a string.");
    }

    const std::string model_namespace = TextEmbedderManager::get_model_namespace(model_config["model_name"].get<std::string>());
    if(model_namespace == "openai") {
        return OpenAIQAModel::validate_model(model_config);
    }

    return Option<bool>(400, "Model namespace `" + model_namespace + "` is not supported.");
}

Option<std::string> QAModel::get_answer(const std::string& context, const std::string& prompt, 
                                        const std::string& system_prompt, const nlohmann::json& model_config, int& conversation_id) {
    const std::string model_namespace = TextEmbedderManager::get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIQAModel::get_answer(context, prompt, system_prompt, model_config, conversation_id);
    }

    throw Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> QAModel::parse_conversation_history(const nlohmann::json& conversation, const nlohmann::json& model_config) {
    const std::string model_namespace = TextEmbedderManager::get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIQAModel::parse_conversation_history(conversation);
    }

    throw Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}


Option<bool> OpenAIQAModel::validate_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0) {
        return Option<bool>(400, "API key is not provided");
    }

    if(!model_config["api_key"].is_string()) {
        return Option<bool>(400, "API key is not a string");
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

Option<std::string> OpenAIQAModel::get_answer(const std::string& context, const std::string& prompt, 
                                              const std::string& system_prompt, const nlohmann::json& model_config, int& conversation_id) {
    const std::string model_name = TextEmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string api_key = model_config["api_key"].get<std::string>();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    nlohmann::json req_body;
    req_body["model"] = model_name;
    req_body["messages"] = nlohmann::json::array();

    nlohmann::json current_conversation = nlohmann::json::array();
    if(!system_prompt.empty()) {
        nlohmann::json system_message = nlohmann::json::object();
        system_message["role"] = "system";
        system_message["content"] = system_prompt;
        current_conversation.push_back(system_message);
    }

    if(conversation_id < 0) {
        nlohmann::json message = nlohmann::json::object();
        message["role"] = "user";
        message["content"] = "Data:\n" + context + "\n\nQuestion:\n" + prompt;
        current_conversation.push_back(message);
        req_body["messages"] = current_conversation;
    } else {
        auto conversation_history_op = ConversationManager::get_conversation(conversation_id);
        if(!conversation_history_op.ok()) {
            return Option<std::string>(conversation_history_op.code(), conversation_history_op.error());
        }

        auto conversation_history = conversation_history_op.get();
        req_body["messages"] = conversation_history;
        nlohmann::json message = nlohmann::json::object();
        message["role"] = "user";
        message["content"] = prompt;
        if(current_conversation.size() > 0) {
            req_body["messages"].push_back(current_conversation[0]);
        }
        current_conversation.push_back(message);
        req_body["messages"].push_back(message);
    }

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


    current_conversation.push_back(json_res["choices"][0]["message"]);
    if(conversation_id < 0) {
        auto conversation_id_op = ConversationManager::create_conversation(current_conversation);
        if(!conversation_id_op.ok()) {
            return Option<std::string>(conversation_id_op.code(), conversation_id_op.error());
        }

        conversation_id = conversation_id_op.get();
    } else {
        auto append_conversation_op = ConversationManager::append_conversation(conversation_id, current_conversation);
        if(!append_conversation_op.ok()) {
            return Option<std::string>(append_conversation_op.code(), append_conversation_op.error());
        }
    }

    return Option<std::string>(json_res["choices"][0]["message"]["content"].get<std::string>());
}

Option<nlohmann::json> OpenAIQAModel::parse_conversation_history(const nlohmann::json& conversation) {
    if(!conversation.is_array()) {
        return Option<nlohmann::json>(400, "Conversation is not an array");
    }

    nlohmann::json messages = nlohmann::json::array();
    for(auto& message : conversation) {
        if(!message.is_object()) {
            return Option<nlohmann::json>(400, "Message is not an object");
        }

        if(message.count("role") == 0 || !message["role"].is_string()) {
            return Option<nlohmann::json>(400, "Message role is not provided or not a string");
        }

        if(message.count("content") == 0 || !message["content"].is_string()) {
            return Option<nlohmann::json>(400, "Message content is not provided or not a string");
        }

        auto parsed_message = nlohmann::json::object();
        parsed_message[message["role"].get<std::string>()] = message["content"].get<std::string>();
        messages.push_back(parsed_message);
    }



    return Option<nlohmann::json>(messages);
}