#include "conversation_model.h"
#include "embedder_manager.h"
#include "text_embedder_remote.h"
#include "conversation_manager.h"


const std::string get_model_namespace(const std::string& model_name) {
    if(model_name.find("/") != std::string::npos) {
        return model_name.substr(0, model_name.find("/"));
    } else {
        return "";
    }
}

Option<bool> ConversationModel::validate_model(const nlohmann::json& model_config) {
    // check model_name is exists and it is a string
    if(model_config.count("model_name") == 0 || !model_config["model_name"].is_string()) {
        return Option<bool>(400, "Property `model_name` is not provided or not a string.");
    }

    if(model_config.count("system_prompt") != 0 && !model_config["system_prompt"].is_string()) {
        return Option<bool>(400, "Property `system_prompt` is not a string.");
    }

    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    if(model_namespace == "openai") {
        return OpenAIConversationModel::validate_model(model_config);
    }

    return Option<bool>(400, "Model namespace `" + model_namespace + "` is not supported.");
}

Option<std::string> ConversationModel::get_answer(const std::string& context, const std::string& prompt, const nlohmann::json& model_config) {
    

    const std::string& model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    std::string system_prompt = "";
    if(model_config.count("system_prompt") != 0 && model_config["system_prompt"].is_string()) {
        system_prompt = model_config["system_prompt"].get<std::string>();
    }

    if(model_namespace == "openai") {
        return OpenAIConversationModel::get_answer(context, prompt, system_prompt, model_config);
    }

    throw Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> ConversationModel::parse_conversation_history(const nlohmann::json& conversation, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::parse_conversation_history(conversation);
    }

    throw Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<std::string> ConversationModel::get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::get_standalone_question(conversation_history, question, model_config);
    }

    throw Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> ConversationModel::format_question(const std::string& message, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::format_question(message);
    }

    throw Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> ConversationModel::format_answer(const std::string& message, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::format_answer(message);
    }

    throw Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}


Option<bool> OpenAIConversationModel::validate_model(const nlohmann::json& model_config) {
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
    auto model_name_without_namespace = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    for (auto& model : models_json["data"]) {
        if (model["id"] == model_name_without_namespace) {
            found = true;
            break;
        }
    }

    if(!found) {
        return Option<bool>(400, "Property `model_name` is not a valid OpenAI model.");
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

Option<std::string> OpenAIConversationModel::get_answer(const std::string& context, const std::string& prompt, 
                                              const std::string& system_prompt, const nlohmann::json& model_config) {
    const std::string model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string api_key = model_config["api_key"].get<std::string>();

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    nlohmann::json req_body;
    req_body["model"] = model_name;
    req_body["messages"] = nlohmann::json::array();

    if(!system_prompt.empty()) {
        nlohmann::json system_message = nlohmann::json::object();
        system_message["role"] = "system";
        system_message["content"] = system_prompt;
        req_body["messages"].push_back(system_message);
    }

    nlohmann::json message = nlohmann::json::object();
    message["role"] = "user";
    message["content"] = "<Data>\n" + context + "\n\n<Question>\n" + prompt + "\n\n<Answer>";
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

Option<nlohmann::json> OpenAIConversationModel::parse_conversation_history(const nlohmann::json& conversation) {
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

Option<std::string> OpenAIConversationModel::get_standalone_question(const nlohmann::json& conversation_history, 
                                                           const std::string& question, const nlohmann::json& model_config) {
    const std::string model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string api_key = model_config["api_key"].get<std::string>();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    nlohmann::json req_body;
    req_body["model"] = model_name;
    req_body["messages"] = nlohmann::json::array();
    std::string res;
    
    std::string standalone_question = STANDALONE_QUESTION_PROMPT;

    standalone_question += "\n\n<Conversation history>\n";
    for(auto& message : conversation_history["conversation"]) {
        if(message.count("user") == 0 && message.count("assistant") == 0) {
            return Option<std::string>(400, "Conversation history is not valid");
        }

        standalone_question += message.dump(0) + "\n";
    }

    standalone_question += "\n\n<Question>\n" + question;
    standalone_question += "\n\n<Standalone question>\n";

    nlohmann::json message = nlohmann::json::object();
    message["role"] = "user";
    message["content"] = standalone_question;

    req_body["messages"].push_back(message);

    auto res_code = RemoteEmbedder::call_remote_api("POST", OPENAI_CHAT_COMPLETION, req_body.dump(), res, res_headers, headers);

    if(res_code == 408) {
        return Option<std::string>(400, "OpenAI API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "OpenAI API error: " + res);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::string>(400, "OpenAI API error: " + res);
        }
        return Option<std::string>(400, "OpenAI API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    nlohmann::json json_res;
    try {
        json_res = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from OpenAI API.");
    }

    return Option<std::string>(json_res["choices"][0]["message"]["content"].get<std::string>());
}

Option<nlohmann::json> OpenAIConversationModel::format_question(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["user"] = message;
    return Option<nlohmann::json>(json);
}

Option<nlohmann::json> OpenAIConversationModel::format_answer(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["assistant"] = message;
    return Option<nlohmann::json>(json);
}

