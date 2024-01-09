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
    } else if(model_namespace == "cf") {
        return CFConversationModel::validate_model(model_config);
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
    } else if(model_namespace == "cf") {
        return CFConversationModel::get_answer(context, prompt, system_prompt, model_config);
    }

    throw Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<std::string> ConversationModel::get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::get_standalone_question(conversation_history, question, model_config);
    } else if(model_namespace == "cf") {
        return CFConversationModel::get_standalone_question(conversation_history, question, model_config);
    }

    throw Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> ConversationModel::format_question(const std::string& message, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::format_question(message);
    } else if(model_namespace == "cf") {
        return CFConversationModel::format_question(message);
    }

    throw Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> ConversationModel::format_answer(const std::string& message, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::format_answer(message);
    } else if(model_namespace == "cf") {
        return CFConversationModel::format_answer(message);
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

const std::string CFConversationModel::get_model_url(const std::string& model_name, const std::string& account_id) {
    return "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/ai/run/@cf/" + model_name;
}

Option<bool> CFConversationModel::validate_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0) {
        return Option<bool>(400, "API key is not provided");
    }

    if(!model_config["api_key"].is_string()) {
        return Option<bool>(400, "API key is not a string");
    }

    if(model_config.count("account_id") == 0) {
        return Option<bool>(400, "Account ID is not provided");
    }

    if(!model_config["account_id"].is_string()) {
        return Option<bool>(400, "Account ID is not a string");
    }

    auto model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());

    bool found = false;

    for(auto& cf_model_name : CF_MODEL_NAMES) {
        if(cf_model_name == model_name) {
            found = true;
            break;
        }
    }

    if(!found) {
        return Option<bool>(400, "Model name is not a valid Cloudflare model");
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + model_config["api_key"].get<std::string>();
    headers["Content-Type"] = "application/json";
    std::string res;
    auto url = get_model_url(model_name, model_config["account_id"].get<std::string>());
    nlohmann::json req_body;
    req_body["messages"] = R"([
        {
            "role":"user",
            "content":"hello"
        }
    ])"_json;
    std::string chat_res;

    auto res_code = RemoteEmbedder::call_remote_api("POST", url, req_body.dump(), chat_res, res_headers, headers);

    if(res_code == 408) {
        return Option<bool>(408, "Cloudflare API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(chat_res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "Cloudflare API error: " + chat_res);
        }

        if(json_res.count("errors") == 0 || json_res["errors"].size() == 0) {
            return Option<bool>(400, "Cloudflare API error: " + chat_res);
        }

        json_res = json_res["errors"][0];
        return Option<bool>(400, "Cloudflare API error: " + json_res["message"].get<std::string>());
    }

    return Option<bool>(true);
}

Option<std::string> CFConversationModel::get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config) {
    const std::string model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string api_key = model_config["api_key"].get<std::string>();
    const std::string account_id = model_config["account_id"].get<std::string>();

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    nlohmann::json req_body;
    req_body["stream"] = true;
    req_body["messages"] = nlohmann::json::array();

    if(!system_prompt.empty()) {
        nlohmann::json system_message = nlohmann::json::object();
        system_message["role"] = "system";
        system_message["content"] = system_prompt;
        req_body["messages"].push_back(system_message);
    }

    nlohmann::json message = nlohmann::json::object();
    message["role"] = "system";
    message["content"] = INFO_PROMPT;
    req_body["messages"].push_back(message);
    message["role"] = "user";
    message["content"] = "[INST]Context:\n" + context + "\n\nQuestion:\n" + prompt + "\n\nAnswer:[/INST]";
    req_body["messages"].push_back(message);

    std::string res;
    auto url = get_model_url(model_name, account_id);
    auto res_code = RemoteEmbedder::call_remote_api("POST_STREAM", url, req_body.dump(), res, res_headers, headers);

    if(res_code == 408) {
        return Option<std::string>(400, "Cloudflare API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
            json_res = nlohmann::json::parse(json_res["response"].get<std::string>());
        } catch (const std::exception& e) {
            throw Option<std::string>(400, "Cloudflare API error: " + res);
        }

        if(json_res.count("errors") == 0 || json_res["errors"].size() == 0) {
            return Option<std::string>(400, "Cloudflare API error: " + json_res.dump(0));
        }

        json_res = json_res["errors"][0];
        return Option<std::string>(400, "Cloudflare API error: " + json_res["message"].get<std::string>());
    }
    try {
        auto json_res = nlohmann::json::parse(res);
        std::string parsed_response = "";
        std::vector<std::string> lines = json_res["response"].get<std::vector<std::string>>();
        for(auto& line : lines) {
            while(line.find("data:") != std::string::npos) {
                auto substr_line = line.substr(line.find("data:") + 6);
                if(substr_line.find("[DONE]") != std::string::npos) {
                    break;
                }
                nlohmann::json json_line;
                if(substr_line.find("\n") != std::string::npos) {
                   json_line = nlohmann::json::parse(substr_line.substr(0, substr_line.find("\n")));
                } else {
                    json_line = nlohmann::json::parse(substr_line);
                }
                parsed_response += json_line["response"];
                line = substr_line;
            }
        }
        return Option<std::string>(parsed_response);
    } catch (const std::exception& e) {
        LOG(ERROR) << e.what();
        return Option<std::string>(400, "Got malformed response from Cloudflare API.");
    }
}

Option<std::string> CFConversationModel::get_standalone_question(const nlohmann::json& conversation_history, 
                                                           const std::string& question, const nlohmann::json& model_config) {
    const std::string model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string api_key = model_config["api_key"].get<std::string>();
    const std::string account_id = model_config["account_id"].get<std::string>();

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + api_key;
    headers["Content-Type"] = "application/json";
    nlohmann::json req_body;
    req_body["stream"] = true;
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

    auto url = get_model_url(model_name, account_id);

    auto res_code = RemoteEmbedder::call_remote_api("POST_STREAM", url, req_body.dump(), res, res_headers, headers);

    if(res_code == 408) {
        return Option<std::string>(400, "OpenAI API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
            json_res = nlohmann::json::parse(json_res["response"].get<std::string>());
        } catch (const std::exception& e) {
            return Option<std::string>(400, "Cloudflare API error: " + res);
        }
        if(json_res.count("errors") == 0 || json_res["errors"].size() == 0) {
            return Option<std::string>(400, "Cloudflare API error: " + json_res.dump(0));
        }

        json_res = json_res["errors"][0];
        return Option<std::string>(400, "Cloudflare API error: " + json_res["message"].get<std::string>());
    }
    
    try {
        auto json_res = nlohmann::json::parse(res);
        std::string parsed_response = "";
        std::vector<std::string> lines = json_res["response"].get<std::vector<std::string>>();
        for(auto& line : lines) {
            while(line.find("data:") != std::string::npos) {
                auto substr_line = line.substr(line.find("data:") + 6);
                if(substr_line.find("[DONE]") != std::string::npos) {
                    break;
                }
                nlohmann::json json_line;
                if(substr_line.find("\n") != std::string::npos) {
                   json_line = nlohmann::json::parse(substr_line.substr(0, substr_line.find("\n")));
                } else {
                    json_line = nlohmann::json::parse(substr_line);
                }
                parsed_response += json_line["response"];
                line = substr_line;
            }
        }
        return Option<std::string>(parsed_response);
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from Cloudflare API.");
    }
}

Option<nlohmann::json> CFConversationModel::format_question(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["user"] = message;
    return Option<nlohmann::json>(json);
}

Option<nlohmann::json> CFConversationModel::format_answer(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["assistant"] = message;
    return Option<nlohmann::json>(json);
}