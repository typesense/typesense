#include <regex>
#include <iterator>
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

    if(model_config.count("max_bytes") == 0 || !model_config["max_bytes"].is_number_unsigned() || model_config["max_bytes"].get<size_t>() == 0) {
        return Option<bool>(400, "Property `max_bytes` is not provided or not a positive integer.");
    }

    if(model_config.count("history_collection") == 0 || !model_config["history_collection"].is_string()) {
        return Option<bool>(400, "Property `history_collection` is not provided or not a string.");
    }

    auto validate_converson_collection_op = ConversationManager::get_instance().validate_conversation_store_collection(model_config["history_collection"].get<std::string>());
    if(!validate_converson_collection_op.ok()) {
        return Option<bool>(400, validate_converson_collection_op.error());
    }

    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    if(model_namespace == "openai") {
        return OpenAIConversationModel::validate_model(model_config);
    } else if(model_namespace == "cloudflare") {
        return CFConversationModel::validate_model(model_config);
    } else if(model_namespace == "vllm") {
        return vLLMConversationModel::validate_model(model_config);
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
    } else if(model_namespace == "cloudflare") {
        return CFConversationModel::get_answer(context, prompt, system_prompt, model_config);
    } else if(model_namespace == "vllm") {
        return vLLMConversationModel::get_answer(context, prompt, system_prompt, model_config);
    }

    return Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<std::string> ConversationModel::get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::get_standalone_question(conversation_history, question, model_config);
    } else if(model_namespace == "cloudflare") {
        return CFConversationModel::get_standalone_question(conversation_history, question, model_config);
    } else if(model_namespace == "vllm") {
        return vLLMConversationModel::get_standalone_question(conversation_history, question, model_config);
    }

    return Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> ConversationModel::format_question(const std::string& message, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::format_question(message);
    } else if(model_namespace == "cloudflare") {
        return CFConversationModel::format_question(message);
    } else if(model_namespace == "vllm") {
        return vLLMConversationModel::format_question(message);
    }

    return Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<nlohmann::json> ConversationModel::format_answer(const std::string& message, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::format_answer(message);
    } else if(model_namespace == "cloudflare") {
        return CFConversationModel::format_answer(message);
    } else if(model_namespace == "vllm") {
        return vLLMConversationModel::format_answer(message);
    }

    return Option<nlohmann::json>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<size_t> ConversationModel::get_minimum_required_bytes(const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return Option<size_t>(OpenAIConversationModel::get_minimum_required_bytes());
    } else if(model_namespace == "cloudflare") {
        return Option<size_t>(CFConversationModel::get_minimum_required_bytes());
    } else if(model_namespace == "vllm") {
        return Option<size_t>(vLLMConversationModel::get_minimum_required_bytes());
    }

    return Option<size_t>(400, "Model namespace " + model_namespace + " is not supported.");
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
    message["content"] = DATA_STR + context + QUESTION_STR + prompt + ANSWER_STR;
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

        if(json_res.count("choices") == 0 || json_res["choices"].size() == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }

        if(json_res["choices"][0].count("message") == 0 || json_res["choices"][0]["message"].count("content") == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }
    } catch (const std::exception& e) {
        throw Option<std::string>(400, "Got malformed response from OpenAI API.");
    }

    return Option<std::string>(json_res["choices"][0]["message"]["content"].get<std::string>());
}

Option<std::string> OpenAIConversationModel::get_standalone_question(const nlohmann::json& conversation_history, 
                                                           const std::string& question, const nlohmann::json& model_config) {
    const size_t min_required_bytes = CONVERSATION_HISTORY.size() + QUESTION.size() + STANDALONE_QUESTION_PROMPT.size() + question.size();
    if(model_config["max_bytes"].get<size_t>() < min_required_bytes) {
        return Option<std::string>(400, "Max bytes is not enough to generate standalone question.");
    }

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
    auto conversation = conversation_history["conversation"];
    auto max_conversation_length = model_config["max_bytes"].get<size_t>() - min_required_bytes;
    auto truncate_conversation_op = ConversationManager::get_instance().truncate_conversation(conversation, max_conversation_length);
    if(!truncate_conversation_op.ok()) {
        return Option<std::string>(400, truncate_conversation_op.error());
    }

    auto truncated_conversation = truncate_conversation_op.get();
    
    for(auto& message : truncated_conversation) {
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
        if(json_res.count("choices") == 0 || json_res["choices"].size() == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }

        if(json_res["choices"][0].count("message") == 0 || json_res["choices"][0]["message"].count("content") == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }
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
    return "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/ai/run/" + model_name;
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
    message["role"] = "user";
    message["content"] = CONTEXT_INFO + SPLITTER_STR + context + QUERY_STR + prompt + ANSWER_STR;
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
            if(json_res.count("response") == 0 || json_res["response"].size() == 0) {
                return Option<std::string>(400, "Cloudflare API error: " + res);
            }
            json_res = nlohmann::json::parse(json_res["response"][0].get<std::string>());
        } catch (const std::exception& e) {
            throw Option<std::string>(400, "Cloudflare API error: " + res);
        }

        if(json_res.count("errors") == 0 || json_res["errors"].size() == 0) {
            return Option<std::string>(400, "Cloudflare API error: " + json_res.dump(0));
        }

        json_res = json_res["errors"][0];
        return Option<std::string>(400, "Cloudflare API error: " + json_res["message"].get<std::string>());
    }

    return parse_stream_response(res);
}

Option<std::string> CFConversationModel::get_standalone_question(const nlohmann::json& conversation_history, 
                                                           const std::string& question, const nlohmann::json& model_config) {
    const size_t min_required_bytes = CONVERSATION_HISTORY.size() + QUESTION.size() + STANDALONE_QUESTION_PROMPT.size() + question.size();
    if(model_config["max_bytes"].get<size_t>() < min_required_bytes) {
        return Option<std::string>(400, "Max bytes is not enough to generate standalone question.");
    }

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
    auto conversation = conversation_history["conversation"];
    auto max_conversation_length = model_config["max_bytes"].get<size_t>() - min_required_bytes;
    auto truncate_conversation_op = ConversationManager::get_instance().truncate_conversation(conversation, max_conversation_length);

    if(!truncate_conversation_op.ok()) {
        return Option<std::string>(400, "Conversation history is not valid");
    }

    auto truncated_conversation = truncate_conversation_op.get();
    
    for(auto& message : truncated_conversation) {
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
        return Option<std::string>(400, "Cloudflare API timeout.");
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
    
    return parse_stream_response(res);
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

Option<std::string> CFConversationModel::parse_stream_response(const std::string& res) {
    try {
        auto json_res = nlohmann::json::parse(res);
        std::string parsed_response = "";
        std::vector<std::string> lines = json_res["response"].get<std::vector<std::string>>();
        std::regex data_regex("data: (.*?)\\n\\n");
        for(auto& line : lines) {
            auto begin = std::sregex_iterator(line.begin(), line.end(), data_regex);
            auto end = std::sregex_iterator();
            for (std::sregex_iterator i = begin; i != end; ++i) {
                std::string substr_line = i->str().substr(6, i->str().size() - 8);
                if(substr_line.find("[DONE]") != std::string::npos) {
                    break;
                }
                nlohmann::json json_line;
                json_line = nlohmann::json::parse(substr_line);
                parsed_response += json_line["response"];
            }
        }
        return Option<std::string>(parsed_response);
    } catch (const std::exception& e) {
        LOG(ERROR) << e.what();
        LOG(ERROR) << "Response: " << res;
        return Option<std::string>(400, "Got malformed response from Cloudflare API.");
    }
}

Option<bool> vLLMConversationModel::validate_model(const nlohmann::json& model_config) {
    if(model_config.count("vllm_url") == 0) {
        return Option<bool>(400, "vLLM URL is not provided");
    }

    if(!model_config["vllm_url"].is_string()) {
        return Option<bool>(400, "vLLM URL is not a string");
    }

    
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    std::string res;

    if(model_config.count("api_key") != 0) {
        if(!model_config["api_key"].is_string()) {
            return Option<bool>(400, "API key is not a string");
        }
        headers["Authorization"] = "Bearer " + model_config["api_key"].get<std::string>();
    }

    auto res_code = RemoteEmbedder::call_remote_api("GET", get_list_models_url(model_config["vllm_url"]), "", res, res_headers, headers);

    if(res_code == 408) {
        return Option<bool>(408, "vLLM API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "vLLM API error: " + res);
        }
        if(json_res.count("message") == 0) {
            return Option<bool>(400, "vLLM API error: " + res);
        }
        return Option<bool>(400, "vLLM API error: " + nlohmann::json::parse(res)["message"].get<std::string>());
    }

    nlohmann::json models_json;
    try {
        models_json = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return Option<bool>(400, "Got malformed response from vLLM API.");
    }
    bool found = false;
    // extract model name by removing "vLLM/" prefix
    auto model_name_without_namespace = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    for (auto& model : models_json["data"]) {
        if (model["id"] == model_name_without_namespace) {
            found = true;
            break;
        }
    }

    if(!found) {
        return Option<bool>(400, "Property `model_name` is not a valid vLLM model.");
    }

    nlohmann::json req_body;
    headers["Content-Type"] = "application/json";
    req_body["model"] = model_name_without_namespace;
    req_body["messages"] = R"([
        {
            "role":"user",
            "content":"hello"
        }
    ])"_json;
    std::string chat_res;

    res_code = RemoteEmbedder::call_remote_api("POST", get_chat_completion_url(model_config["vllm_url"]), req_body.dump(-1), chat_res, res_headers, headers);

    if(res_code == 408) {
        return Option<bool>(408, "vLLM API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "vLLM API error: " + res);
        }
        if(json_res.count("message") == 0) {
            return Option<bool>(400, "vLLM API error: " + res);
        }
        return Option<bool>(400, "vLLM API error: " + nlohmann::json::parse(res)["message"].get<std::string>());
    }

    return Option<bool>(true);
}

Option<std::string> vLLMConversationModel::get_answer(const std::string& context, const std::string& prompt, 
                                              const std::string& system_prompt, const nlohmann::json& model_config) {
    const std::string model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string vllm_url = model_config["vllm_url"].get<std::string>();

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
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
    message["content"] = DATA_STR + context + QUESTION_STR + prompt + ANSWER_STR;
    req_body["messages"].push_back(message);

    std::string res;

    if(model_config.count("api_key") != 0) {
        headers["Authorization"] = "Bearer " + model_config["api_key"].get<std::string>();
    }

    auto res_code = RemoteEmbedder::call_remote_api("POST", get_chat_completion_url(vllm_url), req_body.dump(), res, res_headers, headers);

    if(res_code == 408) {
        throw Option<std::string>(400, "vLLM API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "vLLM API error: " + res);
        }
        if(json_res.count("message") == 0) {
            return Option<std::string>(400, "vLLM API error: " + res);
        }
        return Option<std::string>(400, "vLLM API error: " + nlohmann::json::parse(res)["message"].get<std::string>());
    }

    nlohmann::json json_res;
    try {
        json_res = nlohmann::json::parse(res);

        if(json_res.count("choices") == 0 || json_res["choices"].size() == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }

        if(json_res["choices"][0].count("message") == 0 || json_res["choices"][0]["message"].count("content") == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }
    } catch (const std::exception& e) {
        throw Option<std::string>(400, "Got malformed response from vLLM API.");
    }

    return Option<std::string>(json_res["choices"][0]["message"]["content"].get<std::string>());
}

Option<std::string> vLLMConversationModel::get_standalone_question(const nlohmann::json& conversation_history, 
                                                           const std::string& question, const nlohmann::json& model_config) {
    const size_t min_required_bytes = CONVERSATION_HISTORY.size() + QUESTION.size() + STANDALONE_QUESTION_PROMPT.size() + question.size();
    if(model_config["max_bytes"].get<size_t>() < min_required_bytes) {
        return Option<std::string>(400, "Max bytes is not enough to generate standalone question.");
    }
        
    const std::string model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string vllm_url = model_config["vllm_url"].get<std::string>();
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    nlohmann::json req_body;
    req_body["model"] = model_name;
    req_body["messages"] = nlohmann::json::array();
    std::string res;
    
    std::string standalone_question = STANDALONE_QUESTION_PROMPT;
    auto conversation = conversation_history["conversation"];
    auto max_conversation_length = model_config["max_bytes"].get<size_t>() - min_required_bytes;
    auto truncate_conversation_op = ConversationManager::get_instance().truncate_conversation(conversation, max_conversation_length);
    
    if(!truncate_conversation_op.ok()) {
        return Option<std::string>(400, "Conversation history is not valid");
    }

    auto truncated_conversation = truncate_conversation_op.get();
    
    for(auto& message : truncated_conversation) {
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

    if(model_config.count("api_key") != 0) {
        headers["Authorization"] = "Bearer " + model_config["api_key"].get<std::string>();
    }

    auto res_code = RemoteEmbedder::call_remote_api("POST", get_chat_completion_url(vllm_url), req_body.dump(), res, res_headers, headers);

    if(res_code == 408) {
        return Option<std::string>(400, "vLLM API timeout.");
    }

    if (res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "vLLM API error: " + res);
        }
        if(json_res.count("message") == 0) {
            return Option<std::string>(400, "vLLM API error: " + res);
        }
        return Option<std::string>(400, "vLLM API error: " + nlohmann::json::parse(res)["message"].get<std::string>());
    }

    nlohmann::json json_res;
    try {
        json_res = nlohmann::json::parse(res);

        if(json_res.count("choices") == 0 || json_res["choices"].size() == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }

        if(json_res["choices"][0].count("message") == 0 || json_res["choices"][0]["message"].count("content") == 0) {
            return Option<std::string>(400, "Got malformed response from OpenAI API.");
        }
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from vLLM API.");
    }

    return Option<std::string>(json_res["choices"][0]["message"]["content"].get<std::string>());
}

Option<nlohmann::json> vLLMConversationModel::format_question(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["user"] = message;
    return Option<nlohmann::json>(json);
}

Option<nlohmann::json> vLLMConversationModel::format_answer(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["assistant"] = message;
    return Option<nlohmann::json>(json);
}

const std::string vLLMConversationModel::get_list_models_url(const std::string& vllm_url) {
    return vllm_url.back() == '/' ? vllm_url + "v1/models" : vllm_url + "/v1/models";
}

const std::string vLLMConversationModel::get_chat_completion_url(const std::string& vllm_url) {
    return vllm_url.back() == '/' ? vllm_url + "v1/chat/completions" : vllm_url + "/v1/chat/completions";
}
