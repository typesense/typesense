#include <regex>
#include <iterator>
#include "conversation_model.h"
#include "embedder_manager.h"
#include "text_embedder_remote.h"
#include "conversation_manager.h"
#include "typesense_server_utils.h"
#include "http_proxy.h"
#include "string_utils.h"


static const std::string get_model_namespace(const std::string& model_name) {
    if(model_name.find("/") != std::string::npos) {
        return model_name.substr(0, model_name.find("/"));
    } else {
        return "";
    }
}

Option<bool> ConversationModel::validate_model(const nlohmann::json& model_config) {
    // check model_name exists and it is a string
    if(model_config.count("model_name") == 0 || !model_config["model_name"].is_string()) {
        return Option<bool>(400, "Property `model_name` is not provided or not a string.");
    }

    if(model_config.count("system_prompt") != 0 && !model_config["system_prompt"].is_string()) {
        return Option<bool>(400, "Property `system_prompt` is not a string.");
    }

    if(model_config.count("history_collection") == 0 || !model_config["history_collection"].is_string()) {
        return Option<bool>(400, "Property `history_collection` is missing or is not a string.");
    }

    if(model_config.count("max_bytes") == 0 || !model_config["max_bytes"].is_number_unsigned() || model_config["max_bytes"].get<size_t>() == 0) {
        return Option<bool>(400, "Property `max_bytes` is not provided or not a positive integer.");
    }

    auto validate_converson_collection_op = ConversationManager::get_instance()
            .validate_conversation_store_collection(model_config["history_collection"].get<std::string>());
    if(!validate_converson_collection_op.ok()) {
        return Option<bool>(400, validate_converson_collection_op.error());
    }

    if(model_config.count("ttl") != 0 && !model_config["ttl"].is_number_unsigned()) {
        return Option<bool>(400, "Property `ttl` is not a positive integer.");
    }

    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    if(model_namespace == "openai") {
        return OpenAIConversationModel::validate_model(model_config);
    } else if(model_namespace == "cloudflare") {
        return CFConversationModel::validate_model(model_config);
    } else if(model_namespace == "vllm") {
        return vLLMConversationModel::validate_model(model_config);
    } else if(model_namespace == "gcp") {
        return GeminiConversationModel::validate_model(model_config);
    } else if(model_namespace == "azure") {
        return AzureConversationModel::validate_model(model_config);
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
    } else if(model_namespace == "google") {
        return GeminiConversationModel::get_answer(context, prompt, system_prompt, model_config);
    } else if(model_namespace == "azure") {
        return AzureConversationModel::get_answer(context, prompt, system_prompt, model_config);
    }

    return Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<std::string> ConversationModel::get_answer_stream(const std::string& context, const std::string& prompt, const nlohmann::json& model_config,
                                                        const std::shared_ptr<http_req>& req, 
                                                        const std::shared_ptr<http_res>& res,
                                                        const std::string conversation_id) {

    const std::string& model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());
    std::string system_prompt = "";
    if(model_config.count("system_prompt") != 0 && model_config["system_prompt"].is_string()) {
        system_prompt = model_config["system_prompt"].get<std::string>();
    }

    // construct async_conversation_t object
    async_conversations[req].conversation_id = conversation_id;


    Option<std::string> response_op("");
    if(model_namespace == "openai") {
        response_op =  OpenAIConversationModel::get_answer_stream(context, prompt, system_prompt, model_config, req, res);
    } else if(model_namespace == "cloudflare") {
        response_op =  CFConversationModel::get_answer_stream(context, prompt, system_prompt, model_config, req, res);
    } else if(model_namespace == "vllm") {
        response_op =  vLLMConversationModel::get_answer_stream(context, prompt, system_prompt, model_config, req, res);
    } else if(model_namespace == "gcp") {
        response_op =  GeminiConversationModel::get_answer_stream(context, prompt, system_prompt, model_config, req, res);
    } else if(model_namespace == "azure") {
        response_op = AzureConversationModel::get_answer_stream(model_config, prompt, context, system_prompt, req, res, conversation_id);
    } else {
        async_conversations.erase(req);
        return Option<std::string>(400, "Model namespace " + model_namespace + " is not supported.");
    }

    // remove async_conversation_t object
    async_conversations.erase(req);

    return response_op;
}

Option<std::string> ConversationModel::get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config) {
    const std::string model_namespace = get_model_namespace(model_config["model_name"].get<std::string>());

    if(model_namespace == "openai") {
        return OpenAIConversationModel::get_standalone_question(conversation_history, question, model_config);
    } else if(model_namespace == "cloudflare") {
        return CFConversationModel::get_standalone_question(conversation_history, question, model_config);
    } else if(model_namespace == "vllm") {
        return vLLMConversationModel::get_standalone_question(conversation_history, question, model_config);
    } else if(model_namespace == "gcp") {
        return GeminiConversationModel::get_standalone_question(conversation_history, question, model_config);
    } else if(model_namespace == "azure") {
        return AzureConversationModel::get_standalone_question(conversation_history, question, model_config);
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
    } else if(model_namespace == "gcp") {
        return GeminiConversationModel::format_question(message);
    } else if(model_namespace == "azure") {
        return AzureConversationModel::format_question(message);
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
    } else if(model_namespace == "gcp") {
        return GeminiConversationModel::format_answer(message);
    } else if(model_namespace == "azure") {
        return AzureConversationModel::format_answer(message);
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
    } else if(model_namespace == "gcp") {
        return Option<size_t>(GeminiConversationModel::get_minimum_required_bytes());
    } else if(model_namespace == "azure") {
        return Option<size_t>(AzureConversationModel::get_minimum_required_bytes());
    }

    return Option<size_t>(400, "Model namespace " + model_namespace + " is not supported.");
}

Option<std::string> OpenAIConversationModel::get_openai_url(const nlohmann::json& model_config) {
    std::string openai_url = OPENAI_URL;
    if(model_config.count("openai_url") != 0) {
        if(!model_config["openai_url"].is_string()) {
            return Option<std::string>(400, "Property `openai_url` is not a string.");
        }
        openai_url = model_config["openai_url"].get<std::string>();
        if(!openai_url.empty() && openai_url.back() == '/') {
            openai_url.pop_back();
        }
    }

    return Option<std::string>(openai_url);
}

Option<std::string> OpenAIConversationModel::get_openai_path(const nlohmann::json& model_config) {
    std::string openai_path = OPENAI_CHAT_COMPLETION;
    if(model_config.count("openai_path") != 0) {
        if(!model_config["openai_path"].is_string()) {
            return Option<std::string>(400, "Property `openai_path` is not a string.");
        }
        openai_path = model_config["openai_path"].get<std::string>();
        if(!openai_path.empty() && openai_path.front() != '/') {
            openai_path = "/" + openai_path;
        }
    }

    return Option<std::string>(openai_path);
}

Option<bool> OpenAIConversationModel::validate_model(const nlohmann::json& model_config) {
    if(model_config.count("api_key") == 0) {
        return Option<bool>(400, "API key is not provided");
    }

    if(!model_config["api_key"].is_string()) {
        return Option<bool>(400, "API key is not a string");
    }

    auto openai_url_op = get_openai_url(model_config);
    if(!openai_url_op.ok()) {
        return Option<bool>(openai_url_op.code(), openai_url_op.error());
    }
    const std::string openai_url = openai_url_op.get();
    
    auto openai_path_op = get_openai_path(model_config);
    if(!openai_path_op.ok()) {
        return Option<bool>(openai_path_op.code(), openai_path_op.error());
    }

    const std::string openai_path = openai_path_op.get();

    // extract model name by removing "openai/" prefix
    auto model_name_without_namespace = EmbedderManager::get_model_name_without_namespace(
        model_config["model_name"].get<std::string>());
    
    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Authorization"] = "Bearer " + model_config["api_key"].get<std::string>();
    headers["Content-Type"] = "application/json";
    std::string res;

    nlohmann::json req_body;
    req_body["model"] = model_name_without_namespace;
    req_body["messages"] = R"([
        {
            "role":"user",
            "content":"hello"
        }
    ])"_json;

    std::string chat_res;
    auto res_code = RemoteEmbedder::call_remote_api("POST", openai_url + openai_path, req_body.dump(), chat_res, res_headers, headers);

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
        return Option<bool>(400, "OpenAI API error: " + json_res["error"]["message"].get<std::string>());
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

    auto openai_url_op = get_openai_url(model_config);
    if(!openai_url_op.ok()) {
        return Option<std::string>(openai_url_op.code(), openai_url_op.error());
    }
    const std::string openai_url = openai_url_op.get();

    auto openai_path_op = get_openai_path(model_config);
    if(!openai_path_op.ok()) {
        return Option<std::string>(openai_path_op.code(), openai_path_op.error());
    }

    const std::string openai_path = openai_path_op.get();

    std::string res;
    auto res_code = RemoteEmbedder::call_remote_api("POST", openai_url + openai_path, req_body.dump(), res, res_headers, headers);

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

    auto openai_url_op = get_openai_url(model_config);
    if(!openai_url_op.ok()) {
        return Option<std::string>(openai_url_op.code(), openai_url_op.error());
    }
    const std::string openai_url = openai_url_op.get();

    auto openai_path_op = get_openai_path(model_config);
    if(!openai_path_op.ok()) {
        return Option<std::string>(openai_path_op.code(), openai_path_op.error());
    }

    const std::string openai_path = openai_path_op.get();

    auto res_code = RemoteEmbedder::call_remote_api("POST", openai_url + openai_path, req_body.dump(), res, res_headers, headers);

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

bool OpenAIConversationModel::async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }

    async_conversations[req].status_code = status_code;
    if(status_code != 200) {
        async_conversations[req].response = response;
        return false;
    }

    return true;
}

void OpenAIConversationModel::async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return;
    }

    try {
        bool found_done = false;
        std::string parsed_response;
        std::regex data_regex("data: (.*?)\\n\\n");
        auto begin = std::sregex_iterator(response.begin(), response.end(), data_regex);
        auto end = std::sregex_iterator();
        for (std::sregex_iterator i = begin; i != end; ++i) {
            std::string substr_line = i->str().substr(6, i->str().size() - 8);
            if(substr_line.find("[DONE]") != std::string::npos) {
                found_done = true;  
                break;
            }
            nlohmann::json json_line;
            json_line = nlohmann::json::parse(substr_line);
            if(json_line.count("choices") == 0 || json_line["choices"][0].count("delta") == 0 || json_line["choices"][0]["delta"].count("content") == 0) {
                continue;
            }
            parsed_response += json_line["choices"][0]["delta"]["content"].get<std::string>();
        }

        async_conversations[req].response += parsed_response;
        nlohmann::json json_res;
        json_res["message"] = parsed_response;
        json_res["conversation_id"] = async_conversations[req].conversation_id;
        response = "data: " + json_res.dump(-1) + "\n\n";
        if(found_done) {
            response += "data: [DONE]\n\n";
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << e.what();
        LOG(ERROR) << "Response: " << response;
    }
}

bool OpenAIConversationModel::async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }

    async_conversations[req].ready = true;
    async_conversations[req].cv.notify_one();
    return false;
}

Option<std::string> OpenAIConversationModel::get_answer_stream(const std::string& context, const std::string& prompt, 
                                                                const std::string& system_prompt, const nlohmann::json& model_config,
                                                                const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
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
    req_body["stream"] = true;

    auto openai_url_op = get_openai_url(model_config);
    if(!openai_url_op.ok()) {
        return Option<std::string>(openai_url_op.code(), openai_url_op.error());
    }
    const std::string openai_url = openai_url_op.get();

    auto openai_path_op = get_openai_path(model_config);
    if(!openai_path_op.ok()) {
        return Option<std::string>(openai_path_op.code(), openai_path_op.error());
    }

    const std::string openai_path = openai_path_op.get();

    req->async_res_set_headers_callback = async_res_set_headers_callback;
    req->async_res_write_callback = async_res_write_callback;
    req->async_res_done_callback = async_res_done_callback;
    auto raft_server = RemoteEmbedder::get_raft_server();
    if(raft_server && !raft_server->get_leader_url().empty()) {
        auto proxy_url = raft_server->get_leader_url() + "proxy_sse";
        nlohmann::json proxy_req_body;
        proxy_req_body["method"] = "POST";
        proxy_req_body["url"] = openai_url + openai_path;
        proxy_req_body["body"] = req_body.dump();
        proxy_req_body["headers"] = headers;
        std::unordered_map<std::string, std::string> header_;
        header_["x-typesense-api-key"] = HttpClient::get_api_key();

        res->proxied_stream = true;
        auto status = HttpClient::get_instance().post_response_sse(proxy_url, proxy_req_body.dump(), header_,
                                                                   HttpProxy::default_timeout_ms, req, res, server);
    } else {
        res->proxied_stream = true;
        HttpClient::get_instance().post_response_sse(openai_url + openai_path, req_body.dump(), headers,
                                                     HttpProxy::default_timeout_ms, req, res, server);
    }

    auto& async_conversation = async_conversations[req];
    // wait for the response
    std::unique_lock<std::mutex> lock(async_conversation.mutex);
    async_conversation.cv.wait(lock, [&async_conversation] { return async_conversation.ready; });

    if(async_conversation.status_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(async_conversation.response);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "OpenAI API error: " + async_conversation.response);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::string>(400, "OpenAI API error: " + async_conversation.response);
        }
        return Option<std::string>(400, "OpenAI API error: " + nlohmann::json::parse(async_conversation.response)["error"]["message"].get<std::string>());
    }

    return Option<std::string>(async_conversation.response);
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

Option<std::string> CFConversationModel::get_answer_stream(const std::string& context, const std::string& prompt, 
                                                                const std::string& system_prompt, const nlohmann::json& model_config,
                                                                const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
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
    req_body["stream"] = true;


    auto url = get_model_url(model_name, account_id);

    req->async_res_set_headers_callback = async_res_set_headers_callback;
    req->async_res_write_callback = async_res_write_callback;
    req->async_res_done_callback = async_res_done_callback;
    auto raft_server = RemoteEmbedder::get_raft_server();
    if(raft_server && !raft_server->get_leader_url().empty()) {
        auto proxy_url = raft_server->get_leader_url() + "proxy_sse";
        nlohmann::json proxy_req_body;
        proxy_req_body["method"] = "POST";
        proxy_req_body["url"] = url;
        proxy_req_body["body"] = req_body.dump();
        proxy_req_body["headers"] = headers;
        std::unordered_map<std::string, std::string> header_;
        header_["x-typesense-api-key"] = HttpClient::get_api_key();

        HttpClient::get_instance().post_response_sse(proxy_url, proxy_req_body.dump(), header_, HttpProxy::default_timeout_ms, req, res, server);
    } else {
        HttpClient::get_instance().post_response_sse(url, req_body.dump(), headers, HttpProxy::default_timeout_ms, req, res, server);
    }

    auto& async_conversation = async_conversations[req];
    // wait for the response
    std::unique_lock<std::mutex> lock(async_conversation.mutex);
    async_conversation.cv.wait(lock, [&async_conversation] { return async_conversation.ready; });

    if(async_conversation.status_code != 200) {
        if(async_conversation.status_code == 408) {
            return Option<std::string>(400, "Cloudflare API timeout.");
        }
    
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(async_conversation.response);
            if(json_res.count("response") == 0 || json_res["response"].size() == 0) {
                return Option<std::string>(400, "Cloudflare API error: " + async_conversation.response);
            }
            json_res = nlohmann::json::parse(json_res["response"][0].get<std::string>());
        } catch (const std::exception& e) {
            throw Option<std::string>(400, "Cloudflare API error: " + async_conversation.response);
        }

        if(json_res.count("errors") == 0 || json_res["errors"].size() == 0) {
            return Option<std::string>(400, "Cloudflare API error: " + json_res.dump(0));
        }

        json_res = json_res["errors"][0];
        return Option<std::string>(400, "Cloudflare API error: " + json_res["message"].get<std::string>());
    }


    return Option<std::string>(async_conversation.response);
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


bool CFConversationModel::async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }
    async_conversations[req].status_code = status_code;
    if(status_code != 200) {
        async_conversations[req].response = response;
        return false;
    }

    return true;
}

void CFConversationModel::async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return;
    }

    try {
        bool found_done = false;
        std::string parsed_response;
        std::regex data_regex("data: (.*?)\\n\\n");
        auto begin = std::sregex_iterator(response.begin(), response.end(), data_regex);
        auto end = std::sregex_iterator();
        for (std::sregex_iterator i = begin; i != end; ++i) {
            std::string substr_line = i->str().substr(6, i->str().size() - 8);
            if(substr_line.find("[DONE]") != std::string::npos) {
                found_done = true;  
                break;
            }
            nlohmann::json json_line;
            json_line = nlohmann::json::parse(substr_line);
            if(json_line.count("response") == 0) {
                continue;
            }
            parsed_response += json_line["response"].get<std::string>();
        }

        async_conversations[req].response += parsed_response;
        nlohmann::json json_res;
        json_res["message"] = parsed_response;
        json_res["conversation_id"] = async_conversations[req].conversation_id;
        response = "data: " + json_res.dump(-1) + "\n\n";
        if(found_done) {
            response += "data: [DONE]\n\n";
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << e.what();
        LOG(ERROR) << "Response: " << response;
    }
}

bool CFConversationModel::async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }

    async_conversations[req].ready = true;
    async_conversations[req].cv.notify_one();
    return false;
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


Option<std::string> vLLMConversationModel::get_answer_stream(const std::string& context, const std::string& prompt, 
                                                            const std::string& system_prompt, const nlohmann::json& model_config,
                                                            const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
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
    req_body["stream"] = true;

    if(model_config.count("api_key") != 0) {
        headers["Authorization"] = "Bearer " + model_config["api_key"].get<std::string>();
    }
    

    req->async_res_set_headers_callback = async_res_set_headers_callback;
    req->async_res_write_callback = async_res_write_callback;
    req->async_res_done_callback = async_res_done_callback;
    auto raft_server = RemoteEmbedder::get_raft_server();
    if(raft_server && !raft_server->get_leader_url().empty()) {
        auto proxy_url = raft_server->get_leader_url() + "proxy_sse";
        nlohmann::json proxy_req_body;
        proxy_req_body["method"] = "POST";
        proxy_req_body["url"] = get_chat_completion_url(vllm_url);
        proxy_req_body["body"] = req_body.dump();
        proxy_req_body["headers"] = headers;
        std::unordered_map<std::string, std::string> header_;
        header_["x-typesense-api-key"] = HttpClient::get_api_key();

        HttpClient::get_instance().post_response_sse(proxy_url, proxy_req_body.dump(), header_, HttpProxy::default_timeout_ms, req, res, server);
    } else {
        HttpClient::get_instance().post_response_sse(get_chat_completion_url(vllm_url), req_body.dump(), headers, HttpProxy::default_timeout_ms, req, res, server);
    }

    auto& async_conversation = async_conversations[req];
    // wait for the response
    std::unique_lock<std::mutex> lock(async_conversation.mutex);
    async_conversation.cv.wait(lock, [&async_conversation] { return async_conversation.ready; });

    if(async_conversation.status_code != 200) {
        if(async_conversation.status_code == 408) {
            throw Option<std::string>(400, "vLLM API timeout.");
        }
    
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(async_conversation.response);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "vLLM API error: " + async_conversation.response);
        }
        if(json_res.count("message") == 0) {
            return Option<std::string>(400, "vLLM API error: " + async_conversation.response);
        }
        return Option<std::string>(400, "vLLM API error: " + nlohmann::json::parse(async_conversation.response)["message"].get<std::string>());
    }


    return Option<std::string>(async_conversation.response);
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

bool vLLMConversationModel::async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }
    async_conversations[req].status_code = status_code;
    if(status_code != 200) {
        async_conversations[req].response = response;
        return false;
    }

    return true;
}

void vLLMConversationModel::async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return;
    }

    try {
        bool found_done = false;
        std::string parsed_response;
        std::regex data_regex("data: (.*?)\\n\\n");
        auto begin = std::sregex_iterator(response.begin(), response.end(), data_regex);
        auto end = std::sregex_iterator();
        for (std::sregex_iterator i = begin; i != end; ++i) {
            std::string substr_line = i->str().substr(6, i->str().size() - 8);
            if(substr_line.find("[DONE]") != std::string::npos) {
                found_done = true;  
                break;
            }
            nlohmann::json json_line;
            json_line = nlohmann::json::parse(substr_line);
            if(json_line.count("choices") == 0 || json_line["choices"][0].count("delta") == 0 || json_line["choices"][0]["delta"].count("content") == 0) {
                continue;
            }
            parsed_response += json_line["choices"][0]["delta"]["content"].get<std::string>();
        }

        async_conversations[req].response += parsed_response;
        nlohmann::json json_res;
        json_res["message"] = parsed_response;
        json_res["conversation_id"] = async_conversations[req].conversation_id;
        response = "data: " + json_res.dump(-1) + "\n\n";
        if(found_done) {
            response += "data: [DONE]\n\n";
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << e.what();
        LOG(ERROR) << "Response: " << response;
    }
}

bool vLLMConversationModel::async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }

    async_conversations[req].ready = true;
    async_conversations[req].cv.notify_one();
    return false;
}


Option<std::string> GeminiConversationModel::get_gemini_url(const nlohmann::json& model_config, const bool stream) {
    if(model_config.count("model_name") == 0) {
        return Option<std::string>(400, "Gemini model name is not provided");
    }
    if(!model_config["model_name"].is_string()) {
        return Option<std::string>(400, "Gemini model name is not a string");
    }

    if(model_config.count("api_key") == 0) {
        return Option<std::string>(400, "Gemini API key is not provided");
    }

    if(!model_config["api_key"].is_string()) {
        return Option<std::string>(400, "Gemini API key is not a string");
    }

    auto model_name_without_namespace = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());

    std::string url = GEMINI_URL + model_name_without_namespace;

    if(stream) {
        url += STREAM_RESPONSE_STR;
    } else {
        url += NON_STREAM_RESPONSE_STR;
    }

    return Option<std::string>(url + "?key=" + model_config["api_key"].get<std::string>());
}

Option<bool> GeminiConversationModel::validate_model(const nlohmann::json& model_config) {
    auto get_gemini_url_op = get_gemini_url(model_config, false);
    if(!get_gemini_url_op.ok()) {
        return Option<bool>(get_gemini_url_op.code(), get_gemini_url_op.error());
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";
    std::string res;

    nlohmann::json req_body;
    req_body["contents"] = nlohmann::json::object();
    req_body["contents"]["parts"] = nlohmann::json::array();
    req_body["contents"]["parts"].push_back(R"({
        "text": "hello"
    })"_json);

    auto url = get_gemini_url_op.get();

    auto res_code = RemoteEmbedder::call_remote_api("POST", url, req_body.dump(), res, res_headers, headers);
    if(res_code == 408) {
        return Option<bool>(408, "Gemini API timeout.");
    }

    if(res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<bool>(400, "Gemini API error: " + res);
        }
        if(json_res.count("error") == 0) {
            return Option<bool>(400, "Gemini API error: " + res);
        }
        return Option<bool>(400, "Gemini API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    try {
        nlohmann::json json_res = nlohmann::json::parse(res);
    } catch (const std::exception& e) {
        return Option<bool>(400, "Got malformed response from Gemini API.");
    }

    return Option<bool>(true);
}

Option<std::string> GeminiConversationModel::get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config) {
    auto get_gemini_url_op = get_gemini_url(model_config, false);
    if(!get_gemini_url_op.ok()) {
        return Option<std::string>(get_gemini_url_op.code(), get_gemini_url_op.error());
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";

    nlohmann::json req_body;
    
    if(!system_prompt.empty()) {
        nlohmann::json system_message = nlohmann::json::object();
        system_message["text"] = system_prompt;
        req_body["system_instruction"] = nlohmann::json::object();
        req_body["system_instruction"]["parts"] = nlohmann::json::array();
        req_body["system_instruction"]["parts"].push_back(system_message);
    }

    nlohmann::json message = nlohmann::json::object();
    message["text"] = DATA_STR + context + QUESTION_STR + prompt + ANSWER_STR;
    req_body["contents"] = nlohmann::json::object();
    req_body["contents"]["parts"] = nlohmann::json::array();
    req_body["contents"]["parts"].push_back(message);

    auto url = get_gemini_url_op.get();
    std::string res;

    auto res_code = RemoteEmbedder::call_remote_api("POST", url, req_body.dump(), res, res_headers, headers);

    if(res_code == 408) {
        return Option<std::string>(400, "Gemini API timeout.");
    }

    if(res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "Gemini API error: " + res);
        }
        if(json_res.count("error") == 0) {
            return Option<std::string>(400, "Gemini API error: " + res);
        }
        return Option<std::string>(400, "Gemini API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    nlohmann::json json_res;

    try {
        json_res = nlohmann::json::parse(res);
        return Option<std::string>(json_res["candidates"][0]["content"]["parts"][0]["text"].get<std::string>());
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from Gemini API.");
    }
}

Option<nlohmann::json> GeminiConversationModel::format_question(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["user"] = message;
    return Option<nlohmann::json>(json);
}

Option<nlohmann::json> GeminiConversationModel::format_answer(const std::string& message) {
    nlohmann::json json = nlohmann::json::object();
    json["assistant"] = message;
    return Option<nlohmann::json>(json);
}

Option<std::string> GeminiConversationModel::get_answer_stream(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config,
                                                                const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto get_gemini_url_op = GeminiConversationModel::get_gemini_url(model_config, true);
    if(!get_gemini_url_op.ok()) {
        return Option<std::string>(get_gemini_url_op.code(), get_gemini_url_op.error());
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;
    headers["Content-Type"] = "application/json";

    nlohmann::json req_body;
    
    if(!system_prompt.empty()) {
        nlohmann::json system_message = nlohmann::json::object();
        system_message["text"] = system_prompt;
        req_body["system_instruction"] = nlohmann::json::object();
        req_body["system_instruction"]["parts"] = nlohmann::json::array();
        req_body["system_instruction"]["parts"].push_back(system_message);
    }

    nlohmann::json message = nlohmann::json::object();
    message["text"] = DATA_STR + context + QUESTION_STR + prompt + ANSWER_STR;
    req_body["contents"] = nlohmann::json::object();
    req_body["contents"]["parts"] = nlohmann::json::array();
    req_body["contents"]["parts"].push_back(message);

    auto url = get_gemini_url_op.get();

    req->async_res_set_headers_callback = async_res_set_headers_callback;
    req->async_res_write_callback = async_res_write_callback;
    req->async_res_done_callback = async_res_done_callback;
    auto raft_server = RemoteEmbedder::get_raft_server();
    if(raft_server && !raft_server->get_leader_url().empty()) {
        auto proxy_url = raft_server->get_leader_url() + "proxy_sse";
        nlohmann::json proxy_req_body;
        proxy_req_body["method"] = "POST";
        proxy_req_body["url"] = url;
        proxy_req_body["body"] = req_body.dump();
        proxy_req_body["headers"] = headers;
        std::unordered_map<std::string, std::string> header_;
        header_["x-typesense-api-key"] = HttpClient::get_api_key();

        HttpClient::get_instance().post_response_sse(proxy_url, proxy_req_body.dump(), header_, HttpProxy::default_timeout_ms, req, res, server);
    } else {
        HttpClient::get_instance().post_response_sse(url, req_body.dump(), headers, HttpProxy::default_timeout_ms, req, res, server);
    }
    auto& async_conversation = async_conversations[req];

    std::unique_lock<std::mutex> lock(async_conversation.mutex);
    async_conversation.cv.wait(lock, [&async_conversation] { return async_conversation.ready; });

    if(async_conversation.status_code != 200) {
        if(async_conversation.status_code == 408) {
            return Option<std::string>(400, "Gemini API timeout.");
        }
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(async_conversation.response);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "Gemini API error: " + async_conversation.response);
        }
        if(json_res.count("error") == 0) {
            return Option<std::string>(400, "Gemini API error: " + async_conversation.response);
        }
        return Option<std::string>(400, "Gemini API error: " + nlohmann::json::parse(async_conversation.response)["error"]["message"].get<std::string>());
    }


    return Option<std::string>(async_conversation.response);
}

bool GeminiConversationModel::async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }
    async_conversations[req].status_code = status_code;
    if(status_code != 200) {
        async_conversations[req].response = response;
        return false;
    }
    // gemini api returns content-type as application/json even if it is a stream, we have to manipulate it
    content_type = "text/event-stream";
    return true;
}

void GeminiConversationModel::async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return;
    }

    try {
        if(!response.empty()) {
            if(response[0] == '[' || response[0] == ',') {
                response.erase(0, 1);
            }
            if(response.back() == ',' || response.back() == ']') {
                response.pop_back();
            }
        }

        if(response.empty()) {
            response = "data: \n\n";
            return;
        }
        auto json_res = nlohmann::json::parse(response);
        if(json_res.count("candidates") == 0 || json_res["candidates"].size() == 0) {
            return;
        }
        if(json_res["candidates"][0].count("content") == 0 || json_res["candidates"][0]["content"].count("parts") == 0) {
            return;
        }
        if(json_res["candidates"][0]["content"]["parts"].size() == 0) {
            return;
        }
        std::string parsed_response = json_res["candidates"][0]["content"]["parts"][0]["text"].get<std::string>();
        nlohmann::json json_actual_res;
        json_actual_res["message"] = parsed_response;
        json_actual_res["conversation_id"] = async_conversations[req].conversation_id;
        response = "data: " + json_actual_res.dump(-1) + "\n\n";
        async_conversations[req].response += parsed_response;
        if(json_res["candidates"][0].count("finishReason") != 0) {
            if(json_res["candidates"][0]["finishReason"] == "STOP") {
                response += "data: [DONE]\n\n";
            }
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << e.what();
        LOG(ERROR) << "Response: " << response;
    }
}

bool GeminiConversationModel::async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }

    async_conversations[req].ready = true;
    async_conversations[req].cv.notify_one();
    return false;
}

Option<std::string> GeminiConversationModel::get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config) {
    const size_t min_required_bytes = CONVERSATION_HISTORY.size() + QUESTION.size() + STANDALONE_QUESTION_PROMPT.size() + question.size();
    if(model_config["max_bytes"].get<size_t>() < min_required_bytes) {
        return Option<std::string>(400, "Max bytes is not enough to generate standalone question.");
    }


    auto get_gemini_url_op = GeminiConversationModel::get_gemini_url(model_config, false);
    if(!get_gemini_url_op.ok()) {
        return Option<std::string>(get_gemini_url_op.code(), get_gemini_url_op.error());
    }

    std::unordered_map<std::string, std::string> headers;
    std::map<std::string, std::string> res_headers;

    headers["Content-Type"] = "application/json";

    nlohmann::json req_body;

    req_body["contents"] = nlohmann::json::object();
    req_body["contents"]["parts"] = nlohmann::json::array();
    nlohmann::json question_obj = nlohmann::json::object();
    std::string standalone_question = STANDALONE_QUESTION_PROMPT + CONVERSATION_HISTORY;
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
        standalone_question += message.dump(-1) + "\n";
    }
    standalone_question += "\n\n<Question>\n" + question + "\n\n<Standalone question>\n";
    question_obj["text"] = standalone_question;
    req_body["contents"]["parts"].push_back(question_obj);
    auto url = get_gemini_url_op.get();
    std::string res;

    auto res_code = RemoteEmbedder::call_remote_api("POST", url, req_body.dump(), res, res_headers, headers);
    if(res_code == 408) {
        return Option<std::string>(400, "Gemini API timeout.");
    }

    if(res_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(res);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "Gemini API error: " + res);
        }
        if(json_res.count("error") == 0) {
            return Option<std::string>(400, "Gemini API error: " + res);
        }
        return Option<std::string>(400, "Gemini API error: " + nlohmann::json::parse(res)["error"]["message"].get<std::string>());
    }

    nlohmann::json json_res;
    try {
        json_res = nlohmann::json::parse(res);
        return Option<std::string>(json_res["candidates"][0]["content"]["parts"][0]["text"].get<std::string>());
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from Gemini API.");
    }
}

Option<std::string> AzureConversationModel::get_azure_url(const nlohmann::json& model_config) {
    if (!model_config.contains("url")) {
        return Option<std::string>(400, "url is required for Azure models");
    }
    return Option<std::string>(model_config["url"].get<std::string>());
}

Option<bool> AzureConversationModel::validate_model(const nlohmann::json& model_config) {
    if (!model_config.contains("api_key")) {
        return Option<bool>(400, "api_key is required for Azure models");
    }
    if (!model_config.contains("url")) {
        return Option<bool>(400, "url is required for Azure models");
    }
    return Option<bool>(true);
}

Option<std::string> AzureConversationModel::get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config) {
    auto url_op = get_azure_url(model_config);
    if (!url_op.ok()) {
        return Option<std::string>(url_op.code(), url_op.error());
    }

    std::string url = url_op.get();
    std::string api_key = model_config["api_key"].get<std::string>();

    // Extract model name by removing "azure/" prefix
    auto model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());

    nlohmann::json request_body;
    request_body["model"] = model_name;
    request_body["messages"] = nlohmann::json::array();
    
    if (!system_prompt.empty()) {
        request_body["messages"].push_back({
            {"role", "system"},
            {"content", system_prompt}
        });
    }

    request_body["messages"].push_back({
        {"role", "user"},
        {"content", context + prompt}
    });

    std::string response;
    std::map<std::string, std::string> res_headers;
    std::unordered_map<std::string, std::string> headers;
    headers["api-key"] = api_key;
    headers["Content-Type"] = "application/json";

    long status_code = HttpClient::post_response(url, request_body.dump(), response, res_headers, headers);

    if (status_code != 200) {
        return Option<std::string>(status_code, "Failed to get response from Azure API: " + response);
    }

    try {
        nlohmann::json response_json = nlohmann::json::parse(response);
        if (response_json.contains("choices") && !response_json["choices"].empty() && 
            response_json["choices"][0].contains("message") && 
            response_json["choices"][0]["message"].contains("content")) {
            return Option<std::string>(response_json["choices"][0]["message"]["content"].get<std::string>());
        }
        return Option<std::string>(500, "Invalid response format from Azure API");
    } catch (const std::exception& e) {
        return Option<std::string>(500, "Failed to parse Azure API response: " + std::string(e.what()));
    }
}

Option<std::string> AzureConversationModel::get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config) {
    const size_t min_required_bytes = CONVERSATION_HISTORY.size() + QUESTION.size() + STANDALONE_QUESTION_PROMPT.size() + question.size();
    if(model_config["max_bytes"].get<size_t>() < min_required_bytes) {
        return Option<std::string>(400, "Max bytes is not enough to generate standalone question.");
    }

    auto url_op = get_azure_url(model_config);
    if (!url_op.ok()) {
        return Option<std::string>(url_op.code(), url_op.error());
    }

    std::string url = url_op.get();
    std::string api_key = model_config["api_key"].get<std::string>();

    // Extract model name by removing "azure/" prefix
    auto model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());

    nlohmann::json request_body;
    request_body["model"] = model_name;
    request_body["messages"] = nlohmann::json::array();
    
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
        standalone_question += message.dump(0) + "\n";
    }

    standalone_question += "\n\n<Question>\n" + question;
    standalone_question += "\n\n<Standalone question>\n";

    nlohmann::json message = nlohmann::json::object();
    message["role"] = "user";
    message["content"] = standalone_question;

    request_body["messages"].push_back(message);

    std::string response;
    std::map<std::string, std::string> res_headers;
    std::unordered_map<std::string, std::string> headers;
    headers["api-key"] = api_key;
    headers["Content-Type"] = "application/json";

    long status_code = RemoteEmbedder::call_remote_api("POST", url, request_body.dump(), response, res_headers, headers);

    if (status_code == 408) {
        return Option<std::string>(400, "Azure API timeout.");
    }

    if (status_code != 200) {
        nlohmann::json json_res;
        try {
            json_res = nlohmann::json::parse(response);
        } catch (const std::exception& e) {
            return Option<std::string>(400, "Azure API error: " + response);
        }
        if(json_res.count("error") == 0 || json_res["error"].count("message") == 0) {
            return Option<std::string>(400, "Azure API error: " + response);
        }
        return Option<std::string>(400, "Azure API error: " + json_res["error"]["message"].get<std::string>());
    }

    try {
        nlohmann::json response_json = nlohmann::json::parse(response);
        if(response_json.count("choices") == 0 || response_json["choices"].size() == 0) {
            return Option<std::string>(400, "Got malformed response from Azure API.");
        }

        if(response_json["choices"][0].count("message") == 0 || response_json["choices"][0]["message"].count("content") == 0) {
            return Option<std::string>(400, "Got malformed response from Azure API.");
        }

        return Option<std::string>(response_json["choices"][0]["message"]["content"].get<std::string>());
    } catch (const std::exception& e) {
        return Option<std::string>(400, "Got malformed response from Azure API.");
    }
}

Option<nlohmann::json> AzureConversationModel::format_question(const std::string& message) {
    nlohmann::json formatted;
    formatted["role"] = "user";
    formatted["content"] = message;
    return Option<nlohmann::json>(formatted);
}

Option<nlohmann::json> AzureConversationModel::format_answer(const std::string& message) {
    nlohmann::json formatted;
    formatted["role"] = "assistant";
    formatted["content"] = message;
    return Option<nlohmann::json>(formatted);
}

bool AzureConversationModel::async_res_set_headers_callback(const std::string& response, 
                                                           const std::shared_ptr<http_req> req, 
                                                           long status_code, 
                                                           std::string& content_type) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }
    
    async_conversations[req].status_code = status_code;
    if(status_code != 200) {
        async_conversations[req].response = response;
        async_conversations[req].ready = true;
        async_conversations[req].cv.notify_one();
        return false;
    }
    
    content_type = "text/event-stream";
    return true;
}

void AzureConversationModel::async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return;
    }

    try {
        bool found_done = false;
        std::string parsed_response;
        std::regex data_regex("data: (.*?)\\n\\n");
        auto begin = std::sregex_iterator(response.begin(), response.end(), data_regex);
        auto end = std::sregex_iterator();
        
        
        // Track if we've seen any non-empty content
        bool has_content = false;
        
        for (std::sregex_iterator i = begin; i != end; ++i) {
            std::string substr_line = i->str().substr(6, i->str().size() - 8);
            
            // Handle [DONE] signal
            if(substr_line.find("[DONE]") != std::string::npos) {
                found_done = true;
                continue;  
            }
            
            // Skip empty messages
            if(substr_line.empty() || substr_line == "{}") {
                continue;
            }
            
            nlohmann::json json_line;
            try {
                json_line = nlohmann::json::parse(substr_line);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Azure callback: Failed to parse JSON: " << substr_line << " Error: " << e.what();
                continue;
            }
            
            // Skip content filter results and empty messages
            if (json_line.contains("prompt_filter_results") || 
                (json_line.contains("choices") && json_line["choices"].empty())) {
                continue;
            }

            // Skip role assignment messages
            if (json_line.contains("choices") && !json_line["choices"].empty() && 
                json_line["choices"][0].contains("delta") && 
                json_line["choices"][0]["delta"].contains("role")) {
                continue;
            }

            // Handle content chunks
            if (json_line.contains("choices") && !json_line["choices"].empty() && 
                json_line["choices"][0].contains("delta") && 
                json_line["choices"][0]["delta"].contains("content")) {
                std::string content = json_line["choices"][0]["delta"]["content"].get<std::string>();
                if (!content.empty()) {
                    parsed_response += content;
                    has_content = true;
                }
            }

            // Handle finish reason
            if (json_line.contains("choices") && !json_line["choices"].empty() && 
                json_line["choices"][0].contains("finish_reason") && 
                !json_line["choices"][0]["finish_reason"].is_null()) {
                std::string finish_reason = json_line["choices"][0]["finish_reason"].get<std::string>();
                if (finish_reason == "stop") {
                    found_done = true;
                }
            }
        }

        // Only send response if we have content
        if (has_content) {
            async_conversations[req].response += parsed_response;
            nlohmann::json json_res;
            json_res["message"] = parsed_response;
            json_res["conversation_id"] = async_conversations[req].conversation_id;
            response = "data: " + json_res.dump(-1) + "\n\n";
        } else {
            response = "";  // Don't send empty responses
        }

        // Send [DONE] if we've found it and we have content
        if(found_done && has_content) {
            response += "data: [DONE]\n\n";
            async_conversations[req].ready = true;
            async_conversations[req].cv.notify_one();
        } 

    } catch (const std::exception& e) {
        LOG(ERROR) << "Azure callback: Exception caught: " << e.what();
        LOG(ERROR) << "Azure callback: Response that caused error: " << response;
        // Set error response
        async_conversations[req].response = "{\"error\":{\"message\":\"" + std::string(e.what()) + "\"}}";
        async_conversations[req].ready = true;
        async_conversations[req].cv.notify_one();
    }
}


bool AzureConversationModel::async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
    auto& async_conversations = ConversationModel::async_conversations;
    if(async_conversations.find(req) == async_conversations.end()) {
        return false;
    }

    // Only mark as done if not already marked by write callback
    if (!async_conversations[req].ready) {
        async_conversations[req].ready = true;
        async_conversations[req].cv.notify_one();
    }
    return false;
}

Option<std::string> AzureConversationModel::get_answer_stream(const nlohmann::json& model_config,
                                                             const std::string& prompt,
                                                             const std::string& context,
                                                             const std::string& system_prompt,
                                                             const std::shared_ptr<http_req> req,
                                                             const std::shared_ptr<http_res> res,
                                                             const std::string& conversation_id) {
    const std::string model_name = EmbedderManager::get_model_name_without_namespace(model_config["model_name"].get<std::string>());
    const std::string api_key = model_config["api_key"].get<std::string>();
    const std::string azure_url = model_config["url"].get<std::string>();

    std::unordered_map<std::string, std::string> headers;
    headers["api-key"] = api_key;
    headers["Content-Type"] = "application/json";

    nlohmann::json req_body;
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
    req_body["stream"] = true;

    req->async_res_set_headers_callback = async_res_set_headers_callback;
    req->async_res_write_callback = async_res_write_callback;
    req->async_res_done_callback = async_res_done_callback;

    auto raft_server = RemoteEmbedder::get_raft_server();
    if(raft_server && !raft_server->get_leader_url().empty()) {
        auto proxy_url = raft_server->get_leader_url() + "proxy_sse";
        nlohmann::json proxy_req_body;
        proxy_req_body["method"] = "POST";
        proxy_req_body["url"] = azure_url;
        proxy_req_body["body"] = req_body.dump();
        proxy_req_body["headers"] = headers;
        std::unordered_map<std::string, std::string> header_;
        header_["x-typesense-api-key"] = HttpClient::get_api_key();

        HttpClient::get_instance().post_response_sse(proxy_url, proxy_req_body.dump(), header_, HttpProxy::default_timeout_ms, req, res, server);
    } else {
        HttpClient::get_instance().post_response_sse(azure_url, req_body.dump(), headers, HttpProxy::default_timeout_ms, req, res, server);
    }

    auto& async_conversation = async_conversations[req];
    std::unique_lock<std::mutex> lock(async_conversation.mutex);
    async_conversation.cv.wait(lock, [&async_conversation] { return async_conversation.ready; });

    if(async_conversation.status_code != 200) {
        try {
            nlohmann::json error_json = nlohmann::json::parse(async_conversation.response);
            if (error_json.contains("error") && error_json["error"].contains("message")) {
                return Option<std::string>(400, "Azure API error: " + error_json["error"]["message"].get<std::string>());
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "AzureConversationModel::get_answer_stream: Error parsing JSON: " << e.what();
        }
        return Option<std::string>(400, "Azure API error: " + async_conversation.response);
    }

    return Option<std::string>(async_conversation.response);
}
