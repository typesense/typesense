#pragma once

#include <string>
#include <condition_variable>
#include <mutex>
#include <json.hpp>
#include "option.h"

struct async_conversation_t {
    std::string response;
    std::mutex mutex;
    std::condition_variable cv;
};



class ConversationModel {
    public:
        virtual ~ConversationModel() {};
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_answer(const std::string& message, const nlohmann::json& model_config);
    private:
};

class OpenAIConversationModel : public ConversationModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message);
        static Option<nlohmann::json> format_answer(const std::string& message);
        static const inline std::string STANDALONE_QUESTION_PROMPT = R"(
            Rewrite the follow-up question on top of a human-assistant conversation history as a standalone question that encompasses all pertinent context.
        )";
        // prevent instantiation
        OpenAIConversationModel() = delete;
    private:
        static constexpr char* OPENAI_LIST_MODELS = "https://api.openai.com/v1/models";
        static constexpr char* OPENAI_CHAT_COMPLETION = "https://api.openai.com/v1/chat/completions";
};

class CFConversationModel : public ConversationModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message);
        static Option<nlohmann::json> format_answer(const std::string& message);
        static const inline std::string STANDALONE_QUESTION_PROMPT = R"(
            [INST]Rewrite the follow-up question on top of a human-assistant conversation history as a standalone question that encompasses all pertinent context.[/INST]
        )";
        static const inline std::string INFO_PROMPT = "[INST] You are an assistant for question-answering tasks. Use the following pieces of retrieved context to answer the question. If you don't know the answer, just say that you don't know. Use three sentences maximum and keep the answer concise. [/INST]";
        // prevent instantiation
        CFConversationModel() = delete;
    private:
        static const inline std::vector<std::string> CF_MODEL_NAMES{"mistral/mistral-7b-instruct-v0.1"};
        static const std::string get_model_url(const std::string& model_name, const std::string& account_id);
};
