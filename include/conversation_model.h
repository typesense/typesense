#pragma once

#include <string>
#include <json.hpp>
#include "option.h"



class ConversationModel {
    public:
        virtual ~ConversationModel() {};
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<nlohmann::json> parse_conversation_history(const nlohmann::json& conversation, const nlohmann::json& model_config);
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
        static Option<nlohmann::json> parse_conversation_history(const nlohmann::json& conversation);
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
