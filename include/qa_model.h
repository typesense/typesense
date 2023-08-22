#pragma once

#include <string>
#include <json.hpp>
#include "option.h"



class QAModel {
    public:
        virtual ~QAModel() {};
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config, int& conversation_id);
        static Option<nlohmann::json> parse_conversation_history(const nlohmann::json& conversation, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
    private:
};


class OpenAIQAModel : public QAModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config, int& conversation_id);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<nlohmann::json> parse_conversation_history(const nlohmann::json& conversation);
        // prevent instantiation
        OpenAIQAModel() = delete;
    private:
        static constexpr char* OPENAI_LIST_MODELS = "https://api.openai.com/v1/models";
        static constexpr char* OPENAI_CHAT_COMPLETION = "https://api.openai.com/v1/chat/completions";
};
