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
        static Option<size_t> get_minimum_required_bytes(const nlohmann::json& model_config);
    protected:
        static const inline std::string CONVERSATION_HISTORY = "\n\n<Conversation history>\n";
        static const inline std::string QUESTION = "\n\n<Question>\n";
        static const inline std::string STANDALONE_QUESTION_PROMPT = "\n\n<Standalone question>\n";
};

class OpenAIConversationModel : public ConversationModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message);
        static Option<nlohmann::json> format_answer(const std::string& message);
        // max_bytes must be greater than or equal to the minimum required bytes
        static const size_t get_minimum_required_bytes() {
            return std::string("<Data>\n").size() + std::string("\n\n<Question>\n").size() + std::string("\n\n<Answer>").size();
        }
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
            Rewrite the follow-up question on top of a human-assistant conversation history as a standalone question that encompasses all pertinent context.Use 1024 characters maximum.
        )";
        static const inline std::string INFO_PROMPT = "You are an assistant for question-answering tasks. Use the following pieces of retrieved context to answer the question. If you don't know the answer, just say that you don't know. Use three sentences maximum and do not mention provided context directly, act like already knowing the context.";
        // prevent instantiation
        CFConversationModel() = delete;
        static const size_t get_minimum_required_bytes() {
            return std::string(R"(
                Context information is below.
                ---------------------
                )").size() + std::string(R"(
                ---------------------
                Given the context information and not prior knowledge, answer the query. Context is JSON format, do not return data directly, answer like a human assistant.
                Query: )").size() + std::string(R"(
        
                Answer:
                )").size();
        }
    private:
        static const inline std::vector<std::string> CF_MODEL_NAMES{"mistral/mistral-7b-instruct-v0.1"};
        static const std::string get_model_url(const std::string& model_name, const std::string& account_id);
};

class vLLMConversationModel : public ConversationModel {
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
        vLLMConversationModel() = delete;
        // max_bytes must be greater than or equal to the minimum required bytes
        static const size_t get_minimum_required_bytes() {
            return std::string("<Data>\n").size() + std::string("\n\n<Question>\n").size() + std::string("\n\n<Answer>").size();
        }
    private:
        static const std::string get_list_models_url(const std::string& url);
        static const std::string get_chat_completion_url(const std::string& url);
};