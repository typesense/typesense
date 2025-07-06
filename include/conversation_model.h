#pragma once

#include <string>
#include <condition_variable>
#include <mutex>
#include <json.hpp>
#include "option.h"
#include "http_data.h"

struct async_conversation_t {
    std::string response;
    std::mutex mutex;
    std::condition_variable cv;
    long status_code;
    bool ready;
    std::string conversation_id;
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
        static Option<std::string> get_answer_stream(const std::string& context, const std::string& prompt, const nlohmann::json& model_config,
                                                    const std::shared_ptr<http_req>& req,
                                                    const std::shared_ptr<http_res>& res,
                                                    const std::string conversation_id);
        static std::shared_ptr<ConversationModel> get_conversation_model(const std::string& model_namespace, const nlohmann::json& model_config);
        // for testing
        static inline void _add_async_conversation(std::shared_ptr<http_req> req, const std::string& conversation_id) {
            async_conversations[req].conversation_id = conversation_id;
        }
    protected:
        static const inline std::string CONVERSATION_HISTORY = "\n\n<Conversation history>\n";
        static const inline std::string QUESTION = "\n\n<Question>\n";
        static const inline std::string STANDALONE_QUESTION_PROMPT = "\n\n<Standalone question>\n";
        static inline std::unordered_map<std::shared_ptr<http_req>, async_conversation_t> async_conversations;
};

class OpenAIConversationModel : public ConversationModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message);
        static Option<nlohmann::json> format_answer(const std::string& message);
        static Option<std::string> get_answer_stream(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config,
                                                    const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        // max_bytes must be greater than or equal to the minimum required bytes
        static const size_t get_minimum_required_bytes() {
            return  DATA_STR.size() + QUESTION_STR.size() + ANSWER_STR.size();
        }
        static const inline std::string STANDALONE_QUESTION_PROMPT = R"(
            Rewrite the follow-up question on top of a human-assistant conversation history as a standalone question that encompasses all pertinent context.
        )";
        // prevent instantiation
        OpenAIConversationModel() = delete;
    private:
        static constexpr char* OPENAI_URL = "https://api.openai.com";
        static constexpr char* OPENAI_LIST_MODELS = "/v1/models";
        static constexpr char* OPENAI_CHAT_COMPLETION = "/v1/chat/completions";
        static const inline std::string DATA_STR = "<Data>\n";
        static const inline std::string QUESTION_STR = "\n\n<Question>\n";
        static const inline std::string ANSWER_STR = "\n\n<Answer>";
        static Option<std::string> get_openai_url(const nlohmann::json& model_config);
        static Option<std::string> get_openai_path(const nlohmann::json& model_config);
        static bool async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type);
        static void async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        static bool async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
};

class CFConversationModel : public ConversationModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message);
        static Option<nlohmann::json> format_answer(const std::string& message);
        static Option<std::string> parse_stream_response(const std::string& response);
        static Option<std::string> get_answer_stream(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config,
                                                    const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        static const inline std::string STANDALONE_QUESTION_PROMPT = R"(
            Rewrite the follow-up question on top of a human-assistant conversation history as a standalone question that encompasses all pertinent context.Use 1024 characters maximum.
        )";
        static const inline std::string INFO_PROMPT = "You are an assistant for question-answering tasks. Use the following pieces of retrieved context to answer the question. If you don't know the answer, just say that you don't know. Use three sentences maximum and do not mention provided context directly, act like already knowing the context.";
        // prevent instantiation
        CFConversationModel() = delete;
        static const size_t get_minimum_required_bytes() {
            return CONTEXT_INFO.size() + SPLITTER_STR.size() + QUERY_STR.size() + ANSWER_STR.size();
        }
    private:
        static const inline std::vector<std::string> CF_MODEL_NAMES{"mistral/mistral-7b-instruct-v0.1"};
        static const std::string get_model_url(const std::string& model_name, const std::string& account_id);
        static const inline  std::string CONTEXT_INFO = "Context information is below.\n";
        static const inline std::string SPLITTER_STR = "---------------------\n";
        static const inline std::string QUERY_STR = "Given the context information and not prior knowledge, answer the query. Context is JSON format, do not return data directly, answer like a human assistant.\nQuery: ";
        static const inline std::string ANSWER_STR = "\n\nAnswer:\n";
        static bool async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type);
        static void async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        static bool async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
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
            return  DATA_STR.size() + QUESTION_STR.size() + ANSWER_STR.size();
        }
        static Option<std::string> get_answer_stream(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config,
                                                    const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
    private:
        static const std::string get_list_models_url(const std::string& url);
        static const std::string get_chat_completion_url(const std::string& url);
        static const inline std::string DATA_STR = "<Data>\n";
        static const inline std::string QUESTION_STR = "\n\n<Question>\n";
        static const inline std::string ANSWER_STR = "\n\n<Answer>";
        static bool async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type);
        static void async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        static bool async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
};


class GeminiConversationModel : public ConversationModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message);
        static Option<nlohmann::json> format_answer(const std::string& message);
        static Option<std::string> get_answer_stream(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config,
                                                    const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        // max_bytes must be greater than or equal to the minimum required bytes
        static const size_t get_minimum_required_bytes() {
            return  DATA_STR.size() + QUESTION_STR.size() + ANSWER_STR.size();
        }
        static const inline std::string STANDALONE_QUESTION_PROMPT = R"(
            Rewrite the follow-up question on top of a human-assistant conversation history as a standalone question that encompasses all pertinent context.
        )";
        // prevent instantiation
        GeminiConversationModel() = delete;
        static void _async_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
            // for testing purposes
            return async_res_write_callback(response, req, res);
        }
    private:
        static constexpr char* GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
        static constexpr char* NON_STREAM_RESPONSE_STR = ":generateContent";
        static constexpr char* STREAM_RESPONSE_STR = ":streamGenerateContent";
        static const inline std::string DATA_STR = "<Data>\n";
        static const inline std::string QUESTION_STR = "\n\n<Question>\n";
        static const inline std::string ANSWER_STR = "\n\n<Answer>";
        static Option<std::string> get_gemini_url(const nlohmann::json& model_config, const bool stream);
        static bool async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type);
        static void async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        static bool async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
};

class AzureConversationModel : public ConversationModel {
    public:
        static Option<std::string> get_answer(const std::string& context, const std::string& prompt, const std::string& system_prompt, const nlohmann::json& model_config);
        static Option<bool> validate_model(const nlohmann::json& model_config);
        static Option<std::string> get_standalone_question(const nlohmann::json& conversation_history, const std::string& question, const nlohmann::json& model_config);
        static Option<nlohmann::json> format_question(const std::string& message);
        static Option<nlohmann::json> format_answer(const std::string& message);
        static Option<std::string> get_answer_stream(const nlohmann::json& model_config,
                                                   const std::string& prompt,
                                                   const std::string& context,
                                                   const std::string& system_prompt,
                                                   const std::shared_ptr<http_req> req,
                                                   const std::shared_ptr<http_res> res,
                                                   const std::string& conversation_id);
        // max_bytes must be greater than or equal to the minimum required bytes
        static const size_t get_minimum_required_bytes() {
            return  DATA_STR.size() + QUESTION_STR.size() + ANSWER_STR.size();
        }
        static const inline std::string STANDALONE_QUESTION_PROMPT = R"(
            Rewrite the follow-up question on top of a human-assistant conversation history as a standalone question that encompasses all pertinent context.
        )";
        // prevent instantiation
        AzureConversationModel() = delete;
        static bool async_res_set_headers_callback(const std::string& response, const std::shared_ptr<http_req> req, long status_code, std::string& content_type);
        static void _async_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res) {
            // for testing purposes
            return async_res_write_callback(response, req, res);
        }
    private:
        static const inline std::string DATA_STR = "<Data>\n";
        static const inline std::string QUESTION_STR = "\n\n<Question>\n";
        static const inline std::string ANSWER_STR = "\n\n<Answer>";
        static Option<std::string> get_azure_url(const nlohmann::json& model_config);
        static void async_res_write_callback(std::string& response, const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
        static bool async_res_done_callback(const std::shared_ptr<http_req>& req, const std::shared_ptr<http_res>& res);
};