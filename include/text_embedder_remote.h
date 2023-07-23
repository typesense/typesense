#pragma once

#include <vector>
#include <string>
#include <mutex>
#include "http_client.h"
#include "raft_server.h"
#include "option.h"



struct embedding_res_t {
    std::vector<float> embedding;
    nlohmann::json error = nlohmann::json::object();
    int status_code;
    bool success;

    embedding_res_t(const std::vector<float>& embedding) : embedding(embedding), success(true) {}

    embedding_res_t(int status_code, const nlohmann::json& error) : error(error), success(false), status_code(status_code) {}
};



class RemoteEmbedder {
    protected:
        static Option<bool> validate_string_properties(const nlohmann::json& model_config, const std::vector<std::string>& properties);
        static long call_remote_api(const std::string& method, const std::string& url, const std::string& req_body, std::string& res_body, std::map<std::string, std::string>& res_headers, std::unordered_map<std::string, std::string>& req_headers);
        static inline ReplicationState* raft_server = nullptr;
    public:
        virtual nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) = 0;
        virtual embedding_res_t Embed(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_try = 2) = 0;
        virtual std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200) = 0;
        static void init(ReplicationState* rs) {
            raft_server = rs;
        }
        virtual ~RemoteEmbedder() = default;

};


class OpenAIEmbedder : public RemoteEmbedder {
    private: 
        std::string api_key;
        std::string openai_model_path;
        static constexpr char* OPENAI_LIST_MODELS = "https://api.openai.com/v1/models";
        static constexpr char* OPENAI_CREATE_EMBEDDING = "https://api.openai.com/v1/embeddings";
    public:
        OpenAIEmbedder(const std::string& openai_model_path, const std::string& api_key);
        static Option<bool> is_model_valid(const nlohmann::json& model_config, size_t& num_dims);
        embedding_res_t Embed(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_try = 2) override;
        std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200) override;
        nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) override;
};


class GoogleEmbedder : public RemoteEmbedder {
    private:
        // only support this model for now
        inline static const char* SUPPORTED_MODEL = "embedding-gecko-001";
        inline static constexpr short GOOGLE_EMBEDDING_DIM = 768;
        inline static constexpr char* GOOGLE_CREATE_EMBEDDING = "https://generativelanguage.googleapis.com/v1beta2/models/embedding-gecko-001:embedText?key=";
        std::string google_api_key;
        
    public:
        GoogleEmbedder(const std::string& google_api_key);
        static Option<bool> is_model_valid(const nlohmann::json& model_config, size_t& num_dims);
        embedding_res_t Embed(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_try = 2) override;
        std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200) override;
        nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) override;
};



class GCPEmbedder : public RemoteEmbedder {
    private:
        std::string project_id;
        std::string access_token;
        std::string refresh_token;
        std::string client_id;
        std::string client_secret;
        std::string model_name;
        inline static const std::string GCP_EMBEDDING_BASE_URL = "https://us-central1-aiplatform.googleapis.com/v1/projects/";
        inline static const std::string GCP_EMBEDDING_PATH = "/locations/us-central1/publishers/google/models/";
        inline static const std::string GCP_EMBEDDING_PREDICT = ":predict";
        inline static const std::string GCP_AUTH_TOKEN_URL = "https://oauth2.googleapis.com/token";
        static Option<std::string> generate_access_token(const std::string& refresh_token, const std::string& client_id, const std::string& client_secret);
        static std::string get_gcp_embedding_url(const std::string& project_id, const std::string& model_name) {
            return GCP_EMBEDDING_BASE_URL + project_id + GCP_EMBEDDING_PATH + model_name + GCP_EMBEDDING_PREDICT;
        }
    public: 
        GCPEmbedder(const std::string& project_id, const std::string& model_name, const std::string& access_token, 
                    const std::string& refresh_token, const std::string& client_id, const std::string& client_secret);
        static Option<bool> is_model_valid(const nlohmann::json& model_config, size_t& num_dims);
        embedding_res_t Embed(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_try = 2) override;
        std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200) override;
        nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) override;
};


