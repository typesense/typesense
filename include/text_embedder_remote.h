#pragma once

#include <vector>
#include <string>
#include <mutex>
#include "http_client.h"
#include "raft_server.h"
#include "option.h"
#include "lru/lru.hpp"


struct embedding_res_t {
    std::vector<float> embedding;
    nlohmann::json error = nlohmann::json::object();
    int status_code;
    bool success;

    embedding_res_t() : success(false) {} 

    embedding_res_t(const std::vector<float>& embedding) : embedding(embedding), success(true) {}

    embedding_res_t(int status_code, const nlohmann::json& error) : error(error), success(false), status_code(status_code) {}

    bool operator!=(const embedding_res_t& other) const {
        return !(*this == other);
    }

    bool operator==(const embedding_res_t& other) const {
        if(success != other.success) {
            return false;
        }
        
        if(embedding.size() != other.embedding.size()) {
            return false;
        }

        for(size_t i = 0; i < embedding.size(); i++) {
            if(embedding[i] != other.embedding[i]) {
                return false;
            }
        }
        return true;
    }
};



class RemoteEmbedder {
    protected:
        static Option<bool> validate_string_properties(const nlohmann::json& model_config, const std::vector<std::string>& properties);
        static inline ReplicationState* raft_server = nullptr;
        std::shared_mutex mutex;
    public:
        static inline LRU::Cache<std::string, embedding_res_t> cache = LRU::Cache<std::string, embedding_res_t>(100);
        static inline std::shared_mutex cache_mutex;

        static long call_remote_api(const std::string& method, const std::string& url, const std::string& req_body, std::string& res_body, std::map<std::string, std::string>& res_headers, std::unordered_map<std::string, std::string>& req_headers);
        virtual nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) = 0;
        virtual embedding_res_t embed_query(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_tries = 2) = 0;
        virtual std::vector<embedding_res_t> embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200,
                                                         const size_t remote_embedding_timeout_ms = 60000, const size_t remote_embedding_num_tries = 2) = 0;
        static const std::string get_model_key(const nlohmann::json& model_config);
        static void init(ReplicationState* rs) {
            raft_server = rs;
        }
        virtual ~RemoteEmbedder() = default;
        virtual bool update_api_key(const std::string& api_key) = 0;
        static ReplicationState* get_raft_server() {
            return raft_server;
        }
};

class AzureEmbedder : public RemoteEmbedder {
    private:
        std::string azure_url, api_key;
        bool has_custom_dims;
        size_t num_dims;

    public:
        AzureEmbedder(const std::string& azure_url, const std::string& api_key, const size_t num_dims, const bool has_custom_dims);
        static Option<bool> is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims);
        embedding_res_t embed_query(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_tries = 2) override;
        std::vector<embedding_res_t> embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200,
                                                 const size_t remote_embedding_timeout_ms = 60000, const size_t remote_embedding_num_tries = 2) override;
        nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) override;
        static std::string get_model_key(const nlohmann::json& model_config);
        bool update_api_key(const std::string& api_key) override {
            std::lock_guard<std::shared_mutex> lock(mutex);
            this->api_key = api_key;
            return true;
        }
};


class OpenAIEmbedder : public RemoteEmbedder {
    private: 
        std::string api_key;
        std::string openai_model_path;
        std::string openai_create_embedding_suffix;
        static constexpr char* OPENAI_CREATE_EMBEDDING = "v1/embeddings";
        bool has_custom_dims;
        size_t num_dims;
        std::string openai_url = "https://api.openai.com";

        static std::string get_openai_create_embedding_url(const std::string& openai_url, const std::string& openai_create_embedding_suffix = "") {
            return openai_url.back() == '/' ? openai_url + OPENAI_CREATE_EMBEDDING : openai_url + "/" + openai_create_embedding_suffix;
        }
        friend class AzureEmbedder;
        enum OpenAIEmbedderType {
            OPENAI,
            AZURE_OPENAI
        };
        static std::vector<embedding_res_t> embed_documents(const std::string url, const std::vector<std::string>& inputs, const size_t remote_embedding_timeout_ms, const size_t remote_embedding_num_tries, const std::string& api_key, const size_t num_dims, const bool has_custom_dims, const std::string& model_name, const OpenAIEmbedderType embedder_type);
        static embedding_res_t embed_query(const std::string url, const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries, const std::string& api_key, const size_t num_dims, const bool has_custom_dims, const std::string& model_name, const OpenAIEmbedderType embedder_type);
        static nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body, const std::string& url);
    public:
        OpenAIEmbedder(const std::string& openai_model_path, const std::string& api_key, const size_t num_dims, const bool has_custom_dims, const nlohmann::json& model_config);
        static Option<bool> is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims);
        embedding_res_t embed_query(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_tries = 2) override;
        std::vector<embedding_res_t> embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200,
                                                 const size_t remote_embedding_timeout_ms = 60000, const size_t remote_embedding_num_tries = 2) override;
        nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) override;
        static std::string get_model_key(const nlohmann::json& model_config);

        bool update_api_key(const std::string& apikey) override {
            std::lock_guard<std::shared_mutex> lock(mutex);
            api_key = apikey;
            return true;
        }
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
        static Option<bool> is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims);
        embedding_res_t embed_query(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_tries = 2) override;
        std::vector<embedding_res_t> embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200,
                                                 const size_t remote_embedding_timeout_ms = 60000, const size_t remote_embedding_num_tries = 2) override;
        nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) override;
        static std::string get_model_key(const nlohmann::json& model_config);
        bool update_api_key(const std::string& apikey) override {
            std::lock_guard<std::shared_mutex> lock(mutex);
            google_api_key = apikey;
            return true;
        }
};



class GCPEmbedder : public RemoteEmbedder {
    private:
        std::string project_id;
        std::string access_token;
        std::string refresh_token;
        std::string client_id;
        std::string client_secret;
        std::string model_name;
        std::string document_task;
        std::string query_task;
        std::string region;
        bool has_custom_dims;
        size_t num_dims;
        inline static const std::string GCP_EMBEDDING_BASE_URL = "-aiplatform.googleapis.com/v1/projects/";
        inline static const std::string GCP_EMBEDDING_PATH_BEFORE_REGION = "/locations/";
        inline static const std::string GCP_EMBEDDING_PATH_AFTER_REGION = "/publishers/google/models/";
        inline static const std::string GCP_EMBEDDING_PREDICT = ":predict";
        inline static const std::string GCP_AUTH_TOKEN_URL = "https://oauth2.googleapis.com/token";
        inline static const std::string GCP_DEFAULT_REGION = "us-central1";
        static Option<std::string> generate_access_token(const std::string& refresh_token, const std::string& client_id, const std::string& client_secret);
        static std::string get_gcp_embedding_url(const std::string& project_id, const std::string& model_name, const std::string& region) {
            return "https://" + region + GCP_EMBEDDING_BASE_URL + project_id + GCP_EMBEDDING_PATH_BEFORE_REGION + region + GCP_EMBEDDING_PATH_AFTER_REGION + model_name + GCP_EMBEDDING_PREDICT;
        }
    public: 
        GCPEmbedder(const std::string& project_id, const std::string& model_name, const std::string& access_token, 
                    const std::string& refresh_token, const std::string& client_id, const std::string& client_secret, const bool has_custom_dims = false, const size_t num_dims = 0,
                    const std::string& document_task = "RETRIEVAL_DOCUMENT", const std::string& query_task = "RETRIEVAL_QUERY", const std::string& region = "us-central1");
        static Option<bool> is_model_valid(const nlohmann::json& model_config, size_t& num_dims, const bool has_custom_dims);
        embedding_res_t embed_query(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_tries = 2) override;
        std::vector<embedding_res_t> embed_documents(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200,
                                                 const size_t remote_embedding_timeout_ms = 60000, const size_t remote_embedding_num_tries = 2) override;
        nlohmann::json get_error_json(const nlohmann::json& req_body, long res_code, const std::string& res_body) override;
        static std::string get_model_key(const nlohmann::json& model_config);
        bool update_api_key(const std::string& api_key) override {
            return true;
        }
};