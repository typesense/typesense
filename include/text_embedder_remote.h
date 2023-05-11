#pragma once

#include <vector>
#include <string>
#include "http_client.h"
#include "option.h"




class RemoteEmbedder {
    public:
        virtual Option<std::vector<float>> Embed(const std::string& text) = 0;
        virtual Option<std::vector<std::vector<float>>> batch_embed(const std::vector<std::string>& inputs) = 0;
};


class OpenAIEmbedder : public RemoteEmbedder {
    private: 
        std::string api_key;
        std::string openai_model_path;
        static constexpr char* OPENAI_LIST_MODELS = "https://api.openai.com/v1/models";
        static constexpr char* OPENAI_CREATE_EMBEDDING = "https://api.openai.com/v1/embeddings";
    public:
        OpenAIEmbedder(const std::string& openai_model_path, const std::string& api_key);
        static Option<bool> is_model_valid(const std::string& model_name, const std::string& api_key, unsigned int& num_dims);
        Option<std::vector<float>> Embed(const std::string& text) override;
        Option<std::vector<std::vector<float>>> batch_embed(const std::vector<std::string>& inputs) override;
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
        static Option<bool> is_model_valid(const std::string& model_name, const std::string& api_key, unsigned int& num_dims);
        Option<std::vector<float>> Embed(const std::string& text) override;
        Option<std::vector<std::vector<float>>> batch_embed(const std::vector<std::string>& inputs) override;
};


