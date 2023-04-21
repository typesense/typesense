#pragma once

#include <core/session/onnxruntime_cxx_api.h>
#include <tokenizer/bert_tokenizer.hpp>
#include <vector>
#include "option.h"

struct encoded_input_t {
    std::vector<int64_t> input_ids;
    std::vector<int64_t> token_type_ids;
    std::vector<int64_t> attention_mask;
};


class TextEmbedder {
    public:
        TextEmbedder(const std::string& model_path);
        TextEmbedder(const std::string& openai_model_path, const std::string& openai_api_key);
        ~TextEmbedder();
        Option<std::vector<float>> Embed(const std::string& text);
        Option<std::vector<std::vector<float>>> batch_embed(const std::vector<std::string>& inputs);

        bool is_openai() {
            return !openai_api_key.empty();
        }

        static bool is_model_valid(const std::string& model_path, unsigned int& num_dims);
        static Option<bool> is_model_valid(const std::string openai_model_path, const std::string openai_api_key, unsigned int& num_dims);
    private:
        std::unique_ptr<Ort::Session> session_;
        Ort::Env env_;
        encoded_input_t Encode(const std::string& text);
        std::unique_ptr<BertTokenizer> tokenizer_;
        static std::vector<float> mean_pooling(const std::vector<std::vector<float>>& input);
        std::string output_tensor_name;
        std::string openai_api_key;
        std::string openai_model_path;
        static constexpr char* OPENAI_LIST_MODELS = "https://api.openai.com/v1/models";
        static constexpr char* OPENAI_CREATE_EMBEDDING = "https://api.openai.com/v1/embeddings";
};
