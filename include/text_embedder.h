#pragma once

#include <sentencepiece_processor.h>
#include <core/session/onnxruntime_cxx_api.h>
#include <tokenizer/bert_tokenizer.hpp>
#include <vector>
#include "option.h"
#include "text_embedder_tokenizer.h"
#include "text_embedder_remote.h"


class TextEmbedder {
    public:
        // Constructor for local or public models
        TextEmbedder(const std::string& model_path);
        // Constructor for remote models
        TextEmbedder(const nlohmann::json& model_config);
        ~TextEmbedder();
        embedding_res_t Embed(const std::string& text, const size_t remote_embedder_timeout_ms = 30000, const size_t remote_embedding_num_try = 2);
        std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size = 200);
        const std::string& get_vocab_file_name() const;
        bool is_remote() {
            return remote_embedder_ != nullptr;
        }
        Option<bool> validate(size_t& num_dims);
    private:
        std::unique_ptr<Ort::Session> session_;
        Ort::Env env_;
        encoded_input_t Encode(const std::string& text);
        batch_encoded_input_t batch_encode(const std::vector<std::string>& inputs);
        std::unique_ptr<TextEmbeddingTokenizer> tokenizer_;
        std::unique_ptr<RemoteEmbedder> remote_embedder_;
        std::string vocab_file_name;
        static std::vector<float> mean_pooling(const std::vector<std::vector<float>>& input);
        std::string output_tensor_name;
        std::mutex mutex_;
};
