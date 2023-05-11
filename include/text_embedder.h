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
        TextEmbedder(const std::string& model_path);
        TextEmbedder(const std::string& model_name, const std::string& api_key);
        ~TextEmbedder();
        Option<std::vector<float>> Embed(const std::string& text);
        Option<std::vector<std::vector<float>>> batch_embed(const std::vector<std::string>& inputs);
        const std::string& get_vocab_file_name() const;
        bool is_remote() {
            return remote_embedder_ != nullptr;
        }
        static bool is_model_valid(const std::string& model_path, unsigned int& num_dims);
        static Option<bool> is_model_valid(const std::string model_name, const std::string api_key, unsigned int& num_dims);
    private:
        std::unique_ptr<Ort::Session> session_;
        Ort::Env env_;
        encoded_input_t Encode(const std::string& text);
        std::unique_ptr<TextEmbeddingTokenizer> tokenizer_;
        std::unique_ptr<RemoteEmbedder> remote_embedder_;
        std::string vocab_file_name;
        static std::vector<float> mean_pooling(const std::vector<std::vector<float>>& input);
        std::string output_tensor_name;
};
