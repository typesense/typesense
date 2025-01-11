#pragma once

#include "text_embedder_remote.h"
#include <core/session/onnxruntime_cxx_api.h>
#include <vector>
#include "option.h"
#include "text_embedder_tokenizer.h"

class XTRTextEmbedder {
    public:
        XTRTextEmbedder(std::shared_ptr<Ort::Session> session, std::shared_ptr<Ort::Env> env, const std::string& vocab_path);
        ~XTRTextEmbedder();

        embedding_res_t embed(const std::string& text, const size_t max_seq_len = 512);
        std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs, const size_t max_seq_len = 512);
    private:
        std::mutex mutex_;
        size_t num_dim;

        std::shared_ptr<Ort::Session> session_;
        std::shared_ptr<Ort::Env> env_;
        encoded_input_t encode(const std::string& text, const size_t max_seq_len);
        batch_encoded_input_t batch_encode(const std::vector<std::string>& inputs, const size_t max_seq_len);
        std::unique_ptr<TextEmbeddingTokenizer> tokenizer_;
        std::string output_node_name;
};