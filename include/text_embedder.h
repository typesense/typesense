#pragma once

#include <core/session/onnxruntime_cxx_api.h>
#include <tokenizer/bert_tokenizer.hpp>
#include <vector>

struct encoded_input_t {
    std::vector<int64_t> input_ids;
    std::vector<int64_t> token_type_ids;
    std::vector<int64_t> attention_mask;
};


class TextEmbedder {
    public:
        TextEmbedder(const std::string& model_path);
        ~TextEmbedder();
        std::vector<float> Embed(const std::string& text);

        static bool is_model_valid(const std::string& model_path, unsigned int& num_dims);
    private:
        Ort::Session* session_;
        Ort::Env env_;
        encoded_input_t Encode(const std::string& text);
        BertTokenizer* tokenizer_;
        static std::vector<float> mean_pooling(const std::vector<std::vector<float>>& input);
        static std::string get_absolute_model_path(const std::string& model_path);
        std::string output_tensor_name;
};
