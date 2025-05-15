#pragma once
#include <string>
#include <vector>
#include <map>
#include "embedder_manager.h"
#include <json.hpp>
#include <memory>
#include <mutex>
#include <core/session/onnxruntime_cxx_api.h>
#include "text_embedder_remote.h" // Added for embedding_res_t

class PersonalizationModel {
public:
    static inline const std::map<std::string, std::vector<std::string>> valid_model_names = {
        {"recommendation", {"tyrec-1"}},
        {"search", {"tyrec-2"}}
    };

    PersonalizationModel(const std::string& model_id);
    ~PersonalizationModel();

    static std::string get_model_subdir(const std::string& model_id);
    static Option<bool> validate_model(const nlohmann::json& model_json);
    static Option<nlohmann::json> create_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data);
    static Option<nlohmann::json> update_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data);
    static Option<bool> delete_model(const std::string& model_id);

    size_t get_num_dims() const { return num_dims_; }
    embedding_res_t embed_recommendations(const std::vector<std::vector<float>>& input_vector, const std::vector<int64_t>& user_mask);
    embedding_res_t embed_user(const std::vector<std::string>& features);
    embedding_res_t embed_item(const std::vector<std::string>& features);
    std::vector<embedding_res_t> batch_embed_recommendations(const std::vector<std::vector<std::vector<float>>>& input_vectors, const std::vector<std::vector<int64_t>>& user_masks);
    std::vector<embedding_res_t> batch_embed_users(const std::vector<std::vector<std::string>>& features);
    std::vector<embedding_res_t> batch_embed_items(const std::vector<std::vector<std::string>>& features);
    batch_encoded_input_t encode_features(const std::vector<std::string>& features);
    std::vector<batch_encoded_input_t> encode_batch(const std::vector<std::vector<std::string>>& batch_features);
    Option<bool> validate_model_io();

private:
    std::string recommendation_model_path_;
    std::string user_model_path_;
    std::string item_model_path_;
    std::string model_id_;
    size_t num_dims_;
    std::shared_ptr<Ort::Env> env_;
    std::shared_ptr<Ort::Session> recommendation_session_;
    std::shared_ptr<Ort::Session> user_session_;
    std::shared_ptr<Ort::Session> item_session_;
    std::mutex recommendation_mutex_;
    std::mutex user_mutex_;
    std::mutex item_mutex_;
    std::string query_prompt_;
    std::string item_prompt_;
    std::string vocab_path_;
    std::string prompt_path_;
    std::unique_ptr<TextEmbeddingTokenizer> tokenizer_;
    void initialize_session();
};