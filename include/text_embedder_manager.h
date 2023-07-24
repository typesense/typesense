#pragma once

#include <memory>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include <openssl/md5.h>
#include <fstream>
#include "logger.h"
#include "http_client.h"
#include "option.h"
#include "text_embedder.h"

struct text_embedding_model {
    std::string model_name;
    std::string model_md5;
    std::string vocab_file_name;
    std::string vocab_md5;
    TokenizerType tokenizer_type;
    std::string indexing_prefix = "";
    std::string query_prefix = "";

    text_embedding_model(const nlohmann::json& json);

    text_embedding_model() = default;
};

// Singleton class
class TextEmbedderManager {
public:
    static TextEmbedderManager& get_instance();
    
    TextEmbedderManager(TextEmbedderManager&&) = delete;
    TextEmbedderManager& operator=(TextEmbedderManager&&) = delete;
    TextEmbedderManager(const TextEmbedderManager&) = delete;
    TextEmbedderManager& operator=(const TextEmbedderManager&) = delete;

    Option<TextEmbedder*> get_text_embedder(const nlohmann::json& model_config);
    Option<bool> init_text_embedder(const nlohmann::json& model_config, size_t& num_dim);

    void delete_text_embedder(const std::string& model_path);
    void delete_all_text_embedders();

    static const TokenizerType get_tokenizer_type(const nlohmann::json& model_config);
    const std::string get_indexing_prefix(const nlohmann::json& model_config);
    const std::string get_query_prefix(const nlohmann::json& model_config);
    static void set_model_dir(const std::string& dir);
    static const std::string& get_model_dir();

    ~TextEmbedderManager();

    inline static const std::string MODELS_REPO_URL = "https://models.typesense.org/public/";
    inline static const std::string MODEL_CONFIG_FILE = "config.json";
    inline static std::string model_dir = "";

    static const std::string get_absolute_model_path(const std::string& model_name);
    static const std::string get_absolute_vocab_path(const std::string& model_name, const std::string& vocab_file_name);
    static const std::string get_absolute_config_path(const std::string& model_name);
    static const std::string get_model_url(const text_embedding_model& model);
    static const std::string get_vocab_url(const text_embedding_model& model);
    static Option<nlohmann::json> get_public_model_config(const std::string& model_name);
    static const std::string get_model_name_without_namespace(const std::string& model_name);
    static const std::string get_model_namespace(const std::string& model_name);
    static const std::string get_model_subdir(const std::string& model_name);
    static const bool check_md5(const std::string& file_path, const std::string& target_md5);
    Option<bool> download_public_model(const text_embedding_model& model);

    Option<bool> init_public_model(const std::string& model_name);
    bool is_public_model(const std::string& model_name);
    static bool is_remote_model(const std::string& model_name);

    static Option<bool> validate_and_init_remote_model(const nlohmann::json& model_config, size_t& num_dims);
    static Option<bool> validate_and_init_local_model(const nlohmann::json& model_config, size_t& num_dims);
    static Option<bool> validate_and_init_model(const nlohmann::json& model_config, size_t& num_dims);

private:
    TextEmbedderManager() = default;

    std::unordered_map<std::string, std::shared_ptr<TextEmbedder>> text_embedders;
    std::unordered_map<std::string, text_embedding_model> public_models;
    std::mutex text_embedders_mutex;

    static Option<std::string> get_namespace(const std::string& model_name);
};

