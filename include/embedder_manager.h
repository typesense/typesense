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
#include "image_embedder.h"

struct text_embedding_model {
    std::string model_name;
    std::string model_md5;
    std::string vocab_file_name;
    std::string vocab_md5;
    std::string data_file_md5;
    std::string tokenizer_md5;
    std::string tokenizer_file_name;
    std::string image_processor_md5;
    std::string image_processor_file_name;
    TokenizerType tokenizer_type;
    std::string indexing_prefix = "";
    std::string query_prefix = "";
    bool has_image_embedder = false;

    text_embedding_model(const nlohmann::json& json);

    text_embedding_model() = default;
};

// Singleton class
class EmbedderManager {
public:
    static EmbedderManager& get_instance();
    
    EmbedderManager(EmbedderManager&&) = delete;
    EmbedderManager& operator=(EmbedderManager&&) = delete;
    EmbedderManager(const EmbedderManager&) = delete;
    EmbedderManager& operator=(const EmbedderManager&) = delete;

    Option<TextEmbedder*> get_text_embedder(const nlohmann::json& model_config);
    Option<ImageEmbedder*> get_image_embedder(const nlohmann::json& model_config);

    void delete_text_embedder(const std::string& model_path);
    void delete_all_text_embedders();

    void delete_image_embedder(const std::string& model_path);
    void delete_all_image_embedders();

    static const TokenizerType get_tokenizer_type(const nlohmann::json& model_config);
    const std::string get_indexing_prefix(const nlohmann::json& model_config);
    const std::string get_query_prefix(const nlohmann::json& model_config);
    static void set_model_dir(const std::string& dir);
    static const std::string& get_model_dir();

    ~EmbedderManager();

    inline static const std::string MODELS_REPO_URL = "https://models.typesense.org/public/";
    inline static const std::string MODEL_CONFIG_FILE = "config.json";
    inline static std::string model_dir = "";

    static const std::string get_absolute_model_path(const std::string& model_name);
    static const std::string get_absolute_vocab_path(const std::string& model_name, const std::string& vocab_file_name);
    static const std::string get_absolute_config_path(const std::string& model_name);
    static const std::string get_model_url(const text_embedding_model& model);
    static const std::string get_model_data_url(const text_embedding_model& model);
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

    Option<bool> validate_and_init_remote_model(const nlohmann::json& model_config, size_t& num_dims);
    Option<bool> validate_and_init_local_model(const nlohmann::json& model_config, size_t& num_dims);
    Option<bool> validate_and_init_model(const nlohmann::json& model_config, size_t& num_dims);

    std::unordered_map<std::string, std::shared_ptr<TextEmbedder>> _get_text_embedders() {
        return text_embedders;
    }

private:
    EmbedderManager() = default;

    std::unordered_map<std::string, std::shared_ptr<TextEmbedder>> text_embedders;
    std::unordered_map<std::string, std::shared_ptr<ImageEmbedder>> image_embedders;
    std::unordered_map<std::string, text_embedding_model> public_models;
    std::mutex text_embedders_mutex, image_embedders_mutex;

    static Option<std::string> get_namespace(const std::string& model_name);
};

