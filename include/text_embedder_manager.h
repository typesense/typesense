#pragma once
#include <memory>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include <openssl/md5.h>
#include <fstream>
#include "logger.h"
#include "http_client.h"
#include "text_embedder.h"


struct text_embedding_model {
    std::string model_url;
    std::string model_md5;
    std::string vocab_url;
    std::string vocab_md5;
};


// singleton class
class TextEmbedderManager {
public:
    static TextEmbedderManager& get_instance() {
        static TextEmbedderManager instance;
        return instance;
    }

    TextEmbedderManager(TextEmbedderManager&&) = delete;
    TextEmbedderManager& operator=(TextEmbedderManager&&) = delete;
    TextEmbedderManager(const TextEmbedderManager&) = delete;
    TextEmbedderManager& operator=(const TextEmbedderManager&) = delete;

    TextEmbedder* get_text_embedder(const nlohmann::json& model_parameters) {
        std::unique_lock<std::mutex> lock(text_embedders_mutex);
        const std::string& model_name = model_parameters.count("model_name") == 0 ? DEFAULT_MODEL_NAME : model_parameters.at("model_name");
        if(text_embedders[model_name] == nullptr) {
            if(model_parameters.count("api_key") == 0) {
                // download the model if it doesn't exist
                if(is_public_model(model_name)) {
                    download_public_model(model_name);
                }
                text_embedders[model_name] = std::make_shared<TextEmbedder>(model_name);
            } else {
                text_embedders[model_name] = std::make_shared<TextEmbedder>(model_name, model_parameters.at("api_key"));
            }
        }
        return text_embedders[model_name].get();
    }

    void delete_text_embedder(const std::string& model_path) {
        std::unique_lock<std::mutex> lock(text_embedders_mutex);
        if (text_embedders.find(model_path) != text_embedders.end()) {
            text_embedders.erase(model_path);
        }
    }

    void delete_all_text_embedders() {
        std::unique_lock<std::mutex> lock(text_embedders_mutex);
        text_embedders.clear();
    }

    static const std::string get_indexing_prefix(const nlohmann::json& model_parameters) {
        std::string val = model_parameters.count("model_name") == 0 ? DEFAULT_MODEL_INDEXING_PREFIX : model_parameters["indexing_prefix"].get<std::string>();
        if(!val.empty()) {
            val += " ";
        }

        return val;
    }

    static const std::string get_query_prefix(const nlohmann::json& model_parameters) {
        std::string val = model_parameters.count("model_name") == 0 ? DEFAULT_MODEL_QUERY_PREFIX : model_parameters["query_prefix"].get<std::string>();
        if(!val.empty()) {
            val += " ";
        }

        return val;
    }

    static void set_model_dir(const std::string& dir) {
        // create the directory if it doesn't exist
        if(!std::filesystem::exists(dir)) {
            std::filesystem::create_directories(dir);
        }
        model_dir = dir;
    }

    static const std::string& get_model_dir() {
        return model_dir;
    }

    ~TextEmbedderManager() {
        delete_all_text_embedders();
    }

    static constexpr char* DEFAULT_MODEL_NAME = "ts-e5-small";
    static constexpr char* DEFAULT_MODEL_INDEXING_PREFIX = "passage:";
    static constexpr char* DEFAULT_MODEL_QUERY_PREFIX = "query:";
    inline static std::string model_dir = "";
    inline static const std::string get_absolute_model_path(const std::string& model_name) {
        if(model_dir.back() != '/') {
            // create subdir <model_name> if it doesn't exist
            if(!std::filesystem::exists(model_dir + "/" + model_name)) {
                std::filesystem::create_directories(model_dir + "/" + model_name);
            }
            return model_dir + "/" + model_name + "/" + "model.onnx";
        } else {
            // create subdir <model_name> if it doesn't exist
            if(!std::filesystem::exists(model_dir + model_name)) {
                std::filesystem::create_directories(model_dir + model_name);
            }
            return model_dir + model_name + "/" + "model.onnx";
        }
    };
    inline static const std::string get_absolute_vocab_path(const std::string& model_name) {
        if(model_dir.back() != '/') {
            // create subdir <model_name> if it doesn't exist
            if(!std::filesystem::exists(model_dir + "/" + model_name)) {
                std::filesystem::create_directories(model_dir + "/" + model_name);
            }
            return model_dir + "/" + model_name + "/" + "vocab.txt";
        } else {
            // create subdir <model_name> if it doesn't exist
            if(!std::filesystem::exists(model_dir + model_name)) {
                std::filesystem::create_directories(model_dir + model_name);
            }
            return model_dir + model_name + "/" + "vocab.txt";
        }
    }
    inline static const bool check_md5(const std::string& file_path, const std::string& target_md5) {
        std::ifstream stream(file_path);
        if (stream.fail()) {
            return false;
        }
        unsigned char md5[MD5_DIGEST_LENGTH];
        std::stringstream ss,res;
        ss << stream.rdbuf();
        MD5((unsigned char*)ss.str().c_str(), ss.str().length(), md5);
        for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
            res << std::hex << (int)md5[i];
        }
        return res.str() == target_md5;
    }
     void download_public_model(const std::string& model_name) {
        HttpClient& httpClient = HttpClient::get_instance();
        auto model = public_models[model_name];
        if(!check_md5(get_absolute_model_path(model_name), model.model_md5)) {
            LOG(INFO) << "Downloading public model: " << model_name;
            long res = httpClient.download_file(model.model_url, get_absolute_model_path(model_name));
            if(res != 200) {
                LOG(INFO) << "Failed to download public model " << model_name << ": " << res;
            }
        }
        if(!check_md5(get_absolute_vocab_path(model_name), model.vocab_md5)) {
            LOG(INFO) << "Downloading default vocab for model: " << model_name;
            long res = httpClient.download_file(model.vocab_url, get_absolute_vocab_path(model_name));
            if(res != 200) {
                LOG(INFO) << "Failed to download default vocab " << model_name << ": " << res;
            }
        }
    }

    const bool is_public_model(const std::string& model_name) {
        return public_models.find(model_name) != public_models.end();
    }
private:
    TextEmbedderManager() {
        public_models[DEFAULT_MODEL_NAME] = text_embedding_model{
                                                                "https://models.typesense.org/public/e5-small/model.onnx",
                                                                "3d421dc72859a72368c106415cdebf2",
                                                                "https://models.typesense.org/public/e5-small/vocab.txt",
                                                                "6480d5d8528ce344256daf115d4965e"};
        public_models["ts-all-MiniLM-L12-v2"] = text_embedding_model{
                                                                "https://models.typesense.org/public/all-miniLM-L12-v2/model.onnx",
                                                                "6d196b8f7a8d8abcfb08afcac1704302",
                                                                "https://models.typesense.org/public/all-miniLM-L12-v2/vocab.txt",
                                                                "9d2131a9a433502abeb512978d452ecc"};
        public_models["ts-distiluse-base-multilingual-cased-v2"] = text_embedding_model{
                                                                "https://models.typesense.org/public/distiluse-base-multilingual-cased-v2/model.onnx",
                                                                "91f535cd7ca87359b9bfd1d87f9e452e",
                                                                "https://models.typesense.org/public/distiluse-base-multilingual-cased-v2/vocab.txt",
                                                                "0f05e1b7420dea2db8ade63eb5b80f7a"};
    }
    std::unordered_map<std::string, std::shared_ptr<TextEmbedder>> text_embedders;
    std::unordered_map<std::string, text_embedding_model> public_models;
    std::mutex text_embedders_mutex;
};




