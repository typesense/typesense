#pragma once
#include <memory>
#include <filesystem>
#include <unordered_map>
#include <openssl/md5.h>
#include <fstream>
#include "logger.h"
#include "http_client.h"
#include "text_embedder.h"


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
        if(model_parameters.count("model_name") == 0) {
            if(text_embedders[DEFAULT_MODEL_NAME] == nullptr) {
                text_embedders[DEFAULT_MODEL_NAME] = std::make_shared<TextEmbedder>(DEFAULT_MODEL_NAME);
            }
            return text_embedders[DEFAULT_MODEL_NAME].get();
        } else {
            const std::string& model_name = model_parameters.at("model_name");
            if(text_embedders[model_name] == nullptr) {
                if(model_parameters.count("openai_api_key") == 0) {
                    text_embedders[model_name] = std::make_shared<TextEmbedder>(model_name);
                } else {
                    text_embedders[model_name] = std::make_shared<TextEmbedder>(model_name, model_parameters.at("openai_api_key"));
                }
            }
            return text_embedders[model_name].get();
        }

    }

    void delete_text_embedder(const std::string& model_path) {
        if (text_embedders.find(model_path) != text_embedders.end()) {
            text_embedders.erase(model_path);
        }
    }

    void delete_all_text_embedders() {
        text_embedders.clear();
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

    static constexpr char* DEFAULT_MODEL_URL = "https://huggingface.co/typesense/models/resolve/main/e5-small/model.onnx";
    static constexpr char* DEFAULT_MODEL_NAME = "ts-e5-small";
    static constexpr char* DEFAULT_VOCAB_URL = "https://huggingface.co/typesense/models/resolve/main/e5-small/vocab.txt";
    static constexpr char* DEFAULT_VOCAB_NAME = "vocab.txt";
    static constexpr char* DEFAULT_VOCAB_MD5 = "6480d5d8528ce344256daf115d4965e";
    static constexpr char* DEFAULT_MODEL_MD5 = "3d421dc72859a72368c106415cdebf2";
    inline static std::string model_dir = "";
    inline static const std::string get_absolute_model_path(const std::string& model_name) {
        if(model_dir.back() != '/') {
            if(model_name.front() != '/') {
                return model_dir + "/" + model_name + ".onnx";
            } else {
                return model_dir + model_name + ".onnx";
            }
        } else {
            if(model_name.front() != '/') {
                return model_dir + model_name + ".onnx";
            } else {
                return model_dir + "/" + model_name + ".onnx";
            }
        }
    };
    inline static const std::string get_absolute_vocab_path() {
        if(model_dir.back() != '/') {
            return model_dir + "/" + TextEmbedderManager::DEFAULT_VOCAB_NAME;
        } else {
            return model_dir + TextEmbedderManager::DEFAULT_VOCAB_NAME;
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
    inline static void download_default_model() {
        HttpClient& httpClient = HttpClient::get_instance();
        if(!check_md5(get_absolute_model_path(DEFAULT_MODEL_NAME), DEFAULT_MODEL_MD5)) {
            LOG(INFO) << "Downloading default model";
            long res = httpClient.download_file(DEFAULT_MODEL_URL, get_absolute_model_path(DEFAULT_MODEL_NAME));
            if(res != 200) {
                LOG(INFO) << "Failed to download default model: " << res;
            }
        }
        if(!check_md5(get_absolute_vocab_path(), DEFAULT_VOCAB_MD5)) {
            LOG(INFO) << "Downloading default vocab";
            long res = httpClient.download_file(DEFAULT_VOCAB_URL, get_absolute_vocab_path());
            if(res != 200) {
                LOG(INFO) << "Failed to download default vocab: " << res;
            }
        }
    }
private:
    TextEmbedderManager() = default;
    std::unordered_map<std::string, std::shared_ptr<TextEmbedder>> text_embedders;
};




