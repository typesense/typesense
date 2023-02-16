#pragma once


#include <unordered_map>

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

    TextEmbedder* get_text_embedder(const std::string& model_path) {
        if (text_embedders.find(model_path) == text_embedders.end()) {
            text_embedders[model_path] = new TextEmbedder(model_path);
        }
        return text_embedders[model_path];
    }

    void delete_text_embedder(const std::string& model_path) {
        if (text_embedders.find(model_path) != text_embedders.end()) {
            delete text_embedders[model_path];
            text_embedders.erase(model_path);
        }
    }

    void delete_all_text_embedders() {
        for (auto& text_embedder : text_embedders) {
            delete text_embedder.second;
        }
        text_embedders.clear();
    }

    static void set_model_dir(const std::string& dir) {
        model_dir = dir;
    }

    static const std::string& get_model_dir() {
        return model_dir;
    }

    ~TextEmbedderManager() {
        delete_all_text_embedders();
    }

    static constexpr char* DEFAULT_MODEL_URL = "https://huggingface.co/ozanarmagan/e5-small-onnx/resolve/main/model.onnx";
    static constexpr char* DEFAULT_MODEL_NAME = "model.onnx";
    static constexpr char* DEFAULT_VOCAB_URL = "https://huggingface.co/ozanarmagan/e5-small-onnx/resolve/main/vocab.txt";
    static constexpr char* DEFAULT_VOCAB_NAME = "vocab.txt";
    inline static std::string model_dir = "";
private:
    TextEmbedderManager() = default;
    std::unordered_map<std::string, TextEmbedder*> text_embedders;
};




