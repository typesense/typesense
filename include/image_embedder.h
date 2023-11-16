#pragma once

#include <memory>
#include <vector>
#include <mutex>
#include <core/session/onnxruntime_cxx_api.h>
#include "image_processor.h"
#include "text_embedder_remote.h"

enum class ImageEmbedderType {
    clip
};


class ImageEmbedder {
    public:
        virtual embedding_res_t embed(const std::string& image_encoded) = 0;
        virtual std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs)  = 0;
        virtual ~ImageEmbedder() = default;
        virtual ImageEmbedderType get_image_embedder_type() = 0;
};


class CLIPImageEmbedder : public ImageEmbedder {
    private:
        // use shared session with text embedder
        std::shared_ptr<Ort::Session> session_;
        std::shared_ptr<Ort::Env> env_;
        std::mutex mutex_;
        CLIPImageProcessor image_processor_;     
    public:
        CLIPImageEmbedder(const std::shared_ptr<Ort::Session>& session, const std::shared_ptr<Ort::Env>& env, const std::string& model_path);
        embedding_res_t embed(const std::string& image_encoded) override;
        std::vector<embedding_res_t> batch_embed(const std::vector<std::string>& inputs) override;
        virtual ImageEmbedderType get_image_embedder_type() override {
            return ImageEmbedderType::clip;
        }
};