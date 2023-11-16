#pragma once


#include <mutex>
#include <vector>
#include <core/session/onnxruntime_cxx_api.h>
#include "string_utils.h"
#include "option.h"

// processed_image_t is 4D vector of floats
using processed_image_t = std::vector<float>;
class ImageProcessor {
    public:
        virtual ~ImageProcessor() = default;
        virtual Option<processed_image_t> process_image(const std::string& image) = 0;
};


class CLIPImageProcessor : public ImageProcessor {
    private:
        Ort::Env env_;
        std::unique_ptr<Ort::Session> session_;
        std::mutex mutex_;

    public:
        CLIPImageProcessor(const std::string& model_path);
        Option<processed_image_t> process_image(const std::string& image) override;  
};