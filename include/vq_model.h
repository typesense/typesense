#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <whisper.h>

#include "string_utils.h"
#include "option.h"

class VQModel {
    protected:
        int collection_ref_count = 0;
        std::shared_mutex collection_ref_count_mutex;
        std::string model_name;
    public:
        virtual ~VQModel() = default;
        virtual Option<std::string> transcribe(const std::string& audio) = 0;
        void inc_collection_ref_count() {
            std::unique_lock<std::shared_mutex> lock(collection_ref_count_mutex);
            collection_ref_count++;
        }
        void dec_collection_ref_count() {
            std::unique_lock<std::shared_mutex> lock(collection_ref_count_mutex);
            collection_ref_count--;
        }
        int get_collection_ref_count() {
            std::shared_lock<std::shared_mutex> lock(collection_ref_count_mutex);
            return collection_ref_count;
        }
        const std::string& get_model_name() {
            return model_name;
        }
        VQModel(const std::string& model_name) : model_name(model_name) {}
};


class WhisperModel : public VQModel {
    private:
        whisper_context* ctx = nullptr;
        whisper_full_params params = whisper_full_default_params(WHISPER_SAMPLING_GREEDY);
        bool read_wav(const void* data, size_t size, std::vector<float>& pcmf32);
        std::mutex mutex;
    public:
        WhisperModel() = delete;
        WhisperModel(whisper_context* ctx, const std::string& model_name);
        static whisper_context* validate_and_load_model(const std::string& model_path);
        ~WhisperModel();
        virtual Option<std::string> transcribe(const std::string& audio_base64) override;
};