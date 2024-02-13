#pragma once

#include <unordered_map>
#include "vq_model.h"
#include "json.hpp"
#include "embedder_manager.h"



class VQModelManager {
    private:
        inline static const std::string MODELS_REPO_URL = "https://models.typesense.org/public/";
        static const std::string get_model_url(const std::string& model_name);
        static const std::string get_config_url();
        static const Option<nlohmann::json> get_config();
        static const std::string get_absolute_model_path(const std::string& model_name);
        static const std::string get_model_namespace(const std::string& model_name);
        static const std::string get_model_name_without_namespace(const std::string& model_name);
        Option<bool> download_model(const std::string& model_name);
        std::unordered_map<std::string, std::shared_ptr<VQModel>> models;
        VQModelManager() = default;
        std::shared_mutex models_mutex;
        std::mutex download_mutex;
    public:
        static VQModelManager& get_instance() {
            static VQModelManager instance;
            return instance;
        }
        VQModelManager(VQModelManager&&) = delete;
        VQModelManager& operator=(VQModelManager&&) = delete;
        VQModelManager(const VQModelManager&) = delete;
        VQModelManager& operator=(const VQModelManager&) = delete;
        Option<std::shared_ptr<VQModel>> validate_and_init_model(const std::string& model_name);
        Option<std::shared_ptr<VQModel>> get_model(const std::string& model_name);
        void delete_model(const std::string& model_name);
        void delete_all_models();
        ~VQModelManager();  
        void clear_unused_models();
};

