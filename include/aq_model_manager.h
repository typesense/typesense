#pragma once

#include <unordered_map>
#include "aq_model.h"
#include "json.hpp"
#include "embedder_manager.h"



class AQModelManager {
    private:
        inline static const std::string MODELS_REPO_URL = "https://models.typesense.org/public/";
        static const std::string get_model_url(const std::string& model_name);
        static const std::string get_config_url();
        static const Option<nlohmann::json> get_config();
        static const std::string get_absolute_model_path(const std::string& model_name);
        static const std::string get_model_namespace(const std::string& model_name);
        Option<bool> download_model(const std::string& model_name);
        std::unordered_map<std::string, std::shared_ptr<AQModel>> models;
        AQModelManager() = default;
        std::shared_mutex models_mutex;
        std::mutex download_mutex;
    public:
        static AQModelManager& get_instance() {
            static AQModelManager instance;
            return instance;
        }
        AQModelManager(AQModelManager&&) = delete;
        AQModelManager& operator=(AQModelManager&&) = delete;
        AQModelManager(const AQModelManager&) = delete;
        AQModelManager& operator=(const AQModelManager&) = delete;
        Option<std::shared_ptr<AQModel>> validate_and_init_model(const std::string& model_name);
        Option<std::shared_ptr<AQModel>> get_model(const std::string& model_name);
        void delete_model(const std::string& model_name);
        void delete_all_models();
        ~AQModelManager();  
        void clear_unused_models();
};

