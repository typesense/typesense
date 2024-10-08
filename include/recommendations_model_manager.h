#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <json.hpp>
#include "option.h"
#include "store.h"
#include "recommendations_model.h"

class RecommendationsModelManager {
public:
    RecommendationsModelManager() = delete;
    RecommendationsModelManager(const RecommendationsModelManager&) = delete;
    RecommendationsModelManager(RecommendationsModelManager&&) = delete;
    RecommendationsModelManager& operator=(const RecommendationsModelManager&) = delete;

    static Option<nlohmann::json> get_model(const std::string& model_id);
    static Option<std::string> add_model(nlohmann::json& model, const std::string& model_id, const bool write_to_disk);
    static Option<nlohmann::json> delete_model(const std::string& model_id);
    static Option<nlohmann::json> get_all_models();
    static Option<int> init(Store* store);

private:
    static inline std::unordered_map<std::string, nlohmann::json> models;
    static inline std::shared_mutex models_mutex;
    static inline Store* store;
    static constexpr const char* MODEL_KEY_PREFIX = "$REMP";

    static const std::string get_model_key(const std::string& model_id);
};
