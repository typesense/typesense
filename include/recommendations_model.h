#pragma once
#include <string>
#include <vector>
#include "embedder_manager.h"
#include <json.hpp>

class RecommendationsModel {
public:
    RecommendationsModel(const std::string& model_path);
    ~RecommendationsModel();

    static std::string get_model_subdir(const std::string& model_id);
    static Option<bool> validate_model(const nlohmann::json& model_json);

private:
    std::string model_path_;
    std::string model_id_;
};