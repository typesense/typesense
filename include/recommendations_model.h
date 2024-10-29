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
    static Option<bool> create_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data);
    static Option<bool> update_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data);
    static Option<bool> delete_model(const std::string& model_id);

private:
    std::string model_path_;
    std::string model_id_;
};