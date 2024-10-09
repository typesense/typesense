#include "recommendations_model.h"
#include "collection_manager.h"
#include <iostream>
#include <filesystem>

std::string RecommendationsModel::get_model_subdir(const std::string& model_id) {
    std::string model_dir = EmbedderManager::get_model_dir();
    
    if (model_dir.back() != '/') {
        model_dir += '/';
    }
    
    std::string full_path = model_dir + "rem_" + model_id;
    
    if (!std::filesystem::exists(full_path)) {
        std::filesystem::create_directories(full_path);
    }
    
    return full_path;
}

RecommendationsModel::RecommendationsModel(const std::string& model_id)
    : model_id_(model_id) {
    model_path_ = get_model_subdir(model_id_);
}

RecommendationsModel::~RecommendationsModel() {
}

Option<bool> RecommendationsModel::validate_model(const nlohmann::json& model_json) {
    if (!model_json.contains("id") || !model_json["id"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'id' field.");
    }
    if (!model_json.contains("name") || !model_json["name"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'name' field.");
    }
    if (!model_json.contains("collection") || !model_json["collection"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'collection' field.");
    }

    auto& collection_manager = CollectionManager::get_instance();
    const std::string& collection_name = model_json["collection"].get<std::string>();
    
    if (collection_manager.get_collection(collection_name) == nullptr) {
        return Option<bool>(404, "Collection '" + collection_name + "' not found.");
    }
    return Option<bool>(true);
}