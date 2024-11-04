#include "personalization_model.h"
#include "collection_manager.h"
#include "archive_utils.h"
#include <iostream>
#include <filesystem>

std::string PersonalizationModel::get_model_subdir(const std::string& model_id) {
    std::string model_dir = EmbedderManager::get_model_dir();
    
    if (model_dir.back() != '/') {
        model_dir += '/';
    }
    
    std::string full_path = model_dir + "per_" + model_id;
    
    if (!std::filesystem::exists(full_path)) {
        std::filesystem::create_directories(full_path);
    }
    
    return full_path;
}

PersonalizationModel::PersonalizationModel(const std::string& model_id)
    : model_id_(model_id) {
    model_path_ = get_model_subdir(model_id_);
}

PersonalizationModel::~PersonalizationModel() {
}

Option<bool> PersonalizationModel::validate_model(const nlohmann::json& model_json) {
    if (!model_json.contains("id") || !model_json["id"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'id' field.");
    }
    if (!model_json.contains("name") || !model_json["name"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'name' field.");
    }
    if (!model_json.contains("collection") || !model_json["collection"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'collection' field.");
    }
    const std::string& name = model_json["name"].get<std::string>();
    size_t slash_pos = name.find('/');
    
    if (slash_pos == std::string::npos || name.find('/', slash_pos + 1) != std::string::npos) {
        return Option<bool>(400, "Model name must contain exactly one '/' character.");
    }

    std::string namespace_part = name.substr(0, slash_pos);
    if (namespace_part != "ts") {
        return Option<bool>(400, "Model namespace must be 'ts'.");
    }

    std::string model_name = name.substr(slash_pos + 1);
    if (model_name.empty()) {
        return Option<bool>(400, "Model name part cannot be empty.");
    }

    if (!model_json.contains("type") || !model_json["type"].is_string()) {
        return Option<bool>(400, "Missing or invalid 'type' field. Must be either 'recommendation' or 'search'.");
    }

    const std::string& type = model_json["type"].get<std::string>();
    if (type != "recommendation" && type != "search") {
        return Option<bool>(400, "Invalid type. Must be either 'recommendation' or 'search'.");
    }

    auto type_names = valid_model_names.find(type);
    if (type_names == valid_model_names.end() || 
        std::find(type_names->second.begin(), type_names->second.end(), model_name) == type_names->second.end()) {
        return Option<bool>(400, "Invalid model name for type. Use 'tyrec-1' for recommendation and 'tyrec-2' for search.");
    }

    auto& collection_manager = CollectionManager::get_instance();
    const std::string& collection_name = model_json["collection"].get<std::string>();
    
    if (collection_manager.get_collection(collection_name) == nullptr) {
        return Option<bool>(404, "Collection '" + collection_name + "' not found.");
    }
    return Option<bool>(true);
}

Option<bool> PersonalizationModel::create_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data) {
    std::string model_path = get_model_subdir(model_id);
    std::string metadata_path = model_path + "/metadata.json";
    std::ofstream metadata_file(metadata_path);
    if (!metadata_file) {
        return Option<bool>(500, "Failed to create metadata file");
    }

    metadata_file << model_json.dump(4);
    metadata_file.close();

    if (!metadata_file) {
        return Option<bool>(500, "Failed to write metadata file");
    }

    if (!ArchiveUtils::extract_tar_gz_from_memory(model_data, model_path)) {
        return Option<bool>(500, "Failed to extract model archive");
    }

    std::string onnx_path = model_path + "/model.onnx";
    if (!std::filesystem::exists(onnx_path)) {
        return Option<bool>(400, "Missing required model.onnx file in archive");
    }

    return Option<bool>(true);
}

Option<bool> PersonalizationModel::update_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data) {
    std::string model_path = get_model_subdir(model_id);

    std::string metadata_path = model_path + "/metadata.json";
    std::ofstream metadata_file(metadata_path);
    if (!metadata_file) {
        return Option<bool>(500, "Failed to create metadata file");
    }

    metadata_file << model_json.dump(4);
    metadata_file.close();

    if (!metadata_file) {
        return Option<bool>(500, "Failed to write metadata file");
    }

    if (!model_data.empty()) {
        if (!ArchiveUtils::verify_tar_gz_archive(model_data)) {
            return Option<bool>(400, "Invalid model archive format");
        }

        std::filesystem::path model_dir(model_path);
        for (const auto& entry : std::filesystem::directory_iterator(model_dir)) {
            if (entry.path().filename() != "metadata.json") {
                std::filesystem::remove_all(entry.path());
            }
        }

        if (!ArchiveUtils::extract_tar_gz_from_memory(model_data, model_path)) {
            return Option<bool>(500, "Failed to extract model archive");
        }
    }

    return Option<bool>(true);
}

Option<bool> PersonalizationModel::delete_model(const std::string& model_id) {
    std::string model_path = get_model_subdir(model_id);
    
    if (!std::filesystem::exists(model_path)) {
        return Option<bool>(404, "Model directory not found");
    }

    try {
        std::filesystem::remove_all(model_path);
        return Option<bool>(true);
    } catch (const std::filesystem::filesystem_error& e) {
        return Option<bool>(500, "Failed to delete model directory: " + std::string(e.what()));
    }
}