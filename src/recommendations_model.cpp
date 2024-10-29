#include "recommendations_model.h"
#include "collection_manager.h"
#include "archive_utils.h"
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

Option<bool> RecommendationsModel::create_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data) {
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

    return Option<bool>(true);
}

Option<bool> RecommendationsModel::update_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data) {
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

Option<bool> RecommendationsModel::delete_model(const std::string& model_id) {
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