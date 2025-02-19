#include "personalization_model.h"
#include "text_embedder_remote.h"
#include "collection_manager.h"
#include "archive_utils.h"
#include <iostream>
#include <filesystem>
#include <dlfcn.h>

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
    recommendation_model_path_ = get_model_subdir(model_id) + "/recommendation_model.onnx";
    user_model_path_ = get_model_subdir(model_id) + "/user_model.onnx";
    item_model_path_ = get_model_subdir(model_id) + "/item_model.onnx";
    initialize_session();
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

Option<nlohmann::json> PersonalizationModel::create_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data) {
    std::string model_path = get_model_subdir(model_id);
    std::string metadata_path = model_path + "/metadata.json";

    if (!ArchiveUtils::extract_tar_gz_from_memory(model_data, model_path)) {
        return Option<nlohmann::json>(500, "Failed to extract model archive");
    }

    std::string onnx_path = model_path + "/recommendation_model.onnx";
    std::string user_onnx_path = model_path + "/user_model.onnx";
    std::string item_onnx_path = model_path + "/item_model.onnx";   
    if (!std::filesystem::exists(onnx_path) || !std::filesystem::exists(user_onnx_path) || !std::filesystem::exists(item_onnx_path)) {
        return Option<nlohmann::json>(400, "Missing the required model files in archive");
    }

    // Load model temporarily to get dimensions and check if the model is loadable
    PersonalizationModel temp_model(model_id);
    auto validate_op = temp_model.validate_model_io();
    if(!validate_op.ok()) {
        return Option<nlohmann::json>(400, "Model validation failed. There is a problem with ONNX model");
    }
    auto model_json_with_dims = model_json;
    model_json_with_dims["input_dims"] = temp_model.get_input_dims();
    model_json_with_dims["output_dims"] = temp_model.get_output_dims();

    std::ofstream metadata_file(metadata_path);
    if (!metadata_file) {
        return Option<nlohmann::json>(500, "Failed to create metadata file");
    }

    metadata_file << model_json_with_dims.dump(4);
    metadata_file.close();

    if (!metadata_file) {
        return Option<nlohmann::json>(500, "Failed to write metadata file");
    }

    return Option<nlohmann::json>(model_json_with_dims);
}

Option<nlohmann::json> PersonalizationModel::update_model(const std::string& model_id, const nlohmann::json& model_json, const std::string model_data) {
    std::string model_path = get_model_subdir(model_id);

    auto model_json_with_dims = model_json;
    if (!model_data.empty()) {
        if (!ArchiveUtils::verify_tar_gz_archive(model_data)) {
            return Option<nlohmann::json>(400, "Invalid model archive format");
        }

        std::filesystem::path model_dir(model_path);
        for (const auto& entry : std::filesystem::directory_iterator(model_dir)) {
            if (entry.path().filename() != "metadata.json") {
                std::filesystem::remove_all(entry.path());
            }
        }

        if (!ArchiveUtils::extract_tar_gz_from_memory(model_data, model_path)) {
            return Option<nlohmann::json>(500, "Failed to extract model archive");
        }

        // Load model temporarily to get dimensions and check if the model is loadable
        PersonalizationModel temp_model(model_id);
        auto validate_op = temp_model.validate_model_io();
        if(!validate_op.ok()) {
            return Option<nlohmann::json>(400, "Model validation failed. There is a problem with ONNX model");
        }
        model_json_with_dims["input_dims"] = temp_model.get_input_dims();
        model_json_with_dims["output_dims"] = temp_model.get_output_dims();

        std::string metadata_path = model_path + "/metadata.json";
        std::ofstream metadata_file(metadata_path);
        if (!metadata_file) {
            return Option<nlohmann::json>(500, "Failed to create metadata file");
        }

        metadata_file << model_json_with_dims.dump(4);
        metadata_file.close();

        if (!metadata_file) {
            return Option<nlohmann::json>(500, "Failed to write metadata file");
        }
    } else {
        // If no new model data, just update metadata keeping existing dimensions
        std::string metadata_path = model_path + "/metadata.json";
        std::ifstream existing_metadata(metadata_path);
        nlohmann::json existing_json;
        existing_metadata >> existing_json;

        model_json_with_dims["input_dims"] = existing_json["input_dims"];
        model_json_with_dims["output_dims"] = existing_json["output_dims"];

        std::ofstream metadata_file(metadata_path);
        if (!metadata_file) {
            return Option<nlohmann::json>(500, "Failed to create metadata file");
        }

        metadata_file << model_json_with_dims.dump(4);
        metadata_file.close();

        if (!metadata_file) {
            return Option<nlohmann::json>(500, "Failed to write metadata file");
        }
    }

    return Option<nlohmann::json>(model_json_with_dims);
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

void PersonalizationModel::initialize_session() {
    Ort::SessionOptions recommendation_session_options;
    auto providers = Ort::GetAvailableProviders();
    for(auto& provider : providers) {
        if(provider == "CUDAExecutionProvider") {
            void* handle = dlopen("libonnxruntime_providers_shared.so", RTLD_NOW | RTLD_GLOBAL);
            if(!handle) {
                LOG(INFO) << "ONNX shared libs: off";
                continue;
            }
            dlclose(handle);

            OrtCUDAProviderOptions cuda_options;
            recommendation_session_options.AppendExecutionProvider_CUDA(cuda_options);
        }
    }

    recommendation_session_options.EnableOrtCustomOps();
    LOG(INFO) << "Loading personalization model from: " << recommendation_model_path_;

    env_ = std::make_shared<Ort::Env>();
    recommendation_session_ = std::make_shared<Ort::Session>(*env_, recommendation_model_path_.c_str(), recommendation_session_options);
    user_recommendation_session_ = std::make_shared<Ort::Session>(*env_, user_model_path_.c_str(), recommendation_session_options);
    item_recommendation_session_ = std::make_shared<Ort::Session>(*env_, item_model_path_.c_str(), recommendation_session_options);

    // Initialize input and output dimensions
    Ort::AllocatorWithDefaultOptions allocator;
    auto input_shape = recommendation_session_->GetInputTypeInfo(0).GetTensorTypeAndShapeInfo().GetShape();
    auto output_shape = recommendation_session_->GetOutputTypeInfo(0).GetTensorTypeAndShapeInfo().GetShape();

    input_dims_ = input_shape[input_shape.size() - 1];
    output_dims_ = output_shape[output_shape.size() - 1];
}

embedding_res_t PersonalizationModel::embed_recommendation_vectors(const std::vector<std::vector<float>>& input_vector, const std::vector<int64_t>& user_mask) {
    std::unique_lock<std::mutex> lock(recommendation_mutex_);
    lock.unlock();

    try {
        Ort::AllocatorWithDefaultOptions allocator;
        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
        std::vector<Ort::Value> input_tensors;
        std::vector<std::vector<int64_t>> input_shapes;
        std::vector<const char*> input_node_names = {"user_embeddings", "user_mask"};

        input_shapes.push_back({1, static_cast<int64_t>(input_vector.size()), static_cast<int64_t>(input_vector[0].size())});
        input_shapes.push_back({1, static_cast<int64_t>(user_mask.size())});

        std::vector<float> flattened_input;
        flattened_input.reserve(input_vector.size() * input_vector[0].size());
        for (const auto& vec : input_vector) {
            flattened_input.insert(flattened_input.end(), vec.begin(), vec.end());
        }
        std::vector<int64_t> user_mask_vector(user_mask.begin(), user_mask.end());
        input_tensors.push_back(Ort::Value::CreateTensor<float>(memory_info, flattened_input.data(), flattened_input.size(), input_shapes[0].data(), input_shapes[0].size()));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, user_mask_vector.data(), user_mask_vector.size(), input_shapes[1].data(), input_shapes[1].size()));
        auto output_node_name = recommendation_session_->GetOutputNameAllocated(0, allocator);
        std::vector<const char*> output_node_names = {output_node_name.get()};

        lock.lock();
        auto output_tensors = recommendation_session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());
        lock.unlock();

        float* output_data = output_tensors[0].GetTensorMutableData<float>();
        auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
        std::cout << "Output shape: " << shape[0] << " " << shape[1] << std::endl;
        std::vector<float> embedding;
        embedding.assign(output_data, output_data + output_dims_);
        return embedding_res_t(embedding);

    } catch (const Ort::Exception& e) {
        nlohmann::json error = {
            {"message", "ONNX runtime error"},
            {"error", e.what()}
        };
        return embedding_res_t(500, error);
    }
}
std::vector<embedding_res_t> PersonalizationModel::batch_embed_recommendation_vectors(const std::vector<std::vector<std::vector<float>>>& input_vectors, const std::vector<std::vector<uint64_t>>& user_masks) {
    nlohmann::json error = {
        {"message", "not implemented"}
    };
    return std::vector<embedding_res_t>(input_vectors.size(), embedding_res_t(400, error));
}

// std::vector<embedding_res_t> PersonalizationModel::batch_embed_recommendation_vectors(
//     const std::vector<std::vector<std::vector<float>>>& input_vectors,
//     const std::vector<std::vector<uint64_t>>& user_masks) {
//
//     if (input_vectors.empty()) {
//         return {};
//     }
//
//     if (input_vectors.size() != user_masks.size()) {
//         std::vector<embedding_res_t> results;
//         nlohmann::json error = {
//             {"message", "Input vectors and user masks size mismatch"},
//             {"input_vectors_size", input_vectors.size()},
//             {"user_masks_size", user_masks.size()}
//         };
//         results.push_back(embedding_res_t(400, error));
//         return results;
//     }
//
//     std::vector<embedding_res_t> results;
//     results.reserve(input_vectors.size());
//
//     // Validate input dimensions
//     for (size_t i = 0; i < input_vectors.size(); i++) {
//         for (const auto& vec : input_vectors[i]) {
//             if (vec.size() != input_dims_) {
//                 nlohmann::json error = {
//                     {"message", "Input vector dimension mismatch"},
//                     {"expected", input_dims_},
//                     {"got", vec.size()}
//                 };
//                 results.push_back(embedding_res_t(400, error));
//                 return results;
//             }
//         }
//     }
//
//     std::unique_lock<std::mutex> lock(recommendation_mutex_);
//
//     try {
//         Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(
//             OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
//
//         for (size_t i = 0; i < input_vectors.size(); i++) {
//             std::vector<int64_t> input_shape = {1, static_cast<int64_t>(input_dims_)};
//
//             Ort::Value input_tensor = Ort::Value::CreateTensor<float>(
//                 memory_info, const_cast<float*>(input_vectors[i][0].data()), input_vectors[i][0].size(),
//                 input_shape.data(), input_shape.size());
//
//             Ort::Value user_mask_tensor = Ort::Value::CreateTensor<uint64_t>(
//                 memory_info, const_cast<uint64_t*>(user_masks[i].data()), user_masks[i].size(),
//                 input_shape.data(), input_shape.size());
//
//             Ort::AllocatorWithDefaultOptions allocator;
//             auto input_name = recommendation_session_->GetInputNameAllocated(0, allocator);
//             auto mask_name = recommendation_session_->GetInputNameAllocated(1, allocator);
//             auto output_name = recommendation_session_->GetOutputNameAllocated(0, allocator);
//             const char* input_names[] = {input_name.get(), mask_name.get()};
//             const char* output_names[] = {output_name.get()};
//
//             Ort::Value input_tensors[] = {input_tensor, user_mask_tensor};
//             auto output_tensors = recommendation_session_->Run(
//                 Ort::RunOptions{nullptr}, input_names, input_tensors, 2, output_names, 1);
//
//             float* output_data = output_tensors[0].GetTensorMutableData<float>();
//             std::vector<std::vector<float>> embeddings;
//             embeddings.resize(input_vectors[i].size());
//             for (size_t j = 0; j < input_vectors[i].size(); j++) {
//                 embeddings[j].assign(output_data + (j * output_dims_), output_data + ((j + 1) * output_dims_));
//             }
//             results.push_back(embedding_res_t(embeddings));
//         }
//
//         return results;
//
//     } catch (const Ort::Exception& e) {
//         nlohmann::json error = {
//             {"message", "ONNX runtime error"},
//             {"error", e.what()}
//         };
//         results.push_back(embedding_res_t(500, error));
//         return results;
//     }
// }

Option<bool> PersonalizationModel::validate_model_io() {
    try {
        Ort::AllocatorWithDefaultOptions allocator;

        // Validate input tensor
        auto input_type_info = recommendation_session_->GetInputTypeInfo(0);
        auto input_tensor_info = input_type_info.GetTensorTypeAndShapeInfo();
        auto input_shape = input_tensor_info.GetShape();

        if (input_shape.size() != 3) {
            return Option<bool>(400, "Invalid input tensor shape. Expected 3D tensor with batch size, sequence, and embedding dims");
        }

        // Validate output tensor
        auto output_type_info = recommendation_session_->GetOutputTypeInfo(0);
        auto output_tensor_info = output_type_info.GetTensorTypeAndShapeInfo();
        auto output_shape = output_tensor_info.GetShape();

        if (output_shape.size() != 2) {
            return Option<bool>(400, "Invalid output tensor shape. Expected 2D tensor with batch size, and embedding dims");
        }

        // Validate user input tensor
        auto user_input_type_info = user_recommendation_session_->GetInputTypeInfo(0);
        auto user_input_tensor_info = user_input_type_info.GetTensorTypeAndShapeInfo();
        auto user_input_shape = user_input_tensor_info.GetShape();

        if (user_input_shape.size() != 3) {
            return Option<bool>(400, "Invalid user input tensor shape. Expected 3D tensor with batch size, sentence, and embedding dims");
        }

        // Validate user output tensor
        auto user_output_type_info = user_recommendation_session_->GetOutputTypeInfo(0);
        auto user_output_tensor_info = user_output_type_info.GetTensorTypeAndShapeInfo();
        auto user_output_shape = user_output_tensor_info.GetShape();

        if (user_output_shape.size() != 2) {
            return Option<bool>(400, "Invalid user output tensor shape. Expected 2D tensor with batch size, and embedding dims");
        }

        // Validate item input tensor
        auto item_input_type_info = item_recommendation_session_->GetInputTypeInfo(0);
        auto item_input_tensor_info = item_input_type_info.GetTensorTypeAndShapeInfo();
        auto item_input_shape = item_input_tensor_info.GetShape();

        if (item_input_shape.size() != 3) {
            return Option<bool>(400, "Invalid item input tensor shape. Expected 3D tensor with batch size, sequence, and embedding dims");
        }
        
        // Validate item output tensor
        auto item_output_type_info = item_recommendation_session_->GetOutputTypeInfo(0);
        auto item_output_tensor_info = item_output_type_info.GetTensorTypeAndShapeInfo();
        auto item_output_shape = item_output_tensor_info.GetShape();

        if (item_output_shape.size() != 2) {
            return Option<bool>(400, "Invalid item output tensor shape. Expected 2D tensor with batch size, and embedding dims");
        }

        return Option<bool>(true);
    } catch (const Ort::Exception& e) {
        return Option<bool>(500, std::string("ONNX Runtime error: ") + e.what());
    } catch (const std::exception& e) {
        return Option<bool>(500, std::string("Error validating model: ") + e.what());
    }
}