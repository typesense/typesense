#include "personalization_model.h"
#include "text_embedder_remote.h"
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
    prompt_path_ = get_model_subdir(model_id) + "/prompts.json";
    vocab_path_ = get_model_subdir(model_id) + "/vocab.txt";
    nlohmann::json prompt_json;
    try {
        std::ifstream prompt_file(prompt_path_);
        if (!prompt_file.is_open()) {
            LOG(ERROR) << "Could not open prompt file: " + prompt_path_;
            return;
        }
        
        std::stringstream buffer;
        buffer << prompt_file.rdbuf();
        std::string json_str = buffer.str();
        
        if (json_str.empty()) {
            LOG(ERROR) << "Prompt file is empty";
            return;
        }
        
        prompt_json = nlohmann::json::parse(json_str);
        
        if (!prompt_json.contains("q") || !prompt_json["q"].is_string() ||
            !prompt_json.contains("d") || !prompt_json["d"].is_string()) {
            LOG(ERROR) << "Prompt file missing required string fields 'q' and 'd'";
            return;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse prompt file: " << e.what();
    }
    query_prompt_ = prompt_json["q"].get<std::string>();
    item_prompt_ = prompt_json["d"].get<std::string>();
    tokenizer_ = std::make_unique<BertTokenizerWrapper>(vocab_path_);
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

    std::string tokenzier_path = model_path + "/vocab.txt";
    if (!std::filesystem::exists(tokenzier_path)) {
        return Option<nlohmann::json>(400, "Missing the required vocab.txt file in archive");
    }

    std::string prompt_path = model_path + "/prompts.json";
    if (!std::filesystem::exists(prompt_path)) {
        return Option<nlohmann::json>(400, "Missing the required prompts.json file in archive");
    }

    // Load model temporarily to get dimensions and check if the model is loadable
    PersonalizationModel temp_model(model_id);
    auto validate_op = temp_model.validate_model_io();
    if(!validate_op.ok()) {
        return Option<nlohmann::json>(400, "Model validation failed. There is a problem with ONNX model");
    }
    auto model_json_with_dims = model_json;
    model_json_with_dims["num_dims"] = temp_model.get_num_dims();

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
        model_json_with_dims["num_dims"] = temp_model.get_num_dims();

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

        model_json_with_dims["num_dims"] = existing_json["num_dims"];

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

    env_ = std::make_shared<Ort::Env>();
    recommendation_session_ = std::make_shared<Ort::Session>(*env_, recommendation_model_path_.c_str(), recommendation_session_options);
    user_session_ = std::make_shared<Ort::Session>(*env_, user_model_path_.c_str(), recommendation_session_options);
    item_session_ = std::make_shared<Ort::Session>(*env_, item_model_path_.c_str(), recommendation_session_options);

    // Initialize input and output dimensions
    Ort::AllocatorWithDefaultOptions allocator;
    auto output_shape = recommendation_session_->GetOutputTypeInfo(0).GetTensorTypeAndShapeInfo().GetShape();
    num_dims_ = output_shape[output_shape.size() - 1];
}

embedding_res_t PersonalizationModel::embed_recommendations(const std::vector<std::vector<float>>& input_vector, const std::vector<int64_t>& user_mask) {
    std::unique_lock<std::mutex> lock(recommendation_mutex_);

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

        auto output_tensors = recommendation_session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());

        float* output_data = output_tensors[0].GetTensorMutableData<float>();
        auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
        std::vector<float> embedding;
        embedding.assign(output_data, output_data + num_dims_);
        return embedding_res_t(embedding);

    } catch (const Ort::Exception& e) {
        nlohmann::json error = {
            {"message", "ONNX runtime error"},
            {"error", e.what()}
        };
        return embedding_res_t(500, error);
    }
}

std::vector<embedding_res_t> PersonalizationModel::batch_embed_recommendations(const std::vector<std::vector<std::vector<float>>>& input_vectors, const std::vector<std::vector<int64_t>>& user_masks) {
    std::unique_lock<std::mutex> lock(recommendation_mutex_);

    try {
        Ort::AllocatorWithDefaultOptions allocator;
        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
        std::vector<Ort::Value> input_tensors;
        std::vector<std::vector<int64_t>> input_shapes;
        std::vector<const char*> input_node_names = {"user_embeddings", "user_mask"};

        input_shapes.push_back({static_cast<int64_t>(input_vectors.size()), static_cast<int64_t>(input_vectors[0].size()), static_cast<int64_t>(input_vectors[0][0].size())});
        input_shapes.push_back({static_cast<int64_t>(user_masks.size()), static_cast<int64_t>(user_masks[0].size())});

        std::vector<float> flattened_input;
        flattened_input.reserve(input_vectors.size() * input_vectors[0].size() * input_vectors[0][0].size());
        for (const auto& vec : input_vectors) {
            for (const auto& vec2 : vec) {
                flattened_input.insert(flattened_input.end(), vec2.begin(), vec2.end());
            }
        }
        std::vector<int64_t> flattened_user_mask;
        flattened_user_mask.reserve(user_masks.size() * user_masks[0].size());
        for (const auto& mask : user_masks) {
            flattened_user_mask.insert(flattened_user_mask.end(), mask.begin(), mask.end());
        }
        input_tensors.push_back(Ort::Value::CreateTensor<float>(memory_info, flattened_input.data(), flattened_input.size(), input_shapes[0].data(), input_shapes[0].size()));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, flattened_user_mask.data(), flattened_user_mask.size(), input_shapes[1].data(), input_shapes[1].size()));
        auto output_node_name = recommendation_session_->GetOutputNameAllocated(0, allocator);
        std::vector<const char*> output_node_names = {output_node_name.get()};

        auto output_tensors = recommendation_session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());

        float* output_data = output_tensors[0].GetTensorMutableData<float>();
        auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
        std::vector<embedding_res_t> embeddings;
        for (size_t i = 0; i < shape[0]; i++) {
            std::vector<float> embedding;
            embedding.assign(output_data + (i * num_dims_), output_data + ((i + 1) * num_dims_));
            embeddings.push_back(embedding_res_t(embedding));
        }
        return embeddings;
    } catch (const Ort::Exception& e) {
        nlohmann::json error = {
            {"message", "ONNX runtime error"},
            {"error", e.what()}
        };
        return std::vector<embedding_res_t>(input_vectors.size(), embedding_res_t(500, error));
    }
}

embedding_res_t PersonalizationModel::embed_user(const std::vector<std::string>& features) {
    std::unique_lock<std::mutex> lock(user_mutex_);

    batch_encoded_input_t encoded_inputs = encode_features(features);

    try {
        Ort::AllocatorWithDefaultOptions allocator;
        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
        std::vector<Ort::Value> input_tensors;
        std::vector<std::vector<int64_t>> input_shapes;
        std::vector<const char*> input_node_names = {"input_ids", "attention_mask"};

        input_shapes.push_back({1, static_cast<int64_t>(encoded_inputs.input_ids.size()), static_cast<int64_t>(encoded_inputs.input_ids[0].size())});
        input_shapes.push_back({1, static_cast<int64_t>(encoded_inputs.attention_mask.size()), static_cast<int64_t>(encoded_inputs.attention_mask[0].size())});

        std::vector<int64_t> input_ids_flatten;
        std::vector<int64_t> attention_mask_flatten;

        for(auto& input_ids : encoded_inputs.input_ids) {
            for(auto& id : input_ids) {
                input_ids_flatten.push_back(id);
            }
        }

        for(auto& attention_mask : encoded_inputs.attention_mask) {
            for(auto& mask : attention_mask) {
                attention_mask_flatten.push_back(mask);
            }
        }
        
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, input_ids_flatten.data(), input_ids_flatten.size(), input_shapes[0].data(), input_shapes[0].size()));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, attention_mask_flatten.data(), attention_mask_flatten.size(), input_shapes[1].data(), input_shapes[1].size()));

        auto output_node_name = user_session_->GetOutputNameAllocated(0, allocator);
        std::vector<const char*> output_node_names = {output_node_name.get()};
        
        auto output_tensors = user_session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());

        float* output_data = output_tensors[0].GetTensorMutableData<float>();
        auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
        std::vector<float> embedding;
        embedding.assign(output_data, output_data + num_dims_);
        return embedding_res_t(embedding);
        
    } catch (const Ort::Exception& e) {
        nlohmann::json error = {
            {"message", "ONNX runtime error"},
            {"error", e.what()}
        };
        return embedding_res_t(500, error);
    }
}

std::vector<embedding_res_t> PersonalizationModel::batch_embed_users(const std::vector<std::vector<std::string>>& features) {
    std::unique_lock<std::mutex> lock(user_mutex_);

    std::vector<embedding_res_t> embeddings;
    size_t batch_size = 8;
    for(size_t i = 0; i < features.size(); i+= batch_size) {
        auto input_batch = std::vector<std::vector<std::string>>(features.begin() + i, features.begin() + std::min(i + batch_size, static_cast<size_t>(features.size())));
        auto encoded_inputs = encode_batch(input_batch);

        try {
            Ort::AllocatorWithDefaultOptions allocator;
            Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
            std::vector<Ort::Value> input_tensors;
            std::vector<std::vector<int64_t>> input_shapes;
            std::vector<const char*> input_node_names = {"input_ids", "attention_mask"};

            input_shapes.push_back({static_cast<int64_t>(encoded_inputs.size()), static_cast<int64_t>(encoded_inputs[0].input_ids.size()), static_cast<int64_t>(encoded_inputs[0].input_ids[0].size())});
            input_shapes.push_back({static_cast<int64_t>(encoded_inputs.size()), static_cast<int64_t>(encoded_inputs[0].attention_mask.size()), static_cast<int64_t>(encoded_inputs[0].attention_mask[0].size())});

            std::vector<int64_t> input_ids_flatten;
            std::vector<int64_t> attention_mask_flatten;

            std::cout << "User shapes: " << std::endl;
            for (auto& i: input_shapes) {
                std::cout << "Shape: ";
                int64_t total_size = 1;
                for (auto& j: i) {
                    std::cout << j << " ";
                    total_size *= j;
                }
                std::cout << ", Total size: " << total_size << std::endl;
            }
            std::cout << "Encoded inputs Size: " << std::endl;
            for(auto& i: encoded_inputs) {
                std::cout << "Input IDs: ";
                for(auto& ids : i.input_ids) {
                    std::cout << ids.size() << " ";
                }
                std::cout << std::endl;
            }
            for(auto& batch : encoded_inputs) {
                for(auto& input_ids : batch.input_ids) {
                    for(auto& id : input_ids) {
                        input_ids_flatten.push_back(id);
                    }
                }
                for(auto& attention_mask : batch.attention_mask) {
                    for(auto& mask : attention_mask) {
                        attention_mask_flatten.push_back(mask);
                    }
                }
            }

            std::cout << "Input IDs flatten size: " << input_ids_flatten.size() << std::endl;
            std::cout << "Attention mask flatten size: " << attention_mask_flatten.size() << std::endl;

            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, input_ids_flatten.data(), input_ids_flatten.size(), input_shapes[0].data(), input_shapes[0].size()));
            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, attention_mask_flatten.data(), attention_mask_flatten.size(), input_shapes[1].data(), input_shapes[1].size()));

            auto output_node_name = user_session_->GetOutputNameAllocated(0, allocator);
            std::vector<const char*> output_node_names = {output_node_name.get()};
            
            auto output_tensors = user_session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());

            float* output_data = output_tensors[0].GetTensorMutableData<float>();
            auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
            for (size_t i = 0; i < shape[0]; i++) {
                std::vector<float> embedding;
                embedding.assign(output_data + (i * num_dims_), output_data + ((i + 1) * num_dims_));
                embeddings.push_back(embedding_res_t(embedding));
            }
        } catch (const Ort::Exception& e) {
            nlohmann::json error = {
                {"message", "ONNX runtime error"},
                {"error", e.what()}
            };
            return std::vector<embedding_res_t>(features.size(), embedding_res_t(500, error));
        }
    }
    return embeddings;
}

embedding_res_t PersonalizationModel::embed_item(const std::vector<std::string>& features) {
    std::unique_lock<std::mutex> lock(item_mutex_);

    batch_encoded_input_t encoded_inputs = encode_features(features);

    try {
        Ort::AllocatorWithDefaultOptions allocator;
        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
        std::vector<Ort::Value> input_tensors;
        std::vector<std::vector<int64_t>> input_shapes;
        std::vector<const char*> input_node_names = {"input_ids", "attention_mask"};

        input_shapes.push_back({1, static_cast<int64_t>(encoded_inputs.input_ids.size()), static_cast<int64_t>(encoded_inputs.input_ids[0].size())});
        input_shapes.push_back({1, static_cast<int64_t>(encoded_inputs.attention_mask.size()), static_cast<int64_t>(encoded_inputs.attention_mask[0].size())});

        std::vector<int64_t> input_ids_flatten;
        std::vector<int64_t> attention_mask_flatten;

        for(auto& input_ids : encoded_inputs.input_ids) {
            for(auto& id : input_ids) {
                input_ids_flatten.push_back(id);
            }
        }

        for(auto& attention_mask : encoded_inputs.attention_mask) {
            for(auto& mask : attention_mask) {
                attention_mask_flatten.push_back(mask);
            }
        }

        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, input_ids_flatten.data(), input_ids_flatten.size(), input_shapes[0].data(), input_shapes[0].size()));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, attention_mask_flatten.data(), attention_mask_flatten.size(), input_shapes[1].data(), input_shapes[1].size()));

        auto output_node_name = item_session_->GetOutputNameAllocated(0, allocator);
        std::vector<const char*> output_node_names = {output_node_name.get()};

        auto output_tensors = item_session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());

        float* output_data = output_tensors[0].GetTensorMutableData<float>();
        auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
        std::vector<float> embedding;
        embedding.assign(output_data, output_data + num_dims_);
        return embedding_res_t(embedding);

    } catch (const Ort::Exception& e) {
        nlohmann::json error = {
            {"message", "ONNX runtime error"},
            {"error", e.what()}
        };
        return embedding_res_t(500, error);
    }
}

std::vector<embedding_res_t> PersonalizationModel::batch_embed_items(const std::vector<std::vector<std::string>>& features) {
    std::unique_lock<std::mutex> lock(item_mutex_);

    std::vector<embedding_res_t> embeddings;
    size_t batch_size = 8;
    for(int i = 0; i < features.size(); i+= batch_size) {
        auto input_batch = std::vector<std::vector<std::string>>(features.begin() + i, features.begin() + std::min(i + batch_size, static_cast<size_t>(features.size())));
        auto encoded_inputs = encode_batch(input_batch);

        try {
            Ort::AllocatorWithDefaultOptions allocator;
            Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
            std::vector<Ort::Value> input_tensors;
            std::vector<std::vector<int64_t>> input_shapes;
            std::vector<const char*> input_node_names = {"input_ids", "attention_mask"};

            input_shapes.push_back({static_cast<int64_t>(encoded_inputs.size()), static_cast<int64_t>(encoded_inputs[0].input_ids.size()), static_cast<int64_t>(encoded_inputs[0].input_ids[0].size())});
            input_shapes.push_back({static_cast<int64_t>(encoded_inputs.size()), static_cast<int64_t>(encoded_inputs[0].attention_mask.size()), static_cast<int64_t>(encoded_inputs[0].attention_mask[0].size())});
            
            std::cout << "Item shapes: " << std::endl;
            for (auto& i: input_shapes) {
                std::cout << "Shape: ";
                int64_t total_size = 1;
                for (auto& j: i) {
                    std::cout << j << " ";
                    total_size *= j;
                }
                std::cout << ", Total size: " << total_size << std::endl;
            }
            std::cout << "Encoded inputs Size: " << std::endl;
            for(auto& i: encoded_inputs) {
                std::cout << "Input IDs: ";
                for(auto& ids : i.input_ids) {
                    std::cout << ids.size() << " ";
                }
                std::cout << std::endl;
            }
            std::vector<int64_t> input_ids_flatten;
            std::vector<int64_t> attention_mask_flatten;

            
            for(auto& batch : encoded_inputs) {
                for(auto& input_ids : batch.input_ids) {
                    for(auto& id : input_ids) {
                        input_ids_flatten.push_back(id);
                    }
                }
                for(auto& attention_mask : batch.attention_mask) {
                    for(auto& mask : attention_mask) {
                        attention_mask_flatten.push_back(mask);
                    }
                }
            }
            
            std::cout << "Input IDs flatten size: " << input_ids_flatten.size() << std::endl;
            std::cout << "Attention mask flatten size: " << attention_mask_flatten.size() << std::endl;
            
            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, input_ids_flatten.data(), input_ids_flatten.size(), input_shapes[0].data(), input_shapes[0].size()));
            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, attention_mask_flatten.data(), attention_mask_flatten.size(), input_shapes[1].data(), input_shapes[1].size()));

            auto output_node_name = item_session_->GetOutputNameAllocated(0, allocator);
            std::vector<const char*> output_node_names = {output_node_name.get()};

            auto output_tensors = item_session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());

            float* output_data = output_tensors[0].GetTensorMutableData<float>();
            auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
            for (size_t i = 0; i < shape[0]; i++) {
                std::vector<float> embedding;
                embedding.assign(output_data + (i * num_dims_), output_data + ((i + 1) * num_dims_));
                embeddings.push_back(embedding_res_t(embedding));
            }
        } catch (const Ort::Exception& e) {
            nlohmann::json error = {
                {"message", "ONNX runtime error"},
                {"error", e.what()}
            };
            return std::vector<embedding_res_t>(features.size(), embedding_res_t(500, error));
        }
    }
    return embeddings;
}
  
batch_encoded_input_t PersonalizationModel::encode_features(const std::vector<std::string>& features) {
    batch_encoded_input_t encoded_inputs;
    for(auto& feature : features) {
        auto encoded_input = tokenizer_->Encode(feature);
        encoded_inputs.input_ids.push_back(encoded_input.input_ids);
        encoded_inputs.attention_mask.push_back(encoded_input.attention_mask);
        encoded_inputs.token_type_ids.push_back(encoded_input.token_type_ids);
    }
    // Pad inputs
    size_t max_input_len = 0;
    for(auto& input_ids : encoded_inputs.input_ids) {
        if(input_ids.size() > max_input_len) {
            max_input_len = input_ids.size();
        }
    }

    for(auto& input_ids : encoded_inputs.input_ids) {
        input_ids.resize(max_input_len, 0);
    }

    for(auto& attention_mask : encoded_inputs.attention_mask) {
        attention_mask.resize(max_input_len, 0);
    }

    for(auto& token_type_ids : encoded_inputs.token_type_ids) {
        token_type_ids.resize(max_input_len, 0);
    }
    return encoded_inputs;
}

std::vector<batch_encoded_input_t> PersonalizationModel::encode_batch(const std::vector<std::vector<std::string>>& batch_features) {
    std::vector<batch_encoded_input_t> encoded_inputs;
    for (auto& features : batch_features) {
        encoded_inputs.push_back(encode_features(features));
    }

    size_t max_input_len = 0;
    for(auto& batch_input_ids : encoded_inputs) {
        for(auto& input_ids : batch_input_ids.input_ids) {
            if(input_ids.size() > max_input_len) {
                max_input_len = input_ids.size();
            }
        }
    }

    for(auto& batch_input_ids : encoded_inputs) {
        for(auto& input_ids : batch_input_ids.input_ids) {
            input_ids.resize(max_input_len, 0);
        }
    }

    for(auto& batch_attention_mask : encoded_inputs) {
        for(auto& attention_mask : batch_attention_mask.attention_mask) {
            attention_mask.resize(max_input_len, 0);
        }
    }
    
    for(auto& batch_token_type_ids : encoded_inputs) {
        for(auto& token_type_ids : batch_token_type_ids.token_type_ids) {
            token_type_ids.resize(max_input_len, 0);
        }
    }
    return encoded_inputs;
}

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
        auto user_input_type_info = user_session_->GetInputTypeInfo(0);
        auto user_input_tensor_info = user_input_type_info.GetTensorTypeAndShapeInfo();
        auto user_input_shape = user_input_tensor_info.GetShape();

        if (user_input_shape.size() != 3) {
            return Option<bool>(400, "Invalid user input tensor shape. Expected 3D tensor with batch size, sentence, and embedding dims");
        }

        // Validate user output tensor
        auto user_output_type_info = user_session_->GetOutputTypeInfo(0);
        auto user_output_tensor_info = user_output_type_info.GetTensorTypeAndShapeInfo();
        auto user_output_shape = user_output_tensor_info.GetShape();

        if (user_output_shape.size() != 2) {
            return Option<bool>(400, "Invalid user output tensor shape. Expected 2D tensor with batch size, and embedding dims");
        }

        // Validate item input tensor
        auto item_input_type_info = user_session_->GetInputTypeInfo(0);
        auto item_input_tensor_info = item_input_type_info.GetTensorTypeAndShapeInfo();
        auto item_input_shape = item_input_tensor_info.GetShape();

        if (item_input_shape.size() != 3) {
            return Option<bool>(400, "Invalid item input tensor shape. Expected 3D tensor with batch size, sequence, and embedding dims");
        }
        
        // Validate item output tensor
        auto item_output_type_info = item_session_->GetOutputTypeInfo(0);
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