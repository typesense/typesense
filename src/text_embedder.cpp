#include "text_embedder.h"
#include "text_embedder_manager.h"
#include "logger.h"
#include <string>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <dlfcn.h>

TextEmbedder::TextEmbedder(const std::string& model_name) {
    // create environment for local model
    Ort::SessionOptions session_options;
    auto providers = Ort::GetAvailableProviders();
    for(auto& provider : providers) {
        if(provider == "CUDAExecutionProvider") {

            // check existence of shared lib
            void* handle = dlopen("libonnxruntime_providers_shared.so", RTLD_NOW | RTLD_GLOBAL);
            if(!handle) {
                LOG(INFO) << "ONNX shared libs: off";
                continue;
            }

            dlclose(handle);

            LOG(INFO) << "Using CUDAExecutionProvider";
            OrtCUDAProviderOptions cuda_options;
            session_options.AppendExecutionProvider_CUDA(cuda_options);
        }
    }
    std::string abs_path = TextEmbedderManager::get_absolute_model_path(model_name);
    LOG(INFO) << "Loading model from disk: " << abs_path;
    session_ = std::make_unique<Ort::Session>(env_, abs_path.c_str(), session_options);
    std::ifstream config_file(TextEmbedderManager::get_absolute_config_path(model_name));
    nlohmann::json config;
    config_file >> config;
    TokenizerType tokenizer_type = TextEmbedderManager::get_tokenizer_type(config);
    auto vocab_path = TextEmbedderManager::get_absolute_vocab_path(model_name, config["vocab_file_name"].get<std::string>());
    if(tokenizer_type == TokenizerType::bert) {
        tokenizer_ = std::make_unique<BertTokenizerWrapper>(vocab_path);
    } else if(tokenizer_type == TokenizerType::distilbert) {
        tokenizer_ = std::make_unique<DistilbertTokenizer>(vocab_path);
    }
    else if(tokenizer_type == TokenizerType::xlm_roberta) {
        tokenizer_ = std::make_unique<XLMRobertaTokenizer>(vocab_path);
    }
    auto output_tensor_count = session_->GetOutputCount();
    for (size_t i = 0; i < output_tensor_count; i++) {
        auto shape = session_->GetOutputTypeInfo(i).GetTensorTypeAndShapeInfo().GetShape();
        if (shape.size() == 3 && shape[0] == -1 && shape[1] == -1 && shape[2] > 0) {
            Ort::AllocatorWithDefaultOptions allocator;
            output_tensor_name = std::string(session_->GetOutputNameAllocated(i, allocator).get());
            num_dim = shape[2];
            break;
        }
    }
}

TextEmbedder::TextEmbedder(const nlohmann::json& model_config, size_t num_dims) {
    const std::string& model_name = model_config["model_name"].get<std::string>();
    LOG(INFO) << "Initializing remote embedding model: " << model_name;
    auto model_namespace = TextEmbedderManager::get_model_namespace(model_name);

    if(model_namespace == "openai") {
        auto api_key = model_config["api_key"].get<std::string>();

        remote_embedder_ = std::make_unique<OpenAIEmbedder>(model_name, api_key);
    } else if(model_namespace == "google") {
        auto api_key = model_config["api_key"].get<std::string>();

        remote_embedder_ = std::make_unique<GoogleEmbedder>(api_key);
    } else if(model_namespace == "gcp") {
        auto project_id = model_config["project_id"].get<std::string>();
        auto model_name = model_config["model_name"].get<std::string>();
        auto access_token = model_config["access_token"].get<std::string>();
        auto refresh_token = model_config["refresh_token"].get<std::string>();
        auto client_id = model_config["client_id"].get<std::string>();
        auto client_secret = model_config["client_secret"].get<std::string>();

        remote_embedder_ = std::make_unique<GCPEmbedder>(project_id, model_name, access_token, refresh_token, client_id, client_secret);
    }

    num_dim = num_dims;
}


std::vector<float> TextEmbedder::mean_pooling(const std::vector<std::vector<float>>& inputs) {

    std::vector<float> pooled_output;
    for (int i = 0; i < inputs[0].size(); i++) {
        float sum = 0;
        for (int j = 0; j < inputs.size(); j++) {
            sum += inputs[j][i];
        }
        pooled_output.push_back(sum / inputs.size());
    }
    return pooled_output;
}

embedding_res_t TextEmbedder::Embed(const std::string& text, const size_t remote_embedder_timeout_ms, const size_t remote_embedding_num_tries) {
    if(is_remote()) {
        return remote_embedder_->Embed(text, remote_embedder_timeout_ms, remote_embedding_num_tries);
    } else {
        // Cannot run same model in parallel, so lock the mutex
        std::lock_guard<std::mutex> lock(mutex_);
        auto encoded_input = tokenizer_->Encode(text);
        // create input tensor object from data values
        Ort::AllocatorWithDefaultOptions allocator;
        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
        std::vector<Ort::Value> input_tensors;
        std::vector<std::vector<int64_t>> input_shapes;
        std::vector<const char*> input_node_names = {"input_ids", "attention_mask"};
        // If model is DistilBERT or sentencepiece, it has 2 inputs, else it has 3 inputs
        if(session_->GetInputCount() == 3) {
            input_node_names.push_back("token_type_ids");
        }
        input_shapes.push_back({1, static_cast<int64_t>(encoded_input.input_ids.size())});
        input_shapes.push_back({1, static_cast<int64_t>(encoded_input.attention_mask.size())});
        if(session_->GetInputCount() == 3) {
            input_shapes.push_back({1, static_cast<int64_t>(encoded_input.token_type_ids.size())});
        }
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, encoded_input.input_ids.data(), encoded_input.input_ids.size(), input_shapes[0].data(), input_shapes[0].size()));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, encoded_input.attention_mask.data(), encoded_input.attention_mask.size(), input_shapes[1].data(), input_shapes[1].size()));
        if(session_->GetInputCount() == 3) {
            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, encoded_input.token_type_ids.data(), encoded_input.token_type_ids.size(), input_shapes[2].data(), input_shapes[2].size()));
        }

        //LOG(INFO) << "Running model";
        // create output tensor object
        std::vector<const char*> output_node_names = {output_tensor_name.c_str()};
        auto output_tensor = session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());
        std::vector<std::vector<float>> output;
        float* data = output_tensor[0].GetTensorMutableData<float>();
        // print output tensor shape
        auto shape = output_tensor[0].GetTensorTypeAndShapeInfo().GetShape();

        for (int i = 0; i < shape[1]; i++) {
            std::vector<float> temp;
            for (int j = 0; j < shape[2]; j++) {
                temp.push_back(data[i * shape[2] + j]);
            }
            output.push_back(temp);
        }
        auto pooled_output = mean_pooling(output);  

        return embedding_res_t(pooled_output);
    }
}

std::vector<embedding_res_t> TextEmbedder::batch_embed(const std::vector<std::string>& inputs, const size_t remote_embedding_batch_size) {
    std::vector<embedding_res_t> outputs;
    if(!is_remote()) {
        std::lock_guard<std::mutex> lock(mutex_);
        for(int i = 0; i < inputs.size(); i += 8) {
            auto input_batch = std::vector<std::string>(inputs.begin() + i, inputs.begin() + std::min(i + 8, static_cast<int>(inputs.size())));
            auto encoded_inputs = batch_encode(input_batch);
            
            // create input tensor object from data values
            Ort::AllocatorWithDefaultOptions allocator;
            Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
            std::vector<Ort::Value> input_tensors;
            std::vector<std::vector<int64_t>> input_shapes;
            std::vector<const char*> input_node_names = {"input_ids", "attention_mask"};
            // If model is DistilBERT or sentencepiece, it has 2 inputs, else it has 3 inputs
            if(session_->GetInputCount() == 3) {
                input_node_names.push_back("token_type_ids");
            }
            input_shapes.push_back({static_cast<int64_t>(encoded_inputs.input_ids.size()), static_cast<int64_t>(encoded_inputs.input_ids[0].size())});
            input_shapes.push_back({static_cast<int64_t>(encoded_inputs.attention_mask.size()), static_cast<int64_t>(encoded_inputs.attention_mask[0].size())});
            if(session_->GetInputCount() == 3) {
                input_shapes.push_back({static_cast<int64_t>(encoded_inputs.token_type_ids.size()), static_cast<int64_t>(encoded_inputs.token_type_ids[0].size())});
            }

            std::vector<int64_t> input_ids_flatten;
            std::vector<int64_t> attention_mask_flatten;
            std::vector<int64_t> token_type_ids_flatten;

            for (int i = 0; i < encoded_inputs.input_ids.size(); i++) {
                for (int j = 0; j < encoded_inputs.input_ids[i].size(); j++) {
                    input_ids_flatten.push_back(encoded_inputs.input_ids[i][j]);
                }
            }

            for (int i = 0; i < encoded_inputs.attention_mask.size(); i++) {
                for (int j = 0; j < encoded_inputs.attention_mask[i].size(); j++) {
                    attention_mask_flatten.push_back(encoded_inputs.attention_mask[i][j]);
                }
            }

            if(session_->GetInputCount() == 3) {
                for (int i = 0; i < encoded_inputs.token_type_ids.size(); i++) {
                    for (int j = 0; j < encoded_inputs.token_type_ids[i].size(); j++) {
                        token_type_ids_flatten.push_back(encoded_inputs.token_type_ids[i][j]);
                    }
                }
            }

            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, input_ids_flatten.data(), input_ids_flatten.size(), input_shapes[0].data(), input_shapes[0].size()));
            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, attention_mask_flatten.data(), attention_mask_flatten.size(), input_shapes[1].data(), input_shapes[1].size()));
            if(session_->GetInputCount() == 3) {
                input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, token_type_ids_flatten.data(), token_type_ids_flatten.size(), input_shapes[2].data(), input_shapes[2].size()));
            }

            //LOG(INFO) << "Running model";
            // create output tensor object
            std::vector<const char*> output_node_names = {output_tensor_name.c_str()};

            // if seq length is 0, return empty vector
            if(input_shapes[0][1] == 0) {
                for(int i = 0; i < input_batch.size(); i++) {
                    outputs.push_back(embedding_res_t(400, nlohmann::json({{"error", "Invalid input: empty sequence"}})));
                }
                continue;
            }

            auto output_tensor = session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), output_node_names.size());
            float* data = output_tensor[0].GetTensorMutableData<float>();
            // print output tensor shape
            auto shape = output_tensor[0].GetTensorTypeAndShapeInfo().GetShape();
            for (int i = 0; i < shape[0]; i++) {
                std::vector<std::vector<float>> output;
                for (int j = 0; j < shape[1]; j++) {
                    std::vector<float> output_row;
                    for (int k = 0; k < shape[2]; k++) {
                        output_row.push_back(data[i * shape[1] * shape[2] + j * shape[2] + k]);
                    }
                    output.push_back(output_row);
                }
                outputs.push_back(embedding_res_t(mean_pooling(output)));
            }
        }
    } else {
        outputs = std::move(remote_embedder_->batch_embed(inputs, remote_embedding_batch_size));
    }
    
    return outputs;
}

TextEmbedder::~TextEmbedder() { }

batch_encoded_input_t TextEmbedder::batch_encode(const std::vector<std::string>& inputs) {
    batch_encoded_input_t encoded_inputs;
    for(auto& input : inputs) {
        auto encoded_input = tokenizer_->Encode(input);
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

Option<bool> TextEmbedder::validate() {
    if(session_->GetInputCount() != 3 && session_->GetInputCount() != 2) {
        LOG(ERROR) << "Invalid model: input count is not 3 or 2";
        return Option<bool>(400, "Invalid model: input count is not 3 or 2");
    }

    Ort::AllocatorWithDefaultOptions allocator;
    auto input_ids_name = session_->GetInputNameAllocated(0, allocator);
    if (std::strcmp(input_ids_name.get(), "input_ids") != 0) {
        LOG(ERROR) << "Invalid model: input_ids tensor not found";
        return Option<bool>(400, "Invalid model: input_ids tensor not found");
    }

    auto attention_mask_name = session_->GetInputNameAllocated(1, allocator);
    if (std::strcmp(attention_mask_name.get(), "attention_mask") != 0) {
        LOG(ERROR) << "Invalid model: attention_mask tensor not found";
        return Option<bool>(400, "Invalid model: attention_mask tensor not found");
    }

    if(session_->GetInputCount() == 3) {
        auto token_type_ids_name = session_->GetInputNameAllocated(2, allocator);
        if (std::strcmp(token_type_ids_name.get(), "token_type_ids") != 0) {
            LOG(ERROR) << "Invalid model: token_type_ids tensor not found";
            return Option<bool>(400, "Invalid model: token_type_ids tensor not found");
        }
    }

    auto output_tensor_count = session_->GetOutputCount();
    bool found_output_tensor = false;

    for (size_t i = 0; i < output_tensor_count; i++) {
        auto shape = session_->GetOutputTypeInfo(i).GetTensorTypeAndShapeInfo().GetShape();
        if (shape.size() == 3 && shape[0] == -1 && shape[1] == -1 && shape[2] > 0) {
            found_output_tensor = true;
            break;
        }
    }

    if (!found_output_tensor) {
        LOG(ERROR) << "Invalid model: Output tensor not found";
        return Option<bool>(400, "Invalid model: Output tensor not found");
    }

    return Option<bool>(true);
}

const size_t TextEmbedder::get_num_dim() const {
    return num_dim;
}
