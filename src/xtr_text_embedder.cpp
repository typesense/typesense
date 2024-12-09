#include "xtr_text_embedder.h"

XTRTextEmbedder::XTRTextEmbedder(std::shared_ptr<Ort::Session> session, std::shared_ptr<Ort::Env> env, const std::string& vocab_path) : session_(session), env_(env) {
    tokenizer_ = std::make_unique<BertTokenizerWrapper>(vocab_path);
    auto shape = session_->GetOutputTypeInfo(0).GetTensorTypeAndShapeInfo().GetShape();
    num_dim = 64;
    output_node_name = std::string(session_->GetOutputNameAllocated(0, Ort::AllocatorWithDefaultOptions()).get());
}

XTRTextEmbedder::~XTRTextEmbedder() {
}

encoded_input_t XTRTextEmbedder::encode(const std::string& text, const size_t max_seq_len) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto encoded_input = tokenizer_->Encode(text);
    lock.unlock();
    return encoded_input;
}

batch_encoded_input_t XTRTextEmbedder::batch_encode(const std::vector<std::string>& inputs, const size_t max_seq_len) {
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

embedding_res_t XTRTextEmbedder::embed(const std::string& text, const size_t max_seq_len) {
    auto encoded_inputs = encode(text, max_seq_len);
    std::vector<Ort::Value> input_tensors;
    std::vector<const char*> input_node_names = {"input_ids", "attention_mask", "token_type_ids"};
    std::vector<std::vector<int64_t>> input_shapes;
    input_shapes.push_back({1, static_cast<int64_t>(encoded_inputs.input_ids.size())});
    input_shapes.push_back({1, static_cast<int64_t>(encoded_inputs.attention_mask.size())});
    input_shapes.push_back({1, static_cast<int64_t>(encoded_inputs.token_type_ids.size())});

    // xtr model needs 32-bit integers
    std::vector<int> input_ids(encoded_inputs.input_ids.begin(), encoded_inputs.input_ids.end());
    std::vector<int> attention_mask(encoded_inputs.attention_mask.begin(), encoded_inputs.attention_mask.end());
    std::vector<int> token_type_ids(encoded_inputs.token_type_ids.begin(), encoded_inputs.token_type_ids.end());

    Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
    for(int i = 0; i < input_node_names.size(); i++) {
        input_tensors.push_back(Ort::Value::CreateTensor<int>(memory_info, (int*) input_ids.data(), input_ids.size(), input_shapes[i].data(), input_shapes[i].size()));
    }
    auto allocator = Ort::AllocatorWithDefaultOptions();
    std::vector<embedding_res_t> outputs;
    std::vector<const char*> output_node_names = {output_node_name.c_str()};
    std::unique_lock<std::mutex> lock(mutex_);
    auto output_tensor = session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), 1);
    lock.unlock();

    uint64_t* data = output_tensor[0].GetTensorMutableData<uint64_t>();

    auto shape = output_tensor[0].GetTensorTypeAndShapeInfo().GetShape();

    if(shape.size() < 2) {
        shape.insert(shape.begin(), 1);
    }
    
    std::vector<std::vector<uint64_t>> output;
    for (int i = 0; i < shape[1]; i++) {
        std::vector<uint64_t> output_row;
        for (int j = 0; j < shape[2]; j++) {
            output_row.push_back(data[i * shape[2] + j]);
        }
        output.push_back(output_row);
    }

    return embedding_res_t(output);

}

std::vector<embedding_res_t> XTRTextEmbedder::batch_embed(const std::vector<std::string>& inputs, const size_t max_seq_len) {
    std::vector<embedding_res_t> outputs;
    for(int i = 0; i < inputs.size(); i += 16) {
        auto input_batch = std::vector<std::string>(inputs.begin() + i, inputs.begin() + std::min(i + 16, static_cast<int>(inputs.size())));
        auto encoded_inputs = batch_encode(input_batch, max_seq_len);
        
        // create input tensor object from data values
        Ort::AllocatorWithDefaultOptions allocator;
        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtAllocatorType::OrtArenaAllocator, OrtMemType::OrtMemTypeDefault);
        std::vector<Ort::Value> input_tensors;
        std::vector<std::vector<int64_t>> input_shapes;
        std::vector<const char*> input_node_names = {"input_ids", "attention_mask", "token_type_ids"};
        input_shapes.push_back({static_cast<int64_t>(encoded_inputs.input_ids.size()), static_cast<int64_t>(encoded_inputs.input_ids[0].size())});
        input_shapes.push_back({static_cast<int64_t>(encoded_inputs.attention_mask.size()), static_cast<int64_t>(encoded_inputs.attention_mask[0].size())});
        input_shapes.push_back({static_cast<int64_t>(encoded_inputs.token_type_ids.size()), static_cast<int64_t>(encoded_inputs.token_type_ids[0].size())});

        for(int i = 0; i < input_node_names.size(); i++) {
            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, (int64_t*) encoded_inputs.input_ids.data(), encoded_inputs.input_ids.size(), input_shapes[i].data(), input_shapes[i].size()));
        }

        std::vector<const char*> output_node_names = {output_node_name.c_str()};
        std::unique_lock<std::mutex> lock(mutex_);
        auto output_tensor = session_->Run(Ort::RunOptions{nullptr}, input_node_names.data(), input_tensors.data(), input_tensors.size(), output_node_names.data(), 1);
        lock.unlock();
        uint64_t* data = output_tensor[0].GetTensorMutableData<uint64_t>();
        auto shape = output_tensor[0].GetTensorTypeAndShapeInfo().GetShape();
        if(shape.size() < 2) {
            shape.insert(shape.begin(), 1);
        }
        std::vector<std::vector<uint64_t>> output;

        for (int i = 0; i < shape[1]; i++) {
            std::vector<uint64_t> output_row;
            for (int j = 0; j < shape[2]; j++) {
                output_row.push_back(data[i * shape[2] + j]);
            }
            output.push_back(output_row);
        }

        outputs.push_back(embedding_res_t(output));
    }

    return outputs;

}