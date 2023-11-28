#include "image_embedder.h"
#include "text_embedder_remote.h"

CLIPImageEmbedder::CLIPImageEmbedder(const std::shared_ptr<Ort::Session>& session, const std::shared_ptr<Ort::Env>& env, const std::string& model_path) : image_processor_(model_path), session_(session), env_(env) {
}

embedding_res_t CLIPImageEmbedder::embed(const std::string& encoded_image) {

    // process image
    auto processed_image_op = image_processor_.process_image(encoded_image);

    if (!processed_image_op.ok()) {
        nlohmann::json error_json;
        error_json["error"] = processed_image_op.error();
        return embedding_res_t(processed_image_op.code(), error_json);
    }

    auto processed_image = processed_image_op.get();

    // create input tensor
    std::vector<int64_t> input_shape = {1, 3, 224, 224};
    std::vector<const char*> input_names = {"pixel_values"};
    Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);



    Ort::Value input_tensor = Ort::Value::CreateTensor<float>(memory_info, (float*) processed_image.data(), processed_image.size(), input_shape.data(), input_shape.size());

    // create output tensor
    std::vector<const char*> output_names = {"image_embeds"};

    // run inference
    LOG(INFO) << "Running image embedder";
    auto output_tensors = session_->Run(Ort::RunOptions{nullptr}, input_names.data(), &input_tensor, 1, output_names.data(), output_names.size());

    // get output tensor
    auto output_tensor = output_tensors.front().GetTensorMutableData<float>();
    auto shape = output_tensors.front().GetTensorTypeAndShapeInfo().GetShape();

    if (shape.size() != 2) {
        return embedding_res_t(400, "Invalid shape of output tensor");
    }

    std::vector<float> output_vector;

    for (int i = 0; i < shape[1]; i++) {
        output_vector.push_back(output_tensor[i]);
    }

    return embedding_res_t(std::move(output_vector));
}


std::vector<embedding_res_t> CLIPImageEmbedder::batch_embed(const std::vector<std::string>& inputs) {
    std::vector<processed_image_t> processed_images;
    std::unordered_map<int, embedding_res_t> results;

    int i = 0;
    for (const auto& input : inputs) {
        auto processed_image_op = image_processor_.process_image(input);

        if (!processed_image_op.ok()) {
            nlohmann::json error_json;
            error_json["error"] = processed_image_op.error();
            results[i] = embedding_res_t(processed_image_op.code(), error_json);
            i++;
            continue;
        }

        processed_images.push_back(processed_image_op.get());
        i++;
    }


    // no valid images
    if (processed_images.empty()) {
        std::vector<embedding_res_t> result_vector(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            result_vector[i] = results[i];
        }

        return result_vector;
    }

    // create input tensor
    std::vector<int64_t> input_shape = {static_cast<int64_t>(processed_images.size()), 3, 224, 224};
    std::vector<const char*> input_names = {"input_ids", "pixel_values", "attention_mask"};
    std::vector<int64_t> dummy_input_ids_shape = {1,1};
    std::vector<int64_t> dummy_input_ids = {0};
    std::vector<int64_t> dummy_attention_mask_shape = {1,1};
    std::vector<int64_t> dummy_attention_mask = {1};
    Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

    // convert 2D vector to 1D vector
    std::vector<float> input_vector;
    for (auto& image : processed_images) {
        input_vector.reserve(input_vector.size() + image.size());
        std::move(image.begin(), image.end(), std::back_inserter(input_vector));
        image.clear();
    }
    std::vector<Ort::Value> input_tensors;
    input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, (int64_t*) dummy_input_ids.data(), dummy_input_ids.size(), dummy_input_ids_shape.data(), dummy_input_ids_shape.size()));
    input_tensors.push_back(Ort::Value::CreateTensor<float>(memory_info, (float*) input_vector.data(), input_vector.size(), input_shape.data(), input_shape.size()));
    input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(memory_info, (int64_t*) dummy_attention_mask.data(), dummy_attention_mask.size(), dummy_attention_mask_shape.data(), dummy_attention_mask_shape.size()));


    std::vector<const char*> output_names = {"image_embeds"};


    // run inference
    LOG(INFO) << "Running image embedder";
    auto output_tensors = session_->Run(Ort::RunOptions{nullptr}, input_names.data(), input_tensors.data(), input_tensors.size(), output_names.data(), output_names.size());

    // get output tensor
    auto output_tensor = output_tensors.front().GetTensorMutableData<float>();
    auto shape = output_tensors.front().GetTensorTypeAndShapeInfo().GetShape();

    if (shape.size() != 2) {
        return std::vector<embedding_res_t>(inputs.size(), embedding_res_t(400, "Invalid shape of output tensor"));
    }

    std::vector<embedding_res_t> output(inputs.size());
    i = 0;
    for (int j = 0; j < shape[0]; j++) {
        while (results.find(i) != results.end()) {
            output[i] = results[i];
            i++;
        }
        std::vector<float> output_vector;
        for (int k = 0; k < shape[1]; k++) {
            output_vector.push_back(output_tensor[j * shape[1] + k]);
        }
        output[i] = embedding_res_t(std::move(output_vector));
        i++;
    }

    return output;
}