#include <gtest/gtest.h>
#include <string>
#include <filesystem>
#include "personalization_model.h"
#include "collection_manager.h"

class PersonalizationModelTest : public ::testing::Test {
protected:
    std::string temp_dir;
    Store *store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;
    void SetUp() override {
        temp_dir = (std::filesystem::temp_directory_path() / "personalization_model_test").string();
        system(("rm -rf " + temp_dir + " && mkdir -p " + temp_dir).c_str());
        std::string test_dir = "/tmp/typesense_test/models";
        system(("rm -rf " + test_dir + " && mkdir -p " + test_dir).c_str());
        EmbedderManager::set_model_dir(test_dir);

        // Create test collection
        std::string state_dir_path = "/tmp/typesense_test/personalization_model_test";
        Config::get_instance().set_data_dir(state_dir_path);

        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
        nlohmann::json collection_schema = R"({
            "name": "companies",
            "fields": [
                {"name": "name", "type": "string"}
            ]
        })"_json;

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.create_collection(collection_schema);
        
    }

    void TearDown() override {
        std::string test_dir = "/tmp/typesense_test";
        system(("rm -rf " + test_dir).c_str());
        collectionManager.dispose();
        delete store;
    }

    std::string get_onnx_model_archive() {
        std::string archive_name = "test/resources/models.tar.gz";

        std::ifstream archive_file(archive_name, std::ios::binary);
        std::string archive_content((std::istreambuf_iterator<char>(archive_file)), std::istreambuf_iterator<char>());

        archive_file.close();
        return archive_content;
    }

    std::string get_invalid_onnx_model_archive() {
        std::string content = "This is an invalid ONNX model content";
        std::string filename = (temp_dir + "/model.txt");
        std::ofstream file(filename);
        file << content;
        file.close();

        std::string archive_name = (temp_dir + "/model.tar.gz");
        std::string command = "tar -czf " + archive_name + " -C " + temp_dir + " model.txt";
        system(command.c_str());

        std::ifstream archive_file(archive_name, std::ios::binary);
        std::string archive_content((std::istreambuf_iterator<char>(archive_file)), std::istreambuf_iterator<char>());
        return archive_content;
    }
};

TEST_F(PersonalizationModelTest, ValidateModelBasic) {
    nlohmann::json valid_model = {
        {"id", "test-model"},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };

    auto result = PersonalizationModel::validate_model(valid_model);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(PersonalizationModelTest, ValidateModelMissingFields) {
    nlohmann::json invalid_model = {
        {"name", "ts/tyrec-1"},
        {"collection", "companies"}
    };

    auto result = PersonalizationModel::validate_model(invalid_model);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 400);
    ASSERT_EQ(result.error(), "Missing or invalid 'id' field.");
}

TEST_F(PersonalizationModelTest, ValidateModelInvalidName) {
    nlohmann::json invalid_model = {
        {"id", "test-model"},
        {"name", "invalid/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };

    auto result = PersonalizationModel::validate_model(invalid_model);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 400);
    ASSERT_EQ(result.error(), "Model namespace must be 'ts'.");
}

TEST_F(PersonalizationModelTest, ValidateModelInvalidType) {
    nlohmann::json invalid_model = {
        {"id", "test-model"},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "invalid"}
    };

    auto result = PersonalizationModel::validate_model(invalid_model);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 400);
    ASSERT_EQ(result.error(), "Invalid type. Must be either 'recommendation' or 'search'.");
}

TEST_F(PersonalizationModelTest, ValidateModelInvalidModelName) {
    nlohmann::json invalid_model = {
        {"id", "test-model"},
        {"name", "ts/invalid-model"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };

    auto result = PersonalizationModel::validate_model(invalid_model);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 400);
    ASSERT_EQ(result.error(), "Invalid model name for type. Use 'tyrec-1' for recommendation and 'tyrec-2' for search.");
}

TEST_F(PersonalizationModelTest, GetModelSubdir) {
    std::string model_id = "test-model";
    std::string expected_path = EmbedderManager::get_model_dir() + "/per_" + model_id;
    
    std::string result = PersonalizationModel::get_model_subdir(model_id);
    ASSERT_EQ(result, expected_path);
    ASSERT_TRUE(std::filesystem::exists(result));
}

TEST_F(PersonalizationModelTest, DeleteModel) {
    std::string model_id = "test-model";
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    
    // Create a dummy file in the model directory
    std::ofstream test_file(model_path + "/test.txt");
    test_file.close();
    
    auto result = PersonalizationModel::delete_model(model_id);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(std::filesystem::exists(model_path));
}

TEST_F(PersonalizationModelTest, CreateModel) {
    std::string model_id = "test-model";
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    std::string model_data = get_onnx_model_archive();
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };

    auto result = PersonalizationModel::create_model(model_id, model_json, model_data);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(std::filesystem::exists(model_path));
}

TEST_F(PersonalizationModelTest, CreateModelFailsWithInvalidArchive) {
    std::string model_id = "test-model";
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    std::string invalid_model_data = get_invalid_onnx_model_archive();
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };

    auto result = PersonalizationModel::create_model(model_id, model_json, invalid_model_data);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 400);
    ASSERT_EQ(result.error(), "Missing the required model files in archive");
}


TEST_F(PersonalizationModelTest, UpdateModel) {
    std::string model_id = "test-model";
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    std::string model_data = get_onnx_model_archive();
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };

    std::string updated_model_data = get_onnx_model_archive();
    auto update_result = PersonalizationModel::update_model(model_id, model_json, updated_model_data);
    ASSERT_TRUE(update_result.ok());
    ASSERT_TRUE(std::filesystem::exists(model_path));
}

TEST_F(PersonalizationModelTest, EmbedRecommendations) {
    std::string model_id = "test-model";
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };
    std::string model_data = get_onnx_model_archive();
    auto result = PersonalizationModel::create_model(model_id, model_json, model_data);
    ASSERT_TRUE(result.ok());
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    ASSERT_TRUE(std::filesystem::exists(model_path));
    PersonalizationModel model(model_id);
    std::vector<std::vector<float>> input_vector(8, std::vector<float>(256));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(0.2f, 0.8f);

    for (auto& vec : input_vector) {
        for (float& val : vec) {
            val = 1.0f;
        }
    }

    std::vector<int64_t> user_mask(8, 1);
    embedding_res_t embedding = model.embed_recommendations(input_vector, user_mask);
    ASSERT_TRUE(embedding.success);
    ASSERT_FLOAT_EQ(embedding.embedding[0], -0.10328025f);
    ASSERT_FLOAT_EQ(embedding.embedding[1], -0.10312808f);
    ASSERT_FLOAT_EQ(embedding.embedding[2], -0.088352792f);
    ASSERT_FLOAT_EQ(embedding.embedding[3], -0.045160018f);
    ASSERT_FLOAT_EQ(embedding.embedding[255], -0.050552275f);
    ASSERT_EQ(embedding.embedding.size(), 256);
}

TEST_F(PersonalizationModelTest, BatchEmbedRecommendations) {
    std::string model_id = "test-model";
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };
    std::string model_data = get_onnx_model_archive();
    auto result = PersonalizationModel::create_model(model_id, model_json, model_data);
    ASSERT_TRUE(result.ok());
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    ASSERT_TRUE(std::filesystem::exists(model_path));
    PersonalizationModel model(model_id);
    std::vector input_vector(2, std::vector(8, std::vector<float>(256)));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(0.2f, 0.8f);

    for (auto& vec : input_vector) {
        for (auto& vec2 : vec) {
            for (float& val : vec2) {
                val = 1.0f;
            }
        }
    }

    std::vector user_mask(2, std::vector<int64_t>(8, 1));
    std::vector<embedding_res_t> embeddings = model.batch_embed_recommendations(input_vector, user_mask);
    ASSERT_EQ(embeddings.size(), 2);
    ASSERT_TRUE(embeddings[0].success);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[0], -0.10328025f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[1], -0.10312808f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[2], -0.088352792f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[3], -0.045160018f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[255], -0.050552275f);
    ASSERT_EQ(embeddings[0].embedding.size(), 256);
    ASSERT_TRUE(embeddings[1].success);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[0], -0.10328025f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[1], -0.10312808f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[2], -0.088352792f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[3], -0.045160018f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[255], -0.050552275f);
    ASSERT_EQ(embeddings[1].embedding.size(), 256);
}

TEST_F(PersonalizationModelTest, EmbedUsers) {
    std::string model_id = "test-model";
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };
    std::string model_data = get_onnx_model_archive();
    auto result = PersonalizationModel::create_model(model_id, model_json, model_data);
    ASSERT_TRUE(result.ok());
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    ASSERT_TRUE(std::filesystem::exists(model_path));
    PersonalizationModel model(model_id);
    std::vector<std::string> input_vector(4, "Hello world");
    embedding_res_t embedding = model.embed_user(input_vector);
    ASSERT_TRUE(embedding.success);
    ASSERT_EQ(embedding.embedding.size(), 256);
    ASSERT_FLOAT_EQ(embedding.embedding[0], 0.0054538441f);
    ASSERT_FLOAT_EQ(embedding.embedding[1], 0.044301841f);
    ASSERT_FLOAT_EQ(embedding.embedding[2], -0.091164835f);
    ASSERT_FLOAT_EQ(embedding.embedding[3], -0.076299265f);
    ASSERT_FLOAT_EQ(embedding.embedding[255], 0.092341594f);
}

TEST_F(PersonalizationModelTest, BatchEmbedUsers) {
    std::string model_id = "test-model";
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };
    std::string model_data = get_onnx_model_archive();
    auto result = PersonalizationModel::create_model(model_id, model_json, model_data);
    ASSERT_TRUE(result.ok());
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    ASSERT_TRUE(std::filesystem::exists(model_path));
    PersonalizationModel model(model_id);
    std::vector<std::vector<std::string>> input_vector(2, std::vector<std::string>(4    , "Hello world"));
    std::vector<embedding_res_t> embeddings = model.batch_embed_users(input_vector);
    ASSERT_EQ(embeddings.size(), 2);
    ASSERT_TRUE(embeddings[0].success);
    ASSERT_EQ(embeddings[0].embedding.size(), 256);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[0], 0.0054538441f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[1], 0.044301841f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[2], -0.091164835f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[3], -0.076299265f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[255], 0.092341594f);
    ASSERT_TRUE(embeddings[1].success);
    ASSERT_EQ(embeddings[1].embedding.size(), 256);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[0], 0.0054538441f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[1], 0.044301841f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[2], -0.091164835f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[3], -0.076299265f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[255], 0.092341594f);
}

TEST_F(PersonalizationModelTest, EmbedItem) {
    std::string model_id = "test-model";
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };
    std::string model_data = get_onnx_model_archive();
    auto result = PersonalizationModel::create_model(model_id, model_json, model_data);
    ASSERT_TRUE(result.ok());
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    ASSERT_TRUE(std::filesystem::exists(model_path));
    PersonalizationModel model(model_id);
    std::vector<std::string> input_vector(4, "Hello world");
    embedding_res_t embedding = model.embed_item(input_vector);
    ASSERT_TRUE(embedding.success);
    ASSERT_EQ(embedding.embedding.size(), 256);
    ASSERT_FLOAT_EQ(embedding.embedding[0], 0.020180844f);
    ASSERT_FLOAT_EQ(embedding.embedding[1], 0.016092315f);
    ASSERT_FLOAT_EQ(embedding.embedding[2], -0.02253399f);
    ASSERT_FLOAT_EQ(embedding.embedding[3], 0.073433787f);
    ASSERT_FLOAT_EQ(embedding.embedding[255], 0.058315977f);
}

TEST_F(PersonalizationModelTest, BatchEmbedItems) {
    std::string model_id = "test-model";
    nlohmann::json model_json = {
        {"id", model_id},
        {"name", "ts/tyrec-1"},
        {"collection", "companies"},
        {"type", "recommendation"}
    };
    std::string model_data = get_onnx_model_archive();
    auto result = PersonalizationModel::create_model(model_id, model_json, model_data);
    ASSERT_TRUE(result.ok());
    std::string model_path = PersonalizationModel::get_model_subdir(model_id);
    ASSERT_TRUE(std::filesystem::exists(model_path));
    PersonalizationModel model(model_id);
    std::vector<std::vector<std::string>> input_vector(2, std::vector<std::string>(4, "Hello world"));
    std::vector<embedding_res_t> embeddings = model.batch_embed_items(input_vector);
    ASSERT_EQ(embeddings.size(), 2);
    ASSERT_TRUE(embeddings[0].success);
    ASSERT_EQ(embeddings[0].embedding.size(), 256);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[0], 0.020180844f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[1], 0.016092315f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[2], -0.02253399f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[3], 0.073433787f);
    ASSERT_FLOAT_EQ(embeddings[0].embedding[255], 0.058315977f);
    ASSERT_TRUE(embeddings[1].success);
    ASSERT_EQ(embeddings[1].embedding.size(), 256);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[0], 0.020180844f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[1], 0.016092315f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[2], -0.02253399f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[3], 0.073433787f);
    ASSERT_FLOAT_EQ(embeddings[1].embedding[255], 0.058315977f);
}