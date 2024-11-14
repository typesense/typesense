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
        std::string content = "This is a sample ONNX model content";
        std::string filename = (temp_dir + "/model.onnx");
        std::ofstream file(filename);
        file << content;
        file.close();

        std::string archive_name = (temp_dir + "/model.tar.gz");
        std::string command = "tar -czf " + archive_name + " -C " + temp_dir + " model.onnx";
        system(command.c_str());

        std::ifstream archive_file(archive_name, std::ios::binary);
        std::string archive_content((std::istreambuf_iterator<char>(archive_file)), std::istreambuf_iterator<char>());

        archive_file.close();
        std::filesystem::remove(filename);
        std::filesystem::remove(archive_name);
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
    ASSERT_EQ(result.error(), "Missing required model.onnx file in archive");
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
