#include <gtest/gtest.h>
#include "personalization_model_manager.h"
#include "store.h"
#include <filesystem>
#include "collection_manager.h"

class PersonalizationModelManagerTest : public ::testing::Test {
protected:
    std::string temp_dir;
    Store *store;
    CollectionManager& collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    void SetUp() override {
        temp_dir = (std::filesystem::temp_directory_path() / "personalization_model_manager_test").string();
        system(("rm -rf " + temp_dir + " && mkdir -p " + temp_dir).c_str());
        std::string test_dir = "/tmp/typesense_test/models";
        system(("rm -rf " + test_dir + " && mkdir -p " + test_dir).c_str());
        EmbedderManager::set_model_dir(test_dir);

        // Create test collection
        std::string state_dir_path = "/tmp/typesense_test/personalization_model_manager_test";
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
        PersonalizationModelManager::init(store);
    }

    void TearDown() override {
        std::string test_dir = "/tmp/typesense_test";
        system(("rm -rf " + test_dir).c_str());
        collectionManager.dispose();
        delete store;
    }

    nlohmann::json create_valid_model(const std::string& id = "") {
        nlohmann::json model;
        model["id"] = id;
        model["name"] = "ts/tyrec-1";
        model["type"] = "recommendation";
        model["collection"] = "companies";
        return model;
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
};

TEST_F(PersonalizationModelManagerTest, AddModelSuccess) {
    nlohmann::json model = create_valid_model("test_id");
    std::string model_data = get_onnx_model_archive();
    auto result = PersonalizationModelManager::add_model(model, "test_id", true, model_data);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.get().empty());
}

TEST_F(PersonalizationModelManagerTest, AddModelDuplicate) {
    nlohmann::json model = create_valid_model("test_id");
    auto result1 = PersonalizationModelManager::add_model(model, "test_id", true);
    ASSERT_FALSE(result1.ok());
    ASSERT_EQ(result1.code(), 409);
    ASSERT_EQ(result1.error(), "Model id already exists");
}

TEST_F(PersonalizationModelManagerTest, GetModelSuccess) {
    auto get_result = PersonalizationModelManager::get_model("test_id");
    ASSERT_TRUE(get_result.ok());
    ASSERT_EQ(get_result.get()["id"], "test_id");
}

TEST_F(PersonalizationModelManagerTest, GetModelNotFound) {
    auto result = PersonalizationModelManager::get_model("nonexistent");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 404);
    ASSERT_EQ(result.error(), "Model not found");
}

TEST_F(PersonalizationModelManagerTest, DeleteModelSuccess) {
    auto delete_result = PersonalizationModelManager::delete_model("test_id");
    ASSERT_TRUE(delete_result.ok());
    ASSERT_EQ(delete_result.get()["id"], "test_id");

    auto get_result = PersonalizationModelManager::get_model("test_id");
    ASSERT_FALSE(get_result.ok());
    ASSERT_EQ(get_result.code(), 404);
    ASSERT_EQ(get_result.error(), "Model not found");
}

TEST_F(PersonalizationModelManagerTest, DeleteModelNotFound) {
    auto result = PersonalizationModelManager::delete_model("nonexistent");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 404);
    ASSERT_EQ(result.error(), "Model not found");
}

TEST_F(PersonalizationModelManagerTest, GetAllModelsEmpty) {
    auto result = PersonalizationModelManager::get_all_models();
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.get().empty());
}

TEST_F(PersonalizationModelManagerTest, GetAllModelsWithData) {
    nlohmann::json model1 = create_valid_model("test_id1");
    nlohmann::json model2 = create_valid_model("test_id2");
    
    PersonalizationModelManager::add_model(model1, "test_id1", true, get_onnx_model_archive());
    PersonalizationModelManager::add_model(model2, "test_id2", true, get_onnx_model_archive());

    auto result = PersonalizationModelManager::get_all_models();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.get().size(), 2);
}

TEST_F(PersonalizationModelManagerTest, UpdateModelSuccess) {
    nlohmann::json model = create_valid_model("test_id");
    auto add_result = PersonalizationModelManager::add_model(model, "test_id", true, get_onnx_model_archive());
    ASSERT_TRUE(add_result.ok());

    nlohmann::json update;
    update["name"] = "ts/tyrec-1";
    auto update_result = PersonalizationModelManager::update_model("test_id", update, "");
    ASSERT_TRUE(update_result.ok());
    ASSERT_EQ(update_result.get()["name"], "ts/tyrec-1");
}

TEST_F(PersonalizationModelManagerTest, UpdateModelNotFound) {
    nlohmann::json update;
    update["name"] = "ts/tyrec-1";
    auto result = PersonalizationModelManager::update_model("nonexistent", update, "");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 404);
    ASSERT_EQ(result.error(), "Model not found");
}

TEST_F(PersonalizationModelManagerTest, UpdateModelInvalidData) {
    nlohmann::json update;
    update["name"] = "invalid/name";
    auto update_result = PersonalizationModelManager::update_model("test_id", update, "");
    ASSERT_FALSE(update_result.ok());
    ASSERT_EQ(update_result.code(), 400);
    ASSERT_EQ(update_result.error(), "Model namespace must be 'ts'.");
}
