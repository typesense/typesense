#include <gtest/gtest.h>
#include <string>
#include "json.hpp"
#include "store.h"
#include "natural_language_search_model_manager.h"

class NaturalLanguageSearchModelManagerTest : public ::testing::Test {
protected:
    Store *store;
    std::string state_dir_path;

    void SetUp() override {
        state_dir_path = "/tmp/typesense_test/nls_model_manager_test";
        system(("rm -rf " + state_dir_path + " && mkdir -p " + state_dir_path).c_str());
        store = new Store(state_dir_path);
        NaturalLanguageSearchModelManager::init(store);
    }

    void TearDown() override {
        NaturalLanguageSearchModelManager::dispose();
        delete store;
    }
};

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelSuccess) {
    nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "YOUR_OPENAI_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"temperature", 0.0}
    };
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelFailure) {
    nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "YOUR_OPENAI_API_KEY"},
      {"temperature", 0.0}
    };
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `max_bytes` is not provided or not a positive integer.");
    ASSERT_FALSE(result.ok());
    auto model = NaturalLanguageSearchModelManager::get_model(model_id);
    ASSERT_FALSE(model.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, GetModelSuccess) {
    nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "YOUR_OPENAI_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"temperature", 0.0}
    };
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_TRUE(result.ok());
    auto model = NaturalLanguageSearchModelManager::get_model(model_id);
    ASSERT_TRUE(model.ok());
    ASSERT_EQ(model.get()["id"], model_id);
}

TEST_F(NaturalLanguageSearchModelManagerTest, GetModelFailure) {
    auto model = NaturalLanguageSearchModelManager::get_model("non_existent_model_id");
    ASSERT_FALSE(model.ok());
    ASSERT_EQ(model.error(), "Model not found");
}

TEST_F(NaturalLanguageSearchModelManagerTest, DeleteModelSuccess) {
    nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "YOUR_OPENAI_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"temperature", 0.0}
    };
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_TRUE(result.ok());
    auto model = NaturalLanguageSearchModelManager::get_model(model_id);
    ASSERT_TRUE(model.ok());
    model = NaturalLanguageSearchModelManager::delete_model(model_id);
    ASSERT_TRUE(model.ok());
    ASSERT_EQ(model.get()["id"], model_id);
    model = NaturalLanguageSearchModelManager::get_model(model_id);
    ASSERT_FALSE(model.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, DeleteModelFailure) {
    auto model = NaturalLanguageSearchModelManager::delete_model("non_existent_model_id");
    ASSERT_FALSE(model.ok());
    ASSERT_EQ(model.error(), "Model not found");
}

TEST_F(NaturalLanguageSearchModelManagerTest, GetAllModelsSuccess) {
    nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "YOUR_OPENAI_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"temperature", 0.0}
    };
    std::string model_id_1 = "test_model_id_1";
    std::string model_id_2 = "test_model_id_2";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id_1, false);
    ASSERT_TRUE(result.ok());
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id_2, false);
    ASSERT_TRUE(result.ok());
    auto models = NaturalLanguageSearchModelManager::get_all_models();
    ASSERT_TRUE(models.ok());
    ASSERT_EQ(models.get().size(), 2);
    ASSERT_EQ(models.get()[0]["id"], model_id_2);
    ASSERT_EQ(models.get()[0]["model_name"], "openai/gpt-3.5-turbo");
    ASSERT_EQ(models.get()[1]["id"], model_id_1);
    ASSERT_EQ(models.get()[1]["model_name"], "openai/gpt-3.5-turbo");
}

TEST_F(NaturalLanguageSearchModelManagerTest, UpdateModelSuccess) {
    nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "YOUR_OPENAI_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"temperature", 0.0}
    };
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_TRUE(result.ok());
    model_config["model_name"] = "cloudflare/llama-3.2-70b-instruct";
    model_config["account_id"] = "YOUR_CLOUDFLARE_ACCOUNT_ID";
    auto update_result = NaturalLanguageSearchModelManager::update_model(model_id, model_config);
    ASSERT_EQ(update_result.error(), "");
    ASSERT_TRUE(update_result.ok());
    auto model = NaturalLanguageSearchModelManager::get_model(model_id);
    ASSERT_TRUE(model.ok());
    ASSERT_EQ(model.get()["model_name"], "cloudflare/llama-3.2-70b-instruct");
}

TEST_F(NaturalLanguageSearchModelManagerTest, UpdateModelFailure) {
  nlohmann::json model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"api_key", "YOUR_OPENAI_API_KEY"},
    {"max_bytes", size_t(1024)},
    {"temperature", 0.0}
  };
  std::string model_id = "test_model_id";
  auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(result.ok());
  model_config["model_name"] = "cloudflare/llama-3.2-70b-instruct";
  auto update_result = NaturalLanguageSearchModelManager::update_model(model_id, model_config);
  ASSERT_EQ(update_result.error(), "Property `account_id` is missing or is not a non-empty string.");
  ASSERT_FALSE(update_result.ok());
}