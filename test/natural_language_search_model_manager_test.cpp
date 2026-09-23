#include <gtest/gtest.h>
#include <string>
#include "json.hpp"
#include "store.h"
#include "natural_language_search_model_manager.h"
#include "natural_language_search_model.h"
#include "collection_manager.h"
#include "field.h"
#include "raft_server.h"
#include "tsconfig.h"

class NaturalLanguageSearchModelManagerTest : public ::testing::Test {
protected:
    Store *store;
    std::string state_dir_path;
    std::atomic<bool> quit = false;
    CollectionManager& collectionManager = CollectionManager::get_instance();

    void SetUp() override {
        state_dir_path = "/tmp/typesense_test/nls_model_manager_test";
        system(("rm -rf " + state_dir_path + " && mkdir -p " + state_dir_path).c_str());
        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);
        NaturalLanguageSearchModelManager::init(store);
        NaturalLanguageSearchModel::clear_mock_responses();
    }

    void TearDown() override {
        NaturalLanguageSearchModelManager::dispose();
        collectionManager.dispose();
        delete store;
    }

    nlohmann::json create_valid_model_config() {
        return R"({
          "model_name": "openai/gpt-3.5-turbo",
          "api_key": "YOUR_OPENAI_API_KEY",
          "max_bytes": 1024,
          "temperature": 0.0
        })"_json;
    }

    void add_valid_model_mock_response() {
        NaturalLanguageSearchModel::add_mock_response(R"({
          "object": "chat.completion",
          "model": "gpt-3.5-turbo",
          "choices": [
            {
              "index": 0,
              "message": {
                "role": "assistant",
                "content": "Hello! How can I help you today?"
              },
              "finish_reason": "stop"
            }
          ]
        })", 200, {});
    }

    void add_search_params_mock_response(const nlohmann::json& params) {
        nlohmann::json response = {
            {"choices", {{{"message", {{"role", "assistant"}, {"content", params.dump()}}},
                          {"finish_reason", "stop"}}}}
        };
        NaturalLanguageSearchModel::add_mock_response(response.dump(), 200, {});
    }
};

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelSuccess) {
    add_valid_model_mock_response();

    nlohmann::json model_config = create_valid_model_config();
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelFailure) {
    nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "YOUR_OPENAI_API_KEY",
      "temperature": 0.0
    })"_json;
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `max_bytes` is not provided or not a positive integer.");
    ASSERT_FALSE(result.ok());
    auto model = NaturalLanguageSearchModelManager::get_model(model_id);
    ASSERT_FALSE(model.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, GetModelSuccess) {
    add_valid_model_mock_response();

    nlohmann::json model_config = create_valid_model_config();
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
    add_valid_model_mock_response();

    nlohmann::json model_config = create_valid_model_config();
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

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelInvalidAPIKeyOpenAI) {
    // Mock an invalid API key response from OpenAI
    NaturalLanguageSearchModel::add_mock_response(R"({
      "error": {
        "message": "Incorrect API key provided: YOUR_OPENAI_API_KEY. You can find your API key at https://platform.openai.com/account/api-keys.",
        "type": "invalid_request_error",
        "param": null,
        "code": "invalid_api_key"
      }
    })", 401, {});
    
    nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "YOUR_OPENAI_API_KEY",
      "max_bytes": 1024,
      "temperature": 0.0
    })"_json;
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_FALSE(result.ok());
    ASSERT_NE(result.error().find("Incorrect API key provided"), std::string::npos);
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelAPITimeoutOpenAI) {
    // Mock a timeout response
    NaturalLanguageSearchModel::add_mock_response("", 408, {});
    
    nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "YOUR_OPENAI_API_KEY",
      "max_bytes": 1024,
      "temperature": 0.0
    })"_json;
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error(), "OpenAI API timeout.");
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelInvalidAPIKeyCloudflare) {
    // Mock an invalid API key response from Cloudflare
    NaturalLanguageSearchModel::add_mock_response(R"({
      "errors": [
        {
          "message": "Authentication error: Invalid API key",
          "code": 10000
        }
      ],
      "success": false
    })", 403, {});
    
    nlohmann::json model_config = R"({
      "model_name": "cloudflare/@cf/meta/llama-2-7b-chat-int8",
      "api_key": "INVALID_API_KEY",
      "account_id": "test_account",
      "max_bytes": 1024
    })"_json;
    std::string model_id = "test_cf_model";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_FALSE(result.ok());
    ASSERT_NE(result.error().find("Authentication error"), std::string::npos);
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddModelInvalidCredentialsGCP) {
    // Mock an invalid credentials response from GCP
    NaturalLanguageSearchModel::add_mock_response(R"({
      "error": {
        "code": 401,
        "message": "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other valid authentication credential.",
        "status": "UNAUTHENTICATED"
      }
    })", 401, {});
    
    // Mock failed token refresh
    NaturalLanguageSearchModel::add_mock_response(R"({
      "error": "invalid_grant",
      "error_description": "Token has been expired or revoked."
    })", 400, {});
    
    nlohmann::json model_config = R"({
      "model_name": "gcp/gemini-pro",
      "project_id": "test-project",
      "access_token": "expired_token",
      "refresh_token": "invalid_refresh_token",
      "client_id": "test_client_id",
      "client_secret": "test_client_secret",
      "max_bytes": 1024
    })"_json;
    std::string model_id = "test_gcp_model";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_FALSE(result.ok());
    // Check that the error message is properly formatted without embedded JSON
    ASSERT_NE(result.error().find("Failed to refresh GCP access token: GCP OAuth API error: invalid_grant - Token has been expired or revoked."), std::string::npos);
}

TEST_F(NaturalLanguageSearchModelManagerTest, GetAllModelsSuccess) {
    // Mock successful validation responses for both models
    NaturalLanguageSearchModel::add_mock_response(R"({
      "object": "chat.completion",
      "model": "gpt-3.5-turbo",
      "choices": [
        {
          "index": 0,
          "message": {
            "role": "assistant",
            "content": "Hello!"
          },
          "finish_reason": "stop"
        }
      ]
    })", 200, {});
    
    NaturalLanguageSearchModel::add_mock_response(R"({
      "object": "chat.completion",
      "model": "gpt-3.5-turbo",
      "choices": [
        {
          "index": 0,
          "message": {
            "role": "assistant",
            "content": "Hello!"
          },
          "finish_reason": "stop"
        }
      ]
    })", 200, {});
    
    nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "YOUR_OPENAI_API_KEY",
      "max_bytes": 1024,
      "temperature": 0.0
    })"_json;
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

TEST_F(NaturalLanguageSearchModelManagerTest, ReplicationInitDbReloadsNaturalLanguageSearchModelsFromStore) {
    add_valid_model_mock_response();

    auto model_config = create_valid_model_config();
    auto add_result = NaturalLanguageSearchModelManager::add_model(model_config, "persisted_model", true);
    ASSERT_TRUE(add_result.ok());

    auto models_before_reload = NaturalLanguageSearchModelManager::get_all_models();
    ASSERT_TRUE(models_before_reload.ok());
    ASSERT_EQ(models_before_reload.get().size(), 1);

    NaturalLanguageSearchModelManager::dispose();

    auto models_after_dispose = NaturalLanguageSearchModelManager::get_all_models();
    ASSERT_TRUE(models_after_dispose.ok());
    ASSERT_TRUE(models_after_dispose.get().empty());

    add_valid_model_mock_response();

    Config::get_instance().set_data_dir(state_dir_path);
    ReplicationState replication_state(nullptr, nullptr, store, nullptr, nullptr, nullptr, false,
                                       &Config::get_instance(), 8, 1000);

    ASSERT_EQ(replication_state.init_db(), 0);

    auto models_after_init_db = NaturalLanguageSearchModelManager::get_all_models();
    ASSERT_TRUE(models_after_init_db.ok());
    ASSERT_EQ(models_after_init_db.get().size(), 1);
    ASSERT_EQ(models_after_init_db.get()[0]["id"], "persisted_model");
}

TEST_F(NaturalLanguageSearchModelManagerTest, UpdateModelSuccess) {
    // Mock successful validation for initial OpenAI model
    NaturalLanguageSearchModel::add_mock_response(R"({
      "object": "chat.completion",
      "model": "gpt-3.5-turbo",
      "choices": [
        {
          "index": 0,
          "message": {
            "role": "assistant",
            "content": "Hello!"
          },
          "finish_reason": "stop"
        }
      ]
    })", 200, {});
    
    nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "YOUR_OPENAI_API_KEY",
      "max_bytes": 1024,
      "temperature": 0.0
    })"_json;
    std::string model_id = "test_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_TRUE(result.ok());
    
    // Mock successful validation for updated Cloudflare model
    NaturalLanguageSearchModel::add_mock_response(R"({
      "result": {
        "response": "Hello from Cloudflare!"
      },
      "success": true
    })", 200, {});
    
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
  // Mock successful validation for initial model
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Hello!"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});
  
  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "test_model_id";
  auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(result.ok());
  model_config["model_name"] = "cloudflare/llama-3.2-70b-instruct";
  auto update_result = NaturalLanguageSearchModelManager::update_model(model_id, model_config);
  ASSERT_EQ(update_result.error(), "Property `account_id` is missing or is not a non-empty string.");
  ASSERT_FALSE(update_result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, GetSchemaPromptSuccess) {
  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "price", "type": "int32"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  nlohmann::json insert_doc;
  auto docs = std::vector<std::string>{
    R"({"title": "Cool trousers", "price": 100, "category": "clothing", "tags": ["trousers", "cool"]})",
    R"({"title": "Expensive trousers", "price": 200, "category": "clothing", "tags": ["trousers", "expensive"]})",
    R"({"title": "Utensils", "price": 10, "category": "home", "tags": ["utensils", "tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7"]})"
  };
  auto import_op = coll_create_op.get()->add_many(docs,insert_doc, UPSERT);
  ASSERT_EQ(import_op["num_imported"], 3);

  auto schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(coll_create_op.get()->get_name());
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.get(), R"(You are given the database schema structure below. Your task is to extract relevant SQL-like query parameters from the user's search query.

Database Schema:
Table fields are listed in the format: [Field Name] [Data Type] [Is Indexed] [Is Faceted] [Enum Values]

| Field Name | Data Type | Is Indexed | Is Faceted | Enum Values |
|------------|-----------|------------|------------|-------------|
| price | int32 | Yes | No | N/A |
| category | string | Yes | Yes | [clothing, home] |
| title | string | Yes | No | N/A |
| tags | string[] | Yes | Yes | [trousers, tag7, tag6, tag5, tag4, tag3, tag2, tag1, cool, utensils, ...] |

Instructions:
1. Find all search terms that match fields in the schema.
2. Find filter values for faceted fields. Map user intent to the appropriate value when possible.
3. Ensure that filter terms are properly associated with their fields.
4. For faceted fields, use the example values to interpret user intent even if the exact value isn't specified.
5. Infer query parameters from context, even if not explicitly mentioned.

Typesense Query Syntax:

Filtering:
- Matching values: {fieldName}:{value} or {fieldName}:[value1,value2] for OR conditions
- Numeric filters: {fieldName}:[min..max] for ranges, or {fieldName}:>, {fieldName}:<, {fieldName}:>=, {fieldName}:<=, {fieldName}:=
- Multiple conditions: {condition1} && {condition2}
- OR conditions across fields: {fieldName1}:{value1} || {fieldName2}:{value2}
- Negation: {fieldName}:!= or {fieldName}:!=[value1,value2]
- For values with parentheses, surround with backticks: {fieldName}:`value (with parentheses)`

Sorting:
- Format: {fieldName}:asc or {fieldName}:desc, maximum 3 fields
- Multiple sort fields: {fieldName1}:asc,{fieldName2}:desc

The output should be in JSON format like this:
{
  "q": "Include query only if both filter_by and sort_by are inadequate, remove any other text converted into filter_by or sort_by from the query",
  "filter_by": "typesense filter syntax explained above",
  "sort_by": "typesense sort syntax explained above"
}
)");
}

TEST_F(NaturalLanguageSearchModelManagerTest, SchemaPromptFacetValueCaps) {
  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "category", "type": "string", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  // cat01 gets 12 docs, cat02 gets 11 ... cat12 gets 1, so facet counts order the values deterministically
  std::vector<std::string> docs;
  for(size_t i = 1; i <= 12; i++) {
    std::string category = "cat" + std::string(i < 10 ? "0" : "") + std::to_string(i);
    for(size_t j = 0; j < (13 - i); j++) {
      nlohmann::json doc;
      doc["title"] = category + " doc " + std::to_string(j);
      doc["category"] = category;
      docs.push_back(doc.dump());
    }
  }

  nlohmann::json insert_doc;
  auto import_op = coll_create_op.get()->add_many(docs, insert_doc, UPSERT);
  ASSERT_EQ(import_op["num_imported"], 78);

  const std::string collection_name = coll_create_op.get()->get_name();

  // defaults are unchanged: 20 values fetched, first 10 listed
  auto schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(collection_name);
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_NE(schema_prompt.get().find("| category | string | Yes | Yes | "
                                     "[cat01, cat02, cat03, cat04, cat05, cat06, cat07, cat08, cat09, cat10, ...] |"),
            std::string::npos);

  // raising both caps lists every value and drops the truncation marker
  schema_prompt_params_t all_values;
  all_values.max_facet_values = 12;
  all_values.schema_sample_values = 12;
  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, all_values);
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_NE(schema_prompt.get().find("| category | string | Yes | Yes | "
                                     "[cat01, cat02, cat03, cat04, cat05, cat06, cat07, cat08, cat09, cat10, cat11, cat12] |"),
            std::string::npos);

  // lowering only the display cap still fetches 20, so the marker stays
  schema_prompt_params_t fewer_shown;
  fewer_shown.schema_sample_values = 3;
  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, fewer_shown);
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_NE(schema_prompt.get().find("| category | string | Yes | Yes | [cat01, cat02, cat03, ...] |"),
            std::string::npos);

  // the collection cap truncates before the display cap is applied
  schema_prompt_params_t fewer_fetched;
  fewer_fetched.max_facet_values = 2;
  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, fewer_fetched);
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_NE(schema_prompt.get().find("| category | string | Yes | Yes | [cat01, cat02] |"), std::string::npos);

  // nothing listed, but the field is still flagged as having values
  schema_prompt_params_t none_shown;
  none_shown.schema_sample_values = 0;
  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, none_shown);
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_NE(schema_prompt.get().find("| category | string | Yes | Yes | [...] |"), std::string::npos);
}

TEST_F(NaturalLanguageSearchModelManagerTest, SchemaPromptFacetFieldsSelection) {
  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true},
      {"name": "price", "type": "int32", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  nlohmann::json insert_doc;
  auto docs = std::vector<std::string>{
    R"({"title": "Cool trousers", "category": "clothing", "tags": ["trousers", "cool"], "price": 100})",
    R"({"title": "Warm trousers", "category": "clothing", "tags": ["trousers", "warm"], "price": 200})",
    R"({"title": "Utensils", "category": "home", "tags": ["utensils"], "price": 10})"
  };
  auto import_op = coll_create_op.get()->add_many(docs, insert_doc, UPSERT);
  ASSERT_EQ(import_op["num_imported"], 3);

  const std::string collection_name = coll_create_op.get()->get_name();

  // unlisted facet fields stay in the schema table but contribute no values
  schema_prompt_params_t only_category;
  only_category.facet_fields = {"category"};
  auto schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, only_category);
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_NE(schema_prompt.get().find("| category | string | Yes | Yes | [clothing, home] |"), std::string::npos);
  ASSERT_NE(schema_prompt.get().find("| tags | string[] | Yes | Yes | [Faceted field with unique values] |"),
            std::string::npos);

  schema_prompt_params_t unknown_field;
  unknown_field.facet_fields = {"category", "not_a_field"};
  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, unknown_field);
  ASSERT_FALSE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.code(), 400);
  ASSERT_EQ(schema_prompt.error(),
            "Field `not_a_field` in `nl_facet_fields` is not a faceted string field in collection `titles`.");

  schema_prompt_params_t non_faceted_field;
  non_faceted_field.facet_fields = {"title"};
  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, non_faceted_field);
  ASSERT_FALSE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.error(),
            "Field `title` in `nl_facet_fields` is not a faceted string field in collection `titles`.");

  // faceted, but only string values are enumerated in the prompt
  schema_prompt_params_t numeric_facet_field;
  numeric_facet_field.facet_fields = {"price"};
  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, numeric_facet_field);
  ASSERT_FALSE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.error(),
            "Field `price` in `nl_facet_fields` is not a faceted string field in collection `titles`.");
}

TEST_F(NaturalLanguageSearchModelManagerTest, SchemaPromptCachedPerPromptParams) {
  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  nlohmann::json insert_doc;
  auto docs = std::vector<std::string>{
    R"({"title": "Cool trousers", "category": "clothing", "tags": ["trousers", "cool"]})",
    R"({"title": "Utensils", "category": "home", "tags": ["utensils"]})"
  };
  auto import_op = coll_create_op.get()->add_many(docs, insert_doc, UPSERT);
  ASSERT_EQ(import_op["num_imported"], 2);

  const std::string collection_name = coll_create_op.get()->get_name();

  schema_prompt_params_t only_category;
  only_category.facet_fields = {"category"};

  auto default_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(collection_name);
  ASSERT_TRUE(default_prompt.ok());

  auto scoped_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, only_category);
  ASSERT_TRUE(scoped_prompt.ok());

  // one set of params must not serve another set's cached prompt
  ASSERT_NE(default_prompt.get(), scoped_prompt.get());
  ASSERT_TRUE(NaturalLanguageSearchModelManager::has_cached_schema_prompt(collection_name));

  nlohmann::json update_schema = R"({
    "fields": [
      {"name": "tags", "drop": true}
    ]
  })"_json;
  auto update_op = coll_create_op.get()->alter(update_schema);
  ASSERT_TRUE(update_op.ok());

  // both variants are still served from the cache
  auto cached_default = NaturalLanguageSearchModelManager::get_schema_prompt(collection_name);
  ASSERT_TRUE(cached_default.ok());
  ASSERT_EQ(cached_default.get(), default_prompt.get());

  auto cached_scoped = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, only_category);
  ASSERT_TRUE(cached_scoped.ok());
  ASSERT_EQ(cached_scoped.get(), scoped_prompt.get());

  // clearing the collection drops every variant, not just the default one
  NaturalLanguageSearchModelManager::clear_schema_prompt(collection_name);
  ASSERT_FALSE(NaturalLanguageSearchModelManager::has_cached_schema_prompt(collection_name));

  cached_default = NaturalLanguageSearchModelManager::get_schema_prompt(collection_name);
  ASSERT_TRUE(cached_default.ok());
  ASSERT_EQ(cached_default.get().find("| tags |"), std::string::npos);

  cached_scoped = NaturalLanguageSearchModelManager::get_schema_prompt(
      collection_name, NaturalLanguageSearchModelManager::DEFAULT_SCHEMA_PROMPT_TTL_SEC, only_category);
  ASSERT_TRUE(cached_scoped.ok());
  ASSERT_EQ(cached_scoped.get().find("| tags |"), std::string::npos);
}

TEST_F(NaturalLanguageSearchModelManagerTest, SchemaPromptParamsParsing) {
  std::map<std::string, std::string> req_params;
  auto parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_TRUE(parse_op.ok());
  ASSERT_EQ(parse_op.get().max_facet_values, 20);
  ASSERT_EQ(parse_op.get().schema_sample_values, 10);
  ASSERT_EQ(parse_op.get().facet_sample_percent, 20);
  ASSERT_EQ(parse_op.get().facet_sample_threshold, 1000);
  ASSERT_TRUE(parse_op.get().facet_fields.empty());

  req_params["nl_max_facet_values"] = "500";
  req_params["nl_schema_sample_values"] = "400";
  req_params["nl_facet_sample_percent"] = "100";
  req_params["nl_facet_sample_threshold"] = "5000";
  req_params["nl_facet_fields"] = "category, tags";
  parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_TRUE(parse_op.ok());
  ASSERT_EQ(parse_op.get().max_facet_values, 500);
  ASSERT_EQ(parse_op.get().schema_sample_values, 400);
  ASSERT_EQ(parse_op.get().facet_sample_percent, 100);
  ASSERT_EQ(parse_op.get().facet_sample_threshold, 5000);
  ASSERT_EQ(parse_op.get().facet_fields, std::vector<std::string>({"category", "tags"}));

  req_params.clear();
  req_params["nl_max_facet_values"] = "abc";
  parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_FALSE(parse_op.ok());
  ASSERT_EQ(parse_op.code(), 400);
  ASSERT_EQ(parse_op.error(), "Parameter `nl_max_facet_values` must be a positive integer.");

  req_params.clear();
  req_params["nl_schema_sample_values"] = "-1";
  parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_FALSE(parse_op.ok());
  ASSERT_EQ(parse_op.error(), "Parameter `nl_schema_sample_values` must be a positive integer.");

  req_params.clear();
  req_params["nl_max_facet_values"] = "10001";
  parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_FALSE(parse_op.ok());
  ASSERT_EQ(parse_op.error(), "Parameter `nl_max_facet_values` cannot exceed 10000.");

  req_params.clear();
  req_params["nl_schema_sample_values"] = "10001";
  parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_FALSE(parse_op.ok());
  ASSERT_EQ(parse_op.error(), "Parameter `nl_schema_sample_values` cannot exceed 10000.");

  req_params.clear();
  req_params["nl_facet_sample_percent"] = "101";
  parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_FALSE(parse_op.ok());
  ASSERT_EQ(parse_op.error(), "Parameter `nl_facet_sample_percent` must be between 0 and 100.");

  // empty values fall back to the defaults instead of erroring
  req_params.clear();
  req_params["nl_max_facet_values"] = "";
  req_params["nl_facet_fields"] = "";
  parse_op = schema_prompt_params_t::parse(req_params);
  ASSERT_TRUE(parse_op.ok());
  ASSERT_EQ(parse_op.get().max_facet_values, 20);
  ASSERT_TRUE(parse_op.get().facet_fields.empty());
}

TEST_F(NaturalLanguageSearchModelManagerTest, SchemaPromptParamsFromRequestAndPreset) {
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {"role": "assistant", "content": "Hello!"},
        "finish_reason": "stop"
      }
    ]
  })", 200, {});

  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "default";
  auto add_model_op = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(add_model_op.ok());

  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "category", "type": "string", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "cheap trousers";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";
  req_params["nl_facet_sample_percent"] = "101";

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.code(), 400);
  ASSERT_EQ(nl_search_op.error(), "Parameter `nl_facet_sample_percent` must be between 0 and 100.");
  ASSERT_EQ(req_params["error"], "Parameter `nl_facet_sample_percent` must be between 0 and 100.");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
  ASSERT_EQ(req_params["q"], "cheap trousers");

  // the preset API stores the search params object itself, without a `value` wrapper
  nlohmann::json preset_value = R"({
    "collection": "titles",
    "nl_query": true,
    "nl_max_facet_values": 500,
    "nl_schema_sample_values": 400,
    "nl_facet_fields": "category"
  })"_json;

  auto preset_op = CollectionManager::get_instance().upsert_preset("titles-nl-preset", preset_value);
  ASSERT_TRUE(preset_op.ok());

  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "{\n  \"q\": \"trousers\",\n  \"filter_by\": \"category:clothing\",\n  \"sort_by\": \"\"\n}"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});

  req_params.clear();
  req_params["preset"] = "titles-nl-preset";
  req_params["q"] = "cheap trousers";
  req_params["query_by"] = "title";

  nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_TRUE(nl_search_op.ok());
  ASSERT_EQ(req_params["collection"], "titles");
  ASSERT_EQ(req_params["nl_max_facet_values"], "500");
  ASSERT_EQ(req_params["nl_schema_sample_values"], "400");
  ASSERT_EQ(req_params["nl_facet_fields"], "category");
  ASSERT_EQ(req_params["filter_by"], "category:clothing");
}

TEST_F(NaturalLanguageSearchModelManagerTest, InvalidPresetFacetLimit) {
  for(const auto& value : {nlohmann::json(-1), nlohmann::json(2.5)}) {
    SCOPED_TRACE(value.dump());
    nlohmann::json preset_value = {
      {"nl_query", true},
      {"nl_max_facet_values", value}
    };
    ASSERT_TRUE(collectionManager.upsert_preset("invalid-nl-preset", preset_value).ok());

    std::map<std::string, std::string> req_params = {
      {"preset", "invalid-nl-preset"},
      {"collection", "products"},
      {"q", "cheap trousers"}
    };
    // No model is registered: validation must reject the limit before model processing.
    auto result = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 400);
    ASSERT_EQ(result.error(), "Parameter `nl_max_facet_values` must be a positive integer.");
    ASSERT_EQ(req_params["_nl_processing_failed"], "true");
    ASSERT_EQ(req_params["q"], "cheap trousers");
  }
}

TEST_F(NaturalLanguageSearchModelManagerTest, NonScalarPresetValue) {
  for(const auto& value : {nlohmann::json::array(), nlohmann::json::object(), nlohmann::json(nullptr)}) {
    SCOPED_TRACE(value.dump());
    nlohmann::json preset_value = {
      {"nl_query", true},
      {"collection", value}
    };
    ASSERT_TRUE(collectionManager.upsert_preset("non-scalar-nl-preset", preset_value).ok());

    std::map<std::string, std::string> req_params = {
      {"preset", "non-scalar-nl-preset"},
      {"q", "cheap trousers"}
    };
    auto result = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 400);
    ASSERT_EQ(result.error(), "Invalid value for `collection` in preset `non-scalar-nl-preset`.");
    ASSERT_EQ(req_params["_nl_processing_failed"], "true");
    ASSERT_EQ(req_params.count("collection"), 0);
  }
}

TEST_F(NaturalLanguageSearchModelManagerTest, MissingCollection) {
  std::map<std::string, std::string> req_params = {
    {"nl_query", "true"},
    {"q", "cheap trousers"}
  };
  auto result = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Parameter `collection` is required.");
}

TEST_F(NaturalLanguageSearchModelManagerTest, PresetFacetFieldsMissingFromCollection) {
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {"role": "assistant", "content": "Hello!"},
        "finish_reason": "stop"
      }
    ]
  })", 200, {});

  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "default";
  auto add_model_op = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(add_model_op.ok());

  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "category", "type": "string", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  // a preset is collection agnostic, so its field list can name a field the target collection does not have
  nlohmann::json preset_value = R"({
    "nl_query": true,
    "nl_facet_fields": "brand"
  })"_json;

  auto preset_op = CollectionManager::get_instance().upsert_preset("nl-preset", preset_value);
  ASSERT_TRUE(preset_op.ok());

  std::map<std::string, std::string> req_params;
  req_params["preset"] = "nl-preset";
  req_params["q"] = "cheap trousers";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.code(), 400);
  ASSERT_EQ(nl_search_op.error(), "Error generating schema prompt: Field `brand` in `nl_facet_fields` is not a "
                                  "faceted string field in collection `titles`.");

  // the search still runs, with the raw question as the query
  ASSERT_EQ(req_params["q"], "cheap trousers");
  ASSERT_EQ(req_params["_fallback_q_used"], "true");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
  ASSERT_EQ(req_params.count("filter_by"), 0);

  nlohmann::json results_json = R"({"found": 0, "hits": []})"_json;
  NaturalLanguageSearchModelManager::add_nl_query_data_to_results(results_json, &req_params, 0);
  ASSERT_EQ(results_json["parsed_nl_query"]["error"], "Error generating schema prompt: Field `brand` in "
                                                      "`nl_facet_fields` is not a faceted string field in "
                                                      "collection `titles`.");
  ASSERT_TRUE(results_json["parsed_nl_query"]["generated_params"].empty());
}

TEST_F(NaturalLanguageSearchModelManagerTest, RequestParamsWinOverPreset) {
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {"role": "assistant", "content": "Hello!"},
        "finish_reason": "stop"
      }
    ]
  })", 200, {});

  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "default";
  auto add_model_op = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(add_model_op.ok());

  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  nlohmann::json preset_value = R"({
    "collection": "preset-collection",
    "nl_query": true,
    "q": "preset query",
    "nl_model_id": "preset_model",
    "nl_max_facet_values": 500,
    "nl_schema_sample_values": 400,
    "nl_facet_fields": "category"
  })"_json;

  auto preset_op = CollectionManager::get_instance().upsert_preset("titles-nl-preset", preset_value);
  ASSERT_TRUE(preset_op.ok());

  // an explicit nl_query=false is not flipped on by the preset
  std::map<std::string, std::string> req_params;
  req_params["preset"] = "titles-nl-preset";
  req_params["nl_query"] = "false";
  req_params["q"] = "request query";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.error(), "No nl_query found in either URL parameters or JSON body");
  ASSERT_EQ(req_params["nl_query"], "false");
  ASSERT_EQ(req_params["q"], "request query");

  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "{\n  \"q\": \"trousers\",\n  \"filter_by\": \"category:clothing\",\n  \"sort_by\": \"\"\n}"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});

  // every param the request set survives, the preset only fills the rest
  req_params.clear();
  req_params["preset"] = "titles-nl-preset";
  req_params["nl_query"] = "true";
  req_params["q"] = "request query";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";
  req_params["nl_model_id"] = "default";
  req_params["nl_max_facet_values"] = "30";
  req_params["nl_facet_fields"] = "tags";

  nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_TRUE(nl_search_op.ok());
  ASSERT_EQ(req_params["_original_nl_query"], "request query");
  ASSERT_EQ(req_params["collection"], "titles");
  ASSERT_EQ(req_params["nl_model_id"], "default");
  ASSERT_EQ(req_params["nl_max_facet_values"], "30");
  ASSERT_EQ(req_params["nl_facet_fields"], "tags");
  // unset by the request, so the preset value applies
  ASSERT_EQ(req_params["nl_schema_sample_values"], "400");
}

TEST_F(NaturalLanguageSearchModelManagerTest, SchemaPromptCacheExpiryOnCollectionAlter) {
  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "price", "type": "int32"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  NaturalLanguageSearchModelManager::set_mock_time_for_testing(std::chrono::system_clock::now());
  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  auto schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(coll_create_op.get()->get_name());
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.get(), R"(You are given the database schema structure below. Your task is to extract relevant SQL-like query parameters from the user's search query.

Database Schema:
Table fields are listed in the format: [Field Name] [Data Type] [Is Indexed] [Is Faceted] [Enum Values]

| Field Name | Data Type | Is Indexed | Is Faceted | Enum Values |
|------------|-----------|------------|------------|-------------|
| price | int32 | Yes | No | N/A |
| category | string | Yes | Yes | [Faceted field with unique values] |
| title | string | Yes | No | N/A |
| tags | string[] | Yes | Yes | [Faceted field with unique values] |

Instructions:
1. Find all search terms that match fields in the schema.
2. Find filter values for faceted fields. Map user intent to the appropriate value when possible.
3. Ensure that filter terms are properly associated with their fields.
4. For faceted fields, use the example values to interpret user intent even if the exact value isn't specified.
5. Infer query parameters from context, even if not explicitly mentioned.

Typesense Query Syntax:

Filtering:
- Matching values: {fieldName}:{value} or {fieldName}:[value1,value2] for OR conditions
- Numeric filters: {fieldName}:[min..max] for ranges, or {fieldName}:>, {fieldName}:<, {fieldName}:>=, {fieldName}:<=, {fieldName}:=
- Multiple conditions: {condition1} && {condition2}
- OR conditions across fields: {fieldName1}:{value1} || {fieldName2}:{value2}
- Negation: {fieldName}:!= or {fieldName}:!=[value1,value2]
- For values with parentheses, surround with backticks: {fieldName}:`value (with parentheses)`

Sorting:
- Format: {fieldName}:asc or {fieldName}:desc, maximum 3 fields
- Multiple sort fields: {fieldName1}:asc,{fieldName2}:desc

The output should be in JSON format like this:
{
  "q": "Include query only if both filter_by and sort_by are inadequate, remove any other text converted into filter_by or sort_by from the query",
  "filter_by": "typesense filter syntax explained above",
  "sort_by": "typesense sort syntax explained above"
}
)");

  auto has_cached_schema_prompt = NaturalLanguageSearchModelManager::has_cached_schema_prompt(coll_create_op.get()->get_name());
  ASSERT_TRUE(has_cached_schema_prompt);

  nlohmann::json update_schema = R"({
    "fields": [
      {"name": "tags", "drop": true}
    ]
  })"_json;

  auto update_op = coll_create_op.get()->alter(update_schema);
  ASSERT_TRUE(update_op.ok());

  NaturalLanguageSearchModelManager::clear_schema_prompt(coll_create_op.get()->get_name());

  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(coll_create_op.get()->get_name());
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.get(), R"(You are given the database schema structure below. Your task is to extract relevant SQL-like query parameters from the user's search query.

Database Schema:
Table fields are listed in the format: [Field Name] [Data Type] [Is Indexed] [Is Faceted] [Enum Values]

| Field Name | Data Type | Is Indexed | Is Faceted | Enum Values |
|------------|-----------|------------|------------|-------------|
| price | int32 | Yes | No | N/A |
| category | string | Yes | Yes | [Faceted field with unique values] |
| title | string | Yes | No | N/A |

Instructions:
1. Find all search terms that match fields in the schema.
2. Find filter values for faceted fields. Map user intent to the appropriate value when possible.
3. Ensure that filter terms are properly associated with their fields.
4. For faceted fields, use the example values to interpret user intent even if the exact value isn't specified.
5. Infer query parameters from context, even if not explicitly mentioned.

Typesense Query Syntax:

Filtering:
- Matching values: {fieldName}:{value} or {fieldName}:[value1,value2] for OR conditions
- Numeric filters: {fieldName}:[min..max] for ranges, or {fieldName}:>, {fieldName}:<, {fieldName}:>=, {fieldName}:<=, {fieldName}:=
- Multiple conditions: {condition1} && {condition2}
- OR conditions across fields: {fieldName1}:{value1} || {fieldName2}:{value2}
- Negation: {fieldName}:!= or {fieldName}:!=[value1,value2]
- For values with parentheses, surround with backticks: {fieldName}:`value (with parentheses)`

Sorting:
- Format: {fieldName}:asc or {fieldName}:desc, maximum 3 fields
- Multiple sort fields: {fieldName1}:asc,{fieldName2}:desc

The output should be in JSON format like this:
{
  "q": "Include query only if both filter_by and sort_by are inadequate, remove any other text converted into filter_by or sort_by from the query",
  "filter_by": "typesense filter syntax explained above",
  "sort_by": "typesense sort syntax explained above"
}
)");
}

TEST_F(NaturalLanguageSearchModelManagerTest, SchemaPromptCacheExpiryOnTTL) {
  NaturalLanguageSearchModelManager::set_mock_time_for_testing(std::chrono::system_clock::now());

  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "price", "type": "int32"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  auto schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(coll_create_op.get()->get_name());
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.get(), R"(You are given the database schema structure below. Your task is to extract relevant SQL-like query parameters from the user's search query.

Database Schema:
Table fields are listed in the format: [Field Name] [Data Type] [Is Indexed] [Is Faceted] [Enum Values]

| Field Name | Data Type | Is Indexed | Is Faceted | Enum Values |
|------------|-----------|------------|------------|-------------|
| price | int32 | Yes | No | N/A |
| category | string | Yes | Yes | [Faceted field with unique values] |
| title | string | Yes | No | N/A |
| tags | string[] | Yes | Yes | [Faceted field with unique values] |

Instructions:
1. Find all search terms that match fields in the schema.
2. Find filter values for faceted fields. Map user intent to the appropriate value when possible.
3. Ensure that filter terms are properly associated with their fields.
4. For faceted fields, use the example values to interpret user intent even if the exact value isn't specified.
5. Infer query parameters from context, even if not explicitly mentioned.

Typesense Query Syntax:

Filtering:
- Matching values: {fieldName}:{value} or {fieldName}:[value1,value2] for OR conditions
- Numeric filters: {fieldName}:[min..max] for ranges, or {fieldName}:>, {fieldName}:<, {fieldName}:>=, {fieldName}:<=, {fieldName}:=
- Multiple conditions: {condition1} && {condition2}
- OR conditions across fields: {fieldName1}:{value1} || {fieldName2}:{value2}
- Negation: {fieldName}:!= or {fieldName}:!=[value1,value2]
- For values with parentheses, surround with backticks: {fieldName}:`value (with parentheses)`

Sorting:
- Format: {fieldName}:asc or {fieldName}:desc, maximum 3 fields
- Multiple sort fields: {fieldName1}:asc,{fieldName2}:desc

The output should be in JSON format like this:
{
  "q": "Include query only if both filter_by and sort_by are inadequate, remove any other text converted into filter_by or sort_by from the query",
  "filter_by": "typesense filter syntax explained above",
  "sort_by": "typesense sort syntax explained above"
}
)");
  NaturalLanguageSearchModelManager::advance_mock_time_for_testing(86440);
  nlohmann::json insert_doc;
  auto docs = std::vector<std::string>{
    R"({"title": "Cool trousers", "price": 100, "category": "clothing", "tags": ["trousers", "cool"]})",
    R"({"title": "Expensive trousers", "price": 200, "category": "clothing", "tags": ["trousers", "expensive"]})",
    R"({"title": "Utensils", "price": 10, "category": "home", "tags": ["utensils", "tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7"]})"
  };
  auto import_op = coll_create_op.get()->add_many(docs,insert_doc, UPSERT);
  ASSERT_EQ(import_op["num_imported"], 3);

  schema_prompt = NaturalLanguageSearchModelManager::get_schema_prompt(coll_create_op.get()->get_name());
  ASSERT_TRUE(schema_prompt.ok());
  ASSERT_EQ(schema_prompt.get(), R"(You are given the database schema structure below. Your task is to extract relevant SQL-like query parameters from the user's search query.

Database Schema:
Table fields are listed in the format: [Field Name] [Data Type] [Is Indexed] [Is Faceted] [Enum Values]

| Field Name | Data Type | Is Indexed | Is Faceted | Enum Values |
|------------|-----------|------------|------------|-------------|
| price | int32 | Yes | No | N/A |
| category | string | Yes | Yes | [clothing, home] |
| title | string | Yes | No | N/A |
| tags | string[] | Yes | Yes | [trousers, tag7, tag6, tag5, tag4, tag3, tag2, tag1, cool, utensils, ...] |

Instructions:
1. Find all search terms that match fields in the schema.
2. Find filter values for faceted fields. Map user intent to the appropriate value when possible.
3. Ensure that filter terms are properly associated with their fields.
4. For faceted fields, use the example values to interpret user intent even if the exact value isn't specified.
5. Infer query parameters from context, even if not explicitly mentioned.

Typesense Query Syntax:

Filtering:
- Matching values: {fieldName}:{value} or {fieldName}:[value1,value2] for OR conditions
- Numeric filters: {fieldName}:[min..max] for ranges, or {fieldName}:>, {fieldName}:<, {fieldName}:>=, {fieldName}:<=, {fieldName}:=
- Multiple conditions: {condition1} && {condition2}
- OR conditions across fields: {fieldName1}:{value1} || {fieldName2}:{value2}
- Negation: {fieldName}:!= or {fieldName}:!=[value1,value2]
- For values with parentheses, surround with backticks: {fieldName}:`value (with parentheses)`

Sorting:
- Format: {fieldName}:asc or {fieldName}:desc, maximum 3 fields
- Multiple sort fields: {fieldName1}:asc,{fieldName2}:desc

The output should be in JSON format like this:
{
  "q": "Include query only if both filter_by and sort_by are inadequate, remove any other text converted into filter_by or sort_by from the query",
  "filter_by": "typesense filter syntax explained above",
  "sort_by": "typesense sort syntax explained above"
}
)");
}

TEST_F(NaturalLanguageSearchModelManagerTest, AugmentNLQuerySucess) {
  // Mock successful validation for model creation
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Hello!"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});
  
  // Mock response for the actual NL query processing
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "{\n  \"q\": \"test\",\n  \"filter_by\": \"make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014\",\n  \"sort_by\": \"msrp:desc\"\n}",
          "refusal": null,
          "annotations": []
        },
        "logprobs": null,
        "finish_reason": "stop"
      }
    ],
    "usage": {
      "prompt_tokens": 920,
      "completion_tokens": 58,
      "total_tokens": 978,
      "prompt_tokens_details": {
        "cached_tokens": 0,
        "audio_tokens": 0
      },
      "completion_tokens_details": {
        "reasoning_tokens": 0,
        "audio_tokens": 0,
        "accepted_prediction_tokens": 0,
        "rejected_prediction_tokens": 0
      }
    }
  })", 200, {});

  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "price", "type": "int32"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";

  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "default";
  auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(result.ok());

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_TRUE(nl_search_op.ok());
  ASSERT_EQ(req_params["filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(req_params["sort_by"], "msrp:desc");
  ASSERT_EQ(req_params["q"], "test");
  ASSERT_EQ(req_params["processed_by_nl_model"], "true");
  ASSERT_EQ(req_params["_llm_generated_params"], R"(["filter_by","q","sort_by"])");
  ASSERT_EQ(req_params["_original_llm_filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(req_params["llm_generated_filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");

  req_params["filter_by"] = "engine_hp:>=300";
  
  // Add another mock response for the second call
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "{\n  \"q\": \"test\",\n  \"filter_by\": \"make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014\",\n  \"sort_by\": \"msrp:desc\"\n}",
          "refusal": null,
          "annotations": []
        },
        "logprobs": null,
        "finish_reason": "stop"
      }
    ],
    "usage": {
      "prompt_tokens": 920,
      "completion_tokens": 58,
      "total_tokens": 978
    }
  })", 200, {});
  
  nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_TRUE(nl_search_op.ok());
  ASSERT_EQ(req_params["filter_by"], "engine_hp:>=300 && make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(req_params["sort_by"], "msrp:desc");
  ASSERT_EQ(req_params["q"], "test");
  ASSERT_EQ(req_params["processed_by_nl_model"], "true");
  ASSERT_EQ(req_params["_llm_generated_params"], R"(["filter_by","q","sort_by"])");
  ASSERT_EQ(req_params["_original_llm_filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(req_params["llm_generated_filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
}

TEST_F(NaturalLanguageSearchModelManagerTest, AugmentNLQueryFailureInvalidModel) {
  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "price", "type": "int32"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.error(), "Error getting natural language search model: Model not found");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
}

TEST_F(NaturalLanguageSearchModelManagerTest, AugmentNLQueryFailureInvalidCollection) {
  // Mock successful validation for model creation
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Hello!"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});
  
  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";

  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "default";
  auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(result.ok());

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.error(), "Error generating schema prompt: Collection not found");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
}

TEST_F(NaturalLanguageSearchModelManagerTest, AugmentNLQueryFailureInvalidResponse) {
  // Mock successful validation for model creation
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Hello!"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});
  
  // Mock invalid response for the actual query
  NaturalLanguageSearchModel::add_mock_response("", 200, {});

  nlohmann::json titles_schema = R"({
    "name": "titles",
    "fields": [
      {"name": "title", "type": "string"},
      {"name": "price", "type": "int32"},
      {"name": "category", "type": "string", "facet": true},
      {"name": "tags", "type": "string[]", "facet": true}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(titles_schema);
  ASSERT_TRUE(coll_create_op.ok());

  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";

  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "default";
  auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(result.ok());

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.error(), "Error generating search parameters: Failed to parse OpenAI response: Invalid JSON");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
  ASSERT_EQ(req_params["error"], "Error generating search parameters: Failed to parse OpenAI response: Invalid JSON");

  NaturalLanguageSearchModel::add_mock_response("", 400, {});
  nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.error(), "Error generating search parameters: Failed to get response from OpenAI: 400");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
  ASSERT_EQ(req_params["error"], "Error generating search parameters: Failed to get response from OpenAI: 400");
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddNLQueryDataToResultsSuccess) {
  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";
  req_params["filter_by"] = "engine_hp:>=300 && make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014";
  req_params["sort_by"] = "msrp:desc";
  req_params["q"] = "test";
  req_params["processed_by_nl_model"] = "true";
  req_params["_llm_generated_params"] = R"(["filter_by","q","sort_by"])";
  req_params["_original_llm_filter_by"] = "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014";
  req_params["_original_nl_query"] = "Find expensive laptops";
  req_params["llm_generated_filter_by"] = "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014";

  nlohmann::json results_json;
  NaturalLanguageSearchModelManager::add_nl_query_data_to_results(results_json, &req_params, 1);
  ASSERT_EQ(results_json["parsed_nl_query"]["augmented_params"]["filter_by"], "engine_hp:>=300 && make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(results_json["parsed_nl_query"]["augmented_params"]["sort_by"], "msrp:desc");
  ASSERT_EQ(results_json["parsed_nl_query"]["augmented_params"]["q"], "test");

  ASSERT_EQ(results_json["parsed_nl_query"]["generated_params"]["filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(results_json["parsed_nl_query"]["generated_params"]["sort_by"], "msrp:desc");
  ASSERT_EQ(results_json["parsed_nl_query"]["generated_params"]["q"], "test");

  ASSERT_EQ(results_json["parsed_nl_query"]["parse_time_ms"], 1);
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddNLQueryDataToResultsFailure) {
  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";
  req_params["filter_by"] = "engine_hp:>=300";
  req_params["sort_by"] = "";
  req_params["error"] = "Error generating schema prompt: Collection not found";
  req_params["_nl_processing_failed"] = "true";
  req_params["_fallback_q_used"] = "true";
  nlohmann::json results_json;
  NaturalLanguageSearchModelManager::add_nl_query_data_to_results(results_json, &req_params, 1);
  ASSERT_EQ(results_json["parsed_nl_query"]["augmented_params"]["filter_by"], "engine_hp:>=300");
  ASSERT_EQ(results_json["parsed_nl_query"]["augmented_params"]["q"], "Find expensive laptops");

  ASSERT_EQ(results_json["parsed_nl_query"]["parse_time_ms"], 1);

  ASSERT_EQ(results_json["parsed_nl_query"]["error"], "Error generating schema prompt: Collection not found");
  ASSERT_EQ(results_json["parsed_nl_query"]["generated_params"], nlohmann::json::object());
}

TEST_F(NaturalLanguageSearchModelManagerTest, ExcludeParsedNLQuery) {
  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";
  req_params["filter_by"] = "engine_hp:>=300";
  req_params["sort_by"] = "";
  req_params["error"] = "Error generating schema prompt: Collection not found";
  req_params["_nl_processing_failed"] = "true";
  req_params["_fallback_q_used"] = "true";
  req_params["exclude_fields"] = "parsed_nl_query,found";
  nlohmann::json results_json;
  NaturalLanguageSearchModelManager::add_nl_query_data_to_results(results_json, &req_params, 1);
  ASSERT_EQ(results_json.contains("parsed_nl_query"), false);
  req_params["exclude_fields"] = "parsed_nl_query";
  results_json.clear();
  NaturalLanguageSearchModelManager::add_nl_query_data_to_results(results_json, &req_params, 1);
  ASSERT_EQ(results_json.contains("parsed_nl_query"), false);
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddGoogleModelSuccess) {
    // Mock successful Google Gemini validation
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "Hello from Gemini!"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP"
        }
      ]
    })", 200, {});
    
    nlohmann::json model_config = R"({
      "model_name": "google/gemini-2.5-flash",
      "api_key": "YOUR_GOOGLE_API_KEY",
      "max_bytes": 1024,
      "temperature": 0.0
    })"_json;
    std::string model_id = "test_google_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddGoogleModelWithOptionalParams) {
    // Mock successful Google Gemini validation
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "Hello from Gemini Pro!"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP"
        }
      ]
    })", 200, {});
    
    nlohmann::json model_config = R"({
      "model_name": "google/gemini-2.5-pro",
      "api_key": "YOUR_GOOGLE_API_KEY",
      "max_bytes": 2048,
      "temperature": 0.7,
      "top_p": 0.95,
      "top_k": 40,
      "stop_sequences": ["END", "STOP"],
      "api_version": "v1",
      "system_prompt": "You are a helpful assistant"
    })"_json;
    std::string model_id = "test_google_model_advanced";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, GoogleModelValidationFailures) {
    // Test missing API key
    nlohmann::json model_config = R"({
      "model_name": "google/gemini-2.5-flash",
      "max_bytes": 1024
    })"_json;
    std::string model_id = "test_google_invalid";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `api_key` is missing or is not a non-empty string.");
    ASSERT_FALSE(result.ok());

    // Test invalid temperature
    model_config = R"({
      "model_name": "google/gemini-2.5-flash",
      "api_key": "YOUR_GOOGLE_API_KEY",
      "max_bytes": 1024,
      "temperature": 3.0
    })"_json;
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `temperature` must be a number between 0 and 2.");
    ASSERT_FALSE(result.ok());

    // Test invalid top_p
    model_config = R"({
      "model_name": "google/gemini-2.5-flash",
      "api_key": "YOUR_GOOGLE_API_KEY",
      "max_bytes": 1024,
      "top_p": 1.5
    })"_json;
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `top_p` must be a number between 0 and 1.");
    ASSERT_FALSE(result.ok());

    // Test invalid top_k
    model_config = R"({
      "model_name": "google/gemini-2.5-flash",
      "api_key": "YOUR_GOOGLE_API_KEY",
      "max_bytes": 1024,
      "top_k": -5
    })"_json;
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `top_k` must be a non-negative integer.");
    ASSERT_FALSE(result.ok());

    // Test invalid stop_sequences
    model_config = R"({
      "model_name": "google/gemini-2.5-flash",
      "api_key": "YOUR_GOOGLE_API_KEY",
      "max_bytes": 1024,
      "stop_sequences": "not an array"
    })"_json;
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `stop_sequences` must be an array of strings.");
    ASSERT_FALSE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddGCPModelSuccess) {
    // Mock successful GCP Vertex AI validation
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "Hello from Vertex AI!"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP"
        }
      ]
    })", 200, {});
    
    nlohmann::json model_config = R"({
      "model_name": "gcp/gemini-2.5-flash",
      "project_id": "my-gcp-project",
      "access_token": "initial-access-token",
      "refresh_token": "refresh-token",
      "client_id": "client-id",
      "client_secret": "client-secret",
      "max_bytes": 1024,
      "temperature": 0.0
    })"_json;
    std::string model_id = "test_gcp_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddGCPModelWithOptionalParams) {
    // Mock successful GCP Vertex AI validation
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "Hello from Vertex AI Pro!"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP"
        }
      ]
    })", 200, {});
    
    nlohmann::json model_config = R"({
      "model_name": "gcp/gemini-2.5-pro",
      "project_id": "my-gcp-project",
      "access_token": "initial-access-token",
      "refresh_token": "refresh-token",
      "client_id": "client-id",
      "client_secret": "client-secret",
      "max_bytes": 2048,
      "region": "europe-west1",
      "temperature": 0.7,
      "top_p": 0.95,
      "top_k": 40,
      "max_output_tokens": 4096,
      "system_prompt": "You are a helpful search assistant"
    })"_json;
    std::string model_id = "test_gcp_model_advanced";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, GCPModelValidationFailures) {
    // Test missing project_id
    nlohmann::json model_config = R"({
      "model_name": "gcp/gemini-2.5-flash",
      "access_token": "token",
      "refresh_token": "refresh",
      "client_id": "id",
      "client_secret": "secret",
      "max_bytes": 1024
    })"_json;
    std::string model_id = "test_gcp_invalid";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `project_id` is missing or is not a non-empty string.");
    ASSERT_FALSE(result.ok());

    // Test missing access_token
    model_config = R"({
      "model_name": "gcp/gemini-2.5-flash",
      "project_id": "my-project",
      "refresh_token": "refresh",
      "client_id": "id",
      "client_secret": "secret",
      "max_bytes": 1024
    })"_json;
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `access_token` is missing or is not a non-empty string.");
    ASSERT_FALSE(result.ok());

    // Test invalid temperature
    model_config = R"({
      "model_name": "gcp/gemini-2.5-flash",
      "project_id": "my-project",
      "access_token": "token",
      "refresh_token": "refresh",
      "client_id": "id",
      "client_secret": "secret",
      "max_bytes": 1024,
      "temperature": 3.0
    })"_json;
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `temperature` must be a number between 0 and 2.");
    ASSERT_FALSE(result.ok());

    // Test invalid max_output_tokens
    model_config = R"({
      "model_name": "gcp/gemini-2.5-flash",
      "project_id": "my-project",
      "access_token": "token",
      "refresh_token": "refresh",
      "client_id": "id",
      "client_secret": "secret",
      "max_bytes": 1024,
      "max_output_tokens": -100
    })"_json;
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `max_output_tokens` must be a positive integer.");
    ASSERT_FALSE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, GetNLQueryParamsFromPreset) {
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Hello! How can I help you today?"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});

  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "{\n  \"q\": \"test\",\n  \"filter_by\": \"make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014\",\n  \"sort_by\": \"msrp:desc\"\n}",
          "refusal": null,
          "annotations": []
        },
        "logprobs": null,
        "finish_reason": "stop"
      }
    ],
    "usage": {
      "prompt_tokens": 920,
      "completion_tokens": 58,
      "total_tokens": 978,
      "prompt_tokens_details": {
        "cached_tokens": 0,
        "audio_tokens": 0
      },
      "completion_tokens_details": {
        "reasoning_tokens": 0,
        "audio_tokens": 0,
        "accepted_prediction_tokens": 0,
        "rejected_prediction_tokens": 0
      }
    }
  })", 200, {});
  
  nlohmann::json model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "YOUR_OPENAI_API_KEY",
    "max_bytes": 1024,
    "temperature": 0.0
  })"_json;
  std::string model_id = "test_model_id";
  auto add_model_op = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(add_model_op.ok());

  nlohmann::json collection_schema = R"({
    "name": "companies",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "industry", "type": "string", "facet": true},
      {"name": "location", "type": "string", "facet": true},
      {"name": "employee_count", "type": "int32"}
    ]
  })"_json;

  auto coll_create_op = collectionManager.create_collection(collection_schema);
  ASSERT_TRUE(coll_create_op.ok());


  // the preset API stores the search params object itself, without a `value` wrapper
  nlohmann::json preset_value = R"({
    "nl_model_id": "test_model_id",
    "nl_query": true
  })"_json;

  auto preset_op = CollectionManager::get_instance().upsert_preset("companies-search-preset", preset_value);
  ASSERT_TRUE(preset_op.ok());

  std::map<std::string, std::string> req_params;
  req_params["preset"] = "companies-search-preset";
  req_params["q"] = "test query";
  req_params["collection"] = "companies";

  auto process_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_TRUE(process_op.ok());

  ASSERT_EQ(req_params["nl_query"], "true");
  ASSERT_EQ(req_params["q"], "test");
  ASSERT_EQ(req_params["nl_model_id"], model_id);
}

TEST_F(NaturalLanguageSearchModelManagerTest, FilterOnlyQueryHighlightsOriginalText) {
    add_valid_model_mock_response();
    auto config = create_valid_model_config();
    std::string model_id = "default";
    ASSERT_TRUE(NaturalLanguageSearchModelManager::add_model(config, model_id, false).ok());

    auto schema = R"({
        "name": "titles",
        "enable_nested_fields": true,
        "fields": [
            {"name": "title", "type": "string"},
            {"name": "tags", "type": "string[]"},
            {"name": "details", "type": "object"},
            {"name": "points", "type": "int32"}
        ]
    })"_json;
    auto create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(create_op.ok());
    auto coll = create_op.get();
    ASSERT_TRUE(coll->add(R"({"id":"foo", "title":"Foo", "tags":["Foo", "Other"],
                             "details":{"name":"Foo"}, "points":150})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"bar", "title":"Bar", "tags":["Bar"],
                             "details":{"name":"Bar"}, "points":120})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"excluded", "title":"Foo", "tags":["Foo"],
                             "details":{"name":"Foo"}, "points":50})").ok());

    for(const std::string query: {"*", ""}) {
        for(bool union_search: {false, true}) {
            SCOPED_TRACE(query + (union_search ? " union" : " search"));
            add_search_params_mock_response({{"q", query}, {"filter_by", "points:>100"},
                                             {"sort_by", "points:desc"}});
            std::map<std::string, std::string> params = {
                {"collection", "titles"}, {"query_by", "title"}, {"nl_query", "true"},
                {"q", "Foo over 100 points"}, {"highlight_fields", "title,tags,details.name"},
                {"highlight_full_fields", "title,tags,details.name"}
            };
            auto nl_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(params);
            ASSERT_TRUE(nl_op.ok()) << nl_op.error();
            ASSERT_EQ(query, params["q"]);
            ASSERT_EQ("Foo over 100 points", params["_original_nl_query"]);

            nlohmann::json results;
            if(union_search) {
                std::vector<nlohmann::json> embedded(1, nlohmann::json::object());
                auto op = collectionManager.do_union(params, embedded,
                    nlohmann::json::array({{{"collection", "titles"}}}), results, 0);
                ASSERT_TRUE(op.ok()) << op.error();
            } else {
                std::string response;
                nlohmann::json embedded;
                auto op = collectionManager.do_search(params, embedded, response, 0);
                ASSERT_TRUE(op.ok()) << op.error();
                results = nlohmann::json::parse(response);
                ASSERT_EQ(query, results["request_params"]["q"]);
            }

            ASSERT_EQ(2, results["found"]) << results.dump();
            const auto& hit = results["hits"][0];
            ASSERT_EQ("foo", hit["document"]["id"]);
            ASSERT_EQ("<mark>Foo</mark>", hit["highlight"]["title"]["snippet"]);
            ASSERT_EQ("<mark>Foo</mark>", hit["highlight"]["title"]["value"]);
            ASSERT_EQ("<mark>Foo</mark>", hit["highlight"]["tags"][0]["snippet"]);
            ASSERT_EQ("<mark>Foo</mark>", hit["highlight"]["details"]["name"]["snippet"]);
            ASSERT_FALSE(hit["highlights"].empty());
            ASSERT_EQ("bar", results["hits"][1]["document"]["id"]);
            ASSERT_TRUE(results["hits"][1]["highlight"].empty());
        }
    }
}

TEST_F(NaturalLanguageSearchModelManagerTest, GeneratedQueryControlsRetrievalAndHighlightExpansion) {
    add_valid_model_mock_response();
    auto config = create_valid_model_config();
    std::string model_id = "default";
    ASSERT_TRUE(NaturalLanguageSearchModelManager::add_model(config, model_id, false).ok());

    auto schema = R"({
        "name":"titles",
        "fields":[{"name":"title", "type":"string"}, {"name":"points", "type":"int32"}]
    })"_json;
    auto create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(create_op.ok());
    auto coll = create_op.get();
    ASSERT_TRUE(coll->add(R"({"id":"foo", "title":"Foo", "points":150})").ok());
    ASSERT_TRUE(coll->add(R"({"id":"barrel", "title":"Barrel Foo", "points":120})").ok());

    const std::vector<std::pair<std::string, std::string>> cases = {
        {"bar", "<mark>Bar</mark>rel <mark>Foo</mark>"},
        {"barrelx", "<mark>Barrel</mark> <mark>Foo</mark>"}
    };
    for(const auto& test_case: cases) {
        SCOPED_TRACE(test_case.first);
        add_search_params_mock_response({{"q", test_case.first}, {"filter_by", "points:>100"}});
        std::map<std::string, std::string> params = {
            {"collection", "titles"}, {"query_by", "title"}, {"nl_query", "true"},
            {"q", "Foo over 100 points"}, {"drop_tokens_threshold", "0"}
        };
        auto nl_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(params);
        ASSERT_TRUE(nl_op.ok()) << nl_op.error();
        // A stale raw_query parameter must never override the generated query.
        params["raw_query"] = params["_original_nl_query"];
        std::string response;
        nlohmann::json embedded;
        auto op = collectionManager.do_search(params, embedded, response, 0);
        ASSERT_TRUE(op.ok()) << op.error();
        auto results = nlohmann::json::parse(response);
        ASSERT_EQ(1, results["found"]);
        ASSERT_EQ("barrel", results["hits"][0]["document"]["id"]);
        ASSERT_EQ(test_case.second, results["hits"][0]["highlight"]["title"]["snippet"]);
        ASSERT_EQ(test_case.second, results["hits"][0]["highlights"][0]["snippet"]);
        ASSERT_EQ(test_case.first, results["request_params"]["q"]);
    }
}

TEST_F(NaturalLanguageSearchModelManagerTest, WildcardGeneratedQueryReturnsFilteredBooks) {
    add_valid_model_mock_response();
    auto config = create_valid_model_config();
    std::string model_id = "default";
    ASSERT_TRUE(NaturalLanguageSearchModelManager::add_model(config, model_id, false).ok());

    auto schema = R"({
        "name":"books",
        "fields":[{"name":"title", "type":"string"}, {"name":"category", "type":"string", "facet":true}]
    })"_json;
    auto create_op = collectionManager.create_collection(schema);
    ASSERT_TRUE(create_op.ok());
    auto coll = create_op.get();
    ASSERT_TRUE(coll->add(R"({"title":"Dune", "category":"fiction"})").ok());
    ASSERT_TRUE(coll->add(R"({"title":"Foundation", "category":"fiction"})").ok());
    ASSERT_TRUE(coll->add(R"({"title":"A Brief History of Time", "category":"non-fiction"})").ok());

    add_search_params_mock_response({{"q", "*"}, {"filter_by", "category:[`fiction`]"}});
    std::map<std::string, std::string> params = {
        {"collection", "books"}, {"query_by", "title"}, {"nl_query", "true"},
        {"q", "show me fiction books please"}
    };
    ASSERT_TRUE(NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(params).ok());
    std::string response;
    nlohmann::json embedded;
    ASSERT_TRUE(collectionManager.do_search(params, embedded, response, 0).ok());
    auto results = nlohmann::json::parse(response);
    ASSERT_EQ(2, results["found"]);
    ASSERT_EQ("*", results["request_params"]["q"]);
    for(const auto& hit: results["hits"]) {
        ASSERT_EQ("fiction", hit["document"]["category"]);
        ASSERT_TRUE(hit["highlight"].empty());
        ASSERT_TRUE(hit["highlights"].empty());
    }
}
