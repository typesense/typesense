#include <gtest/gtest.h>
#include <string>
#include "json.hpp"
#include "store.h"
#include "natural_language_search_model_manager.h"
#include "natural_language_search_model.h"
#include "collection_manager.h"
#include "field.h"

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
    }

    void TearDown() override {
        NaturalLanguageSearchModelManager::dispose();
        collectionManager.dispose();
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
  NaturalLanguageSearchModel::set_mock_response(R"({
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

  nlohmann::json model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"api_key", "YOUR_OPENAI_API_KEY"},
    {"max_bytes", size_t(1024)},
    {"temperature", 0.0}
  };
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
  std::map<std::string, std::string> req_params;
  req_params["nl_query"] = "true";
  req_params["q"] = "Find expensive laptops";
  req_params["collection"] = "titles";
  req_params["query_by"] = "title";

  nlohmann::json model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"api_key", "YOUR_OPENAI_API_KEY"},
    {"max_bytes", size_t(1024)},
    {"temperature", 0.0}
  };
  std::string model_id = "default";
  auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(result.ok());

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.error(), "Error generating schema prompt: Collection not found");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
}

TEST_F(NaturalLanguageSearchModelManagerTest, AugmentNLQueryFailureInvalidResponse) {
  NaturalLanguageSearchModel::set_mock_response("", 200, {});

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

  nlohmann::json model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"api_key", "YOUR_OPENAI_API_KEY"},
    {"max_bytes", size_t(1024)},
    {"temperature", 0.0}
  };
  std::string model_id = "default";
  auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
  ASSERT_TRUE(result.ok());

  auto nl_search_op = NaturalLanguageSearchModelManager::process_nl_query_and_augment_params(req_params);
  ASSERT_FALSE(nl_search_op.ok());
  ASSERT_EQ(nl_search_op.error(), "Error generating search parameters: Failed to parse OpenAI response: Invalid JSON");
  ASSERT_EQ(req_params["_nl_processing_failed"], "true");
  ASSERT_EQ(req_params["error"], "Error generating search parameters: Failed to parse OpenAI response: Invalid JSON");

  NaturalLanguageSearchModel::set_mock_response("", 400, {});
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
    nlohmann::json model_config = {
      {"model_name", "google/gemini-2.5-flash"},
      {"api_key", "YOUR_GOOGLE_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"temperature", 0.0}
    };
    std::string model_id = "test_google_model_id";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, AddGoogleModelWithOptionalParams) {
    nlohmann::json model_config = {
      {"model_name", "google/gemini-2.5-pro"},
      {"api_key", "YOUR_GOOGLE_API_KEY"},
      {"max_bytes", size_t(2048)},
      {"temperature", 0.7},
      {"top_p", 0.95},
      {"top_k", 40},
      {"stop_sequences", nlohmann::json::array({"END", "STOP"})},
      {"api_version", "v1"},
      {"system_prompt", "You are a helpful assistant"}
    };
    std::string model_id = "test_google_model_advanced";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "");
    ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelManagerTest, GoogleModelValidationFailures) {
    // Test missing API key
    nlohmann::json model_config = {
      {"model_name", "google/gemini-2.5-flash"},
      {"max_bytes", size_t(1024)}
    };
    std::string model_id = "test_google_invalid";
    auto result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `api_key` is missing or is not a non-empty string.");
    ASSERT_FALSE(result.ok());

    // Test invalid temperature
    model_config = {
      {"model_name", "google/gemini-2.5-flash"},
      {"api_key", "YOUR_GOOGLE_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"temperature", 3.0}
    };
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `temperature` must be a number between 0 and 2.");
    ASSERT_FALSE(result.ok());

    // Test invalid top_p
    model_config = {
      {"model_name", "google/gemini-2.5-flash"},
      {"api_key", "YOUR_GOOGLE_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"top_p", 1.5}
    };
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `top_p` must be a number between 0 and 1.");
    ASSERT_FALSE(result.ok());

    // Test invalid top_k
    model_config = {
      {"model_name", "google/gemini-2.5-flash"},
      {"api_key", "YOUR_GOOGLE_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"top_k", -5}
    };
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `top_k` must be a non-negative integer.");
    ASSERT_FALSE(result.ok());

    // Test invalid stop_sequences
    model_config = {
      {"model_name", "google/gemini-2.5-flash"},
      {"api_key", "YOUR_GOOGLE_API_KEY"},
      {"max_bytes", size_t(1024)},
      {"stop_sequences", "not an array"}
    };
    result = NaturalLanguageSearchModelManager::add_model(model_config, model_id, false);
    ASSERT_EQ(result.error(), "Property `stop_sequences` must be an array of strings.");
    ASSERT_FALSE(result.ok());
}