#include "gtest/gtest.h"
#include "natural_language_search_model.h"
#include "json.hpp"

class NaturalLanguageSearchModelTest : public ::testing::Test {
protected:
    void SetUp() override {
        NaturalLanguageSearchModel::set_mock_response("", 200, {});
    }

    void TearDown() override {
        NaturalLanguageSearchModel::clear_mock_response();
    }
};

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAISuccess) {
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

    std::string query = "Find expensive laptops";
    std::string collection_schema_prompt = "Fields: price, name, ...";
    nlohmann::json model_config = {
        {"model_name", "openai/gpt-3.5-turbo"},
        {"api_key", "sk-test"},
        {"max_bytes", 1024}
    };

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
    ASSERT_EQ(params["q"], "test");
    ASSERT_EQ(params["sort_by"], "msrp:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIRegexJSONSuccess) {
  NaturalLanguageSearchModel::set_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Here is the search params that you should use requested in SQL type:\n{\n  \"q\": \"test\",\n  \"filter_by\": \"make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014\",\n  \"sort_by\": \"msrp:desc\"\n}",
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

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "sk-test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_TRUE(result.ok());
  auto params = result.get();
  ASSERT_EQ(params["filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(params["q"], "test");
  ASSERT_EQ(params["sort_by"], "msrp:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIFailure) {
  NaturalLanguageSearchModel::set_mock_response("No response", 400, {});

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "sk-test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Failed to get response from OpenAI: No response");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIInvalidJSON) {
  NaturalLanguageSearchModel::set_mock_response("Invalid JSON", 200, {});

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "sk-test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Failed to parse OpenAI response: Invalid JSON");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIInvalidResponse) {
  NaturalLanguageSearchModel::set_mock_response(R"({
      "object": "chat.completion",
      "model": "gpt-3.5-turbo",
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

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "sk-test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "No valid response from OpenAI");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIInvalidContentResponse) {
  NaturalLanguageSearchModel::set_mock_response(R"({
      "object": "chat.completion",
      "model": "gpt-3.5-turbo",
      "choices": [
        {
          "index": 0
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

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "sk-test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "No valid response content from OpenAI");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsJSONFailure) {
  NaturalLanguageSearchModel::set_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Here is the search params that you should use requested in SQL type:",
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

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "sk-test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Could not extract search parameters");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsRegexJSONFailure) {
  NaturalLanguageSearchModel::set_mock_response(R"({
    "object": "chat.completion",
    "model": "gpt-3.5-turbo",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Here is the search params that you should use requested in SQL type: { \"q\": \"test\", }",
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

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "openai/gpt-3.5-turbo"},
      {"api_key", "sk-test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Regex JSON parse failed on content");
}