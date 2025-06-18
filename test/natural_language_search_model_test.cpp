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
  ASSERT_EQ(result.error(), "Failed to get response from OpenAI: 400");
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

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareSuccess) {
    NaturalLanguageSearchModel::set_mock_response(R"({
      "result": {
        "response": "To extract the relevant SQL-like query parameters from the user's search query, let's analyze the given information:\n\n- The make can be Honda or BMW.\n- The engine_hp should be at least 200.\n- The driven_wheels should be rear wheel drive.\n- The price range (msrp) is from 20K to 50K.\n- The year should be newer than 2014.\n\nBased on the provided database schema and the Typesense Query Syntax, here's how we can map the user's search query:\n\n### Query Parameters:\n\n- **Make**: Honda or BMW\n- **Engine HP**: at least 200\n- **Driven Wheels**: rear-wheel drive\n- **Price Range (MSRP)**: 20K to 50K\n- **Year**: newer than 2014\n\n### Typesense Query:\n\n```json\n{\n  \"q\": \"test\",\n  \"filter_by\": \"make:[Honda, BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014\",\n  \"sort_by\": \"\"\n}\n```\n\n### Explanation:\n\n- **Make**: We use `make:[Honda, BMW]` to filter by Honda or BMW.\n- **Engine HP**: We use `engine_hp:>=200` to filter by at least 200hp.\n- **Driven Wheels**: We use `driven_wheels:rear wheel drive` to filter by rear-wheel drive.\n- **Price Range (MSRP)**: We assume `20K` and `50K` are in dollars and map them to `msrp:[20000..50000]`.\n- **Year**: We use `year:>2014` to filter by cars newer than 2014.\n\nSince there are no specific sorting criteria mentioned in the query, we leave `sort_by` empty. The query string `q` is also left empty as the user's query can be adequately represented using `filter_by`. \n\nThis query will return results that match the specified criteria.",
        "tool_calls": [],
        "usage": {
          "prompt_tokens": 912,
          "completion_tokens": 415,
          "total_tokens": 1327
        }
      },
      "success": true,
      "errors": [],
      "messages": []
    })", 200, {});

    std::string query = "Find expensive laptops";
    std::string collection_schema_prompt = "Fields: price, name, ...";
    nlohmann::json model_config = {
        {"model_name", "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct"},
        {"api_key", "sk-test"},
        {"account_id", "test"},
        {"max_bytes", 1024}
    };

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["filter_by"], "make:[Honda, BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
    ASSERT_EQ(params["q"], "test");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareResponseFailure) {
    NaturalLanguageSearchModel::set_mock_response("No response", 200, {});

    std::string query = "Find expensive laptops";
    std::string collection_schema_prompt = "Fields: price, name, ...";
    nlohmann::json model_config = {
        {"model_name", "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct"},
        {"api_key", "sk-test"},
        {"account_id", "test"},
        {"max_bytes", 1024}
    };

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 500);
    ASSERT_EQ(result.error(), "Cloudflare API response JSON parse error: Invalid JSON");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareInvalidResponse) {
  NaturalLanguageSearchModel::set_mock_response(R"({
    "result": {
      "response1": "To extract the relevant SQL-like query parameters from the user's search query, let's analyze the given information:\n\n- The make can be Honda or BMW.\n- The engine_hp should be at least 200.\n- The driven_wheels should be rear wheel drive.\n- The price range (msrp) is from 20K to 50K.\n- The year should be newer than 2014.\n\nBased on the provided database schema and the Typesense Query Syntax, here's how we can map the user's search query:\n\n### Query Parameters:\n\n- **Make**: Honda or BMW\n- **Engine HP**: at least 200\n- **Driven Wheels**: rear-wheel drive\n- **Price Range (MSRP)**: 20K to 50K\n- **Year**: newer than 2014\n\n### Typesense Query:\n\n```json\n{\n  \"q\": \"test\",\n  \"filter_by\": \"make:[Honda, BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014\",\n  \"sort_by\": \"\"\n}\n```\n\n### Explanation:\n\n- **Make**: We use `make:[Honda, BMW]` to filter by Honda or BMW.\n- **Engine HP**: We use `engine_hp:>=200` to filter by at least 200hp.\n- **Driven Wheels**: We use `driven_wheels:rear wheel drive` to filter by rear-wheel drive.\n- **Price Range (MSRP)**: We assume `20K` and `50K` are in dollars and map them to `msrp:[20000..50000]`.\n- **Year**: We use `year:>2014` to filter by cars newer than 2014.\n\nSince there are no specific sorting criteria mentioned in the query, we leave `sort_by` empty. The query string `q` is also left empty as the user's query can be adequately represented using `filter_by`. \n\nThis query will return results that match the specified criteria.",
      "tool_calls": [],
      "usage": {
        "prompt_tokens": 912,
        "completion_tokens": 415,
        "total_tokens": 1327
      }
    },
    "success": true,
    "errors": [],
    "messages": []
  })", 200, {});

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct"},
      {"api_key", "sk-test"},
      {"account_id", "test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Invalid format from Cloudflare API");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareFailure) {
  NaturalLanguageSearchModel::set_mock_response("No response", 400, {});

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = {
      {"model_name", "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct"},
      {"api_key", "sk-test"},
      {"account_id", "test"},
      {"max_bytes", 1024}
  };

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Cloudflare API error: HTTP 400");
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

TEST_F(NaturalLanguageSearchModelTest, ValidateModelSuccess) {
  nlohmann::json model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"api_key", "sk-test"},
    {"max_bytes", size_t(1024)}
  };

  auto result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_TRUE(result.ok());

  model_config = {
    {"model_name", "cloudflare/@cf/meta/llama-2-7b-chat-int8"},
    {"api_key", "YOUR_CLOUDFLARE_API_KEY"},
    {"account_id", "YOUR_CLOUDFLARE_ACCOUNT_ID"},
    {"max_bytes", size_t(16000)}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_TRUE(result.ok());

  model_config = {
    {"model_name", "vllm/mistral-7b-instruct"},
    {"api_url", "http://your-vllm-server:8000/generate"},
    {"max_bytes", size_t(16000)},
    {"temperature", 0.0}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelTest, ValidateModelFailure) {
  nlohmann::json model_config = {
    {"api_key", "sk-test"},
    {"max_bytes", size_t(1024)}
  };

  auto result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `model_name` is not provided or not a string.");

  model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"max_bytes", size_t(1024)}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `api_key` is missing or is not a non-empty string.");

  model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"api_key", "sk-test"},
    {"max_bytes", -1}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `max_bytes` is not provided or not a positive integer.");

  model_config = {
    {"model_name", "openai/gpt-3.5-turbo"},
    {"api_key", "sk-test"},
    {"max_bytes", size_t(1024)},
    {"temperature", -1.0}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `temperature` must be a number between 0 and 2.");

  model_config = {
    {"model_name", "cloudflare/@cf/meta/llama-2-7b-chat-int8"},
    {"api_key", "YOUR_CLOUDFLARE_API_KEY"},
    {"max_bytes", size_t(16000)}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `account_id` is missing or is not a non-empty string.");

  model_config = {
    {"model_name", "cloudflare/@cf/meta/llama-2-7b-chat-int8"},
    {"account_id", "YOUR_CLOUDFLARE_ACCOUNT_ID"},
    {"max_bytes", size_t(16000)}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `api_key` is missing or is not a non-empty string.");

  model_config = {
    {"model_name", "cloudflare/@cf/meta/llama-2-7b-chat-int8"},
    {"api_key", "YOUR_CLOUDFLARE_API_KEY"},
    {"account_id", "YOUR_CLOUDFLARE_ACCOUNT_ID"}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `max_bytes` is not provided or not a positive integer.");

  model_config = {
    {"model_name", "vllm/mistral-7b-instruct"},
    {"max_bytes", size_t(16000)},
    {"temperature", 0.0}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `api_url` is missing or is not a non-empty string.");

  model_config = {
    {"model_name", "vllm/mistral-7b-instruct"},
    {"api_url", "http://your-vllm-server:8000/generate"},
    {"temperature", -1.0},
    {"max_bytes", size_t(16000)}
  };

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `temperature` must be a number between 0 and 2.");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleSuccess) {
    NaturalLanguageSearchModel::set_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "{\n  \"q\": \"laptops\",\n  \"filter_by\": \"price:>1000\",\n  \"sort_by\": \"price:desc\"\n}"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP",
          "index": 0
        }
      ],
      "promptFeedback": {
      }
    })", 200, {});

    std::string query = "Find expensive laptops";
    std::string collection_schema_prompt = "Fields: price, name, category...";
    nlohmann::json model_config = {
        {"model_name", "google/gemini-2.5-flash"},
        {"api_key", "test-api-key"},
        {"max_bytes", 1024},
        {"temperature", 0.0}
    };

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["q"], "laptops");
    ASSERT_EQ(params["filter_by"], "price:>1000");
    ASSERT_EQ(params["sort_by"], "price:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleWithOptionalParams) {
    NaturalLanguageSearchModel::set_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "{\n  \"q\": \"*\",\n  \"filter_by\": \"category:electronics && price:[500..2000]\",\n  \"sort_by\": \"rating:desc\"\n}"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP",
          "index": 0
        }
      ]
    })", 200, {});

    std::string query = "Best electronics between $500 and $2000";
    std::string collection_schema_prompt = "Fields: price, name, category, rating...";
    nlohmann::json model_config = {
        {"model_name", "google/gemini-2.5-pro"},
        {"api_key", "test-api-key"},
        {"max_bytes", 2048},
        {"temperature", 0.7},
        {"top_p", 0.95},
        {"top_k", 40},
        {"stop_sequences", nlohmann::json::array({"END", "STOP"})},
        {"api_version", "v1"},
        {"system_prompt", "You are a helpful search assistant"}
    };

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["q"], "*");
    ASSERT_EQ(params["filter_by"], "category:electronics && price:[500..2000]");
    ASSERT_EQ(params["sort_by"], "rating:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleFailure) {
    NaturalLanguageSearchModel::set_mock_response("Internal Server Error", 500, {});

    std::string query = "Find laptops";
    std::string collection_schema_prompt = "Fields: price, name...";
    nlohmann::json model_config = {
        {"model_name", "google/gemini-2.5-flash"},
        {"api_key", "test-api-key"},
        {"max_bytes", 1024}
    };

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 500);
    ASSERT_EQ(result.error(), "Failed to get response from Google Gemini: 500");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleInvalidResponse) {
    NaturalLanguageSearchModel::set_mock_response(R"({
      "error": {
        "code": 400,
        "message": "Invalid request",
        "status": "INVALID_ARGUMENT"
      }
    })", 200, {});

    std::string query = "Find laptops";
    std::string collection_schema_prompt = "Fields: price, name...";
    nlohmann::json model_config = {
        {"model_name", "google/gemini-2.5-flash"},
        {"api_key", "test-api-key"},
        {"max_bytes", 1024}
    };

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 500);
    ASSERT_EQ(result.error(), "No valid candidates in Google Gemini response");
}