#include "gtest/gtest.h"
#include "natural_language_search_model.h"
#include "json.hpp"

class NaturalLanguageSearchModelTest : public ::testing::Test {
protected:
    void SetUp() override {
        NaturalLanguageSearchModel::clear_mock_responses();
        NaturalLanguageSearchModel::enable_request_capture();
        // Clear any captured requests from previous tests
        NaturalLanguageSearchModel::disable_request_capture();
        NaturalLanguageSearchModel::enable_request_capture();
    }

    void TearDown() override {
        NaturalLanguageSearchModel::clear_mock_responses();
        NaturalLanguageSearchModel::disable_request_capture();
    }
};

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAISuccess) {
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

    std::string query = "Find expensive laptops";
    std::string collection_schema_prompt = "Fields: price, name, ...";
    nlohmann::json model_config = R"({
        "model_name": "openai/gpt-3.5-turbo",
        "api_key": "sk-test",
        "max_bytes": 1024
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
    ASSERT_EQ(params["q"], "test");
    ASSERT_EQ(params["sort_by"], "msrp:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIRegexJSONSuccess) {
  NaturalLanguageSearchModel::add_mock_response(R"({
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
  nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "sk-test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_TRUE(result.ok());
  auto params = result.get();
  ASSERT_EQ(params["filter_by"], "make:[Honda,BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
  ASSERT_EQ(params["q"], "test");
  ASSERT_EQ(params["sort_by"], "msrp:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIFailure) {
  NaturalLanguageSearchModel::add_mock_response("No response", 400, {});

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "sk-test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Failed to get response from OpenAI: 400");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIInvalidJSON) {
  NaturalLanguageSearchModel::add_mock_response("Invalid JSON", 200, {});

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "sk-test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Failed to parse OpenAI response: Invalid JSON");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIInvalidResponse) {
  NaturalLanguageSearchModel::add_mock_response(R"({
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
  nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "sk-test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "No valid response from OpenAI");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsOpenAIInvalidContentResponse) {
  NaturalLanguageSearchModel::add_mock_response(R"({
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
  nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "sk-test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "No valid response content from OpenAI");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareSuccess) {
    NaturalLanguageSearchModel::add_mock_response(R"({
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
    nlohmann::json model_config = R"({
        "model_name": "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct",
        "api_key": "sk-test",
        "account_id": "test",
        "max_bytes": 1024
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["filter_by"], "make:[Honda, BMW] && engine_hp:>=200 && driven_wheels:`rear wheel drive` && msrp:[20000..50000] && year:>2014");
    ASSERT_EQ(params["q"], "test");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareResponseFailure) {
    NaturalLanguageSearchModel::add_mock_response("No response", 200, {});

    std::string query = "Find expensive laptops";
    std::string collection_schema_prompt = "Fields: price, name, ...";
    nlohmann::json model_config = R"({
        "model_name": "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct",
        "api_key": "sk-test",
        "account_id": "test",
        "max_bytes": 1024
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 500);
    ASSERT_EQ(result.error(), "Cloudflare API response JSON parse error: Invalid JSON");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareInvalidResponse) {
  NaturalLanguageSearchModel::add_mock_response(R"({
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
  nlohmann::json model_config = R"({
      "model_name": "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct",
      "api_key": "sk-test",
      "account_id": "test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Invalid format from Cloudflare API");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsCloudflareFailure) {
  NaturalLanguageSearchModel::add_mock_response("No response", 400, {});

  std::string query = "Find expensive laptops";
  std::string collection_schema_prompt = "Fields: price, name, ...";
  nlohmann::json model_config = R"({
      "model_name": "cloudflare/@cf/meta/llama-4-scout-17b-16e-instruct",
      "api_key": "sk-test",
      "account_id": "test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Cloudflare API error: HTTP 400");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsJSONFailure) {
  NaturalLanguageSearchModel::add_mock_response(R"({
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
  nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "sk-test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Could not extract search parameters");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsRegexJSONFailure) {
  NaturalLanguageSearchModel::add_mock_response(R"({
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
  nlohmann::json model_config = R"({
      "model_name": "openai/gpt-3.5-turbo",
      "api_key": "sk-test",
      "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 500);
  ASSERT_EQ(result.error(), "Regex JSON parse failed on content");
}

TEST_F(NaturalLanguageSearchModelTest, ValidateModelSuccess) {
  // Mock successful OpenAI validation
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
    "api_key": "sk-test",
    "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_TRUE(result.ok());

  // Mock successful Cloudflare validation
  NaturalLanguageSearchModel::add_mock_response(R"({
    "result": {
      "response": "Hello from Cloudflare!"
    },
    "success": true
  })", 200, {});
  
  model_config = R"({
    "model_name": "cloudflare/@cf/meta/llama-2-7b-chat-int8",
    "api_key": "YOUR_CLOUDFLARE_API_KEY",
    "account_id": "YOUR_CLOUDFLARE_ACCOUNT_ID",
    "max_bytes": 16000
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_TRUE(result.ok());

  // Mock successful vLLM validation
  NaturalLanguageSearchModel::add_mock_response(R"({
    "object": "chat.completion",
    "model": "mistral-7b-instruct",
    "choices": [
      {
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "Hello from vLLM!"
        },
        "finish_reason": "stop"
      }
    ]
  })", 200, {});
  
  model_config = R"({
    "model_name": "vllm/mistral-7b-instruct",
    "api_url": "http://your-vllm-server:8000/generate",
    "max_bytes": 16000,
    "temperature": 0.0
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_TRUE(result.ok());
}

TEST_F(NaturalLanguageSearchModelTest, ValidateModelFailure) {
  nlohmann::json model_config = R"({
    "api_key": "sk-test",
    "max_bytes": 1024
  })"_json;

  auto result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `model_name` is not provided or not a string.");

  model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "max_bytes": 1024
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `api_key` is missing or is not a non-empty string.");

  model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "sk-test",
    "max_bytes": -1
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `max_bytes` is not provided or not a positive integer.");

  model_config = R"({
    "model_name": "openai/gpt-3.5-turbo",
    "api_key": "sk-test",
    "max_bytes": 1024,
    "temperature": -1.0
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `temperature` must be a number between 0 and 2.");

  model_config = R"({
    "model_name": "cloudflare/@cf/meta/llama-2-7b-chat-int8",
    "api_key": "YOUR_CLOUDFLARE_API_KEY",
    "max_bytes": 16000
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `account_id` is missing or is not a non-empty string.");

  model_config = R"({
    "model_name": "cloudflare/@cf/meta/llama-2-7b-chat-int8",
    "account_id": "YOUR_CLOUDFLARE_ACCOUNT_ID",
    "max_bytes": 16000
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `api_key` is missing or is not a non-empty string.");

  model_config = R"({
    "model_name": "cloudflare/@cf/meta/llama-2-7b-chat-int8",
    "api_key": "YOUR_CLOUDFLARE_API_KEY",
    "account_id": "YOUR_CLOUDFLARE_ACCOUNT_ID"
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `max_bytes` is not provided or not a positive integer.");

  model_config = R"({
    "model_name": "vllm/mistral-7b-instruct",
    "max_bytes": 16000,
    "temperature": 0.0
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `api_url` is missing or is not a non-empty string.");

  model_config = R"({
    "model_name": "vllm/mistral-7b-instruct",
    "api_url": "http://your-vllm-server:8000/generate",
    "temperature": -1.0,
    "max_bytes": 16000
  })"_json;

  result = NaturalLanguageSearchModel::validate_model(model_config);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.code(), 400);
  ASSERT_EQ(result.error(), "Property `temperature` must be a number between 0 and 2.");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleSuccess) {
    NaturalLanguageSearchModel::add_mock_response(R"({
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
    nlohmann::json model_config = R"({
        "model_name": "google/gemini-2.5-flash",
        "api_key": "test-api-key",
        "max_bytes": 1024,
        "temperature": 0.0
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["q"], "laptops");
    ASSERT_EQ(params["filter_by"], "price:>1000");
    ASSERT_EQ(params["sort_by"], "price:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleRequestBody) {
    // Test that Google API request is properly constructed
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "{\n  \"q\": \"test\",\n  \"filter_by\": \"\",\n  \"sort_by\": \"\"\n}"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP",
          "index": 0
        }
      ]
    })", 200, {});

    std::string query = "Find products";
    std::string collection_schema_prompt = "Schema information";
    nlohmann::json model_config = R"({
        "model_name": "google/gemini-2.5-flash",
        "api_key": "test-api-key",
        "max_bytes": 1024,
        "temperature": 0.5,
        "top_p": 0.9,
        "top_k": 30,
        "stop_sequences": ["STOP", "END"],
        "api_version": "v1beta",
        "system_prompt": "Custom instructions"
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify URL construction
    std::string url = NaturalLanguageSearchModel::get_last_request_url();
    ASSERT_EQ(url, "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=test-api-key");
    
    // Verify request body
    std::string request_body_str = NaturalLanguageSearchModel::get_last_request_body();
    nlohmann::json request_body = nlohmann::json::parse(request_body_str);
    
    // Check system instruction
    ASSERT_TRUE(request_body.contains("systemInstruction"));
    ASSERT_EQ(request_body["systemInstruction"]["parts"][0]["text"], "Custom instructions\n\nSchema information");
    
    // Check generation config
    ASSERT_TRUE(request_body.contains("generationConfig"));
    auto& gen_config = request_body["generationConfig"];
    ASSERT_EQ(gen_config["temperature"], 0.5);
    ASSERT_FLOAT_EQ(gen_config["topP"].get<float>(), 0.9f);
    ASSERT_EQ(gen_config["topK"], 30);
    ASSERT_EQ(gen_config["maxOutputTokens"], 1024);
    ASSERT_EQ(gen_config["stopSequences"], nlohmann::json::parse(R"(["STOP", "END"])"));
    
    // Check contents
    ASSERT_TRUE(request_body.contains("contents"));
    ASSERT_EQ(request_body["contents"][0]["parts"][0]["text"], "Find products");
    
    // Verify headers
    auto headers = NaturalLanguageSearchModel::get_last_request_headers();
    ASSERT_EQ(headers["Content-Type"], "application/json");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleWithOptionalParams) {
    NaturalLanguageSearchModel::add_mock_response(R"({
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
    nlohmann::json model_config = R"({
        "model_name": "google/gemini-2.5-pro",
        "api_key": "test-api-key",
        "max_bytes": 2048,
        "temperature": 0.7,
        "top_p": 0.95,
        "top_k": 40,
        "stop_sequences": ["END", "STOP"],
        "api_version": "v1",
        "system_prompt": "You are a helpful search assistant"
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["q"], "*");
    ASSERT_EQ(params["filter_by"], "category:electronics && price:[500..2000]");
    ASSERT_EQ(params["sort_by"], "rating:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleFailure) {
    NaturalLanguageSearchModel::add_mock_response("Internal Server Error", 500, {});

    std::string query = "Find laptops";
    std::string collection_schema_prompt = "Fields: price, name...";
    nlohmann::json model_config = R"({
        "model_name": "google/gemini-2.5-flash",
        "api_key": "test-api-key",
        "max_bytes": 1024
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 500);
    ASSERT_EQ(result.error(), "Failed to get response from Google Gemini: Google Gemini API error: HTTP 500");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGoogleInvalidResponse) {
    NaturalLanguageSearchModel::add_mock_response(R"({
      "error": {
        "code": 400,
        "message": "Invalid request",
        "status": "INVALID_ARGUMENT"
      }
    })", 200, {});

    std::string query = "Find laptops";
    std::string collection_schema_prompt = "Fields: price, name...";
    nlohmann::json model_config = R"({
        "model_name": "google/gemini-2.5-flash",
        "api_key": "test-api-key",
        "max_bytes": 1024
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.code(), 500);
    ASSERT_EQ(result.error(), "No valid candidates in Google Gemini response");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGCPSuccess) {
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "{\n  \"q\": \"electronics\",\n  \"filter_by\": \"category:laptops && price:[1000..3000]\",\n  \"sort_by\": \"rating:desc\"\n}"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP",
          "index": 0,
          "safetyRatings": [
            {
              "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
              "probability": "NEGLIGIBLE"
            }
          ]
        }
      ],
      "promptFeedback": {
        "safetyRatings": [
          {
            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "probability": "NEGLIGIBLE"
          }
        ]
      }
    })", 200, {});

    std::string query = "Find good laptops between $1000 and $3000";
    std::string collection_schema_prompt = "Fields: price, name, category, rating...";
    nlohmann::json model_config = R"({
        "model_name": "gcp/gemini-2.5-flash",
        "project_id": "test-project",
        "access_token": "test-access-token",
        "refresh_token": "test-refresh-token",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "max_bytes": 1024,
        "temperature": 0.0
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);

    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["q"], "electronics");
    ASSERT_EQ(params["filter_by"], "category:laptops && price:[1000..3000]");
    ASSERT_EQ(params["sort_by"], "rating:desc");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGCPTokenRefresh) {
    NaturalLanguageSearchModel::clear_mock_responses();
    
    // 1. First API call returns 401
    NaturalLanguageSearchModel::add_mock_response("Unauthorized", 401, {});
    
    // 2. Token refresh call returns new token
    NaturalLanguageSearchModel::add_mock_response(R"({
        "access_token": "new-access-token",
        "expires_in": 3600,
        "token_type": "Bearer"
    })", 200, {});
    
    // 3. Retry API call with new token succeeds
    NaturalLanguageSearchModel::add_mock_response(R"({
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": "{\n  \"q\": \"products\",\n  \"filter_by\": \"\",\n  \"sort_by\": \"\"\n}"
                        }
                    ],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }
        ]
    })", 200, {});

    std::string query = "Find products";
    std::string collection_schema_prompt = "Fields: name, price...";
    nlohmann::json model_config = R"({
        "model_name": "gcp/gemini-2.5-flash",
        "project_id": "test-project",
        "access_token": "expired-token",
        "refresh_token": "test-refresh-token",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "max_bytes": 1024
    })"_json;

    // This should trigger the full flow: 401 -> token refresh -> retry
    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);
    
    ASSERT_TRUE(result.ok());
    auto params = result.get();
    ASSERT_EQ(params["q"], "products");
    
    // Verify all three requests were made
    ASSERT_EQ(NaturalLanguageSearchModel::get_num_captured_requests(), 3);
    
    // First request: Initial API call that gets 401
    const auto& first_request = NaturalLanguageSearchModel::get_captured_request(0);
    ASSERT_TRUE(first_request.url.find("https://us-central1-aiplatform.googleapis.com") != std::string::npos);
    ASSERT_TRUE(first_request.url.find("gemini-2.5-flash:generateContent") != std::string::npos);
    ASSERT_EQ(first_request.headers.at("Authorization"), "Bearer expired-token");
    
    // Second request: Token refresh
    const auto& token_request = NaturalLanguageSearchModel::get_captured_request(1);
    ASSERT_EQ(token_request.url, "https://oauth2.googleapis.com/token");
    ASSERT_TRUE(token_request.body.find("grant_type=refresh_token") != std::string::npos);
    ASSERT_TRUE(token_request.body.find("refresh_token=test-refresh-token") != std::string::npos);
    ASSERT_TRUE(token_request.body.find("client_id=test-client-id") != std::string::npos);
    ASSERT_TRUE(token_request.body.find("client_secret=test-client-secret") != std::string::npos);
    
    // Third request: Retry with new token
    const auto& retry_request = NaturalLanguageSearchModel::get_captured_request(2);
    ASSERT_EQ(retry_request.url, first_request.url); // Same URL as first request
    ASSERT_EQ(retry_request.body, first_request.body); // Same body as first request
    ASSERT_EQ(retry_request.headers.at("Authorization"), "Bearer new-access-token"); // New token!
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGCPTokenRefreshFailure) {
    // Test token refresh failure
    NaturalLanguageSearchModel::add_mock_response(R"({
      "error": {
        "message": "The refresh token is invalid"
      }
    })", 400, {});

    auto token_result = NaturalLanguageSearchModel::generate_gcp_access_token(
        "invalid-refresh-token", "test-client-id", "test-client-secret");
    
    ASSERT_FALSE(token_result.ok());
    ASSERT_EQ(token_result.error(), "GCP OAuth API error: The refresh token is invalid");
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGCPRequestBody) {
    // Test that request body is properly constructed with all parameters
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "{\n  \"q\": \"test\",\n  \"filter_by\": \"\",\n  \"sort_by\": \"\"\n}"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP",
          "index": 0
        }
      ]
    })", 200, {});

    std::string query = "Find products";
    std::string collection_schema_prompt = "Schema information";
    nlohmann::json model_config = R"({
        "model_name": "gcp/gemini-2.5-pro",
        "project_id": "test-project",
        "access_token": "test-token",
        "refresh_token": "refresh-token",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "max_bytes": 2048,
        "temperature": 0.7,
        "top_p": 0.95,
        "top_k": 40,
        "max_output_tokens": 4096
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify request body
    std::string request_body_str = NaturalLanguageSearchModel::get_last_request_body();
    nlohmann::json request_body = nlohmann::json::parse(request_body_str);
    
    // Check generation config
    ASSERT_TRUE(request_body.contains("generationConfig"));
    auto& gen_config = request_body["generationConfig"];
    ASSERT_FLOAT_EQ(gen_config["temperature"].get<float>(), 0.7f);
    ASSERT_FLOAT_EQ(gen_config["topP"].get<float>(), 0.95f);
    ASSERT_EQ(gen_config["topK"], 40);
    ASSERT_EQ(gen_config["maxOutputTokens"], 4096);
    
    // Check contents
    ASSERT_TRUE(request_body.contains("contents"));
    ASSERT_TRUE(request_body["contents"].is_array());
    ASSERT_EQ(request_body["contents"].size(), 1);
    
    // Verify headers
    auto headers = NaturalLanguageSearchModel::get_last_request_headers();
    ASSERT_EQ(headers["Authorization"], "Bearer test-token");
    ASSERT_EQ(headers["Content-Type"], "application/json");
}

TEST_F(NaturalLanguageSearchModelTest, ValidateOpenAIModelWithValidAPIKey) {
    // Test successful API key validation
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
    
    nlohmann::json model_config = R"({
        "model_name": "openai/gpt-3.5-turbo",
        "api_key": "sk-test-valid-key",
        "max_bytes": 1024
    })"_json;
    
    auto result = NaturalLanguageSearchModel::validate_model(model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify validation API call was made
    ASSERT_EQ(NaturalLanguageSearchModel::get_num_captured_requests(), 1);
    std::string url = NaturalLanguageSearchModel::get_last_request_url();
    ASSERT_EQ(url, "https://api.openai.com/v1/chat/completions");
    
    std::string request_body_str = NaturalLanguageSearchModel::get_last_request_body();
    nlohmann::json request_body = nlohmann::json::parse(request_body_str);
    ASSERT_EQ(request_body["model"], "gpt-3.5-turbo");
    ASSERT_EQ(request_body["messages"], R"([{"role":"user","content":"hello"}])"_json);
    ASSERT_EQ(request_body["max_tokens"], 10);
    ASSERT_EQ(request_body["temperature"], 0);
}

TEST_F(NaturalLanguageSearchModelTest, ValidateOpenAIModelWithInvalidAPIKey) {
    // Test API key validation failure
    NaturalLanguageSearchModel::add_mock_response(R"({
      "error": {
        "message": "Incorrect API key provided: sk-test-invalid. You can find your API key at https://platform.openai.com/account/api-keys.",
        "type": "invalid_request_error",
        "param": null,
        "code": "invalid_api_key"
      }
    })", 401, {});
    
    nlohmann::json model_config = R"({
        "model_name": "openai/gpt-3.5-turbo",
        "api_key": "sk-test-invalid-key",
        "max_bytes": 1024
    })"_json;
    
    auto result = NaturalLanguageSearchModel::validate_model(model_config);
    ASSERT_FALSE(result.ok());
    ASSERT_NE(result.error().find("Incorrect API key provided"), std::string::npos);
}

TEST_F(NaturalLanguageSearchModelTest, ValidateCloudflareModelWithValidCredentials) {
    // Test successful Cloudflare validation
    NaturalLanguageSearchModel::add_mock_response(R"({
      "result": {
        "response": "Hello! I'm here to help."
      },
      "success": true
    })", 200, {});
    
    nlohmann::json model_config = R"({
        "model_name": "cloudflare/@cf/meta/llama-2-7b-chat-int8",
        "api_key": "valid-cf-key",
        "account_id": "valid-account-id",
        "max_bytes": 1024
    })"_json;
    
    auto result = NaturalLanguageSearchModel::validate_model(model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify validation API call
    std::string url = NaturalLanguageSearchModel::get_last_request_url();
    ASSERT_EQ(url, "https://api.cloudflare.com/client/v4/accounts/valid-account-id/ai/run/@cf/meta/llama-2-7b-chat-int8");
}

TEST_F(NaturalLanguageSearchModelTest, ValidateVLLMModelWithAPIUrl) {
    // Test successful vLLM validation
    NaturalLanguageSearchModel::add_mock_response(R"({
      "object": "chat.completion",
      "model": "custom-model",
      "choices": [
        {
          "index": 0,
          "message": {
            "role": "assistant",
            "content": "Hello from vLLM!"
          },
          "finish_reason": "stop"
        }
      ]
    })", 200, {});
    
    nlohmann::json model_config = R"({
        "model_name": "vllm/custom-model",
        "api_url": "http://localhost:8000/v1/chat/completions",
        "max_bytes": 1024
    })"_json;
    
    auto result = NaturalLanguageSearchModel::validate_model(model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify validation API call
    std::string url = NaturalLanguageSearchModel::get_last_request_url();
    ASSERT_EQ(url, "http://localhost:8000/v1/chat/completions");
}

TEST_F(NaturalLanguageSearchModelTest, ValidateGoogleModelWithValidAPIKey) {
    // Test successful Google Gemini validation
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
        "model_name": "google/gemini-pro",
        "api_key": "valid-google-api-key",
        "max_bytes": 1024
    })"_json;
    
    auto result = NaturalLanguageSearchModel::validate_model(model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify validation API call
    std::string url = NaturalLanguageSearchModel::get_last_request_url();
    ASSERT_TRUE(url.find("https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key=valid-google-api-key") != std::string::npos);
}

TEST_F(NaturalLanguageSearchModelTest, ValidateGCPModelWithTokenRefresh) {
    // Test GCP validation with token refresh
    // First call returns 401
    NaturalLanguageSearchModel::add_mock_response(R"({
      "error": {
        "code": 401,
        "message": "Request had invalid authentication credentials.",
        "status": "UNAUTHENTICATED"
      }
    })", 401, {});
    
    // Token refresh succeeds
    NaturalLanguageSearchModel::add_mock_response(R"({
      "access_token": "new-access-token",
      "token_type": "Bearer",
      "expires_in": 3600
    })", 200, {});
    
    // Retry with new token succeeds
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
        "model_name": "gcp/gemini-pro",
        "project_id": "test-project",
        "access_token": "expired-token",
        "refresh_token": "valid-refresh-token",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "max_bytes": 1024
    })"_json;
    
    auto result = NaturalLanguageSearchModel::validate_model(model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify 3 API calls were made (initial, refresh, retry)
    ASSERT_EQ(NaturalLanguageSearchModel::get_num_captured_requests(), 3);
}

TEST_F(NaturalLanguageSearchModelTest, GenerateSearchParamsGCPDifferentRegions) {
    // Test that different regions are properly reflected in the URL
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "{\n  \"q\": \"test\",\n  \"filter_by\": \"\",\n  \"sort_by\": \"\"\n}"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP",
          "index": 0
        }
      ]
    })", 200, {});

    std::string query = "test query";
    std::string collection_schema_prompt = "Fields: name...";
    
    // Test with default region (us-central1)
    nlohmann::json model_config = R"({
        "model_name": "gcp/gemini-2.5-flash",
        "project_id": "test-project",
        "access_token": "test-token",
        "refresh_token": "refresh-token",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "max_bytes": 1024
    })"_json;

    auto result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify default region URL
    std::string url = NaturalLanguageSearchModel::get_last_request_url();
    ASSERT_TRUE(url.find("https://us-central1-aiplatform.googleapis.com/v1/projects/test-project/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent") != std::string::npos);

    // Test with custom region
    model_config["region"] = "europe-west1";
    
    // Add another mock response for the second call
    NaturalLanguageSearchModel::add_mock_response(R"({
      "candidates": [
        {
          "content": {
            "parts": [
              {
                "text": "{\n  \"q\": \"test\",\n  \"filter_by\": \"\",\n  \"sort_by\": \"\"\n}"
              }
            ],
            "role": "model"
          },
          "finishReason": "STOP",
          "index": 0
        }
      ]
    })", 200, {});
    
    result = NaturalLanguageSearchModel::generate_search_params(query, collection_schema_prompt, model_config);
    ASSERT_TRUE(result.ok());
    
    // Verify custom region URL
    url = NaturalLanguageSearchModel::get_last_request_url();
    ASSERT_TRUE(url.find("https://europe-west1-aiplatform.googleapis.com/v1/projects/test-project/locations/europe-west1/publishers/google/models/gemini-2.5-flash:generateContent") != std::string::npos);
}

