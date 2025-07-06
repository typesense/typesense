#include <gtest/gtest.h>
#include "conversation_manager.h"
#include "conversation_model.h"


class ConversationTest : public ::testing::Test {
    protected:
        CollectionManager & collectionManager = CollectionManager::get_instance();
        Store* store;
        std::atomic<bool> quit = false;
        nlohmann::json model = R"({
            "id": "0",
            "history_collection": "conversation_store",
            "ttl": 86400
        })"_json;
        void SetUp() override {
            std::string state_dir_path = "/tmp/typesense_test/conversation_test";
            system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

            store = new Store(state_dir_path);
            collectionManager.init(store, 1.0, "auth_key", quit);
            collectionManager.load(8, 1000);

            nlohmann::json schema_json = R"({
                "name": "conversation_store",
                "fields": [
                    {
                        "name": "conversation_id",
                        "type": "string"
                    },
                    {
                        "name": "role",
                        "type": "string",
                        "index": false
                    },
                    {
                        "name": "message",
                        "type": "string",
                        "index": false
                    },
                    {
                        "name": "timestamp",
                        "type": "int32",
                        "sort": true
                    },
                    {
                        "name": "model_id",
                        "type": "string"
                    }
                ]
            })"_json;

            collectionManager.create_collection(schema_json);
            ConversationModelManager::insert_model_for_testing("0", model);
        }

        void TearDown() override {
            collectionManager.dispose();
            delete store;
        }
};


TEST_F(ConversationTest, CreateConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    auto create_res = ConversationManager::get_instance().add_conversation(conversation, model);
    ASSERT_TRUE(create_res.ok());
}

TEST_F(ConversationTest, CreateConversationInvalidType) {
    nlohmann::json conversation = nlohmann::json::object();
    auto create_res = ConversationManager::get_instance().add_conversation(conversation, "conversation_store");
    ASSERT_FALSE(create_res.ok());
    ASSERT_EQ(create_res.code(), 400);
    ASSERT_EQ(create_res.error(), "Conversation is not an array");
}

TEST_F(ConversationTest, GetInvalidConversation) {
    auto get_res = ConversationManager::get_instance().get_conversation("qwerty", model);
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), 404);
    ASSERT_EQ(get_res.error(), "Conversation not found");
}

TEST_F(ConversationTest, AppendConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    nlohmann::json message = nlohmann::json::object();
    message["user"] = "Hello";
    conversation.push_back(message);
    auto create_res = ConversationManager::get_instance().add_conversation(conversation, model);

    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();
    LOG(INFO) << conversation_id;
    auto append_res = ConversationManager::get_instance().add_conversation(conversation, model, conversation_id);
    ASSERT_TRUE(append_res.ok());
    ASSERT_EQ(append_res.get(), conversation_id);
    
    auto get_res = ConversationManager::get_instance().get_conversation(conversation_id, model);

    ASSERT_TRUE(get_res.ok());
    ASSERT_TRUE(get_res.get()["conversation"].is_array());
    ASSERT_EQ(get_res.get()["id"], conversation_id);
    ASSERT_EQ(get_res.get()["conversation"].size(), 2);
    ASSERT_EQ(get_res.get()["conversation"][0]["user"], "Hello");
    ASSERT_EQ(get_res.get()["conversation"][1]["user"], "Hello");
}


TEST_F(ConversationTest, AppendInvalidConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    nlohmann::json message = nlohmann::json::object();
    message["user"] = "Hello";
    auto create_res = ConversationManager::get_instance().add_conversation(conversation, model);

    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();

    message = "invalid";

    auto append_res = ConversationManager::get_instance().add_conversation(message, model, conversation_id);
    ASSERT_FALSE(append_res.ok());
    ASSERT_EQ(append_res.code(), 400);
    ASSERT_EQ(append_res.error(), "Conversation is not an array");
}

TEST_F(ConversationTest, DeleteConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    nlohmann::json message = nlohmann::json::object();
    message["user"] = "Hello";
    conversation.push_back(message);
    auto create_res = ConversationManager::get_instance().add_conversation(conversation, model);
    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();
    LOG(INFO) << conversation_id;

    auto delete_res = ConversationManager::get_instance().delete_conversation(conversation_id, model["id"]);
    LOG(INFO) << delete_res.error();
    ASSERT_TRUE(delete_res.ok());

    auto delete_res_json = delete_res.get();

    ASSERT_EQ(delete_res_json["id"], conversation_id);

    auto get_res = ConversationManager::get_instance().get_conversation(conversation_id, model);
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), 404);
    ASSERT_EQ(get_res.error(), "Conversation not found");
}

TEST_F(ConversationTest, DeleteInvalidConversation) {
    auto delete_res = ConversationManager::get_instance().delete_conversation("qwerty", model["id"]);
    ASSERT_FALSE(delete_res.ok());
    ASSERT_EQ(delete_res.code(), 404);
    ASSERT_EQ(delete_res.error(), "Conversation not found");
}

TEST_F(ConversationTest, TruncateConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    nlohmann::json message = nlohmann::json::object();
    message["user"] = "Hello";
    for (int i = 0; i < 1000; i++) {
        conversation.push_back(message);
    }

    auto truncated = ConversationManager::get_instance().truncate_conversation(conversation, 100);
    ASSERT_TRUE(truncated.ok());
    ASSERT_TRUE(truncated.get().size() < conversation.size());
    ASSERT_TRUE(truncated.get().dump(0).size() < 100);
}

TEST_F(ConversationTest, TruncateConversationEmpty) {
    nlohmann::json conversation = nlohmann::json::array();
    auto truncated = ConversationManager::get_instance().truncate_conversation(conversation, 100);
    ASSERT_TRUE(truncated.ok());
    ASSERT_TRUE(truncated.get().size() == 0);
}

TEST_F(ConversationTest, TruncateConversationInvalidType) {
    nlohmann::json conversation = nlohmann::json::object();
    auto truncated = ConversationManager::get_instance().truncate_conversation(conversation, 100);
    ASSERT_FALSE(truncated.ok());
    ASSERT_EQ(truncated.code(), 400);
    ASSERT_EQ(truncated.error(), "Conversation history is not an array");
}

TEST_F(ConversationTest, TruncateConversationInvalidLimit) {
    nlohmann::json conversation = nlohmann::json::array();
    auto truncated = ConversationManager::get_instance().truncate_conversation(conversation, 0);
    ASSERT_FALSE(truncated.ok());
    ASSERT_EQ(truncated.code(), 400);
    ASSERT_EQ(truncated.error(), "Limit must be positive integer");
}

TEST_F(ConversationTest, TestConversationExpire) {
    nlohmann::json conversation = nlohmann::json::array();
    nlohmann::json message = nlohmann::json::object();
    message["user"] = "Hello";
    conversation.push_back(message);
    auto create_res = ConversationManager::get_instance().add_conversation(conversation, model);

    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();
    
    ConversationManager::get_instance().clear_expired_conversations();

    auto get_res = ConversationManager::get_instance().get_conversation(conversation_id, model);
    ASSERT_TRUE(get_res.ok());
    ASSERT_TRUE(get_res.get()["conversation"].is_array());
    ASSERT_EQ(get_res.get()["id"], conversation_id);
    ASSERT_EQ(get_res.get()["conversation"].size(), 1);

    ConversationManager::get_instance()._set_ttl_offset(24 * 60 * 60 * 2);
    LOG(INFO) << "Clearing expired conversations";
    ConversationManager::get_instance().clear_expired_conversations();
    LOG(INFO) << "Cleared expired conversations";

    get_res = ConversationManager::get_instance().get_conversation(conversation_id, model);
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), 404);
    ASSERT_EQ(get_res.error(), "Conversation not found");

    ConversationManager::get_instance()._set_ttl_offset(0);
}


TEST_F(ConversationTest, TestInvalidConversationCollection) {
    nlohmann::json schema_json = R"({
        "name": "conversation_store2",
        "fields": [
            {
                "name": "lorem",
                "type": "string"
            }
        ]
    })"_json;

    auto coll = collectionManager.create_collection(schema_json).get();
    auto res = ConversationManager::get_instance().validate_conversation_store_schema(coll);
    ASSERT_FALSE(res.ok());
    ASSERT_EQ(res.code(), 400);
    ASSERT_EQ(res.error(), "Schema is missing `conversation_id` field");
}

TEST_F(ConversationTest, TestGettingFullConversation) {
    nlohmann::json dummy_model = nlohmann::json::object();
    dummy_model["model_name"] = "openai/gpt-4-turbo";
    dummy_model["history_collection"] = "conversation_store";
    dummy_model["id"] = "1";

    std::string question = "What is the capital of France?";
    std::string answer = "The capital of France is Paris.";

    auto conversation_history_op = ConversationManager::get_instance().get_full_conversation(question, answer, dummy_model, "");
    ASSERT_TRUE(conversation_history_op.ok());

    auto conversation_history = conversation_history_op.get();
    ASSERT_TRUE(conversation_history["conversation"].is_array());
    ASSERT_EQ(conversation_history["conversation"].size(), 2);
    ASSERT_EQ(conversation_history["conversation"][0]["user"], question);
    ASSERT_EQ(conversation_history["conversation"][1]["assistant"], answer);
    ASSERT_TRUE(conversation_history["last_updated"].is_number());

    auto dummy_history = nlohmann::json::array();
    dummy_history.push_back(conversation_history["conversation"][0]);
    dummy_history.push_back(conversation_history["conversation"][1]);

    auto add_conversation_op = ConversationManager::get_instance().add_conversation(dummy_history, model);
    ASSERT_TRUE(add_conversation_op.ok());
    std::string conversation_id = add_conversation_op.get();

    question = "What is the capital of Germany?";
    answer = "The capital of Germany is Berlin.";

    conversation_history_op = ConversationManager::get_instance().get_full_conversation(question, answer, dummy_model, conversation_id);
    ASSERT_TRUE(conversation_history_op.ok());

    conversation_history = conversation_history_op.get();
    ASSERT_TRUE(conversation_history["conversation"].is_array());
    ASSERT_EQ(conversation_history["conversation"].size(), 4);
    ASSERT_EQ(conversation_history["conversation"][0]["user"], "What is the capital of France?");
    ASSERT_EQ(conversation_history["conversation"][1]["assistant"], "The capital of France is Paris.");
    ASSERT_EQ(conversation_history["conversation"][2]["user"], "What is the capital of Germany?");
    ASSERT_EQ(conversation_history["conversation"][3]["assistant"], "The capital of Germany is Berlin.");
}

TEST_F(ConversationTest, TestGeminiStreamManipulation) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");
    // test JSON to SSE
    std::string test = R"([
    {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": "Hello"
                        }
                    ],
                    "role": "model"
                }
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 1,
            "totalTokenCount": 1,
            "promptTokensDetails": [
                {
                    "modality": "TEXT",
                    "tokenCount": 1
                }
            ]
        },
        "modelVersion": "gemini-2.0-flash"
    })";

    std::string expected = "data: {\"conversation_id\":\"test\",\"message\":\"Hello\"}\n\n";
    GeminiConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);

    test = R"(,{
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": "! How can"
                        }
                    ],
                    "role": "model"
                }
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 1,
            "totalTokenCount": 1,
            "promptTokensDetails": [
                {
                    "modality": "TEXT",
                    "tokenCount": 1
                }
            ]
        },
        "modelVersion": "gemini-2.0-flash"
    })";

    expected = "data: {\"conversation_id\":\"test\",\"message\":\"! How can\"}\n\n";
    GeminiConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);

    test = R"(,
        {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": " I help you today?\n"
                            }
                        ],
                        "role": "model"
                    },
                    "finishReason": "STOP"
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 1,
                "candidatesTokenCount": 10,
                "totalTokenCount": 11,
                "promptTokensDetails": [
                    {
                        "modality": "TEXT",
                        "tokenCount": 1
                    }
                ],
                "candidatesTokensDetails": [
                    {
                        "modality": "TEXT",
                        "tokenCount": 10
                    }
                ]
            },
            "modelVersion": "gemini-2.0-flash"
        }
    ])";

    expected = "data: {\"conversation_id\":\"test\",\"message\":\" I help you today?\\n\"}\n\n";
    expected += "data: [DONE]\n\n";
    GeminiConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}

TEST_F(ConversationTest, TestAzureStreamManipulation) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");

    // Test initial prompt filter results
    std::string test = "{\"choices\":[],\"created\":0,\"id\":\"\",\"model\":\"\",\"object\":\"\",\"prompt_filter_results\":[{\"prompt_index\":0,\"content_filter_results\":{\"hate\":{\"filtered\":false,\"severity\":\"safe\"},\"jailbreak\":{\"filtered\":false,\"detected\":false},\"self_harm\":{\"filtered\":false,\"severity\":\"safe\"},\"sexual\":{\"filtered\":false,\"severity\":\"safe\"},\"violence\":{\"filtered\":false,\"severity\":\"safe\"}}}]}";

    // This should be ignored as it has no content
    std::string expected = "";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}

TEST_F(ConversationTest, TestAzureStreamBasicContent) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");

    // Test basic content streaming
    std::string test = std::string(R"(data: {"choices":[{"delta":{"content":"Hello"},"finish_reason":null}]})") + "\n\n";
    std::string expected = std::string(R"(data: {"conversation_id":"test","message":"Hello"})") + "\n\n";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}

TEST_F(ConversationTest, TestAzureStreamEmptyMessages) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");

    // Test empty messages
    std::string test = std::string(R"(data: {"choices":[]})") + "\n\n";
    std::string expected = "";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);

    // Test empty JSON object
    test = std::string(R"(data: {})") + "\n\n";
    expected = "";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}

TEST_F(ConversationTest, TestAzureStreamRoleAssignment) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");

    // Test role assignment message
    std::string test = std::string(R"(data: {"choices":[{"delta":{"role":"assistant"},"finish_reason":null}]})") + "\n\n";
    std::string expected = "";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}

TEST_F(ConversationTest, TestAzureStreamFinishReason) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");

    // Test finish reason with content
    std::string test = std::string(R"(data: {"choices":[{"delta":{"content":"Goodbye"},"finish_reason":"stop"}]})") + "\n\n";
    std::string expected = std::string(R"(data: {"conversation_id":"test","message":"Goodbye"})") + "\n\n" + "data: [DONE]\n\n";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}

TEST_F(ConversationTest, TestAzureStreamMultipleChunks) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");

    // Test multiple content chunks
    std::string test = std::string(R"(data: {"choices":[{"delta":{"content":"Hello "},"finish_reason":null}]})") + "\n\n";
    std::string expected = std::string(R"(data: {"conversation_id":"test","message":"Hello "})") + "\n\n";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);

    test = std::string(R"(data: {"choices":[{"delta":{"content":"World"},"finish_reason":"stop"}]})") + "\n\n";
    expected = std::string(R"(data: {"conversation_id":"test","message":"World"})") + "\n\n" + "data: [DONE]\n\n";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}

TEST_F(ConversationTest, TestAzureStreamErrorHandling) {
    std::shared_ptr<http_req> req = std::make_shared<http_req>();
    std::shared_ptr<http_res> res = std::make_shared<http_res>(nullptr);
    ConversationModel::_add_async_conversation(req, "test");

    // Test invalid JSON
    std::string test = std::string(R"(data: {invalid json})") + "\n\n";
    std::string expected = "";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);

    // Test malformed content
    test = std::string(R"(data: {"choices":[{"delta":{},"finish_reason":null}]})") + "\n\n";
    expected = "";
    AzureConversationModel::_async_write_callback(test, req, res);
    ASSERT_EQ(test, expected);
}