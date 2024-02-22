#include <gtest/gtest.h>
#include "conversation_manager.h"


class ConversationTest : public ::testing::Test {
    protected:
        void SetUp() override {
            std::string state_dir_path = "/tmp/typesense_test/conversation_test";
            system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
            store = new Store(state_dir_path);
            ConversationManager::get_instance().init(store);
        }

        void TearDown() override {
            auto conversations = ConversationManager::get_instance().get_all_conversations();
            for (auto& conversation : conversations.get()) {
                ConversationManager::get_instance().delete_conversation(conversation["id"]);
            }
            delete store;
        }

        Store* store;
};


TEST_F(ConversationTest, CreateConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    auto create_res = ConversationManager::get_instance().create_conversation(conversation);
    ASSERT_TRUE(create_res.ok());
}

TEST_F(ConversationTest, CreateConversationInvalidType) {
    nlohmann::json conversation = nlohmann::json::object();
    auto create_res = ConversationManager::get_instance().create_conversation(conversation);
    ASSERT_FALSE(create_res.ok());
    ASSERT_EQ(create_res.code(), 400);
    ASSERT_EQ(create_res.error(), "Conversation is not an array");
}

TEST_F(ConversationTest, GetInvalidConversation) {
    auto get_res = ConversationManager::get_instance().get_conversation("qwerty");
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), 404);
    ASSERT_EQ(get_res.error(), "Conversation not found");
}

TEST_F(ConversationTest, AppendConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    nlohmann::json message = nlohmann::json::object();
    message["user"] = "Hello";
    conversation.push_back(message);
    auto create_res = ConversationManager::get_instance().create_conversation(conversation);

    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();

    auto append_res = ConversationManager::get_instance().append_conversation(conversation_id, message);
    ASSERT_TRUE(append_res.ok());
    ASSERT_EQ(append_res.get(), true);
    
    auto get_res = ConversationManager::get_instance().get_conversation(conversation_id);

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
    auto create_res = ConversationManager::get_instance().create_conversation(conversation);

    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();

    message = "invalid";

    auto append_res = ConversationManager::get_instance().append_conversation(conversation_id, message);
    ASSERT_FALSE(append_res.ok());
    ASSERT_EQ(append_res.code(), 400);
    ASSERT_EQ(append_res.error(), "Message is not an object or array");
}

TEST_F(ConversationTest, DeleteConversation) {
    nlohmann::json conversation = nlohmann::json::array();
    auto create_res = ConversationManager::get_instance().create_conversation(conversation);
    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();

    auto delete_res = ConversationManager::get_instance().delete_conversation(conversation_id);
    ASSERT_TRUE(delete_res.ok());

    auto delete_res_json = delete_res.get();

    ASSERT_EQ(delete_res_json["id"], conversation_id);
    ASSERT_TRUE(delete_res_json["conversation"].is_array());

    auto get_res = ConversationManager::get_instance().get_conversation(conversation_id);
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), 404);
    ASSERT_EQ(get_res.error(), "Conversation not found");
}

TEST_F(ConversationTest, DeleteInvalidConversation) {
    auto delete_res = ConversationManager::get_instance().delete_conversation("qwerty");
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
    auto create_res = ConversationManager::get_instance().create_conversation(conversation);

    ASSERT_TRUE(create_res.ok());
    std::string conversation_id = create_res.get();
    
    ConversationManager::get_instance().clear_expired_conversations();

    auto get_res = ConversationManager::get_instance().get_conversation(conversation_id);
    ASSERT_TRUE(get_res.ok());
    ASSERT_TRUE(get_res.get()["conversation"].is_array());
    ASSERT_EQ(get_res.get()["id"], conversation_id);
    ASSERT_EQ(get_res.get()["conversation"].size(), 1);

    ConversationManager::get_instance()._set_ttl_offset(24 * 60 * 60 * 2);
    ConversationManager::get_instance().clear_expired_conversations();

    get_res = ConversationManager::get_instance().get_conversation(conversation_id);
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), 404);
    ASSERT_EQ(get_res.error(), "Conversation not found");

    ConversationManager::get_instance()._set_ttl_offset(0);
}


