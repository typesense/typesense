#include "conversation_manager.h"


Option<int> ConversationManager::create_conversation(const nlohmann::json& conversation) {
    if(!conversation.is_array()) {
        return Option<int>(400, "Conversation is not an array");
    }

    conversations[conversation_id] = conversation;
    return Option<int>(conversation_id++);
}

Option<nlohmann::json> ConversationManager::get_conversation(int conversation_id) {
    auto conversation = conversations.find(conversation_id);
    if (conversation == conversations.end()) {
        return Option<nlohmann::json>(400, "Conversation not found");
    }

    return Option<nlohmann::json>(conversation->second);
}

Option<bool> ConversationManager::append_conversation(int conversation_id, const nlohmann::json& message) {
    auto conversation = conversations.find(conversation_id);
    if (conversation == conversations.end()) {
        return Option<bool>(400, "Conversation not found");
    }

    if(!conversation->second.is_array()) {
        return Option<bool>(400, "Conversation is not an array");
    }

    if(!message.is_array()) {
        conversation->second.push_back(message);
    } else {
        for(auto& m : message) {
            conversation->second.push_back(m);
        }
    }

    return Option<bool>(true);
}
