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
        return Option<nlohmann::json>(404, "Conversation not found");
    }

    return Option<nlohmann::json>(conversation->second);
}

Option<bool> ConversationManager::append_conversation(int conversation_id, const nlohmann::json& message) {
    auto conversation = conversations.find(conversation_id);
    if (conversation == conversations.end()) {
        return Option<bool>(404, "Conversation not found");
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


size_t ConversationManager::get_token_count(const nlohmann::json& message) {
    // from OpenAI API docs:
    // A helpful rule of thumb is that one token generally corresponds to ~4 characters of text for common English text. 
    // This translates to roughly ¾ of a word (so 100 tokens ~= 75 words).

    return message.dump(0).size() / 4;
}

Option<nlohmann::json> ConversationManager::truncate_conversation(nlohmann::json conversation) {
    while(get_token_count(conversation) > MAX_TOKENS) {
        // pop front element from json array
        try {
            conversation.erase(0);
        } catch (std::exception& e) {
            return Option<nlohmann::json>(400, "Conversation history is not an array");
        }
    }

    return Option<nlohmann::json>(conversation);
}

Option<nlohmann::json> ConversationManager::delete_conversation(int conversation_id) {
    auto conversation = conversations.find(conversation_id);
    if (conversation == conversations.end()) {
        return Option<nlohmann::json>(404, "Conversation not found");
    }

    Option<nlohmann::json> conversation_res(*conversation);

    conversations.erase(conversation);
    return conversation_res;
} 

Option<nlohmann::json> ConversationManager::get_all_conversations() {
    nlohmann::json all_conversations = nlohmann::json::array();
    for(auto& conversation : conversations) {
        nlohmann::json conversation_json;
        conversation_json["id"] = conversation.first;
        conversation_json["conversation"] = conversation.second;
        all_conversations.push_back(conversation_json);
    }

    return Option<nlohmann::json>(all_conversations);
}