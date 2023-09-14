#include "conversation_manager.h"


Option<int> ConversationManager::create_conversation(const nlohmann::json& conversation) {
    std::unique_lock lock(conversations_mutex);
    if(!conversation.is_array()) {
        return Option<int>(400, "Conversation is not an array");
    }

    auto conversation_key = get_conversation_key(conversation_id);
    nlohmann::json conversation_store_json;
    conversation_store_json["id"] = conversation_id;
    conversation_store_json["conversation"] = conversation;
    bool insert_op = store->insert(conversation_key, conversation_store_json.dump(0));
    if(!insert_op) {
        return Option<int>(500, "Error while inserting conversation into the store");
    }
    store->increment(std::string(CONVERSATION_NEXT_ID), 1);

    conversations[conversation_id] = conversation;

    return Option<int>(conversation_id++);
}

Option<nlohmann::json> ConversationManager::get_conversation(int conversation_id) {
    std::shared_lock lock(conversations_mutex);
    auto conversation = conversations.find(conversation_id);
    if (conversation == conversations.end()) {
        return Option<nlohmann::json>(404, "Conversation not found");
    }

    return Option<nlohmann::json>(conversation->second);
}

Option<bool> ConversationManager::append_conversation(int conversation_id, const nlohmann::json& message) {
    std::unique_lock lock(conversations_mutex);
    auto conversation_it = conversations.find(conversation_id);
    if (conversation_it == conversations.end()) {
        return Option<bool>(404, "Conversation not found");
    }

    if(!conversation_it->second.is_array()) {
        return Option<bool>(400, "Conversation is not an array");
    }

    nlohmann::json conversation = nlohmann::json::object();
    conversation["id"] = conversation_id;
    conversation["conversation"] = conversation_it->second;

    if(!message.is_array()) {
        conversation["conversation"].push_back(message);
    } else {
        for(auto& m : message) {
            conversation["conversation"].push_back(m);
        }
    }

    auto conversation_key = get_conversation_key(conversation_id);
    bool insert_op = store->insert(conversation_key, conversation.dump(0));
    if(!insert_op) {
        return Option<bool>(500, "Error while inserting conversation into the store");
    }

    conversations[conversation_id] = conversation["conversation"];

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
    std::unique_lock lock(conversations_mutex);
    auto conversation = conversations.find(conversation_id);
    if (conversation == conversations.end()) {
        return Option<nlohmann::json>(404, "Conversation not found");
    }

    Option<nlohmann::json> conversation_res(*conversation);

    auto conversation_key = get_conversation_key(conversation_id);
    bool delete_op = store->remove(conversation_key);
    if(!delete_op) {
        return Option<nlohmann::json>(500, "Error while deleting conversation from the store");
    }

    conversations.erase(conversation);
    return conversation_res;
} 

Option<nlohmann::json> ConversationManager::get_all_conversations() {
    std::shared_lock lock(conversations_mutex);
    nlohmann::json all_conversations = nlohmann::json::array();
    for(auto& conversation : conversations) {
        nlohmann::json conversation_json;
        conversation_json["id"] = conversation.first;
        conversation_json["conversation"] = conversation.second;
        all_conversations.push_back(conversation_json);
    }

    return Option<nlohmann::json>(all_conversations);
}

Option<int> ConversationManager::init(Store* store) {
    std::unique_lock lock(conversations_mutex);
    ConversationManager::store = store;

    std::string last_id_str;
    StoreStatus last_id_str_status = store->get(std::string(CONVERSATION_NEXT_ID), last_id_str);

    if(last_id_str_status == StoreStatus::ERROR) {
        return Option<int>(500, "Error while loading conversations next id from the store");
    } else if(last_id_str_status == StoreStatus::FOUND) {
        conversation_id = StringUtils::deserialize_uint32_t(last_id_str);
    } else {
        conversation_id = 0;
    }

    std::vector<std::string> conversation_strs;
    store->scan_fill(std::string(CONVERSATION_RPEFIX) + "_", std::string(CONVERSATION_RPEFIX) + "`", conversation_strs);
    
    size_t loaded_conversations = 0;

    for(auto& conversation_str : conversation_strs) {
        nlohmann::json conversation_json = nlohmann::json::parse(conversation_str);
        int conversation_id = conversation_json["id"];
        conversations[conversation_id] = conversation_json["conversation"];
        loaded_conversations++;
    }

    return Option<int>(loaded_conversations);
}   

const std::string ConversationManager::get_conversation_key(int conversation_id) {
    return std::string(CONVERSATION_RPEFIX) + "_" + StringUtils::serialize_uint32_t(conversation_id);
}