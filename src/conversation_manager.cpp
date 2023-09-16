#include "conversation_manager.h"
#include "logger.h"
#include <chrono>


Option<int> ConversationManager::create_conversation(const nlohmann::json& conversation) {
    std::unique_lock lock(conversations_mutex);
    if(!conversation.is_array()) {
        return Option<int>(400, "Conversation is not an array");
    }

    auto conversation_key = get_conversation_key(conversation_id);
    nlohmann::json conversation_store_json;
    conversation_store_json["id"] = conversation_id;
    conversation_store_json["conversation"] = conversation;
    conversation_store_json["last_updated"] = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    conversation_store_json["ttl"] = CONVERSATION_TTL;
    bool insert_op = store->insert(conversation_key, conversation_store_json.dump(0));
    if(!insert_op) {
        return Option<int>(500, "Error while inserting conversation into the store");
    }
    store->increment(std::string(CONVERSATION_NEXT_ID), 1);

    conversations[conversation_id] = conversation_store_json;

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

    if(!message.is_object() && !message.is_array()) {
        return Option<bool>(400, "Message is not an object or array");
    }

    nlohmann::json conversation = conversation_it->second;

    if(!message.is_array()) {
        conversation["conversation"].push_back(message);
    } else {
        for(auto& m : message) {
            conversation["conversation"].push_back(m);
        }
    }

    conversation["last_updated"] = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    auto conversation_key = get_conversation_key(conversation_id);
    bool insert_op = store->insert(conversation_key, conversation.dump(0));
    if(!insert_op) {
        return Option<bool>(500, "Error while inserting conversation into the store");
    }

    conversations[conversation_id] = conversation;

    return Option<bool>(true);
}


size_t ConversationManager::get_token_count(const nlohmann::json& message) {
    // from OpenAI API docs:
    // A helpful rule of thumb is that one token generally corresponds to ~4 characters of text for common English text. 
    // This translates to roughly ¾ of a word (so 100 tokens ~= 75 words).

    return message.dump(0).size() / 4;
}

// pop front elements until the conversation is less than MAX_TOKENS
Option<nlohmann::json> ConversationManager::truncate_conversation(nlohmann::json conversation) {
    if(!conversation.is_array()) {
        return Option<nlohmann::json>(400, "Conversation history is not an array");
    }
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



    Option<nlohmann::json> conversation_res(conversation->second);

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
        all_conversations.push_back(conversation.second);
    }

    return Option<nlohmann::json>(all_conversations);
}

Option<int> ConversationManager::init(Store* store) {

    if(store == nullptr) {
        return Option<int>(500, "Store is null");
    }

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
        if(conversation_json.count("id") == 0) {
            LOG(INFO) << "key: " << conversation_str << " is missing id";
            continue;
        }
        conversations[conversation_json["id"]] = conversation_json;
        loaded_conversations++;
    }

    return Option<int>(loaded_conversations);
}   

const std::string ConversationManager::get_conversation_key(int conversation_id) {
    return std::string(CONVERSATION_RPEFIX) + "_" + std::to_string(conversation_id);
}

void ConversationManager::clear_expired_conversations() {
    std::unique_lock lock(conversations_mutex);

    int cleared_conversations = 0;

    for(auto& conversation : conversations) {
        int conversation_id = conversation.first;
        nlohmann::json conversation_json = conversation.second;
        if(conversation_json.count("last_updated") == 0 || conversation_json.count("ttl") == 0) {
            bool delete_op = store->remove(get_conversation_key(conversation_id));
            if(!delete_op) {
                LOG(ERROR) << "Error while deleting conversation from the store";
            }
            conversations.erase(conversation_id);
            cleared_conversations++;
            continue;   
        }
        int last_updated = conversation_json["last_updated"];
        int ttl = conversation_json["ttl"];
        if(last_updated + ttl < TTL_OFFSET + std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count()) {
            auto delete_op = store->remove(get_conversation_key(conversation_id));
            if(!delete_op) {
                LOG(ERROR) << "Error while deleting conversation from the store";
            }
            conversations.erase(conversation_id);
            cleared_conversations++;
        }
    }

    LOG(INFO) << "Cleared " << cleared_conversations << " expired conversations";
}