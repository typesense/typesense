#include "conversation_manager.h"
#include "logger.h"
#include <chrono>

Option<std::string> ConversationManager::create_conversation(const nlohmann::json& conversation) {
    std::unique_lock lock(conversations_mutex);
    if(!conversation.is_array()) {
        return Option<std::string>(400, "Conversation is not an array");
    }

    std::string conversation_id = sole::uuid4().str();
    auto conversation_key = get_conversation_key(conversation_id);
    nlohmann::json conversation_store_json;
    conversation_store_json["id"] = conversation_id;
    conversation_store_json["conversation"] = conversation;
    conversation_store_json["last_updated"] = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    conversation_store_json["ttl"] = CONVERSATION_TTL;
    bool insert_op = store->insert(conversation_key, conversation_store_json.dump(0));
    if(!insert_op) {
        return Option<std::string>(500, "Error while inserting conversation into the store");
    }

    conversations[conversation_id] = conversation_store_json;

    return Option<std::string>(conversation_id);
}

Option<nlohmann::json> ConversationManager::get_conversation(const std::string& conversation_id) {
    std::unique_lock lock(conversations_mutex);
    auto conversation = conversations.find(conversation_id);
    if (conversation == conversations.end()) {
        return Option<nlohmann::json>(404, "Conversation not found");
    }

    return Option<nlohmann::json>(conversation->second);
}

Option<bool> ConversationManager::append_conversation(const std::string& conversation_id, const nlohmann::json& message) {
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


// pop front elements until the conversation is less than MAX_TOKENS
Option<nlohmann::json> ConversationManager::truncate_conversation(nlohmann::json conversation, size_t limit) {
    if(!conversation.is_array()) {
        return Option<nlohmann::json>(400, "Conversation history is not an array");
    }
    if(limit <= 0) {
        return Option<nlohmann::json>(400, "Limit must be positive integer");
    }
    while(conversation.dump(0).size() > limit) {
        // pop front element from json array
        try {
            conversation.erase(0);
        } catch (std::exception& e) {
            return Option<nlohmann::json>(400, "Conversation history is not an array");
        }
    }

    return Option<nlohmann::json>(conversation);
}

Option<nlohmann::json> ConversationManager::delete_conversation(const std::string& conversation_id) {
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
    std::unique_lock lock(conversations_mutex);
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

const std::string ConversationManager::get_conversation_key(const std::string& conversation_id) {
    return std::string(CONVERSATION_RPEFIX) + "_" + conversation_id;
}

void ConversationManager::clear_expired_conversations() {
    std::unique_lock lock(conversations_mutex);

    int cleared_conversations = 0;

    for(auto it = conversations.begin(); it != conversations.end();) {
        const std::string& conversation_id = it->first;
        nlohmann::json conversation_json = it->second;
        if(conversation_json.count("last_updated") == 0 || conversation_json.count("ttl") == 0) {
            bool delete_op = store->remove(get_conversation_key(conversation_id));
            if(!delete_op) {
                LOG(ERROR) << "Error while deleting conversation from the store";
            }
            it = conversations.erase(it);
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
            it = conversations.erase(it);
            cleared_conversations++;
        } else {
            it++;
        }
    }

    LOG(INFO) << "Cleared " << cleared_conversations << " expired conversations";
}

Option<nlohmann::json> ConversationManager::update_conversation(nlohmann::json conversation) {
    std::unique_lock lock(conversations_mutex);
    if(!conversation.is_object()) {
        return Option<nlohmann::json>(400, "Conversation is not an object");
    }

    if(conversation.count("id") == 0) {
        return Option<nlohmann::json>(400, "Conversation is missing id");
    }

    const std::string& conversation_id = conversation["id"];

    auto conversation_it = conversations.find(conversation_id);
    if (conversation_it == conversations.end()) {
        return Option<nlohmann::json>(404, "Conversation not found");
    }

    auto conversation_key = get_conversation_key(conversation_id);

    auto actual_conversation = conversation_it->second;

    if(conversation.count("ttl") == 0) {
        return Option<nlohmann::json>(400, "Only `ttl` can be updated");
    }
    
    actual_conversation["ttl"] = conversation["ttl"];

    bool insert_op = store->insert(conversation_key, actual_conversation.dump(0));
    if(!insert_op) {
        return Option<nlohmann::json>(500, "Error while inserting conversation into the store");
    }

    conversations[conversation_id] = actual_conversation;

    return Option<nlohmann::json>(actual_conversation);
}

void ConversationManager::run() {
    while(!quit) {
        std::unique_lock lock(conversations_mutex);
        cv.wait_for(lock, std::chrono::seconds(60), [&] { return quit.load(); });

        if(quit) {
            return;
        }

        lock.unlock();
        clear_expired_conversations();
    }
}

void ConversationManager::stop() {
    quit = true;
    cv.notify_all();
}