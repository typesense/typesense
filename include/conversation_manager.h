#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <mutex>
#include <json.hpp>
#include "option.h"
#include "store.h"
#include "sole.hpp"
#include "collection_manager.h"
#include "conversation_model_manager.h"

class ConversationManager {
    public:
        ConversationManager(const ConversationManager&) = delete;
        ConversationManager(ConversationManager&&) = delete;
        ConversationManager& operator=(const ConversationManager&) = delete;
        ConversationManager& operator=(ConversationManager&&) = delete;
        static ConversationManager& get_instance() {
            static ConversationManager instance;
            return instance;
        }
        Option<std::string> add_conversation(const nlohmann::json& conversation, const nlohmann::json& model, const std::string& id = "");
        Option<nlohmann::json> get_conversation(const std::string& conversation_id);
        static Option<nlohmann::json> truncate_conversation(nlohmann::json conversation, size_t limit);
        Option<nlohmann::json> delete_conversation(const std::string& conversation_id);
        Option<bool> check_conversation_exists(const std::string& conversation_id);
        Option<std::unordered_set<std::string>> get_conversation_ids();
        static constexpr size_t MAX_TOKENS = 3000;
        Option<bool> init(ReplicationState* raft_server);
        void clear_expired_conversations();
        void run();
        void stop();
        void _set_ttl_offset(size_t offset) {
            TTL_OFFSET = offset;
        }

        Option<bool> validate_conversation_store_schema(Collection* collection);
        Option<bool> validate_conversation_store_collection(const std::string& collection);
        Option<Collection*> get_history_collection(const std::string& conversation_id);
    private:
        ConversationManager() {}
        std::mutex conversations_mutex;
        
        ReplicationState* raft_server;
        size_t TTL_OFFSET = 0;

        std::atomic<bool> quit = false;
        std::condition_variable cv;

        Option<nlohmann::json> delete_conversation_unsafe(const std::string& conversation_id);
};