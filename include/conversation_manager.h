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
        Option<std::string> add_conversation(const nlohmann::json& conversation, const nlohmann::json& model, const std::string& id = "", const bool check_if_exists = true);
        Option<nlohmann::json> get_conversation(const std::string& conversation_id, const nlohmann::json& model);
        Option<nlohmann::json> get_full_conversation(const std::string& question, const std::string& answer, const nlohmann::json& model, const std::string& conversation_id);
        static Option<nlohmann::json> get_last_n_messages(const nlohmann::json& conversation, size_t n);
        static Option<nlohmann::json> truncate_conversation(nlohmann::json conversation, size_t limit);
        Option<nlohmann::json> delete_conversation(const std::string& conversation_id, const std::string& model_id);
        Option<bool> check_conversation_exists(const std::string& conversation_id, Collection* collection);
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
        // get history collection by conversation model
        Option<Collection*> get_history_collection(const nlohmann::json& model);
    private:
        ConversationManager() {}
        std::mutex conversations_mutex;
        
        ReplicationState* raft_server;
        size_t TTL_OFFSET = 0;

        std::atomic<bool> quit = false;
        std::condition_variable cv;

        Option<nlohmann::json> delete_conversation_unsafe(const std::string& conversation_id, const std::string& model_id);
};