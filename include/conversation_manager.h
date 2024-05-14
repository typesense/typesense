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
        Option<std::string> add_conversation(const nlohmann::json& conversation, const std::string& conversation_collection, const std::string& id = "");
        Option<nlohmann::json> get_conversation(const std::string& conversation_id);
        static Option<nlohmann::json> truncate_conversation(nlohmann::json conversation, size_t limit);
        Option<nlohmann::json> update_conversation(nlohmann::json conversation);
        Option<nlohmann::json> delete_conversation(const std::string& conversation_id);
        Option<bool> check_conversation_exists(const std::string& conversation_id);
        Option<std::unordered_set<std::string>> get_conversation_ids();
        Option<nlohmann::json> get_all_conversations();
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
        Option<bool> add_conversation_collection(const std::string& collection);
        Option<bool> remove_conversation_collection(const std::string& collection);
        Option<Collection*> get_conversation_collection(const std::string& conversation_id);
    private:
        ConversationManager() {}
        std::mutex conversations_mutex;
        
        ReplicationState* raft_server;
        static constexpr size_t CONVERSATION_TTL = 60 * 60 * 24;
        size_t TTL_OFFSET = 0;
        size_t MAX_CONVERSATIONS_TO_DELETE_ONCE = 5;

        std::atomic<bool> quit = false;
        std::condition_variable cv;
        std::unordered_map<std::string, uint32_t> conversation_collection_map;
        std::unordered_map<std::string, std::string> conversation_mapper;
};