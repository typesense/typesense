#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <json.hpp>
#include "option.h"
#include "store.h"
#include "sole.hpp"


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
        Option<std::string> create_conversation(const nlohmann::json& conversation);
        Option<nlohmann::json> get_conversation(const std::string& conversation_id);
        Option<bool> append_conversation(const std::string& conversation_id, const nlohmann::json& message);
        Option<nlohmann::json> truncate_conversation(nlohmann::json conversation, size_t limit);
        Option<nlohmann::json> update_conversation(nlohmann::json conversation);
        Option<nlohmann::json> delete_conversation(const std::string& conversation_id);
        Option<nlohmann::json> get_all_conversations();
        static constexpr size_t MAX_TOKENS = 3000;
        Option<int> init(Store* store);
        void clear_expired_conversations();
        void run();
        void stop();
        void _set_ttl_offset(size_t offset) {
            TTL_OFFSET = offset;
        }
    private:
        ConversationManager() {}
        std::unordered_map<std::string, nlohmann::json> conversations;
        std::mutex conversations_mutex;

        static constexpr char* CONVERSATION_RPEFIX = "$CNVP";
        
        Store* store;

        static const std::string get_conversation_key(const std::string& conversation_id);

        static constexpr size_t CONVERSATION_TTL = 60 * 60 * 24;
        size_t TTL_OFFSET = 0;

        std::atomic<bool> quit = false;
        std::condition_variable cv;
};