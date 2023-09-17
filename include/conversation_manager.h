#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <json.hpp>
#include "option.h"
#include "store.h"


class ConversationManager {
    public:
        ConversationManager() = delete;
        static Option<int> create_conversation(const nlohmann::json& conversation);
        static Option<nlohmann::json> get_conversation(int conversation_id);
        static Option<bool> append_conversation(int conversation_id, const nlohmann::json& message);
        static Option<nlohmann::json> truncate_conversation(nlohmann::json conversation);
        static Option<nlohmann::json> update_conversation(nlohmann::json conversation);
        static size_t get_token_count(const nlohmann::json& message);
        static Option<nlohmann::json> delete_conversation(int conversation_id);
        static Option<nlohmann::json> get_all_conversations();
        static constexpr size_t MAX_TOKENS = 3000;
        static Option<int> init(Store* store);
        static void clear_expired_conversations();
        static void _set_ttl_offset(size_t offset) {
            TTL_OFFSET = offset;
        }
    private:
        static inline std::unordered_map<int, nlohmann::json> conversations;
        static inline int conversation_id = 0;
        static inline std::shared_mutex conversations_mutex;

        static constexpr char* CONVERSATION_NEXT_ID = "$CNVN";
        static constexpr char* CONVERSATION_RPEFIX = "$CNVP";
        
        static inline Store* store;

        static const std::string get_conversation_key(int conversation_id);

        static constexpr size_t CONVERSATION_TTL = 60 * 60 * 24;
        static inline size_t TTL_OFFSET = 0;
};