#pragma once

#include <string>
#include <unordered_map>
#include <json.hpp>
#include "option.h"


class ConversationManager {
    public:
        ConversationManager() = delete;
        static Option<int> create_conversation(const nlohmann::json& conversation);
        static Option<nlohmann::json> get_conversation(int conversation_id);
        static Option<bool> append_conversation(int conversation_id, const nlohmann::json& message);
        static Option<nlohmann::json> truncate_conversation(nlohmann::json conversation);
        static size_t get_token_count(const nlohmann::json& message);
        static Option<nlohmann::json> delete_conversation(int conversation_id);
        static Option<nlohmann::json> get_all_conversations();
        static constexpr size_t MAX_TOKENS = 3000;
    private:
        static inline std::unordered_map<int, nlohmann::json> conversations;
        static inline int conversation_id = 0;
};