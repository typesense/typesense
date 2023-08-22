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
    private:
        static inline std::unordered_map<int, nlohmann::json> conversations;
        static inline int conversation_id = 0;
};