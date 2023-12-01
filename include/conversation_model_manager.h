#pragma once

#include <shared_mutex>
#include <mutex>
#include <unordered_map>
#include <json.hpp>
#include <option.h>
#include "store.h"


class ConversationModelManager
{
    public:
        ConversationModelManager() = delete;
        ConversationModelManager(const ConversationModelManager&) = delete;
        ConversationModelManager(ConversationModelManager&&) = delete;
        ConversationModelManager& operator=(const ConversationModelManager&) = delete;

        static Option<nlohmann::json> get_model(const uint32_t model_id);
        static Option<nlohmann::json> add_model(nlohmann::json model);
        static Option<nlohmann::json> delete_model(const uint32_t model_id);
        static Option<nlohmann::json> update_model(const uint32_t model_id, nlohmann::json model);
        static Option<nlohmann::json> get_all_models();

        
        static Option<int> init(Store* store);
    private:
        static inline std::unordered_map<uint32_t, nlohmann::json> models;
        static inline uint32_t model_id = 0;
        static inline std::shared_mutex models_mutex;

        static constexpr char* MODEL_NEXT_ID = "$CVMN";
        static constexpr char* MODEL_KEY_PREFIX = "$CVMP";

        static inline Store* store;
        static const std::string get_model_key(uint32_t model_id);
};