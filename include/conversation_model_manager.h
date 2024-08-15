#pragma once

#include <shared_mutex>
#include <mutex>
#include <unordered_map>
#include <json.hpp>
#include <option.h>
#include "store.h"
#include "sole.hpp"
#include "collection.h"

class ConversationModelManager
{
    public:
        ConversationModelManager() = delete;
        ConversationModelManager(const ConversationModelManager&) = delete;
        ConversationModelManager(ConversationModelManager&&) = delete;
        ConversationModelManager& operator=(const ConversationModelManager&) = delete;

        static Option<nlohmann::json> get_model(const std::string& model_id);
        static Option<bool> add_model(nlohmann::json& model, const std::string& model_id,
                                      const bool write_to_disk);
        static Option<nlohmann::json> delete_model(const std::string& model_id);
        static Option<nlohmann::json> update_model(const std::string& model_id, nlohmann::json model);
        static Option<nlohmann::json> get_all_models();
        static Option<int> init(Store* store);
        static bool migrate_model(nlohmann::json& model);
        static std::unordered_set<std::string> get_history_collections(); 
        // For testing Purpose only
        static void insert_model_for_testing(const std::string& model_id, nlohmann::json model) {
            std::unique_lock lock(models_mutex);
            models[model_id] = model;
        }
    private:
        static inline std::unordered_map<std::string, nlohmann::json> models;
        static inline std::shared_mutex models_mutex;

        static constexpr char* MODEL_NEXT_ID = "$CVMN";
        static constexpr char* MODEL_KEY_PREFIX = "$CVMP";
        static inline int64_t DEFAULT_HISTORY_COLLECTION_SUFFIX = 0;
        static inline Store* store;
        static const std::string get_model_key(const std::string& model_id);
        static Option<Collection*> create_default_history_collection(const std::string& model_id);
        static Option<nlohmann::json> delete_model_unsafe(const std::string& model_id);

};