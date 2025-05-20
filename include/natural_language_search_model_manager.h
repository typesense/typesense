#pragma once

#include <string>
#include <shared_mutex>
#include <mutex>
#include <unordered_map>
#include <json.hpp>
#include <option.h>
#include "store.h"
#include "sole.hpp"
#include "lru/lru.hpp"
#include <chrono>

class NaturalLanguageSearchModelManager {
public:
    NaturalLanguageSearchModelManager() = delete;
    NaturalLanguageSearchModelManager(const NaturalLanguageSearchModelManager&) = delete;
    NaturalLanguageSearchModelManager(NaturalLanguageSearchModelManager&&) = delete;
    NaturalLanguageSearchModelManager& operator=(const NaturalLanguageSearchModelManager&) = delete;

    static Option<nlohmann::json> get_model(const std::string& model_id);
    static Option<bool> add_model(nlohmann::json& model, const std::string& model_id, const bool write_to_disk);
    static Option<nlohmann::json> delete_model(const std::string& model_id);
    static Option<nlohmann::json> get_all_models();
    static Option<nlohmann::json> update_model(const std::string& model_id, nlohmann::json model);
    static Option<int> init(Store* store);
    static bool migrate_model(nlohmann::json& model);

    static void init_schema_prompts_cache(uint32_t capacity);
    static Option<std::string> get_schema_prompt(const std::string& collection_name, uint64_t ttl_seconds = DEFAULT_SCHEMA_PROMPT_TTL_SEC);
    static void clear_schema_prompt(const std::string& collection_name);
    static void clear_all_schema_prompts();
    static bool has_cached_schema_prompt(const std::string& collection_name);
    
    static Option<uint64_t> process_nl_query_and_augment_params(std::map<std::string, std::string>& req_params, uint64_t schema_prompt_ttl_seconds = DEFAULT_SCHEMA_PROMPT_TTL_SEC);
    static void add_nl_query_data_to_results(nlohmann::json& results_json, const std::map<std::string, std::string>* req_params, uint64_t nl_processing_time_ms, bool error = false);
    static Option<nlohmann::json> process_natural_language_query(
        const std::string& nl_query, 
        const std::string& collection_name, 
        const std::string& nl_model_id = "default",
        uint64_t prompt_cache_ttl_seconds = DEFAULT_SCHEMA_PROMPT_TTL_SEC);

    static void dispose();

    static inline const uint64_t DEFAULT_SCHEMA_PROMPT_TTL_SEC = 86400;

    struct SchemaPromptEntry {
        std::string prompt;
        std::chrono::time_point<std::chrono::system_clock> created_at;

        SchemaPromptEntry(const std::string& prompt) : 
            prompt(prompt), 
            created_at(NaturalLanguageSearchModelManager::now()) {}

        bool operator==(const SchemaPromptEntry& other) const {
            return prompt == other.prompt && created_at == other.created_at;
        }

        bool operator!=(const SchemaPromptEntry& other) const {
            return !(*this == other);
        }
    };

    static void set_mock_time_for_testing(std::chrono::time_point<std::chrono::system_clock> mock_time);

    static void advance_mock_time_for_testing(uint64_t seconds);

    static void reset_mock_time();

    static std::chrono::time_point<std::chrono::system_clock> now();

private:
    static inline std::unordered_map<std::string, nlohmann::json> models;
    static inline std::shared_mutex models_mutex;

    static inline LRU::Cache<std::string, SchemaPromptEntry> schema_prompts{1000};
    static inline std::shared_mutex schema_prompts_mutex;

    static constexpr const char* MODEL_KEY_PREFIX = "$NLSP";
    static inline Store* store;

    static const std::string get_model_key(const std::string& model_id);
    static Option<nlohmann::json> delete_model_unsafe(const std::string& model_id);
    static Option<std::string> generate_schema_prompt(const std::string& collection_name);
    static nlohmann::json build_augmented_params(const std::map<std::string, std::string>* req_params);
    static nlohmann::json build_generated_params(const std::map<std::string, std::string>* req_params);

    static inline bool use_mock_time = false;
    static inline std::chrono::time_point<std::chrono::system_clock> mock_time_for_testing = std::chrono::system_clock::now();
}; 