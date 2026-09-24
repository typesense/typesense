#pragma once

#include <string>
#include <shared_mutex>
#include <mutex>
#include <map>
#include <vector>
#include <unordered_map>
#include <json.hpp>
#include <option.h>
#include "store.h"
#include "sole.hpp"
#include "lru/lru.hpp"
#include <chrono>

class Collection;

// per-request knobs for the schema prompt sent to the LLM: which facet fields are enumerated and how many
// values each one contributes. defaults mirror what used to be hardcoded in generate_schema_prompt().
struct schema_prompt_params_t {
    static inline const size_t DEFAULT_MAX_FACET_VALUES = 20;
    static inline const size_t DEFAULT_SCHEMA_SAMPLE_VALUES = 10;
    static inline const size_t DEFAULT_FACET_SAMPLE_PERCENT = 20;
    static inline const size_t DEFAULT_FACET_SAMPLE_THRESHOLD = 1000;

    // sanity ceiling for both value caps, guards against a typo blowing up the prompt
    static inline const size_t MAX_FACET_VALUES_LIMIT = 10000;

    // number of values fetched per facet field
    size_t max_facet_values = DEFAULT_MAX_FACET_VALUES;
    // number of fetched values listed in the prompt
    size_t schema_sample_values = DEFAULT_SCHEMA_SAMPLE_VALUES;
    // an explicit request value must not be widened by the jev defaults, even when it equals a default
    bool max_facet_values_set = false;
    bool schema_sample_values_set = false;
    // facet counts are estimated from this % of docs once the result set exceeds the threshold
    size_t facet_sample_percent = DEFAULT_FACET_SAMPLE_PERCENT;
    size_t facet_sample_threshold = DEFAULT_FACET_SAMPLE_THRESHOLD;
    // empty means every string facet field
    std::vector<std::string> facet_fields;

    static Option<schema_prompt_params_t> parse(const std::map<std::string, std::string>& req_params);

    std::string cache_key(const std::string& collection_name) const;
};

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

    static Option<std::vector<std::string>> resolve_facet_fields(Collection* coll,
                                                                 const schema_prompt_params_t& prompt_params);

    // an empty map means the sample failed, scope_filter limits sampling to what the caller's key can see
    static std::unordered_map<std::string, std::vector<std::string>> fetch_facet_values(
            Collection* coll, const std::vector<std::string>& facet_fields,
            const schema_prompt_params_t& prompt_params, const std::string& scope_filter = "");

    static void init_schema_prompts_cache(uint32_t capacity);
    static Option<std::string> get_schema_prompt(const std::string& collection_name, uint64_t ttl_seconds = DEFAULT_SCHEMA_PROMPT_TTL_SEC,
                                                 const schema_prompt_params_t& prompt_params = schema_prompt_params_t());
    static void clear_schema_prompt(const std::string& collection_name);
    static void clear_all_schema_prompts();
    static bool has_cached_schema_prompt(const std::string& collection_name);

    // embedded_params carries a scoped api key's forced search params, its filter_by scopes sampling
    static Option<uint64_t> process_nl_query_and_augment_params(std::map<std::string, std::string>& req_params,
                                                                uint64_t schema_prompt_ttl_seconds = DEFAULT_SCHEMA_PROMPT_TTL_SEC,
                                                                const nlohmann::json& embedded_params = nlohmann::json::object());
    static void add_nl_query_data_to_results(nlohmann::json& results_json, const std::map<std::string, std::string>* req_params, uint64_t nl_processing_time_ms, bool error = false);
    static Option<nlohmann::json> process_natural_language_query(
        const std::string& nl_query,
        const std::string& collection_name,
        const std::string& nl_model_id = "default",
        uint64_t prompt_cache_ttl_seconds = DEFAULT_SCHEMA_PROMPT_TTL_SEC,
        const schema_prompt_params_t& prompt_params = schema_prompt_params_t(),
        const std::string& scope_filter = "");

    static void dispose();

    static inline const uint64_t DEFAULT_SCHEMA_PROMPT_TTL_SEC = 86400;

    struct SchemaPromptEntry {
        std::string prompt;
        // the cache is keyed by collection + prompt params, so the collection is tracked for bulk invalidation
        std::string collection_name;
        std::chrono::time_point<std::chrono::system_clock> created_at;

        SchemaPromptEntry(const std::string& prompt, const std::string& collection_name = "") :
            prompt(prompt),
            collection_name(collection_name),
            created_at(NaturalLanguageSearchModelManager::now()) {}

        bool operator==(const SchemaPromptEntry& other) const {
            return prompt == other.prompt && collection_name == other.collection_name &&
                   created_at == other.created_at;
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
    static Option<std::string> generate_schema_prompt(const std::string& collection_name,
                                                      const schema_prompt_params_t& prompt_params);
    static nlohmann::json build_augmented_params(const std::map<std::string, std::string>* req_params);
    static nlohmann::json build_generated_params(const std::map<std::string, std::string>* req_params);

    static inline bool use_mock_time = false;
    static inline std::chrono::time_point<std::chrono::system_clock> mock_time_for_testing = std::chrono::system_clock::now();
}; 