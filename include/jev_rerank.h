#pragma once

#include <string>
#include <vector>
#include "json.hpp"

class JevRerank {
public:
    static constexpr size_t DEFAULT_TOP_K = 20;
    // the 32k state budget divided across candidates bounds how many ride in one request
    static constexpr size_t MAX_TOP_K = 50;
    static constexpr size_t MAX_CANDIDATE_BYTES = 2048;

    // looks up the model by id and delegates, a bad id is a warning and the original order stands
    static void rerank(nlohmann::json& hits, const std::string& query,
                       const std::vector<std::vector<std::string>>& query_by_per_search,
                       const std::string& model_id, size_t top_k);

    static void rerank_with_model(nlohmann::json& hits, const std::string& query,
                                  const std::vector<std::vector<std::string>>& query_by_per_search,
                                  const nlohmann::json& model_config, size_t top_k);

    // uses the hit's query_by fields when present, falls back to the pruned document
    static nlohmann::json candidate_from_hit(const nlohmann::json& hit,
                                             const std::vector<std::vector<std::string>>& query_by_per_search);

    static nlohmann::json build_state(const std::string& query, const nlohmann::json& candidates);
    static nlohmann::json build_questions(size_t num_candidates);
    static std::string candidate_id(size_t i);
};
