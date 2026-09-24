#pragma once

#include "json.hpp"
#include "include/option.h"
#include "field.h"
#include "facet_index.h"

struct Hasher32 {
    // Helps to spread the hash key and is used for sort index.
    // see: https://github.com/greg7mdp/sparsepp/issues/21#issuecomment-270816275
    size_t operator()(uint32_t k) const { return (k ^ 2166136261U)  * 16777619UL; }
};

struct diversity_t {
    static constexpr float DEFAULT_LAMDA_VALUE = 0.5;

    float lambda{};
    size_t limit{};

// ": int" fixes "In template: no matching function for call to 'is_valid'" for magic_enum::enum_cast
// https://github.com/Neargye/magic_enum/issues/204#issuecomment-1238393619
    enum similarity_methods : int {
        equality,
        jaccard,
        vector_distance
    };

    struct similarity_metric_t {
        std::string field{};
        similarity_methods method{};
        float weight = 1;
        bool is_field_array = false;

        similarity_metric_t(std::string& field, similarity_methods& method, float& weight) :
                field(field), method(method), weight(weight) {}
    };
    std::vector<similarity_metric_t> similarity_equation{};

    static Option<bool> parse(const nlohmann::json& json, diversity_t& diversity);

    static void to_json(const diversity_t& diversity, nlohmann::json& json);
};

struct pair_hash {
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2> &pair) const {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};

struct similarity_t {
    static Option<double> calculate(uint32_t seq_id_i, uint32_t seq_id_j, const diversity_t& diversity,
                                    const spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t, Hasher32>*>& sort_index,
                                    const facet_index_t* facet_index_v4,
                                    const spp::sparse_hash_map<std::string, hnsw_index_t*>& vector_index);
};
