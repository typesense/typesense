#pragma once

#include "json.hpp"
#include "include/option.h"
#include "field.h"

struct Hasher32 {
    // Helps to spread the hash key and is used for sort index.
    // see: https://github.com/greg7mdp/sparsepp/issues/21#issuecomment-270816275
    size_t operator()(uint32_t k) const { return (k ^ 2166136261U)  * 16777619UL; }
};

struct diversity_t {
// ": int" fixes "In template: no matching function for call to 'is_valid'" for magic_enum::enum_cast
// https://github.com/Neargye/magic_enum/issues/204#issuecomment-1238393619
    enum similarity_methods : int {
        equality,
        jaccard
    };

    struct similarity_metric_t {
        std::string field{};
        similarity_methods method{};
        float weight = 1;
        bool is_field_array = false;

        similarity_metric_t(std::string& field, similarity_methods& method, float& weight) :
                field(field), method(method), weight(weight) {}
    };
    std::vector<similarity_metric_t> similarity_equation;

    static Option<bool> parse(const nlohmann::json& json, diversity_t& diversity);

    static void to_json(const diversity_t& diversity, nlohmann::json& json);
};
