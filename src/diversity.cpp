#include <magic_enum.hpp>
#include "diversity.h"
#include "logger.h"
#include "string_utils.h"

Option<bool> diversity_t::parse(const nlohmann::json& json, diversity_t& diversity) {
    // format:
    // "diversity": {
    //          "similarity_metric": [ { "field": "...", "method": "...", "weight": ... }, ... ]
    //      }

    auto it = json.find("diversity");
    if (it == json.end()) {
        return Option<bool>(400, "`diversity` not found: " + json.dump());
    } else if (!it.value().is_object()) {
        return Option<bool>(400, "Invalid `diversity` format: " + it.value().dump());
    } else if (it.value().empty()) {
        return Option<bool>(true);
    }

    auto& rule = it.value();
    if (!rule.is_object() || rule.empty()) {
        return Option<bool>(400, "Invalid `diversity` format, expected an object: " + rule.dump());
    }

    it = rule.find("similarity_metric");
    if (it == rule.end()) {
        return Option<bool>(400, "`similarity_metric` not found: " + rule.dump());
    } else if (!it.value().is_array() || it.value().empty()) {
        return Option<bool>(400, "Invalid `similarity_metric` format: " + it.value().dump());
    }

    std::vector<similarity_metric_t> similarity_metric;
    for (const auto& metric: it.value()) {
        if (!metric.is_object() || metric.empty()) {
            return Option<bool>(400, "Invalid `similarity_metric` format, expected an object: " + metric.dump());
        }

        auto metric_it = metric.find("field");
        if (metric_it == metric.end()) {
            return Option<bool>(400, "`field` not found: " + metric.dump());
        } else if (!metric_it.value().is_string() || metric_it.value().empty()) {
            return Option<bool>(400, "Invalid `field` format: " + metric_it.value().dump());
        }
        std::string field = metric_it.value();

        metric_it = metric.find("method");
        if (metric_it == metric.end()) {
            return Option<bool>(400, "`method` not found: " + metric.dump());
        } else if (!metric_it.value().is_string() || metric_it.value().empty()) {
            return Option<bool>(400, "Invalid `method` format: " + metric_it.value().dump());
        }

        std::string str = metric_it.value().get<std::string>();
        StringUtils::tolowercase(str);
        similarity_methods method;
        auto op = magic_enum::enum_cast<similarity_methods>(str);
        if (op.has_value()) {
            method = op.value();
        } else {
            return Option<bool>(400, "`method` not found: " + metric.dump());
        }

        float weight;
        metric_it = metric.find("weight");
        if (metric_it == metric.end()) {
            weight = 1;
        } else if (!metric_it.value().is_number_float()) {
            return Option<bool>(400, "Invalid `weight` format: " + metric_it.value().dump());
        } else {
            weight = metric_it.value();
        }

        similarity_metric.emplace_back(field, method, weight);
    }

    diversity.similarity_equation = std::move(similarity_metric);

    return Option<bool>(true);
}

void diversity_t::to_json(const diversity_t &diversity, nlohmann::json& json) {
    for (const auto& item: diversity.similarity_equation) {
        nlohmann::json metric;
        metric["field"] = item.field;
        metric["method"] = magic_enum::enum_name(item.method);
        metric["weight"] = item.weight;
        json["diversity"]["similarity_metric"] += metric;
    }
}
