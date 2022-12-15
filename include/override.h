#pragma once
#include <string>
#include <json.hpp>
#include "option.h"

struct override_t {
    static const std::string MATCH_EXACT;
    static const std::string MATCH_CONTAINS;

    struct rule_t {
        std::string query;
        std::string match;
        bool dynamic_query = false;
        std::string filter_by;
    };

    struct add_hit_t {
        std::string doc_id;
        uint32_t position = 0;
    };

    struct drop_hit_t {
        std::string doc_id;
    };

    std::string id;

    rule_t rule;
    std::vector<add_hit_t> add_hits;
    std::vector<drop_hit_t> drop_hits;

    std::string filter_by;
    bool remove_matched_tokens = false;
    bool filter_curated_hits = false;

    bool stop_processing = true;

    std::string sort_by;
    std::string replace_query;

    // epoch seconds
    int64_t effective_from_ts = -1;
    int64_t effective_to_ts = -1;

    override_t() = default;

    static Option<bool> parse(const nlohmann::json& override_json, const std::string& id, override_t& override);

    nlohmann::json to_json() const;
};
