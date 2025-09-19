#pragma once
#include <string>
#include <json.hpp>
#include <diversity.h>
#include "option.h"

struct override_t {
    static const std::string MATCH_EXACT;
    static const std::string MATCH_CONTAINS;

    struct rule_t {
        std::string query;
        std::string normalized_query;       // not actually stored, used for lowercasing etc.
        std::string match;
        bool dynamic_query = false;
        bool dynamic_filter = false;
        std::string filter_by;
        std::set<std::string> tags;
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

    nlohmann::json metadata;

    // epoch seconds
    int64_t effective_from_ts = -1;
    int64_t effective_to_ts = -1;

    diversity_t diversity{};

    override_t() = default;

    static Option<bool> parse(const nlohmann::json& override_json, const std::string& id,
                              override_t& override,
                              const std::string& locale = "",
                              const std::vector<char>& symbols_to_index = {},
                              const std::vector<char>& token_separators = {},
                              const tsl::htrie_map<char, field>& search_schema = tsl::htrie_map<char, field>());

    nlohmann::json to_json() const;
};
