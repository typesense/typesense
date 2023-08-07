#include <string_utils.h>
#include "override.h"
#include "tokenizer.h"

Option<bool> override_t::parse(const nlohmann::json& override_json, const std::string& id,
                               override_t& override,
                               const std::string& locale,
                               const std::vector<char>& symbols_to_index,
                               const std::vector<char>& token_separators) {
    if(!override_json.is_object()) {
        return Option<bool>(400, "Bad JSON.");
    }

    if(override_json.count("rule") == 0 || !override_json["rule"].is_object()) {
        return Option<bool>(400, "Missing `rule` definition.");
    }

    if (override_json["rule"].count("filter_by") == 0 &&
        (override_json["rule"].count("query") == 0 || override_json["rule"].count("match") == 0)) {
        return Option<bool>(400, "The `rule` definition must contain a `query` and `match`.");
    }

    if(override_json.count("includes") == 0 && override_json.count("excludes") == 0 &&
       override_json.count("filter_by") == 0 && override_json.count("sort_by") == 0 &&
       override_json.count("remove_matched_tokens") == 0 &&
       override_json.count("replace_query") == 0) {
        return Option<bool>(400, "Must contain one of: `includes`, `excludes`, "
                                 "`filter_by`, `sort_by`, `remove_matched_tokens`, `replace_query`.");
    }

    if(override_json.count("includes") != 0) {
        if(!override_json["includes"].is_array()) {
            return Option<bool>(400, "The `includes` value must be an array.");
        }

        for(const auto & include_obj: override_json["includes"]) {
            if(!include_obj.is_object()) {
                return Option<bool>(400, "The `includes` value must be an array of objects.");
            }

            if(include_obj.count("id") == 0 || include_obj.count("position") == 0) {
                return Option<bool>(400, "Inclusion definition must define both `id` and `position` keys.");
            }

            if(!include_obj["id"].is_string()) {
                return Option<bool>(400, "Inclusion `id` must be a string.");
            }

            if(!include_obj["position"].is_number_integer()) {
                return Option<bool>(400, "Inclusion `position` must be an integer.");
            }
        }
    }

    if(override_json.count("excludes") != 0) {
        if(!override_json["excludes"].is_array()) {
            return Option<bool>(400, "The `excludes` value must be an array.");
        }

        for(const auto & exclude_obj: override_json["excludes"]) {
            if(!exclude_obj.is_object()) {
                return Option<bool>(400, "The `excludes` value must be an array of objects.");
            }

            if(exclude_obj.count("id") == 0) {
                return Option<bool>(400, "Exclusion definition must define an `id`.");
            }

            if(!exclude_obj["id"].is_string()) {
                return Option<bool>(400, "Exclusion `id` must be a string.");
            }
        }

    }

    if(override_json.count("filter_by") != 0) {
        if(!override_json["filter_by"].is_string()) {
            return Option<bool>(400, "The `filter_by` must be a string.");
        }

        if(override_json["filter_by"].get<std::string>().empty()) {
            return Option<bool>(400, "The `filter_by` must be a non-empty string.");
        }
    }

    if(override_json.count("remove_matched_tokens") != 0) {
        if (!override_json["remove_matched_tokens"].is_boolean()) {
            return Option<bool>(400, "The `remove_matched_tokens` must be a boolean.");
        }
    }

    if(override_json.count("filter_curated_hits") != 0) {
        if (!override_json["filter_curated_hits"].is_boolean()) {
            return Option<bool>(400, "The `filter_curated_hits` must be a boolean.");
        }
    }

    if(override_json.count("stop_processing") != 0) {
        if (!override_json["stop_processing"].is_boolean()) {
            return Option<bool>(400, "The `stop_processing` must be a boolean.");
        }
    }

    if(!id.empty()) {
        override.id = id;
    } else if(override_json.count("id") != 0) {
        override.id = override_json["id"].get<std::string>();
    } else {
        return Option<bool>(400, "Override `id` not provided.");
    }

    const auto& json_rule = override_json["rule"];
    override.rule.query = json_rule.count("query") == 0 ? "" : json_rule["query"].get<std::string>();
    override.rule.match = json_rule.count("match") == 0 ? "" : json_rule["match"].get<std::string>();

    if(!override.rule.query.empty()) {
        auto symbols = symbols_to_index;
        symbols.push_back('{');
        symbols.push_back('}');
        symbols.push_back('*');
        Tokenizer tokenizer(override.rule.query, true, false, locale, symbols, token_separators);
        std::vector<std::string> tokens;
        tokenizer.tokenize(tokens);
        override.rule.normalized_query = StringUtils::join(tokens, " ");
    }

    if(json_rule.count("filter_by") != 0) {
        if(!override_json["rule"]["filter_by"].is_string()) {
            return Option<bool>(400, "Override `rule.filter_by` must be a string.");
        }

        override.rule.filter_by = override_json["rule"]["filter_by"].get<std::string>();
    }

    if (override_json.count("includes") != 0) {
        for(const auto & include: override_json["includes"]) {
            add_hit_t add_hit;
            add_hit.doc_id = include["id"].get<std::string>();
            add_hit.position = include["position"].get<uint32_t>();
            override.add_hits.push_back(add_hit);
        }
    }

    if (override_json.count("excludes") != 0) {
        for(const auto & exclude: override_json["excludes"]) {
            drop_hit_t drop_hit;
            drop_hit.doc_id = exclude["id"].get<std::string>();
            override.drop_hits.push_back(drop_hit);
        }
    }

    if (override_json.count("filter_by") != 0) {
        override.filter_by = override_json["filter_by"].get<std::string>();
    }

    if (override_json.count("sort_by") != 0) {
        override.sort_by = override_json["sort_by"].get<std::string>();
    }

    if (override_json.count("replace_query") != 0) {
        if(override_json.count("remove_matched_tokens") != 0 && override_json["remove_matched_tokens"].get<bool>()) {
            return Option<bool>(400, "Only one of `replace_query` or `remove_matched_tokens` can be specified.");
        }
        override.replace_query = override_json["replace_query"].get<std::string>();
    }

    if(override_json.count("remove_matched_tokens") != 0) {
        override.remove_matched_tokens = override_json["remove_matched_tokens"].get<bool>();
    } else {
        override.remove_matched_tokens = (override_json.count("filter_by") != 0);
    }

    if(override_json.count("filter_curated_hits") != 0) {
        override.filter_curated_hits = override_json["filter_curated_hits"].get<bool>();
    }

    if(override_json.count("stop_processing") != 0) {
        override.stop_processing = override_json["stop_processing"].get<bool>();
    }

    if(override_json.count("effective_from_ts") != 0) {
        override.effective_from_ts = override_json["effective_from_ts"].get<int64_t>();
    }

    if(override_json.count("effective_to_ts") != 0) {
        override.effective_to_ts = override_json["effective_to_ts"].get<int64_t>();
    }

    // we have to also detect if it is a dynamic query rule
    size_t i = 0;
    while(i < override.rule.normalized_query.size()) {
        if(override.rule.normalized_query[i] == '{') {
            // look for closing curly
            i++;
            while(i < override.rule.normalized_query.size()) {
                if(override.rule.normalized_query[i] == '}') {
                    override.rule.dynamic_query = true;
                    // remove spaces around curlies
                    override.rule.normalized_query = StringUtils::trim_curly_spaces(override.rule.normalized_query);
                    break;
                }
                i++;
            }
        }
        i++;
    }

    return Option<bool>(true);
}

nlohmann::json override_t::to_json() const {
    nlohmann::json override;
    override["id"] = id;

    if(!rule.query.empty()) {
        override["rule"]["query"] = rule.query;
    }

    if(!rule.match.empty()) {
        override["rule"]["match"] = rule.match;
    }

    if(!rule.filter_by.empty()) {
        override["rule"]["filter_by"] = rule.filter_by;
    }

    override["includes"] = nlohmann::json::array();

    for(const auto & add_hit: add_hits) {
        nlohmann::json include;
        include["id"] = add_hit.doc_id;
        include["position"] = add_hit.position;
        override["includes"].push_back(include);
    }

    override["excludes"] = nlohmann::json::array();
    for(const auto & drop_hit: drop_hits) {
        nlohmann::json exclude;
        exclude["id"] = drop_hit.doc_id;
        override["excludes"].push_back(exclude);
    }

    if(!filter_by.empty()) {
        override["filter_by"] = filter_by;
    }

    if(!sort_by.empty()) {
        override["sort_by"] = sort_by;
    }

    if(!replace_query.empty()) {
        override["replace_query"] = replace_query;
    }

    if(effective_from_ts != -1) {
        override["effective_from_ts"] = effective_from_ts;
    }

    if(effective_to_ts != -1) {
        override["effective_to_ts"] = effective_to_ts;
    }

    override["remove_matched_tokens"] = remove_matched_tokens;
    override["filter_curated_hits"] = filter_curated_hits;
    override["stop_processing"] = stop_processing;

    return override;
}
