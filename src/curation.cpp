#include <string_utils.h>
#include "curation.h"
#include "tokenizer.h"

Option<bool> curation_t::parse(const nlohmann::json& curation_json, const std::string& id,
                               curation_t& curation,
                               const std::string& locale,
                               const std::vector<char>& symbols_to_index,
                               const std::vector<char>& token_separators) {
    if(!curation_json.is_object()) {
        return Option<bool>(400, "Bad JSON.");
    }

    if(curation_json.count("rule") == 0 || !curation_json["rule"].is_object()) {
        return Option<bool>(400, "Missing `rule` definition.");
    }

    if (curation_json["rule"].count("filter_by") == 0 && curation_json["rule"].count("tags") == 0 &&
        (curation_json["rule"].count("query") == 0 || curation_json["rule"].count("match") == 0)) {
        return Option<bool>(400, "The `rule` definition must contain either a `tags` or a `query` and `match`.");
    }

    if (curation_json["rule"].count("match") != 0 && curation_json["rule"].count("query") == 0) {
        return Option<bool>(400, "The `match` field requires a `query` field to be present.");
    }

    if(curation_json.count("includes") == 0 && curation_json.count("excludes") == 0 &&
       curation_json.count("filter_by") == 0 && curation_json.count("sort_by") == 0 &&
       curation_json.count("remove_matched_tokens") == 0 && curation_json.count("metadata") == 0 &&
       curation_json.count("replace_query") == 0 && !curation_json.contains("diversity")) {
        return Option<bool>(400, "Must contain one of: `includes`, `excludes`, `metadata`, "
                                 "`filter_by`, `sort_by`, `remove_matched_tokens`, `replace_query`.");
    }

    if(curation_json["rule"].count("tags") != 0) {
        if(!curation_json["rule"]["tags"].is_array()) {
            return Option<bool>(400, "The `tags` value must be an array of strings.");
        }

        for(const auto& tag: curation_json["rule"]["tags"]) {
            if(!tag.is_string()) {
                return Option<bool>(400, "The `tags` value must be an array of strings.");
            }

            curation.rule.tags.insert(tag.get<std::string>());
        }
    }

    if(curation_json.count("includes") != 0) {
        if(!curation_json["includes"].is_array()) {
            return Option<bool>(400, "The `includes` value must be an array.");
        }

        for(const auto & include_obj: curation_json["includes"]) {
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

    if(curation_json.count("excludes") != 0) {
        if(!curation_json["excludes"].is_array()) {
            return Option<bool>(400, "The `excludes` value must be an array.");
        }

        for(const auto & exclude_obj: curation_json["excludes"]) {
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

    if(curation_json.count("filter_by") != 0) {
        if(!curation_json["filter_by"].is_string()) {
            return Option<bool>(400, "The `filter_by` must be a string.");
        }

        if(curation_json["filter_by"].get<std::string>().empty()) {
            return Option<bool>(400, "The `filter_by` must be a non-empty string.");
        }
    }

    if(curation_json.count("remove_matched_tokens") != 0) {
        if (!curation_json["remove_matched_tokens"].is_boolean()) {
            return Option<bool>(400, "The `remove_matched_tokens` must be a boolean.");
        }
    }

    if(curation_json.count("filter_curated_hits") != 0) {
        if (!curation_json["filter_curated_hits"].is_boolean()) {
            return Option<bool>(400, "The `filter_curated_hits` must be a boolean.");
        }
    }

    if(curation_json.count("stop_processing") != 0) {
        if (!curation_json["stop_processing"].is_boolean()) {
            return Option<bool>(400, "The `stop_processing` must be a boolean.");
        }
    }

    if(!id.empty()) {
        curation.id = id;
    } else if(curation_json.count("id") != 0) {
        curation.id = curation_json["id"].get<std::string>();
    } else {
        return Option<bool>(400, "Curation `id` not provided.");
    }

    const auto& json_rule = curation_json["rule"];
    curation.rule.query = json_rule.count("query") == 0 ? "" : json_rule["query"].get<std::string>();
    curation.rule.match = json_rule.count("match") == 0 ? "" : json_rule["match"].get<std::string>();


    if(json_rule.count("stem") != 0 && json_rule["stem"].is_boolean()) {
        curation.rule.stem = json_rule["stem"].get<bool>();
        curation.rule.locale = locale;

        if(json_rule.count("stemming_dictionary") != 0) {
            curation.rule.stemming_dictionary = json_rule["stemming_dictionary"];
        }
    }

    if(json_rule.count("synonyms") != 0 && json_rule["synonyms"].is_boolean()) {
        curation.rule.synonyms = json_rule["synonyms"].get<bool>();
    }

    if(!curation.rule.query.empty()) {
        auto symbols = symbols_to_index;
        symbols.push_back('{');
        symbols.push_back('}');
        symbols.push_back('*');
        symbols.push_back('.');

        auto stemmer = curation.rule.stem ? StemmerManager::get_instance().get_stemmer(locale, curation.rule.stemming_dictionary) : nullptr;

        Tokenizer tokenizer(curation.rule.query, true, false, locale, symbols, token_separators, stemmer, true);
        std::vector<std::string> tokens;
        tokenizer.tokenize(tokens);
        curation.rule.normalized_query = StringUtils::join(tokens, " ");
    }

    if(json_rule.count("filter_by") != 0) {
        if(!curation_json["rule"]["filter_by"].is_string()) {
            return Option<bool>(400, "Curation `rule.filter_by` must be a string.");
        }

        curation.rule.filter_by = curation_json["rule"]["filter_by"].get<std::string>();

        //check if it is dynamic filter rule
        size_t i = 0;
        while(i < curation.rule.filter_by.size()) {
            if(curation.rule.filter_by[i] == '{') {
                // look for closing curly
                i++;
                while(i < curation.rule.filter_by.size()) {
                    if(curation.rule.filter_by[i] == '}') {
                        curation.rule.dynamic_filter = true;
                        // remove spaces around curlies
                        curation.rule.filter_by = StringUtils::trim_curly_spaces(curation.rule.filter_by);
                        break;
                    }
                    i++;
                }
            }
            i++;
        }
    }

    if (curation_json.count("includes") != 0) {
        for(const auto & include: curation_json["includes"]) {
            add_hit_t add_hit;
            add_hit.doc_id = include["id"].get<std::string>();
            add_hit.position = include["position"].get<uint32_t>();
            curation.add_hits.push_back(add_hit);
        }
    }

    if (curation_json.count("excludes") != 0) {
        for(const auto & exclude: curation_json["excludes"]) {
            drop_hit_t drop_hit;
            drop_hit.doc_id = exclude["id"].get<std::string>();
            curation.drop_hits.push_back(drop_hit);
        }
    }

    if (curation_json.count("filter_by") != 0) {
        curation.filter_by = curation_json["filter_by"].get<std::string>();
    }

    if (curation_json.count("sort_by") != 0) {
        curation.sort_by = curation_json["sort_by"].get<std::string>();
    }

    if (curation_json.count("replace_query") != 0) {
        if(curation_json.count("remove_matched_tokens") != 0 && curation_json["remove_matched_tokens"].get<bool>()) {
            return Option<bool>(400, "Only one of `replace_query` or `remove_matched_tokens` can be specified.");
        }
        curation.replace_query = curation_json["replace_query"].get<std::string>();
    }

    if (curation_json.count("metadata") != 0) {
        if(!curation_json["metadata"].is_object()) {
            return Option<bool>(400, "The `metadata` must be a JSON object.");
        }
        curation.metadata = curation_json["metadata"];
    }

    if(curation_json.count("remove_matched_tokens") != 0) {
        curation.remove_matched_tokens = curation_json["remove_matched_tokens"].get<bool>();
    } else {
        curation.remove_matched_tokens = (curation_json.count("filter_by") != 0);
    }

    if(curation_json.count("filter_curated_hits") != 0) {
        curation.filter_curated_hits = curation_json["filter_curated_hits"].get<bool>();
    }

    if(curation_json.count("stop_processing") != 0) {
        curation.stop_processing = curation_json["stop_processing"].get<bool>();
    }

    if(curation_json.count("effective_from_ts") != 0) {
        curation.effective_from_ts = curation_json["effective_from_ts"].get<int64_t>();
    }

    if(curation_json.count("effective_to_ts") != 0) {
        curation.effective_to_ts = curation_json["effective_to_ts"].get<int64_t>();
    }

    // we have to also detect if it is a dynamic query rule
    size_t i = 0;
    while(i < curation.rule.normalized_query.size()) {
        if(curation.rule.normalized_query[i] == '{') {
            // look for closing curly
            i++;
            while(i < curation.rule.normalized_query.size()) {
                if(curation.rule.normalized_query[i] == '}') {
                    curation.rule.dynamic_query = true;
                    // remove spaces around curlies
                    curation.rule.normalized_query = StringUtils::trim_curly_spaces(curation.rule.normalized_query);
                    break;
                }
                i++;
            }
        }
        i++;
    }

    if (curation_json.contains("diversity")) {
        auto op = diversity_t::parse(curation_json, curation.diversity);
        if (!op.ok()) {
            return op;
        }

        // The validation logic with the search schema is moved to collection.cpp curate_results method
    }

    return Option<bool>(true);
}

nlohmann::json curation_t::to_json() const {
    nlohmann::json curation;
    curation["id"] = id;

    if(!rule.query.empty()) {
        curation["rule"]["query"] = rule.query;
    }

    if(!rule.match.empty()) {
        curation["rule"]["match"] = rule.match;
    }

    if(!rule.filter_by.empty()) {
        curation["rule"]["filter_by"] = rule.filter_by;
    }

    if(!rule.tags.empty()) {
        curation["rule"]["tags"] = rule.tags;
    }

    curation["includes"] = nlohmann::json::array();

    for(const auto & add_hit: add_hits) {
        nlohmann::json include;
        include["id"] = add_hit.doc_id;
        include["position"] = add_hit.position;
        curation["includes"].push_back(include);
    }

    curation["excludes"] = nlohmann::json::array();
    for(const auto & drop_hit: drop_hits) {
        nlohmann::json exclude;
        exclude["id"] = drop_hit.doc_id;
        curation["excludes"].push_back(exclude);
    }

    if(!filter_by.empty()) {
        curation["filter_by"] = filter_by;
    }

    if(!sort_by.empty()) {
        curation["sort_by"] = sort_by;
    }

    if(!replace_query.empty()) {
        curation["replace_query"] = replace_query;
    }

    if(effective_from_ts != -1) {
        curation["effective_from_ts"] = effective_from_ts;
    }

    if(effective_to_ts != -1) {
        curation["effective_to_ts"] = effective_to_ts;
    }

    curation["remove_matched_tokens"] = remove_matched_tokens;
    curation["filter_curated_hits"] = filter_curated_hits;
    curation["stop_processing"] = stop_processing;

    if(!metadata.empty()) {
        curation["metadata"] = metadata;
    }

    if (!diversity.similarity_equation.empty()) {
        diversity_t::to_json(diversity, curation);
    }

    return curation;
}
