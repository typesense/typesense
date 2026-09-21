#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <option.h>
#include "json.hpp"
#include "natural_language_search_model_manager.h"

struct jev_facet_field_t {
    std::string name;
    std::vector<std::string> values;
    // count of `values` matched by the query, sorted to the front, zero once the shortlist is off
    size_t num_matched = 0;
    // values the shortlist chose from, equals values.size() when nothing was dropped
    size_t vocabulary_size = 0;
};

struct jev_numeric_field_t {
    std::string name;
    std::string type;
};

struct jev_catalog_t {
    std::string collection_name;
    std::vector<jev_facet_field_t> facet_fields;
    std::vector<jev_numeric_field_t> numeric_fields;
    std::vector<std::string> bool_fields;
    std::vector<std::string> sort_fields;
    std::unordered_map<std::string, std::string> field_descriptions;
};

struct jev_query_facts_t {
    std::string query;
    std::string today;
    // occurrences are preserved, `under 50 and over 50` is two independent constraints
    std::vector<std::string> numbers;
    std::vector<std::string> number_spans;
    // surrounding words per occurrence, lets jev tell two `50`s apart
    std::vector<std::string> number_contexts;
    std::vector<std::string> tokens;
    // a phrase or exclusion operator in the raw query skips token level rewriting
    bool has_query_operators = false;
    // an or word in the query is the only time the alternative questions are asked
    bool has_or = false;
};

struct jev_options_t {
    double present_threshold = 0.6;
    double confidence_threshold = 0.5;
    double or_min_probability = 0.15;
    // a second facet value joins its field's clause past this
    double second_value_threshold = 0.5;
    double token_threshold = 0.5;
    // a literal fans out to a second field past this, and negated clauses need this much conviction
    double also_bound_threshold = 0.8;

    size_t max_options_per_question = 120;
    // the frequency floor fills what is left. 0 offers the whole sampled vocabulary
    size_t max_shortlist_values = 40;
    // a paraphrase the matcher cannot see has to find its value here
    size_t shortlist_floor_values = 20;
    size_t max_questions = 600;
    size_t max_token_questions = 24;
    // n anded clauses cost 2n-1 postfix tokens against filter_by_max_ops
    size_t max_clauses = 40;
    size_t max_sort_fields = 3;

    // 0 derives twice the per call timeout, optional rounds are skipped when the leftover budget cannot fund one
    size_t total_timeout_ms = 0;

    // a digitless query gets numeral questions and, on a hit, a second round, each an extra api call
    bool word_numbers = false;
    size_t max_word_number_literals = 3;
    // surviving q tokens are judged against the emitted filters in a follow up round
    bool consumed_check = false;
};

class JevSearchParams {
public:
    // a hard ceiling for jev rather than a hint, wider than the llm schema prompt default
    static constexpr size_t DEFAULT_SAMPLE_VALUES = 100;

    // scope_filter comes from the scoped api key, the catalog must not sample values the caller cannot see
    static Option<nlohmann::json> generate(const std::string& query,
                                           const std::string& collection_name,
                                           const nlohmann::json& model_config,
                                           const schema_prompt_params_t& prompt_params,
                                           const std::string& scope_filter = "",
                                           const jev_options_t& opts = jev_options_t());

    static Option<bool> options_from_config(const nlohmann::json& model_config, jev_options_t& opts);

    static Option<jev_catalog_t> build_catalog(const std::string& collection_name,
                                               const schema_prompt_params_t& prompt_params,
                                               const std::string& scope_filter = "");

    static jev_query_facts_t pre_parse(const std::string& query);

    // code only retrieves here, the model still decides, return value is the per field trace
    static nlohmann::json shortlist_values(jev_catalog_t& catalog, const jev_query_facts_t& facts,
                                           const jev_options_t& opts);

    static nlohmann::json build_state(const jev_query_facts_t& facts, const jev_catalog_t& catalog,
                                      const std::string& context = "");

    static Option<nlohmann::json> build_questions(const jev_catalog_t& catalog, const jev_query_facts_t& facts,
                                                  const jev_options_t& opts);

    static nlohmann::json assemble(const jev_catalog_t& catalog, const jev_query_facts_t& facts,
                                   const nlohmann::json& answers, const jev_options_t& opts = jev_options_t());

    // drops what cannot round trip through the filter grammar, empty means drop the value entirely
    static std::string sanitize_filter_value(const std::string& raw_value);
    static std::string escape_filter_value(const std::string& raw_value);

    // question ids, derived from the catalog index
    static std::string facet_present_id(size_t i);
    static std::string facet_value_id(size_t i);
    static std::string facet_value2_id(size_t i);
    static std::string facet_both_id(size_t i);
    static std::string facet_negate_id(size_t i);
    static std::string facet_alt_id(size_t i);
    static std::string bool_alt_id(size_t i);
    static std::string number_alt_id(size_t k);
    static std::string number_field_id(size_t k);
    static std::string number_op_id(size_t k);
    static std::string number_also_id(size_t k, size_t i);
    static std::string bool_id(size_t i);
    static std::string token_id(size_t i);
    static std::string token_number_id(size_t i);
    static constexpr const char* SORT_FIELD_ID = "sort__field";
    static std::string sort_dir_id(size_t i);
};
