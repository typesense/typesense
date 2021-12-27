#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <art.h>
#include <number.h>
#include <sparsepp.h>
#include <store.h>
#include <topster.h>
#include <json.hpp>
#include <field.h>
#include <option.h>
#include <set>
#include "string_utils.h"
#include "num_tree.h"
#include "magic_enum.hpp"
#include "match_score.h"
#include "posting_list.h"
#include "threadpool.h"
#include "adi_tree.h"

static constexpr size_t ARRAY_FACET_DIM = 4;
using facet_map_t = spp::sparse_hash_map<uint32_t, facet_hash_values_t>;
using array_mapped_facet_t = std::array<facet_map_t*, ARRAY_FACET_DIM>;

struct token_t {
    size_t position;
    std::string value;
    bool prefix;

    token_t(size_t position, const std::string& value, bool prefix): position(position), value(value), prefix(prefix) {

    }
};

struct token_candidates {
    token_t token;
    size_t cost;
    bool prefix_search;
    std::vector<art_leaf*> candidates;
};

struct search_field_t {
    std::string name;
    size_t priority;
    size_t weight;
};

struct query_tokens_t {
    std::vector<std::string> q_include_tokens;
    std::vector<std::vector<std::string>> q_exclude_tokens;
    std::vector<std::vector<std::string>> q_phrases;
    std::vector<std::vector<std::string>> q_synonyms;
};

struct override_t {
    static const std::string MATCH_EXACT;
    static const std::string MATCH_CONTAINS;

    struct rule_t {
        std::string query;
        std::string match;
        bool dynamic_query = false;
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
    bool remove_matched_tokens = true;

    override_t() = default;

    static Option<bool> parse(const nlohmann::json& override_json, const std::string& id, override_t& override) {
        if(!override_json.is_object()) {
            return Option<bool>(400, "Bad JSON.");
        }

        if(override_json.count("rule") == 0 || !override_json["rule"].is_object()) {
            return Option<bool>(400, "Missing `rule` definition.");
        }

        if(override_json["rule"].count("query") == 0 || override_json["rule"].count("match") == 0) {
            return Option<bool>(400, "The `rule` definition must contain a `query` and `match`.");
        }

        if(override_json.count("includes") == 0 && override_json.count("excludes") == 0 &&
           override_json.count("filter_by") == 0) {
            return Option<bool>(400, "Must contain one of:`includes`, `excludes`, `filter_by`.");
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

            override.remove_matched_tokens = override_json["remove_matched_tokens"].get<bool>();
        }

        if(!id.empty()) {
            override.id = id;
        } else if(override_json.count("id") != 0) {
            override.id = override_json["id"].get<std::string>();
        } else {
            return Option<bool>(400, "Override `id` not provided.");
        }

        override.rule.query = override_json["rule"]["query"].get<std::string>();
        override.rule.match = override_json["rule"]["match"].get<std::string>();

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

        // we have to also detect if it is a dynamic query rule
        size_t i = 0;
        while(i < override.rule.query.size()) {
            if(override.rule.query[i] == '{') {
                // look for closing curly
                i++;
                while(i < override.rule.query.size()) {
                    if(override.rule.query[i] == '}') {
                        override.rule.dynamic_query = true;
                        // remove spaces around curlies
                        override.rule.query = StringUtils::trim_curly_spaces(override.rule.query);
                        break;
                    }
                    i++;
                }
            }
            i++;
        }

        return Option<bool>(true);
    }

    nlohmann::json to_json() const {
        nlohmann::json override;
        override["id"] = id;
        override["rule"]["query"] = rule.query;
        override["rule"]["match"] = rule.match;

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
            override["remove_matched_tokens"] = remove_matched_tokens;
        }

        return override;
    }
};

struct search_args {
    std::vector<query_tokens_t> field_query_tokens;
    std::vector<search_field_t> search_fields;
    std::vector<filter> filters;
    std::vector<facet>& facets;
    std::map<size_t, std::map<size_t, uint32_t>> included_ids;
    std::vector<uint32_t> excluded_ids;
    std::vector<sort_by> sort_fields_std;
    facet_query_t facet_query;
    std::vector<uint32_t> num_typos;
    size_t max_facet_values;
    size_t per_page;
    size_t page;
    token_ordering token_order;
    std::vector<bool> prefixes;
    size_t drop_tokens_threshold;
    size_t typo_tokens_threshold;
    std::vector<std::string> group_by_fields;
    size_t group_limit;
    std::string default_sorting_field;
    bool prioritize_exact_match;
    size_t all_result_ids_len;
    bool exhaustive_search;
    size_t concurrency;
    const std::vector<const override_t*>& filter_overrides;
    size_t search_cutoff_ms;
    size_t min_len_1typo;
    size_t min_len_2typo;

    spp::sparse_hash_set<uint64_t> groups_processed;
    std::vector<std::vector<art_leaf*>> searched_queries;
    Topster* topster;
    Topster* curated_topster;
    std::vector<std::vector<KV*>> raw_result_kvs;
    std::vector<std::vector<KV*>> override_result_kvs;

    search_args(std::vector<query_tokens_t> field_query_tokens,
                std::vector<search_field_t> search_fields, std::vector<filter> filters,
                std::vector<facet>& facets,
                std::map<size_t, std::map<size_t, uint32_t>> included_ids, std::vector<uint32_t> excluded_ids,
                std::vector<sort_by> sort_fields_std, facet_query_t facet_query, const std::vector<uint32_t>& num_typos,
                size_t max_facet_values, size_t max_hits, size_t per_page, size_t page, token_ordering token_order,
                const std::vector<bool>& prefixes,
                size_t drop_tokens_threshold, size_t typo_tokens_threshold,
                const std::vector<std::string>& group_by_fields, size_t group_limit,
                const std::string& default_sorting_field,
                bool prioritize_exact_match,
                bool exhaustive_search,
                size_t concurrency,
                const std::vector<const override_t*>& dynamic_overrides,
                size_t search_cutoff_ms,
                size_t min_len_1typo,
                size_t min_len_2typo):
            field_query_tokens(field_query_tokens),
            search_fields(search_fields), filters(filters), facets(facets),
            included_ids(included_ids), excluded_ids(excluded_ids), sort_fields_std(sort_fields_std),
            facet_query(facet_query), num_typos(num_typos), max_facet_values(max_facet_values), per_page(per_page),
            page(page), token_order(token_order), prefixes(prefixes),
            drop_tokens_threshold(drop_tokens_threshold), typo_tokens_threshold(typo_tokens_threshold),
            group_by_fields(group_by_fields), group_limit(group_limit), default_sorting_field(default_sorting_field),
            prioritize_exact_match(prioritize_exact_match), all_result_ids_len(0),
            exhaustive_search(exhaustive_search), concurrency(concurrency),
            filter_overrides(dynamic_overrides), search_cutoff_ms(search_cutoff_ms),
            min_len_1typo(min_len_1typo), min_len_2typo(min_len_2typo) {

        const size_t topster_size = std::max((size_t)1, max_hits);  // needs to be atleast 1 since scoring is mandatory
        topster = new Topster(topster_size, group_limit);
        curated_topster = new Topster(topster_size, group_limit);
    }

    ~search_args() {
        delete topster;
        delete curated_topster;
    };
};

enum index_operation_t {
    CREATE,
    UPSERT,
    UPDATE,
    EMPLACE,
    DELETE
};

enum class DIRTY_VALUES {
    REJECT = 1,
    DROP = 2,
    COERCE_OR_REJECT = 3,
    COERCE_OR_DROP = 4,
};

struct offsets_facet_hashes_t {
    std::unordered_map<std::string, std::vector<uint32_t>> offsets;
    std::vector<uint64_t> facet_hashes;
};

struct index_record {
    size_t position;                    // position of record in the original request
    uint32_t seq_id;

    nlohmann::json doc;                 // actual document sent in request (could be partial)
    nlohmann::json old_doc;             // previously stored *full* document from disk
    nlohmann::json new_doc;             // new *full* document to be stored into disk
    nlohmann::json del_doc;             // document containing the fields that should be deleted

    index_operation_t operation;
    bool is_update;

    // pre-processed data primed for indexing
    std::unordered_map<std::string, offsets_facet_hashes_t> field_index;
    int64_t points;

    Option<bool> indexed;               // indicates if the indexing operation was a success

    DIRTY_VALUES dirty_values;

    index_record(size_t record_pos, uint32_t seq_id, const nlohmann::json& doc, index_operation_t operation,
                 const DIRTY_VALUES& dirty_values):
            position(record_pos), seq_id(seq_id), doc(doc), operation(operation), is_update(false),
            indexed(false), dirty_values(dirty_values) {

    }

    index_record(index_record&& rhs) = default;

    index_record& operator=(index_record&& mE) = default;

    void index_failure(const uint32_t err_code, const std::string & err_msg) {
        indexed = Option<bool>(err_code, err_msg);
    }

    void index_success() {
        indexed = Option<bool>(true);
    }
};

class Index {
private:
    mutable std::shared_mutex mutex;

    std::string name;

    const uint32_t collection_id;

    const Store* store;

    ThreadPool* thread_pool;

    size_t num_documents;

    std::unordered_map<std::string, field> search_schema;

    std::map<std::string, field> facet_schema;  // std::map guarantees order of fields

    std::unordered_map<std::string, field> sort_schema;

    spp::sparse_hash_map<std::string, art_tree*> search_index;

    spp::sparse_hash_map<std::string, num_tree_t*> numerical_index;

    spp::sparse_hash_map<std::string, spp::sparse_hash_map<std::string, std::vector<uint32_t>>*> geopoint_index;

    // facet_field => (seq_id => values)
    spp::sparse_hash_map<std::string, array_mapped_facet_t> facet_index_v3;

    // sort_field => (seq_id => value)
    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t>*> sort_index;

    // str_sort_field => adi_tree_t
    spp::sparse_hash_map<std::string, adi_tree_t*> str_sort_index;

    // geo_array_field => (seq_id => values) used for exact filtering of geo array records
    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t*>*> geo_array_index;

    // this is used for wildcard queries
    sorted_array seq_ids;

    std::vector<char> symbols_to_index;

    std::vector<char> token_separators;

    StringUtils string_utils;

    // used as sentinels

    static spp::sparse_hash_map<uint32_t, int64_t> text_match_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> seq_id_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> geo_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> str_sentinel_value;

    // Internal utility functions

    static inline uint32_t next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                       long long int n,
                                       std::vector<art_leaf *>& actual_query_suggestion,
                                       std::vector<art_leaf *>& query_suggestion,
                                       uint32_t& token_bits,
                                       uint64& qhash);

    void log_leaves(int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const;

    void do_facets(std::vector<facet> & facets, facet_query_t & facet_query,
                   const std::vector<facet_info_t>& facet_infos,
                   size_t group_limit, const std::vector<std::string>& group_by_fields,
                   const uint32_t* result_ids, size_t results_size) const;

    bool static_filter_query_eval(const override_t* override, std::vector<std::string>& tokens,
                                  std::vector<filter>& filters) const;

    void process_filter_overrides(const std::vector<const override_t*>& filter_overrides,
                                   std::vector<query_tokens_t>& field_query_tokens,
                                   token_ordering token_order,
                                   std::vector<filter>& filters) const;

    bool resolve_override(const std::vector<std::string>& rule_tokens, bool exact_rule_match,
                          const std::vector<std::string>& query_tokens,
                          token_ordering token_order, std::set<std::string>& absorbed_tokens,
                          std::string& filter_by_clause) const;

    bool check_for_overrides(const token_ordering& token_order, const string& field_name, bool slide_window,
                             bool exact_rule_match, std::vector<std::string>& tokens,
                             std::set<std::string>& absorbed_tokens,
                             std::vector<std::string>& field_absorbed_tokens) const;

    static void aggregate_topster(Topster* agg_topster, Topster* index_topster);

    void search_field(const uint8_t & field_id,
                      std::vector<token_t>& query_tokens,
                      std::vector<token_t>& search_tokens,
                      const uint32_t* exclude_token_ids,
                      size_t exclude_token_ids_size,
                      size_t& num_tokens_dropped,
                      const field& the_field, const std::string& field_name,
                      const uint32_t *filter_ids, size_t filter_ids_length,
                      const std::vector<uint32_t>& curated_ids,
                      const std::vector<sort_by> & sort_fields,
                      int num_typos, std::vector<std::vector<art_leaf*>> & searched_queries,
                      Topster* topster, spp::sparse_hash_set<uint64_t>& groups_processed,
                      uint32_t** all_result_ids, size_t & all_result_ids_len,
                      size_t& field_num_results,
                      size_t group_limit,
                      const std::vector<std::string>& group_by_fields,
                      bool prioritize_exact_match,
                      size_t concurrency,
                      std::set<uint64>& query_hashes,
                      token_ordering token_order, const bool prefix,
                      size_t drop_tokens_threshold,
                      size_t typo_tokens_threshold,
                      bool exhaustive_search,
                      size_t min_len_1typo,
                      size_t min_len_2typo) const;

    void search_candidates(const uint8_t & field_id,
                           bool field_is_array,
                           const uint32_t* filter_ids, size_t filter_ids_length,
                           const uint32_t* exclude_token_ids, size_t exclude_token_ids_size,
                           const std::vector<uint32_t>& curated_ids,
                           const std::vector<sort_by> & sort_fields, std::vector<token_candidates> & token_to_candidates,
                           std::vector<std::vector<art_leaf*>> & searched_queries,
                           Topster* topster, spp::sparse_hash_set<uint64_t>& groups_processed,
                           uint32_t** all_result_ids,
                           size_t & all_result_ids_len,
                           size_t& field_num_results,
                           const size_t typo_tokens_threshold,
                           const size_t group_limit, const std::vector<std::string>& group_by_fields,
                           const std::vector<token_t>& query_tokens,
                           bool prioritize_exact_match,
                           bool exhaustive_search,
                           size_t concurrency,
                           std::set<uint64>& query_hashes,
                           std::vector<uint32_t>& id_buff) const;

    void do_filtering(uint32_t*& filter_ids, uint32_t& filter_ids_length, const std::vector<filter>& filters,
                      const bool enable_short_circuit) const;

    void insert_doc(const int64_t score, art_tree *t, uint32_t seq_id,
                    const std::unordered_map<std::string, std::vector<uint32_t>> &token_to_offsets) const;

    static void tokenize_string_with_facets(const std::string& text, bool is_facet, const field& a_field,
                                            const std::vector<char>& symbols_to_index,
                                            const std::vector<char>& token_separators,
                                            std::unordered_map<std::string, std::vector<uint32_t>>& token_to_offsets,
                                            std::vector<uint64_t>& facet_hashes);

    static void tokenize_string_array_with_facets(const std::vector<std::string>& strings, bool is_facet,
                                           const field& a_field,
                                           const std::vector<char>& symbols_to_index,
                                           const std::vector<char>& token_separators,
                                           std::unordered_map<std::string, std::vector<uint32_t>>& token_to_offsets,
                                           std::vector<uint64_t>& facet_hashes);

    void collate_included_ids(const std::vector<std::string>& q_included_tokens,
                              const std::string & field, const uint8_t field_id,
                              const std::map<size_t, std::map<size_t, uint32_t>> & included_ids_map,
                              Topster* curated_topster, std::vector<std::vector<art_leaf*>> & searched_queries) const;

    static uint64_t facet_token_hash(const field & a_field, const std::string &token);

    static void compute_facet_stats(facet &a_facet, uint64_t raw_value, const std::string & field_type);

    static void get_doc_changes(const index_operation_t op, nlohmann::json &update_doc,
                                const nlohmann::json &old_doc, nlohmann::json &new_doc, nlohmann::json &del_doc);

    static Option<uint32_t> coerce_string(const DIRTY_VALUES& dirty_values, const std::string& fallback_field_type,
                                          const field& a_field, nlohmann::json &document,
                                          const std::string &field_name,
                                          nlohmann::json::iterator& array_iter,
                                          bool is_array,
                                          bool& array_ele_erased);

    static Option<uint32_t> coerce_int32_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                           const std::string &field_name,
                                           nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_int64_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                           const std::string &field_name,
                                           nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_float(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                         const std::string &field_name,
                                         nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_bool(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                        const std::string &field_name,
                                        nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_geopoint(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                            const std::string &field_name,
                                            nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    bool common_results_exist(std::vector<art_leaf*>& leaves);

public:
    // for limiting number of results on multiple candidates / query rewrites
    enum {TYPO_TOKENS_THRESHOLD = 1};

    // for limiting number of fields that can be searched on
    enum {FIELD_LIMIT_NUM = 100};

    enum {COMBINATION_MAX_LIMIT = 10000};
    enum {COMBINATION_MIN_LIMIT = 10};

    // If the number of results found is less than this threshold, Typesense will attempt to drop the tokens
    // in the query that have the least individual hits one by one until enough results are found.
    static const int DROP_TOKENS_THRESHOLD = 1;

    Index() = delete;

    Index(const std::string& name,
          const uint32_t collection_id,
          const Store* store,
          ThreadPool* thread_pool,
          const std::unordered_map<std::string, field>& search_schema,
          const std::map<std::string, field>& facet_schema,
          const std::unordered_map<std::string, field>& sort_schema,
          const std::vector<char>& symbols_to_index, const std::vector<char>& token_separators);

    ~Index();

    // reference: https://stackoverflow.com/a/27952689/131050
    static uint64_t hash_combine(uint64_t combined, uint64_t hash) {
        combined ^= hash + 0x517cc1b727220a95 + (combined << 6) + (combined >> 2);
        return combined;
    }

    static void concat_topster_ids(Topster* topster, spp::sparse_hash_map<uint64_t, std::vector<KV*>>& topster_ids);

    void score_results(const std::vector<sort_by> &sort_fields, const uint16_t &query_index, const uint8_t &field_id,
                       bool field_is_array, const uint32_t total_cost,
                       Topster *topster, const std::vector<art_leaf *> &query_suggestion,
                       spp::sparse_hash_set<uint64_t> &groups_processed,
                       const uint32_t seq_id, const int sort_order[3],
                       std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3> field_values,
                       const std::vector<size_t>& geopoint_indices,
                       const size_t group_limit,
                       const std::vector<std::string> &group_by_fields, uint32_t token_bits,
                       bool prioritize_exact_match,
                       bool single_exact_query_token,
                       const std::vector<posting_list_t::iterator_t>& posting_lists) const;

    static int64_t get_points_from_doc(const nlohmann::json &document, const std::string & default_sorting_field);

    const spp::sparse_hash_map<std::string, art_tree *>& _get_search_index() const;

    const spp::sparse_hash_map<std::string, num_tree_t*>& _get_numerical_index() const;

    static int get_bounded_typo_cost(const size_t max_cost, const size_t token_len,
                                     size_t min_len_1typo, size_t min_len_2typo);

    static int64_t float_to_in64_t(float n);

    uint64_t get_distinct_id(const std::vector<std::string>& group_by_fields, const uint32_t seq_id) const;

    static void compute_token_offsets_facets(index_record& record,
                                             const std::unordered_map<std::string, field>& search_schema,
                                             const std::map<std::string, field>& facet_schema,
                                             const std::vector<char>& local_token_separators,
                                             const std::vector<char>& local_symbols_to_index);

    static void scrub_reindex_doc(const std::unordered_map<std::string, field>& search_schema,
                                  nlohmann::json& update_doc, nlohmann::json& del_doc, const nlohmann::json& old_doc);

    static void tokenize_string_field(const nlohmann::json& document,
                                      const field& search_field, std::vector<std::string>& tokens,
                                      const std::string& locale);

    // Public operations

    void run_search(search_args* search_params);

    void search(std::vector<query_tokens_t>& field_query_tokens,
                const std::vector<search_field_t> & search_fields,
                std::vector<filter> & filters, std::vector<facet> & facets,
                facet_query_t & facet_query,
                const std::map<size_t, std::map<size_t, uint32_t>> & included_ids_map,
                const std::vector<uint32_t> & excluded_ids,
                const std::vector<sort_by> & sort_fields_std, const std::vector<uint32_t>& num_typos,
                Topster* topster, Topster* curated_topster,
                const size_t per_page, const size_t page, const token_ordering token_order,
                const std::vector<bool>& prefixes, const size_t drop_tokens_threshold,
                size_t & all_result_ids_len,
                spp::sparse_hash_set<uint64_t>& groups_processed,
                std::vector<std::vector<art_leaf*>> & searched_queries,
                std::vector<std::vector<KV*>> & raw_result_kvs,
                std::vector<std::vector<KV*>> & override_result_kvs,
                const size_t typo_tokens_threshold,
                const size_t group_limit,
                const std::vector<std::string>& group_by_fields,
                const std::vector<const override_t*>& filter_overrides,
                const std::string& default_sorting_field,
                bool prioritize_exact_match,
                bool exhaustive_search,
                size_t concurrency,
                size_t search_cutoff_ms,
                size_t min_len_1typo,
                size_t min_len_2typo) const;

    Option<uint32_t> remove(const uint32_t seq_id, const nlohmann::json & document, const bool is_update);

    static void validate_and_preprocess(Index *index, std::vector<index_record>& iter_batch,
                                          const size_t batch_start_index, const size_t batch_size,
                                          const std::string & default_sorting_field,
                                          const std::unordered_map<std::string, field> & search_schema,
                                          const std::map<std::string, field> & facet_schema,
                                          const std::string& fallback_field_type,
                                          const std::vector<char>& token_separators,
                                          const std::vector<char>& symbols_to_index);

    static size_t batch_memory_index(Index *index,
                                     std::vector<index_record> & iter_batch,
                                     const std::string & default_sorting_field,
                                     const std::unordered_map<std::string, field> & search_schema,
                                     const std::map<std::string, field> & facet_schema,
                                     const std::string& fallback_field_type,
                                     const std::vector<char>& token_separators,
                                     const std::vector<char>& symbols_to_index);

    void index_field_in_memory(const field& afield, std::vector<index_record>& iter_batch);

    template<class T>
    void iterate_and_index_numerical_field(std::vector<index_record>& iter_batch, const field& afield, T func);

    //static bool is_point_in_polygon(const Geofence& poly, const GeoCoord& point);

    //static double transform_for_180th_meridian(Geofence& poly);

    //static void transform_for_180th_meridian(GeoCoord& point, double offset);

    art_leaf* get_token_leaf(const std::string & field_name, const unsigned char* token, uint32_t token_len);

    void do_filtering_with_lock(uint32_t*& filter_ids, uint32_t& filter_ids_length,
                                const std::vector<filter>& filters) const;

    void refresh_schemas(const std::vector<field>& new_fields);

    // the following methods are not synchronized because their parent calls are synchronized or they are const/static

    static Option<uint32_t> validate_index_in_memory(nlohmann::json &document, uint32_t seq_id,
                                                     const std::string & default_sorting_field,
                                                     const std::unordered_map<std::string, field> & search_schema,
                                                     const std::map<std::string, field> & facet_schema,
                                                     const index_operation_t op,
                                                     const std::string& fallback_field_type,
                                                     const DIRTY_VALUES& dirty_values);

    void search_wildcard(const std::vector<std::string>& qtokens, const std::vector<filter>& filters,
                         const std::map<size_t, std::map<size_t, uint32_t>>& included_ids_map,
                         const std::vector<sort_by>& sort_fields_std, Topster* topster, Topster* curated_topster,
                         spp::sparse_hash_set<uint64_t>& groups_processed,
                         std::vector<std::vector<art_leaf*>>& searched_queries, const size_t group_limit,
                         const std::vector<std::string>& group_by_fields, const std::set<uint32_t>& curated_ids,
                         const std::vector<uint32_t>& curated_ids_sorted, const uint32_t* exclude_token_ids,
                         size_t exclude_token_ids_size, const uint8_t field_id, const string& field,
                         uint32_t*& all_result_ids, size_t& all_result_ids_len, const uint32_t* filter_ids,
                         uint32_t filter_ids_length, const size_t concurrency) const;

    void curate_filtered_ids(const std::vector<filter>& filters, const std::set<uint32_t>& curated_ids,
                             const uint32_t* exclude_token_ids, size_t exclude_token_ids_size, uint32_t*& filter_ids,
                             uint32_t& filter_ids_length, const std::vector<uint32_t>& curated_ids_sorted) const;

    void populate_sort_mapping(int* sort_order, std::vector<size_t>& geopoint_indices,
                               const std::vector<sort_by>& sort_fields_std,
                               std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values) const;

    static void remove_matched_tokens(std::vector<std::string>& tokens, const std::set<std::string>& rule_token_set) ;

    void compute_facet_infos(const std::vector<facet>& facets, facet_query_t& facet_query,
                             const uint32_t* all_result_ids, const size_t& all_result_ids_len,
                             const std::vector<std::string>& group_by_fields,
                             std::vector<facet_info_t>& facet_infos) const;

    void resolve_space_as_typos(std::vector<std::string>& qtokens, const std::string& field_name,
                                std::vector<std::vector<std::string>>& resolved_queries);

    size_t num_seq_ids() const;
};

template<class T>
void Index::iterate_and_index_numerical_field(std::vector<index_record>& iter_batch, const field& afield, T func) {
    for(auto& record: iter_batch) {
        if(!record.indexed.ok()) {
            continue;
        }

        const auto& document = record.doc;
        const auto seq_id = record.seq_id;

        if (document.count(afield.name) == 0 || !afield.index) {
            continue;
        }

        try {
            func(record, seq_id);
        } catch(const std::exception &e) {
            LOG(INFO) << "Error while indexing numerical field." << e.what();
            record.index_failure(400, e.what());
        }
    }
}
