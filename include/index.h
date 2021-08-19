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
#include <h3api.h>
#include "string_utils.h"
#include "num_tree.h"
#include "magic_enum.hpp"
#include "match_score.h"
#include "posting_list.h"

struct token_t {
    size_t position;
    std::string value;
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
    std::vector<std::string> q_exclude_tokens;
    std::vector<std::vector<std::string>> q_synonyms;
};

struct search_args {
    std::vector<query_tokens_t> field_query_tokens;
    std::vector<search_field_t> search_fields;
    std::vector<filter> filters;
    std::vector<facet> facets;
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
    size_t combination_limit;
    spp::sparse_hash_set<uint64_t> groups_processed;
    std::vector<std::vector<art_leaf*>> searched_queries;
    Topster* topster;
    Topster* curated_topster;
    std::vector<std::vector<KV*>> raw_result_kvs;
    std::vector<std::vector<KV*>> override_result_kvs;

    search_args() {

    }

    search_args(std::vector<query_tokens_t> field_query_tokens,
                std::vector<search_field_t> search_fields, std::vector<filter> filters,
                std::vector<facet> facets, std::map<size_t, std::map<size_t, uint32_t>> included_ids, std::vector<uint32_t> excluded_ids,
                std::vector<sort_by> sort_fields_std, facet_query_t facet_query, const std::vector<uint32_t>& num_typos,
                size_t max_facet_values, size_t max_hits, size_t per_page, size_t page, token_ordering token_order,
                const std::vector<bool>& prefixes,
                size_t drop_tokens_threshold, size_t typo_tokens_threshold,
                const std::vector<std::string>& group_by_fields, size_t group_limit,
                const std::string& default_sorting_field,
                bool prioritize_exact_match,
                size_t combination_limit):
            field_query_tokens(field_query_tokens),
            search_fields(search_fields), filters(filters), facets(facets),
            included_ids(included_ids), excluded_ids(excluded_ids), sort_fields_std(sort_fields_std),
            facet_query(facet_query), num_typos(num_typos), max_facet_values(max_facet_values), per_page(per_page),
            page(page), token_order(token_order), prefixes(prefixes),
            drop_tokens_threshold(drop_tokens_threshold), typo_tokens_threshold(typo_tokens_threshold),
            group_by_fields(group_by_fields), group_limit(group_limit), default_sorting_field(default_sorting_field),
            prioritize_exact_match(prioritize_exact_match), all_result_ids_len(0),
            combination_limit(combination_limit) {

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
    DELETE
};

enum class DIRTY_VALUES {
    REJECT = 1,
    DROP = 2,
    COERCE_OR_REJECT = 3,
    COERCE_OR_DROP = 4,
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

    Option<bool> indexed;               // indicates if the indexing operation was a success

    const DIRTY_VALUES dirty_values;

    index_record(size_t record_pos, uint32_t seq_id, const nlohmann::json& doc, index_operation_t operation,
                 const DIRTY_VALUES& dirty_values):
            position(record_pos), seq_id(seq_id), doc(doc), operation(operation), is_update(false),
            indexed(false), dirty_values(dirty_values) {

    }

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

    const uint64_t FACET_ARRAY_DELIMETER = std::numeric_limits<uint64_t>::max();

    std::string name;

    size_t num_documents;

    std::unordered_map<std::string, field> search_schema;

    std::map<std::string, field> facet_schema;  // std::map guarantees order of fields

    std::unordered_map<std::string, field> sort_schema;

    spp::sparse_hash_map<std::string, art_tree*> search_index;

    spp::sparse_hash_map<std::string, num_tree_t*> numerical_index;

    spp::sparse_hash_map<std::string, spp::sparse_hash_map<std::string, std::vector<uint32_t>>*> geopoint_index;

    // facet_field => (seq_id => values)
    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, facet_hash_values_t>*> facet_index_v3;

    // sort_field => (seq_id => value)
    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t>*> sort_index;

    // this is used for wildcard queries
    sorted_array seq_ids;

    StringUtils string_utils;

    // used as sentinels

    static spp::sparse_hash_map<uint32_t, int64_t> text_match_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> seq_id_sentinel_value;

    // Internal utility functions

    static inline uint32_t next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                       long long int n,
                                       std::vector<art_leaf *>& actual_query_suggestion,
                                       std::vector<art_leaf *>& query_suggestion,
                                       uint32_t& token_bits);

    void log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const;

    void do_facets(std::vector<facet> & facets, facet_query_t & facet_query,
                   size_t group_limit, const std::vector<std::string>& group_by_fields,
                   const uint32_t* result_ids, size_t results_size) const;

    void search_field(const uint8_t & field_id,
                      std::vector<token_t>& query_tokens,
                      std::vector<token_t>& search_tokens,
                      const uint32_t* exclude_token_ids,
                      size_t exclude_token_ids_size,
                      size_t& num_tokens_dropped,
                      const std::string & field, uint32_t *filter_ids, size_t filter_ids_length,
                      const std::vector<uint32_t>& curated_ids,
                      std::vector<facet> & facets, const std::vector<sort_by> & sort_fields,
                      const int num_typos, std::vector<std::vector<art_leaf*>> & searched_queries,
                      Topster* topster, spp::sparse_hash_set<uint64_t>& groups_processed,
                      uint32_t** all_result_ids, size_t & all_result_ids_len,
                      size_t& field_num_results,
                      const size_t group_limit,
                      const std::vector<std::string>& group_by_fields,
                      bool prioritize_exact_match,
                      const token_ordering token_order = FREQUENCY, const bool prefix = false,
                      const size_t drop_tokens_threshold = Index::DROP_TOKENS_THRESHOLD,
                      const size_t typo_tokens_threshold = Index::TYPO_TOKENS_THRESHOLD,
                      const size_t combination_limit = Index::COMBINATION_LIMIT) const;

    void search_candidates(const uint8_t & field_id,
                           bool field_is_array,
                           uint32_t* filter_ids, size_t filter_ids_length,
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
                           size_t combination_limit) const;

    void insert_doc(const int64_t score, art_tree *t, uint32_t seq_id,
                    const std::unordered_map<std::string, std::vector<uint32_t>> &token_to_offsets) const;

    void index_string_field(const std::string & text, const int64_t score, art_tree *t, uint32_t seq_id,
                            bool is_facet, const field & a_field);

    void index_string_array_field(const std::vector<std::string> & strings, const int64_t score, art_tree *t,
                                  uint32_t seq_id, bool is_facet, const field & a_field);

    void collate_included_ids(const std::vector<std::string>& q_included_tokens,
                              const std::string & field, const uint8_t field_id,
                              const std::map<size_t, std::map<size_t, uint32_t>> & included_ids_map,
                              Topster* curated_topster, std::vector<std::vector<art_leaf*>> & searched_queries) const;

    static uint64_t facet_token_hash(const field & a_field, const std::string &token);

    static void compute_facet_stats(facet &a_facet, uint64_t raw_value, const std::string & field_type);

    static void get_doc_changes(const index_operation_t op, const nlohmann::json &update_doc,
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

public:
    // for limiting number of results on multiple candidates / query rewrites
    enum {TYPO_TOKENS_THRESHOLD = 1};

    // for limiting number of fields that can be searched on
    enum {FIELD_LIMIT_NUM = 100};

    enum {COMBINATION_LIMIT = 10};

    // If the number of results found is less than this threshold, Typesense will attempt to drop the tokens
    // in the query that have the least individual hits one by one until enough results are found.
    static const int DROP_TOKENS_THRESHOLD = 1;

    Index() = delete;

    Index(const std::string name, const std::unordered_map<std::string, field> & search_schema,
          std::map<std::string, field> facet_schema, std::unordered_map<std::string, field> sort_schema);

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
                       const std::vector<token_t> &query_tokens,
                       bool prioritize_exact_match,
                       std::vector<posting_list_t::iterator_t>& posting_lists) const;

    static int64_t get_points_from_doc(const nlohmann::json &document, const std::string & default_sorting_field);

    const spp::sparse_hash_map<std::string, art_tree *>& _get_search_index() const;

    const spp::sparse_hash_map<std::string, num_tree_t*>& _get_numerical_index() const;

    static int get_bounded_typo_cost(const size_t max_cost, const size_t token_len);

    static int64_t float_to_in64_t(float n);

    uint64_t get_distinct_id(const std::vector<std::string>& group_by_fields, const uint32_t seq_id) const;

    void scrub_reindex_doc(nlohmann::json& update_doc, nlohmann::json& del_doc, const nlohmann::json& old_doc);

    static void tokenize_string_field(const nlohmann::json& document,
                                      const field& search_field, std::vector<std::string>& tokens,
                                      const std::string& locale);

    // Public operations

    void run_search(search_args* search_params);

    void search(const std::vector<query_tokens_t>& field_query_tokens,
                const std::vector<search_field_t> & search_fields,
                const std::vector<filter> & filters, std::vector<facet> & facets,
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
                const std::string& default_sorting_field,
                bool prioritize_exact_match,
                const size_t combination_limit) const;

    Option<uint32_t> remove(const uint32_t seq_id, const nlohmann::json & document, const bool is_update);

    Option<uint32_t> index_in_memory(const nlohmann::json & document, uint32_t seq_id,
                                     const std::string & default_sorting_field,
                                     const bool is_update);

    static size_t batch_memory_index(Index *index,
                                     std::vector<index_record> & iter_batch,
                                     const std::string & default_sorting_field,
                                     const std::unordered_map<std::string, field> & search_schema,
                                     const std::map<std::string, field> & facet_schema,
                                     const std::string& fallback_field_type);

    static bool is_point_in_polygon(const Geofence& poly, const GeoCoord& point);

    static double transform_for_180th_meridian(Geofence& poly);

    static void transform_for_180th_meridian(GeoCoord& point, double offset);

    art_leaf* get_token_leaf(const std::string & field_name, const unsigned char* token, uint32_t token_len);

    // the following methods are not synchronized because their parent calls are synchronized

    uint32_t do_filtering(uint32_t** filter_ids_out, const std::vector<filter> & filters) const;

    static Option<uint32_t> validate_index_in_memory(nlohmann::json &document, uint32_t seq_id,
                                                     const std::string & default_sorting_field,
                                                     const std::unordered_map<std::string, field> & search_schema,
                                                     const std::map<std::string, field> & facet_schema,
                                                     const index_operation_t op,
                                                     const std::string& fallback_field_type,
                                                     const DIRTY_VALUES& dirty_values);

    void refresh_schemas(const std::vector<field>& new_fields);

};

