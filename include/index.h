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
#include "tsl/htrie_set.h"
#include <tsl/htrie_map.h>
#include "id_list.h"
#include "synonym_index.h"
#include "override.h"
#include "vector_query_ops.h"
#include "hnswlib/hnswlib.h"

static constexpr size_t ARRAY_FACET_DIM = 4;
using facet_map_t = spp::sparse_hash_map<uint32_t, facet_hash_values_t>;
using array_mapped_facet_t = std::array<facet_map_t*, ARRAY_FACET_DIM>;

static constexpr size_t ARRAY_INFIX_DIM = 4;
using array_mapped_infix_t = std::vector<tsl::htrie_set<char>*>;

struct token_t {
    size_t position;
    std::string value;

    bool is_prefix_searched;
    uint32_t root_len;        // if prefix searched, length of the root token
    uint32_t num_typos;

    token_t() {};

    token_t(size_t position, const std::string& value, bool is_prefix_searched, uint32_t root_len, uint32_t num_typos):
            position(position), value(value), is_prefix_searched(is_prefix_searched),
            root_len(root_len), num_typos(num_typos) {

    }
};

// FIXME: deprecated
struct token_candidates {
    token_t token;
    size_t cost;
    bool prefix_search;
    std::vector<art_leaf*> candidates;
};

struct tok_candidates {
    token_t token;
    size_t cost;
    bool prefix_search;
    std::vector<std::string> candidates;
};

struct search_field_t {
    std::string name;
    size_t weight;
    size_t orig_index;
};

struct query_tokens_t {
    std::vector<token_t> q_include_tokens;
    std::vector<std::vector<std::string>> q_exclude_tokens;
    std::vector<std::vector<std::string>> q_phrases;
    std::vector<std::vector<std::string>> q_synonyms;
};

enum enable_t {
    always,
    fallback,
    off
};

enum text_match_type_t {
    max_score,
    max_weight
};

struct search_args {
    std::vector<query_tokens_t> field_query_tokens;
    std::vector<search_field_t> search_fields;
    const text_match_type_t match_type;
    const filter_node_t* filter_tree_root;
    std::vector<facet>& facets;
    std::vector<std::pair<uint32_t, uint32_t>>& included_ids;
    std::vector<uint32_t> excluded_ids;
    std::vector<sort_by>& sort_fields_std;
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
    bool prioritize_token_position;
    size_t all_result_ids_len;
    bool exhaustive_search;
    size_t concurrency;
    size_t search_cutoff_ms;
    size_t min_len_1typo;
    size_t min_len_2typo;
    size_t max_candidates;
    std::vector<enable_t> infixes;
    const size_t max_extra_prefix;
    const size_t max_extra_suffix;
    const size_t facet_query_num_typos;
    const bool filter_curated_hits;
    const enable_t split_join_tokens;
    tsl::htrie_map<char, token_leaf> qtoken_set;

    spp::sparse_hash_set<uint64_t> groups_processed;
    std::vector<std::vector<art_leaf*>> searched_queries;
    Topster* topster;
    Topster* curated_topster;
    std::vector<std::vector<KV*>> raw_result_kvs;
    std::vector<std::vector<KV*>> override_result_kvs;

    vector_query_t& vector_query;

    search_args(std::vector<query_tokens_t> field_query_tokens, std::vector<search_field_t> search_fields,
                const text_match_type_t match_type,
                filter_node_t* filter_tree_root, std::vector<facet>& facets,
                std::vector<std::pair<uint32_t, uint32_t>>& included_ids, std::vector<uint32_t> excluded_ids,
                std::vector<sort_by>& sort_fields_std, facet_query_t facet_query, const std::vector<uint32_t>& num_typos,
                size_t max_facet_values, size_t max_hits, size_t per_page, size_t page, token_ordering token_order,
                const std::vector<bool>& prefixes, size_t drop_tokens_threshold, size_t typo_tokens_threshold,
                const std::vector<std::string>& group_by_fields, size_t group_limit,
                const string& default_sorting_field, bool prioritize_exact_match,
                const bool prioritize_token_position, bool exhaustive_search,
                size_t concurrency, size_t search_cutoff_ms,
                size_t min_len_1typo, size_t min_len_2typo, size_t max_candidates, const std::vector<enable_t>& infixes,
                const size_t max_extra_prefix, const size_t max_extra_suffix, const size_t facet_query_num_typos,
                const bool filter_curated_hits, const enable_t split_join_tokens, vector_query_t& vector_query) :
            field_query_tokens(field_query_tokens),
            search_fields(search_fields), match_type(match_type), filter_tree_root(filter_tree_root), facets(facets),
            included_ids(included_ids), excluded_ids(excluded_ids), sort_fields_std(sort_fields_std),
            facet_query(facet_query), num_typos(num_typos), max_facet_values(max_facet_values), per_page(per_page),
            page(page), token_order(token_order), prefixes(prefixes),
            drop_tokens_threshold(drop_tokens_threshold), typo_tokens_threshold(typo_tokens_threshold),
            group_by_fields(group_by_fields), group_limit(group_limit), default_sorting_field(default_sorting_field),
            prioritize_exact_match(prioritize_exact_match), prioritize_token_position(prioritize_token_position),
            all_result_ids_len(0), exhaustive_search(exhaustive_search), concurrency(concurrency),
            search_cutoff_ms(search_cutoff_ms),
            min_len_1typo(min_len_1typo), min_len_2typo(min_len_2typo), max_candidates(max_candidates),
            infixes(infixes), max_extra_prefix(max_extra_prefix), max_extra_suffix(max_extra_suffix),
            facet_query_num_typos(facet_query_num_typos), filter_curated_hits(filter_curated_hits),
            split_join_tokens(split_join_tokens), vector_query(vector_query) {

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

class VectorFilterFunctor: public hnswlib::FilterFunctor {
    const uint32_t* filter_ids = nullptr;
    const uint32_t filter_ids_length = 0;

public:
    explicit VectorFilterFunctor(const uint32_t* filter_ids, const uint32_t filter_ids_length) :
            filter_ids(filter_ids), filter_ids_length(filter_ids_length) {}

    bool operator()(unsigned int id) {
        if(filter_ids_length == 0) {
            return true;
        }

        return std::binary_search(filter_ids, filter_ids + filter_ids_length, id);
    }
};

struct hnsw_index_t {
    hnswlib::InnerProductSpace* space;
    hnswlib::HierarchicalNSW<float, VectorFilterFunctor>* vecdex;
    size_t num_dim;
    vector_distance_type_t distance_type;

    hnsw_index_t(size_t num_dim, size_t init_size, vector_distance_type_t distance_type):
        space(new hnswlib::InnerProductSpace(num_dim)),
        vecdex(new hnswlib::HierarchicalNSW<float, VectorFilterFunctor>(space, init_size, 16, 200, 100, true)),
        num_dim(num_dim), distance_type(distance_type) {

    }

    ~hnsw_index_t() {
        delete vecdex;
        delete space;
    }

    // needed for cosine similarity
    static void normalize_vector(const std::vector<float>& src, std::vector<float>& norm_dest) {
        float norm = 0.0f;
        for (float i : src) {
            norm += i * i;
        }
        norm = 1.0f / (sqrtf(norm) + 1e-30f);
        for (size_t i = 0; i < src.size(); i++) {
            norm_dest[i] = src[i] * norm;
        }
    }
};

class Index {
private:
    mutable std::shared_mutex mutex;

    std::string name;

    const uint32_t collection_id;

    const Store* store;

    const SynonymIndex* synonym_index;

    ThreadPool* thread_pool;

    size_t num_documents;

    tsl::htrie_map<char, field> search_schema;

    spp::sparse_hash_map<std::string, art_tree*> search_index;

    spp::sparse_hash_map<std::string, num_tree_t*> numerical_index;

    spp::sparse_hash_map<std::string, spp::sparse_hash_map<std::string, std::vector<uint32_t>>*> geopoint_index;

    // geo_array_field => (seq_id => values) used for exact filtering of geo array records
    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t*>*> geo_array_index;

    // facet_field => (seq_id => values)
    spp::sparse_hash_map<std::string, array_mapped_facet_t> facet_index_v3;

    // sort_field => (seq_id => value)
    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t>*> sort_index;

    // str_sort_field => adi_tree_t
    spp::sparse_hash_map<std::string, adi_tree_t*> str_sort_index;

    // infix field => value
    spp::sparse_hash_map<std::string, array_mapped_infix_t> infix_index;

    // vector field => vector index
    spp::sparse_hash_map<std::string, hnsw_index_t*> vector_index;

    // this is used for wildcard queries
    id_list_t* seq_ids;

    std::vector<char> symbols_to_index;

    std::vector<char> token_separators;

    StringUtils string_utils;

    // used as sentinels

    static spp::sparse_hash_map<uint32_t, int64_t> text_match_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> seq_id_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> eval_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> geo_sentinel_value;
    static spp::sparse_hash_map<uint32_t, int64_t> str_sentinel_value;

    // Internal utility functions

    static inline uint32_t next_suggestion2(const std::vector<tok_candidates>& token_candidates_vec,
                                            long long int n,
                                            std::vector<token_t>& query_suggestion,
                                            uint64& qhash);

    static inline uint32_t next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                       long long int n,
                                       std::vector<art_leaf *>& actual_query_suggestion,
                                       std::vector<art_leaf *>& query_suggestion,
                                       int syn_orig_num_tokens,
                                       uint32_t& token_bits,
                                       uint64& qhash);

    void log_leaves(int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const;

    void do_facets(std::vector<facet> & facets, facet_query_t & facet_query,
                   const std::vector<facet_info_t>& facet_infos,
                   size_t group_limit, const std::vector<std::string>& group_by_fields,
                   const uint32_t* result_ids, size_t results_size) const;

    bool static_filter_query_eval(const override_t* override, std::vector<std::string>& tokens,
                                  filter_node_t*& filter_tree_root) const;

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
                      const std::vector<token_t>& query_tokens,
                      const uint32_t* exclude_token_ids,
                      size_t exclude_token_ids_size,
                      size_t& num_tokens_dropped,
                      const field& the_field, const std::string& field_name,
                      const uint32_t *filter_ids, size_t filter_ids_length,
                      const std::vector<uint32_t>& curated_ids,
                      std::vector<sort_by> & sort_fields,
                      int last_typo,
                      int max_typos,
                      std::vector<std::vector<art_leaf*>> & searched_queries,
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
                      int syn_orig_num_tokens,
                      size_t min_len_1typo,
                      size_t min_len_2typo,
                      size_t max_candidates) const;

    void search_all_candidates(const size_t num_search_fields,
                               const text_match_type_t match_type,
                               const std::vector<search_field_t>& the_fields,
                               const uint32_t* filter_ids, size_t filter_ids_length,
                               const uint32_t* exclude_token_ids, size_t exclude_token_ids_size,
                               const std::vector<sort_by>& sort_fields,
                               std::vector<tok_candidates>& token_candidates_vec,
                               std::vector<std::vector<art_leaf*>>& searched_queries,
                               tsl::htrie_map<char, token_leaf>& qtoken_set,
                               Topster* topster,
                               spp::sparse_hash_set<uint64_t>& groups_processed,
                               uint32_t*& all_result_ids, size_t& all_result_ids_len,
                               const size_t typo_tokens_threshold,
                               const size_t group_limit,
                               const std::vector<std::string>& group_by_fields,
                               const std::vector<token_t>& query_tokens,
                               const std::vector<uint32_t>& num_typos,
                               const std::vector<bool>& prefixes,
                               bool prioritize_exact_match,
                               const bool prioritize_token_position,
                               const bool exhaustive_search,
                               const size_t max_candidates,
                               int syn_orig_num_tokens,
                               const int* sort_order,
                               std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values,
                               const std::vector<size_t>& geopoint_indices,
                               std::set<uint64>& query_hashes,
                               std::vector<uint32_t>& id_buff) const;

    void search_candidates(const uint8_t & field_id,
                           bool field_is_array,
                           const uint32_t* filter_ids, size_t filter_ids_length,
                           const uint32_t* exclude_token_ids, size_t exclude_token_ids_size,
                           const std::vector<uint32_t>& curated_ids,
                           std::vector<sort_by> & sort_fields, std::vector<token_candidates> & token_to_candidates,
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
                           int syn_orig_num_tokens,
                           size_t concurrency,
                           std::set<uint64>& query_hashes,
                           std::vector<uint32_t>& id_buff) const;

    static void popular_fields_of_token(const spp::sparse_hash_map<std::string, art_tree*>& search_index,
                                        const std::string& previous_token,
                                        const std::vector<search_field_t>& the_fields,
                                        const size_t num_search_fields,
                                        std::vector<size_t>& popular_field_ids);

    void do_filtering(uint32_t*& filter_ids,
                      uint32_t& filter_ids_length,
                      filter_node_t const* const root) const;

    void recursive_filter(uint32_t*& filter_ids,
                          uint32_t& filter_ids_length,
                          filter_node_t const* const root,
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

    void collate_included_ids(const std::vector<token_t>& q_included_tokens,
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

    bool common_results_exist(std::vector<art_leaf*>& leaves, bool must_match_phrase) const;

    static void remove_facet_token(const field& search_field, spp::sparse_hash_map<std::string, art_tree*>& search_index,
                                   const std::string& token, uint32_t seq_id);

public:
    // for limiting number of results on multiple candidates / query rewrites
    enum {TYPO_TOKENS_THRESHOLD = 1};

    // for limiting number of fields that can be searched on
    enum {FIELD_LIMIT_NUM = 100};

    // Values 0 to 15 are allowed
    enum {FIELD_MAX_WEIGHT = 15};

    enum {COMBINATION_MAX_LIMIT = 10000};
    enum {COMBINATION_MIN_LIMIT = 10};

    enum {NUM_CANDIDATES_DEFAULT_MIN = 4};
    enum {NUM_CANDIDATES_DEFAULT_MAX = 10};

    // If the number of results found is less than this threshold, Typesense will attempt to drop the tokens
    // in the query that have the least individual hits one by one until enough results are found.
    static const int DROP_TOKENS_THRESHOLD = 1;

    Index() = delete;

    Index(const std::string& name,
          const uint32_t collection_id,
          const Store* store,
          SynonymIndex* synonym_index,
          ThreadPool* thread_pool,
          const tsl::htrie_map<char, field>& search_schema,
          const std::vector<char>& symbols_to_index,
          const std::vector<char>& token_separators);

    ~Index();

    static void concat_topster_ids(Topster* topster, spp::sparse_hash_map<uint64_t, std::vector<KV*>>& topster_ids);

    int64_t score_results2(const std::vector<sort_by> & sort_fields, const uint16_t & query_index,
                           const size_t field_id, const bool field_is_array, const uint32_t total_cost,
                           int64_t& match_score,
                           const uint32_t seq_id, const int sort_order[3],
                           const bool prioritize_exact_match,
                           const bool single_exact_query_token,
                           const bool prioritize_token_position,
                           size_t num_query_tokens,
                           int syn_orig_num_tokens,
                           const std::vector<posting_list_t::iterator_t>& posting_lists) const;

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
                       int syn_orig_num_tokens,
                       const std::vector<posting_list_t::iterator_t>& posting_lists) const;

    static int64_t get_points_from_doc(const nlohmann::json &document, const std::string & default_sorting_field);

    const spp::sparse_hash_map<std::string, art_tree *>& _get_search_index() const;

    const spp::sparse_hash_map<std::string, num_tree_t*>& _get_numerical_index() const;

    const spp::sparse_hash_map<std::string, array_mapped_infix_t>& _get_infix_index() const;

    const spp::sparse_hash_map<std::string, hnsw_index_t*>& _get_vector_index() const;

    static int get_bounded_typo_cost(const size_t max_cost, const size_t token_len,
                                     size_t min_len_1typo, size_t min_len_2typo);

    static int64_t float_to_int64_t(float n);

    static float int64_t_to_float(int64_t n);

    uint64_t get_distinct_id(const std::vector<std::string>& group_by_fields, const uint32_t seq_id) const;

    static void compute_token_offsets_facets(index_record& record,
                                             const tsl::htrie_map<char, field>& search_schema,
                                             const std::vector<char>& local_token_separators,
                                             const std::vector<char>& local_symbols_to_index);

    static void scrub_reindex_doc(const tsl::htrie_map<char, field>& search_schema,
                                  nlohmann::json& update_doc, nlohmann::json& del_doc, const nlohmann::json& old_doc);

    static void tokenize_string_field(const nlohmann::json& document,
                                      const field& search_field, std::vector<std::string>& tokens,
                                      const std::string& locale,
                                      const std::vector<char>& symbols_to_index,
                                      const std::vector<char>& token_separators);

    // Public operations

    void run_search(search_args* search_params);

    void search(std::vector<query_tokens_t>& field_query_tokens, const std::vector<search_field_t>& the_fields,
                const text_match_type_t match_type,
                filter_node_t const* const& filter_tree_root, std::vector<facet>& facets, facet_query_t& facet_query,
                const std::vector<std::pair<uint32_t, uint32_t>>& included_ids,
                const std::vector<uint32_t>& excluded_ids, std::vector<sort_by>& sort_fields_std,
                const std::vector<uint32_t>& num_typos, Topster* topster, Topster* curated_topster,
                const size_t per_page,
                const size_t page, const token_ordering token_order, const std::vector<bool>& prefixes,
                const size_t drop_tokens_threshold, size_t& all_result_ids_len,
                spp::sparse_hash_set<uint64_t>& groups_processed,
                std::vector<std::vector<art_leaf*>>& searched_queries,
                tsl::htrie_map<char, token_leaf>& qtoken_set,
                std::vector<std::vector<KV*>>& raw_result_kvs, std::vector<std::vector<KV*>>& override_result_kvs,
                const size_t typo_tokens_threshold, const size_t group_limit,
                const std::vector<std::string>& group_by_fields,
                const string& default_sorting_field, bool prioritize_exact_match,
                const bool prioritize_token_position, bool exhaustive_search,
                size_t concurrency, size_t search_cutoff_ms, size_t min_len_1typo, size_t min_len_2typo,
                size_t max_candidates, const std::vector<enable_t>& infixes, const size_t max_extra_prefix,
                const size_t max_extra_suffix, const size_t facet_query_num_typos,
                const bool filter_curated_hits, enable_t split_join_tokens,
                const vector_query_t& vector_query) const;

    void remove_field(uint32_t seq_id, const nlohmann::json& document, const std::string& field_name);

    Option<uint32_t> remove(const uint32_t seq_id, const nlohmann::json & document,
                            const std::vector<field>& del_fields, const bool is_update);

    static void validate_and_preprocess(Index *index, std::vector<index_record>& iter_batch,
                                          const size_t batch_start_index, const size_t batch_size,
                                          const std::string & default_sorting_field,
                                          const tsl::htrie_map<char, field> & search_schema,
                                          const std::string& fallback_field_type,
                                          const std::vector<char>& token_separators,
                                          const std::vector<char>& symbols_to_index,
                                          const bool do_validation);

    static size_t batch_memory_index(Index *index,
                                     std::vector<index_record>& iter_batch,
                                     const std::string& default_sorting_field,
                                     const tsl::htrie_map<char, field>& search_schema,
                                     const std::string& fallback_field_type,
                                     const std::vector<char>& token_separators,
                                     const std::vector<char>& symbols_to_index,
                                     const bool do_validation);

    void index_field_in_memory(const field& afield, std::vector<index_record>& iter_batch);

    template<class T>
    void iterate_and_index_numerical_field(std::vector<index_record>& iter_batch, const field& afield, T func);

    //static bool is_point_in_polygon(const Geofence& poly, const GeoCoord& point);

    //static double transform_for_180th_meridian(Geofence& poly);

    //static void transform_for_180th_meridian(GeoCoord& point, double offset);

    art_leaf* get_token_leaf(const std::string & field_name, const unsigned char* token, uint32_t token_len);

    void do_filtering_with_lock(
            uint32_t*& filter_ids,
            uint32_t& filter_ids_length,
            filter_node_t const* const& filter_tree_root) const;

    void refresh_schemas(const std::vector<field>& new_fields, const std::vector<field>& del_fields);

    // the following methods are not synchronized because their parent calls are synchronized or they are const/static

    static Option<uint32_t> validate_index_in_memory(nlohmann::json &document, uint32_t seq_id,
                                                     const std::string & default_sorting_field,
                                                     const tsl::htrie_map<char, field> & search_schema,
                                                     const index_operation_t op,
                                                     const std::string& fallback_field_type,
                                                     const DIRTY_VALUES& dirty_values);

    void search_wildcard(filter_node_t const* const& filter_tree_root,
                         const std::map<size_t, std::map<size_t, uint32_t>>& included_ids_map,
                         const std::vector<sort_by>& sort_fields, Topster* topster, Topster* curated_topster,
                         spp::sparse_hash_set<uint64_t>& groups_processed,
                         std::vector<std::vector<art_leaf*>>& searched_queries, const size_t group_limit,
                         const std::vector<std::string>& group_by_fields, const std::set<uint32_t>& curated_ids,
                         const std::vector<uint32_t>& curated_ids_sorted, const uint32_t* exclude_token_ids,
                         size_t exclude_token_ids_size,
                         uint32_t*& all_result_ids, size_t& all_result_ids_len, const uint32_t* filter_ids,
                         uint32_t filter_ids_length, const size_t concurrency,
                         const int* sort_order,
                         std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values,
                         const std::vector<size_t>& geopoint_indices) const;

    void search_infix(const std::string& query, const std::string& field_name, std::vector<uint32_t>& ids,
                      size_t max_extra_prefix, size_t max_extra_suffix) const;

    void curate_filtered_ids(filter_node_t const* const& filter_tree_root, const std::set<uint32_t>& curated_ids,
                             const uint32_t* exclude_token_ids, size_t exclude_token_ids_size, uint32_t*& filter_ids,
                             uint32_t& filter_ids_length, const std::vector<uint32_t>& curated_ids_sorted) const;

    void populate_sort_mapping(int* sort_order, std::vector<size_t>& geopoint_indices,
                               std::vector<sort_by>& sort_fields_std,
                               std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values) const;

    static void remove_matched_tokens(std::vector<std::string>& tokens, const std::set<std::string>& rule_token_set) ;

    void compute_facet_infos(const std::vector<facet>& facets, facet_query_t& facet_query,
                             const size_t facet_query_num_typos,
                             const uint32_t* all_result_ids, const size_t& all_result_ids_len,
                             const std::vector<std::string>& group_by_fields,
                             size_t max_candidates,
                             std::vector<facet_info_t>& facet_infos) const;

    void resolve_space_as_typos(std::vector<std::string>& qtokens, const std::string& field_name,
                                std::vector<std::vector<std::string>>& resolved_queries) const;

    size_t num_seq_ids() const;

    void handle_exclusion(const size_t num_search_fields, std::vector<query_tokens_t>& field_query_tokens,
                          const std::vector<search_field_t>& search_fields, uint32_t*& exclude_token_ids,
                          size_t& exclude_token_ids_size) const;

    void do_infix_search(const size_t num_search_fields, const std::vector<search_field_t>& the_fields,
                         const std::vector<enable_t>& infixes,
                         const std::vector<sort_by>& sort_fields,
                         std::vector<std::vector<art_leaf*>>& searched_queries, const size_t group_limit,
                         const std::vector<std::string>& group_by_fields, const size_t max_extra_prefix,
                         const size_t max_extra_suffix, const std::vector<token_t>& query_tokens, Topster* actual_topster,
                         const uint32_t *filter_ids, size_t filter_ids_length,
                         const int sort_order[3],
                         std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3> field_values,
                         const std::vector<size_t>& geopoint_indices,
                         const std::vector<uint32_t>& curated_ids_sorted,
                         uint32_t*& all_result_ids, size_t& all_result_ids_len,
                         spp::sparse_hash_set<uint64_t>& groups_processed) const;

    void do_synonym_search(const std::vector<search_field_t>& the_fields,
                           const text_match_type_t match_type,
                           filter_node_t const* const& filter_tree_root,
                           const std::map<size_t, std::map<size_t, uint32_t>>& included_ids_map,
                           const std::vector<sort_by>& sort_fields_std, Topster* curated_topster,
                           const token_ordering& token_order,
                           const size_t typo_tokens_threshold, const size_t group_limit,
                           const std::vector<std::string>& group_by_fields, bool prioritize_exact_match,
                           const bool prioritize_token_position,
                           const bool exhaustive_search, const size_t concurrency,
                           const std::vector<bool>& prefixes,
                           size_t min_len_1typo,
                           size_t min_len_2typo, const size_t max_candidates, const std::set<uint32_t>& curated_ids,
                           const std::vector<uint32_t>& curated_ids_sorted, const uint32_t* exclude_token_ids,
                           size_t exclude_token_ids_size,
                           Topster* actual_topster,
                           std::vector<std::vector<token_t>>& q_pos_synonyms,
                           int syn_orig_num_tokens,
                           spp::sparse_hash_set<uint64_t>& groups_processed,
                           std::vector<std::vector<art_leaf*>>& searched_queries,
                           uint32_t*& all_result_ids, size_t& all_result_ids_len,
                           const uint32_t* filter_ids, uint32_t filter_ids_length, 
                           std::set<uint64>& query_hashes,
                           const int* sort_order,
                           std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values,
                           const std::vector<size_t>& geopoint_indices,
                           tsl::htrie_map<char, token_leaf>& qtoken_set) const;

    void do_phrase_search(const size_t num_search_fields, const std::vector<search_field_t>& search_fields,
                          std::vector<query_tokens_t>& field_query_tokens,
                          uint32_t*& filter_ids, uint32_t& filter_ids_length) const;

    void fuzzy_search_fields(const std::vector<search_field_t>& the_fields,
                             const std::vector<token_t>& query_tokens,
                             const text_match_type_t match_type,
                             const bool dropped_tokens,
                             const uint32_t* exclude_token_ids,
                             size_t exclude_token_ids_size,
                             const uint32_t* filter_ids, size_t filter_ids_length,
                             const std::vector<uint32_t>& curated_ids,
                             const std::vector<sort_by>& sort_fields,
                             const std::vector<uint32_t>& num_typos,
                             std::vector<std::vector<art_leaf*>>& searched_queries,
                             tsl::htrie_map<char, token_leaf>& qtoken_set,
                             Topster* topster, spp::sparse_hash_set<uint64_t>& groups_processed,
                             uint32_t*& all_result_ids, size_t& all_result_ids_len,
                             const size_t group_limit, const std::vector<std::string>& group_by_fields,
                             bool prioritize_exact_match,
                             const bool prioritize_token_position,
                             std::set<uint64>& query_hashes,
                             const token_ordering token_order,
                             const std::vector<bool>& prefixes,
                             const size_t typo_tokens_threshold,
                             const bool exhaustive_search,
                             const size_t max_candidates,
                             size_t min_len_1typo,
                             size_t min_len_2typo,
                             int syn_orig_num_tokens,
                             const int* sort_order,
                             std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values,
                             const std::vector<size_t>& geopoint_indices) const;

    void find_across_fields(const token_t& previous_token,
                            const std::string& previous_token_str,
                            const std::vector<search_field_t>& the_fields,
                            const size_t num_search_fields,
                            const uint32_t* filter_ids, uint32_t filter_ids_length,
                            const uint32_t* exclude_token_ids,
                            size_t exclude_token_ids_size,
                            std::vector<uint32_t>& prev_token_doc_ids,
                            std::vector<size_t>& top_prefix_field_ids) const;

    void search_across_fields(const std::vector<token_t>& query_tokens,
                              const std::vector<uint32_t>& num_typos,
                              const std::vector<bool>& prefixes,
                              const std::vector<search_field_t>& the_fields,
                              const size_t num_search_fields,
                              const text_match_type_t match_type,
                              const std::vector<sort_by>& sort_fields,
                              Topster* topster,
                              spp::sparse_hash_set<uint64_t>& groups_processed,
                              std::vector<std::vector<art_leaf*>>& searched_queries,
                              tsl::htrie_map<char, token_leaf>& qtoken_set,
                              const size_t group_limit,
                              const std::vector<std::string>& group_by_fields,
                              bool prioritize_exact_match,
                              const bool search_all_candidates,
                              const uint32_t* filter_ids, uint32_t filter_ids_length,
                              const uint32_t total_cost,
                              const int syn_orig_num_tokens,
                              const uint32_t* exclude_token_ids,
                              size_t exclude_token_ids_size,
                              const int* sort_order,
                              std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3>& field_values,
                              const std::vector<size_t>& geopoint_indices,
                              std::vector<uint32_t>& id_buff,
                              uint32_t*& all_result_ids,
                              size_t& all_result_ids_len) const;

    void
    search_fields(const std::vector<filter>& filters,
                  const std::map<size_t, std::map<size_t, uint32_t>>& included_ids_map,
                  const std::vector<sort_by>& sort_fields_std,
                  const size_t min_typo, const std::vector<uint32_t>& num_typos,
                  Topster* topster, Topster* curated_topster, const token_ordering& token_order,
                  const std::vector<bool>& prefixes, const size_t drop_tokens_threshold,
                  spp::sparse_hash_set<uint64_t>& groups_processed,
                  std::vector<std::vector<art_leaf*>>& searched_queries,
                  const size_t typo_tokens_threshold, const size_t group_limit,
                  const std::vector<std::string>& group_by_fields, bool prioritize_exact_match,
                  const bool exhaustive_search, const size_t concurrency, size_t min_len_1typo, size_t min_len_2typo,
                  const size_t max_candidates, const std::vector<enable_t>& infixes, const size_t max_extra_prefix,
                  const size_t max_extra_suffix, uint32_t* filter_ids, uint32_t filter_ids_length,
                  const std::set<uint32_t>& curated_ids, const std::vector<uint32_t>& curated_ids_sorted,
                  const size_t num_search_fields, const uint32_t* exclude_token_ids, size_t exclude_token_ids_size,
                  std::vector<Topster*>& ftopsters, bool is_wildcard_query, bool split_join_tokens,
                  std::vector<query_tokens_t>& field_query_tokens,
                  const std::vector<search_field_t>& the_fields, size_t& all_result_ids_len,
                  uint32_t*& all_result_ids,
                  spp::sparse_hash_map<uint64_t, std::vector<KV*>>& topster_ids) const;

    void process_filter_overrides(const std::vector<const override_t*>& filter_overrides,
                                  std::vector<std::string>& query_tokens,
                                  token_ordering token_order,
                                  filter_node_t*& filter_tree_root,
                                  std::vector<const override_t*>& matched_dynamic_overrides) const;

    void compute_sort_scores(const std::vector<sort_by>& sort_fields, const int* sort_order,
                             std::array<spp::sparse_hash_map<uint32_t, int64_t>*, 3> field_values,
                             const std::vector<size_t>& geopoint_indices, uint32_t seq_id,
                             size_t filter_index,
                             int64_t max_field_match_score,
                             int64_t* scores,
                             int64_t& match_score_index) const;

    void
    process_curated_ids(const std::vector<std::pair<uint32_t, uint32_t>>& included_ids,
                        const std::vector<uint32_t>& excluded_ids,
                        const size_t group_limit, const bool filter_curated_hits, const uint32_t* filter_ids,
                        uint32_t filter_ids_length, std::set<uint32_t>& curated_ids,
                        std::map<size_t, std::map<size_t, uint32_t>>& included_ids_map,
                        std::vector<uint32_t>& included_ids_vec) const;
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
