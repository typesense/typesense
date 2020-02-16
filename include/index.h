#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <art.h>
#include <number.h>
#include <sparsepp.h>
#include <store.h>
#include <topster.h>
#include <json.hpp>
#include <field.h>
#include <option.h>
#include "string_utils.h"

struct token_candidates {
    std::string token;
    size_t cost;
    std::vector<art_leaf*> candidates;
};

struct search_args {
    std::string query;
    std::vector<std::string> search_fields;
    std::vector<filter> filters;
    std::vector<facet> facets;
    std::vector<uint32_t> included_ids;
    std::vector<uint32_t> excluded_ids;
    std::vector<sort_by> sort_fields_std;
    int num_typos;
    size_t max_facet_values;
    size_t max_hits;
    size_t per_page;
    size_t page;
    token_ordering token_order;
    bool prefix;
    size_t drop_tokens_threshold;
    std::vector<KV> raw_result_kvs;
    size_t all_result_ids_len;
    std::vector<std::vector<art_leaf*>> searched_queries;
    std::vector<KV> override_result_kvs;
    Option<uint32_t> outcome;

    search_args(): outcome(0) {

    }

    search_args(std::string query, std::vector<std::string> search_fields, std::vector<filter> filters,
                std::vector<facet> facets, std::vector<uint32_t> included_ids, std::vector<uint32_t> excluded_ids,
                std::vector<sort_by> sort_fields_std, int num_typos, size_t max_facet_values,
                size_t max_hits, size_t per_page, size_t page, token_ordering token_order, bool prefix,
                size_t drop_tokens_threshold):
            query(query), search_fields(search_fields), filters(filters), facets(facets), included_ids(included_ids),
            excluded_ids(excluded_ids), sort_fields_std(sort_fields_std), num_typos(num_typos),
            max_facet_values(max_facet_values), max_hits(max_hits), per_page(per_page),
            page(page), token_order(token_order), prefix(prefix), drop_tokens_threshold(drop_tokens_threshold),
            all_result_ids_len(0), outcome(0) {

    }
};

struct index_record {
    size_t record_pos;         // position of record in the original request

    uint32_t seq_id;
    std::string json_str;
    nlohmann::json document;

    index_record(size_t record_pos, uint32_t seq_id, const std::string & json_str, const nlohmann::json & doc):
                 record_pos(record_pos), seq_id(seq_id), json_str(json_str), document(doc) {

    }
};

struct index_result {
    index_record record;
    Option<bool> index_op;     // indicates if the indexing operation was a success

    index_result(const index_record & record, const Option<bool> & index_op):
                 record(record), index_op(index_op) {

    }

    bool operator<(const index_result & a) const {
        return record.record_pos < a.record.record_pos;
    }
};

struct batch_index_result {
    std::vector<index_result> items;
    size_t num_indexed = 0;

    batch_index_result() {

    }

    void failure(const index_record & record, const uint32_t err_code, const std::string & err_msg) {
        Option<bool> index_op_failure(err_code, err_msg);
        index_result res(record, index_op_failure);
        items.push_back(res);
    }

    void success(const index_record & record) {
        Option<bool> index_op_success(true);
        index_result res(record, index_op_success);
        items.push_back(res);
        num_indexed++;
    }
};

class Index {
private:
    std::string name;

    size_t num_documents;

    std::unordered_map<std::string, field> search_schema;

    std::map<std::string, field> facet_schema;  // std::map guarantees order of fields

    std::unordered_map<std::string, field> sort_schema;

    spp::sparse_hash_map<std::string, art_tree*> search_index;

    spp::sparse_hash_map<uint32_t, std::vector<std::vector<uint64_t>>> facet_index_v2;

    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, number_t>*> sort_index;

    StringUtils string_utils;

    static inline std::vector<art_leaf *> next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                                          long long int n);

    void log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const;

    size_t union_of_ids(std::vector<std::pair<uint32_t*, size_t>> & result_array_pairs, uint32_t **results_out);

    Option<uint32_t> do_filtering(uint32_t** filter_ids_out, const std::vector<filter> & filters);

    void do_facets(std::vector<facet> & facets, const uint32_t* result_ids, size_t results_size);

    void drop_facets(std::vector<facet> & facets, const std::vector<uint32_t> & ids);

    void search_field(const uint8_t & field_id, std::string & query,
                      const std::string & field, uint32_t *filter_ids, size_t filter_ids_length,
                      std::vector<facet> & facets, const std::vector<sort_by> & sort_fields,
                      const int num_typos, const size_t num_results,
                      std::vector<std::vector<art_leaf*>> & searched_queries,
                      Topster & topster, uint32_t** all_result_ids,
                      size_t & all_result_ids_len, const token_ordering token_order = FREQUENCY,
                      const bool prefix = false, const size_t drop_tokens_threshold = Index::DROP_TOKENS_THRESHOLD);

    void search_candidates(const uint8_t & field_id, uint32_t* filter_ids, size_t filter_ids_length, std::vector<facet> & facets,
                           const std::vector<sort_by> & sort_fields, std::vector<token_candidates> & token_to_candidates,
                           const token_ordering token_order, std::vector<std::vector<art_leaf*>> & searched_queries,
                           Topster & topster, uint32_t** all_result_ids,
                           size_t & all_result_ids_len, const size_t & max_results);

    void insert_doc(const uint32_t score, art_tree *t, uint32_t seq_id,
                    const std::unordered_map<std::string, std::vector<uint32_t>> &token_to_offsets) const;

    void index_string_field(const std::string & text, const uint32_t score, art_tree *t, uint32_t seq_id,
                            const bool verbatim) const;

    void index_string_array_field(const std::vector<std::string> & strings, const uint32_t score, art_tree *t,
                                  uint32_t seq_id, const bool verbatim) const;

    void index_int32_field(const int32_t value, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_int64_field(const int64_t value, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_float_field(const float value, const uint32_t score, art_tree *t, uint32_t seq_id) const;
    
    void index_bool_field(const bool value, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_int32_array_field(const std::vector<int32_t> & values, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_int64_array_field(const std::vector<int64_t> & values, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_float_array_field(const std::vector<float> & values, const uint32_t score, art_tree *t, uint32_t seq_id) const;
    
    void index_bool_array_field(const std::vector<bool> & values, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void remove_and_shift_offset_index(sorted_array &offset_index, const uint32_t *indices_sorted,
                                       const uint32_t indices_length);

    void collate_curated_ids(const std::string & query, const std::string & field, const uint8_t field_id,
                             const std::vector<uint32_t> & included_ids,
                             Topster & curated_topster, std::vector<std::vector<art_leaf*>> & searched_queries);

public:
    Index() = delete;

    Index(const std::string name, const std::unordered_map<std::string, field> & search_schema,
          std::map<std::string, field> facet_schema, std::unordered_map<std::string, field> sort_schema);

    ~Index();

    void run_search();

    void search(Option<uint32_t> & outcome, std::string query, const std::vector<std::string> & search_fields,
                          const std::vector<filter> & filters, std::vector<facet> & facets,
                          const std::vector<uint32_t> & included_ids, const std::vector<uint32_t> & excluded_ids,
                          const std::vector<sort_by> & sort_fields_std, const int num_typos,
                          const size_t max_hits, const size_t per_page, const size_t page, const token_ordering token_order,
                          const bool prefix, const size_t drop_tokens_threshold, std::vector<KV> & raw_result_kvs,
                          size_t & all_result_ids_len, std::vector<std::vector<art_leaf*>> & searched_queries,
                          std::vector<KV> & override_result_kvs);

    Option<uint32_t> remove(const uint32_t seq_id, nlohmann::json & document);

    art_leaf* get_token_leaf(const std::string & field_name, const unsigned char* token, uint32_t token_len);

    static void populate_token_positions(const std::vector<art_leaf *> &query_suggestion,
                                         spp::sparse_hash_map<const art_leaf *, uint32_t *> &leaf_to_indices,
                                         size_t result_index,
                                         std::vector<std::vector<std::vector<uint16_t>>> &array_token_positions);

    void score_results(const std::vector<sort_by> & sort_fields, const uint16_t & query_index, const uint8_t & field_id,
                       const uint32_t total_cost, Topster &topster, const std::vector<art_leaf *> & query_suggestion,
                       const uint32_t *result_ids, const size_t result_size) const;

    static int32_t get_points_from_doc(const nlohmann::json &document, const std::string & default_sorting_field);

    Option<uint32_t> index_in_memory(const nlohmann::json & document, uint32_t seq_id,
                                     const std::string & default_sorting_field);

    static Option<uint32_t> validate_index_in_memory(const nlohmann::json &document, uint32_t seq_id,
                                                     const std::string & default_sorting_field,
                                                     const std::unordered_map<std::string, field> & search_schema,
                                                     const std::map<std::string, field> & facet_schema);

    static batch_index_result batch_memory_index(Index *index,
                                        std::vector<index_record> & iter_batch,
                                        const std::string & default_sorting_field,
                                        const std::unordered_map<std::string, field> & search_schema,
                                        const std::map<std::string, field> & facet_schema);

    // for limiting number of results on multiple candidates / query rewrites
    enum {SEARCH_LIMIT_NUM = 100};

    // for limiting number of fields that can be searched on
    enum {FIELD_LIMIT_NUM = 100};

    // If the number of results found is less than this threshold, Typesense will attempt to drop the tokens
    // in the query that have the least individual hits one by one until enough results are found.
    static const int DROP_TOKENS_THRESHOLD = 10;

    // strings under this length will be fully highlighted, instead of showing a snippet of relevant portion
    enum {SNIPPET_STR_ABOVE_LEN = 30};

    // Using a $ prefix so that these meta keys stay above record entries in a lexicographically ordered KV store
    static constexpr const char* COLLECTION_META_PREFIX = "$CM";
    static constexpr const char* COLLECTION_NEXT_SEQ_PREFIX = "$CS";
    static constexpr const char* SEQ_ID_PREFIX = "$SI";
    static constexpr const char* DOC_ID_PREFIX = "$DI";

    /*
     * Concurrency Primitives
    */

    // Used for passing control back and forth between main and worker threads
    std::mutex m;
    std::condition_variable cv;

    bool ready;       // prevents spurious wake up of the worker thread
    bool processed;   // prevents spurious wake up of the main thread
    bool terminate;   // used for interrupting the thread during tear down

    search_args search_params;

    static void
    populate_array_token_positions(std::vector<std::vector<std::vector<uint16_t>>> &array_token_positions,
                                   const art_leaf *token_leaf, uint32_t doc_index);
};

