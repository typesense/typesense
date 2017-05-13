#pragma once

#include <string>
#include <vector>
#include <art.h>
#include <sparsepp.h>
#include <store.h>
#include <topster.h>
#include <json.hpp>
#include <field.h>
#include <option.h>

class Collection {
private:
    std::string name;

    uint32_t collection_id;

    // Auto incrementing record ID used internally for indexing - not exposed to the client
    uint32_t next_seq_id;

    spp::sparse_hash_map<std::string, field> search_schema;

    spp::sparse_hash_map<std::string, field> facet_schema;

    std::vector<sort_field> sort_fields;

    Store* store;

    spp::sparse_hash_map<std::string, art_tree*> search_index;

    spp::sparse_hash_map<std::string, art_tree*> facet_index;

    spp::sparse_hash_map<std::string, spp::sparse_hash_map<uint32_t, int64_t>*> sort_index;

    std::string token_ordering_field;

    std::string get_doc_id_key(std::string doc_id);

    std::string get_seq_id_key(uint32_t seq_id);

    static inline std::vector<art_leaf *> next_suggestion(const std::vector<std::vector<art_leaf *>> &token_leaves,
                                                          long long int n);

    void log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const;

    size_t union_of_leaf_ids(std::vector<const art_leaf *> &leaves, uint32_t **results_out);

    Option<uint32_t> do_filtering(uint32_t** filter_ids_out, const std::string & simple_filter_str);

    void do_facets(std::vector<facet> & facets, uint32_t* result_ids, size_t results_size);

    void search_field(std::string & query, const std::string & field, uint32_t *filter_ids, size_t filter_ids_length,
                      std::vector<facet> & facets, const std::vector<sort_field> & sort_fields, const int num_typos,
                      const size_t num_results, Topster<100> &topster, uint32_t** all_result_ids,
                      size_t & all_result_ids_len, const token_ordering token_order = FREQUENCY, const bool prefix = false);

    void search_candidates(uint32_t* filter_ids, size_t filter_ids_length, std::vector<facet> & facets,
                           const std::vector<sort_field> & sort_fields, int & token_rank,
                           std::vector<std::vector<art_leaf*>> & token_leaves, Topster<100> & topster,
                           size_t & total_results, uint32_t** all_result_ids, size_t & all_result_ids_len,
                           const size_t & max_results);

    void index_string_field(const std::string & text, const uint32_t score, art_tree *t, uint32_t seq_id,
                            const bool verbatim) const;

    void index_string_array_field(const std::vector<std::string> & strings, const uint32_t score, art_tree *t,
                                  uint32_t seq_id, const bool verbatim) const;

    void index_int32_field(const int32_t value, uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_int64_field(const int64_t value, uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_int32_array_field(const std::vector<int32_t> & values, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void index_int64_array_field(const std::vector<int64_t> & values, const uint32_t score, art_tree *t, uint32_t seq_id) const;

    void remove_and_shift_offset_index(sorted_array &offset_index, const uint32_t *indices_sorted,
                                       const uint32_t indices_length);

public:
    Collection() = delete;

    Collection(const std::string name, const uint32_t collection_id, const uint32_t next_seq_id, Store *store,
               const std::vector<field> & search_fields, const std::vector<field> & facet_fields,
               const std::vector<sort_field> & sort_fields, const std::string token_ordering_field);

    ~Collection();

    static std::string get_next_seq_id_key(std::string collection_name);

    static std::string get_meta_key(std::string collection_name);

    std::string get_seq_id_collection_prefix();

    uint32_t get_collection_id();

    uint32_t get_next_seq_id();

    uint32_t doc_id_to_seq_id(std::string doc_id);

    std::vector<std::string> get_facet_fields();

    std::vector<sort_field> get_sort_fields();

    spp::sparse_hash_map<std::string, field> get_schema();

    std::string get_token_ordering_field();

    Option<std::string> add(std::string json_str);

    nlohmann::json search(std::string query, const std::vector<std::string> search_fields,
                          const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                          const std::vector<sort_field> & sort_fields, const int num_typos,
                          const size_t num_results, const token_ordering token_order = FREQUENCY, const bool prefix = false);

    void remove(std::string id);

    void score_results(const std::vector<sort_field> & sort_fields, const int & token_rank, Topster<100> &topster,
                       const std::vector<art_leaf *> & query_suggestion, const uint32_t *result_ids,
                       const size_t result_size) const;

    Option<uint32_t> index_in_memory(const nlohmann::json &document, uint32_t seq_id);

    enum {MAX_SEARCH_TOKENS = 20};
    enum {MAX_RESULTS = 100};

    // Using a $ prefix so that these meta keys stay above record entries in a lexicographically ordered KV store
    static constexpr const char* COLLECTION_META_PREFIX = "$CM";
    static constexpr const char* COLLECTION_NEXT_SEQ_PREFIX = "$CS";
    static constexpr const char* SEQ_ID_PREFIX = "$SI";
    static constexpr const char* DOC_ID_PREFIX = "$DI";
};

