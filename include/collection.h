#pragma once

#include <string>
#include <vector>
#include <string>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <art.h>
#include <index.h>
#include <number.h>
#include <sparsepp.h>
#include <store.h>
#include <topster.h>
#include <json.hpp>
#include <field.h>
#include <option.h>

class Collection {
private:

    struct highlight_t {
        std::string field;
        std::vector<std::string> snippets;
        std::vector<size_t> indices;
        uint64_t match_score;

        highlight_t() {

        }

        bool operator<(const highlight_t& a) const {
            return match_score > a.match_score;
        }
    };

    std::string name;

    uint32_t collection_id;

    size_t num_documents;

    std::vector<Index*> indices;

    std::vector<std::thread*> index_threads;

    // Auto incrementing record ID used internally for indexing - not exposed to the client
    uint32_t next_seq_id;

    Store* store;

    std::vector<field> fields;

    std::unordered_map<std::string, field> search_schema;

    std::unordered_map<std::string, field> facet_schema;

    std::unordered_map<std::string, field> sort_schema;

    std::string default_sorting_field;

    size_t num_indices;

    std::string get_doc_id_key(const std::string & doc_id);

    std::string get_seq_id_key(uint32_t seq_id);

    Option<uint32_t> validate_index_in_memory(const nlohmann::json &document, uint32_t seq_id);

    void highlight_result(const field &search_field, const std::vector<std::vector<art_leaf *>> &searched_queries,
                          const Topster<512>::KV &field_order_kv, const nlohmann::json &document,
                          StringUtils & string_utils, highlight_t &highlight);

public:
    Collection() = delete;

    Collection(const std::string name, const uint32_t collection_id, const uint32_t next_seq_id, Store *store,
               const std::vector<field> & fields, const std::string & default_sorting_field, const size_t num_indices=4);

    ~Collection();

    static std::string get_next_seq_id_key(const std::string & collection_name);

    static std::string get_meta_key(const std::string & collection_name);

    std::string get_seq_id_collection_prefix();

    std::string get_name();

    size_t get_num_documents();

    uint32_t get_collection_id();

    uint32_t get_next_seq_id();

    void set_next_seq_id(uint32_t seq_id);

    void increment_next_seq_id_field();

    Option<uint32_t> doc_id_to_seq_id(std::string doc_id);

    std::vector<std::string> get_facet_fields();

    std::vector<field> get_sort_fields();

    std::vector<field> get_fields();

    std::unordered_map<std::string, field> get_schema();

    std::string get_default_sorting_field();

    Option<nlohmann::json> add(const std::string & json_str);

    Option<nlohmann::json> search(std::string query, const std::vector<std::string> search_fields,
                          const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                          const std::vector<sort_by> & sort_fields, const int num_typos,
                          const size_t per_page = 10, const size_t page = 1,
                          const token_ordering token_order = FREQUENCY, const bool prefix = false,
                          const size_t drop_tokens_threshold = Index::DROP_TOKENS_THRESHOLD,
                          const spp::sparse_hash_set<std::string> include_fields = spp::sparse_hash_set<std::string>(),
                          const spp::sparse_hash_set<std::string> exclude_fields = spp::sparse_hash_set<std::string>());

    Option<nlohmann::json> get(const std::string & id);

    Option<std::string> remove(const std::string & id, const bool remove_from_store = true);

    Option<uint32_t> index_in_memory(const nlohmann::json & document, uint32_t seq_id);

    static void prune_document(nlohmann::json &document, const spp::sparse_hash_set<std::string> include_fields,
                               const spp::sparse_hash_set<std::string> exclude_fields);

    static const int MAX_SEARCH_TOKENS = 10;
    static const int MAX_RESULTS = 500;

    // strings under this length will be fully highlighted, instead of showing a snippet of relevant portion
    enum {SNIPPET_STR_ABOVE_LEN = 30};

    enum {MAX_ARRAY_MATCHES = 5};

    // Using a $ prefix so that these meta keys stay above record entries in a lexicographically ordered KV store
    static constexpr const char* COLLECTION_META_PREFIX = "$CM";
    static constexpr const char* COLLECTION_NEXT_SEQ_PREFIX = "$CS";
    static constexpr const char* SEQ_ID_PREFIX = "$SI";
    static constexpr const char* DOC_ID_PREFIX = "$DI";
};

