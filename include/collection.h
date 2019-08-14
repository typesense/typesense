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


struct override_t {
    static const std::string MATCH_EXACT;
    static const std::string MATCH_CONTAINS;

    struct rule_t {
        std::string query;
        std::string match;
    };

    struct add_hit_t {
        std::string doc_id;
        uint32_t position;
    };

    struct drop_hit_t {
        std::string doc_id;
    };

    std::string id;
    rule_t rule;
    std::vector<add_hit_t> add_hits;
    std::vector<drop_hit_t> drop_hits;

    override_t() {}

    override_t(const nlohmann::json & override) {
        id = override["id"].get<std::string>();
        rule.query = override["rule"]["query"].get<std::string>();
        rule.match = override["rule"]["match"].get<std::string>();

        if (override.count("includes") != 0) {
            for(const auto & include: override["includes"]) {
                add_hit_t add_hit;
                add_hit.doc_id = include["id"].get<std::string>();
                add_hit.position = include["position"].get<uint32_t>();
                add_hits.push_back(add_hit);
            }
        }

        if (override.count("excludes") != 0) {
            for(const auto & exclude: override["excludes"]) {
                drop_hit_t drop_hit;
                drop_hit.doc_id = exclude["id"].get<std::string>();
                drop_hits.push_back(drop_hit);
            }
        }
    }

    std::string to_json_str() const {
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

        return override.dump();
    }
};

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

    std::map<std::string, override_t> overrides;

    std::string name;

    uint32_t collection_id;

    uint64_t created_at;

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

    void highlight_result(const field &search_field, const std::vector<std::vector<art_leaf *>> &searched_queries,
                          const KV &field_order_kv, const nlohmann::json &document,
                          StringUtils & string_utils, highlight_t &highlight);

    void remove_document(nlohmann::json & document, const uint32_t seq_id, bool remove_from_store);

    void populate_overrides(std::string query, std::map<uint32_t, size_t> & id_pos_map,
                            std::vector<uint32_t> & included_ids, std::vector<uint32_t> & excluded_ids);

public:
    Collection() = delete;

    Collection(const std::string name, const uint32_t collection_id, const uint64_t created_at,
               const uint32_t next_seq_id, Store *store, const std::vector<field> & fields,
               const std::string & default_sorting_field, const size_t num_indices=4);

    ~Collection();

    static std::string get_next_seq_id_key(const std::string & collection_name);

    static std::string get_meta_key(const std::string & collection_name);

    static std::string get_override_key(const std::string & collection_name, const std::string & override_id);

    std::string get_seq_id_collection_prefix();

    std::string get_name();

    uint64_t get_created_at();

    size_t get_num_documents();

    uint32_t get_collection_id();

    uint32_t get_next_seq_id();

    void set_next_seq_id(uint32_t seq_id);

    void increment_next_seq_id_field();

    Option<uint32_t> doc_id_to_seq_id(const std::string & doc_id);

    std::vector<std::string> get_facet_fields();

    std::vector<field> get_sort_fields();

    std::vector<field> get_fields();

    std::unordered_map<std::string, field> get_schema();

    std::string get_default_sorting_field();

    Option<uint32_t> to_doc(const std::string & json_str, nlohmann::json & document);

    Option<nlohmann::json> add(const std::string & json_str);

    Option<nlohmann::json> add_many(const std::string & json_str);

    Option<nlohmann::json> search(std::string query, const std::vector<std::string> search_fields,
                          const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                          const std::vector<sort_by> & sort_fields, const int num_typos,
                          const size_t per_page = 10, const size_t page = 1,
                          const token_ordering token_order = FREQUENCY, const bool prefix = false,
                          const size_t drop_tokens_threshold = Index::DROP_TOKENS_THRESHOLD,
                          const spp::sparse_hash_set<std::string> include_fields = spp::sparse_hash_set<std::string>(),
                          const spp::sparse_hash_set<std::string> exclude_fields = spp::sparse_hash_set<std::string>(),
                          const size_t max_facet_values=10);

    Option<nlohmann::json> get(const std::string & id);

    Option<std::string> remove(const std::string & id, const bool remove_from_store = true);

    Option<uint32_t> add_override(const override_t & override);

    Option<uint32_t> remove_override(const std::string & id);

    std::map<std::string, override_t> get_overrides() {
        return overrides;
    };

    size_t get_num_indices();

    static uint32_t get_seq_id_from_key(const std::string & key);

    Option<uint32_t> index_in_memory(const nlohmann::json & document, uint32_t seq_id);

    void par_index_in_memory(std::vector<std::vector<index_record>> & iter_batch,
                             batch_index_result & result);

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
    static constexpr const char* COLLECTION_OVERRIDE_PREFIX = "$CO";
    static constexpr const char* SEQ_ID_PREFIX = "$SI";
    static constexpr const char* DOC_ID_PREFIX = "$DI";
};

