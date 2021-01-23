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
#include "tokenizer.h"


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

        return override;
    }
};

struct doc_seq_id_t {
    uint32_t seq_id;
    bool is_new;
};

struct synonym_t {
    std::string id;
    std::vector<std::string> root;
    std::vector<std::vector<std::string>> synonyms;

    synonym_t() = default;

    synonym_t(const std::string& id, const std::vector<std::string>& root,
              const std::vector<std::vector<std::string>>& synonyms):
              id(id), root(root), synonyms(synonyms) {

    }

    explicit synonym_t(const nlohmann::json& synonym) {
        id = synonym["id"].get<std::string>();
        if(synonym.count("root") != 0) {
            root = synonym["root"].get<std::vector<std::string>>();
        }
        synonyms = synonym["synonyms"].get<std::vector<std::vector<std::string>>>();
    }

    nlohmann::json to_json() const {
        nlohmann::json obj;
        obj["id"] = id;
        obj["root"] = root;
        obj["synonyms"] = synonyms;
        return obj;
    }

    nlohmann::json to_view_json() const {
        nlohmann::json obj;
        obj["id"] = id;
        obj["root"] = StringUtils::join(root, " ");

        obj["synonyms"] = nlohmann::json::array();

        for(const auto& synonym: synonyms) {
            obj["synonyms"].push_back(StringUtils::join(synonym, " "));
        }

        return obj;
    }

    static Option<bool> parse(const nlohmann::json& synonym_json, synonym_t& syn) {
        if(synonym_json.count("id") == 0) {
            return Option<bool>(400, "Missing `id` field.");
        }

        if(synonym_json.count("synonyms") == 0) {
            return Option<bool>(400, "Could not find an array of `synonyms`");
        }

        if(synonym_json.count("root") != 0 && !synonym_json["root"].is_string()) {
            return Option<bool>(400, "Key `root` should be a string.");
        }

        if (!synonym_json["synonyms"].is_array() || synonym_json["synonyms"].empty()) {
            return Option<bool>(400, "Could not find an array of `synonyms`");
        }

        for(const auto & synonym: synonym_json["synonyms"]) {
            if(!synonym.is_string()) {
                return Option<bool>(400, "Could not find a valid string array of `synonyms`");
            }

            std::vector<std::string> tokens;
            Tokenizer(synonym, false).tokenize(tokens);
            syn.synonyms.push_back(tokens);
        }

        if(synonym_json.count("root") != 0) {
            std::vector<std::string> tokens;
            Tokenizer(synonym_json["root"], false).tokenize(tokens);
            syn.root = tokens;
        }

        syn.id = synonym_json["id"];
        return Option<bool>(true);
    }

    static uint64_t get_hash(const std::vector<std::string>& tokens) {
        uint64_t hash = 1;
        for(size_t i=0; i < tokens.size(); i++) {
            auto& token = tokens[i];
            uint64_t token_hash = StringUtils::hash_wy(token.c_str(), token.size());
            if(i == 0) {
                hash = token_hash;
            } else {
                hash = Index::hash_combine(hash, token_hash);
            }
        }

        return hash;
    }
};

class Collection {
private:

    const uint8_t CURATED_RECORD_IDENTIFIER = 100;

    struct highlight_t {
        std::string field;
        std::vector<std::string> snippets;
        std::vector<std::string> values;
        std::vector<size_t> indices;
        uint64_t match_score;
        std::vector<std::vector<std::string>> matched_tokens;

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

    std::map<std::string, field> facet_schema;   // std::map guarantees order of fields

    std::unordered_map<std::string, field> sort_schema;

    spp::sparse_hash_map<std::string, synonym_t> synonym_definitions;
    spp::sparse_hash_map<uint64_t, std::vector<std::string>> synonym_index;

    std::string default_sorting_field;

    size_t num_memory_shards;

    const float max_memory_ratio;

    std::string get_doc_id_key(const std::string & doc_id);

    std::string get_seq_id_key(uint32_t seq_id);

    void highlight_result(const field &search_field, const std::vector<std::vector<art_leaf *>> &searched_queries,
                          const KV* field_order_kv, const nlohmann::json &document,
                          StringUtils & string_utils,
                          const size_t snippet_threshold,
                          const size_t highlight_affix_num_tokens,
                          bool highlighted_fully,
                          const std::string& highlight_start_tag,
                          const std::string& highlight_end_tag,
                          highlight_t &highlight);

    void remove_document(const nlohmann::json & document, const uint32_t seq_id, bool remove_from_store);

    void populate_overrides(std::string query,
                            const std::map<size_t, std::vector<std::string>>& pinned_hits,
                            const std::vector<std::string>& hidden_hits,
                            std::map<size_t, std::vector<uint32_t>>& include_ids,
                            std::vector<uint32_t> & excluded_ids);

    static bool facet_count_compare(const std::pair<uint64_t, facet_count_t>& a,
                                    const std::pair<uint64_t, facet_count_t>& b) {
        return std::tie(a.second.count, a.first) > std::tie(b.second.count, b.first);
    }

    static bool facet_count_str_compare(const facet_value_t& a,
                                        const facet_value_t& b) {
        return a.count > b.count;
    }

    void free_leaf_indices(std::vector<uint32_t*>& leaf_to_indices) const;

    Option<bool> parse_filter_query(const std::string& simple_filter_query, std::vector<filter>& filters);

    Option<bool> parse_pinned_hits(const std::string& pinned_hits_str,
                                   std::map<size_t, std::vector<std::string>>& pinned_hits);

    void synonym_reduction_internal(const std::vector<std::string>& tokens,
                                    size_t start_window_size,
                                    size_t start_index_pos,
                                    std::set<uint64_t>& processed_syn_hashes,
                                    std::vector<std::vector<std::string>>& results);

public:
    Collection() = delete;

    Collection(const std::string& name, const uint32_t collection_id, const uint64_t created_at,
               const uint32_t next_seq_id, Store *store, const std::vector<field>& fields,
               const std::string& default_sorting_field, const size_t num_memory_shards, const float max_memory_ratio);

    ~Collection();

    static std::string get_next_seq_id_key(const std::string & collection_name);

    static std::string get_meta_key(const std::string & collection_name);

    static std::string get_override_key(const std::string & collection_name, const std::string & override_id);

    static std::string get_synonym_key(const std::string & collection_name, const std::string & synonym_id);

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

    Option<doc_seq_id_t> to_doc(const std::string& json_str, nlohmann::json& document,
                                const index_operation_t& operation, const std::string& id="");

    nlohmann::json get_summary_json();

    Option<nlohmann::json> add(const std::string & json_str,
                               const index_operation_t& operation=CREATE, const std::string& id="");

    nlohmann::json add_many(std::vector<std::string>& json_lines, nlohmann::json& document,
                            const index_operation_t& operation=CREATE, const std::string& id="");

    Option<nlohmann::json> search(const std::string & query, const std::vector<std::string> & search_fields,
                          const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                          const std::vector<sort_by> & sort_fields, int num_typos,
                          size_t per_page = 10, size_t page = 1,
                          token_ordering token_order = FREQUENCY, bool prefix = false,
                          size_t drop_tokens_threshold = Index::DROP_TOKENS_THRESHOLD,
                          const spp::sparse_hash_set<std::string> & include_fields = spp::sparse_hash_set<std::string>(),
                          const spp::sparse_hash_set<std::string> & exclude_fields = spp::sparse_hash_set<std::string>(),
                          size_t max_facet_values=10,
                          const std::string & simple_facet_query = "",
                          const size_t snippet_threshold = 30,
                          const size_t highlight_affix_num_tokens = 4,
                          const std::string & highlight_full_fields = "",
                          size_t typo_tokens_threshold = Index::TYPO_TOKENS_THRESHOLD,
                          const std::string& pinned_hits_str="",
                          const std::string& hidden_hits="",
                          const std::vector<std::string>& group_by_fields={},
                          const size_t group_limit = 0,
                          const std::string& highlight_start_tag="<mark>",
                          const std::string& highlight_end_tag="</mark>",
                          std::vector<size_t> query_by_weights={});

    Option<bool> get_filter_ids(const std::string & simple_filter_query,
                                std::vector<std::pair<size_t, uint32_t*>>& index_ids);

    Option<nlohmann::json> get(const std::string & id);

    bool doc_exists(const std::string & id);

    Option<std::string> remove(const std::string & id, bool remove_from_store = true);

    Option<bool> remove_if_found(uint32_t seq_id, bool remove_from_store = true);

    Option<uint32_t> add_override(const override_t & override);

    Option<uint32_t> remove_override(const std::string & id);

    std::map<std::string, override_t>& get_overrides() {
        return overrides;
    };

    size_t get_num_memory_shards();

    static uint32_t get_seq_id_from_key(const std::string & key);

    Option<bool> get_document_from_store(const std::string & seq_id_key, nlohmann::json & document);

    Option<uint32_t> index_in_memory(const nlohmann::json & document, uint32_t seq_id, bool is_update);

    size_t par_index_in_memory(std::vector<std::vector<index_record>> & iter_batch, std::vector<size_t>& indexed_counts);

    static void prune_document(nlohmann::json &document, const spp::sparse_hash_set<std::string> & include_fields,
                               const spp::sparse_hash_set<std::string> & exclude_fields);

    const std::vector<Index *> &_get_indexes() const;

    enum {MAX_ARRAY_MATCHES = 5};

    const size_t PER_PAGE_MAX = 250;

    const size_t GROUP_LIMIT_MAX = 99;

    // Using a $ prefix so that these meta keys stay above record entries in a lexicographically ordered KV store
    static constexpr const char* COLLECTION_META_PREFIX = "$CM";
    static constexpr const char* COLLECTION_NEXT_SEQ_PREFIX = "$CS";
    static constexpr const char* COLLECTION_OVERRIDE_PREFIX = "$CO";
    static constexpr const char* COLLECTION_SYNONYM_PREFIX = "$CY";
    static constexpr const char* SEQ_ID_PREFIX = "$SI";
    static constexpr const char* DOC_ID_PREFIX = "$DI";

    bool facet_value_to_string(const facet &a_facet, const facet_count_t &facet_count, const nlohmann::json &document,
                               std::string &value);

    void aggregate_topster(size_t query_index, Topster &topster, Topster *index_topster) const;

    void populate_result_kvs(Topster *topster, std::vector<std::vector<KV *>> &result_kvs) const;

    void batch_index(std::vector<std::vector<index_record>> &index_batches, std::vector<std::string>& json_out,
                     size_t &num_indexed);

    bool is_exceeding_memory_threshold() const;

    void get_doc_changes(const nlohmann::json &document, nlohmann::json &old_doc,
                         nlohmann::json &new_doc,
                         nlohmann::json &del_doc);

    void parse_search_query(const std::string &query, std::vector<std::string>& q_include_tokens,
                                            std::vector<std::string>& q_exclude_tokens) const;

    // synonym operations
    void synonym_reduction(const std::vector<std::string>& tokens, std::vector<std::vector<std::string>>& results);

    spp::sparse_hash_map<std::string, synonym_t>& get_synonyms();
    bool get_synonym(const std::string& id, synonym_t& synonym);
    Option<bool> add_synonym(const synonym_t& synonym);
    Option<bool> remove_synonym(const std::string & id);
};

