#pragma once

#include <string>
#include <vector>
#include <string>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <shared_mutex>
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

        for(const auto& synonym: synonym_json["synonyms"]) {
            if(!synonym.is_string() || synonym == "") {
                return Option<bool>(400, "Could not find a valid string array of `synonyms`");
            }

            std::vector<std::string> tokens;
            Tokenizer(synonym, true).tokenize(tokens);
            syn.synonyms.push_back(tokens);
        }

        if(synonym_json.count("root") != 0) {
            std::vector<std::string> tokens;
            Tokenizer(synonym_json["root"], true).tokenize(tokens);
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

    mutable std::shared_mutex mutex;

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

    const std::string name;

    const std::atomic<uint32_t> collection_id;

    const std::atomic<uint64_t> created_at;

    std::atomic<size_t> num_documents;

    // Auto incrementing record ID used internally for indexing - not exposed to the client
    std::atomic<uint32_t> next_seq_id;

    Store* store;

    std::vector<field> fields;

    std::unordered_map<std::string, field> search_schema;

    std::map<std::string, field> facet_schema;   // std::map guarantees order of fields

    std::unordered_map<std::string, field> sort_schema;

    std::map<std::string, override_t> overrides;

    spp::sparse_hash_map<std::string, synonym_t> synonym_definitions;
    spp::sparse_hash_map<uint64_t, std::vector<std::string>> synonym_index;

    const std::string default_sorting_field;

    const float max_memory_ratio;

    const std::string fallback_field_type;

    std::vector<field> dynamic_fields;

    std::vector<char> symbols_to_index;

    std::vector<char> token_separators;

    Index* index;

    // methods

    std::string get_doc_id_key(const std::string & doc_id) const;

    std::string get_seq_id_key(uint32_t seq_id) const;

    void highlight_result(const field &search_field, const std::vector<std::vector<art_leaf *>> &searched_queries,
                          const std::vector<std::string>& q_tokens,
                          const KV* field_order_kv, const nlohmann::json &document,
                          StringUtils & string_utils,
                          const size_t snippet_threshold,
                          const size_t highlight_affix_num_tokens,
                          bool highlighted_fully,
                          const std::string& highlight_start_tag,
                          const std::string& highlight_end_tag,
                          highlight_t &highlight) const;

    void remove_document(const nlohmann::json & document, const uint32_t seq_id, bool remove_from_store);

    void curate_results(string& actual_query, bool enable_overrides, bool already_segmented,
                        const std::map<size_t, std::vector<std::string>>& pinned_hits,
                        const std::vector<std::string>& hidden_hits,
                        std::map<size_t, std::vector<uint32_t>>& include_ids,
                        std::vector<uint32_t>& excluded_ids, std::vector<const override_t*>& filter_overrides) const;

    Option<bool> check_and_update_schema(nlohmann::json& document, const DIRTY_VALUES& dirty_values);

    static bool facet_count_compare(const std::pair<uint64_t, facet_count_t>& a,
                                    const std::pair<uint64_t, facet_count_t>& b) {
        return std::tie(a.second.count, a.first) > std::tie(b.second.count, b.first);
    }

    static bool facet_count_str_compare(const facet_value_t& a,
                                        const facet_value_t& b) {
        size_t a_count = a.count;
        size_t b_count = b.count;

        size_t a_value_size = UINT64_MAX - a.value.size();
        size_t b_value_size = UINT64_MAX - b.value.size();

        return std::tie(a_count, a_value_size) > std::tie(b_count, b_value_size);
    }

    static Option<bool> parse_pinned_hits(const std::string& pinned_hits_str,
                                   std::map<size_t, std::vector<std::string>>& pinned_hits);

    void synonym_reduction_internal(const std::vector<std::string>& tokens,
                                    size_t start_window_size,
                                    size_t start_index_pos,
                                    std::set<uint64_t>& processed_syn_hashes,
                                    std::vector<std::vector<std::string>>& results) const;

    Index* init_index();

    static std::vector<char> to_char_array(const std::vector<std::string>& strs);

public:

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

    static constexpr const char* COLLECTION_NAME_KEY = "name";
    static constexpr const char* COLLECTION_ID_KEY = "id";
    static constexpr const char* COLLECTION_SEARCH_FIELDS_KEY = "fields";
    static constexpr const char* COLLECTION_DEFAULT_SORTING_FIELD_KEY = "default_sorting_field";
    static constexpr const char* COLLECTION_CREATED = "created_at";
    static constexpr const char* COLLECTION_NUM_MEMORY_SHARDS = "num_memory_shards";
    static constexpr const char* COLLECTION_FALLBACK_FIELD_TYPE = "fallback_field_type";

    static constexpr const char* COLLECTION_SYMBOLS_TO_INDEX = "symbols_to_index";
    static constexpr const char* COLLECTION_SEPARATORS = "token_separators";

    // methods

    Collection() = delete;

    Collection(const std::string& name, const uint32_t collection_id, const uint64_t created_at,
               const uint32_t next_seq_id, Store *store, const std::vector<field>& fields,
               const std::string& default_sorting_field,
               const float max_memory_ratio, const std::string& fallback_field_type,
               const std::vector<std::string>& symbols_to_index, const std::vector<std::string>& token_separators);

    ~Collection();

    static std::string get_next_seq_id_key(const std::string & collection_name);

    static std::string get_meta_key(const std::string & collection_name);

    static std::string get_override_key(const std::string & collection_name, const std::string & override_id);

    static std::string get_synonym_key(const std::string & collection_name, const std::string & synonym_id);

    std::string get_seq_id_collection_prefix() const;

    std::string get_name() const;

    uint64_t get_created_at() const;

    uint32_t get_collection_id() const;

    uint32_t get_next_seq_id();

    Option<uint32_t> doc_id_to_seq_id(const std::string & doc_id) const;

    std::vector<std::string> get_facet_fields();

    std::vector<field> get_sort_fields();

    std::vector<field> get_fields();

    std::vector<field> get_dynamic_fields();

    std::unordered_map<std::string, field> get_schema();

    std::string get_default_sorting_field();

    Option<doc_seq_id_t> to_doc(const std::string& json_str, nlohmann::json& document,
                                const index_operation_t& operation,
                                const DIRTY_VALUES dirty_values,
                                const std::string& id="");

    static uint32_t get_seq_id_from_key(const std::string & key);

    Option<bool> get_document_from_store(const std::string & seq_id_key, nlohmann::json & document) const;

    Option<bool> get_document_from_store(const uint32_t& seq_id, nlohmann::json & document) const;

    Option<uint32_t> index_in_memory(nlohmann::json & document, uint32_t seq_id,
                                     const index_operation_t op, const DIRTY_VALUES& dirty_values);

    static void prune_document(nlohmann::json &document, const spp::sparse_hash_set<std::string> & include_fields,
                               const spp::sparse_hash_set<std::string> & exclude_fields);

    const Index* _get_index() const;

    bool facet_value_to_string(const facet &a_facet, const facet_count_t &facet_count, const nlohmann::json &document,
                               std::string &value) const;

    static void populate_result_kvs(Topster *topster, std::vector<std::vector<KV *>> &result_kvs);

    void batch_index(std::vector<index_record>& index_records, std::vector<std::string>& json_out, size_t &num_indexed);

    bool is_exceeding_memory_threshold() const;

    void parse_search_query(const std::string &query, std::vector<std::string>& q_include_tokens,
                            std::vector<std::vector<std::string>>& q_exclude_tokens,
                            std::vector<std::vector<std::string>>& q_phrases,
                            const std::string& locale, const bool already_segmented) const;

    // PUBLIC OPERATIONS

    nlohmann::json get_summary_json() const;

    size_t batch_index_in_memory(std::vector<index_record>& index_records);

    Option<nlohmann::json> add(const std::string & json_str,
                               const index_operation_t& operation=CREATE, const std::string& id="",
                               const DIRTY_VALUES& dirty_values=DIRTY_VALUES::COERCE_OR_REJECT);

    nlohmann::json add_many(std::vector<std::string>& json_lines, nlohmann::json& document,
                            const index_operation_t& operation=CREATE, const std::string& id="",
                            const DIRTY_VALUES& dirty_values=DIRTY_VALUES::COERCE_OR_REJECT);

    Option<nlohmann::json> search(const std::string & query, const std::vector<std::string> & search_fields,
                                  const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                                  const std::vector<sort_by> & sort_fields, const std::vector<uint32_t>& num_typos,
                                  size_t per_page = 10, size_t page = 1,
                                  token_ordering token_order = FREQUENCY, const std::vector<bool>& prefixes = {false},
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
                                  std::vector<size_t> query_by_weights={},
                                  size_t limit_hits=UINT32_MAX,
                                  bool prioritize_exact_match=true,
                                  bool pre_segmented_query=false,
                                  bool enable_overrides=true,
                                  const std::string& highlight_fields="",
                                  const bool exhaustive_search = false,
                                  size_t search_stop_millis = 6000*1000,
                                  size_t min_len_1typo = 4,
                                  size_t min_len_2typo = 7) const;

    Option<bool> get_filter_ids(const std::string & simple_filter_query,
                                std::vector<std::pair<size_t, uint32_t*>>& index_ids);

    Option<nlohmann::json> get(const std::string & id) const;

    Option<std::string> remove(const std::string & id, bool remove_from_store = true);

    Option<bool> remove_if_found(uint32_t seq_id, bool remove_from_store = true);

    bool facet_value_to_string(const facet &a_facet, const facet_count_t &facet_count, const nlohmann::json &document,
                               std::string &value);

    size_t get_num_documents() const;

    DIRTY_VALUES parse_dirty_values_option(std::string& dirty_values) const;

    std::vector<char> get_symbols_to_index();

    std::vector<char> get_token_separators();

    // Override operations

    Option<uint32_t> add_override(const override_t & override);

    Option<uint32_t> remove_override(const std::string & id);

    std::map<std::string, override_t> get_overrides() {
        std::shared_lock lock(mutex);
        return overrides;
    };

    // synonym operations

    void synonym_reduction(const std::vector<std::string>& tokens,
                           std::vector<std::vector<std::string>>& results) const;

    spp::sparse_hash_map<std::string, synonym_t> get_synonyms();

    bool get_synonym(const std::string& id, synonym_t& synonym);

    Option<bool> add_synonym(const synonym_t& synonym);

    Option<bool> remove_synonym(const std::string & id);
};

