#pragma once

#include <iostream>
#include <string>
#include <sparsepp.h>
#include "store.h"
#include "field.h"
#include "collection.h"
#include "auth_manager.h"
#include "threadpool.h"
#include "batched_indexer.h"

template<typename ResourceType>
struct locked_resource_view_t {
    locked_resource_view_t(std::shared_mutex &mutex, ResourceType &resource) : _lock(mutex), _resource(&resource) {}

    locked_resource_view_t(std::shared_mutex &mutex, ResourceType *resource) : _lock(mutex), _resource(resource) {}

    locked_resource_view_t(ResourceType &&) = delete;

    locked_resource_view_t(const ResourceType &) = delete;

    locked_resource_view_t &operator=(const ResourceType &) = delete;

    ResourceType* operator->() {
        return _resource;
    }

    bool operator==(const ResourceType* other) {
        return other == _resource;
    }

    bool operator!=(const ResourceType* other) {
        return other != _resource;
    }

    void unlock() {
        _lock.unlock();
    }

    ResourceType* get() {
        return _resource;
    }

private:
    std::shared_lock<std::shared_mutex> _lock;
    ResourceType* _resource;
};

struct collection_search_args {
    static constexpr auto NUM_TYPOS = "num_typos";
    static constexpr auto MIN_LEN_1TYPO = "min_len_1typo";
    static constexpr auto MIN_LEN_2TYPO = "min_len_2typo";

    static constexpr auto PREFIX = "prefix";
    static constexpr auto DROP_TOKENS_THRESHOLD = "drop_tokens_threshold";
    static constexpr auto TYPO_TOKENS_THRESHOLD = "typo_tokens_threshold";
    static constexpr auto FILTER = "filter_by";
    static constexpr auto QUERY = "q";
    static constexpr auto QUERY_BY = "query_by";
    static constexpr auto QUERY_BY_WEIGHTS = "query_by_weights";
    static constexpr auto SORT_BY = "sort_by";

    static constexpr auto FACET_BY = "facet_by";
    static constexpr auto FACET_QUERY = "facet_query";
    static constexpr auto FACET_QUERY_NUM_TYPOS = "facet_query_num_typos";
    static constexpr auto MAX_FACET_VALUES = "max_facet_values";
    static constexpr auto FACET_STRATEGY = "facet_strategy";

    static constexpr auto FACET_RETURN_PARENT = "facet_return_parent";

    static constexpr auto VECTOR_QUERY = "vector_query";

    static constexpr auto REMOTE_EMBEDDING_TIMEOUT_MS = "remote_embedding_timeout_ms";
    static constexpr auto REMOTE_EMBEDDING_NUM_TRIES = "remote_embedding_num_tries";

    static constexpr auto GROUP_BY = "group_by";
    static constexpr auto GROUP_LIMIT = "group_limit";
    static constexpr auto GROUP_MISSING_VALUES = "group_missing_values";

    static constexpr auto LIMIT_HITS = "limit_hits";
    static constexpr auto PER_PAGE = "per_page";
    static constexpr auto PAGE = "page";
    static constexpr auto OFFSET = "offset";
    static constexpr auto LIMIT = "limit";
    static constexpr auto RANK_TOKENS_BY = "rank_tokens_by";
    static constexpr auto INCLUDE_FIELDS = "include_fields";
    static constexpr auto EXCLUDE_FIELDS = "exclude_fields";

    static constexpr auto PINNED_HITS = "pinned_hits";
    static constexpr auto HIDDEN_HITS = "hidden_hits";
    static constexpr auto ENABLE_OVERRIDES = "enable_overrides";
    static constexpr auto FILTER_CURATED_HITS = "filter_curated_hits";
    static constexpr auto ENABLE_SYNONYMS = "enable_synonyms";

    static constexpr auto MAX_CANDIDATES = "max_candidates";

    static constexpr auto INFIX = "infix";
    static constexpr auto MAX_EXTRA_PREFIX = "max_extra_prefix";
    static constexpr auto MAX_EXTRA_SUFFIX = "max_extra_suffix";

// strings under this length will be fully highlighted, instead of showing a snippet of relevant portion
    static constexpr auto SNIPPET_THRESHOLD = "snippet_threshold";

// the number of tokens that should surround the highlighted text
    static constexpr auto HIGHLIGHT_AFFIX_NUM_TOKENS = "highlight_affix_num_tokens";

// list of fields which will be highlighted fully without snippeting
    static constexpr auto HIGHLIGHT_FULL_FIELDS = "highlight_full_fields";
    static constexpr auto HIGHLIGHT_FIELDS = "highlight_fields";

    static constexpr auto HIGHLIGHT_START_TAG = "highlight_start_tag";
    static constexpr auto HIGHLIGHT_END_TAG = "highlight_end_tag";

    static constexpr auto PRIORITIZE_EXACT_MATCH = "prioritize_exact_match";
    static constexpr auto PRIORITIZE_TOKEN_POSITION = "prioritize_token_position";
    static constexpr auto PRE_SEGMENTED_QUERY = "pre_segmented_query";

    static constexpr auto SEARCH_CUTOFF_MS = "search_cutoff_ms";
    static constexpr auto EXHAUSTIVE_SEARCH = "exhaustive_search";
    static constexpr auto SPLIT_JOIN_TOKENS = "split_join_tokens";

    static constexpr auto TEXT_MATCH_TYPE = "text_match_type";

    static constexpr auto ENABLE_HIGHLIGHT_V1 = "enable_highlight_v1";

    static constexpr auto FACET_SAMPLE_PERCENT = "facet_sample_percent";
    static constexpr auto FACET_SAMPLE_THRESHOLD = "facet_sample_threshold";

    static constexpr auto CONVERSATION = "conversation";
    static constexpr auto CONVERSATION_ID = "conversation_id";
    static constexpr auto SYSTEM_PROMPT = "system_prompt";
    static constexpr auto CONVERSATION_MODEL_ID = "conversation_model_id";

    static constexpr auto DROP_TOKENS_MODE = "drop_tokens_mode";
    static constexpr auto PRIORITIZE_NUM_MATCHING_FIELDS = "prioritize_num_matching_fields";
    static constexpr auto OVERRIDE_TAGS = "override_tags";

    static constexpr auto VOICE_QUERY = "voice_query";

    static constexpr auto ENABLE_TYPOS_FOR_NUMERICAL_TOKENS = "enable_typos_for_numerical_tokens";
    static constexpr auto ENABLE_TYPOS_FOR_ALPHA_NUMERICAL_TOKENS = "enable_typos_for_alpha_numerical_tokens";
    static constexpr auto ENABLE_LAZY_FILTER = "enable_lazy_filter";
    static constexpr auto MAX_FILTER_BY_CANDIDATES = "max_filter_by_candidates";

    static constexpr auto SYNONYM_PREFIX = "synonym_prefix";
    static constexpr auto SYNONYM_NUM_TYPOS = "synonym_num_typos";

//query time flag to enable analyitcs for that query
    static constexpr auto ENABLE_ANALYTICS = "enable_analytics";

//for hybrid search, compute text_match_score for only vector search results and vector_distance for only text_match results
    static constexpr auto RERANK_HYBRID_MATCHES = "rerank_hybrid_matches";

    static constexpr auto VALIDATE_FIELD_NAMES = "validate_field_names";

    std::string raw_query;
    std::vector<std::string> search_fields;
    std::string filter_query;
    std::vector<std::string> facet_fields;
    std::vector<sort_by> sort_fields;
    std::vector<uint32_t> num_typos;
    size_t per_page;
    size_t page;
    token_ordering token_order;
    std::vector<bool> prefixes;
    size_t drop_tokens_threshold;
    spp::sparse_hash_set<std::string> include_fields;
    spp::sparse_hash_set<std::string> exclude_fields;
    size_t max_facet_values;
    std::string simple_facet_query;
    size_t snippet_threshold;
    size_t highlight_affix_num_tokens;
    std::string highlight_full_fields;
    size_t typo_tokens_threshold;
    std::string pinned_hits_str;
    std::string hidden_hits_str;
    std::vector<std::string> group_by_fields;
    size_t group_limit;
    std::string highlight_start_tag;
    std::string highlight_end_tag;
    std::vector<uint32_t> query_by_weights;
    size_t limit_hits;
    bool prioritize_exact_match;
    bool pre_segmented_query;
    bool enable_overrides;
    std::string highlight_fields;
    bool exhaustive_search;
    size_t search_cutoff_ms;
    size_t min_len_1typo;
    size_t min_len_2typo;
    enable_t split_join_tokens;
    size_t max_candidates;
    std::vector<enable_t> infixes;
    size_t max_extra_prefix;
    size_t max_extra_suffix;
    size_t facet_query_num_typos;
    bool filter_curated_hits_option;
    bool prioritize_token_position;
    std::string vector_query;
    bool enable_highlight_v1;
    uint64_t start_ts;
    text_match_type_t match_type;
    size_t facet_sample_percent;
    size_t facet_sample_threshold;
    size_t offset;
    std::string facet_strategy;
    size_t remote_embedding_timeout_ms;
    size_t remote_embedding_num_tries;
    std::string stopwords_set;
    std::vector<std::string> facet_return_parent;
    std::vector<ref_include_exclude_fields> ref_include_exclude_fields_vec;
    std::string drop_tokens_mode_str;
    bool prioritize_num_matching_fields;
    bool group_missing_values;
    bool conversation;
    std::string conversation_model_id;
    std::string conversation_id;
    std::string override_tags;
    std::string voice_query;
    bool enable_typos_for_numerical_tokens;
    bool enable_synonyms;
    bool synonym_prefix;
    size_t synonym_num_typos;
    bool enable_lazy_filter;
    bool enable_typos_for_alpha_numerical_tokens;
    size_t max_filter_by_candidates;
    bool rerank_hybrid_matches;
    bool enable_analytics;
    bool validate_field_names;

    Topster* raw_result_kvs = nullptr;
    Topster* override_result_kvs = nullptr;

    collection_search_args(std::string raw_query, std::vector<std::string> search_fields, std::string filter_query,
                           std::vector<std::string> facet_fields, std::vector<sort_by> sort_fields,
                           std::vector<uint32_t> num_typos, size_t per_page, size_t page, token_ordering token_order,
                           std::vector<bool> prefixes, size_t drop_tokens_threshold,
                           spp::sparse_hash_set<std::string> include_fields, spp::sparse_hash_set<std::string> exclude_fields,
                           size_t max_facet_values, std::string simple_facet_query, size_t snippet_threshold,
                           size_t highlight_affix_num_tokens, std::string highlight_full_fields,
                           size_t typo_tokens_threshold, std::string pinned_hits_str, std::string hidden_hits_str,
                           std::vector<std::string> group_by_fields, size_t group_limit,
                           std::string highlight_start_tag, std::string highlight_end_tag,
                           std::vector<uint32_t> query_by_weights, size_t limit_hits, bool prioritize_exact_match,
                           bool pre_segmented_query, bool enable_overrides, std::string highlight_fields,
                           bool exhaustive_search, size_t search_cutoff_ms, size_t min_len_1typo, size_t min_len_2typo,
                           enable_t split_join_tokens, size_t max_candidates, std::vector<enable_t> infixes,
                           size_t max_extra_prefix, size_t max_extra_suffix, size_t facet_query_num_typos,
                           bool filter_curated_hits_option, bool prioritize_token_position, std::string vector_query,
                           bool enable_highlight_v1, uint64_t start_ts, text_match_type_t match_type,
                           size_t facet_sample_percent, size_t facet_sample_threshold, size_t offset,
                           std::string facet_strategy, size_t remote_embedding_timeout_ms, size_t remote_embedding_num_tries,
                           std::string stopwords_set, std::vector<std::string> facet_return_parent,
                           std::vector<ref_include_exclude_fields> ref_include_exclude_fields_vec,
                           std::string drop_tokens_mode_str, bool prioritize_num_matching_fields, bool group_missing_values,
                           bool conversation, std::string conversation_model_id, std::string conversation_id,
                           std::string override_tags, std::string voice_query, bool enable_typos_for_numerical_tokens,
                           bool enable_synonyms, bool synonym_prefix, size_t synonym_num_typos, bool enable_lazy_filter,
                           bool enable_typos_for_alpha_numerical_tokens, size_t max_filter_by_candidates,
                           bool rerank_hybrid_matches, bool enable_analytics, bool validate_field_names) :
raw_query(std::move(raw_query)), search_fields(std::move(search_fields)), filter_query(std::move(filter_query)),
facet_fields(std::move(facet_fields)), sort_fields(std::move(sort_fields)),
num_typos(std::move(num_typos)), per_page(per_page), page(page), token_order(token_order),
prefixes(std::move(prefixes)), drop_tokens_threshold(drop_tokens_threshold),
include_fields(std::move(include_fields)), exclude_fields(std::move(exclude_fields)),
max_facet_values(max_facet_values), simple_facet_query(std::move(simple_facet_query)), snippet_threshold(snippet_threshold),
highlight_affix_num_tokens(highlight_affix_num_tokens), highlight_full_fields(std::move(highlight_full_fields)),
typo_tokens_threshold(typo_tokens_threshold), pinned_hits_str(std::move(pinned_hits_str)), hidden_hits_str(std::move(hidden_hits_str)),
group_by_fields(std::move(group_by_fields)), group_limit(group_limit),
highlight_start_tag(std::move(highlight_start_tag)), highlight_end_tag(std::move(highlight_end_tag)),
query_by_weights(std::move(query_by_weights)), limit_hits(limit_hits), prioritize_exact_match(prioritize_exact_match),
pre_segmented_query(pre_segmented_query), enable_overrides(enable_overrides), highlight_fields(std::move(highlight_fields)),
exhaustive_search(exhaustive_search), search_cutoff_ms(search_cutoff_ms), min_len_1typo(min_len_1typo), min_len_2typo(min_len_2typo),
split_join_tokens(split_join_tokens), max_candidates(max_candidates), infixes(std::move(infixes)),
max_extra_prefix(max_extra_prefix), max_extra_suffix(max_extra_suffix), facet_query_num_typos(facet_query_num_typos),
filter_curated_hits_option(filter_curated_hits_option), prioritize_token_position(prioritize_token_position), vector_query(std::move(vector_query)),
enable_highlight_v1(enable_highlight_v1), start_ts(start_ts), match_type(match_type),
facet_sample_percent(facet_sample_percent), facet_sample_threshold(facet_sample_threshold), offset(offset),
facet_strategy(std::move(facet_strategy)), remote_embedding_timeout_ms(remote_embedding_timeout_ms), remote_embedding_num_tries(remote_embedding_num_tries),
stopwords_set(std::move(stopwords_set)), facet_return_parent(std::move(facet_return_parent)),
ref_include_exclude_fields_vec(std::move(ref_include_exclude_fields_vec)),
drop_tokens_mode_str(std::move(drop_tokens_mode_str)), prioritize_num_matching_fields(prioritize_num_matching_fields), group_missing_values(group_missing_values),
conversation(conversation), conversation_model_id(std::move(conversation_model_id)), conversation_id(std::move(conversation_id)),
override_tags(std::move(override_tags)), voice_query(std::move(voice_query)), enable_typos_for_numerical_tokens(enable_typos_for_numerical_tokens),
enable_synonyms(enable_synonyms), synonym_prefix(synonym_prefix), synonym_num_typos(synonym_num_typos), enable_lazy_filter(enable_lazy_filter),
enable_typos_for_alpha_numerical_tokens(enable_typos_for_alpha_numerical_tokens), max_filter_by_candidates(max_filter_by_candidates),
rerank_hybrid_matches(rerank_hybrid_matches), enable_analytics(enable_analytics), validate_field_names(validate_field_names) {}

    collection_search_args() = default;

    ~collection_search_args() {
        delete raw_result_kvs;
        delete override_result_kvs;
    }

    static Option<bool> init(std::map<std::string, std::string>& req_params,
                             const uint32_t& coll_num_documents,
                             const std::string& stopwords_set,
                             const uint64_t& start_ts,
                             collection_search_args& args);
};

// Singleton, for managing meta information of all collections and house keeping
class CollectionManager {
private:
    mutable std::shared_mutex mutex;

    mutable std::shared_mutex noop_coll_mutex;

    Store *store;
    ThreadPool* thread_pool;

    AuthManager auth_manager;

    spp::sparse_hash_map<std::string, Collection*> collections;

    spp::sparse_hash_map<uint32_t, std::string> collection_id_names;

    spp::sparse_hash_map<std::string, std::string> collection_symlinks;

    spp::sparse_hash_map<std::string, nlohmann::json> preset_configs;

    // Auto incrementing ID assigned to each collection
    // Using a ID instead of a collection's name makes renaming possible
    std::atomic<uint32_t> next_collection_id;

    std::string bootstrap_auth_key;

    std::atomic<float> max_memory_ratio;

    std::atomic<bool>* quit;

    // All the references to a particular collection are stored until it is created.
    std::map<std::string, std::set<reference_info_t>> referenced_in_backlog;

    CollectionManager();

    ~CollectionManager() = default;

    static std::string get_first_index_error(const std::vector<index_record>& index_records) {
        for(const auto & index_record: index_records) {
            if(!index_record.indexed.ok()) {
                return index_record.indexed.error();
            }
        }

        return "";
    }

public:
    static constexpr const size_t DEFAULT_NUM_MEMORY_SHARDS = 4;

    static constexpr const char* NEXT_COLLECTION_ID_KEY = "$CI";
    static constexpr const char* SYMLINK_PREFIX = "$SL";
    static constexpr const char* PRESET_PREFIX = "$PS";

    uint16_t filter_by_max_ops;

    static CollectionManager & get_instance() {
        static CollectionManager instance;
        return instance;
    }

    CollectionManager(CollectionManager const&) = delete;
    void operator=(CollectionManager const&) = delete;

    static Collection* init_collection(const nlohmann::json & collection_meta,
                                       const uint32_t collection_next_seq_id,
                                       Store* store,
                                       float max_memory_ratio,
                                       spp::sparse_hash_map<std::string, std::string>& referenced_in,
                                       spp::sparse_hash_map<std::string, std::set<reference_pair_t>>& async_referenced_ins);

    static Option<bool> load_collection(const nlohmann::json& collection_meta,
                                        const size_t batch_size,
                                        const StoreStatus& next_coll_id_status,
                                        const std::atomic<bool>& quit,
                                        spp::sparse_hash_map<std::string, std::string>& referenced_in,
                                        spp::sparse_hash_map<std::string, std::set<reference_pair_t>>& async_referenced_ins);

    Option<Collection*> clone_collection(const std::string& existing_name, const nlohmann::json& req_json);

    void add_to_collections(Collection* collection);

    Option<std::vector<Collection*>> get_collections(uint32_t limit = 0, uint32_t offset = 0,
                                                     const std::vector<std::string>& api_key_collections = {}) const;

    std::vector<std::string> get_collection_names() const;

    Collection* get_collection_unsafe(const std::string & collection_name) const;

    // PUBLICLY EXPOSED API

    void init(Store *store, ThreadPool* thread_pool, const float max_memory_ratio,
              const std::string & auth_key, std::atomic<bool>& quit,
              const uint16_t& filter_by_max_operations = Config::FILTER_BY_DEFAULT_OPERATIONS);

    // only for tests!
    void init(Store *store, const float max_memory_ratio, const std::string & auth_key, std::atomic<bool>& exit,
              const uint16_t& filter_by_max_operations = Config::FILTER_BY_DEFAULT_OPERATIONS);

    Option<bool> load(const size_t collection_batch_size, const size_t document_batch_size);

    // frees in-memory data structures when server is shutdown - helps us run a memory leak detector properly
    void dispose();

    bool auth_key_matches(const string& req_auth_key, const string& action,
                          const std::vector<collection_key_t>& collection_keys,
                          std::map<std::string, std::string>& params,
                          std::vector<nlohmann::json>& embedded_params_vec) const;

    static Option<Collection*> create_collection(nlohmann::json& req_json);

    Option<Collection*> create_collection(const std::string& name, const size_t num_memory_shards,
                                          const std::vector<field> & fields,
                                          const std::string & default_sorting_field="",
                                          const uint64_t created_at = static_cast<uint64_t>(std::time(nullptr)),
                                          const std::string& fallback_field_type = "",
                                          const std::vector<std::string>& symbols_to_index = {},
                                          const std::vector<std::string>& token_separators = {},
                                          const bool enable_nested_fields = false, std::shared_ptr<VQModel> model = nullptr,
                                          const nlohmann::json& metadata = {});

    locked_resource_view_t<Collection> get_collection(const std::string & collection_name) const;

    locked_resource_view_t<Collection> get_collection_with_id(uint32_t collection_id) const;

    Option<nlohmann::json> get_collection_summaries(uint32_t limit = 0 , uint32_t offset = 0,
                                                    const std::vector<std::string>& exclude_fields = {},
                                                    const std::vector<std::string>& api_key_collections = {}) const;

    Option<nlohmann::json> drop_collection(const std::string& collection_name,
                                           const bool remove_from_store = true,
                                           const bool compact_store = true);

    uint32_t get_next_collection_id() const;

    static std::string get_symlink_key(const std::string & symlink_name);

    static std::string get_preset_key(const std::string & preset_name);

    Store* get_store();

    ThreadPool* get_thread_pool() const;

    AuthManager& getAuthManager();

    static Option<bool> do_search(std::map<std::string, std::string>& req_params,
                                  nlohmann::json& embedded_params,
                                  std::string& results_json_str,
                                  uint64_t start_ts);

    static Option<bool> do_union(std::map<std::string, std::string>& req_params,
                                 std::vector<nlohmann::json>& embedded_params_vec, nlohmann::json searches,
                                 nlohmann::json& response, uint64_t start_ts);

    static bool parse_sort_by_str(std::string sort_by_str, std::vector<sort_by>& sort_fields);

    // symlinks
    Option<std::string> resolve_symlink(const std::string & symlink_name) const;

    spp::sparse_hash_map<std::string, std::string> get_symlinks() const;

    Option<bool> upsert_symlink(const std::string & symlink_name, const std::string & collection_name);

    Option<bool> delete_symlink(const std::string & symlink_name);

    // presets
    spp::sparse_hash_map<std::string, nlohmann::json> get_presets() const;

    Option<bool> get_preset(const std::string & preset_name, nlohmann::json& preset) const;

    Option<bool> upsert_preset(const std::string & preset_name, const nlohmann::json& preset_config);

    Option<bool> delete_preset(const std::string & preset_name);

    void add_referenced_in_backlog(const std::string& collection_name, reference_info_t&& ref_info);

    std::map<std::string, std::set<reference_info_t>> _get_referenced_in_backlog() const;

    void process_embedding_field_delete(const std::string& model_name);

    static void _populate_referenced_ins(const std::string& collection_meta_json,
                                         std::map<std::string, spp::sparse_hash_map<std::string, std::string>>& referenced_ins,
                                         std::map<std::string, spp::sparse_hash_map<std::string, std::set<reference_pair_t>>>& async_referenced_ins);

    std::unordered_set<std::string> get_collection_references(const std::string& coll_name);

    bool is_valid_api_key_collection(const std::vector<std::string>& api_key_collections, Collection* coll) const;

    Option<bool> update_collection_metadata(const std::string& collection, const nlohmann::json& metadata);
};
