#pragma once

#include "option.h"
#include "json.hpp"
#include "tsl/htrie_map.h"
#include "field.h"
#include "tsl/htrie_set.h"
#include "filter_result_iterator.h"

struct base_reference_info_t {
    std::string collection{};
    std::string field{};

    base_reference_info_t() = default;

    base_reference_info_t(std::string collection, std::string field) :
    collection(std::move(collection)), field(std::move(field)) {}

    bool operator < (const base_reference_info_t& other) const noexcept {
        if (collection == other.collection) {
            return field < other.field;
        }

        return collection < other.collection;
    }
};

struct reference_info_t: base_reference_info_t {
    bool is_async;
    std::string referenced_field_name;
    struct field referenced_field{};

    reference_info_t(std::string collection, std::string field, bool is_async, std::string referenced_field_name = "") :
            base_reference_info_t(std::move(collection), std::move(field)), is_async(is_async),
            referenced_field_name(std::move(referenced_field_name)) {}

    reference_info_t(const nlohmann::json& json): reference_info_t(json["collection"],
                                                                   json["field"],
                                                                   json["is_async"],
                                                                   json["referenced_field_name"]) {
        referenced_field = field::field_from_json(json["referenced_field"]);
    }

    static nlohmann::json to_json(const reference_info_t& ref_info) {
        nlohmann::json json;
        json["collection"] = ref_info.collection;
        json["field"] = ref_info.field;
        json["is_async"] = ref_info.is_async;
        json["referenced_field_name"] = ref_info.referenced_field_name;
        json["referenced_field"] = field::field_to_json_field(ref_info.referenced_field);
        return json;
    }
};

struct update_reference_info_t: base_reference_info_t {
    struct field referenced_field;

    update_reference_info_t(std::string collection, std::string field, struct field referenced_field) :
            base_reference_info_t(std::move(collection), std::move(field)), referenced_field(std::move(referenced_field)) {}
};

struct ref_include_collection_names_t {
    std::set<std::string> collection_names;
    ref_include_collection_names_t* nested_include = nullptr;

    ~ref_include_collection_names_t() {
        delete nested_include;
    }
};

struct negate_left_join_t {
    bool is_negate_join = false;
    size_t excluded_ids_size = 0;
    std::unique_ptr<uint32_t []> excluded_ids = nullptr;

    negate_left_join_t() = default;
};

struct Hasher32 {
    // Helps to spread the hash key and is used for sort index.
    // see: https://github.com/greg7mdp/sparsepp/issues/21#issuecomment-270816275
    size_t operator()(uint32_t k) const { return (k ^ 2166136261U)  * 16777619UL; }
};

enum enable_t {
    always,
    fallback,
    off
};

struct search_field_t {
    std::string name;
    std::string str_name;   // for lookup of non-string fields in art index
    size_t weight;
    size_t num_typos;
    bool prefix;
    enable_t infix;
    std::string referenced_collection_name{};
    base_reference_info_t ref_info{};

    search_field_t(const std::string& name, const std::string& str_name, size_t weight, size_t num_typos,
                   bool prefix, enable_t infix):
            name(name), str_name(str_name), weight(weight), num_typos(num_typos), prefix(prefix), infix(infix) { }
};

class Join {
public:

    static Option<bool> add_reference_helper_fields(nlohmann::json& document,
                                                    const tsl::htrie_map<char, field>& schema,
                                                    const spp::sparse_hash_map<std::string, reference_info_t>& reference_fields,
                                                    tsl::htrie_set<char>& object_reference_helper_fields,
                                                    const bool& is_update);

    static Option<bool> prune_ref_doc(nlohmann::json& doc,
                                      const reference_filter_result_t& references,
                                      const tsl::htrie_set<char>& ref_include_fields_full,
                                      const tsl::htrie_set<char>& ref_exclude_fields_full,
                                      const bool& is_reference_array,
                                      const ref_include_exclude_fields& ref_include_exclude);

    static Option<bool> include_references(nlohmann::json& doc, const uint32_t& seq_id, Collection *const collection,
                                           const std::map<std::string, reference_filter_result_t>& reference_filter_results,
                                           const std::vector<ref_include_exclude_fields>& ref_include_exclude_fields_vec,
                                           const nlohmann::json& original_doc);

    static Option<bool> parse_reference_filter(const std::string& filter_query, std::queue<std::string>& tokens, size_t& index);

    static Option<bool> split_reference_include_exclude_fields(const std::string& include_exclude_fields,
                                                               size_t& index, std::string& token);

    static void get_reference_collection_names(const std::string& filter_query, ref_include_collection_names_t*& ref_include);

    // Separate out the reference includes and excludes into `ref_include_exclude_fields_vec`.
    static Option<bool> initialize_ref_include_exclude_fields_vec(const std::string& filter_query,
                                                                  std::vector<std::string>& include_fields_vec,
                                                                  std::vector<std::string>& exclude_fields_vec,
                                                                  std::vector<ref_include_exclude_fields>& ref_include_exclude_fields_vec);

    [[nodiscard]] static bool merge_join_conditions(string& embedded_filter, string& query_filter);

    static Option<bool> single_value_filter_query(nlohmann::json& document, const std::string& field_name,
                                                  const std::string& ref_field_type, std::string& filter_value,
                                                  const bool& is_reference_value = true);

    static void aggregate_nested_references(single_filter_result_t *const reference_result,
                                            reference_filter_result_t& ref_filter_result);

    static void negate_left_join(id_list_t* const seq_ids, std::unique_ptr<uint32_t[]>& reference_docs, uint32_t& reference_docs_count,
                                 std::function<std::vector<uint32_t>(const uint32_t& ref_doc_id)> const& get_doc_id,
                                 const bool& is_match_all_ids_filter, std::vector<std::pair<uint32_t, uint32_t>>& id_pairs,
                                 std::set<uint32_t>& unique_doc_ids, negate_left_join_t& negate_left_join_info);

    static void get_ref_index_ids(num_tree_t& ref_index, const uint32_t& reference_doc_id,
                                  std::vector<std::pair<uint32_t, uint32_t>>& id_pairs,
                                  std::set<uint32_t>& unique_doc_ids, const bool& is_normal_join);

    static void get_ref_index_ids(const spp::sparse_hash_map<uint32_t, int64_t, Hasher32>& ref_index,
                                  const uint32_t& reference_doc_id,
                                  std::vector<std::pair<uint32_t, uint32_t>>& id_pairs,
                                  std::set<uint32_t>& unique_doc_ids, const bool& is_normal_join);

    static void do_nested_join(std::function<std::pair<size_t, uint32_t*>(const uint32_t& ref_doc_id)> const& get_related_ids,
                               const uint32_t& count, uint32_t const* const& reference_docs,
                               filter_result_t* ref_filter_result, const std::string& ref_collection_name,
                               filter_result_t& filter_result);

    static void process_related_ids(std::vector<std::pair<uint32_t, uint32_t>> id_pairs, const size_t& unique_doc_ids_size,
                                    const std::string& ref_collection_name, filter_result_t& filter_result);

    static void get_ref_field_token_its(const std::string& collection_name, const search_field_t& search_field,
                                        const size_t& field_id, const std::string& token_str,
                                        const spp::sparse_hash_map<std::string, num_tree_t*>& numerical_index,
                                        std::vector<art_leaf*>& query_suggestion,
                                        std::vector<std::unique_ptr<posting_list_t::base_iterator_t>>& its,
                                        std::vector<posting_list_t*>& expanded_plists);
};