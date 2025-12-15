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
    bool is_mutual_reference = false;

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
    bool is_async{};
    bool is_array{};
    std::string referenced_field_name{};
    struct field referenced_field{};

    reference_info_t() = default;

    reference_info_t(std::string collection, std::string field, bool is_async, bool is_array,
                     std::string referenced_field_name = "") :
            base_reference_info_t(std::move(collection), std::move(field)), is_async(is_async), is_array(is_array),
            referenced_field_name(std::move(referenced_field_name)) {}

    reference_info_t(const nlohmann::json& json): reference_info_t(json["collection"],
                                                                   json["field"],
                                                                   json["is_async"],
                                                                   json["is_array"],
                                                                   json["referenced_field_name"]) {
        referenced_field = field::field_from_json(json["referenced_field"]);
    }

    static nlohmann::json to_json(const reference_info_t& ref_info) {
        nlohmann::json json;
        json["collection"] = ref_info.collection;
        json["field"] = ref_info.field;
        json["is_async"] = ref_info.is_async;
        json["is_array"] = ref_info.is_array;
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

class Join {
public:

    /// Value used when async_reference is true and a reference doc is not found.
    static constexpr int64_t reference_helper_sentinel_value = UINT32_MAX;

    static Option<bool> populate_reference_helper_fields(nlohmann::json& document,
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

    static Option<bool> include_references(nlohmann::json& doc, const uint32_t& seq_id, const std::string& collection_name,
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

    template <typename F>
    static void negate_left_join(id_list_t* const seq_ids, uint32_t*& reference_docs, uint32_t& reference_docs_count,
                                 F&& get_doc_id, const bool& is_match_all_ids_filter, std::vector<std::pair<uint32_t, uint32_t>>& id_pairs,
                                 std::set<uint32_t>& unique_doc_ids,
                                 negate_left_join_t& negate_left_join_info);
};

template <typename F>
void Join::negate_left_join(id_list_t* const seq_ids, uint32_t*& reference_docs, uint32_t& reference_docs_count,
                            F&& get_doc_id, const bool& is_match_all_ids_filter, std::vector<std::pair<uint32_t, uint32_t>>& id_pairs,
                            std::set<uint32_t>& unique_doc_ids,
                            negate_left_join_t& negate_left_join_info) {
    uint32_t* negate_reference_docs = nullptr;
    size_t negate_index = 0;
    std::set<uint32_t> unique_negate_doc_ids;

    // If the negate join is on all ids like !$CollName(id:*), we don't need to collect any references.
    if (!is_match_all_ids_filter) {
        auto it = seq_ids->new_iterator();
        negate_reference_docs = new uint32_t[seq_ids->num_ids() - reference_docs_count];
        for (size_t i = 0; i < reference_docs_count && it.valid(); i++) {
            while (it.valid() && it.id() < reference_docs[i]) {
                const auto &reference_doc_id = it.id();
                it.next();

                negate_reference_docs[negate_index++] = reference_doc_id;
                std::vector<uint32_t> doc_ids = get_doc_id(reference_doc_id);
                for (const auto &doc_id: doc_ids) {
                    // If we have 3 products: product_a, product_b, product_c
                    // and products_viewed like:
                    // user_a:  [product_a]
                    // user_b:  [product_a, product_b]
                    // We should return product_b and product_c for "Products not seen by user_a".
                    // So rejecting doc_id's already present in unique_doc_ids (product_a in the above example).
                    if (doc_id == Join::reference_helper_sentinel_value || unique_doc_ids.count(doc_id) != 0) {
                        continue;
                    }

                    id_pairs.emplace_back(doc_id, reference_doc_id);
                    unique_negate_doc_ids.insert(doc_id);
                }
            }
            if (!it.valid()) {
                break;
            }
            while (i + 1 < reference_docs_count &&
                   (reference_docs[i] + 1 == reference_docs[i + 1])) { // Skip consecutive ids.
                i++;
            }
            it.skip_to(reference_docs[i] + 1);
        }

        if (reference_docs_count > 0 && it.valid()) {
            it.skip_to(reference_docs[reference_docs_count - 1] + 1);
        }
        while (it.valid()) {
            const auto &reference_doc_id = it.id();
            it.next();

            negate_reference_docs[negate_index++] = reference_doc_id;
            std::vector<uint32_t> doc_ids = get_doc_id(reference_doc_id);
            for (const auto &doc_id: doc_ids) {
                // If we have 3 products: product_a, product_b, product_c
                // and products_viewed like:
                // user_a:  [product_a]
                // user_b:  [product_a, product_b]
                // We should return product_b and product_c for "Products not seen by user_a".
                // So rejecting doc_id's already present in unique_doc_ids (product_a in the above example).
                if (doc_id == Join::reference_helper_sentinel_value || unique_doc_ids.count(doc_id) != 0) {
                    continue;
                }

                id_pairs.emplace_back(doc_id, reference_doc_id);
                unique_negate_doc_ids.insert(doc_id);
            }
        }
    }

    delete [] reference_docs;
    reference_docs = negate_reference_docs;
    reference_docs_count = negate_index;

    // Main purpose of `negate_left_join_info.excluded_ids` is help identify the doc_ids that don't have any references.
    negate_left_join_info.excluded_ids_size = unique_doc_ids.size();
    negate_left_join_info.excluded_ids.reset(new uint32_t[unique_doc_ids.size()]);
    std::copy(unique_doc_ids.begin(), unique_doc_ids.end(), negate_left_join_info.excluded_ids.get());

    unique_doc_ids = unique_negate_doc_ids;
}