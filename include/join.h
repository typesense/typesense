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
    id_list_t* seq_ids = nullptr;

    negate_left_join_t() = default;
};

struct cascade_remove_node_t {
    std::shared_ptr<Collection> coll_ptr;
    std::unique_lock<std::shared_mutex> lock;
    std::vector<reference_info_t> ref_infos{};
    std::vector<cascade_remove_node_t*> nested_references{};
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

    static Option<bool> single_value_filter_query(nlohmann::json& document, const std::string& field_name,
                                                  const std::string& ref_field_type, std::string& filter_value,
                                                  const bool& is_reference_value = true);

    template <typename F>
    static Option<bool> negate_left_join(const std::string& ref_collection_name, const bool& is_reference_array_field,
                                         F&& get_related_child_ids,
                                         std::set<uint32_t>& unique_doc_ids,
                                         negate_left_join_t& negate_left_join_info,
                                         filter_result_t& filter_result);
};

template <typename F>
Option<bool> Join::negate_left_join(const std::string& ref_collection_name, const bool& is_reference_array_field,
                                    F&& get_related_child_ids,
                                    std::set<uint32_t>& unique_doc_ids,
                                    negate_left_join_t& negate_left_join_info,
                                    filter_result_t& filter_result) {
    std::vector<uint32_t> doc_ids;
    std::copy(unique_doc_ids.begin(), unique_doc_ids.end(), std::back_inserter(doc_ids));
    unique_doc_ids.clear();

    // If we have 3 products: product_a, product_b, product_c
    // and products_viewed like:
    // user_a:  [product_a]
    // user_b:  [product_a, product_b]
    // We should return product_b and product_c for "Products not seen by user_a".
    // `unique_doc_ids` has the list of products viewed by user_a, `seq_ids - unique_doc_ids` will be the products not viewed.
    uint32_t* all_doc_ids = negate_left_join_info.seq_ids->uncompress();
    filter_result.count = ArrayUtils::exclude_scalar(all_doc_ids, negate_left_join_info.seq_ids->num_ids(), doc_ids.data(),
                                                     doc_ids.size(), &filter_result.docs);
    delete[] all_doc_ids;

    filter_result.coll_to_references = new std::map<std::string, reference_filter_result_t>[filter_result.count] {};
    for (size_t i = 0; i < filter_result.count; i++) {
        const auto& doc_id = filter_result.docs[i];

        std::vector<uint32_t> related_doc_ids;
        get_related_child_ids(doc_id, related_doc_ids);

        // product_c will not have any references.
        if (related_doc_ids.empty()) {
            continue;
        }

        auto related_docs = new uint32_t[related_doc_ids.size()];
        std::copy(related_doc_ids.begin(), related_doc_ids.end(), related_docs);
        auto references = reference_filter_result_t(related_doc_ids.size(), related_docs, is_reference_array_field);
        filter_result.coll_to_references[i][ref_collection_name] = std::move(references);
    }

    return Option<bool>(true);
}
