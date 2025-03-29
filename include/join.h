#pragma once

#include "option.h"
#include "json.hpp"
#include "tsl/htrie_map.h"
#include "field.h"
#include "tsl/htrie_set.h"
#include "filter_result_iterator.h"

struct base_reference_info_t {
    std::string collection;
    std::string field;

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

    static Option<bool> parse_reference_filter(const std::string& filter_query, std::queue<std::string>& tokens, size_t& index,
                                               std::set<std::string>& ref_collection_names);

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
};