#pragma once

#include "option.h"
#include "json.hpp"
#include "tsl/htrie_map.h"
#include "field.h"
#include "tsl/htrie_set.h"
#include "filter_result_iterator.h"

struct reference_info_t {
    std::string collection;
    std::string field;
    bool is_async;

    std::string referenced_field_name;

    reference_info_t(std::string collection, std::string field, bool is_async, std::string referenced_field_name = "") :
            collection(std::move(collection)), field(std::move(field)), is_async(is_async),
            referenced_field_name(std::move(referenced_field_name)) {}

    bool operator < (const reference_info_t& pair) const {
        return collection < pair.collection;
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
};