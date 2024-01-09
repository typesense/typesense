#pragma once

#include "option.h"
#include <cctype>
#include "json.hpp"
#include "tsl/htrie_map.h"
#include "field.h"

class validator_t {
public:

    static Option<uint32_t> validate_index_in_memory(nlohmann::json &document, uint32_t seq_id,
                                                     const std::string & default_sorting_field,
                                                     const tsl::htrie_map<char, field> & search_schema,
                                                     const tsl::htrie_map<char, field> & embedding_fields,
                                                     const index_operation_t op,
                                                     const bool is_update,
                                                     const std::string& fallback_field_type,
                                                     const DIRTY_VALUES& dirty_values, const bool validate_embedding_fields = true);


    static Option<uint32_t> coerce_element(const field& a_field, nlohmann::json& document,
                                           nlohmann::json& doc_ele,
                                           const std::string& fallback_field_type,
                                           const DIRTY_VALUES& dirty_values);

    static Option<uint32_t> coerce_string(const DIRTY_VALUES& dirty_values, const std::string& fallback_field_type,
                                          const field& a_field, nlohmann::json &document,
                                          const std::string &field_name,
                                          nlohmann::json::iterator& array_iter,
                                          bool is_array,
                                          bool& array_ele_erased);

    static Option<uint32_t> coerce_int32_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                           const std::string &field_name,
                                           nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_int64_t(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                           const std::string &field_name,
                                           nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_float(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                         const std::string &field_name,
                                         nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_bool(const DIRTY_VALUES& dirty_values, const field& a_field, nlohmann::json &document,
                                        const std::string &field_name,
                                        nlohmann::json::iterator& array_iter, bool is_array, bool& array_ele_erased);

    static Option<uint32_t> coerce_geopoint(const DIRTY_VALUES& dirty_values, const field& a_field,
                                            nlohmann::json &document, const std::string &field_name,
                                            nlohmann::json& lat, nlohmann::json& lng,
                                            nlohmann::json::iterator& array_iter,
                                            bool is_array, bool& array_ele_erased);

    static Option<bool> validate_embed_fields(const nlohmann::json& document, 
                                        const tsl::htrie_map<char, field>& embedding_fields, 
                                        const tsl::htrie_map<char, field> & search_schema,
                                        const bool& is_update);

};