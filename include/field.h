#pragma once

#include <string>
#include "art.h"
#include "option.h"
#include "string_utils.h"
#include "json.hpp"

namespace field_types {
    // first field value indexed will determine the type
    static const std::string AUTO = "auto";

    static const std::string STRING = "string";
    static const std::string INT32 = "int32";
    static const std::string INT64 = "int64";
    static const std::string FLOAT = "float";
    static const std::string BOOL = "bool";
    static const std::string GEOPOINT = "geopoint";
    static const std::string STRING_ARRAY = "string[]";
    static const std::string INT32_ARRAY = "int32[]";
    static const std::string INT64_ARRAY = "int64[]";
    static const std::string FLOAT_ARRAY = "float[]";
    static const std::string BOOL_ARRAY = "bool[]";

    static bool is_string_or_array(const std::string type_def) {
        return type_def == "string*";
    }
}

namespace fields {
    static const std::string name = "name";
    static const std::string type = "type";
    static const std::string facet = "facet";
    static const std::string optional = "optional";
    static const std::string geo_resolution = "geo_resolution";
}

static const uint8_t DEFAULT_GEO_RESOLUTION = 7;
static const uint8_t FINEST_GEO_RESOLUTION = 15;

struct field {
    std::string name;
    std::string type;
    bool facet;
    bool optional;

    uint8_t geo_resolution;

    field(const std::string & name, const std::string & type, const bool facet):
        name(name), type(type), facet(facet), optional(false), geo_resolution(DEFAULT_GEO_RESOLUTION) {

    }

    field(const std::string & name, const std::string & type, const bool facet, const bool optional):
            name(name), type(type), facet(facet), optional(optional), geo_resolution(DEFAULT_GEO_RESOLUTION) {

    }

    field(const std::string & name, const std::string & type, const bool facet, const bool optional,
          const uint8_t geo_resolution):
            name(name), type(type), facet(facet), optional(optional), geo_resolution(geo_resolution) {

    }

    bool is_auto() const {
        return (type == field_types::AUTO);
    }

    bool is_single_integer() const {
        return (type == field_types::INT32 || type == field_types::INT64);
    }

    bool is_single_float() const {
        return (type == field_types::FLOAT);
    }

    bool is_single_bool() const {
        return (type == field_types::BOOL);
    }

    bool is_integer() const {
        return (type == field_types::INT32 || type == field_types::INT32_ARRAY ||
               type == field_types::INT64 || type == field_types::INT64_ARRAY);
    }

    bool is_int32() const {
        return (type == field_types::INT32 || type == field_types::INT32_ARRAY);
    }

    bool is_int64() const {
        return (type == field_types::INT64 || type == field_types::INT64_ARRAY);
    }

    bool is_float() const {
        return (type == field_types::FLOAT || type == field_types::FLOAT_ARRAY);
    }

    bool is_bool() const {
        return (type == field_types::BOOL || type == field_types::BOOL_ARRAY);
    }

    bool is_geopoint() const {
        return (type == field_types::GEOPOINT);
    }

    bool is_string() const {
        return (type == field_types::STRING || type == field_types::STRING_ARRAY);
    }

    bool is_facet() const {
        return facet;
    }

    bool is_array() const {
        return (type == field_types::STRING_ARRAY || type == field_types::INT32_ARRAY ||
                type == field_types::FLOAT_ARRAY ||
                type == field_types::INT64_ARRAY || type == field_types::BOOL_ARRAY);
    }

    bool is_singular() const {
        return !is_array();
    }

    bool has_numerical_index() const {
        return (type == field_types::INT32 || type == field_types::INT64 ||
                type == field_types::FLOAT || type == field_types::BOOL);
    }

    bool is_sortable() const {
        return is_single_integer() || is_single_float() || is_single_bool() || is_geopoint();
    }

    bool has_valid_type() const {
        bool is_basic_type = is_string() || is_integer() || is_float() || is_bool() || is_geopoint() || is_auto();
        if(!is_basic_type) {
            return field_types::is_string_or_array(type);
        }
        return true;
    }

    std::string faceted_name() const {
        return (facet && !is_string()) ? "_fstr_" + name : name;
    }

    static bool get_type(const nlohmann::json& obj, std::string& field_type) {
        if(obj.is_array()) {
            if(obj.empty()) {
                return false;
            }

            bool parseable = get_single_type(obj[0], field_type);
            if(!parseable) {
                return false;
            }

            field_type = field_type + "[]";
            return true;
        }

        if(obj.is_object()) {
            return false;
        }

        return get_single_type(obj, field_type);
    }

    static bool get_single_type(const nlohmann::json& obj, std::string& field_type) {
        if(obj.is_string()) {
            field_type = field_types::STRING;
            return true;
        }

        if(obj.is_number_float()) {
            field_type = field_types::FLOAT;
            return true;
        }

        if(obj.is_number_integer()) {
            field_type = field_types::INT64;
            return true;
        }

        if(obj.is_boolean()) {
            field_type = field_types::BOOL;
            return true;
        }

        return false;
    }

    static Option<bool> fields_to_json_fields(const std::vector<field> & fields,
                                              const std::string & default_sorting_field,
                                              nlohmann::json& fields_json) {
        bool found_default_sorting_field = false;

        for(const field & field: fields) {
            nlohmann::json field_val;
            field_val[fields::name] = field.name;
            field_val[fields::type] = field.type;
            field_val[fields::facet] = field.facet;
            field_val[fields::optional] = field.optional;
            if(field.is_geopoint()) {
                field_val[fields::geo_resolution] = field.geo_resolution;
            }

            fields_json.push_back(field_val);

            if(!field.has_valid_type()) {
                return Option<bool>(400, "Field `" + field.name +
                                                "` has an invalid data type `" + field.type +
                                                "`, see docs for supported data types.");
            }

            if(field.name == default_sorting_field && !(field.type == field_types::INT32 ||
                                                        field.type == field_types::INT64 ||
                                                        field.type == field_types::FLOAT)) {
                return Option<bool>(400, "Default sorting field `" + default_sorting_field +
                                                "` must be a single valued numerical field.");
            }

            if(field.name == default_sorting_field) {
                if(field.optional) {
                    return Option<bool>(400, "Default sorting field `" + default_sorting_field +
                                                    "` cannot be an optional field.");
                }

                found_default_sorting_field = true;
            }
        }

        if(!default_sorting_field.empty() && !found_default_sorting_field) {
            return Option<bool>(400, "Default sorting field is defined as `" + default_sorting_field +
                                            "` but is not found in the schema.");
        }

        return Option<bool>(true);
    }

    static Option<bool> json_fields_to_fields(nlohmann::json& fields_json,
                                              std::string& fallback_field_type,
                                              std::vector<field>& fields) {
        size_t num_auto_detect_fields = 0;

        for(nlohmann::json & field_json: fields_json) {
            if(!field_json.is_object() ||
               field_json.count(fields::name) == 0 || field_json.count(fields::type) == 0 ||
               !field_json.at(fields::name).is_string() || !field_json.at(fields::type).is_string()) {

                return Option<bool>(400, "Wrong format for `fields`. It should be an array of objects containing "
                            "`name`, `type`, `optional` and `facet` properties.");
            }

            if(field_json.count(fields::facet) != 0 && !field_json.at(fields::facet).is_boolean()) {
                return Option<bool>(400, std::string("The `facet` property of the field `") +
                            field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
            }

            if(field_json.count(fields::optional) != 0 && !field_json.at(fields::optional).is_boolean()) {
                return Option<bool>(400, std::string("The `optional` property of the field `") +
                                         field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
            }

            if(field_json.count(fields::geo_resolution) != 0) {
                if(!field_json.at(fields::geo_resolution).is_number_integer()) {
                    return Option<bool>(400, std::string("The `geo_resolution` property of the field `") +
                                             field_json[fields::name].get<std::string>() + std::string("` should be an integer."));
                }

                int field_geo_res = field_json.at(fields::geo_resolution).get<int>();
                if(field_geo_res < 0 || field_geo_res > 15) {
                    return Option<bool>(400, std::string("The `geo_resolution` property of the field `") +
                           field_json[fields::name].get<std::string>() + std::string("` should be between 0 and 15."));
                }
            }

            if(field_json["name"] == "*") {
                if(field_json.count("facet") == 0) {
                    field_json["facet"] = false;
                }

                if(field_json.count("optional") == 0) {
                    field_json["optional"] = true;
                }

                if(field_json["optional"] == false) {
                    return Option<bool>(400, "Field `*` must be an optional field.");
                }

                if(field_json["facet"] == true) {
                    return Option<bool>(400, "Field `*` cannot be a facet field.");
                }

                field fallback_field(field_json["name"], field_json["type"], field_json["facet"],
                                     field_json["optional"]);

                if(fallback_field.has_valid_type()) {
                    fallback_field_type = fallback_field.type;
                    num_auto_detect_fields++;
                } else {
                    return Option<bool>(400, "The `type` of field `*` is invalid.");
                }

                fields.emplace_back(fallback_field);
                continue;
            }

            if(field_json.count("facet") == 0) {
                field_json["facet"] = false;
            }

            if(field_json.count("optional") == 0) {
                field_json["optional"] = false;
            }

            fields.emplace_back(
                field(field_json["name"], field_json["type"], field_json["facet"], field_json["optional"])
            );
        }

        if(num_auto_detect_fields > 1) {
            return Option<bool>(400,"There can be only one field named `*`.");
        }

        return Option<bool>(true);
    }
};

struct filter {
    std::string field_name;
    std::vector<std::string> values;
    std::vector<NUM_COMPARATOR> comparators;

    static const std::string RANGE_OPERATOR() {
        return "..";
    }

    static Option<bool> validate_numerical_filter_value(field _field, const std::string& raw_value) {
        if(_field.is_int32() && !StringUtils::is_int32_t(raw_value)) {
            return Option<bool>(400, "Error with filter field `" + _field.name + "`: Not an int32.");
        }

        else if(_field.is_int64() && !StringUtils::is_int64_t(raw_value)) {
            return Option<bool>(400, "Error with filter field `" + _field.name + "`: Not an int64.");
        }

        else if(_field.is_float() && !StringUtils::is_float(raw_value)) {
            return Option<bool>(400, "Error with filter field `" + _field.name + "`: Not a float.");
        }

        return Option<bool>(true);
    }

    static Option<NUM_COMPARATOR> extract_num_comparator(std::string & comp_and_value) {
        auto num_comparator = EQUALS;

        if(StringUtils::is_integer(comp_and_value) || StringUtils::is_float(comp_and_value)) {
            num_comparator = EQUALS;
        }

        // the ordering is important - we have to compare 2-letter operators first
        else if(comp_and_value.compare(0, 2, "<=") == 0) {
            num_comparator = LESS_THAN_EQUALS;
        }

        else if(comp_and_value.compare(0, 2, ">=") == 0) {
            num_comparator = GREATER_THAN_EQUALS;
        }

        else if(comp_and_value.compare(0, 1, "<") == 0) {
            num_comparator = LESS_THAN;
        }

        else if(comp_and_value.compare(0, 1, ">") == 0) {
            num_comparator = GREATER_THAN;
        }

        else if(comp_and_value.find("..") != std::string::npos) {
            num_comparator = RANGE_INCLUSIVE;
        }

        else {
            return Option<NUM_COMPARATOR>(400, "Numerical field has an invalid comparator.");
        }

        if(num_comparator == LESS_THAN || num_comparator == GREATER_THAN) {
            comp_and_value = comp_and_value.substr(1);
        } else if(num_comparator == LESS_THAN_EQUALS || num_comparator == GREATER_THAN_EQUALS) {
            comp_and_value = comp_and_value.substr(2);
        }

        comp_and_value = StringUtils::trim(comp_and_value);

        return Option<NUM_COMPARATOR>(num_comparator);
    }
};

namespace sort_field_const {
    static const std::string name = "name";
    static const std::string order = "order";
    static const std::string asc = "ASC";
    static const std::string desc = "DESC";
    static const std::string text_match = "_text_match";
    static const std::string seq_id = "_seq_id";
}

struct sort_by {
    std::string name;
    std::string order;
    int64_t geopoint;

    sort_by(const std::string & name, const std::string & order): name(name), order(order), geopoint(0) {

    }

    sort_by(const std::string &name, const std::string &order, int64_t geopoint) :
            name(name), order(order), geopoint(geopoint) {

    }

    sort_by& operator=(sort_by other) {
        name = other.name;
        order = other.order;
        geopoint = other.geopoint;
        return *this;
    }
};

struct token_pos_cost_t {
    size_t pos;
    uint32_t cost;
};

struct facet_count_t {
    uint32_t count;
    spp::sparse_hash_set<uint64_t> groups;  // used for faceting grouped results

    // used to fetch the actual document and value for representation
    uint32_t doc_id;
    uint32_t array_pos;

    std::unordered_map<uint32_t, token_pos_cost_t> query_token_pos;
};

struct facet_stats_t {
    double fvmin = std::numeric_limits<double>::max(),
            fvmax = -std::numeric_limits<double>::min(),
            fvcount = 0,
            fvsum = 0;
};

struct facet {
    const std::string field_name;
    std::unordered_map<uint64_t, facet_count_t> result_map;
    facet_stats_t stats;

    facet(const std::string & field_name): field_name(field_name) {

    }
};

struct facet_query_t {
    std::string field_name;
    std::string query;
};

struct facet_value_t {
    std::string value;
    std::string highlighted;
    uint32_t count;
};

struct facet_hash_values_t {
    uint32_t length = 0;
    uint64_t* hashes = nullptr;

    uint64_t size() const {
        return length;
    }

    uint64_t back() const {
        return hashes[length - 1];
    }
};