#pragma once

#include <string>
#include <s2/s2latlng.h>
#include "art.h"
#include "option.h"
#include "string_utils.h"
#include "logger.h"
#include <sparsepp.h>
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
    static const std::string GEOPOINT_ARRAY = "geopoint[]";

    static bool is_string_or_array(const std::string& type_def) {
        return type_def == "string*";
    }
}

namespace fields {
    static const std::string name = "name";
    static const std::string type = "type";
    static const std::string facet = "facet";
    static const std::string optional = "optional";
    static const std::string index = "index";
    static const std::string sort = "sort";
    static const std::string locale = "locale";
}

struct field {
    std::string name;
    std::string type;
    bool facet;
    bool optional;
    bool index;
    std::string locale;
    bool sort;

    field(const std::string &name, const std::string &type, const bool facet, const bool optional = false,
          bool index = true, std::string locale = "", int sort = -1) :
            name(name), type(type), facet(facet), optional(optional), index(index), locale(locale) {

        if(sort != -1) {
            this->sort = bool(sort);
        } else {
            this->sort = is_num_sort_field();
        }
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

    bool is_single_geopoint() const {
        return (type == field_types::GEOPOINT);
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
        return (type == field_types::GEOPOINT || type == field_types::GEOPOINT_ARRAY);
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
                type == field_types::INT64_ARRAY || type == field_types::BOOL_ARRAY ||
                type == field_types::GEOPOINT_ARRAY);
    }

    bool is_singular() const {
        return !is_array();
    }

    bool is_dynamic() const {
         return is_dynamic(name, type);
    }

    static bool is_dynamic(const std::string& name, const std::string& type) {
        return type == "string*" || (name != ".*" && type == field_types::AUTO) || (name != ".*" && name.find(".*") != std::string::npos);
    }

    bool has_numerical_index() const {
        return (type == field_types::INT32 || type == field_types::INT64 ||
                type == field_types::FLOAT || type == field_types::BOOL);
    }

    bool is_num_sort_field() const {
        return (has_numerical_index() || is_geopoint());
    }

    bool is_sort_field() const {
        return is_num_sort_field() || (type == field_types::STRING);
    }

    bool is_num_sortable() const {
        return sort && is_num_sort_field();
    }

    bool is_str_sortable() const {
        return sort && type == field_types::STRING;
    }

    bool is_sortable() const {
        return is_num_sortable() || is_str_sortable();
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

        // Check for duplicates in field names
        std::map<std::string, std::vector<const field*>> unique_fields;

        for(const field & field: fields) {
            unique_fields[field.name].push_back(&field);

            if(field.name == "id") {
                continue;
            }

            nlohmann::json field_val;
            field_val[fields::name] = field.name;
            field_val[fields::type] = field.type;
            field_val[fields::facet] = field.facet;
            field_val[fields::optional] = field.optional;
            field_val[fields::sort] = field.sort;

            field_val[fields::locale] = field.locale;

            fields_json.push_back(field_val);

            if(!field.has_valid_type()) {
                return Option<bool>(400, "Field `" + field.name +
                                                "` has an invalid data type `" + field.type +
                                                "`, see docs for supported data types.");
            }

            if(field.name == default_sorting_field && !field.is_sortable()) {
                return Option<bool>(400, "Default sorting field `" + default_sorting_field +
                                                "` is not a sortable type.");
            }

            if(field.name == default_sorting_field) {
                if(field.optional) {
                    return Option<bool>(400, "Default sorting field `" + default_sorting_field +
                                                    "` cannot be an optional field.");
                }

                found_default_sorting_field = true;
            }

            if(field.is_dynamic() && !field.optional) {
                return Option<bool>(400, "Field `" + field.name + "` must be an optional field.");
            }

            if(!field.index && !field.optional) {
                return Option<bool>(400, "Field `" + field.name + "` must be optional since it is marked as non-indexable.");
            }

            if(field.name == ".*" && !field.index) {
                return Option<bool>(400, "Field `" + field.name + "` cannot be marked as non-indexable.");
            }

            if(!field.index && field.facet) {
                return Option<bool>(400, "Field `" + field.name + "` cannot be a facet since "
                                                                  "it's marked as non-indexable.");
            }

            if(!field.is_sort_field() && field.sort) {
                return Option<bool>(400, "Field `" + field.name + "` cannot be a sortabale field.");
            }
        }

        if(!default_sorting_field.empty() && !found_default_sorting_field) {
            return Option<bool>(400, "Default sorting field is defined as `" + default_sorting_field +
                                            "` but is not found in the schema.");
        }

        // check for duplicate field names in schema
        for(auto& fname_fields: unique_fields) {
            if(fname_fields.second.size() > 1) {
                // if there are more than 1 field with the same field name, then
                // a) only 1 field can be of static type
                // b) only 1 field can be of dynamic type
                size_t num_static = 0;
                size_t num_dynamic = 0;

                for(const field* f: fname_fields.second) {
                    if(f->name == ".*" || f->is_dynamic()) {
                        num_dynamic++;
                    } else {
                        num_static++;
                    }
                }

                if(num_static != 0 && num_static > 1) {
                    return Option<bool>(400, "There are duplicate field names in the schema.");
                }

                if(num_dynamic != 0 && num_dynamic > 1) {
                    return Option<bool>(400, "There are duplicate field names in the schema.");
                }
            }
        }

        return Option<bool>(true);
    }

    static Option<bool> json_fields_to_fields(nlohmann::json& fields_json,
                                              std::string& fallback_field_type,
                                              std::vector<field>& fields) {

        size_t num_auto_detect_fields = 0;

        for(nlohmann::json & field_json: fields_json) {
            if(field_json["name"] == "id") {
                // No field should exist with the name "id" as it is reserved for internal use
                // We cannot throw an error here anymore since that will break backward compatibility!
                LOG(WARNING) << "Collection schema cannot contain a field with name `id`. Ignoring field.";
                continue;
            }

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

            if(field_json.count(fields::index) != 0 && !field_json.at(fields::index).is_boolean()) {
                return Option<bool>(400, std::string("The `index` property of the field `") +
                                         field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
            }

            if(field_json.count(fields::sort) != 0 && !field_json.at(fields::sort).is_boolean()) {
                return Option<bool>(400, std::string("The `sort` property of the field `") +
                                         field_json[fields::name].get<std::string>() + std::string("` should be a boolean."));
            }

            if(field_json.count(fields::locale) != 0){
                if(!field_json.at(fields::locale).is_string()) {
                    return Option<bool>(400, std::string("The `locale` property of the field `") +
                                             field_json[fields::name].get<std::string>() + std::string("` should be a string."));
                }

                if(!field_json[fields::locale].get<std::string>().empty() &&
                    field_json[fields::locale].get<std::string>().size() != 2) {
                    return Option<bool>(400, std::string("The `locale` value of the field `") +
                                             field_json[fields::name].get<std::string>() + std::string("` is not valid."));
                }
            }

            if(field_json["name"] == ".*") {
                if(field_json.count(fields::facet) == 0) {
                    field_json[fields::facet] = false;
                }

                if(field_json.count(fields::optional) == 0) {
                    field_json[fields::optional] = true;
                }

                if(field_json.count(fields::index) == 0) {
                    field_json[fields::index] = true;
                }

                if(field_json.count(fields::locale) == 0) {
                    field_json[fields::locale] = "";
                }

                if(field_json.count(fields::sort) == 0) {
                    field_json[fields::sort] = false;
                }

                if(field_json[fields::optional] == false) {
                    return Option<bool>(400, "Field `.*` must be an optional field.");
                }

                if(field_json[fields::facet] == true) {
                    return Option<bool>(400, "Field `.*` cannot be a facet field.");
                }

                if(field_json[fields::index] == false) {
                    return Option<bool>(400, "Field `.*` must be an index field.");
                }

                field fallback_field(field_json["name"], field_json["type"], field_json["facet"],
                                     field_json["optional"], field_json[fields::index], field_json[fields::locale],
                                     field_json[fields::sort]);

                if(fallback_field.has_valid_type()) {
                    fallback_field_type = fallback_field.type;
                    num_auto_detect_fields++;
                } else {
                    return Option<bool>(400, "The `type` of field `*` is invalid.");
                }

                fields.emplace_back(fallback_field);
                continue;
            }

            if(field_json.count(fields::facet) == 0) {
                field_json[fields::facet] = false;
            }

            if(field_json.count(fields::index) == 0) {
                field_json[fields::index] = true;
            }

            if(field_json.count(fields::locale) == 0) {
                field_json[fields::locale] = "";
            }

            if(field_json.count(fields::sort) == 0) {
                if(field_json["type"] == field_types::INT32 || field_json["type"] == field_types::INT64 ||
                   field_json["type"] == field_types::FLOAT || field_json["type"] == field_types::BOOL ||
                   field_json["type"] == field_types::GEOPOINT || field_json["type"] == field_types::GEOPOINT_ARRAY) {
                    field_json[fields::sort] = true;
                } else {
                    field_json[fields::sort] = false;
                }
            }

            if(field_json.count(fields::optional) == 0) {
                // dynamic fields are always optional
                bool is_dynamic = field::is_dynamic(field_json[fields::name], field_json[fields::type]);
                field_json[fields::optional] = is_dynamic;
            }

            fields.emplace_back(
                field(field_json[fields::name], field_json[fields::type], field_json[fields::facet],
                      field_json[fields::optional], field_json[fields::index], field_json[fields::locale],
                      field_json[fields::sort])
            );
        }

        if(num_auto_detect_fields > 1) {
            return Option<bool>(400,"There can be only one field named `.*`.");
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

    static Option<bool> parse_geopoint_filter_value(std::string& raw_value,
                                                    const std::string& format_err_msg,
                                                    std::string& processed_filter_val,
                                                    NUM_COMPARATOR& num_comparator);

    static Option<bool> parse_filter_query(const std::string& simple_filter_query,
                                           const std::unordered_map<std::string, field>& search_schema,
                                           const Store* store,
                                           const std::string& doc_id_prefix,
                                           std::vector<filter>& filters);
};

namespace sort_field_const {
    static const std::string name = "name";
    static const std::string order = "order";
    static const std::string asc = "ASC";
    static const std::string desc = "DESC";

    static const std::string text_match = "_text_match";
    static const std::string seq_id = "_seq_id";

    static const std::string exclude_radius = "exclude_radius";
    static const std::string precision = "precision";
}

struct sort_by {
    std::string name;
    std::string order;

    // geo related fields
    int64_t geopoint;
    uint32_t exclude_radius;
    uint32_t geo_precision;

    sort_by(const std::string & name, const std::string & order):
        name(name), order(order), geopoint(0), exclude_radius(0), geo_precision(0) {

    }

    sort_by(const std::string &name, const std::string &order, int64_t geopoint,
            uint32_t exclude_radius, uint32_t geo_precision) :
            name(name), order(order), geopoint(geopoint), exclude_radius(exclude_radius), geo_precision(geo_precision) {

    }

    sort_by& operator=(const sort_by& other) {
        name = other.name;
        order = other.order;
        geopoint = other.geopoint;
        exclude_radius = other.exclude_radius;
        geo_precision = other.geo_precision;
        return *this;
    }
};

class GeoPoint {
    constexpr static const double EARTH_RADIUS = 3958.75;
    constexpr static const double METER_CONVERT = 1609.00;
    constexpr static const uint64_t MASK_H32_BITS = 0xffffffffUL;
public:
    static uint64_t pack_lat_lng(double lat, double lng) {
        // https://stackoverflow.com/a/1220393/131050
        const int32_t ilat = lat * 1000000;
        const int32_t ilng = lng * 1000000;
        // during int32_t -> uint64_t, higher order bits will be 1, so we have to mask that
        const uint64_t lat_lng = (uint64_t(ilat) << 32) | (uint64_t)(ilng & MASK_H32_BITS);
        return lat_lng;
    }

    static void unpack_lat_lng(uint64_t packed_lat_lng, S2LatLng& latlng) {
        const double lat = double(int32_t((packed_lat_lng >> 32) & MASK_H32_BITS)) / 1000000;
        const double lng = double(int32_t(packed_lat_lng & MASK_H32_BITS)) / 1000000;
        latlng = S2LatLng::FromDegrees(lat, lng);
    }

    // distance in meters
    static int64_t distance(const S2LatLng& a, const S2LatLng& b) {
        double rdist = a.GetDistance(b).radians();
        double dist = EARTH_RADIUS * rdist;
        return dist * METER_CONVERT;
    }
};

struct facet_count_t {
    uint32_t count = 0;
    // used to fetch the actual document and value for representation
    uint32_t doc_id = 0;
    uint32_t array_pos = 0;
};

struct facet_stats_t {
    double fvmin = std::numeric_limits<double>::max(),
            fvmax = -std::numeric_limits<double>::min(),
            fvcount = 0,
            fvsum = 0;
};

struct facet {
    const std::string field_name;
    spp::sparse_hash_map<uint64_t, facet_count_t> result_map;

    // used for facet value query
    spp::sparse_hash_map<uint64_t, std::vector<std::string>> hash_tokens;

    // used for faceting grouped results
    spp::sparse_hash_map<uint64_t, spp::sparse_hash_set<uint64_t>> hash_groups;

    facet_stats_t stats;

    explicit facet(const std::string& field_name): field_name(field_name) {

    }
};

struct facet_info_t {
    // facet hash => resolved tokens
    std::unordered_map<uint64_t, std::vector<std::string>> hashes;
    bool use_facet_query = false;
    bool should_compute_stats = false;
    field facet_field{"", "", false};
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

    facet_hash_values_t() {
        length = 0;
        hashes = nullptr;
    }

    facet_hash_values_t(facet_hash_values_t&& hash_values) noexcept {
        length = hash_values.length;
        hashes = hash_values.hashes;

        hash_values.length = 0;
        hash_values.hashes = nullptr;
    }

    facet_hash_values_t& operator=(facet_hash_values_t&& other) noexcept {
        if (this != &other) {
            delete[] hashes;

            hashes = other.hashes;
            length = other.length;

            other.hashes = nullptr;
            other.length = 0;
        }

        return *this;
    }

    ~facet_hash_values_t() {
        delete [] hashes;
        hashes = nullptr;
    }

    uint64_t size() const {
        return length;
    }

    uint64_t back() const {
        return hashes[length - 1];
    }
};