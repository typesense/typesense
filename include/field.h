#pragma once

#include <string>
#include <s2/s2latlng.h>
#include "art.h"
#include "option.h"
#include "string_utils.h"
#include "logger.h"
#include <sparsepp.h>
#include <tsl/htrie_map.h>
#include "json.hpp"

namespace field_types {
    // first field value indexed will determine the type
    static const std::string AUTO = "auto";
    static const std::string OBJECT = "object";
    static const std::string OBJECT_ARRAY = "object[]";

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
    static const std::string infix = "infix";
    static const std::string locale = "locale";
    static const std::string nested = "nested";
    static const std::string nested_array = "nested_array";
    static const std::string num_dim = "num_dim";
    static const std::string vec_dist = "vec_dist";
}

enum vector_distance_type_t {
    ip,
    cosine
};

struct field {
    std::string name;
    std::string type;
    bool facet;
    bool optional;
    bool index;
    std::string locale;
    bool sort;
    bool infix;

    bool nested;        // field inside an object

    // field inside an array of objects that is forced to be an array
    // integer to handle tri-state: true (1), false (0), not known yet (2)
    // third state is used to diff between array of object and array within object during write
    int nested_array;

    size_t num_dim;
    vector_distance_type_t vec_dist;

    static constexpr int VAL_UNKNOWN = 2;

    field() {}

    field(const std::string &name, const std::string &type, const bool facet, const bool optional = false,
          bool index = true, std::string locale = "", int sort = -1, int infix = -1, bool nested = false,
          int nested_array = 0, size_t num_dim = 0, vector_distance_type_t vec_dist = cosine) :
            name(name), type(type), facet(facet), optional(optional), index(index), locale(locale),
            nested(nested), nested_array(nested_array), num_dim(num_dim), vec_dist(vec_dist) {

        set_computed_defaults(sort, infix);
    }

    void set_computed_defaults(int sort, int infix) {
        if(sort != -1) {
            this->sort = bool(sort);
        } else {
            this->sort = is_num_sort_field();
        }

        this->infix = (infix != -1) ? bool(infix) : false;
    }

    bool operator<(const field& f) const {
        return name < f.name;
    }

    bool operator==(const field& f) const {
        return name == f.name;
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

    bool is_object() const {
        return (type == field_types::OBJECT || type == field_types::OBJECT_ARRAY);
    }

    bool is_string() const {
        return (type == field_types::STRING || type == field_types::STRING_ARRAY);
    }

    bool is_string_star() const {
        return field_types::is_string_or_array(type);
    }

    bool is_facet() const {
        return facet;
    }

    bool is_array() const {
        return (type == field_types::STRING_ARRAY || type == field_types::INT32_ARRAY ||
                type == field_types::FLOAT_ARRAY ||
                type == field_types::INT64_ARRAY || type == field_types::BOOL_ARRAY ||
                type == field_types::GEOPOINT_ARRAY || type == field_types::OBJECT_ARRAY);
    }

    bool is_singular() const {
        return !is_array();
    }

    static bool is_dynamic(const std::string& name, const std::string& type) {
        return type == "string*" || (name != ".*" && type == field_types::AUTO) ||
               (name != ".*" && name.find(".*") != std::string::npos);
    }

    bool is_dynamic() const {
        return is_dynamic(name, type);
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
        bool is_basic_type = is_string() || is_integer() || is_float() || is_bool() || is_geopoint() ||
                             is_object() || is_auto();
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

        if(obj.is_object()) {
            field_type = field_types::OBJECT;
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
            field_val[fields::index] = field.index;
            field_val[fields::sort] = field.sort;
            field_val[fields::infix] = field.infix;

            field_val[fields::locale] = field.locale;

            field_val[fields::nested] = field.nested;
            if(field.nested) {
                field_val[fields::nested_array] = field.nested_array;
            }

            if(field.num_dim > 0) {
                field_val[fields::num_dim] = field.num_dim;
                field_val[fields::vec_dist] = field.vec_dist == ip ? "ip" : "cosine";
            }

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

                if(field.is_geopoint()) {
                    return Option<bool>(400, "Default sorting field cannot be of type geopoint.");
                }

                found_default_sorting_field = true;
            }

            if(field.is_dynamic() && !field.nested && !field.optional) {
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

        if(!default_sorting_field.empty() && !found_default_sorting_field && !fields.empty()) {
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

    static Option<bool> json_field_to_field(bool enable_nested_fields, nlohmann::json& field_json,
                                            std::vector<field>& the_fields,
                                            string& fallback_field_type, size_t& num_auto_detect_fields);

    static Option<bool> json_fields_to_fields(bool enable_nested_fields,
                                              nlohmann::json& fields_json,
                                              std::string& fallback_field_type,
                                              std::vector<field>& the_fields) {

        size_t num_auto_detect_fields = 0;

        for(nlohmann::json & field_json: fields_json) {
            auto op = json_field_to_field(enable_nested_fields,
                                          field_json, the_fields, fallback_field_type, num_auto_detect_fields);
            if(!op.ok()) {
                return op;
            }
        }

        if(num_auto_detect_fields > 1) {
            return Option<bool>(400,"There can be only one field named `.*`.");
        }

        return Option<bool>(true);
    }

    static bool flatten_obj(nlohmann::json& doc, nlohmann::json& value, bool has_array, bool has_obj_array,
                            const field& the_field, const std::string& flat_name,
                            std::unordered_map<std::string, field>& flattened_fields);

    static Option<bool> flatten_field(nlohmann::json& doc, nlohmann::json& obj, const field& the_field,
                                      std::vector<std::string>& path_parts, size_t path_index, bool has_array,
                                      bool has_obj_array, std::unordered_map<std::string, field>& flattened_fields);

    static Option<bool> flatten_doc(nlohmann::json& document, const tsl::htrie_map<char, field>& nested_fields,
                                    bool missing_is_ok, std::vector<field>& flattened_fields);

    static void compact_nested_fields(tsl::htrie_map<char, field>& nested_fields);
};

struct filter_node_t;

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
                                           const tsl::htrie_map<char, field>& search_schema,
                                           const Store* store,
                                           const std::string& doc_id_prefix,
                                           filter_node_t*& root);
};

struct filter_node_t {
    filter filter_exp;
    FILTER_OPERATOR filter_operator;
    bool isOperator;
    filter_node_t* left;
    filter_node_t* right;

    filter_node_t(filter filter_exp)
            : filter_exp(std::move(filter_exp)),
              isOperator(false),
              left(nullptr),
              right(nullptr) {}

    filter_node_t(FILTER_OPERATOR filter_operator,
                  filter_node_t* left,
                  filter_node_t* right)
            : filter_operator(filter_operator),
              isOperator(true),
              left(left),
              right(right) {}

    ~filter_node_t() {
        delete left;
        delete right;
    }
};

namespace sort_field_const {
    static const std::string name = "name";
    static const std::string order = "order";
    static const std::string asc = "ASC";
    static const std::string desc = "DESC";

    static const std::string text_match = "_text_match";
    static const std::string eval = "_eval";
    static const std::string seq_id = "_seq_id";

    static const std::string exclude_radius = "exclude_radius";
    static const std::string precision = "precision";

    static const std::string missing_values = "missing_values";
}

struct sort_by {
    enum missing_values_t {
        first,
        last,
        normal,
    };

    struct eval_t {
        filter_node_t* filter_tree_root;
        uint32_t* ids = nullptr;
        uint32_t  size = 0;
    };

    std::string name;
    std::string order;

    // for text_match score bucketing
    uint32_t text_match_buckets;

    // geo related fields
    int64_t geopoint;
    uint32_t exclude_radius;
    uint32_t geo_precision;

    missing_values_t missing_values;
    eval_t eval;

    sort_by(const std::string & name, const std::string & order):
            name(name), order(order), text_match_buckets(0), geopoint(0), exclude_radius(0), geo_precision(0),
            missing_values(normal) {

    }

    sort_by(const std::string &name, const std::string &order, uint32_t text_match_buckets, int64_t geopoint,
            uint32_t exclude_radius, uint32_t geo_precision) :
            name(name), order(order), text_match_buckets(text_match_buckets),
            geopoint(geopoint), exclude_radius(exclude_radius), geo_precision(geo_precision),
            missing_values(normal) {

    }

    sort_by& operator=(const sort_by& other) {
        name = other.name;
        order = other.order;
        text_match_buckets = other.text_match_buckets;
        geopoint = other.geopoint;
        exclude_radius = other.exclude_radius;
        geo_precision = other.geo_precision;
        missing_values = other.missing_values;
        eval = other.eval;
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