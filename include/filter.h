#pragma once

#include <string>
#include <map>
#include <tsl/htrie_map.h>
#include <art.h>
#include <json.hpp>
#include "store.h"

enum FILTER_OPERATOR {
    AND,
    OR
};

struct filter_node_t;
struct field;

struct filter {
    std::string field_name;
    std::vector<std::string> values;
    std::vector<NUM_COMPARATOR> comparators;
    // Would be set when `field: != ...` is encountered with id/string field or `field: != [ ... ]` is encountered in the
    // case of int and float fields. During filtering, all the results of matching the field against the values are
    // aggregated and then this flag is checked if negation on the aggregated result is required.
    bool apply_not_equals = false;

    // Would store `Foo` in case of a filter expression like `$Foo(bar := baz)`
    std::string referenced_collection_name;

    std::vector<nlohmann::json> params;

    /// For searching places within a given radius of a given latlong (mi for miles and km for kilometers)
    static constexpr const char* GEO_FILTER_RADIUS_KEY = "radius";

    /// Radius threshold beyond which exact filtering on geo_result_ids will not be done.
    static constexpr const char* EXACT_GEO_FILTER_RADIUS_KEY = "exact_filter_radius";
    static constexpr double DEFAULT_EXACT_GEO_FILTER_RADIUS_VALUE = 10000; // meters

    static const std::string RANGE_OPERATOR() {
        return "..";
    }

    static Option<bool> validate_numerical_filter_value(field _field, const std::string& raw_value);

    static Option<NUM_COMPARATOR> extract_num_comparator(std::string & comp_and_value);

    static Option<bool> parse_geopoint_filter_value(std::string& raw_value,
                                                    const std::string& format_err_msg,
                                                    std::string& processed_filter_val,
                                                    NUM_COMPARATOR& num_comparator);

    static Option<bool> parse_geopoint_filter_value(std::string& raw_value,
                                                    const std::string& format_err_msg,
                                                    filter& filter_exp);

    static Option<bool> parse_filter_query(const std::string& filter_query,
                                           const tsl::htrie_map<char, field>& search_schema,
                                           const Store* store,
                                           const std::string& doc_id_prefix,
                                           filter_node_t*& root);
};

struct filter_node_t {
    filter filter_exp;
    FILTER_OPERATOR filter_operator;
    bool isOperator;
    filter_node_t* left = nullptr;
    filter_node_t* right = nullptr;

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
