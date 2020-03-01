#pragma once

#include <string>
#include "art.h"
#include "option.h"
#include "string_utils.h"

namespace field_types {
    static const std::string STRING = "string";
    static const std::string INT32 = "int32";
    static const std::string INT64 = "int64";
    static const std::string FLOAT = "float";
    static const std::string BOOL = "bool";
    static const std::string STRING_ARRAY = "string[]";
    static const std::string INT32_ARRAY = "int32[]";
    static const std::string INT64_ARRAY = "int64[]";
    static const std::string FLOAT_ARRAY = "float[]";
    static const std::string BOOL_ARRAY = "bool[]";
}

namespace fields {
    static const std::string name = "name";
    static const std::string type = "type";
    static const std::string facet = "facet";
}

struct field {
    std::string name;
    std::string type;
    bool facet;

    field(const std::string & name, const std::string & type, const bool & facet): name(name), type(type), facet(facet) {

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

    bool is_float() const {
        return (type == field_types::FLOAT || type == field_types::FLOAT_ARRAY);
    }

    bool is_bool() const {
        return (type == field_types::BOOL || type == field_types::BOOL_ARRAY);
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

    std::string faceted_name() const {
        return (facet && !is_string()) ? "_fstr_" + name : name;
    }
};

struct filter {
    std::string field_name;
    std::vector<std::string> values;
    NUM_COMPARATOR compare_operator;

    static Option<NUM_COMPARATOR> extract_num_comparator(const std::string & comp_and_value) {
        if(StringUtils::is_integer(comp_and_value)) {
            return Option<NUM_COMPARATOR>(EQUALS);
        }

        // the ordering is important - we have to compare 2-letter operators first
        if(comp_and_value.compare(0, 2, "<=") == 0) {
            return Option<NUM_COMPARATOR>(LESS_THAN_EQUALS);
        }

        if(comp_and_value.compare(0, 2, ">=") == 0) {
            return Option<NUM_COMPARATOR>(GREATER_THAN_EQUALS);
        }

        if(comp_and_value.compare(0, 1, "<") == 0) {
            return Option<NUM_COMPARATOR>(LESS_THAN);
        }

        if(comp_and_value.compare(0, 1, ">") == 0) {
            return Option<NUM_COMPARATOR>(GREATER_THAN);
        }

        return Option<NUM_COMPARATOR>(400, "Numerical field has an invalid comparator.");
    }
};

namespace sort_field_const {
    static const std::string name = "name";
    static const std::string order = "order";
    static const std::string asc = "ASC";
    static const std::string desc = "DESC";
}

struct sort_by {
    std::string name;
    std::string order;

    sort_by(const std::string & name, const std::string & order): name(name), order(order) {

    }

    sort_by& operator=(sort_by other) {
        name = other.name;
        order = other.order;
        return *this;
    }
};

struct token_pos_cost_t {
    size_t pos;
    uint32_t cost;
};

struct facet_count_t {
    uint32_t count;

    // used to fetch the actual document and value for representation
    uint32_t doc_id;
    uint32_t array_pos;

    spp::sparse_hash_map<uint32_t, token_pos_cost_t> query_token_pos;
};

struct facet_stats_t {
    double fvmin = std::numeric_limits<double>::max(),
            fvmax = -std::numeric_limits<double>::min(),
            fvcount = 0,
            fvsum = 0;
};

struct facet {
    const std::string field_name;
    std::map<uint64_t, facet_count_t> result_map;
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