#pragma once

#include <string>
#include "art.h"

namespace field_types {
    static const std::string STRING = "STRING";
    static const std::string INT32 = "INT32";
    static const std::string INT64 = "INT64";
    static const std::string STRING_ARRAY = "STRING_ARRAY";
    static const std::string INT32_ARRAY = "INT32_ARRAY";
    static const std::string INT64_ARRAY = "INT64_ARRAY";
}

namespace fields {
    static const std::string name = "name";
    static const std::string type = "type";
}

struct field {
    std::string name;
    std::string type;

    field(std::string name, std::string type): name(name), type(type) {

    }
};

struct filter {
    std::string field_name;
    std::string value_json;
    std::string compare_operator;

    NUM_COMPARATOR get_comparator() const {
        if(compare_operator == "LESS_THAN") {
            return LESS_THAN;
        } else if(compare_operator == "LESS_THAN_EQUALS") {
            return LESS_THAN_EQUALS;
        } else if(compare_operator == "EQUALS") {
            return EQUALS;
        } else if(compare_operator == "GREATER_THAN") {
            return GREATER_THAN;
        } else {
            return GREATER_THAN_EQUALS;
        }
    }
};