#pragma once

#include <string>
#include "art.h"
#include "option.h"
#include "string_utils.h"

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

    bool integer() {
        return type == field_types::INT32 || type == field_types::INT32_ARRAY ||
               type == field_types::INT64 || type == field_types::INT64_ARRAY;
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