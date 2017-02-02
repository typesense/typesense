#pragma once

#include <string>

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
};