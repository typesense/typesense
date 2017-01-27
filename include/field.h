#pragma once

#include <string>

namespace field_types {
    static const std::string STRING = "STRING";
    static const std::string INT32 = "INT32";
    static const std::string INT64 = "INT64";
    static const std::string STRING_ARR = "STRING_ARR";
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