#pragma once

#include <string>

namespace field_types {
    static const std::string STRING = "STRING";
    static const std::string INT32 = "INT32";
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