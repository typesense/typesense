#include <string>

enum field_type {
    INT32,
    STRING
};

struct field {
    std::string name;
    field_type type;

    field(std::string name, field_type type): name(name), type(type) {

    }
};