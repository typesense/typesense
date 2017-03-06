#pragma once
#include <stdint.h>

template <typename T=uint32_t>
class Option {
private:

    T value;
    bool is_ok;

    std::string error_msg;
    uint32_t code;

public:

    Option(const T & value): value(value), is_ok(true) {

    }

    Option(uint32_t code, const std::string & error_msg): code(code), error_msg(error_msg), is_ok(false) {

    }

    bool ok() {
        return is_ok;
    }

    T get() {
        return value;
    }

    std::string error() {
        return error_msg;
    }
};