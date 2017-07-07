#pragma once
#include <stdint.h>

template <typename T=uint32_t>
class Option {
private:

    T value;
    bool is_ok;

    std::string error_msg;
    uint32_t error_code;

public:

    Option(const T & value): value(value), is_ok(true) {

    }

    Option(const uint32_t code, const std::string & error_msg): error_code(code), error_msg(error_msg), is_ok(false) {

    }

    Option(const Option &obj) {
        value = obj.value;
        is_ok = obj.is_ok;
        error_msg = obj.error_msg;
        error_code = obj.error_code;
    }

    bool ok() const {
        return is_ok;
    }

    T get() {
        return value;
    }

    std::string error() const {
        return error_msg;
    }

    uint32_t code() const {
        return error_code;
    }
};