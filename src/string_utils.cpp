#include "string_utils.h"
#include <iostream>

std::string lower_and_no_special_chars(const std::string & str) {
    std::stringstream ss;

    for(const auto c: str) {
        bool should_remove = (!std::isalnum(c) && (int)(c) >= 0);
        if(!should_remove) {
            ss << (char) std::tolower(c);
        }
    }

    return ss.str();
}

void StringUtils::unicode_normalize(std::string& str) const {
    size_t outbuflen = str.length() * 2;
    char output[outbuflen];
    char *outptr = output;

    char *input = (char *) str.c_str();
    size_t insize = str.length();
    size_t outsize = outbuflen;

    iconv(cd, &input, &insize, &outptr, &outsize);
    size_t actual_size = outbuflen - outsize;

    if(actual_size == 0) {
        str.assign(lower_and_no_special_chars(str));
        return ;
    }

    std::string nstr = std::string(output, actual_size);
    str.assign(lower_and_no_special_chars(nstr));
}