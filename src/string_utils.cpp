#include "string_utils.h"
#include <iostream>

std::string lower_and_no_special_chars(const std::string & str) {
    std::stringstream ss;

    for(const auto c: str) {
        bool should_remove = ( (int)(c) >= 0 &&  // check for ASCII range
                                !std::isalnum(c) );
        if(!should_remove) {
            ss << (char) std::tolower(c);
        }
    }

    return ss.str();
}

void StringUtils::unicode_normalize(std::string & str) const {
    std::stringstream out;

    for (char *s = &str[0]; *s;) {
        char inbuf[5];
        char *p = inbuf;
        *p++ = *s++;
        if ((*s & 0xC0) == 0x80) *p++ = *s++;
        if ((*s & 0xC0) == 0x80) *p++ = *s++;
        if ((*s & 0xC0) == 0x80) *p++ = *s++;
        *p = 0;
        size_t insize = (p - &inbuf[0]);

        char outbuf[5] = {};
        size_t outsize = sizeof(outbuf);
        char *outptr = outbuf;
        char *inptr = inbuf;

        //printf("[%s]\n", inbuf);

        errno = 0;
        iconv(cd, &inptr, &insize, &outptr, &outsize);

        if(errno == EILSEQ) {
            // symbol cannot be represented as ASCII, so write the original symbol
            out << inbuf;
        } else {
            out << outbuf;
        }
    }

    str.assign(lower_and_no_special_chars(out.str()));
}

std::string StringUtils::randstring(size_t length, uint64_t seed) {
    static auto& chrs = "0123456789"
                        "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    thread_local static std::mt19937 rg(seed);
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

    std::string s;
    s.reserve(length);

    while(length--) {
        s += chrs[pick(rg)];
    }

    return s;
}
