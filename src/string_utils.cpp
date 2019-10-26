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

void StringUtils::unicode_normalize2(std::string& str) const {
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