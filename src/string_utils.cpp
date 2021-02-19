#include "string_utils.h"
#include <iostream>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <random>
#include <openssl/sha.h>

std::string lower_and_no_special_chars(const std::string & str) {
    std::stringstream ss;

    for(char c : str) {
        bool is_ascii = ((int)(c) >= 0);
        bool keep_char = !is_ascii || std::isalnum(c);

        if(keep_char) {
            ss << (char) std::tolower(c);
        }
    }

    return ss.str();
}

void StringUtils::unicode_normalize(std::string & str) const {
    if(str.empty()) {
        return ;
    }

    std::stringstream out;

    for (char *s = &str[0]; *s;) {
        char inbuf[5];
        char *p = inbuf;

        if((*s & ~0x7f) == 0 ) {
            // ascii character
            out << *s++;
            continue;
        }

        // group bytes to form a unicode representation
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

    str = lower_and_no_special_chars(out.str());
}

std::string StringUtils::randstring(size_t length) {
    static auto& chrs = "0123456789"
                        "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    thread_local std::mt19937 rg(std::random_device{}());
    thread_local std::uniform_int_distribution<uint32_t> pick(0, sizeof(chrs) - 2);

    std::string s;
    s.reserve(length);

    while(length--) {
        s += chrs[pick(rg)];
    }

    return s;
}

std::string StringUtils::hmac(const std::string& key, const std::string& msg) {
    unsigned int hmac_len;
    unsigned char hmac[EVP_MAX_MD_SIZE];
    HMAC(EVP_sha256(), key.c_str(), key.size(),
         reinterpret_cast<const unsigned char *>(msg.c_str()), msg.size(),
         hmac, &hmac_len);

    std::string digest_raw(reinterpret_cast<char*>(&hmac), hmac_len);
    return StringUtils::base64_encode(digest_raw);
}

std::string StringUtils::str2hex(const std::string &str, bool capital) {
    std::string hexstr;
    hexstr.resize(str.size() * 2);
    const size_t a = capital ? 'A' - 1 : 'a' - 1;
    for (size_t i = 0, c = str[0] & 0xFF; i < hexstr.size(); c = str[i / 2] & 0xFF) {
        hexstr[i++] = c > 0x9F ? (c / 16 - 9) | a : c / 16 | '0';
        hexstr[i++] = (c & 0xF) > 9 ? (c % 16 - 9) | a : c % 16 | '0';
    }

    return hexstr;
}

std::string StringUtils::hash_sha256(const std::string& str) {
    const size_t SHA256_SIZE = 32;
    unsigned char hash_buf[SHA256_SIZE];
    SHA256(reinterpret_cast<const unsigned char *>(str.c_str()), str.size(), hash_buf);
    return StringUtils::str2hex(std::string(reinterpret_cast<char*>(hash_buf), SHA256_SIZE));
}

/*size_t StringUtils::unicode_length(const std::string& bytes) {
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> utf8conv;
    return utf8conv.from_bytes(bytes).size();
}*/
