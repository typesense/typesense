#include "string_utils.h"

void StringUtils::unicode_normalize(std::string& str) const {
    // remove special chars within ASCII range
    str.erase(std::remove_if(str.begin(), str.end(), [](char c) {
        return !std::isalnum(c) && (int)(c) >= 0;
    }), str.end());

    icu::UnicodeString u_str = icu::UnicodeString::fromUTF8(str);
    str.clear();
    u_str.toLower().toUTF8String(str);
}