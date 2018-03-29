#include <gtest/gtest.h>
#include "string_utils.h"

TEST(StringUtilsTest, ShouldNormalizeString) {
    std::string alphanum = "Aa12Zz";
    StringUtils::normalize(alphanum);
    ASSERT_STREQ("aa12zz", alphanum.c_str());

    std::string alphanum_space = "Aa12Zz 12A";
    StringUtils::normalize(alphanum_space);
    ASSERT_STREQ("aa12zz12a", alphanum_space.c_str());

    std::string alphanum_specialchars = "Aa12Zz@W-_?,.R";
    StringUtils::normalize(alphanum_specialchars);
    ASSERT_STREQ("aa12zzwr", alphanum_specialchars.c_str());

    // retain non-ascii unicode characters and should also lower case them
    std::string alphanum_unicodechars = "abcÅà123";
    StringUtils::normalize(alphanum_unicodechars);
    ASSERT_STREQ("abcåà123", alphanum_unicodechars.c_str());
}