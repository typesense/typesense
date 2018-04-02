#include <gtest/gtest.h>
#include "string_utils.h"

TEST(StringUtilsTest, ShouldNormalizeString) {
    StringUtils string_utils;

    std::string alphanum = "Aa12Zz";
    string_utils.unicode_normalize(alphanum);
    ASSERT_STREQ("aa12zz", alphanum.c_str());

    std::string alphanum_space = "Aa12Zz 12A";
    string_utils.unicode_normalize(alphanum_space);
    ASSERT_STREQ("aa12zz12a", alphanum_space.c_str());

    std::string alphanum_specialchars = "Aa12Zz@W-_?,.R";
    string_utils.unicode_normalize(alphanum_specialchars);
    ASSERT_STREQ("aa12zzwr", alphanum_specialchars.c_str());

    // transliterate unicode characters and should also lower case them
    std::string alphanum_unicodechars = "abcÅà123";
    string_utils.unicode_normalize(alphanum_unicodechars);
    ASSERT_STREQ("abcaa123", alphanum_unicodechars.c_str());

    std::string tamil_unicodechars = "தமிழ் நாடு";
    string_utils.unicode_normalize(tamil_unicodechars);
    ASSERT_STREQ("தமிழ்நாடு", tamil_unicodechars.c_str());
}