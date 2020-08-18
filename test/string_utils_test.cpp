#include <gtest/gtest.h>
#include "string_utils.h"
#include <iconv.h>
#include <unicode/translit.h>
#include <json.hpp>

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

    std::string alphanum_unicodechars = "abcÅà123ß12";
    string_utils.unicode_normalize(alphanum_unicodechars);
    ASSERT_STREQ("abcaa123ss12", alphanum_unicodechars.c_str());

    std::string tamil_unicodechars = "தமிழ் நாடு";
    string_utils.unicode_normalize(tamil_unicodechars);
    ASSERT_STREQ("தமிழ்நாடு", tamil_unicodechars.c_str());

    std::string chinese_unicodechars = "你好吗";
    string_utils.unicode_normalize(chinese_unicodechars);
    ASSERT_STREQ("你好吗", chinese_unicodechars.c_str());

    std::string mixed_unicodechars = "çн தமிழ் நாடு so...";
    string_utils.unicode_normalize(mixed_unicodechars);
    ASSERT_STREQ("cнதமிழ்நாடுso", mixed_unicodechars.c_str());

    // Any-Latin; Latin-ASCII; Lower()
}

TEST(StringUtilsTest, ShouldJoinString) {
    std::vector<std::string> parts = {"foo", "bar", "baz", "bazinga"};

    const std::string & joined_str1 = StringUtils::join(parts, "/");
    ASSERT_STREQ("foo/bar/baz/bazinga", joined_str1.c_str());

    const std::string & joined_str2 = StringUtils::join(parts, "/", 2);
    ASSERT_STREQ("baz/bazinga", joined_str2.c_str());

    const std::string & joined_str3 = StringUtils::join({}, "/");
    ASSERT_STREQ("", joined_str3.c_str());
}

TEST(StringUtilsTest, HMAC) {
    std::string digest1 = StringUtils::hmac("KeyVal", "{\"filter_by\": \"user_id:1080\"}");
    ASSERT_STREQ("IvjqWNZ5M5ElcvbMoXj45BxkQrZG4ZKEaNQoRioCx2s=", digest1.c_str());
}

TEST(StringUtilsTest, UInt32Validation) {
    std::string big_num = "99999999999999999999999999999999";
    ASSERT_FALSE(StringUtils::is_uint32_t(big_num));
}

TEST(StringUtilsTest, ShouldSplitString) {
    nlohmann::json obj1;
    obj1["s"] = "Line one.\nLine two.\n";

    nlohmann::json obj2;
    obj2["s"] = "Line 1.\nLine 2.\n";

    std::string text;
    text = obj1.dump();
    text += "\n" + obj2.dump();

    std::vector<std::string> lines;
    StringUtils::split(text, lines, "\n");

    ASSERT_STREQ("{\"s\":\"Line one.\\nLine two.\\n\"}", lines[0].c_str());
    ASSERT_STREQ("{\"s\":\"Line 1.\\nLine 2.\\n\"}", lines[1].c_str());

    // empty string should produce empty list
    std::vector<std::string> lines_empty;
    StringUtils::split("", lines_empty, "\n");
    ASSERT_TRUE(lines_empty.empty());

    // restrict list of max_values
    std::vector<std::string> lines_limited;
    size_t end_index = StringUtils::split("a b c d e f", lines_limited, " ", false, 0, 3);
    ASSERT_EQ(3, lines_limited.size());
    ASSERT_EQ(6, end_index);

    // start from an arbitrary position in string
    std::vector<std::string> lines_custom_start;
    end_index = StringUtils::split("a b c d e f", lines_custom_start, " ", false, 2, 100);
    ASSERT_EQ(5, lines_custom_start.size());
    ASSERT_EQ(11, end_index);
}
