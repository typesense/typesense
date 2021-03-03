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

    std::string comma_and_space = "foo, bar";
    std::vector<std::string> comma_space_parts;
    StringUtils::split(comma_and_space, comma_space_parts, ",");
    ASSERT_STREQ("foo", comma_space_parts[0].c_str());
    ASSERT_STREQ("bar", comma_space_parts[1].c_str());
}

TEST(StringUtilsTest, ShouldTrimString) {
    std::string str = " a ";
    StringUtils::trim(str);
    ASSERT_STREQ("a", str.c_str());

    str = "abc";
    StringUtils::trim(str);
    ASSERT_STREQ("abc", str.c_str());

    str = " abc def";
    StringUtils::trim(str);
    ASSERT_STREQ("abc def", str.c_str());

    str = " abc def   ";
    StringUtils::trim(str);
    ASSERT_STREQ("abc def", str.c_str());

    str = "  ";
    StringUtils::trim(str);
    ASSERT_STREQ("", str.c_str());
}

TEST(StringUtilsTest, ShouldComputeSHA256) {
    ASSERT_STREQ("c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2",
                 StringUtils::hash_sha256("foobar").c_str());

    ASSERT_STREQ("d8705968091d40b60436675240712c584c187eef091514d4092483dc342ca3de",
                 StringUtils::hash_sha256("some random key").c_str());

    ASSERT_STREQ("6613f67d3d78d48e2678faf55c33fabc5895c538ce70ea10218ce9b7eccbf394",
                  StringUtils::hash_sha256("791a27668b3e01fc6ab3482b6e6a36255154df3ecd7dcec").c_str());
}

TEST(StringUtilsTest, ShouldParseQueryString) {
    std::string qs = "?q=bar&filter_by=points: >100 && points: <200";
    auto qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points: >100 && points: <200", qmap["filter_by"]);

    qs = "?q=bar&filter_by=points%3A%20%3E100%20%26%26%20points%3A%20%3C200";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points: >100 && points: <200", qmap["filter_by"]);

    qs = "?q=bar&filter_by=points%3A%20%3E100%20%26%26%20points%3A%20%3C200&";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points: >100 && points: <200", qmap["filter_by"]);

    qs = "q=bar&filter_by=baz&&";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("baz&", qmap["filter_by"]);

    qs = "q=bar&filter_by=";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("", qmap["filter_by"]);

    qs = "q=bar&filter_by=&";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("", qmap["filter_by"]);

    qs = "q=bar&filter_by=points :> 100&enable_typos";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(3, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points :> 100", qmap["filter_by"]);
    ASSERT_EQ("", qmap["enable_typos"]);

    qs = "foo=" + StringUtils::randstring(2000);
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(0, qmap.size());

    qs = "foo=bar&baz=&bazinga=true";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(3, qmap.size());
    ASSERT_EQ("bar", qmap["foo"]);
    ASSERT_EQ("", qmap["baz"]);
    ASSERT_EQ("true", qmap["bazinga"]);

    qs = "foo";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(1, qmap.size());
    ASSERT_EQ("", qmap["foo"]);

    qs = "?foo=";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(1, qmap.size());
    ASSERT_EQ("", qmap["foo"]);

    qs = "?foo";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(1, qmap.size());
    ASSERT_EQ("", qmap["foo"]);

    qs = "?";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(0, qmap.size());

    qs = "";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(0, qmap.size());

    qs = "&";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(0, qmap.size());

    qs = "&&";
    qmap = StringUtils::parse_query_string(qs);
    ASSERT_EQ(0, qmap.size());
}
