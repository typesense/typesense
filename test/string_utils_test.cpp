#include <gtest/gtest.h>
#include "string_utils.h"
#include <iconv.h>
#include <unicode/translit.h>
#include <json.hpp>

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
    size_t end_index = StringUtils::split("a b c d e f", lines_limited, " ", false, true, 0, 3);
    ASSERT_EQ(3, lines_limited.size());
    ASSERT_EQ(6, end_index);

    // start from an arbitrary position in string
    std::vector<std::string> lines_custom_start;
    end_index = StringUtils::split("a b c d e f", lines_custom_start, " ", false, true, 2, 100);
    ASSERT_EQ(5, lines_custom_start.size());
    ASSERT_EQ(11, end_index);

    std::string comma_and_space = "foo, bar";
    std::vector<std::string> comma_space_parts;
    StringUtils::split(comma_and_space, comma_space_parts, ",");
    ASSERT_STREQ("foo", comma_space_parts[0].c_str());
    ASSERT_STREQ("bar", comma_space_parts[1].c_str());

    // preserve trailing space
    std::string str_trailing_space = "foo\nbar ";
    std::vector<std::string> trailing_space_parts;
    StringUtils::split(str_trailing_space, trailing_space_parts, "\n", false, false);
    ASSERT_EQ(2, trailing_space_parts.size());
    ASSERT_EQ("foo", trailing_space_parts[0]);
    ASSERT_EQ("bar ", trailing_space_parts[1]);
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

TEST(StringUtilsTest, ShouldCheckFloat) {
    ASSERT_TRUE(StringUtils::is_float("0.23"));
    ASSERT_TRUE(StringUtils::is_float("9.872019290924072e-07"));

    ASSERT_FALSE(StringUtils::is_float("4.2f"));
    ASSERT_FALSE(StringUtils::is_float("-5.3f"));
    ASSERT_FALSE(StringUtils::is_float("+6.2f"));
    ASSERT_FALSE(StringUtils::is_float("0.x87"));
    ASSERT_FALSE(StringUtils::is_float("1.0.0"));
    ASSERT_FALSE(StringUtils::is_float("2f"));
    ASSERT_FALSE(StringUtils::is_float("2.0f1"));
}

TEST(StringUtilsTest, ShouldParseQueryString) {
    std::map<std::string, std::string> qmap;
    
    std::string qs = "?q=bar&filter_by=points: >100 && points: <200";

    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points: >100 && points: <200", qmap["filter_by"]);

    qs = "?q=bar&filter_by=points%3A%20%3E100%20%26%26%20points%3A%20%3C200";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points: >100 && points: <200", qmap["filter_by"]);

    qs = "?q=bar&filter_by=points%3A%20%3E100%20%26%26%20points%3A%20%3C200&";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points: >100 && points: <200", qmap["filter_by"]);

    qs = "q=bar&filter_by=baz&&";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("baz&", qmap["filter_by"]);

    qs = "q=bar&filter_by=";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("", qmap["filter_by"]);

    qs = "q=bread && breakfast&filter_by=";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bread && breakfast", qmap["q"]);
    ASSERT_EQ("", qmap["filter_by"]);

    qs = "q=bread & breakfast&filter_by=";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(3, qmap.size());
    ASSERT_EQ("bread ", qmap["q"]);
    ASSERT_EQ("", qmap[" breakfast"]);
    ASSERT_EQ("", qmap["filter_by"]);

    qs = "q=bar&filter_by=&";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("", qmap["filter_by"]);

    qs = "q=bar&filter_by=points :> 100&enable_typos";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(3, qmap.size());
    ASSERT_EQ("bar", qmap["q"]);
    ASSERT_EQ("points :> 100", qmap["filter_by"]);
    ASSERT_EQ("", qmap["enable_typos"]);

    qs = "foo=bar&baz=&bazinga=true";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(3, qmap.size());
    ASSERT_EQ("bar", qmap["foo"]);
    ASSERT_EQ("", qmap["baz"]);
    ASSERT_EQ("true", qmap["bazinga"]);

    qs = "foo=bar&bazinga=true&foo=buzz";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("buzz", qmap["foo"]);
    ASSERT_EQ("true", qmap["bazinga"]);

    qs = "filter_by=points:>100&bazinga=true&filter_by=points:<=200";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("points:>100&&points:<=200", qmap["filter_by"]);
    ASSERT_EQ("true", qmap["bazinga"]);

    qs = "filter_by=points:>100 && brand:= nike&bazinga=true&filter_by=points:<=200";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(2, qmap.size());
    ASSERT_EQ("points:>100 && brand:= nike&&points:<=200", qmap["filter_by"]);
    ASSERT_EQ("true", qmap["bazinga"]);

    qs = "foo";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(1, qmap.size());
    ASSERT_EQ("", qmap["foo"]);

    qs = "?foo=";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(1, qmap.size());
    ASSERT_EQ("", qmap["foo"]);

    qs = "?foo";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(1, qmap.size());
    ASSERT_EQ("", qmap["foo"]);

    qs = "?";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(0, qmap.size());

    qs = "";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(0, qmap.size());

    qs = "&";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(0, qmap.size());

    qs = "&&";
    qmap.clear();
    StringUtils::parse_query_string(qs, qmap);
    ASSERT_EQ(0, qmap.size());
}

TEST(StringUtilsTest, ShouldParseStringifiedList) {
    std::string str = "John Galt, Random Jack";
    std::vector<std::string> strs;

    StringUtils::split_to_values(str, strs);
    ASSERT_EQ(2, strs.size());
    ASSERT_EQ("John Galt", strs[0]);
    ASSERT_EQ("Random Jack", strs[1]);

    strs.clear();
    str = "`John Galt`, `Random, Jack`";
    StringUtils::split_to_values(str, strs);
    ASSERT_EQ(2, strs.size());
    ASSERT_EQ("John Galt", strs[0]);
    ASSERT_EQ("Random, Jack", strs[1]);

    strs.clear();
    str = "`John Galt, `Random, Jack`";
    StringUtils::split_to_values(str, strs);
    ASSERT_EQ(2, strs.size());
    ASSERT_EQ("John Galt, Random", strs[0]);
    ASSERT_EQ("Jack", strs[1]);

    strs.clear();
    str = "`Traveller's \\`delight\\`!`, Not wrapped, Last word";
    StringUtils::split_to_values(str, strs);
    ASSERT_EQ(3, strs.size());
    ASSERT_EQ("Traveller's \\`delight\\`!", strs[0]);
    ASSERT_EQ("Not wrapped", strs[1]);
    ASSERT_EQ("Last word", strs[2]);

    strs.clear();
    str = "`John Galt`";
    StringUtils::split_to_values(str, strs);
    ASSERT_EQ(1, strs.size());
    ASSERT_EQ("John Galt", strs[0]);
}

TEST(StringUtilsTest, ShouldTrimCurlySpaces) {
    ASSERT_EQ("foo {bar}", StringUtils::trim_curly_spaces("foo { bar }"));
    ASSERT_EQ("foo  {bar}", StringUtils::trim_curly_spaces("foo  { bar }"));
    ASSERT_EQ("", StringUtils::trim_curly_spaces(""));
    ASSERT_EQ("{}", StringUtils::trim_curly_spaces("{ }"));
    ASSERT_EQ("foo {bar} {baz}", StringUtils::trim_curly_spaces("foo { bar } {  baz}"));
}

TEST(StringUtilsTest, ContainsWord) {
    ASSERT_TRUE(StringUtils::contains_word("foo bar", "foo"));
    ASSERT_TRUE(StringUtils::contains_word("foo bar", "bar"));
    ASSERT_TRUE(StringUtils::contains_word("foo bar baz", "bar"));
    ASSERT_TRUE(StringUtils::contains_word("foo bar baz", "foo bar"));
    ASSERT_TRUE(StringUtils::contains_word("foo bar baz", "bar baz"));

    ASSERT_FALSE(StringUtils::contains_word("foobar", "bar"));
    ASSERT_FALSE(StringUtils::contains_word("foobar", "foo"));
    ASSERT_FALSE(StringUtils::contains_word("foobar baz", "bar"));
    ASSERT_FALSE(StringUtils::contains_word("foobar baz", "bar baz"));
    ASSERT_FALSE(StringUtils::contains_word("baz foobar", "foo"));
}

TEST(StringUtilsTest, ShouldSplitRangeFacet){
    std::string range_facets_string = "score(fail:[0, 40], pass:[40, 100]), grade(A:[80,100], B:[60, 80], C:[40, 60])";
    std::vector<std::string> range_facets;
    StringUtils::split_facet(range_facets_string, range_facets);
    ASSERT_STREQ("score(fail:[0, 40], pass:[40, 100])", range_facets[0].c_str());
    ASSERT_STREQ("grade(A:[80,100], B:[60, 80], C:[40, 60])", range_facets[1].c_str());


    std::string facets_string = "score, grade";
    std::vector<std::string> facets;
    StringUtils::split_facet(facets_string, facets);
    ASSERT_STREQ("score", facets[0].c_str());
    ASSERT_STREQ("grade", facets[1].c_str());


    std::string mixed_facets_string = "score, grade(A:[80,100], B:[60, 80], C:[40, 60]), rank";
    std::vector<std::string> mixed_facets;
    StringUtils::split_facet(mixed_facets_string, mixed_facets);
    ASSERT_STREQ("score", mixed_facets[0].c_str());
    ASSERT_STREQ("grade(A:[80,100], B:[60, 80], C:[40, 60])", mixed_facets[1].c_str());
    ASSERT_STREQ("rank", mixed_facets[2].c_str());


    // empty string should produce empty list
    std::vector<std::string> lines_empty;
    StringUtils::split_facet("", lines_empty);
    ASSERT_TRUE(lines_empty.empty());
}

void tokenizeTestHelper(const std::string& filter_query, const std::vector<std::string>& tokenList) {
    std::queue<std::string> tokenizeOutput;
    auto tokenize_op = StringUtils::tokenize_filter_query(filter_query, tokenizeOutput);
    ASSERT_TRUE(tokenize_op.ok());
    for (auto const& token: tokenList) {
        ASSERT_EQ(token, tokenizeOutput.front());
        tokenizeOutput.pop();
    }
    ASSERT_TRUE(tokenizeOutput.empty());
}

TEST(StringUtilsTest, TokenizeFilterQuery) {
    std::string filter_query;
    std::vector<std::string> tokenList;

    filter_query = "name: Steve Smith";
    tokenList = {"name: Steve Smith"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "name: `Toccata & Fugue`";
    tokenList = {"name: `Toccata & Fugue`"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "name: [Steve Smith, `Jack & Jill`]";
    tokenList = {"name: [Steve Smith, `Jack & Jill`]"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "age:[10..100]";
    tokenList = {"age:[10..100]"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "age:>20 && category:= [`Running Shoes, Men`, Sneaker]";
    tokenList = {"age:>20", "&&", "category:= [`Running Shoes, Men`, Sneaker]"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "location:(48.906, 2.343, 5 mi)";
    tokenList = {"location:(48.906, 2.343, 5 mi)"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "((age: <5 || age: >10) && category:= [shoes]) || is_curated: true";
    tokenList = {"(", "(", "age: <5", "||", "age: >10", ")", "&&", "category:= [shoes]", ")", "||", "is_curated: true"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "((age:<5||age:>10)&&location:(48.906,2.343,5mi))||tags:AT&T";
    tokenList = {"(", "(", "age:<5", "||", "age:>10", ")", "&&", "location:(48.906,2.343,5mi)", ")", "||", "tags:AT&T"};
    tokenizeTestHelper(filter_query, tokenList);

    filter_query = "((age: <5 || age: >10) && category:= [shoes]) && $Customers(customer_id:=customer_a && (product_price:>100 && product_price:<200))";
    tokenList = {"(", "(", "age: <5", "||", "age: >10", ")", "&&", "category:= [shoes]", ")", "&&", "$Customers(customer_id:=customer_a && (product_price:>100 && product_price:<200))"};
    tokenizeTestHelper(filter_query, tokenList);
}
