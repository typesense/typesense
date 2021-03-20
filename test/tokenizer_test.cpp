#include <gtest/gtest.h>
#include "tokenizer.h"

TEST(TokenizerTest, ShouldTokenizeNormalizeDifferentStrings) {
    const std::string withnewline = "Michael Jordan:\nWelcome, everybody. Welcome! ";
    std::vector<std::string> tokens;
    Tokenizer(withnewline, false, true, false).tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());
    ASSERT_STREQ("welcome", tokens[2].c_str());
    ASSERT_STREQ("everybody", tokens[3].c_str());
    ASSERT_STREQ("welcome", tokens[4].c_str());

    const std::string withspaces = " Michael  Jordan  ";
    tokens.clear();
    Tokenizer(withspaces, true, true, false).tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_STREQ(" ", tokens[0].c_str());
    ASSERT_STREQ("michael", tokens[1].c_str());
    ASSERT_STREQ("  ", tokens[2].c_str());
    ASSERT_STREQ("jordan", tokens[3].c_str());
    ASSERT_STREQ("  ", tokens[4].c_str());

    tokens.clear();
    Tokenizer(withspaces, false, true, false).tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());

    // single token
    const std::string single_token = "foobar";
    tokens.clear();
    Tokenizer(single_token, false, false, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("foobar", tokens[0].c_str());

    // split tokens
    const std::string split_tokens = "foo-bar-baz";
    tokens.clear();
    Tokenizer(split_tokens, false, false, false).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_STREQ("foo", tokens[0].c_str());
    ASSERT_STREQ("bar", tokens[1].c_str());
    ASSERT_STREQ("baz", tokens[2].c_str());

    tokens.clear();
    Tokenizer(split_tokens, false, true, false).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_STREQ("foo", tokens[0].c_str());
    ASSERT_STREQ("bar", tokens[1].c_str());
    ASSERT_STREQ("baz", tokens[2].c_str());

    // multiple spaces
    const std::string multispace_tokens = "foo     bar";
    tokens.clear();
    Tokenizer(multispace_tokens, true, false, false).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_STREQ("foo", tokens[0].c_str());
    ASSERT_STREQ("     ", tokens[1].c_str());
    ASSERT_STREQ("bar", tokens[2].c_str());

    // special chars
    const std::string specialchar_tokens = "https://www.amazon.com/s?k=phone&ref=nb_sb_noss_2";;
    tokens.clear();
    Tokenizer(specialchar_tokens, true, false, false).tokenize(tokens);
    ASSERT_EQ(23, tokens.size());
    ASSERT_STREQ("https", tokens[0].c_str());
    ASSERT_STREQ("://", tokens[1].c_str());
    ASSERT_STREQ("www", tokens[2].c_str());
    ASSERT_STREQ(".", tokens[3].c_str());
    ASSERT_STREQ("noss", tokens[20].c_str());
    ASSERT_STREQ("_", tokens[21].c_str());
    ASSERT_STREQ("2", tokens[22].c_str());

    // noop

    tokens.clear();
    const std::string withspecialchars = "Special ½¥ and தமிழ் 你好吗 abcÅà123ß12 here.";
    Tokenizer(withspecialchars, false, true, true).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ(withspecialchars.c_str(), tokens[0].c_str());
}

TEST(TokenizerTest, ShouldTokenizeNormalizeUnicodeStrings) {
    std::vector<std::string> tokens;

    const std::string withspecialchars = "Special ½¥ and -தமிழ் 你2好吗 abcÅà123ß12 here.";
    tokens.clear();
    Tokenizer(withspecialchars, false, true, false).tokenize(tokens);
    ASSERT_EQ(7, tokens.size());
    ASSERT_STREQ("special", tokens[0].c_str());
    ASSERT_STREQ("12yen", tokens[1].c_str());
    ASSERT_STREQ("and", tokens[2].c_str());
    ASSERT_STREQ("தமிழ்", tokens[3].c_str());
    ASSERT_STREQ("你2好吗", tokens[4].c_str());
    ASSERT_STREQ("abcaa123ss12", tokens[5].c_str());
    ASSERT_STREQ("here", tokens[6].c_str());

    // when normalization is disabled and keep empty is enabled
    const std::string withoutnormalize = "Mise  à,  jour.";
    tokens.clear();
    Tokenizer(withoutnormalize, true, false, false).tokenize(tokens);
    ASSERT_EQ(6, tokens.size());
    ASSERT_STREQ("Mise", tokens[0].c_str());
    ASSERT_STREQ("  ", tokens[1].c_str());
    ASSERT_STREQ("à", tokens[2].c_str());
    ASSERT_STREQ(",  ", tokens[3].c_str());
    ASSERT_STREQ("jour", tokens[4].c_str());
    ASSERT_STREQ(".", tokens[5].c_str());

    // when normalization and keep empty are disabled
    const std::string withoutnormalizeandkeepempty = "Mise  à  jour.";
    tokens.clear();
    Tokenizer(withoutnormalizeandkeepempty, false, false, false).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_STREQ("Mise", tokens[0].c_str());
    ASSERT_STREQ("à", tokens[1].c_str());
    ASSERT_STREQ("jour", tokens[2].c_str());

    // single accented word tokenization
    std::string singleword = "à";
    tokens.clear();
    Tokenizer(singleword, false, true, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("a", tokens[0].c_str());

    tokens.clear();
    Tokenizer(singleword, true, true, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("a", tokens[0].c_str());
}

TEST(TokenizerTest, ShouldTokenizeIteratively) {
    const std::string withnewline = "Michael Jordan:\n\nWelcome, everybody. Welcome!";
    std::vector<std::string> tokens;
    Tokenizer tokenizer1(withnewline, true, true, false);

    std::string token;
    size_t token_index;

    while(tokenizer1.next(token, token_index)) {
        tokens.push_back(token);
    }

    ASSERT_EQ(10, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ(" ", tokens[1].c_str());
    ASSERT_STREQ("jordan", tokens[2].c_str());
    ASSERT_STREQ(":\n\n", tokens[3].c_str());
    ASSERT_STREQ("welcome", tokens[4].c_str());
    ASSERT_STREQ(", ", tokens[5].c_str());
    ASSERT_STREQ("everybody", tokens[6].c_str());
    ASSERT_STREQ(". ", tokens[7].c_str());
    ASSERT_STREQ("welcome", tokens[8].c_str());
    ASSERT_STREQ("!", tokens[9].c_str());

    // check for index when separators are not kept
    Tokenizer tokenizer2(withnewline, false, true, false);
    size_t expected_token_index = 0;
    std::vector<std::string> expected_tokens = {"michael", "jordan", "welcome", "everybody", "welcome"};
    while(tokenizer2.next(token, token_index)) {
        ASSERT_EQ(expected_token_index, token_index);
        ASSERT_EQ(expected_tokens[expected_token_index], token);
        expected_token_index++;
    }

    // verbatim (no_op=true)

    tokens.clear();
    Tokenizer tokenizer3(withnewline, true, false, true);

    while(tokenizer3.next(token, token_index)) {
        tokens.push_back(token);
    }

    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("Michael Jordan:\n\nWelcome, everybody. Welcome!", tokens[0].c_str());
}
