#include <gtest/gtest.h>
#include "tokenizer.h"

TEST(TokenizerTest, ShouldTokenizeNormalizeDifferentStrings) {
    const std::string withnewline = "Michael Jordan:\nWelcome, everybody. Welcome! ";
    std::vector<std::string> tokens;
    Tokenizer(withnewline, true, true, false).tokenize(tokens);
    ASSERT_EQ(6, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());
    ASSERT_STREQ("welcome", tokens[2].c_str());
    ASSERT_STREQ("everybody", tokens[3].c_str());
    ASSERT_STREQ("welcome", tokens[4].c_str());
    ASSERT_STREQ("", tokens[5].c_str());

    const std::string withspaces = " Michael  Jordan  ";
    tokens.clear();
    Tokenizer(withspaces, true, true, false).tokenize(tokens);
    ASSERT_EQ(6, tokens.size());
    ASSERT_STREQ("", tokens[0].c_str());
    ASSERT_STREQ("michael", tokens[1].c_str());
    ASSERT_STREQ("", tokens[2].c_str());
    ASSERT_STREQ("jordan", tokens[3].c_str());
    ASSERT_STREQ("", tokens[4].c_str());
    ASSERT_STREQ("", tokens[5].c_str());

    tokens.clear();
    Tokenizer(withspaces, false, true, false).tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());

    const std::string withspecialchars = "Special ½¥ and தமிழ் 你好吗 abcÅà123ß12 here.";
    tokens.clear();
    Tokenizer(withspecialchars, false, true, false).tokenize(tokens);
    ASSERT_EQ(7, tokens.size());
    ASSERT_STREQ("special", tokens[0].c_str());
    ASSERT_STREQ("12yen", tokens[1].c_str());
    ASSERT_STREQ("and", tokens[2].c_str());
    ASSERT_STREQ("தமிழ்", tokens[3].c_str());
    ASSERT_STREQ("你好吗", tokens[4].c_str());
    ASSERT_STREQ("abcaa123ss12", tokens[5].c_str());
    ASSERT_STREQ("here", tokens[6].c_str());

    // when normalization is disabled and keep empty is enabled
    const std::string withoutnormalize = "Mise  à,  jour.";
    tokens.clear();
    Tokenizer(withoutnormalize, true, false, false).tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_STREQ("Mise", tokens[0].c_str());
    ASSERT_STREQ("", tokens[1].c_str());
    ASSERT_STREQ("à,", tokens[2].c_str());
    ASSERT_STREQ("", tokens[3].c_str());
    ASSERT_STREQ("jour.", tokens[4].c_str());

    // when normalization and keep empty are disabled
    const std::string withoutnormalizeandkeepempty = "Mise  à  jour.";
    tokens.clear();
    Tokenizer(withoutnormalizeandkeepempty, false, false, false).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_STREQ("Mise", tokens[0].c_str());
    ASSERT_STREQ("à", tokens[1].c_str());
    ASSERT_STREQ("jour.", tokens[2].c_str());

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
    ASSERT_EQ(6, tokens.size());
    ASSERT_STREQ("foo", tokens[0].c_str());
    ASSERT_STREQ("", tokens[1].c_str());
    ASSERT_STREQ("", tokens[2].c_str());
    ASSERT_STREQ("", tokens[3].c_str());
    ASSERT_STREQ("", tokens[4].c_str());
    ASSERT_STREQ("bar", tokens[5].c_str());

    // noop

    tokens.clear();
    Tokenizer(withspecialchars, false, true, true).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ(withspecialchars.c_str(), tokens[0].c_str());
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

    ASSERT_EQ(6, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());
    ASSERT_STREQ("", tokens[2].c_str());
    ASSERT_STREQ("welcome", tokens[3].c_str());
    ASSERT_STREQ("everybody", tokens[4].c_str());
    ASSERT_STREQ("welcome", tokens[5].c_str());

    // verbatim (no_op=true)

    tokens.clear();
    Tokenizer tokenizer2(withnewline, true, false, true);

    while(tokenizer2.next(token, token_index)) {
        tokens.push_back(token);
    }

    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("Michael Jordan:\n\nWelcome, everybody. Welcome!", tokens[0].c_str());
}
