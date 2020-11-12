#include <gtest/gtest.h>
#include "tokenizer.h"

TEST(TokenizerTest, ShouldTokenizeNormalizeDifferentStrings) {
    const std::string withnewline = "Michael Jordan:\nWelcome, everybody. Welcome!";
    std::vector<std::string> tokens;
    Tokenizer(withnewline, true, true).tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());
    ASSERT_STREQ("welcome", tokens[2].c_str());
    ASSERT_STREQ("everybody", tokens[3].c_str());
    ASSERT_STREQ("welcome", tokens[4].c_str());

    const std::string withspaces = " Michael  Jordan  ";
    tokens.clear();
    Tokenizer(withspaces, true, true).tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_STREQ("", tokens[0].c_str());
    ASSERT_STREQ("michael", tokens[1].c_str());
    ASSERT_STREQ("", tokens[2].c_str());
    ASSERT_STREQ("jordan", tokens[3].c_str());
    ASSERT_STREQ("", tokens[4].c_str());

    tokens.clear();
    Tokenizer(withspaces, false, true).tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());

    const std::string withspecialchars = "Special ½¥ and தமிழ் 你好吗 abcÅà123ß12 here.";
    tokens.clear();
    Tokenizer(withspecialchars, false, true).tokenize(tokens);
    ASSERT_EQ(7, tokens.size());
    ASSERT_STREQ("special", tokens[0].c_str());
    ASSERT_STREQ("12yen", tokens[1].c_str());
    ASSERT_STREQ("and", tokens[2].c_str());
    ASSERT_STREQ("தமிழ்", tokens[3].c_str());
    ASSERT_STREQ("你好吗", tokens[4].c_str());
    ASSERT_STREQ("abcaa123ss12", tokens[5].c_str());
    ASSERT_STREQ("here", tokens[6].c_str());

    // when normalize is false, should be verbatim

    tokens.clear();
    Tokenizer(withspecialchars, false, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ(withspecialchars.c_str(), tokens[0].c_str());
}

TEST(TokenizerTest, ShouldTokenizeIteratively) {
    const std::string withnewline = "Michael Jordan:\n\nWelcome, everybody. Welcome!";
    std::vector<std::string> tokens;
    Tokenizer tokenizer1(withnewline, true, true);

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

    // verbatim (normalize=false)

    tokens.clear();
    Tokenizer tokenizer2(withnewline, true, false);

    while(tokenizer2.next(token, token_index)) {
        tokens.push_back(token);
    }

    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("Michael Jordan:\n\nWelcome, everybody. Welcome!", tokens[0].c_str());
}
