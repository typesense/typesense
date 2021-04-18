#include <gtest/gtest.h>
#include "tokenizer.h"
#include "logger.h"

TEST(TokenizerTest, ShouldTokenizeNormalizeDifferentStrings) {
    const std::string withaccent = "Mise T.J. à  jour  Timy depuis PC";
    std::vector<std::string> tokens;
    Tokenizer(withaccent, true, false).tokenize(tokens);

    std::vector<std::string> withaccent_tokens = {"mise", "tj", "a", "jour", "timy", "depuis", "pc"};
    ASSERT_EQ(withaccent_tokens.size(), tokens.size());
    for(size_t i = 0; i < withaccent_tokens.size(); i++) {
        ASSERT_EQ(withaccent_tokens[i], tokens[i]);
    }

    const std::string withnewline = "Michael Jordan:\nWelcome, everybody. Welcome! ";
    tokens.clear();
    Tokenizer(withnewline, true, false).tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());
    ASSERT_STREQ("welcome", tokens[2].c_str());
    ASSERT_STREQ("everybody", tokens[3].c_str());
    ASSERT_STREQ("welcome", tokens[4].c_str());

    const std::string withspaces = " Michael  Jordan  ";
    tokens.clear();
    Tokenizer(withspaces, true, false).tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());

    // single token
    const std::string single_token = "foobar";
    tokens.clear();
    Tokenizer(single_token, false, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("foobar", tokens[0].c_str());

    // split tokens
    const std::string split_tokens = "foo-bar-baz";
    tokens.clear();
    Tokenizer(split_tokens, false, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("foobarbaz", tokens[0].c_str());

    tokens.clear();
    Tokenizer(split_tokens, true, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("foobarbaz", tokens[0].c_str());

    // multiple spaces
    const std::string multispace_tokens = "foo     bar";
    tokens.clear();
    Tokenizer(multispace_tokens, false, false).tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_STREQ("foo", tokens[0].c_str());
    ASSERT_STREQ("bar", tokens[1].c_str());

    // special chars
    const std::string specialchar_tokens = "https://www.amazon.com/s?k=phone&ref=nb_sb_noss_2";
    tokens.clear();
    Tokenizer(specialchar_tokens, false, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("httpswwwamazoncomskphonerefnbsbnoss2", tokens[0].c_str());

    // noop

    tokens.clear();
    const std::string withspecialchars = "Special ½¥ and தமிழ் 你好吗 abcÅà123ß12 here.";
    Tokenizer(withspecialchars, true, true).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ(withspecialchars.c_str(), tokens[0].c_str());
}

TEST(TokenizerTest, ShouldTokenizeNormalizeUnicodeStrings) {
    std::vector<std::string> tokens;

    const std::string withspecialchars = "Special ½¥ and -thenதமிழ், 你2好吗 abcÅà123ß12 verläßlich here.";
    tokens.clear();
    Tokenizer(withspecialchars, true, false).tokenize(tokens);
    ASSERT_EQ(8, tokens.size());
    ASSERT_STREQ("special", tokens[0].c_str());
    ASSERT_STREQ("12yen", tokens[1].c_str());
    ASSERT_STREQ("and", tokens[2].c_str());
    ASSERT_STREQ("thenதமிழ்", tokens[3].c_str());
    ASSERT_STREQ("你2好吗", tokens[4].c_str());
    ASSERT_STREQ("abcaa123ss12", tokens[5].c_str());
    ASSERT_STREQ("verlasslich", tokens[6].c_str());
    ASSERT_STREQ("here", tokens[7].c_str());

    // when normalization is disabled
    const std::string withoutnormalize = "Mise  à,  jour.";
    tokens.clear();
    Tokenizer(withoutnormalize, false, false).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_STREQ("Mise", tokens[0].c_str());
    ASSERT_STREQ("à", tokens[1].c_str());
    ASSERT_STREQ("jour", tokens[2].c_str());

    // single accented word tokenization
    std::string singleword = "à";
    tokens.clear();
    Tokenizer(singleword, true, false).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("a", tokens[0].c_str());
}

TEST(TokenizerTest, ShouldTokenizeIteratively) {
    const std::string withnewline = "Michael Jordan:\n\nWelcome, everybody. Welcome!";
    std::vector<std::string> tokens;
    Tokenizer tokenizer1(withnewline, true, false);

    std::string token;
    size_t token_index;

    while(tokenizer1.next(token, token_index)) {
        tokens.push_back(token);
    }

    ASSERT_EQ(5, tokens.size());
    ASSERT_STREQ("michael", tokens[0].c_str());
    ASSERT_STREQ("jordan", tokens[1].c_str());
    ASSERT_STREQ("welcome", tokens[2].c_str());
    ASSERT_STREQ("everybody", tokens[3].c_str());
    ASSERT_STREQ("welcome", tokens[4].c_str());

    // check for index when separators are not kept
    Tokenizer tokenizer2(withnewline, true, false);
    size_t expected_token_index = 0;
    std::vector<std::string> expected_tokens = {"michael", "jordan", "welcome", "everybody", "welcome"};
    while(tokenizer2.next(token, token_index)) {
        ASSERT_EQ(expected_token_index, token_index);
        ASSERT_EQ(expected_tokens[expected_token_index], token);
        expected_token_index++;
    }

    // verbatim (no_op=true)

    tokens.clear();
    Tokenizer tokenizer3(withnewline, false, true);

    while(tokenizer3.next(token, token_index)) {
        tokens.push_back(token);
    }

    ASSERT_EQ(1, tokens.size());
    ASSERT_STREQ("Michael Jordan:\n\nWelcome, everybody. Welcome!", tokens[0].c_str());
}

TEST(TokenizerTest, ShouldTokenizeTextWithCustomSpecialChars) {
    std::vector<std::string> tokens;
    Tokenizer("and -some -more", true, false, "en", {'-'}).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("and", tokens[0]);
    ASSERT_EQ("-some", tokens[1]);
    ASSERT_EQ("-more", tokens[2]);
}

TEST(TokenizerTest, ShouldTokenizeLocaleText) {
    std::string tstr = "ลงรถไฟ";
    std::vector<std::string> ttokens;
    Tokenizer(tstr, true, false, "th").tokenize(ttokens);

    std::string str = "จิ้งจอกสีน้ำตาลด่วน";
    std::vector<std::string> tokens;
    Tokenizer(str, false, false, "th").tokenize(tokens);

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ("จิ้งจอก", tokens[0]);
    ASSERT_EQ("สี", tokens[1]);
    ASSERT_EQ("น้ำตาล", tokens[2]);
    ASSERT_EQ("ด่วน", tokens[3]);

    tokens.clear();
    str = "น. วันที่ 31 มี.ค.";
    Tokenizer(str, false, false, "th").tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_EQ("น", tokens[0]);
    ASSERT_EQ("วัน", tokens[1]);
    ASSERT_EQ("ที่", tokens[2]);
    ASSERT_EQ("31", tokens[3]);
    ASSERT_EQ("มี.ค", tokens[4]);

    tokens.clear();
    Tokenizer("Odd Thomas", false, false, "en").tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ("Odd", tokens[0]);
    ASSERT_EQ("Thomas", tokens[1]);

    // korean

    tokens.clear();
    Tokenizer("경승지·산악·협곡", false, false, "ko").tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("경승지", tokens[0]);
    ASSERT_EQ("산악", tokens[1]);
    ASSERT_EQ("협곡", tokens[2]);

    tokens.clear();
    Tokenizer("안녕은하철도999극장판", false, false, "ko").tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("안녕은하철도", tokens[0]);
    ASSERT_EQ("999", tokens[1]);
    ASSERT_EQ("극장판", tokens[2]);

    // japanese
    tokens.clear();
    Tokenizer("退屈", false, false, "ja").tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ("た", tokens[0]);
    ASSERT_EQ("いくつ", tokens[1]);

    tokens.clear();
    Tokenizer("ア退屈であ", false, false, "ja").tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_EQ("あ", tokens[0]);
    ASSERT_EQ("た", tokens[1]);
    ASSERT_EQ("いくつ", tokens[2]);
    ASSERT_EQ("で", tokens[3]);
    ASSERT_EQ("あ", tokens[4]);

    tokens.clear();
    Tokenizer("怠惰な犬", false, false, "ja").tokenize(tokens);
    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ("たい", tokens[0]);
    ASSERT_EQ("だ", tokens[1]);
    ASSERT_EQ("な", tokens[2]);
    ASSERT_EQ("いぬ", tokens[3]);
}
