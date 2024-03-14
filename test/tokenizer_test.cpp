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

    // check for index when token_separators are not kept
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

TEST(TokenizerTest, ShouldTokenizeChineseText) {
    std::vector<std::string> tokens;

    // traditional -> simplified
    Tokenizer("語", false, false, "zh").tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ("语", tokens[0]);

    tokens.clear();
    Tokenizer("說", false, false, "zh").tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ("说", tokens[0]);

    // tokenize traditional
    tokens.clear();
    Tokenizer("愛並不會因時間而", false, false, "zh").tokenize(tokens);
    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ("爱", tokens[0]);
    ASSERT_EQ("并不", tokens[1]);
    ASSERT_EQ("会", tokens[2]);
    ASSERT_EQ("因", tokens[3]);
    ASSERT_EQ("时间", tokens[4]);
    ASSERT_EQ("而", tokens[5]);

    // tokenize simplified
    tokens.clear();
    Tokenizer("爱并不会因时间而", false, false, "zh").tokenize(tokens);
    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ("爱", tokens[0]);
    ASSERT_EQ("并不", tokens[1]);
    ASSERT_EQ("会", tokens[2]);
    ASSERT_EQ("因", tokens[3]);
    ASSERT_EQ("时间", tokens[4]);
    ASSERT_EQ("而", tokens[5]);

    // with token_separators
    tokens.clear();
    Tokenizer("很久以前，傳說在臺中北屯的一個地方", false, false, "zh").tokenize(tokens);
    ASSERT_EQ(10, tokens.size());
    ASSERT_EQ("很久", tokens[0]);
    ASSERT_EQ("以前", tokens[1]);
    ASSERT_EQ("传说", tokens[2]);
    ASSERT_EQ("在", tokens[3]);
    ASSERT_EQ("台中", tokens[4]);
    ASSERT_EQ("北", tokens[5]);
    ASSERT_EQ("屯", tokens[6]);
    ASSERT_EQ("的", tokens[7]);
    ASSERT_EQ("一个", tokens[8]);
    ASSERT_EQ("地方", tokens[9]);

    tokens.clear();
    Tokenizer("朵雲──海", false, false, "zh").tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("朵", tokens[0]);
    ASSERT_EQ("云", tokens[1]);
    ASSERT_EQ("海", tokens[2]);

    tokens.clear();
    Tokenizer("山丘上。媽媽", false, false, "zh").tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("山丘", tokens[0]);
    ASSERT_EQ("上", tokens[1]);
    ASSERT_EQ("妈妈", tokens[2]);
}

TEST(TokenizerTest, ShouldTokenizeLocaleText) {
    std::vector<std::string> tokens;

    tokens.clear();
    std::string str = "ความเหลื่อมล้ำ";
    Tokenizer(str, true, false, "th").tokenize(tokens);
    //ASSERT_EQ(2, tokens.size());

    tokens.clear();
    str = "เหลื่";
    Tokenizer(str, false, false, "th").tokenize(tokens);
    //ASSERT_EQ(1, tokens.size());

    tokens.clear();
    str = "จิ้งจอกสีน้ำตาลด่วน";
    Tokenizer(str, true, false, "th").tokenize(tokens);
    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ("จิ้งจอก", tokens[0]);
    ASSERT_EQ("สี", tokens[1]);
    ASSERT_EQ("น้ําตาล", tokens[2]);
    ASSERT_EQ("ด่วน", tokens[3]);

    tokens.clear();
    str = "น. วันที่ 31 มี.ค.";
    Tokenizer(str, false, false, "th").tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_EQ("น", tokens[0]);
    ASSERT_EQ("วัน", tokens[1]);
    ASSERT_EQ("ที่", tokens[2]);
    ASSERT_EQ("31", tokens[3]);
    ASSERT_EQ("มีค", tokens[4]);

    tokens.clear();
    str = "12345_678";
    Tokenizer(str, false, false, "th").tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ("12345678", tokens[0]);

    tokens.clear();
    Tokenizer("Odd Thomas", false, false, "en").tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ("Odd", tokens[0]);
    ASSERT_EQ("Thomas", tokens[1]);

    // korean

    tokens.clear();
    Tokenizer("경승지·산악·협곡", false, false, "ko").tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("경승지", tokens[0]);
    ASSERT_EQ("산악", tokens[1]);
    ASSERT_EQ("협곡", tokens[2]);

    tokens.clear();
    Tokenizer("안녕은하철도999극장판", false, false, "ko").tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("안녕은하철도", tokens[0]);
    ASSERT_EQ("999", tokens[1]);
    ASSERT_EQ("극장판", tokens[2]);

    // japanese
    tokens.clear();
    Tokenizer("退屈", true, false, "ja").tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ("た", tokens[0]);
    ASSERT_EQ("いくつ", tokens[1]);

    tokens.clear();
    Tokenizer("魈", true, false, "ja").tokenize(tokens);
    ASSERT_EQ(0, tokens.size());

    tokens.clear();
    Tokenizer("「業果材", true, false, "ja").tokenize(tokens);
    ASSERT_EQ(6, tokens.size());

    tokens.clear();
    Tokenizer("ア退屈であ", true, false, "ja").tokenize(tokens);
    ASSERT_EQ(5, tokens.size());
    ASSERT_EQ("あ", tokens[0]);
    ASSERT_EQ("た", tokens[1]);
    ASSERT_EQ("いくつ", tokens[2]);
    ASSERT_EQ("で", tokens[3]);
    ASSERT_EQ("あ", tokens[4]);

    tokens.clear();
    Tokenizer("怠惰な犬", true, false, "ja").tokenize(tokens);
    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ("たい", tokens[0]);
    ASSERT_EQ("だ", tokens[1]);
    ASSERT_EQ("な", tokens[2]);
    ASSERT_EQ("いぬ", tokens[3]);

    tokens.clear();
    Tokenizer("今ぶり拍治ルツ", true, false, "ja").tokenize(tokens);
    ASSERT_EQ(9, tokens.size());
    ASSERT_EQ("いま", tokens[0]);
    ASSERT_EQ("ぶり", tokens[1]);
    ASSERT_EQ("は", tokens[2]);
    ASSERT_EQ("く", tokens[3]);
    ASSERT_EQ("お", tokens[4]);
    ASSERT_EQ("さ", tokens[5]);
    ASSERT_EQ("む", tokens[6]);
    ASSERT_EQ("る", tokens[7]);
    ASSERT_EQ("つ", tokens[8]);

    tokens.clear();  // 配管
    Tokenizer("配管", true, false, "ja").tokenize(tokens);

    // persian containing zwnj
    tokens.clear();
    Tokenizer("روان\u200Cشناسی", false, false, "fa").tokenize(tokens);
    ASSERT_EQ(2, tokens.size());
}

TEST(TokenizerTest, ShouldTokenizeLocaleTextWithEnglishText) {
    std::string tstr = "ผู้เขียนมีความสนใจเกี่ยวกับ Discrete Math และการคำนวณโดยทั่วไป";
    std::vector<std::string> ttokens;
    Tokenizer(tstr, true, false, "th").tokenize(ttokens);
    ASSERT_EQ(14, ttokens.size());
    ASSERT_EQ("discrete", ttokens[7]);
    ASSERT_EQ("math", ttokens[8]);
}

TEST(TokenizerTest, ShouldRemoveGenericPunctuationFromThaiText) {
    std::string tstr = "f’’b";
    std::vector<std::string> ttokens;
    Tokenizer(tstr, true, false, "th").tokenize(ttokens);
    ASSERT_EQ(2, ttokens.size());
    ASSERT_EQ("f", ttokens[0]);
    ASSERT_EQ("b", ttokens[1]);

    ttokens.clear();
    tstr = "Lay’s";
    Tokenizer(tstr, true, false, "th").tokenize(ttokens);
    ASSERT_EQ(1, ttokens.size());
    ASSERT_EQ("lays", ttokens[0]);
}

TEST(TokenizerTest, ShouldTokenizeLocaleTextWithSwedishText) {
    std::string tstr = "södra";
    std::vector<std::string> ttokens;
    Tokenizer(tstr, true, false, "sv").tokenize(ttokens);
    ASSERT_EQ(1, ttokens.size());
    ASSERT_EQ("södra", ttokens[0]);

    tstr = "Ängelholm";
    ttokens.clear();
    Tokenizer(tstr, true, false, "sv").tokenize(ttokens);
    ASSERT_EQ(1, ttokens.size());
    ASSERT_EQ("ängelholm", ttokens[0]);

    tstr = "Ängelholm";
    ttokens.clear();
    Tokenizer(tstr, true, false, "").tokenize(ttokens);
    ASSERT_EQ(1, ttokens.size());
    ASSERT_EQ("angelholm", ttokens[0]);
}

TEST(TokenizerTest, ShouldTokenizeWithDifferentSymbolConfigs) {
    std::string str1 = "ความ-เหลื่อมล้ำ";

    // '-' in symbols_to_index: "ความ", "-", "เหลื่อม", "ล้ำ"
    // '-' in separators: "ความ", "เหลื่อม", "ล้ำ"
    // 'none: "ความ", "เหลื่อม", "ล้ำ"

    std::vector<std::string> tokens;
    Tokenizer(str1, true, false, "th", {'-'}, {}).tokenize(tokens);
    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ("ความ", tokens[0]);
    ASSERT_EQ("-", tokens[1]);
    ASSERT_EQ("เหลื่อม", tokens[2]);
    ASSERT_EQ("ล้ํา", tokens[3]);

    tokens.clear();
    Tokenizer(str1, true, false, "th", {}, {'-'}).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("ความ", tokens[0]);
    ASSERT_EQ("เหลื่อม", tokens[1]);
    ASSERT_EQ("ล้ํา", tokens[2]);

    tokens.clear();
    Tokenizer(str1, true, false, "th", {}, {}).tokenize(tokens);
    ASSERT_EQ(3, tokens.size());
    ASSERT_EQ("ความ", tokens[0]);
    ASSERT_EQ("เหลื่อม", tokens[1]);
    ASSERT_EQ("ล้ํา", tokens[2]);

    tokens.clear();
    Tokenizer("ความ_เห", true, false, "th", {}, {}).tokenize(tokens);
    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ("ความเห", tokens[0]);
}
