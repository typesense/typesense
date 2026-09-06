#include <gtest/gtest.h>
#include <string>
#include <vector>
#include "text_embedder_tokenizer.h"

// A small byte level BPE tokenizer in the Hugging Face tokenizer.json format.
// Its vocabulary is the 256 byte symbols plus these merges, in rank order:
//
//   0 (s,e)->se 256   1 (se,a)->sea 257   2 (Ġ,s)->Ġs 258   3 (Ġs,ea)->Ġsea 259
//   4 (a,r)->ar 260   5 (ar,c)->arc 261   6 (arc,h)->arch 262
//   7 (b,c)->bc 263   8 (a,b)->ab 264     9 (ab,c)->abc 265
//
// <|endoftext|> is id 266. Ġ is the byte symbol for a space.
static const std::string tiny_qwen_path = std::string(ROOT_DIR) + "test/resources/tiny_qwen_tokenizer.json";

static constexpr int64_t EOS_TOKEN_ID = 266;
static constexpr size_t MAX_LENGTH = 2048;

TEST(QwenTokenizerTest, AppliesMergesAndAppendsEos) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    // "search" merges to [sea][r][c][h], since (s,e) then (se,a) outrank (a,r).
    const std::vector<int64_t> expected{257, 114, 99, 104, EOS_TOKEN_ID};

    ASSERT_EQ(expected, tokenizer.Encode("search").input_ids);
}

TEST(QwenTokenizerTest, MergesLowestRankPairFirst) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    // " search" byte encodes to [Ġ][s][e][a][r][c][h], which offers both (Ġ,s)
    // at rank 2 and (s,e) at rank 0. Taking the leftmost available merge instead
    // of the lowest ranked one would give [Ġs][e][arch] rather than this.
    const std::vector<int64_t> expected{32, 257, 114, 99, 104, EOS_TOKEN_ID};

    ASSERT_EQ(expected, tokenizer.Encode(" search").input_ids);
}

TEST(QwenTokenizerTest, DoesNotConflateMergesThatShareConcatenatedHalves) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    // (b,c) outranks (a,b), so "abc" becomes [a][bc] and stops: (a,bc) is not a
    // merge. Only (ab,c) is. Keying merges on the halves joined together would
    // make those two indistinguishable and collapse this to [abc].
    const std::vector<int64_t> expected{97, 263, EOS_TOKEN_ID};

    ASSERT_EQ(expected, tokenizer.Encode("abc").input_ids);
}

TEST(QwenTokenizerTest, PreservesCase) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    // "Search" cannot start from (s,e), so it merges right to left into
    // [S][e][arch] instead.
    const std::vector<int64_t> expected{83, 101, 262, EOS_TOKEN_ID};

    ASSERT_EQ(expected, tokenizer.Encode("Search").input_ids);
    ASSERT_NE(tokenizer.Encode("Search").input_ids, tokenizer.Encode("search").input_ids);
}

TEST(QwenTokenizerTest, EncodesNonAsciiAsBytesWithoutUnknownToken) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    // é is two UTF-8 bytes, and byte level BPE has a symbol for every byte, so
    // it never falls back to an unknown token.
    const std::vector<int64_t> expected{195, 169, EOS_TOKEN_ID};

    ASSERT_EQ(expected, tokenizer.Encode("é").input_ids);
}

TEST(QwenTokenizerTest, NormalizesToNfcBeforeEncoding) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    // "e" followed by a combining acute accent composes to "é" under NFC, so
    // both spellings have to produce the same ids.
    const std::vector<int64_t> expected{195, 169, EOS_TOKEN_ID};

    ASSERT_EQ(expected, tokenizer.Encode("e\u0301").input_ids);
    ASSERT_EQ(tokenizer.Encode("é").input_ids, tokenizer.Encode("e\u0301").input_ids);
}

TEST(QwenTokenizerTest, EncodesEmptyInputAsEosOnly) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    const auto encoded = tokenizer.Encode("");

    ASSERT_EQ((std::vector<int64_t>{EOS_TOKEN_ID}), encoded.input_ids);
    ASSERT_EQ((std::vector<int64_t>{1}), encoded.attention_mask);
}

TEST(QwenTokenizerTest, TruncatesLongInputKeepingEosLast) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    // No merge covers "z", so this is one token per character.
    const std::string text(MAX_LENGTH + 2000, 'z');

    const auto encoded = tokenizer.Encode(text);

    ASSERT_EQ(MAX_LENGTH, encoded.input_ids.size());
    ASSERT_EQ(MAX_LENGTH, encoded.attention_mask.size());
    ASSERT_EQ(122, encoded.input_ids.front());
    ASSERT_EQ(EOS_TOKEN_ID, encoded.input_ids.back());
}

TEST(QwenTokenizerTest, MarksEveryTokenInTheAttentionMask) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    const auto encoded = tokenizer.Encode("search the archive");

    ASSERT_EQ(encoded.input_ids.size(), encoded.attention_mask.size());
    ASSERT_EQ(std::vector<int64_t>(encoded.input_ids.size(), 1), encoded.attention_mask);
    ASSERT_TRUE(encoded.token_type_ids.empty());
}

TEST(QwenTokenizerTest, ReportsQwenTokenizerType) {
    QwenTokenizer tokenizer(tiny_qwen_path);

    ASSERT_EQ(TokenizerType::qwen, tokenizer.get_tokenizer_type());
}
