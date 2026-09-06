#include <gtest/gtest.h>
#include <sentencepiece_processor.h>
#include <string>
#include <vector>
#include "text_embedder_tokenizer.h"

// A small SentencePiece model trained with Gemma's special token layout:
// pad=0, eos=1, bos=2, unk=3.
static const std::string tiny_spm_path = std::string(ROOT_DIR) + "test/resources/tiny_sentencepiece.model";

static constexpr int64_t BOS_TOKEN_ID = 2;
static constexpr int64_t EOS_TOKEN_ID = 1;
static constexpr size_t MAX_LENGTH = 2048;

TEST(GemmaTokenizerTest, WrapsRawSentencePieceIdsInBosAndEos) {
    GemmaTokenizer tokenizer(tiny_spm_path);

    const std::string text = "search engine";

    sentencepiece::SentencePieceProcessor spm;
    ASSERT_TRUE(spm.Load(tiny_spm_path).ok());
    std::vector<int> raw_ids;
    ASSERT_TRUE(spm.Encode(text, &raw_ids).ok());

    std::vector<int64_t> expected;
    expected.push_back(BOS_TOKEN_ID);
    expected.insert(expected.end(), raw_ids.begin(), raw_ids.end());
    expected.push_back(EOS_TOKEN_ID);

    const auto encoded = tokenizer.Encode(text);

    // The ids are used as-is. The xlm_roberta tokenizer adds a fairseq offset of
    // 1 to every id, which would shift all of these by one.
    ASSERT_EQ(expected, encoded.input_ids);
    ASSERT_EQ(std::vector<int64_t>(expected.size(), 1), encoded.attention_mask);
    ASSERT_TRUE(encoded.token_type_ids.empty());
}

TEST(GemmaTokenizerTest, PreservesCase) {
    GemmaTokenizer tokenizer(tiny_spm_path);

    // The siglip tokenizer lowercases its input, which would make these equal.
    ASSERT_NE(tokenizer.Encode("Apple").input_ids, tokenizer.Encode("apple").input_ids);
}

TEST(GemmaTokenizerTest, EncodesEmptyInputAsBosEos) {
    GemmaTokenizer tokenizer(tiny_spm_path);

    const auto encoded = tokenizer.Encode("");

    ASSERT_EQ((std::vector<int64_t>{BOS_TOKEN_ID, EOS_TOKEN_ID}), encoded.input_ids);
    ASSERT_EQ((std::vector<int64_t>{1, 1}), encoded.attention_mask);
}

TEST(GemmaTokenizerTest, TruncatesLongInputKeepingEosLast) {
    GemmaTokenizer tokenizer(tiny_spm_path);

    std::string text;
    for(size_t i = 0; i < 700; i++) {
        text += "search engine ";
    }

    const auto encoded = tokenizer.Encode(text);

    ASSERT_EQ(MAX_LENGTH, encoded.input_ids.size());
    ASSERT_EQ(MAX_LENGTH, encoded.attention_mask.size());
    ASSERT_EQ(BOS_TOKEN_ID, encoded.input_ids.front());
    ASSERT_EQ(EOS_TOKEN_ID, encoded.input_ids.back());
}

TEST(GemmaTokenizerTest, ReportsGemmaTokenizerType) {
    GemmaTokenizer tokenizer(tiny_spm_path);

    ASSERT_EQ(TokenizerType::gemma, tokenizer.get_tokenizer_type());
}
