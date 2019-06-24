#include <gtest/gtest.h>
#include <match_score.h>

TEST(MatchTest, ShouldPackTokenOffsets) {
    uint16_t min_token_offset1[3] = {567, 568, 570};
    char offset_diffs[16];

    Match::pack_token_offsets(min_token_offset1, 3, 567, offset_diffs);

    ASSERT_EQ(3, offset_diffs[0]);
    ASSERT_EQ(0, offset_diffs[1]);
    ASSERT_EQ(1, offset_diffs[2]);
    ASSERT_EQ(3, offset_diffs[3]);

    uint16_t min_token_offset2[3] = {0, 1, 2};
    Match::pack_token_offsets(min_token_offset2, 3, 0, offset_diffs);

    ASSERT_EQ(3, offset_diffs[0]);
    ASSERT_EQ(0, offset_diffs[1]);
    ASSERT_EQ(1, offset_diffs[2]);
    ASSERT_EQ(2, offset_diffs[3]);

    uint16_t min_token_offset3[1] = {123};
    Match::pack_token_offsets(min_token_offset3, 1, 123, offset_diffs);

    ASSERT_EQ(1, offset_diffs[0]);
    ASSERT_EQ(0, offset_diffs[1]);

    // a token might not have an offset because it might not be in the best matching window
    uint16_t min_token_offset4[3] = {0, MAX_DISPLACEMENT, 2};
    Match::pack_token_offsets(min_token_offset4, 3, 0, offset_diffs);

    ASSERT_EQ(3, offset_diffs[0]);
    ASSERT_EQ(0, offset_diffs[1]);
    ASSERT_EQ(std::numeric_limits<int8_t>::max(), offset_diffs[2]);
    ASSERT_EQ(2, offset_diffs[3]);

    uint16_t min_token_offset5[3] = {MAX_DISPLACEMENT, 2, 4};
    Match::pack_token_offsets(min_token_offset5, 3, 2, offset_diffs);

    ASSERT_EQ(3, offset_diffs[0]);
    ASSERT_EQ(std::numeric_limits<int8_t>::max(), offset_diffs[1]);
    ASSERT_EQ(0, offset_diffs[2]);
    ASSERT_EQ(2, offset_diffs[3]);
}

TEST(MatchTest, TokenOffsetsExceedWindowSize) {
    std::vector<std::vector<uint16_t>> token_positions = {
        std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}),
        std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}),
        std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1})
    };

    const Match & this_match = Match::match(100, token_positions);

    ASSERT_EQ(WINDOW_SIZE, this_match.words_present);
}