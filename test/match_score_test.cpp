#include <gtest/gtest.h>
#include <match_score.h>

TEST(MatchScoreTest, ShouldPackTokenOffsets) {
    uint16_t min_token_offset1[3] = {567, 568, 570};
    char offset_diffs[16];

    MatchScore::pack_token_offsets(min_token_offset1, 3, 0, offset_diffs);

    ASSERT_EQ(3, offset_diffs[0]);
    ASSERT_EQ(0, offset_diffs[1]);
    ASSERT_EQ(1, offset_diffs[2]);
    ASSERT_EQ(3, offset_diffs[3]);

    uint16_t min_token_offset2[3] = {0, 1, 2};
    MatchScore::pack_token_offsets(min_token_offset2, 3, 0, offset_diffs);

    ASSERT_EQ(3, offset_diffs[0]);
    ASSERT_EQ(0, offset_diffs[1]);
    ASSERT_EQ(1, offset_diffs[2]);
    ASSERT_EQ(2, offset_diffs[3]);

    uint16_t min_token_offset3[1] = {123};
    MatchScore::pack_token_offsets(min_token_offset3, 1, 0, offset_diffs);

    ASSERT_EQ(1, offset_diffs[0]);
    ASSERT_EQ(0, offset_diffs[1]);
}