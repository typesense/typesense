#include <gtest/gtest.h>
#include <match_score.h>

TEST(MatchTest, TokenOffsetsExceedWindowSize) {
    std::vector<std::vector<uint16_t>> token_positions = {
        std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}),
        std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}),
        std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1}), std::vector<uint16_t>({1})
    };

    const Match & this_match = Match(100, token_positions);

    ASSERT_EQ(WINDOW_SIZE, (size_t)this_match.words_present);
}

TEST(MatchTest, MatchScoreV2) {
    std::vector<std::vector<uint16_t>> token_offsets;
    token_offsets.push_back({38, 50, 170, 187, 195, 222});
    token_offsets.push_back({39, 140, 171, 189, 223});
    token_offsets.push_back({169, 180});

//    token_offsets.push_back({38, 50, 187, 195, 201});
//    token_offsets.push_back({120, 167, 171, 223});  // 39,
//    token_offsets.push_back({240, 250});

    size_t total_distance = 0, words_present = 0, offset_sum = 0;
    auto begin = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < 1; i++) {
        auto match = Match(100, token_offsets, true);
        total_distance += match.distance;
        words_present += match.words_present;
        offset_sum += match.offsets.size();
    }

    uint64_t timeNanos = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - begin).count();
    LOG(INFO) << "Time taken: " << timeNanos;

    LOG(INFO) << total_distance << ", " << words_present << ", " << offset_sum;

}