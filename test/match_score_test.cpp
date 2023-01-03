#include <chrono>
#include <gtest/gtest.h>
#include <match_score.h>
#include "posting_list.h"
#include <fstream>

#define token_offsets_file_path (std::string(ROOT_DIR) + std::string("external/token_offsets/file/token_offsets.txt")).c_str()

TEST(MatchTest, TokenOffsetsExceedWindowSize) {
    std::vector<token_positions_t> token_positions = {
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})},
        token_positions_t{false, std::vector<uint16_t>({1})}
    };

    const Match & this_match = Match(100, token_positions);

    ASSERT_EQ(WINDOW_SIZE, (size_t)this_match.words_present);
}

TEST(MatchTest, MatchScoreV2) {
    std::vector<token_positions_t> token_offsets;
    token_offsets.push_back(token_positions_t{false, {25}});
    token_offsets.push_back(token_positions_t{false, {26}});
    token_offsets.push_back(token_positions_t{false, {11, 18, 24, 60}});
    token_offsets.push_back(token_positions_t{false, {14, 27, 63}});

    auto match = Match(100, token_offsets, true);
    ASSERT_EQ(4, match.words_present);
    ASSERT_EQ(3, match.distance);
    ASSERT_FALSE(posting_list_t::has_phrase_match(token_offsets));

    std::vector<uint16_t> expected_offsets = {25, 26, 24, 27};
    for(size_t i=0; i<token_offsets.size(); i++) {
        ASSERT_EQ(expected_offsets[i], match.offsets[i].offset);
    }

    // without populate window
    match = Match(100, token_offsets, false);
    ASSERT_EQ(4, match.words_present);
    ASSERT_EQ(3, match.distance);
    ASSERT_EQ(0, match.offsets.size());

    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {38, 50, 170, 187, 195, 222}});
    token_offsets.push_back(token_positions_t{true, {39, 140, 171, 189, 223}});
    token_offsets.push_back(token_positions_t{false, {169, 180}});

    match = Match(100, token_offsets, true, true);
    ASSERT_EQ(3, match.words_present);
    ASSERT_EQ(2, match.distance);
    ASSERT_EQ(0, match.exact_match);
    ASSERT_FALSE(posting_list_t::has_phrase_match(token_offsets));

    expected_offsets = {170, 171, 169};
    for(size_t i=0; i<token_offsets.size(); i++) {
        ASSERT_EQ(expected_offsets[i], match.offsets[i].offset);
    }

    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {38, 50, 187, 195, 201}});
    token_offsets.push_back(token_positions_t{false, {120, 167, 171, 223}});
    token_offsets.push_back(token_positions_t{true, {240, 250}});

    match = Match(100, token_offsets, true);
    ASSERT_EQ(1, match.words_present);
    ASSERT_EQ(0, match.distance);
    ASSERT_EQ(0, match.exact_match);
    ASSERT_FALSE(posting_list_t::has_phrase_match(token_offsets));

    expected_offsets = {38, MAX_DISPLACEMENT, MAX_DISPLACEMENT};
    for(size_t i=0; i<token_offsets.size(); i++) {
        ASSERT_EQ(expected_offsets[i], match.offsets[i].offset);
    }

    // without populate window
    match = Match(100, token_offsets, false);
    ASSERT_EQ(1, match.words_present);
    ASSERT_EQ(0, match.distance);
    ASSERT_EQ(0, match.offsets.size());
    ASSERT_EQ(0, match.exact_match);

    // exact match
    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {0}});
    token_offsets.push_back(token_positions_t{true, {2}});
    token_offsets.push_back(token_positions_t{false, {1}});

    match = Match(100, token_offsets, true, true);
    ASSERT_EQ(3, match.words_present);
    ASSERT_EQ(2, match.distance);
    ASSERT_EQ(1, match.exact_match);
    ASSERT_FALSE(posting_list_t::has_phrase_match(token_offsets));

    match = Match(100, token_offsets, true, false);
    ASSERT_EQ(3, match.words_present);
    ASSERT_EQ(2, match.distance);
    ASSERT_EQ(0, match.exact_match);
    ASSERT_FALSE(posting_list_t::has_phrase_match(token_offsets));

    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {1}});
    token_offsets.push_back(token_positions_t{false, {2}});
    token_offsets.push_back(token_positions_t{true, {3}});

    match = Match(100, token_offsets, true, true);
    ASSERT_EQ(0, match.exact_match);
    ASSERT_TRUE(posting_list_t::has_phrase_match(token_offsets));

    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {0}});
    token_offsets.push_back(token_positions_t{false, {1}});
    token_offsets.push_back(token_positions_t{false, {2}});

    match = Match(100, token_offsets, true, true);
    ASSERT_EQ(0, match.exact_match);
    ASSERT_TRUE(posting_list_t::has_phrase_match(token_offsets));

    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {74}});
    token_offsets.push_back(token_positions_t{false, {75}});
    token_offsets.push_back(token_positions_t{false, {3, 42}});

    expected_offsets = {74, 75, MAX_DISPLACEMENT};
    match = Match(100, token_offsets, true, true);
    ASSERT_EQ(3, match.offsets.size());
    for(size_t i = 0; i < match.offsets.size(); i++) {
        ASSERT_EQ(expected_offsets[i], match.offsets[i].offset);
    }

    // check phrase match
    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {38, 50, 187, 195, 201}});
    token_offsets.push_back(token_positions_t{false, {120, 167, 171, 196}});
    token_offsets.push_back(token_positions_t{true, {197, 250}});

    match = Match(100, token_offsets);
    ASSERT_TRUE(posting_list_t::has_phrase_match(token_offsets));

    token_offsets.clear();
    token_offsets.push_back(token_positions_t{false, {120, 167, 171, 196}});
    token_offsets.push_back(token_positions_t{false, {38, 50, 187, 195, 201}});
    token_offsets.push_back(token_positions_t{true, {197, 250}});

    match = Match(100, token_offsets);
    ASSERT_FALSE(posting_list_t::has_phrase_match(token_offsets));

    /*size_t total_distance = 0, words_present = 0, offset_sum = 0;
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
    LOG(INFO) << total_distance << ", " << words_present << ", " << offset_sum;*/
}

TEST(MatchTest, MatchScoreWithOffsetWrapAround) {
    std::vector<token_positions_t> token_offsets;

    std::ifstream infile(token_offsets_file_path);
    std::string line;

    while (std::getline(infile, line)) {
        if(line == "last_token:") {
            std::vector<uint16_t> positions;
            token_offsets.push_back(token_positions_t{false, positions});
        } else {
            token_offsets.back().positions.push_back(std::stoi(line));
        }
    }

    infile.close();

    ASSERT_FALSE(posting_list_t::has_phrase_match(token_offsets));

    auto match = Match(100, token_offsets, true, true);
    ASSERT_EQ(2, match.words_present);
    ASSERT_EQ(2, match.distance);

    ASSERT_EQ(2, match.offsets.size());
    ASSERT_EQ(4062, match.offsets[0].offset);
    ASSERT_EQ(4060, match.offsets[1].offset);
}
