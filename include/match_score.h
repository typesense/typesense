#pragma once

#include <cstdint>
#include <vector>
#include <queue>
#include <algorithm>
#include <cstdlib>
#include <limits>
#include "logger.h"

const size_t WINDOW_SIZE = 10;
const uint16_t MAX_DISPLACEMENT = std::numeric_limits<uint16_t>::max();

struct token_positions_t {
    bool last_token = false;
    std::vector<uint16_t> positions;
};

struct TokenOffset {
    uint8_t token_id;                            // token identifier
    uint16_t offset = MAX_DISPLACEMENT;          // token's offset in the text
    uint32_t offset_index;                       // index of the offset in the offset vector

    bool operator()(const TokenOffset &a, const TokenOffset &b) {
        return a.offset > b.offset;
    }

    bool operator>(const TokenOffset &a) const {
        return offset > a.offset;
    }

    bool operator<(const TokenOffset &a) const {
        return offset < a.offset;
    }
};

struct Match {
    uint8_t words_present = 0;
    uint8_t distance = 0;
    uint8_t max_offset = 0;
    uint8_t exact_match = 0;

    std::vector<TokenOffset> offsets;

    Match() : words_present(0), distance(0), max_offset(0), exact_match(0) {

    }

    Match(uint8_t words_present, uint8_t distance, uint8_t max_offset, uint8_t exact_match = 0) :
            words_present(words_present), distance(distance), max_offset(max_offset),
            exact_match(exact_match) {

    }

    // Construct a single match score from individual components (for multi-field sort)
    inline uint64_t get_match_score(const uint32_t total_cost, const uint32_t unique_words) const {
        uint64_t match_score = (
            (int64_t(unique_words) << 40) |
            (int64_t(words_present) << 32) |
            (int64_t(255 - total_cost) << 24) |
            (int64_t(100 - distance) << 16) |
            (int64_t(exact_match) << 8) |
            (int64_t(255 - max_offset) << 0)
        );

        return match_score;
    }

    static void print_token_offsets(std::vector<std::vector<uint16_t>> &token_offsets) {
        for (auto offsets: token_offsets) {
            for (auto offset: offsets) {
                LOG(INFO) << offset << ", ";
            }
            LOG(INFO) << "";
        }
    }

    template<typename T>
    void sort2(std::vector<T>& a) {
        if(a[0] < a[1]) {
            std::swap(a[0], a[1]);
        }
    }

    template<typename T>
    void sort3(std::vector<T>& a) {
        if (a[0] > a[1]) {
            if (a[1] > a[2]) {
                return;
            } else if (a[0] > a[2]) {
                std::swap(a[1], a[2]);
            } else {
                T tmp = std::move(a[0]);
                a[0] = std::move(a[2]);
                a[2] = std::move(a[1]);
                a[1] = std::move(tmp);
            }
        } else {
            if (a[0] > a[2]) {
                std::swap(a[0], a[1]);
            } else if (a[2] > a[1]) {
                std::swap(a[0], a[2]);
            } else {
                T tmp = std::move(a[0]);
                a[0] = std::move(a[1]);
                a[1] = std::move(a[2]);
                a[2] = std::move(tmp);
            }
        }
    }

    /*
        Given *sorted offsets* of each target token in a *single* document (token_offsets), generates a score indicating:
        a) How many tokens are present within a match window
        b) The proximity between the tokens within the match window

        How it works:
        ------------
        Create vector with first offset from each token.
        Sort vector descending.
        Calculate distance, use only tokens within max window size from lowest offset.
        Reassign best window and distance if found.
        Pop end of vector (smallest offset).
        Push to vector next offset of token just popped.
        Until queue size is 1.
    */

    Match(uint32_t doc_id, const std::vector<token_positions_t>& token_offsets,
          bool populate_window=true, bool check_exact_match=false) {
        // in case if number of tokens in query is greater than max window
        const size_t tokens_size = std::min(token_offsets.size(), WINDOW_SIZE);

        std::vector<TokenOffset> window(tokens_size);
        for (size_t token_id = 0; token_id < tokens_size; token_id++) {
            window[token_id] = TokenOffset{static_cast<uint8_t>(token_id), token_offsets[token_id].positions[0], 0};
        }

        std::vector<TokenOffset> best_window;
        if(populate_window) {
            best_window = window;
        }

        size_t best_num_match = 1;
        size_t best_displacement = MAX_DISPLACEMENT;

        int prev_min_offset = -1;

        while (window.size() > 1) {
            switch(window.size()) {
                case 2:
                    sort2<TokenOffset>(window);
                    break;
                case 3:
                    sort3<TokenOffset>(window);
                    break;
                default:
                    // descending comparator
                    std::sort(window.begin(), window.end(), std::greater<TokenOffset>());
            }

            size_t min_offset = window.back().offset;

            if(int(min_offset) < prev_min_offset) {
                // indicates that one of the offsets are wrapping around (e.g. long document)
                break;
            }

            prev_min_offset = min_offset;

            size_t this_displacement = 0;
            size_t this_num_match = 0;
            std::vector<TokenOffset> this_window(tokens_size);

            uint16_t prev_offset = window[0].offset;
            bool all_offsets_are_same = true;

            for (size_t i = 0; i < window.size(); i++) {
                if(populate_window) {
                    this_window[window[i].token_id] = window[i];
                    this_window[window[i].token_id].offset = MAX_DISPLACEMENT;
                }

                if ((window[i].offset - min_offset) <= WINDOW_SIZE) {
                    uint16_t next_offset = (i == window.size() - 1) ? window[i].offset : window[i + 1].offset;
                    this_displacement += window[i].offset - next_offset;
                    this_num_match++;

                    if(populate_window) {
                        this_window[window[i].token_id].offset = window[i].offset;
                    }
                }

                all_offsets_are_same = all_offsets_are_same && (window[i].offset == prev_offset);
            }

            if ( ((this_num_match > best_num_match) ||
                 (this_num_match == best_num_match && this_displacement < best_displacement))) {
                best_displacement = this_displacement;
                best_num_match = this_num_match;
                max_offset = std::min((uint16_t)255, window.front().offset);
                if(populate_window) {
                    best_window = this_window;
                }
            }

            if (best_num_match == tokens_size && best_displacement == (window.size() - 1)) {
                // this is the best we can get, so quit early!
                break;
            }

            // fill window with next possible smallest offset across available token this_token_offsets
            const TokenOffset &smallest_offset = window.back();
            window.pop_back();

            const uint8_t token_id = smallest_offset.token_id;
            const std::vector<uint16_t>& this_token_offsets = token_offsets[token_id].positions;

            if (smallest_offset.offset == this_token_offsets.back()) {
                // no more offsets for this token
                continue;
            }

            // Push next offset of same token popped
            uint32_t next_offset_index = (smallest_offset.offset_index + 1);
            TokenOffset token_offset{token_id, this_token_offsets[next_offset_index], next_offset_index};
            window.emplace_back(token_offset);
        }

        if (best_displacement == MAX_DISPLACEMENT) {
            best_displacement = 0;
        }

        words_present = best_num_match;
        distance = uint8_t(best_displacement);
        if(populate_window) {
            offsets = best_window;
        }

        exact_match = 0;

        if(check_exact_match) {
            if(distance > token_offsets.size()-1) {
                // we can exit early and don't have to care about other requirements
                return;
            }

            // 1) distance < num tokens when there are repeating query tokens
            // 2) distance can be same as num tokens and still not be an exact match

            int last_token_index = -1;
            size_t total_offsets = 0;

            for(const auto& token_positions: token_offsets) {
                if(token_positions.last_token && !token_positions.positions.empty()) {
                    last_token_index = token_positions.positions.back();
                }

                total_offsets += token_positions.positions.size();

                if(total_offsets > token_offsets.size() && distance == token_offsets.size()-1) {
                    // if total offsets exceed query length, there cannot possibly be an exact match
                    return;
                }
            }

            if(last_token_index == int(token_offsets.size())-1) {
                if(total_offsets == token_offsets.size() && distance == token_offsets.size()-1) {
                    exact_match = 1;
                } else if(distance < token_offsets.size()-1) {
                    exact_match = 1;
                }
            }
        }
    }
};
