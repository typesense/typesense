#pragma once

#include <stdint.h>
#include <vector>
#include <queue>
#include <stdlib.h>
#include <limits>
#include "logger.h"

#ifdef DEBUG
#define D(x) x
#else
#define D(x)
#endif

#define TokenOffsetHeap std::priority_queue<TokenOffset, std::vector<TokenOffset>, TokenOffset>

const size_t WINDOW_SIZE = 10;
const uint16_t MAX_DISPLACEMENT = std::numeric_limits<uint16_t>::max();

struct TokenOffset {
    uint8_t token_id;         // token identifier
    uint16_t offset;          // token's offset in the text
    uint16_t offset_index;    // index of the offset in the offset vector

    bool operator()(const TokenOffset &a, const TokenOffset &b) {
        return a.offset > b.offset;
    }

    bool operator>(const TokenOffset &a) const {
        return offset > a.offset;
    }
};

struct Match {
    uint8_t words_present;
    uint8_t distance;
    std::vector<TokenOffset> offsets;

    Match() : words_present(0), distance(0) {

    }

    Match(uint8_t words_present, uint8_t distance) : words_present(words_present), distance(distance) {

    }

    // Explicit construction of match score
    static inline uint64_t get_match_score(const uint32_t words_present, const uint32_t total_cost, const uint8_t distance) {
        uint64_t match_score = (
            (int64_t(words_present) << 16) |
            (int64_t(255 - total_cost) << 8) |
            (int64_t(100 - distance))
        );

        return match_score;
    }

    // Construct a single match score from individual components (for multi-field sort)
    inline uint64_t get_match_score(const uint32_t total_cost) const {
        uint64_t match_score = (
            (int64_t(words_present) << 16) |
            (int64_t(255 - total_cost) << 8) |
            (int64_t(100 - distance))
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

    Match(uint32_t doc_id, const std::vector<std::vector<uint16_t>> &token_offsets, bool populate_window=true) {
        // in case if number of tokens in query is greater than max window
        const size_t tokens_size = std::min(token_offsets.size(), WINDOW_SIZE);

        std::vector<TokenOffset> window(tokens_size);
        for (size_t token_id = 0; token_id < tokens_size; token_id++) {
            window[token_id] = TokenOffset{static_cast<uint8_t>(token_id), token_offsets[token_id][0], 0};
        }

        std::vector<TokenOffset> best_window;
        if(populate_window) {
            best_window = window;
        }

        size_t best_num_match = 1;
        size_t best_displacement = MAX_DISPLACEMENT;

        while (window.size() > 1) {
            if(window.size() == 3) {
                sort3<TokenOffset>(window);
            } else {
                std::sort(window.begin(), window.end(), std::greater<TokenOffset>());  // descending comparator
            }

            size_t min_offset = window.back().offset;

            size_t this_displacement = 0;
            size_t this_num_match = 0;
            std::vector<TokenOffset> this_window(tokens_size);

            for (size_t i = 0; i < window.size(); i++) {
                if(populate_window) {
                    this_window[window[i].token_id] = window[i];
                }

                if ((window[i].offset - min_offset) <= WINDOW_SIZE) {
                    uint16_t next_offset = (i == window.size() - 1) ? window[i].offset : window[i + 1].offset;
                    this_displacement += window[i].offset - next_offset;
                    this_num_match++;
                } else {
                    // to indicate that this offset should not be considered
                    if(populate_window) {
                        this_window[window[i].token_id].offset = MAX_DISPLACEMENT;
                    }
                }
            }

            if(populate_window) {
                this_window[window.back().token_id] = window.back();
            }

            if ( (this_num_match > best_num_match) ||
                 (this_num_match == best_num_match && this_displacement < best_displacement)) {
                best_displacement = this_displacement;
                best_num_match = this_num_match;
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
            const std::vector<uint16_t> &this_token_offsets = token_offsets[token_id];

            if (smallest_offset.offset == this_token_offsets.back()) {
                // no more offsets for this token
                continue;
            }

            // Push next offset of same token popped
            uint16_t next_offset_index = (smallest_offset.offset_index + 1);
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
    }
};
