#pragma once

#include <stdint.h>
#include <vector>
#include <queue>
#include <stdlib.h>
#include <limits>

#ifdef DEBUG
#define D(x) x
#else
#define D(x)
#endif

#define TokenOffsetHeap std::priority_queue<TokenOffset, std::vector<TokenOffset>, TokenOffset>

struct MatchScore {
  struct TokenOffset {
    uint8_t token_id;         // token identifier
    uint16_t offset;          // token's offset in the text
    uint16_t offset_index;    // index of the offset in the vector

    bool operator() (const TokenOffset& a, const TokenOffset& b) {
        return a.offset > b.offset;
    }
  };

  static void print_token_offsets(std::vector<std::vector<uint16_t>> &token_offsets) {
    for(auto offsets: token_offsets) {
      for(auto offset: offsets) {
        std::cout << offset  << ", ";
      }
      std::cout << std::endl;
    }
  }

  static inline void addTopOfHeapToWindow(TokenOffsetHeap &heap, std::queue<TokenOffset> &window,
                                           std::vector<std::vector<uint16_t>> &token_offsets, uint16_t *token_offset) {
    TokenOffset top = heap.top();
    heap.pop();
    window.push(top);
    token_offset[top.token_id] = top.offset;
    top.offset_index++;

    // Must refill the heap - push the next offset of the same token
    if(top.offset_index < token_offsets[top.token_id].size()) {
        heap.push(TokenOffset{top.token_id, token_offsets[top.token_id][top.offset_index], top.offset_index});
    }
  }

  uint16_t words_present;
  uint16_t distance;

  /*
  *  Given *sorted offsets* of each target token in a *single* document, generates a score that indicates:
  *  a) How many tokens are present in the document
  *  b) The proximity between the tokens in the document
  *
  *  We use a priority queue to read the offset vectors in a sorted manner, slide a window of a given size, and
  *  compute the max_match and min_displacement of target tokens across the windows.
  */
  static MatchScore match_score(uint32_t doc_id, std::vector<std::vector<uint16_t>> &token_offsets) {
    const size_t WINDOW_SIZE = Collection::MAX_SEARCH_TOKENS;
    const uint16_t MAX_DISPLACEMENT = Collection::MAX_SEARCH_TOKENS;

    std::priority_queue<TokenOffset, std::vector<TokenOffset>, TokenOffset> heap;

    for(uint8_t token_id=0; token_id < token_offsets.size(); token_id++) {
      heap.push(TokenOffset{token_id, token_offsets[token_id].front(), 0});
    }

    // heap now contains the first occurring offset of each token in the given document

    uint16_t max_match = 1;
    uint16_t min_displacement = MAX_DISPLACEMENT;

    std::queue<TokenOffset> window;
    uint16_t token_offset[Collection::MAX_SEARCH_TOKENS] = { };
    std::fill_n(token_offset, Collection::MAX_SEARCH_TOKENS, MAX_DISPLACEMENT);

    do {
      if(window.empty()) {
        addTopOfHeapToWindow(heap, window, token_offsets, token_offset);
      }

      D(std::cout << "Loop till window fills... doc_id: " << doc_id << std::endl;)

      // Fill the queue with tokens within a given window frame size of the start offset
      // At the same time, we also record the *last* occurrence of each token within the window
      // For e.g. if `cat` appeared at offsets 1,3 and 5, we will record `token_offset[cat] = 5`
      const uint16_t start_offset = window.front().offset;
      while(!heap.empty() && heap.top().offset < start_offset+WINDOW_SIZE) {
        addTopOfHeapToWindow(heap, window, token_offsets, token_offset);
      }

      D(std::cout << std::endl << "----" << std::endl);

      uint16_t prev_pos = MAX_DISPLACEMENT;
      uint16_t num_match = 0;
      uint16_t displacement = 0;

      for(size_t token_id=0; token_id<token_offsets.size(); token_id++) {
        // If a token appeared within the window, we would have recorded its offset
        if(token_offset[token_id] != MAX_DISPLACEMENT) {
          num_match++;
          if(prev_pos == MAX_DISPLACEMENT) prev_pos = token_offset[token_id];
          else {
            // Calculate the distance between the tokens within the window
            // Ideally, this should be (NUM_TOKENS - 1) when all the tokens are adjacent to each other
            D(std::cout << "prev_pos: " << prev_pos << " , curr_pos: " << token_offset[token_id] << std::endl);
            displacement += abs(token_offset[token_id]-prev_pos);
            prev_pos = token_offset[token_id];
          }
        }
      }

      D(std::cout << std::endl << "!!!displacement: " << displacement << " | num_match: " << num_match << std::endl);

      // Track the best `displacement` and `num_match` seen so far across all the windows
      if(num_match >= max_match) {
        max_match = num_match;
        if(displacement != 0 && displacement < min_displacement) {
          min_displacement = displacement;
        }
      }

      // As we slide the window, drop the first token of the window from the computation
      token_offset[window.front().token_id] = 0;
      window.pop();
    } while(!heap.empty());

    return MatchScore{max_match, min_displacement};
  }
};
