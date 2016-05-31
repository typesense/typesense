#pragma once

#include <stdint.h>
#include <vector>
#include <queue>
#include <stdlib.h>

#ifdef DEBUG
#define D(x) x
#else
#define D(x)
#endif

struct TokenPosition {
  uint8_t token_id;         // token identifier
  uint16_t position;        // token's position in the text
  uint16_t position_index;  // index of the position in the vector

  bool operator() (const TokenPosition& a, const TokenPosition& b) {
    return a.position > b.position;
  }
};

struct MatchScore {
  uint16_t words_present;
  uint16_t distance;
};

#define addTopOfHeapToWindow(heap,q,token_positions,token_pos) {\
    TokenPosition top = heap.top();\
    heap.pop();\
    q.push(top);\
    token_pos[top.token_id] = top.position; \
    top.position_index++;\
    /* Must refill the heap - push the next position of the same token */\
    if(top.position_index < token_positions[top.token_id].size()) {\
        heap.push(TokenPosition{top.token_id, token_positions[top.token_id][top.position_index], top.position_index});\
    }\
}
/*
 *  Given *sorted positions* of each target token in a *single* document, generates a score that indicates:
 *  a) How many tokens are present in the document
 *  b) The proximity between the tokens in the document
 *
 *  We use a priority queue to read the position vectors in a sorted manner, slide a window of a given size, and
 *  compute the max_match and min_displacement of target tokens across the windows.
 */
MatchScore match_score(std::vector<std::vector<uint16_t>> &token_positions) {
  const size_t WINDOW_SIZE = 5;
  const size_t MAX_TOKENS_IN_A_QUERY = 20;
  
  std::priority_queue<TokenPosition, std::vector<TokenPosition>, TokenPosition> heap;

  for(uint8_t token_id=0; token_id < token_positions.size(); token_id++) {
    heap.push(TokenPosition{token_id, token_positions[token_id].front(), 0});
  }

  // heap now contains the first occurring position of each token in the given document

  uint16_t max_match = 1;
  uint16_t min_displacement = UINT16_MAX;

  std::queue<TokenPosition> q;
  uint16_t token_pos[MAX_TOKENS_IN_A_QUERY] = { };

  do {
    if(q.empty()) {
      addTopOfHeapToWindow(heap, q, token_positions, token_pos);
    }

    D(cout << "Loop till window fills..." << endl;)

    // Fill the queue with tokens within a given window frame size of the start position
    // At the same time, we also record the *last* occurrence of each token within the window
    // For e.g. if `cat` appeared at positions 1,3 and 5, we will record `word_pos_sum[cat] = 5`
    const uint16_t start_pos = q.front().position;
    while(!heap.empty() && heap.top().position < start_pos+WINDOW_SIZE) {
      addTopOfHeapToWindow(heap, q, token_positions, token_pos);
    }

    D(cout << endl << "----" << endl);

    uint16_t prev_pos = 0;
    uint16_t num_match = 0;
    uint16_t displacement = 0;

    for(size_t token_id=0; token_id<token_positions.size(); token_id++) {
      // If a token appeared within the window, we would have recorded its position
      if(token_pos[token_id] != 0) {
        num_match++;
        if(prev_pos == 0) prev_pos = token_pos[token_id];
        else {
          // Calculate the distance between the tokens within the window
          // Ideally, this should be (NUM_TOKENS - 1) when all the tokens are adjacent to each other
          D(cout << "prev_pos: " << prev_pos << " , curr_pos: " << token_pos[token_id] << endl);
          displacement += abs(token_pos[token_id]-prev_pos);
          prev_pos = token_pos[token_id];
        }
      }
    }

    D(cout << endl << "!!!displacement: " << displacement << " | num_match: " << num_match << endl);

    // Track the best `displacement` and `num_match` seen so far across all the windows
    if(num_match >= max_match) {
      max_match = num_match;
      if(displacement != 0 && displacement < min_displacement) {
        min_displacement = displacement;
      }
    }

    // As we slide the window, drop the first token of the window from the computation
    token_pos[q.front().token_id] = 0;
    q.pop();
  } while(!heap.empty());

  return MatchScore{max_match, min_displacement};
}
