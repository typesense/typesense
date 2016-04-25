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

struct WordPosition {
  uint8_t word;        // word identifier - one list for each word
  uint16_t position;   // word's position in the text
  uint16_t index;      // array index of the position in the list

  bool operator() (const WordPosition& a, const WordPosition& b) {
      return a.position > b.position;
  }
};

struct MatchScore {
  uint16_t words_present;
  uint16_t distance;
};

#define addTopOfHeapToWindow(heap,q,word_positions,word_pos_sum) {\
    WordPosition top = heap.top();\
    heap.pop();\
    q.push(top);\
    word_pos_sum[top.word] = top.position; \
    top.index++;\
    /* push the next element from the same queue onto the heap (must check for bounds) */\
    if(top.index < word_positions[top.word].size()) {\
        heap.push(WordPosition{top.word, word_positions[top.word][top.index], top.index});\
    }\
}
/*
 *  Given *sorted positions* of target tokens in a document, generates a score that indicates:
 *  a) How many tokens are present in the document
 *  b) The proximity between the tokens in the document
 *
 *  We use a priority queue to read the position vectors in a sorted manner, slide a window of a given size, and
 *  compute the max_match and min_displacement of target tokens across the windows.
 */
MatchScore match_score(std::vector<std::vector<uint16_t>> &word_positions) {
  std::priority_queue<WordPosition, std::vector<WordPosition>, WordPosition> heap;

  for(uint8_t word=0; word < word_positions.size(); word++) {
    heap.push(WordPosition{word, word_positions[word][0], 0});
  }

  uint16_t max_match = 1;
  uint16_t min_displacement = UINT16_MAX;

  std::queue<WordPosition> q;
  uint16_t word_pos_sum[20] = { };

  do {
    if(q.empty()) {
        addTopOfHeapToWindow(heap, q, word_positions, word_pos_sum);
    }

    D(cout << "Loop till window fills..." << endl;)

    const uint16_t start_pos = q.front().position;
    while(!heap.empty() && heap.top().position < start_pos+5) {
        addTopOfHeapToWindow(heap, q, word_positions, word_pos_sum);
    }

    uint16_t prev_pos = 0;
    uint16_t num_match = 0;
    uint16_t displacement = 0;
    size_t k = 0;

    D(cout << endl << "----" << endl);

    while(k < word_positions.size()) {
        if(word_pos_sum[k] != 0) {
            num_match++;
            if(prev_pos == 0) prev_pos = word_pos_sum[k];
            else {
                D(cout << "prev_pos: " << prev_pos << " , curr_pos: " << word_pos_sum[k] << endl);
                displacement += abs(word_pos_sum[k]-prev_pos);
                prev_pos = word_pos_sum[k];
            }
        }
        k++;
    }

    D(cout << endl << "!!!displacement: " << displacement << " | num_match: " << num_match << endl);

    if(num_match >= max_match) {
        max_match = num_match;
        if(displacement != 0 && displacement < min_displacement) {
            min_displacement = displacement;
        }
    }

    word_pos_sum[q.front().word] -= q.front().position;
    q.pop();
  } while(!heap.empty());

  return MatchScore{max_match, min_displacement};
}
