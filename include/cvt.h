#pragma once

/* Compact Variable Trie
================================================================================================================
  ates, at, as, but, tok, too

        [   *    ]
       ⁄   |     ＼
     a     b      t-o
   ／ ＼    \     ／＼
   s   t    utØ  k  o
  /   /  ＼      /   ＼
 Ø   esØ  Ø     Ø     Ø


  BASIC DESIGN
  ============

  * All nodes in the tree level in the same block.
  * Pointer to ONLY FIRST child node of each sibling.
  * Each sibling node's children represented by their character/byte
  * For root there are no siblings, so pointer only to `a` child.
  * Each node can be a single-char prefix, multi-char prefix or leaf.

  ROOT -> [0|PTRA][3][a][b][t]
  PTRA -> [0|PTRS] [2|PTRU] [4|PTRK] [2][s][t] [2][u][t] [2][k][o]
  PTRS -> [0|PTRØ][1|PTRE][1][Ø][2][e][Ø]
  PTRT -> [0|L_PTRE][3|PTRØ][3][e][s][Ø][1][Ø]  (path compression)
  PTRØ -> [0|LEAF]

  [OFFSET][PTR][TYPE]..[NUM_CHILDREN][A][B]..[X]
  [  16  ][45][  3  ]  (64 bits)

  2 bytes for type+offset
  6 bytes for address
  1 byte for num_children
  x bytes for bytes

  Actual offset to the Nth node's children: (8*NUM_CHILDREN) + N + offset

  if num_children >= 32:
    Use bitset to represent children present
    Read 32 bytes and do bitset operations to extract matched index
  else:
    Use array to represent children
    Read `num_children` bytes and do sequential search

  Multi-char node (COMPRESSED node) will be packed as:
  [num_prefix][prefixes][num_children][children]

  Removal of [be]

  1. Realloc contents of PTR1 by removing "e" from the nodes list
  2. Free PTR3
  3. Realloc contents of ROOT by removing "b" from the nodes list

*/

#include <cstdint>
#include <cstddef>
#include "logger.h"

struct cvt_leaf_t {
    size_t value;
};

enum CVT_NODE {
    INTERNAL = 0,
    LEAF = 1,
    COMPRESSED = 2,
};

class CVTrie {
private:
    size_t size;
    uint8_t* root;

    const uintptr_t PTR_MASK = ~(1ULL << 48ULL);

public:

    CVTrie(): root(nullptr) {

    }

    inline void* get_ptr(const void* tagged_ptr) {
        // Right shift of signed integer for sign extension is implementation-defined but works on major compilers
        return (void*)( ((intptr_t)((uintptr_t)tagged_ptr << 16ULL) >> 16ULL) & ~3 );
    }

    inline void* tag_ptr(const void* ptr, const uint16_t offset, const CVT_NODE node_type) {
        return (void*)(((uintptr_t)ptr & PTR_MASK) | (uint64_t(offset) << 48ULL) | uint64_t(node_type));
    }

    inline uint8_t get_node_type(const void* tagged_ptr) {
        return (uintptr_t)(tagged_ptr) & 3;
    }

    inline uint16_t get_offset(const void* ptr) {
        return (uintptr_t)(ptr) >> 48ULL;
    }

    void* find(const char* key, const uint8_t length);

    bool add(const char* key, const uint8_t length, void* value);

};


