/* Compact Variable Trie
================================================================================================================
  ates, at, as, bet, to, tk

        [   *    ]
       ⁄   |     ＼
     a     b      t
   ／ ＼    \     ／＼
   s   t    etØ  k  o
  /   /  ＼      /   ＼
 Ø   esØ  Ø     Ø     Ø


  BASIC DESIGN
  ============

  * All siblings in the same block.
  * Pointer to ONLY FIRST child node of each sibling.
  * Each sibling node's children represented by their character/byte
  * For root there are no siblings, so pointer only to `a` child.

  ROOT -> [0|0|PTRA][3][a][b][t]
  PTRA -> [0|PTRS][2|PTRE][4|PTRK[2][s][t][2][e][t][2][k][o]
  PTRS -> [0|PTRØ][1|PTRT][1][Ø][2][e][Ø]
  PTRT -> [0|L_PTRE][3|PTRØ][3][e][s][Ø][1][Ø]  (path compression)
  PTRØ -> [0|LEAF]

  [TYPE+OFFSET][PTR_1]..[NUM_CHILDREN][A][B]..[X]

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

  Removal of [be]

  1. Realloc contents of PTR1 by removing "e" from the nodes list
  2. Free PTR3
  3. Realloc contents of ROOT by removing "b" from the nodes list

*/

#include <cstdint>
#include <cstddef>




class CVTrie {
private:
    size_t size;

    // [TYPE+OFFSET][PTR_1]...     (8 bytes each)
    // [NUM_PREFIX][A][B]..[X]     (upto 33 bytes)
    uint8_t* root;

    const uintptr_t PTR_MASK = ~(1ULL << 48ULL);

public:

    uint8_t* get_ptr(const uint8_t* tagged_ptr) {
        // Unfortunately, right shifting of signed integer for sign extension is implementation-defined
        // but works on all major compilers
        return (uint8_t*)( (intptr_t)((uintptr_t)tagged_ptr << 16ULL) >> 16ULL );
    }

    uint8_t* tag_ptr(const uint8_t* ptr, const uint64_t data) {
        return (uint8_t*)(((uintptr_t)ptr & PTR_MASK) | (data << 48ULL));
    }

    uint16_t get_data(const uint16_t* ptr) {
        return (uintptr_t)(ptr) >> 56ULL;;
    }

    void add(const unsigned char* key, const size_t length, void* value) {
        // If the key exists, augment the node, otherwise insert a new node

        size_t num_siblings = 1;
        uint8_t* buf = root;
        size_t key_index = 0;

        if(root == nullptr) {
            // trie is empty
            root = new uint8_t[8+1+1];

        }

        while(true) {
            // for each sibling
            for(auto sindex = 0; sindex < num_siblings; sindex++) {
                unsigned char c = key[key_index];


            }
        }
    }

};


