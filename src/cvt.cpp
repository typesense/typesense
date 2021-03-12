#include <cvt.h>
#include <cstring>
#include "logger.h"

bool CVTrie::add(const char *key, const uint8_t length, void *value) {
    // If the key exists, augment the node, otherwise insert a new node

    if(root == nullptr) {
        // Trie is empty, so add a single leaf node:
        // [0|PTRLEAF][3][f][o][o]

        uint8_t* block = new uint8_t[8 + 1 + length];

        void* node = tag_ptr(value, 0, LEAF);

        std::memcpy(block, &node, sizeof(void*));
        std::memcpy(block+sizeof(void*), &length, 1);
        std::memcpy(block+sizeof(void*)+1, key, length);

        root = block;
        return true;
    }

    uint8_t node_type = get_node_type(root);

    if(node_type == LEAF) {
        // Compare new key with child key to identify common prefix
        // e.g. welcome vs welding (or) we vs welcome (or) welcome vs foobar
    }

    /*size_t num_siblings = 1;
    size_t key_index = 0;

    while(true) {
        // for each sibling
        for(auto sindex = 0; sindex < num_siblings; sindex++) {
            unsigned char c = key[key_index];


        }
    }*/

    return true;
}

void *CVTrie::find(const char *key, const uint8_t length) {
    size_t key_index = 0;
    void* curr = nullptr;
    std::memcpy(&curr, root, sizeof curr);

    while(true) {
        uint8_t node_type = get_node_type(curr);
        if(node_type == LEAF) {
            size_t key_len = *(uint8_t*)(root+sizeof(void*));

            if(key_index+key_len != length) {
                return nullptr;
            }

            for(size_t i=0; i<key_len; i++) {
                uint8_t this_char = *(uint8_t *) (root + sizeof(void *) + 1 + i);
                if(key[key_index++] != this_char) {
                    return nullptr;
                }
            }

            void* leaf = get_ptr(curr);
            return leaf;
        }

        break;
    }

    return nullptr;
}