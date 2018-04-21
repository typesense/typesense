#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <art.h>
#include <functional>
#include <chrono>
#include <algorithm>
#include <iostream>
#include <limits>
#include <queue>
#include <stdint.h>
#include "art.h"
#include "logger.h"

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((void*)((uintptr_t)x & ~1))

#define MIN3(a, b, c) ((a) < (b) ? ((a) < (c) ? (a) : (c)) : ((b) < (c) ? (b) : (c)))

#ifdef IGNORE_PRINTF
#define printf(fmt, ...) (0)
#endif

#define microseconds std::chrono::duration_cast<std::chrono::microseconds>

enum recurse_progress { RECURSE, ABORT, ITERATE };

static void art_fuzzy_recurse(char p, char c, const art_node *n, int depth, const unsigned char *term,
                              const int term_len, const int* irow, const int* jrow, const int min_cost,
                              const int max_cost, const bool prefix, std::vector<const art_node *> &results);

void art_int_fuzzy_recurse(art_node *n, int depth, const unsigned char* int_str, int int_str_len,
                           NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results);

bool compare_art_leaf_frequency(const art_leaf *a, const art_leaf *b) {
    return a->values->ids.getLength() > b->values->ids.getLength();
}

bool compare_art_leaf_score(const art_leaf *a, const art_leaf *b) {
    return a->max_score > b->max_score;
}

bool compare_art_node_frequency(const art_node *a, const art_node *b) {
    uint32_t a_value = 0, b_value = 0;

    if(IS_LEAF(a)) {
        art_leaf* al = (art_leaf *) LEAF_RAW(a);
        a_value = al->values->ids.getLength();
    } else {
        a_value = a->max_token_count;
    }

    if(IS_LEAF(b)) {
        art_leaf* bl = (art_leaf *) LEAF_RAW(b);
        b_value = bl->values->ids.getLength();
    } else {
        b_value = b->max_token_count;
    }

    return a_value > b_value;
}

bool compare_art_node_score(const art_node* a, const art_node* b) {
    int32_t a_value = 0, b_value = 0;

    if(IS_LEAF(a)) {
        art_leaf* al = (art_leaf *) LEAF_RAW(a);
        a_value = al->max_score;
    } else {
        a_value = a->max_score;
    }

    if(IS_LEAF(b)) {
        art_leaf* bl = (art_leaf *) LEAF_RAW(b);
        b_value = bl->max_score;
    } else {
        b_value = b->max_score;
    }

    return a_value > b_value;
}

bool compare_art_node_frequency_pq(const art_node *a, const art_node *b) {
    return !compare_art_node_frequency(a, b);
}

bool compare_art_node_score_pq(const art_node* a, const art_node* b) {
    return !compare_art_node_score(a, b);
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static art_node* alloc_node(uint8_t type) {
    art_node* n;
    switch (type) {
        case NODE4:
            n = (art_node *) calloc(1, sizeof(art_node4));
            break;
        case NODE16:
            n = (art_node *) calloc(1, sizeof(art_node16));
            break;
        case NODE48:
            n = (art_node *) calloc(1, sizeof(art_node48));
            break;
        case NODE256:
            n = (art_node *) calloc(1, sizeof(art_node256));
            break;
        default:
            abort();
    }
    n->type = type;
    n->max_score = 0;
    n->max_token_count = 0;
    return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t) {
    t->root = NULL;
    t->size = 0;
    return 0;
}

// Recursively destroys the tree
static void destroy_node(art_node *n) {
    // Break if null
    if (!n) return;

    // Special case leafs
    if (IS_LEAF(n)) {
        art_leaf *leaf = (art_leaf *) LEAF_RAW(n);
        delete leaf->values;
        free(leaf);
        return;
    }

    // Handle each node type
    int i;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p1->children[i]);
            }
            break;

        case NODE16:
            p.p2 = (art_node16*)n;
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p2->children[i]);
            }
            break;

        case NODE48:
            p.p3 = (art_node48*)n;
            for (i=0;i<48;i++) {
                destroy_node(p.p3->children[i]);
            }
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            for (i=0;i<256;i++) {
                if (p.p4->children[i])
                    destroy_node(p.p4->children[i]);
            }
            break;

        default:
            abort();
    }

    // Free ourself on the way up
    free(n);
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
    destroy_node(t->root);
    return 0;
}

/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t art_size(art_tree *t);

void compare_and_match_leaf(const unsigned char *int_str, int int_str_len, const NUM_COMPARATOR &comparator,
                            std::vector<const art_leaf *> &results, const art_leaf *l);

#endif

static art_node** find_child(art_node *n, unsigned char c) {
    int i, mask, bitfield;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0;i < n->num_children; i++) {
                if (p.p1->keys[i] == c)
                    return &p.p1->children[i];
            }
            break;

            {
                __m128i cmp;
                case NODE16:
                    p.p2 = (art_node16*)n;

                // Compare the key to all 16 stored keys
                cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                                     _mm_loadu_si128((__m128i*)p.p2->keys));

                // Use a mask to ignore children that don't exist
                mask = (1 << n->num_children) - 1;
                bitfield = _mm_movemask_epi8(cmp) & mask;

                /*
                 * If we have a match (any bit set) then we can
                 * return the pointer match using ctz to get
                 * the index.
                 */
                if (bitfield)
                    return &p.p2->children[__builtin_ctz(bitfield)];
                break;
            }

        case NODE48:
            p.p3 = (art_node48*)n;
            i = p.p3->keys[c];
            if (i)
                return &p.p3->children[i-1];
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            if (p.p4->children[c])
                return &p.p4->children[c];
            break;

        default:
            abort();
    }
    return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
    return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const art_leaf *n, const unsigned char *key, int key_len, int depth) {
    (void)depth;
    // Fail if the key lengths are different
    if (n->key_len != (uint32_t)key_len) return 1;

    // Compare the keys starting at the depth
    return memcmp(n->key, key, key_len);
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const unsigned char *key, int key_len) {
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node *) LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((art_leaf*)n, key, key_len, depth)) {
                return ((art_leaf*)n);
            }
            return NULL;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
                return NULL;
            }

            depth = depth + n->partial_len;
            if(depth >= key_len) {
                return NULL;
            }
        }

        assert(depth < key_len);

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return NULL;
}

// Find the minimum leaf under a node
static art_leaf* minimum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return (art_leaf *) LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case NODE4:
            return minimum(((art_node4*)n)->children[0]);
        case NODE16:
            return minimum(((art_node16*)n)->children[0]);
        case NODE48:
            idx=0;
            while (!((art_node48*)n)->keys[idx]) idx++;
            idx = ((art_node48*)n)->keys[idx] - 1;
            return minimum(((art_node48*)n)->children[idx]);
        case NODE256:
            idx=0;
            while (!((art_node256*)n)->children[idx]) idx++;
            return minimum(((art_node256*)n)->children[idx]);
        default:
            abort();
    }
}

// Find the maximum leaf under a node
static art_leaf* maximum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return (art_leaf *) LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case NODE4:
            return maximum(((art_node4*)n)->children[n->num_children-1]);
        case NODE16:
            return maximum(((art_node16*)n)->children[n->num_children-1]);
        case NODE48:
            idx=255;
            while (!((art_node48*)n)->keys[idx]) idx--;
            idx = ((art_node48*)n)->keys[idx] - 1;
            return maximum(((art_node48*)n)->children[idx]);
        case NODE256:
            idx=255;
            while (!((art_node256*)n)->children[idx]) idx--;
            return maximum(((art_node256*)n)->children[idx]);
        default:
            abort();
    }
}

/**
 * Returns the minimum valued leaf
 */
art_leaf* art_minimum(art_tree *t) {
    return minimum((art_node*)t->root);
}

/**
 * Returns the maximum valued leaf
 */
art_leaf* art_maximum(art_tree *t) {
    return maximum((art_node*)t->root);
}

static void add_document_to_leaf(const art_document *document, art_leaf *leaf) {
    leaf->max_score = MAX(leaf->max_score, document->score);
    leaf->values->ids.append(document->id);
    uint32_t curr_index = leaf->values->offsets.getLength();
    leaf->values->offset_index.append(curr_index);

    for(uint32_t i=0; i<document->offsets_len; i++) {
        leaf->values->offsets.append(document->offsets[i]);
    }
}

static art_leaf* make_leaf(const unsigned char *key, uint32_t key_len, art_document *document) {
    art_leaf *l = (art_leaf *) malloc(sizeof(art_leaf) + key_len);
    l->values = new art_values;
    l->max_score = 0;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    add_document_to_leaf(document, l);
    return l;
}

static uint32_t longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
    int max_cmp = min(l1->key_len, l2->key_len) - depth;
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key[depth+idx] != l2->key[depth+idx])
            return idx;
    }
    return idx;
}

static void copy_header(art_node *dest, art_node *src) {
    dest->max_score = src->max_score;
    dest->max_token_count = src->max_token_count;
    dest->num_children = src->num_children;
    dest->partial_len = src->partial_len;
    memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}

static void add_child256(art_node256 *n, art_node **ref, unsigned char c, void *child) {
    (void)ref;
    n->n.max_score = MAX(n->n.max_score, ((art_leaf *) LEAF_RAW(child))->max_score);
    n->n.max_token_count = MAX(n->n.max_token_count, ((art_leaf *) LEAF_RAW(child))->values->ids.getLength());
    n->n.num_children++;
    n->children[c] = (art_node *) child;
}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 48) {
        int pos = 0;
        while (n->children[pos]) pos++;
        n->n.max_score = MAX(n->n.max_score, ((art_leaf *) LEAF_RAW(child))->max_score);
        n->n.max_token_count = MAX(n->n.max_token_count, ((art_leaf *) LEAF_RAW(child))->values->ids.getLength());
        n->children[pos] = (art_node *) child;
        n->keys[c] = pos + 1;
        n->n.num_children++;
    } else {
        art_node256 *new_n = (art_node256*)alloc_node(NODE256);
        for (int i=0;i<256;i++) {
            if (n->keys[i]) {
                new_n->children[i] = n->children[n->keys[i] - 1];
            }
        }
        copy_header((art_node*)new_n, (art_node*)n);
        *ref = (art_node*)new_n;
        free(n);
        add_child256(new_n, ref, c, child);
    }
}

static void add_child16(art_node16 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 16) {
        __m128i cmp;

        // Compare the key to all 16 stored keys
        cmp = _mm_cmplt_epi8(_mm_set1_epi8(c),
                             _mm_loadu_si128((__m128i*)n->keys));

        // Use a mask to ignore children that don't exist
        unsigned mask = (1 << n->n.num_children) - 1;
        unsigned bitfield = _mm_movemask_epi8(cmp) & mask;

        // Check if less than any
        unsigned idx;
        if (bitfield) {
            idx = __builtin_ctz(bitfield);
            memmove(n->keys+idx+1,n->keys+idx,n->n.num_children-idx);
            memmove(n->children+idx+1,n->children+idx,
                    (n->n.num_children-idx)*sizeof(void*));
        } else
            idx = n->n.num_children;

        // Set the child
        n->n.max_score = MAX(n->n.max_score, ((art_leaf *) LEAF_RAW(child))->max_score);
        n->n.max_token_count = MAX(n->n.max_token_count, ((art_leaf *) LEAF_RAW(child))->values->ids.getLength());
        n->keys[idx] = c;
        n->children[idx] = (art_node *) child;
        n->n.num_children++;

    } else {
        art_node48 *new_n = (art_node48*)alloc_node(NODE48);

        // Copy the child pointers and populate the key map
        memcpy(new_n->children, n->children,
                sizeof(void*)*n->n.num_children);
        for (int i=0;i<n->n.num_children;i++) {
            new_n->keys[n->keys[i]] = i + 1;
        }
        copy_header((art_node*)new_n, (art_node*)n);
        *ref = (art_node*)new_n;
        free(n);
        add_child48(new_n, ref, c, child);
    }
}

static void add_child4(art_node4 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 4) {
        int idx;
        for (idx=0; idx < n->n.num_children; idx++) {
            if (c < n->keys[idx]) break;
        }

        // Shift to make room
        memmove(n->keys+idx+1, n->keys+idx, n->n.num_children - idx);
        memmove(n->children+idx+1, n->children+idx,
                (n->n.num_children - idx)*sizeof(void*));

        int32_t child_max_score = IS_LEAF(child) ? ((art_leaf *) LEAF_RAW(child))->max_score : ((art_node *) child)->max_score;
        uint32_t child_token_count = IS_LEAF(child) ? ((art_leaf *) LEAF_RAW(child))->values->ids.getLength() : ((art_node *) child)->max_token_count;

        n->n.max_score = MAX(n->n.max_score, child_max_score);
        n->n.max_token_count = MAX(n->n.max_token_count, child_token_count);

        n->keys[idx] = c;
        n->children[idx] = (art_node *) child;
        n->n.num_children++;

    } else {
        art_node16 *new_n = (art_node16*)alloc_node(NODE16);

        // Copy the child pointers and the key map
        memcpy(new_n->children, n->children,
                sizeof(void*)*n->n.num_children);
        memcpy(new_n->keys, n->keys,
                sizeof(unsigned char)*n->n.num_children);
        copy_header((art_node*)new_n, (art_node*)n);
        *ref = (art_node*)new_n;
        free(n);
        add_child16(new_n, ref, c, child);
    }
}

static void add_child(art_node *n, art_node **ref, unsigned char c, void *child) {
    switch (n->type) {
        case NODE4:
            return add_child4((art_node4*)n, ref, c, child);
        case NODE16:
            return add_child16((art_node16*)n, ref, c, child);
        case NODE48:
            return add_child48((art_node48*)n, ref, c, child);
        case NODE256:
            return add_child256((art_node256*)n, ref, c, child);
        default:
            abort();
    }
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->partial_len > MAX_PREFIX_LEN) {
        // Prefix is longer than what we've checked, find a leaf
        art_leaf *l = minimum(n);
        max_cmp = min(l->key_len, key_len)- depth;
        for (; idx < max_cmp; idx++) {
            if (l->key[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}

static void* recursive_insert(art_node *n, art_node **ref, const unsigned char *key, uint32_t key_len, art_document *document, uint32_t num_hits, int depth, int *old) {
    // If we are at a NULL node, inject a leaf
    if (!n) {
        *ref = (art_node*)SET_LEAF(make_leaf(key, key_len, document));
        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (IS_LEAF(n)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(n);

        // Check if we are updating an existing value
        if (!leaf_matches(l, key, key_len, depth)) {
            *old = 1;
            art_values *ret_val = l->values;

            // updates are not supported
            if(!l->values->ids.contains(document->id)) {
                add_document_to_leaf(document, l);
            }
            
            return ret_val;
        }

        // New value, we must split the leaf into a node4
        art_node4 *new_n = (art_node4*)alloc_node(NODE4);

        // Create a new leaf
        art_leaf *l2 = make_leaf(key, key_len, document);

        uint32_t longest_prefix = longest_common_prefix(l, l2, depth);
        new_n->n.partial_len = longest_prefix;
        memcpy(new_n->n.partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));

        // Add the leafs to the new node4
        *ref = (art_node*)new_n;
        add_child4(new_n, ref, l->key[depth+longest_prefix], SET_LEAF(l));
        add_child4(new_n, ref, l2->key[depth+longest_prefix], SET_LEAF(l2));
        return NULL;
    }

    n->max_score = MAX(n->max_score, document->score);
    n->max_token_count = MAX(n->max_token_count, num_hits);

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        int prefix_diff = prefix_mismatch(n, key, key_len, depth);
        if ((uint32_t)prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new node
        art_node4 *new_n = (art_node4*)alloc_node(NODE4);
        *ref = (art_node*)new_n;
        new_n->n.partial_len = prefix_diff;
        memcpy(new_n->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

        // Adjust the prefix of the old node
        if (n->partial_len <= MAX_PREFIX_LEN) {
            add_child4(new_n, ref, n->partial[prefix_diff], n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n);
            add_child4(new_n, ref, l->key[depth+prefix_diff], n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                   min(MAX_PREFIX_LEN, n->partial_len));
        }

        // Insert the new leaf
        art_leaf *l = make_leaf(key, key_len, document);
        add_child4(new_n, ref, key[depth+prefix_diff], SET_LEAF(l));
        return NULL;
    }

    RECURSE_SEARCH:;

    // Find a child to recurse to
    art_node **child = find_child(n, key[depth]);
    if (child) {
        return recursive_insert(*child, child, key, key_len, document, num_hits, depth + 1, old);
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(key, key_len, document);
    add_child(n, ref, key[depth], SET_LEAF(l));
    return NULL;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const unsigned char *key, int key_len, art_document* document, uint32_t num_hits) {
    int old_val = 0;

    void *old = recursive_insert(t->root, &t->root, key, key_len, document, num_hits, 0, &old_val);
    if (!old_val) t->size++;
    return old;
}

static void remove_child256(art_node256 *n, art_node **ref, unsigned char c) {
    n->children[c] = NULL;
    n->n.num_children--;

    // Resize to a node48 on underflow, not immediately to prevent
    // trashing if we sit on the 48/49 boundary
    if (n->n.num_children == 37) {
        art_node48 *new_n = (art_node48*)alloc_node(NODE48);
        *ref = (art_node*)new_n;
        copy_header((art_node*)new_n, (art_node*)n);

        int pos = 0;
        for (int i=0;i<256;i++) {
            if (n->children[i]) {
                new_n->children[pos] = n->children[i];
                new_n->keys[i] = pos + 1;
                pos++;
            }
        }
        free(n);
    }
}

static void remove_child48(art_node48 *n, art_node **ref, unsigned char c) {
    int pos = n->keys[c];
    n->keys[c] = 0;
    n->children[pos-1] = NULL;
    n->n.num_children--;

    if (n->n.num_children == 12) {
        art_node16 *new_n = (art_node16*)alloc_node(NODE16);
        *ref = (art_node*)new_n;
        copy_header((art_node*)new_n, (art_node*)n);

        int child = 0;
        for (int i=0;i<256;i++) {
            pos = n->keys[i];
            if (pos) {
                new_n->keys[child] = i;
                new_n->children[child] = n->children[pos - 1];
                child++;
            }
        }
        free(n);
    }
}

static void remove_child16(art_node16 *n, art_node **ref, art_node **l) {
    int pos = l - n->children;
    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    if (n->n.num_children == 3) {
        art_node4 *new_n = (art_node4*)alloc_node(NODE4);
        *ref = (art_node*)new_n;
        copy_header((art_node*)new_n, (art_node*)n);
        memcpy(new_n->keys, n->keys, 4);
        memcpy(new_n->children, n->children, 4*sizeof(void*));
        free(n);
    }
}

static void remove_child4(art_node4 *n, art_node **ref, art_node **l) {
    int pos = l - n->children;
    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    // Remove nodes with only a single child
    if (n->n.num_children == 1) {
        art_node *child = n->children[0];
        if (!IS_LEAF(child)) {
            // Concatenate the prefixes
            int prefix = n->n.partial_len;
            if (prefix < MAX_PREFIX_LEN) {
                n->n.partial[prefix] = n->keys[0];
                prefix++;
            }
            if (prefix < MAX_PREFIX_LEN) {
                int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
                memcpy(n->n.partial+prefix, child->partial, sub_prefix);
                prefix += sub_prefix;
            }

            // Store the prefix in the child
            memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
            child->partial_len += n->n.partial_len + 1;
        }
        *ref = child;
        free(n);
    }
}

static void remove_child(art_node *n, art_node **ref, unsigned char c, art_node **l) {
    switch (n->type) {
        case NODE4:
            return remove_child4((art_node4*)n, ref, l);
        case NODE16:
            return remove_child16((art_node16*)n, ref, l);
        case NODE48:
            return remove_child48((art_node48*)n, ref, c);
        case NODE256:
            return remove_child256((art_node256*)n, ref, c);
        default:
            abort();
    }
}

static art_leaf* recursive_delete(art_node *n, art_node **ref, const unsigned char *key, int key_len, int depth) {
    // Search terminated
    if (!n) return NULL;

    // Handle hitting a leaf node
    if (IS_LEAF(n)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            *ref = NULL;
            return l;
        }
        return NULL;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        int prefix_len = check_prefix(n, key, key_len, depth);
        if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
            return NULL;
        }
        depth = depth + n->partial_len;
        if(depth >= key_len) {
            return NULL;
        }
    }

    assert(depth < key_len);

    // Find child node
    art_node **child = find_child(n, key[depth]);
    if (!child) return NULL;

    // If the child is leaf, delete from this node
    if (IS_LEAF(*child)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(*child);
        if (!leaf_matches(l, key, key_len, depth)) {
            remove_child(n, ref, key[depth], child);
            return l;
        }
        return NULL;

        // Recurse
    } else {
        return recursive_delete(*child, child, key, key_len, depth+1);
    }
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const unsigned char *key, int key_len) {
    art_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
    if (l) {
        t->size--;
        void *old = l->values;
        free(l);
        return old;
    }
    return NULL;
}

/*static uint32_t get_score(art_node* child) {
    if (IS_LEAF(child)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(child);
        return l->values->ids.getLength();
    }

    return child->max_token_count;
}*/

int art_topk_iter(const art_node *root, token_ordering token_order, size_t max_results,
                         std::vector<art_leaf *> &results) {
    printf("INSIDE art_topk_iter: root->type: %d\n", root->type);

    std::priority_queue<const art_node *, std::vector<const art_node *>,
            decltype(&compare_art_node_score_pq)> q(compare_art_node_score_pq);

    if(token_order == FREQUENCY) {
        q = std::priority_queue<const art_node *, std::vector<const art_node *>,
                decltype(&compare_art_node_frequency_pq)>(compare_art_node_frequency_pq);
    }

    q.push(root);

    while(!q.empty() && results.size() < max_results) {
        art_node *n = (art_node *) q.top();
        q.pop();

        if (!n) continue;
        if (IS_LEAF(n)) {
            art_leaf *l = (art_leaf *) LEAF_RAW(n);
            results.push_back(l);
            continue;
        }

        int idx;
        switch (n->type) {
            case NODE4:
                //LOG(INFO)  << "\nNODE4, SCORE: " << n->max_token_count;
                for (int i=0; i < n->num_children; i++) {
                    art_node* child = ((art_node4*)n)->children[i];
                    q.push(child);
                }
                break;

            case NODE16:
                //LOG(INFO) << "\nNODE16, SCORE: " << n->max_token_count;
                for (int i=0; i < n->num_children; i++) {
                    q.push(((art_node16*)n)->children[i]);
                }
                break;

            case NODE48:
                //LOG(INFO) << "\nNODE48, SCORE: " << n->max_token_count;
                for (int i=0; i < 256; i++) {
                    idx = ((art_node48*)n)->keys[i];
                    if (!idx) continue;
                    art_node *child = ((art_node48*)n)->children[idx - 1];
                    //LOG(INFO) << "--PUSHING NODE48 CHILD WITH SCORE: " << get_score(child);
                    q.push(child);
                }
                break;

            case NODE256:
                //LOG(INFO) << "\nNODE256, SCORE: " << n->max_token_count;
                for (int i=0; i < 256; i++) {
                    if (!((art_node256*)n)->children[i]) continue;
                    q.push(((art_node256*)n)->children[i]);
                }
                break;

            default:
                printf("ABORTING BECAUSE OF UNKNOWN NODE TYPE: %d\n", n->type);
                abort();
        }
    }

    printf("OUTSIDE art_topk_iter: results size: %d\n", results.size());
    return 0;
}

// Recursively iterates over the tree
static int recursive_iter(art_node *n, art_callback cb, void *data) {
    // Handle base cases
    if (!n) return 0;
    if (IS_LEAF(n)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(n);
        //printf("REC LEAF len: %d, key: %s\n", l->key_len, l->key);
        return cb(data, (const unsigned char*)l->key, l->key_len, l->values);
    }

    //printf("INTERNAL LEAF children: %d, partial_len: %d, partial: %s\n", n->num_children, n->partial_len, n->partial);

    int idx, res;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                //printf("INTERNAL LEAF key[i]: %c\n", ((art_node4*)n)->keys[i]);
                res = recursive_iter(((art_node4*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node16*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;

                res = recursive_iter(((art_node48*)n)->children[idx-1], cb, data);
                if (res) return res;
            }
            break;

        case NODE256:
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                res = recursive_iter(((art_node256*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        default:
            abort();
    }
    return 0;
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art_tree *t, art_callback cb, void *data) {
    return recursive_iter(t->root, cb, data);
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_matches(const art_leaf *n, const unsigned char *prefix, int prefix_len) {
    // Fail if the key length is too short
    if (n->key_len < (uint32_t)prefix_len) return 1;

    // Compare the keys
    return memcmp(n->key, prefix, prefix_len);
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_prefix(art_tree *t, const unsigned char *key, int key_len, art_callback cb, void *data) {
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        //printf("partial_len: %d\n", n->num_children);

        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node *) LEAF_RAW(n);

            printf("RAW LEAF len: %d, children: %d\n", n->partial_len, n->num_children);

            // Check if the expanded path matches
            if (!leaf_prefix_matches((art_leaf*)n, key, key_len)) {
                art_leaf *l = (art_leaf*)n;
                return cb(data, (const unsigned char*)l->key, l->key_len, l->values);
            }
            return 0;
        }

        printf("IS_INTERNAL\n");
        printf("Prefix len: %d, children: %d, depth: %d, partial: %s\n", n->partial_len, n->num_children, depth, n->partial);

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            art_leaf *l = minimum(n);
            printf("DEPTH LEAF len: %d, key: %s\n", l->key_len, l->key);

            if (!leaf_prefix_matches(l, key, key_len))
                return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if (prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return 0;
            } else if (depth + prefix_len == key_len) {
                // If we've matched the prefix, iterate on this node
                return recursive_iter(n, cb, data);
            } else if(depth + n->partial_len >= key_len) {
                return 0;
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        assert(depth < key_len);

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return 0;
}

void print_row(const int* row, const int row_len) {
    for(int i=0; i<=row_len; i++) {
        printf("%d ", row[i]);
    }

    printf("\n");
}

static inline void copyIntArray2(const int *src, int *dest, const int len) {
    for(int t=0; t<len; t++) {
        dest[t] = src[t];
    }
}

static inline int levenshtein_dist(const int depth, const char p, const char c, const unsigned char* term, const int term_len,
                                   const int* irow, const int* jrow, int* krow) {
    int row_min = std::numeric_limits<int>::max();
    const int columns = term_len+1;
    krow[0] = jrow[0] + 1;

    // Calculate levenshtein distance incrementally (j => column, b => term):
    // https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Optimal_string_alignment_distance

    for(int column=1; column<columns; column++) {
        int delete_cost = jrow[column] + 1;
        int insert_cost = krow[column - 1] + 1;

        int cost = (c != term[column-1]) ? 1 : 0;
        int replace_cost = jrow[column - 1] + cost;

        krow[column] = min(min(insert_cost, delete_cost), replace_cost);

        if(depth > 1 && column > 1 && c == term[column-2] && p == term[column-1]) {
            krow[column] = std::min(krow[column], irow[column-2] + cost);
        }

        if(krow[column] < row_min) {
            row_min = krow[column];
        }
    }

    return row_min;
}

static inline void art_fuzzy_children(char p, const art_node *n, int depth, const unsigned char *term, const int term_len,
                                      const int* irow, const int* jrow, const int min_cost, const int max_cost,
                                      const bool prefix, std::vector<const art_node *> &results) {
    char child_char;
    art_node* child;

    switch (n->type) {
        case NODE4:
            printf("\nNODE4\n");
            for (int i=n->num_children-1; i >= 0; i--) {
                child_char = ((art_node4*)n)->keys[i];
                printf("4!child_char: %c, %d, depth: %d\n", child_char, child_char, depth);
                child = ((art_node4*)n)->children[i];
                art_fuzzy_recurse(p, child_char, child, depth, term, term_len, irow, jrow, min_cost, max_cost, prefix, results);
            }
            break;
        case NODE16:
            printf("\nNODE16\n");
            for (int i=n->num_children-1; i >= 0; i--) {
                child_char = ((art_node16*)n)->keys[i];
                printf("16!child_char: %c, depth: %d\n", child_char, depth);
                child = ((art_node16*)n)->children[i];
                art_fuzzy_recurse(p, child_char, child, depth, term, term_len, irow, jrow, min_cost, max_cost, prefix, results);
            }
            break;
        case NODE48:
            printf("\nNODE48\n");
            for (int i=255; i >= 0; i--) {
                int ix = ((art_node48*)n)->keys[i];
                if (!ix) continue;
                child = ((art_node48*)n)->children[ix - 1];
                child_char = (char)i;
                printf("48!child_char: %c, depth: %d, ix: %d\n", child_char, depth, ix);
                art_fuzzy_recurse(p, child_char, child, depth, term, term_len, irow, jrow, min_cost, max_cost, prefix, results);
            }
            break;
        case NODE256:
            printf("\nNODE256\n");
            for (int i=255; i >= 0; i--) {
                if (!((art_node256*)n)->children[i]) continue;
                child_char = (char) i;
                printf("256!child_char: %c, depth: %d\n", child_char, depth);
                child = ((art_node256*)n)->children[i];
                art_fuzzy_recurse(p, child_char, child, depth, term, term_len, irow, jrow, min_cost, max_cost, prefix, results);
            }
            break;
        default:
            abort();
    }
}

static inline void rotate(int &i, int &j, int &k) {
    int old_i = i;
    i = j;
    j = k;
    k = old_i;
}

// e.g. catapult against coratapult
// e.g. microafot against microsoft
static void art_fuzzy_recurse(char p, char c, const art_node *n, int depth, const unsigned char *term,
                              const int term_len, const int* irow, const int* jrow, const int min_cost,
                              const int max_cost, const bool prefix, std::vector<const art_node *> &results) {
    const int columns = term_len+1;
    int i=0, j=1, k=2;
    int row0[columns];
    int row1[columns];
    int row2[columns];
    int* rows[3] = {row0, row1, row2};

    copyIntArray2(irow, rows[i], columns);
    copyIntArray2(jrow, rows[j], columns);

    int cost = levenshtein_dist(depth, p, c, term, term_len, rows[i], rows[j], rows[k]);
    rotate(i, j, k);
    p = c;

    depth++;

    printf("Recurse char: %c, cost: %d, depth: %d\n", c, cost, depth);

    if(cost > max_cost) {
        // We do this to speed up things drastically, but at the cost of missing out on some genuine typos
        return;
    }

    if (!n) return ;

    if(IS_LEAF(n)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(n);
        printf("\nIS_LEAF\nLEAF KEY: %s, depth: %d\n", l->key, depth);

        /*
           For prefix search, when key is longer than term, we could potentially iterate till `term_len+max_cost`. E.g:
           term = `th`, key = `mathematics` - if we compared only first 2 chars, it will exceed max_cost
           However, we refrain from doing so for performance reasons, or atleast until we hear strong objections.

           Also, for prefix searches we don't compare with full leaf key.
         */
        const int end_index = prefix ? min(l->key_len, term_len) : l->key_len;

        // If at any point, `cost > 2*max_cost` we can terminate immediately as we can never recover from that
        while(depth < end_index && cost <= 2*max_cost) {
            c = l->key[depth];
            cost = levenshtein_dist(depth, p, c, term, term_len, rows[i], rows[j], rows[k]);
            printf("leaf char: %c\n", l->key[depth]);
            printf("cost: %d, depth: %d, term_len: %d\n", cost, depth, term_len);
            rotate(i, j, k);
            p = c;
            depth++;
        }

        /* `rows[j][columns-1]` holds the final cost, `cost` holds the intermediate cost.
            We will use the intermediate cost if the term is shorter than the key.
         */
        if(prefix && term_len < (int) l->key_len && cost >= min_cost && cost <= max_cost) {
            results.push_back(n);
            return;
        }

        if(prefix && term_len >= (int) l->key_len && rows[j][columns-1] >= min_cost && rows[j][columns-1] <= max_cost) {
            results.push_back(n);
            return;
        }

        if(!prefix && rows[j][columns-1] >= min_cost && rows[j][columns-1] <= max_cost) {
            results.push_back(n);
            return;
        }

        return ;
    }

    if(prefix && depth >= term_len && rows[j][columns-1] <= max_cost) {
        // For a prefix search, we store the node and not recurse further right now
        results.push_back(n);
        return ;
    }

    const int partial_len = min(MAX_PREFIX_LEN, n->partial_len);
    const int end_index = min(partial_len, term_len+max_cost);

    printf("partial_len: %d\n", partial_len);
    
    for(int idx=0; idx<end_index; idx++) {
        c = n->partial[idx];
        printf("partial: %c\n", c);
        rows[k][0] = rows[j][0] + 1;
        cost = levenshtein_dist(depth, p, c, term, term_len, rows[i], rows[j], rows[k]);
        rotate(i, j, k);
        p = c;
        
        if(prefix && depth+idx+1 >= term_len && rows[j][columns-1] <= max_cost) {
            // For a prefix search, we store the node and not recurse further right now
            results.push_back(n);
            return ;
        }
    }

    depth += n->partial_len;
    printf("cost: %d\n", cost);

    art_fuzzy_children(c, n, depth, term, term_len, rows[i], rows[j], min_cost, max_cost, prefix, results);
}

/**
 * Returns leaves that match a given string within a fuzzy distance of max_cost.
 */
int art_fuzzy_search(art_tree *t, const unsigned char *term, const int term_len, const int min_cost, const int max_cost,
                     const int max_words, const token_ordering token_order, const bool prefix,
                     std::vector<art_leaf *> &results) {

    std::vector<const art_node*> nodes;
    int irow[term_len + 1];
    int jrow[term_len + 1];
    for (int i = 0; i <= term_len; i++){
        jrow[i] = i;
        irow[i] = i;
    }

    //auto begin = std::chrono::high_resolution_clock::now();

    if(IS_LEAF(t->root)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(t->root);
        art_fuzzy_recurse(0, l->key[0], t->root, 0, term, term_len, irow, jrow, min_cost, max_cost, prefix, nodes);
    } else {
        if(t->root == NULL) {
            return 0;
        }
        art_fuzzy_children(0, t->root, 0, term, term_len, irow, jrow, min_cost, max_cost, prefix, nodes);
    }

    if(token_order == FREQUENCY) {
        std::sort(nodes.begin(), nodes.end(), compare_art_node_frequency);
    } else {
        std::sort(nodes.begin(), nodes.end(), compare_art_node_score);
    }

    //long long int time_micro = microseconds(std::chrono::high_resolution_clock::now() - begin).count();
    //!LOG(INFO) << "Time taken for fuzz: " << time_micro << "us, size of nodes: " << nodes.size();

    //begin = std::chrono::high_resolution_clock::now();

    for(auto node: nodes) {
        art_topk_iter(node, token_order, max_words, results);
    }

    if(token_order == FREQUENCY) {
        std::sort(results.begin(), results.end(), compare_art_leaf_frequency);
    } else {
        std::sort(results.begin(), results.end(), compare_art_leaf_score);
    }

    //time_micro = microseconds(std::chrono::high_resolution_clock::now() - begin).count();
    //!LOG(INFO) << "Time taken for art_topk_iter: " << time_micro << "us";
    return 0;
}

void encode_int32(int32_t n, unsigned char *chars) {
    unsigned char symbols[16] = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    };

    unsigned char bytes[4];

    bytes[0] = (unsigned char) ((n >> 24) & 0xFF);
    bytes[1] = (unsigned char) ((n >> 16) & 0xFF);
    bytes[2] = (unsigned char) ((n >> 8) & 0xFF);
    bytes[3] = (unsigned char) (n & 0xFF);

    for(uint32_t i = 0; i < 4; i++) {
        chars[2*i] = symbols[((bytes[i] >> 4) & 0x0F)];
        chars[2*i+1] = symbols[(bytes[i] & 0x0F)];
    }
}

void encode_int64(int64_t n, unsigned char *chars) {
    chars[0] = (unsigned char) ((n >> 56) & 0xFF);
    chars[1] = (unsigned char) ((n >> 48) & 0xFF);
    chars[2] = (unsigned char) ((n >> 40) & 0xFF);
    chars[3] = (unsigned char) ((n >> 32) & 0xFF);
    chars[4] = (unsigned char) ((n >> 24) & 0xFF);
    chars[5] = (unsigned char) ((n >> 16) & 0xFF);
    chars[6] = (unsigned char) ((n >> 8) & 0xFF);
    chars[7] = (unsigned char) (n & 0xFF);
}

// See: https://github.com/apache/hbase/blob/master/hbase-common/src/main/java/org/apache/hadoop/hbase/util/OrderedBytes.java#L1372
void encode_float(float n, unsigned char *chars) {
    int32_t i;
    memcpy(&i, &n, sizeof(int32_t));
    i ^= ((i >> (std::numeric_limits<int32_t>::digits - 1)) | INT32_MIN);
    encode_int32(i, chars);
}

// Implements ==, <= and >=
recurse_progress matches(unsigned char a, unsigned char b, NUM_COMPARATOR comparator) {
    switch(comparator) {
        case LESS_THAN:
        case LESS_THAN_EQUALS:
            if (a == b) return RECURSE;
            else if(a < b) return ITERATE;
            return ABORT;
        case EQUALS:
            if(a == b) return RECURSE;
            return ABORT;
        case GREATER_THAN:
        case GREATER_THAN_EQUALS:
            if (a == b) return RECURSE;
            else if(a > b) return ITERATE;
            return ABORT;
        default:
            abort();
    }
}


static void art_iter(const art_node *n, const unsigned char* int_str, int int_str_len, NUM_COMPARATOR comparator,
                     std::vector<const art_leaf *> &results) {
    // Handle base cases
    if (!n) return ;
    if (IS_LEAF(n)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(n);
        compare_and_match_leaf(int_str, int_str_len, comparator, results, l);
        return ;
    }

    int idx;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                art_iter(((art_node4 *) n)->children[i], int_str, int_str_len, comparator, results);
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                art_iter(((art_node16 *) n)->children[i], int_str, int_str_len, comparator, results);
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;
                art_iter(((art_node48 *) n)->children[idx - 1], int_str, int_str_len, comparator, results);
            }
            break;

        case NODE256:
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                art_iter(((art_node256 *) n)->children[i], int_str, int_str_len, comparator, results);
            }
            break;

        default:
            abort();
    }

    return ;
}

static inline void art_int_fuzzy_children(const art_node *n, int depth, const unsigned char* int_str, int int_str_len,
                                          NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results) {
    unsigned char child_char;
    art_node* child;

    switch (n->type) {
        case NODE4:
            printf("\nNODE4\n");
            for (int i=n->num_children-1; i >= 0; i--) {
                child_char = ((art_node4*)n)->keys[i];
                printf("4!child_char: %c, %d, depth: %d\n", child_char, child_char, depth);
                child = ((art_node4*)n)->children[i];
                recurse_progress progress = matches(child_char, int_str[depth], comparator);
                if(progress == RECURSE) {
                    art_int_fuzzy_recurse(child, depth+1, int_str, int_str_len, comparator, results);
                } else if(progress == ITERATE) {
                    art_iter(child, int_str, int_str_len, comparator, results);
                }
            }
            break;
        case NODE16:
            printf("\nNODE16\n");
            for (int i=n->num_children-1; i >= 0; i--) {
                child_char = ((art_node16*)n)->keys[i];
                printf("16!child_char: %c, depth: %d\n", child_char, depth);
                child = ((art_node16*)n)->children[i];
                recurse_progress progress = matches(child_char, int_str[depth], comparator);
                if(progress == RECURSE) {
                    art_int_fuzzy_recurse(child, depth+1, int_str, int_str_len, comparator, results);
                } else if(progress == ITERATE) {
                    art_iter(child, int_str, int_str_len, comparator, results);
                }
            }
            break;
        case NODE48:
            printf("\nNODE48\n");
            for (int i=255; i >= 0; i--) {
                int ix = ((art_node48*)n)->keys[i];
                if (!ix) continue;
                child = ((art_node48*)n)->children[ix - 1];
                child_char = (unsigned char)i;
                printf("48!child_char: %c, depth: %d, ix: %d\n", child_char, depth, ix);
                recurse_progress progress = matches(child_char, int_str[depth], comparator);
                if(progress == RECURSE) {
                    art_int_fuzzy_recurse(child, depth+1, int_str, int_str_len, comparator, results);
                } else if(progress == ITERATE) {
                    art_iter(child, int_str, int_str_len, comparator, results);
                }
            }
            break;
        case NODE256:
            printf("\nNODE256\n");
            for (int i=255; i >= 0; i--) {
                if (!((art_node256*)n)->children[i]) continue;
                child_char = (unsigned char) i;
                printf("256!child_char: %c, depth: %d\n", child_char, depth);
                child = ((art_node256*)n)->children[i];
                recurse_progress progress = matches(child_char, int_str[depth], comparator);
                if(progress == RECURSE) {
                    art_int_fuzzy_recurse(child, depth+1, int_str, int_str_len, comparator, results);
                } else if(progress == ITERATE) {
                    art_iter(child, int_str, int_str_len, comparator, results);
                }
            }
            break;
        default:
            abort();
    }
}

void art_int_fuzzy_recurse(art_node *n, int depth, const unsigned char* int_str, int int_str_len,
                           NUM_COMPARATOR comparator, std::vector<const art_leaf*> &results) {
    if (!n) return ;

    if(IS_LEAF(n)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(n);
        while(depth < int_str_len) {
            unsigned char c = l->key[depth];
            recurse_progress progress = matches(c, int_str[depth], comparator);
            if(progress == ABORT) {
                return;
            }

            if(progress == ITERATE) {
                break;
            }

            depth++;
        }

        compare_and_match_leaf(int_str, int_str_len, comparator, results, l);
        return ;
    }

    const int partial_len = min(MAX_PREFIX_LEN, n->partial_len);
    const int end_index = min(partial_len, int_str_len);

    printf("\npartial_len: %d", partial_len);

    for(int idx=0; idx<end_index; idx++) {
        unsigned char c = n->partial[idx];
        recurse_progress progress = matches(c, int_str[depth+idx], comparator);
        if(progress == ABORT) {
            return;
        }

        if(progress == ITERATE) {
            return art_iter(n, int_str, int_str_len, comparator, results);
        }
    }

    depth += n->partial_len;
    art_int_fuzzy_children(n, depth, int_str, int_str_len, comparator, results);
}

void compare_and_match_leaf(const unsigned char *int_str, int int_str_len, const NUM_COMPARATOR &comparator,
                            std::vector<const art_leaf *> &results, const art_leaf *l) {
    if(comparator == LESS_THAN || comparator == GREATER_THAN) {
        for(uint32_t i = 0; i < l->key_len; i++) {
            if(int_str[i] != l->key[i]) {
                results.push_back(l);
                return ;
            }
        }
    } else {
        results.push_back(l);
    }
}

int art_int32_search(art_tree *t, int32_t value, NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results) {
    unsigned char chars[8];
    encode_int32(value, chars);
    art_int_fuzzy_recurse(t->root, 0, chars, 8, comparator, results);
    return 0;
}

int art_int64_search(art_tree *t, int64_t value, NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results) {
    unsigned char chars[8];
    encode_int64(value, chars);
    art_int_fuzzy_recurse(t->root, 0, chars, 8, comparator, results);
    return 0;
}

int art_float_search(art_tree *t, float value, NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results) {
    unsigned char chars[8];
    encode_float(value, chars);
    art_int_fuzzy_recurse(t->root, 0, chars, 8, comparator, results);
    return 0;
}