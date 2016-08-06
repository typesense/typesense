#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <art.h>
#include <iostream>
#include <limits>
#include <queue>
#include "art.h"

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

/*
 * Comparator for art_leaf struct.
 * We order the leaves descending based on the score.
 */
bool compare_art_leaf(const art_leaf* a, const art_leaf* b) {
    return a->token_count > b->token_count;
}

bool compare_art_node(const art_node* a, const art_node* b) {
    uint32_t a_score = 0, b_score = 0;

    if(IS_LEAF(a)) {
        art_leaf* al = (art_leaf *) LEAF_RAW(a);
        a_score = al->token_count;
    } else {
        a_score = a->max_token_count;
    }

    if(IS_LEAF(b)) {
        art_leaf* bl = (art_leaf *) LEAF_RAW(b);
        b_score = bl->token_count;
    } else {
        b_score = b->max_token_count;
    }

    return a_score > b_score;
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
        free(LEAF_RAW(n));
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
            for (i=0;i<n->num_children;i++) {
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
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return NULL;
            depth = depth + n->partial_len;
        }

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
    leaf->token_count += document->offsets_len;
    leaf->values->ids.append_sorted(document->id);
    uint32_t curr_index = leaf->values->offsets.getLength();
    leaf->values->offset_index.append_sorted(curr_index);

    leaf->values->offsets.append_unsorted(document->offsets_len);
    for(uint32_t i=0; i<document->offsets_len; i++) {
        leaf->values->offsets.append_unsorted(document->offsets[i]);
    }
}

static art_leaf* make_leaf(const unsigned char *key, uint32_t key_len, art_document *document) {
    art_leaf *l = (art_leaf *) malloc(sizeof(art_leaf) + key_len);
    l->values = new art_values;
    l->token_count = 0;
    l->max_score = 0;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    add_document_to_leaf(document, l);
    return l;
}

static uint32_t longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
    int max_cmp = min(l1->key_len, l2->key_len) - depth;
    uint32_t idx;
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
    n->n.max_token_count = MAX(n->n.max_token_count, ((art_leaf *) LEAF_RAW(child))->token_count);
    n->n.num_children++;
    n->children[c] = (art_node *) child;
}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 48) {
        int pos = 0;
        while (n->children[pos]) pos++;
        n->n.max_score = MAX(n->n.max_score, ((art_leaf *) LEAF_RAW(child))->max_score);
        n->n.max_token_count = MAX(n->n.max_token_count, ((art_leaf *) LEAF_RAW(child))->token_count);
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
        n->n.max_token_count = MAX(n->n.max_token_count, ((art_leaf *) LEAF_RAW(child))->token_count);
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

        uint16_t child_max_score = IS_LEAF(child) ? ((art_leaf *) LEAF_RAW(child))->max_score : ((art_node *) child)->max_score;
        uint32_t child_token_count = IS_LEAF(child) ? ((art_leaf *) LEAF_RAW(child))->token_count : ((art_node *) child)->max_token_count;

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

static void* recursive_insert(art_node *n, art_node **ref, const unsigned char *key, uint32_t key_len, art_document *document, uint32_t num_hits, int depth, int *old_val) {
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
            // updates are not supported
            if(l->values->ids.contains(document->id)) {
                return NULL;
            }

            *old_val = 1;
            art_values *old_val = l->values;
            add_document_to_leaf(document, l);
            return old_val;
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

    n->max_score = (uint16_t) MAX(n->max_score, (const uint16_t &) document->score);
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
        return recursive_insert(*child, child, key, key_len, document, num_hits, depth + 1, old_val);
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
    }

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

static uint32_t get_score(art_node* child) {
    if (IS_LEAF(child)) {
        art_leaf *l = (art_leaf *) LEAF_RAW(child);
        return l->token_count;
    }

    return child->max_token_count;
}

static int topk_iter(art_node *root, int term_len, int k, std::vector<art_leaf*> & results) {
    printf("INSIDE topk_iter: root->type: %d\n", root->type);

    auto cmp = [term_len](art_node * left, art_node * right) {
        int lscore, rscore;
        if (IS_LEAF(left)) {
            art_leaf *l = (art_leaf *) LEAF_RAW(left);
            lscore = l->token_count;
        } else {
            lscore = left->max_token_count;
        }

        if(IS_LEAF(right)) {
            art_leaf *r = (art_leaf *) LEAF_RAW(right);
            rscore = r->token_count;
        } else {
            rscore = right->max_token_count;
        }

        // priority queue sorts based on priority (so use < for sorting descending order by score)
        return lscore < rscore;
    };

    std::priority_queue<art_node *, std::vector<art_node *>, decltype(cmp)> q(cmp);

    q.push(root);

    while(!q.empty() && results.size() < k) {
        art_node *n = (art_node *) q.top();
        q.pop();

        if (!n) continue;
        if (IS_LEAF(n)) {
            art_leaf *l = (art_leaf *) LEAF_RAW(n);
            //printf("\nLEAF: %.*s", leaf->key_len, leaf->key);
            //std::cout << ", SCORE: " << l->token_count << std::endl;
            results.push_back(l);
            continue;
        }

        int idx;
        switch (n->type) {
            case NODE4:
                //std::cout << "\nNODE4, SCORE: " << n->max_token_count << std::endl;
                for (int i=0; i < n->num_children; i++) {
                    art_node* child = ((art_node4*)n)->children[i];
                    q.push(child);
                }
                break;

            case NODE16:
                //std::cout << "\nNODE16, SCORE: " << n->max_token_count << std::endl;
                for (int i=0; i < n->num_children; i++) {
                    q.push(((art_node16*)n)->children[i]);
                }
                break;

            case NODE48:
                //std::cout << "\nNODE48, SCORE: " << n->max_token_count << std::endl;
                for (int i=0; i < 256; i++) {
                    idx = ((art_node48*)n)->keys[i];
                    if (!idx) continue;
                    art_node *child = ((art_node48*)n)->children[idx - 1];
                    //std::cout << "--PUSHING NODE48 CHILD WITH SCORE: " << get_score(child) << std::endl;
                    q.push(child);
                }
                break;

            case NODE256:
                //std::cout << "\nNODE256, SCORE: " << n->max_token_count << std::endl;
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

    std::sort(results.begin(), results.end(), compare_art_leaf);
    printf("OUTSIDE topk_iter: results size: %d\n", results.size());

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

            // If there is no match, search is terminated
            if (!prefix_len)
                return 0;

                // If we've matched the prefix, iterate on this node
            else if (depth + prefix_len == key_len) {
                return recursive_iter(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return 0;
}

#define fuzzy_recurse(child, child_char, term, term_len, depth, previous_row, results) {\
    int new_current_row[term_len+1];\
    new_current_row[0] = previous_row[0] + 1;\
    row_min = levenshtein_score(child_char, term, term_len, previous_row, new_current_row);\
\
    printf("fuzzy_recurse - score: %d, child char: %c, cost: %d, max_cost: %d, row_min: %d, depth: %d, term[depth]: %c \n",\
            child->max_score, child_char, new_current_row[term_len], max_cost, row_min, depth, term[depth]);\
\
    if(depth == term_len-1) {\
      /* reached end of term, and cost is below threshold, print children of this node as matches*/\
      if(row_min <= max_cost) {\
        printf("START RECURSIVE ITER\n");\
        results.push_back(child);\
      }\
    } else if(row_min <= max_cost) {\
      int new_depth = (child_char != 0) ? depth+1 : depth;\
      art_iter_fuzzy_prefix_recurse(child, term, term_len, max_cost, new_depth, new_current_row, results);\
    }\
}\

void print_row(const int* row, const int row_len) {
    for(int i=0; i<=row_len; i++) {
        printf("%d ", row[i]);
    }

    printf("\n");
}

int levenshtein_score(const char ch, const unsigned char* term, const int term_len, const int* previous_row, int* current_row) {
    int row_min = std::numeric_limits<int>::max();
    int insert_or_del, replace;

    printf("\nPREVIOUS ROW: ");
    print_row(previous_row, term_len);

    // calculate the min cost of insertion, deletion, match or replace
    for (int i = 1; i <= term_len; i++) {
        insert_or_del = min(current_row[i-1] + 1, previous_row[i] + 1);
        replace = (term[i - 1] == ch) ? previous_row[i - 1] : (previous_row[i - 1] + 1);
        current_row[i] = min(insert_or_del, replace);

        if(current_row[i] < row_min) row_min = current_row[i];
    }

    printf("\nCURRENT ROW: ");
    print_row(current_row, term_len);
    return row_min;
}

void copyIntArray(int *dest, int *src, int len) {
    for(int t=0; t<len; t++) {
        dest[t] = src[t];
    }
}

static int art_iter_fuzzy_prefix_recurse(art_node *n, const unsigned char *term, const int term_len, const int max_cost,
                                         int depth, int* previous_row, std::vector<art_node*> & results) {
    if (!n) return 0;

    if (IS_LEAF(n)) {
        printf("IS_LEAF\n");

        art_leaf *l = (art_leaf *) LEAF_RAW(n);

        int row_min = 0;
        int current_row[term_len+1];
        copyIntArray(current_row, previous_row, term_len+1);
        current_row[0] = previous_row[0] + 1;

        printf("LEAF KEY: %s, depth: %d\n", l->key, depth);

        for(int idx=depth; idx<l->key_len && row_min <= max_cost; idx++) {
            row_min = levenshtein_score(l->key[idx], term, term_len, previous_row, current_row);
            printf("leaf char: %c\n", l->key[idx]);
            printf("score: %d, depth: %d, term_len: %d, row_min: %d\n", current_row[term_len], depth, term_len, row_min);

            //printf("CURRENT RETURNED\n");
            //print_row(current_row, term_len);

            copyIntArray(previous_row, current_row, term_len+1);
            current_row[0] = previous_row[0] + 1;
            depth   += 1;
            //printf("PREVIOUS BEFORE CALLING\n");
            //print_row(previous_row, term_len);
        }

        if(current_row[term_len] <= max_cost) {
            results.push_back(n);
        }

        return 0;
    }

    printf("START PARTIAL: score: %d, partial_len: %d, partial: %s, term_len: %d, depth: %d\n",
           n->max_score, n->partial_len, n->partial, term_len, depth);

    // internal node - first we check partial (via path compression) and then child index
    int partial_len = min(MAX_PREFIX_LEN, n->partial_len);
    int row_min = 0;
    int current_row[term_len+1];

    for(int idx=0; idx<partial_len && depth < term_len && row_min <= max_cost; idx++) {
        printf("partial: %c ", n->partial[idx]);
        current_row[0] = previous_row[0] + 1;
        row_min = levenshtein_score(n->partial[idx], term, term_len, previous_row, current_row);
        copyIntArray(previous_row, current_row, term_len+1);
        depth += 1;
    }

    printf("CURRENT ROW PARTIAL:\n");
    print_row(current_row, term_len);

    printf("PREVIOUS ROW PARTIAL:\n");
    print_row(previous_row, term_len);
    printf("\n");

    if(depth == term_len) {
        if(current_row[term_len] <= max_cost) {
            printf("PARTIAL START RECURSIVE ITER\n");
            results.push_back(n);
        }

        return 0;
    }

    printf("END PARTIAL\n");
    printf("Children: %d\n", n->num_children);

    switch (n->type) {
        case NODE4:
            printf("NODE4\n");
            for (int i=0; i < n->num_children; i++) {
                char child_char = ((art_node4*)n)->keys[i];
                printf("4!child_char: %c, %d, depth: %d", child_char, child_char, depth);
                art_node* child = ((art_node4*)n)->children[i];
                fuzzy_recurse(child, child_char, term, term_len, depth, previous_row, results);
            }
            break;

        case NODE16:
            printf("NODE16\n");
            for (int i=0; i < n->num_children; i++) {
                char child_char = ((art_node16*)n)->keys[i];
                printf("16!child_char: %c, depth: %d", child_char, depth);
                art_node* child = ((art_node16*)n)->children[i];
                fuzzy_recurse(child, child_char, term, term_len, depth, previous_row, results);
            }
            break;

        case NODE48:
            printf("NODE48\n");
            for (int i=0; i < 256; i++) {
                int ix = ((art_node48*)n)->keys[i];
                if (!ix) continue;
                art_node* child = ((art_node48*)n)->children[ix - 1];
                char child_char = (char)i;
                printf("48!child_char: %c, depth: %d, ix: %d", child_char, depth, ix);
                fuzzy_recurse(child, child_char, term, term_len, depth, previous_row, results);
            }
            break;

        case NODE256:
            printf("NODE256\n");
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                char child_char = (char) i;
                printf("256!child_char: %c, depth: %d", child_char, depth);
                art_node* child = ((art_node256*)n)->children[i];
                fuzzy_recurse(child, child_char, term, term_len, depth, previous_row, results);
            }
            break;

        default:
            abort();
    }

    return 0;
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix within a fuzzy distance of 2.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg max_cost Maximum fuzzy edit distance
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_fuzzy_prefix(art_tree *t, const unsigned char *term, int term_len,
                          int max_cost, int max_words, std::vector<art_leaf*> & results) {

    int previous_row[term_len + 1];

    for (int i = 0; i <= term_len; i++){
        previous_row[i] = i;
    }

    std::vector<art_node*> nodes;

    auto begin = std::chrono::high_resolution_clock::now();
    art_iter_fuzzy_prefix_recurse(t->root, term, term_len, max_cost, 0, previous_row, nodes);
    std::sort(nodes.begin(), nodes.end(), compare_art_node);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Time taken for fuzz: " << timeMillis << "us" << std::endl;

    begin = std::chrono::high_resolution_clock::now();

    for(auto node: nodes) {
        topk_iter(node, term_len, max_words, results);
    }

    timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Time taken 2 iter: " << timeMillis << "us" << std::endl;
    return 0;
}