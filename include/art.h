#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <vector>
#include <set>
#include "array.h"
#include "sorted_array.h"
#include "filter_result_iterator.h"
#include "filter.h"

#define IGNORE_PRINTF 1

#ifdef __cplusplus
extern "C" {
#endif

#define NODE4   1
#define NODE16  2
#define NODE48  3
#define NODE256 4

#define MAX_PREFIX_LEN 8

#define MAX(x, y) (((x) > (y)) ? (x) : (y))

#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

typedef int(*art_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    unsigned char partial[MAX_PREFIX_LEN];
    int64_t max_score;
} art_node;

/**
 * Small node with only 4 children
 */
typedef struct {
    art_node n;
    unsigned char keys[4];
    art_node *children[4];
} art_node4;

/**
 * Node with 16 children
 */
typedef struct {
    art_node n;
    unsigned char keys[16];
    art_node *children[16];
} art_node16;

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
typedef struct {
    art_node n;
    unsigned char keys[256];
    art_node *children[48];
} art_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    art_node n;
    art_node *children[256];
} art_node256;

/**
 * Container for holding the documents that belong to a leaf.
 */
typedef struct {
    sorted_array ids;
    sorted_array offset_index;
    array offsets;
} art_values;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    uint32_t key_len;
    int64_t max_score;
    void* values;
    unsigned char key[];
} art_leaf;

struct token_leaf {
    art_leaf* leaf;
    bool is_prefix;
    uint32_t root_len;
    uint32_t num_typos;

    token_leaf(art_leaf* leaf, uint32_t root_len, uint32_t num_typos, bool is_prefix) :
            leaf(leaf), root_len(root_len), num_typos(num_typos), is_prefix(is_prefix) {

    }
};

/**
 * Main struct, points to root.
 */
typedef struct {
    art_node *root;
    uint64_t size;
} art_tree;

/*
 * Represents a document to be indexed.
 * `offsets` refer to the index locations where a token appeared in the document
 */
struct art_document {
    const uint32_t id;
    const int64_t score;
    const std::vector<uint32_t> offsets;

    art_document(const uint32_t id, const int64_t score, const std::vector<uint32_t>& offsets):
            id(id), score(score), offsets(offsets) {

    }
};

enum token_ordering {
    NOT_SET,

    FREQUENCY,
    MAX_SCORE
};

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_art_tree(...) art_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_art_tree(...) art_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.
 */
#ifdef BROKEN_GCC_C99_INLINE
# define art_size(t) ((t)->size)
#else
inline uint64_t art_size(art_tree *t) {
    return t->size;
}
#endif

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const unsigned char *key, int key_len, art_document* document);

/* Insert multiple docs sharing the same key */
void* art_inserts(art_tree *t, const unsigned char *key, int key_len, const int64_t docs_max_score,
                  std::vector<art_document>& documents);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const unsigned char *key, int key_len);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const unsigned char *key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
art_leaf* art_minimum(art_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
art_leaf* art_maximum(art_tree *t);

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
int art_iter(art_tree *t, art_callback cb, void *data);

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
int art_iter_prefix(art_tree *t, const unsigned char *prefix, int prefix_len, art_callback cb, void *data);

/**
 * Returns leaves that match a given string within a fuzzy distance of max_cost.
 */
int art_fuzzy_search(art_tree *t, const unsigned char *term, const int term_len, const int min_cost, const int max_cost,
                     const size_t max_words, const token_ordering token_order,
                     const bool prefix, bool last_token, const std::string& prev_token,
                     const uint32_t *filter_ids, const size_t filter_ids_length,
                     std::vector<art_leaf *> &results, std::set<std::string>& exclude_leaves);

int art_fuzzy_search_i(art_tree *t, const unsigned char *term, const int term_len, const int min_cost, const int max_cost,
                     const size_t max_words, const token_ordering token_order,
                     const bool prefix, bool last_token, const std::string& prev_token,
                     filter_result_iterator_t* const filter_result_iterator,
                     std::vector<art_leaf *> &results, std::set<std::string>& exclude_leaves);

void encode_int32(int32_t n, unsigned char *chars);

void encode_int64(int64_t n, unsigned char *chars);

void encode_float(float n, unsigned char *chars);

int art_int32_search(art_tree *t, int32_t value, NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results);

int art_int64_search(art_tree *t, int64_t value, NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results);

int art_float_search(art_tree *t, float value, NUM_COMPARATOR comparator, std::vector<const art_leaf *> &results);

#ifdef __cplusplus
}
#endif