#include <gtest/gtest.h>
#include <cvt.h>

/*
TEST(CVTTest, TaggedPointers) {
    CVTrie trie;
    uint8_t* bytes = new uint8_t[3];
    bytes[0] = 0;
    bytes[1] = 1;
    bytes[2] = 2;

    // LEAF

    void* leaf_tagged_ptr = trie.tag_ptr(bytes, 100, LEAF);
    ASSERT_EQ(LEAF, trie.get_node_type(leaf_tagged_ptr));
    ASSERT_EQ(100, trie.get_offset(leaf_tagged_ptr));

    uint8_t* leaf_data = static_cast<uint8_t *>(trie.get_ptr(leaf_tagged_ptr));

    ASSERT_EQ(0, leaf_data[0]);
    ASSERT_EQ(1, leaf_data[1]);
    ASSERT_EQ(2, leaf_data[2]);

    // INTERNAL

    void* internal_tagged_ptr = trie.tag_ptr(bytes, 100, INTERNAL);
    ASSERT_EQ(INTERNAL, trie.get_node_type(internal_tagged_ptr));
    ASSERT_EQ(100, trie.get_offset(internal_tagged_ptr));

    uint8_t* internal_data = static_cast<uint8_t *>(trie.get_ptr(internal_tagged_ptr));

    ASSERT_EQ(0, internal_data[0]);
    ASSERT_EQ(1, internal_data[1]);
    ASSERT_EQ(2, internal_data[2]);

    // COMPRESSED

    void* compressed_tagged_ptr = trie.tag_ptr(bytes, 100, COMPRESSED);
    ASSERT_EQ(COMPRESSED, trie.get_node_type(compressed_tagged_ptr));
    ASSERT_EQ(100, trie.get_offset(compressed_tagged_ptr));

    uint8_t* compressed_data = static_cast<uint8_t *>(trie.get_ptr(compressed_tagged_ptr));

    ASSERT_EQ(0, compressed_data[0]);
    ASSERT_EQ(1, compressed_data[1]);
    ASSERT_EQ(2, compressed_data[2]);

    delete [] bytes;
}

TEST(CVTTest, AddSingleLeaf) {
    CVTrie trie;

    cvt_leaf_t leaf{108};
    ASSERT_TRUE(trie.add("foo", 3, &leaf));

    ASSERT_EQ(&leaf, trie.find("foo", 3));
    ASSERT_EQ(nullptr, trie.find("foooo", 5));
    ASSERT_EQ(nullptr, trie.find("f", 1));
}
 */
