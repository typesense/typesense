#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <gtest/gtest.h>
#include <art.h>

#define words_file_path std::string(std::string(ROOT_DIR)+"/build/test_resources/words.txt").c_str()
#define uuid_file_path std::string(std::string(ROOT_DIR)+"/build/test_resources/uuid.txt").c_str()

art_document get_document(uint32_t id) {
    art_document document;
    document.score = id;
    document.id = id;
    document.offsets = new uint32_t[1]{0};
    document.offsets_len = 1;

    return document;
}

TEST(ArtTest, test_art_init_and_destroy) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    ASSERT_TRUE(art_size(&t) == 0);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_insert) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    size_t len;
    char buf[512];
    FILE *f;
    f = fopen(words_file_path, "r");

    uintptr_t line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        art_document document = get_document(line);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)buf, len, &document, 1));
        ASSERT_TRUE(art_size(&t) == line);
        line++;

        delete document.offsets;
    }

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_insert_verylong) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    unsigned char key1[300] = {16,0,0,0,7,10,0,0,0,2,17,10,0,0,0,120,10,0,0,0,120,10,0,
                               0,0,216,10,0,0,0,202,10,0,0,0,194,10,0,0,0,224,10,0,0,0,
                               230,10,0,0,0,210,10,0,0,0,206,10,0,0,0,208,10,0,0,0,232,
                               10,0,0,0,124,10,0,0,0,124,2,16,0,0,0,2,12,185,89,44,213,
                               251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
                               8,18,182,92,236,147,171,101,150,195,112,185,218,108,246,
                               139,164,234,195,58,177,0,8,16,0,0,0,2,12,185,89,44,213,
                               251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
                               8,18,180,93,46,151,9,212,190,95,102,178,217,44,178,235,
                               29,190,218,8,16,0,0,0,2,12,185,89,44,213,251,173,202,
                               211,95,185,89,110,118,251,173,202,199,101,0,8,18,180,93,
                               46,151,9,212,190,95,102,183,219,229,214,59,125,182,71,
                               108,180,220,238,150,91,117,150,201,84,183,128,8,16,0,0,
                               0,2,12,185,89,44,213,251,173,202,211,95,185,89,110,118,
                               251,173,202,199,101,0,8,18,180,93,46,151,9,212,190,95,
                               108,176,217,47,50,219,61,134,207,97,151,88,237,246,208,
                               8,18,255,255,255,219,191,198,134,5,223,212,72,44,208,
                               250,180,14,1,0,0,8, '\0'};
    unsigned char key2[303] = {16,0,0,0,7,10,0,0,0,2,17,10,0,0,0,120,10,0,0,0,120,10,0,
                               0,0,216,10,0,0,0,202,10,0,0,0,194,10,0,0,0,224,10,0,0,0,
                               230,10,0,0,0,210,10,0,0,0,206,10,0,0,0,208,10,0,0,0,232,
                               10,0,0,0,124,10,0,0,0,124,2,16,0,0,0,2,12,185,89,44,213,
                               251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
                               8,18,182,92,236,147,171,101,150,195,112,185,218,108,246,
                               139,164,234,195,58,177,0,8,16,0,0,0,2,12,185,89,44,213,
                               251,173,202,211,95,185,89,110,118,251,173,202,199,101,0,
                               8,18,180,93,46,151,9,212,190,95,102,178,217,44,178,235,
                               29,190,218,8,16,0,0,0,2,12,185,89,44,213,251,173,202,
                               211,95,185,89,110,118,251,173,202,199,101,0,8,18,180,93,
                               46,151,9,212,190,95,102,183,219,229,214,59,125,182,71,
                               108,180,220,238,150,91,117,150,201,84,183,128,8,16,0,0,
                               0,3,12,185,89,44,213,251,133,178,195,105,183,87,237,150,
                               155,165,150,229,97,182,0,8,18,161,91,239,50,10,61,150,
                               223,114,179,217,64,8,12,186,219,172,150,91,53,166,221,
                               101,178,0,8,18,255,255,255,219,191,198,134,5,208,212,72,
                               44,208,250,180,14,1,0,0,8, '\0'};


    art_document doc1 = get_document(1);
    art_document doc2 = get_document(2);

    ASSERT_TRUE(NULL == art_insert(&t, key1, 299, &doc1, 1));
    ASSERT_TRUE(NULL == art_insert(&t, key2, 302, &doc2, 2));
    art_insert(&t, key2, 302, &doc2, 2);
    EXPECT_EQ(art_size(&t), 2);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_insert_search) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen(words_file_path, "r");

    uintptr_t line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        art_document doc = get_document((uint32_t) line);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)buf, len, &doc, 1));
        line++;
    }

    // Seek back to the start
    fseek(f, 0, SEEK_SET);

    // Search for each line
    line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';

        art_leaf* l = (art_leaf *) art_search(&t, (unsigned char*)buf, len);
        EXPECT_EQ(line, l->values->ids.at(0));
        line++;
    }

    // Check the minimum
    art_leaf *l = art_minimum(&t);
    ASSERT_TRUE(l && strcmp((char*)l->key, "A") == 0);

    // Check the maximum
    l = art_maximum(&t);
    ASSERT_TRUE(l && strcmp((char*)l->key, "zythum") == 0);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}


TEST(ArtTest, test_art_insert_delete) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen(words_file_path, "r");

    uintptr_t line = 1, nlines;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        art_document doc = get_document((uint32_t) line);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)buf, len, &doc, 1));
        line++;
    }

    nlines = line - 1;

    // Seek back to the start
    fseek(f, 0, SEEK_SET);

    // Search for each line
    line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';

        // Search first, ensure all entries still
        // visible
        art_leaf* l = (art_leaf *) art_search(&t, (unsigned char*)buf, len);
        EXPECT_EQ(line, l->values->ids.at(0));

        // Delete, should get lineno back
        art_values* values = (art_values*)art_delete(&t, (unsigned char*)buf, len);
        EXPECT_EQ(line, values->ids.at(0));
        free(values);

        // Check the size
        ASSERT_TRUE(art_size(&t) == nlines - line);
        line++;
    }

    // Check the minimum and maximum
    ASSERT_TRUE(!art_minimum(&t));
    ASSERT_TRUE(!art_maximum(&t));
    ASSERT_TRUE(art_size(&t) == 0);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

int iter_cb(void *data, const unsigned char* key, uint32_t key_len, void *val) {
    uint64_t *out = (uint64_t*)data;
    uintptr_t line = ((art_values*)val)->ids.at(0);
    uint64_t mask = (line * (key[0] + key_len));
    out[0]++;
    out[1] ^= mask;
    return 0;
}

TEST(ArtTest, test_art_insert_iter) {
                art_tree t;
        int res = art_tree_init(&t);
        ASSERT_TRUE(res == 0);

        int len;
        char buf[512];
        FILE *f = fopen(words_file_path, "r");

        uint64_t xor_mask = 0;
        uintptr_t line = 1, nlines;
        while (fgets(buf, sizeof buf, f)) {
            len = strlen(buf);
            buf[len-1] = '\0';
            art_document doc = get_document((uint32_t) line);
            ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)buf, len, &doc, 1));

            xor_mask ^= (line * (buf[0] + len));
            line++;
        }
        nlines = line - 1;

        uint64_t out[] = {0, 0};
        ASSERT_TRUE(art_iter(&t, iter_cb, &out) == 0);

        ASSERT_TRUE(out[0] == nlines);
        ASSERT_TRUE(out[1] == xor_mask);

        res = art_tree_destroy(&t);
        ASSERT_TRUE(res == 0);
}


typedef struct {
    int count;
    int max_count;
    const char **expected;
} prefix_data;

static int test_prefix_cb(void *data, const unsigned char *k, uint32_t k_len, void *val) {
    prefix_data *p = (prefix_data*)data;
    assert(p->count < p->max_count);
    assert(memcmp(k, p->expected[p->count], k_len) == 0);
    p->count++;
    return 0;
}

TEST(ArtTest, test_art_iter_prefix) {
        art_tree t;
        int res = art_tree_init(&t);
        ASSERT_TRUE(res == 0);

        const char *s = "api.foo.bar";
        art_document doc = get_document((uint32_t) 1);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

        s = "api.foo.baz";
        doc = get_document((uint32_t) 2);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

        s = "api.foe.fum";
        doc = get_document((uint32_t) 3);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

        s = "abc.123.456";
        doc = get_document((uint32_t) 4);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

        s = "api.foo";
        doc = get_document((uint32_t) 5);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

        s = "api";
        doc = get_document((uint32_t) 6);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

        // Iterate over api
        const char *expected[] = {"api", "api.foe.fum", "api.foo", "api.foo.bar", "api.foo.baz"};
        prefix_data p = { 0, 5, expected };
        ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"api", 3, test_prefix_cb, &p));
        ASSERT_TRUE(p.count == p.max_count);

        // Iterate over 'a'
        const char *expected2[] = {"abc.123.456", "api", "api.foe.fum", "api.foo", "api.foo.bar", "api.foo.baz"};
        prefix_data p2 = { 0, 6, expected2 };
        ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"a", 1, test_prefix_cb, &p2));
        ASSERT_TRUE(p2.count == p2.max_count);

        // Check a failed iteration
        prefix_data p3 = { 0, 0, NULL };
        ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"b", 1, test_prefix_cb, &p3));
        ASSERT_TRUE(p3.count == 0);

        // Iterate over api.
        const char *expected4[] = {"api.foe.fum", "api.foo", "api.foo.bar", "api.foo.baz"};
        prefix_data p4 = { 0, 4, expected4 };
        ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"api.", 4, test_prefix_cb, &p4));
        ASSERT_TRUE(p4.count == p4.max_count);

        // Iterate over api.foo.ba
        const char *expected5[] = {"api.foo.bar"};
        prefix_data p5 = { 0, 1, expected5 };
        ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"api.foo.bar", 11, test_prefix_cb, &p5));
        ASSERT_TRUE(p5.count == p5.max_count);

        // Check a failed iteration on api.end
        prefix_data p6 = { 0, 0, NULL };
        ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"api.end", 7, test_prefix_cb, &p6));
        ASSERT_TRUE(p6.count == 0);

        // Iterate over empty prefix
        prefix_data p7 = { 0, 6, expected2 };
        ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"", 0, test_prefix_cb, &p7));
        ASSERT_TRUE(p7.count == p7.max_count);

        res = art_tree_destroy(&t);
        ASSERT_TRUE(res == 0);
}


TEST(ArtTest, test_art_long_prefix) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    uintptr_t id;
    const char *s;

    s = "this:key:has:a:long:prefix:3";
    id = 3;
    art_document doc = get_document((uint32_t) id);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

    s = "this:key:has:a:long:common:prefix:2";
    id = 2;
    doc = get_document((uint32_t) id);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

    s = "this:key:has:a:long:common:prefix:1";
    id = 1;
    doc = get_document((uint32_t) id);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)s, strlen(s)+1, &doc, 1));

    // Search for the keys
    s = "this:key:has:a:long:common:prefix:1";
    EXPECT_EQ(1, (((art_leaf *)art_search(&t, (unsigned char*)s, strlen(s)+1))->values->ids.at(0)));

    s = "this:key:has:a:long:common:prefix:2";
    EXPECT_EQ(2, (((art_leaf *)art_search(&t, (unsigned char*)s, strlen(s)+1))->values->ids.at(0)));

    s = "this:key:has:a:long:prefix:3";
    EXPECT_EQ(3, (((art_leaf *)art_search(&t, (unsigned char*)s, strlen(s)+1))->values->ids.at(0)));

    const char *expected[] = {
            "this:key:has:a:long:common:prefix:1",
            "this:key:has:a:long:common:prefix:2",
            "this:key:has:a:long:prefix:3",
    };
    prefix_data p = { 0, 3, expected };
    ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)"this:key:has", 12, test_prefix_cb, &p));
    ASSERT_TRUE(p.count == p.max_count);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_insert_search_uuid) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen(uuid_file_path, "r");

    uintptr_t line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        art_document doc = get_document((uint32_t) line);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)buf, len, &doc, 1));
        line++;
    }

    // Seek back to the start
    fseek(f, 0, SEEK_SET);

    // Search for each line
    line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';

        uintptr_t id = ((art_leaf*)art_search(&t, (unsigned char*)buf, len))->values->ids.at(0);
        ASSERT_TRUE(line == id);
        line++;
    }

    // Check the minimum
    art_leaf *l = art_minimum(&t);
    ASSERT_TRUE(l && strcmp((char*)l->key, "00026bda-e0ea-4cda-8245-522764e9f325") == 0);

    // Check the maximum
    l = art_maximum(&t);
    ASSERT_TRUE(l && strcmp((char*)l->key, "ffffcb46-a92e-4822-82af-a7190f9c1ec5") == 0);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_max_prefix_len_scan_prefix) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    const char* key1 = "foobarbaz1-test1-foo";
    art_document doc = get_document((uint32_t) 1);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key1, strlen(key1)+1, &doc, 1));

    const char *key2 = "foobarbaz1-test1-bar";
    doc = get_document((uint32_t) 2);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key2, strlen(key2)+1, &doc, 1));

    const char *key3 = "foobarbaz1-test2-foo";
    doc = get_document((uint32_t) 3);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key3, strlen(key3)+1, &doc, 2));

    ASSERT_TRUE(art_size(&t) == 3);

    // Iterate over api
    const char *expected[] = {key2, key1};
    prefix_data p = { 0, 2, expected };
    const char *prefix = "foobarbaz1-test1";
    ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)prefix, strlen(prefix), test_prefix_cb, &p));
    ASSERT_TRUE(p.count == p.max_count);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_prefix_iter_out_of_bounds) {
    // Regression: ensures that `assert(depth < key_len);` is not invoked
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    const char* key1 = "foobarbaz1-long-test1-foo";
    art_document doc = get_document((uint32_t) 1);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key1, strlen(key1)+1, &doc, 1));

    const char *key2 = "foobarbaz1-long-test1-bar";
    doc = get_document((uint32_t) 2);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key2, strlen(key2)+1, &doc, 1));

    const char *key3 = "foobarbaz1-long-test2-foo";
    doc = get_document((uint32_t) 3);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key3, strlen(key3)+1, &doc, 2));

    ASSERT_TRUE(art_size(&t) == 3);

    // Iterate over api
    const char *expected[] = {key2, key1};
    prefix_data p = { 0, 0, expected };
    const char *prefix = "f2oobar";
    ASSERT_TRUE(!art_iter_prefix(&t, (unsigned char*)prefix, strlen(prefix), test_prefix_cb, &p));
    ASSERT_TRUE(p.count == p.max_count);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_search_out_of_bounds) {
    // Regression: ensures that `assert(depth < key_len);` is not invoked
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    const char* key1 = "foobarbaz1-long-test1-foo";
    art_document doc = get_document((uint32_t) 1);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key1, strlen(key1)+1, &doc, 1));

    const char *key2 = "foobarbaz1-long-test1-bar";
    doc = get_document((uint32_t) 2);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key2, strlen(key2)+1, &doc, 1));

    const char *key3 = "foobarbaz1-long-test2-foo";
    doc = get_document((uint32_t) 3);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key3, strlen(key3)+1, &doc, 2));

    ASSERT_TRUE(art_size(&t) == 3);

    // Search for a non-existing key
    const char *prefix = "foobarbaz1-long-";
    art_leaf* l = (art_leaf *) art_search(&t, (const unsigned char *) prefix, strlen(prefix));
    ASSERT_EQ(NULL, l);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_delete_out_of_bounds) {
    // Regression: ensures that `assert(depth < key_len);` is not invoked
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    const char* key1 = "foobarbaz1-long-test1-foo";
    art_document doc = get_document((uint32_t) 1);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key1, strlen(key1)+1, &doc, 1));

    const char *key2 = "foobarbaz1-long-test1-bar";
    doc = get_document((uint32_t) 2);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key2, strlen(key2)+1, &doc, 1));

    const char *key3 = "foobarbaz1-long-test2-foo";
    doc = get_document((uint32_t) 3);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key3, strlen(key3)+1, &doc, 2));

    ASSERT_TRUE(art_size(&t) == 3);

    // Try to delete a non-existing key
    const char *prefix = "foobarbaz1-long-";
    art_values* values = (art_values *) art_delete(&t, (const unsigned char *) prefix, strlen(prefix));
    ASSERT_EQ(NULL, values);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_insert_multiple_ids_for_same_token) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    const char* key1 = "implement";
    art_document doc = get_document((uint32_t) 1);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key1, strlen(key1)+1, &doc, 1));

    doc = get_document((uint32_t) 2);
    art_values* value = (art_values*) art_insert(&t, (unsigned char*)key1, strlen(key1) + 1, &doc, 2);
    ASSERT_TRUE(value != NULL);

    doc = get_document((uint32_t) 3);
    ASSERT_TRUE(value == (art_values*) art_insert(&t, (unsigned char*)key1, strlen(key1)+1, &doc, 3));

    ASSERT_TRUE(art_size(&t) == 1);
    ASSERT_EQ(value->ids.getLength(), 3);
    ASSERT_EQ(value->ids.at(0), 1);
    ASSERT_EQ(value->ids.at(1), 2);
    ASSERT_EQ(value->ids.at(2), 3);

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_fuzzy_search_single_leaf) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    const char* implement_key = "implement";
    art_document doc = get_document((uint32_t) 1);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)implement_key, strlen(implement_key)+1, &doc, 1));

    art_leaf* l = (art_leaf *) art_search(&t, (const unsigned char *)implement_key, strlen(implement_key)+1);
    EXPECT_EQ(1, l->values->ids.at(0));

    std::vector<art_leaf*> leaves;
    art_fuzzy_search(&t, (const unsigned char *) implement_key, strlen(implement_key) + 1, 0, 0, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());

    const char* implement_key_typo1 = "implment";
    const char* implement_key_typo2 = "implwnent";

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) implement_key_typo1, strlen(implement_key_typo1) + 1, 0, 0, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(0, leaves.size());

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) implement_key_typo1, strlen(implement_key_typo1) + 1, 0, 1, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) implement_key_typo2, strlen(implement_key_typo2) + 1, 0, 2, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_fuzzy_search_single_leaf_prefix) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    const char* key = "application";
    art_document doc = get_document((uint32_t) 1);
    ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)key, strlen(key)+1, &doc, 1));

    art_leaf* l = (art_leaf *) art_search(&t, (const unsigned char *)key, strlen(key)+1);
    EXPECT_EQ(1, l->values->ids.at(0));

    std::vector<art_leaf*> leaves;
    art_fuzzy_search(&t, (const unsigned char *) "aplication", strlen(key), 0, 1, 10, FREQUENCY, true, leaves);
    ASSERT_EQ(1, leaves.size());

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_art_fuzzy_search) {
    art_tree t;
    int res = art_tree_init(&t);
    ASSERT_TRUE(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen(words_file_path, "r");

    uintptr_t line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        art_document doc = get_document((uint32_t) line);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)buf, len, &doc, 1));
        line++;
    }

    std::vector<art_leaf*> leaves;

    // transpose
    art_fuzzy_search(&t, (const unsigned char *) "zymosthneic", strlen("zymosthneic") + 1, 0, 1, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());
    ASSERT_STREQ("zymosthenic", (const char *)leaves.at(0)->key);

    // transpose + missing
    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "dacrcyystlgia", strlen("dacrcyystlgia") + 1, 0, 2, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());
    ASSERT_STREQ("dacrycystalgia", (const char *)leaves.at(0)->key);

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "dacrcyystlgia", strlen("dacrcyystlgia") + 1, 1, 2, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());
    ASSERT_STREQ("dacrycystalgia", (const char *)leaves.at(0)->key);

    // missing char
    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "gaberlunze", strlen("gaberlunze") + 1, 0, 1, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());
    ASSERT_STREQ("gaberlunzie", (const char *)leaves.at(0)->key);

    // extra char
    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "higghliving", strlen("higghliving") + 1, 0, 1, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());
    ASSERT_STREQ("highliving", (const char *)leaves.at(0)->key);

    // substituted char
    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "eacemiferous", strlen("eacemiferous") + 1, 0, 1, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());
    ASSERT_STREQ("racemiferous", (const char *)leaves.at(0)->key);

    // missing char + extra char
    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "Sarbruckken", strlen("Sarbruckken") + 1, 0, 2, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(1, leaves.size());
    ASSERT_STREQ("Saarbrucken", (const char *)leaves.at(0)->key);

    // multiple matching results
    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "hown", strlen("hown") + 1, 0, 1, 10, FREQUENCY, false, leaves);
    ASSERT_EQ(10, leaves.size());

    std::vector<const char*> words = {"town", "sown", "shown", "own", "mown", "lown", "howl", "howk", "howe", "how"};
    for(size_t leaf_index = 0; leaf_index < leaves.size(); leaf_index++) {
        ASSERT_STREQ(words.at(leaf_index), (const char *)leaves.at(leaf_index)->key);
    }

    // fuzzy prefix search
    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "lionhear", strlen("lionhear"), 0, 0, 10, FREQUENCY, true, leaves);
    ASSERT_EQ(3, leaves.size());

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "lineage", strlen("lineage"), 0, 0, 10, FREQUENCY, true, leaves);
    ASSERT_EQ(2, leaves.size());

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "liq", strlen("liq"), 0, 0, 50, FREQUENCY, true, leaves);
    ASSERT_EQ(39, leaves.size());

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "antitraditian", strlen("antitraditian"), 0, 1, 10, FREQUENCY, true, leaves);
    ASSERT_EQ(1, leaves.size());

    leaves.clear();
    art_fuzzy_search(&t, (const unsigned char *) "antisocao", strlen("antisocao"), 0, 2, 10, FREQUENCY, true, leaves);
    ASSERT_EQ(8, leaves.size());

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_encode_int32) {
    unsigned char chars[8];

    // 175 => 0000,0000,0000,0000,0000,0000,1010,1111
    unsigned char chars_175[8] = {0, 0, 0, 0, 0, 0, 10, 15};
    encode_int32(175, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_175[i], chars[i]);
    }

    // 0 => 0000,0000,0000,0000,0000,0000,0000,0000
    unsigned char chars_0[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    encode_int32(0, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_0[i], chars[i]);
    }

    // 255 => 0000,0000,0000,0000,0000,0000,1111,1111
    unsigned char chars_255[8] = {0, 0, 0, 0, 0, 0, 15, 15};
    encode_int32(255, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_255[i], chars[i]);
    }

    // 4531 => 0000,0000,0000,0000,0001,0001,1011,0011
    unsigned char chars_4531[8] = {0, 0, 0, 0, 1, 1, 11, 3};
    encode_int32(4531, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_4531[i], chars[i]);
    }

    // 1200000 => 0000,0000,0001,0010,0100,1111,1000,0000
    unsigned char chars_1M[8] = {0, 0, 1, 2, 4, 15, 8, 0};
    encode_int32(1200000, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_1M[i], chars[i]);
    }

    unsigned char chars_neg_4531[8] = {15, 15, 15, 15, 14, 14, 4, 13};
    encode_int32(-4531, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_neg_4531[i], chars[i]);
    }
}

TEST(ArtTest, test_int32_overlap) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    std::vector<const art_leaf *> results;

    std::vector<std::vector<uint32_t>> values = {{2014, 2015, 2016}, {2015, 2016}, {2016},
                                                 {1981, 1985}, {1999, 2000, 2001, 2002}};

    for(uint32_t i = 0; i < values.size(); i++) {
        for(size_t j = 0; j < values[i].size(); j++) {
            encode_int32(values[i][j], chars);
            doc.id = i;
            art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1);
        }
    }

    int res = art_int32_search(&t, 2002, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(3, results.size());

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_int32_range_hundreds) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    std::vector<const art_leaf*> results;

    for(uint32_t i = 100; i < 110; i++) {
        encode_int32(i, chars);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1));
    }

    encode_int32(106, chars);

    int res = art_int32_search(&t, 106, EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_int32_search(&t, 106, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(4, results.size());
    results.clear();

    res = art_int32_search(&t, 106, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(3, results.size());
    results.clear();

    res = art_int32_search(&t, 106, LESS_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(7, results.size());
    results.clear();

    res = art_int32_search(&t, 106, LESS_THAN, results);
    ASSERT_TRUE(res == 0);

    ASSERT_EQ(6, results.size());

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_int32_duplicates) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    for(size_t i = 0; i < 10000; i++) {
        doc.id = i;
        int value = 1900 + (rand() % static_cast<int>(2018 - 1900 + 1));
        encode_int32(value, chars);
        art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1);
    }

    std::vector<const art_leaf*> results;

    int res = art_int32_search(&t, 0, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    size_t counter = 0;

    for(auto res: results) {
        counter += res->values->ids.getLength();
    }

    ASSERT_EQ(10000, counter);
    results.clear();
}

TEST(ArtTest, test_int32_negative) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    for(int32_t i = -100; i < 0; i++) {
        encode_int32(i, chars);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1));
    }

    encode_int32(-99, chars);

    std::vector<const art_leaf*> results;

    int res = art_int32_search(&t, -99, EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_int32_search(&t, -90, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(90, results.size());
    results.clear();

    res = art_int32_search(&t, -90, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(89, results.size());
    results.clear();

    res = art_int32_search(&t, -99, LESS_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(2, results.size());
    results.clear();

    res = art_int32_search(&t, -99, LESS_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_int32_search(&t, -100, LESS_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_tree_destroy(&t);
    ASSERT_TRUE(res == 0);
}

TEST(ArtTest, test_int32_million) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    for(uint32_t i = 0; i < 1000000; i++) {
        encode_int32(i, chars);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1));
    }

    encode_int32(5, chars);
    std::vector<const art_leaf*> results;

    // ==
    for(uint32_t i = 0; i < 6; i++) {
        results.clear();
        art_int32_search(&t, (uint32_t) pow(10, i), EQUALS, results);
        ASSERT_EQ(1, results.size());

        results.clear();
        art_int32_search(&t, (uint32_t) (pow(10, i) + 7), EQUALS, results);
        ASSERT_EQ(1, results.size());
    }

    results.clear();
    art_int32_search(&t, 1000000 - 1, EQUALS, results);
    ASSERT_EQ(1, results.size());

    // >=
    results.clear();
    art_int32_search(&t, 1000000 - 5, GREATER_THAN_EQUALS, results);
    ASSERT_EQ(5, results.size());

    results.clear();
    art_int32_search(&t, 1000000 - 5, GREATER_THAN, results);
    ASSERT_EQ(4, results.size());

    results.clear();
    art_int32_search(&t, 1000000 - 1, GREATER_THAN_EQUALS, results);
    ASSERT_EQ(1, results.size());

    results.clear();
    art_int32_search(&t, 1000000, GREATER_THAN_EQUALS, results);
    ASSERT_EQ(0, results.size());

    results.clear();
    art_int32_search(&t, 5, GREATER_THAN_EQUALS, results);
    ASSERT_EQ(1000000-5, results.size());

    // <=
    results.clear();
    art_int32_search(&t, 1000000 - 5, LESS_THAN_EQUALS, results);
    ASSERT_EQ(1000000-5+1, results.size());

    results.clear();
    art_int32_search(&t, 1000000 - 1, LESS_THAN_EQUALS, results);
    ASSERT_EQ(1000000, results.size());

    results.clear();
    art_int32_search(&t, 1000000 - 1, LESS_THAN, results);
    ASSERT_EQ(1000000-1, results.size());

    results.clear();
    art_int32_search(&t, 1000000, LESS_THAN_EQUALS, results);
    ASSERT_EQ(1000000, results.size());

    results.clear();
    art_int32_search(&t, 5, LESS_THAN_EQUALS, results);
    ASSERT_EQ(5+1, results.size());

    results.clear();
    art_int32_search(&t, 5, LESS_THAN, results);
    ASSERT_EQ(5, results.size());
}

TEST(ArtTest, test_int_range_byte_boundary) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);

    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    for(uint32_t i = 200; i < 300; i++) {
        encode_int32(i, chars);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1));
    }

    encode_int32(255, chars);
    std::vector<const art_leaf*> results;

    results.clear();
    art_int32_search(&t, 255, GREATER_THAN_EQUALS, results);
    ASSERT_EQ(45, results.size());

    results.clear();
    art_int32_search(&t, 255, GREATER_THAN, results);
    ASSERT_EQ(44, results.size());
}

TEST(ArtTest, test_encode_int64) {
    unsigned char chars[8];

    unsigned char chars_175[8] = {0, 0, 0, 0, 0, 0, 0, 175};
    encode_int64(175, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_175[i], chars[i]);
    }

    unsigned char chars_neg_175[8] = {255, 255, 255, 255, 255, 255, 255, 81};
    encode_int64(-175, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_neg_175[i], chars[i]);
    }

    unsigned char chars_100K[8] = {0, 0, 0, 0, 0, 1, 134, 160};
    encode_int64(100*1000, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_100K[i], chars[i]);
    }

    unsigned char chars_large_num[8] = {0, 0, 0, 0, 128, 0, 0, 199};
    int64_t large_num = (int64_t)(std::numeric_limits<std::int32_t>::max()) + 200;
    encode_int64(large_num, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_large_num[i], chars[i]);
    }

    unsigned char chars_large_neg_num[8] = {255, 255, 255, 255, 127, 255, 255, 57};
    encode_int64(-1 * large_num, chars);
    for(uint32_t i = 0; i < 8; i++) {
        ASSERT_EQ(chars_large_neg_num[i], chars[i]);
    }
}

TEST(ArtTest, test_search_int64) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    const uint64_t lmax = std::numeric_limits<std::int32_t>::max();

    for(uint64_t i = lmax; i < lmax+100; i++) {
        encode_int64(i, chars);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1));
    }

    std::vector<const art_leaf*> results;

    int res = art_int64_search(&t, lmax, EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_int64_search(&t, lmax, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(100, results.size());
    results.clear();

    res = art_int64_search(&t, lmax, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(99, results.size());
    results.clear();

    res = art_int64_search(&t, lmax+50, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(49, results.size());
    results.clear();

    res = art_int64_search(&t, lmax+50, LESS_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(50, results.size());
    results.clear();
}

TEST(ArtTest, test_search_negative_int64) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    const int64_t lmax = -1 * std::numeric_limits<std::int32_t>::max();

    for(int64_t i = lmax-100; i < lmax; i++) {
        encode_int64(i, chars);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars, CHAR_LEN, &doc, 1));
    }

    std::vector<const art_leaf*> results;

    int res = art_int64_search(&t, lmax-1, EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_int64_search(&t, lmax-1, LESS_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(100, results.size());
    results.clear();

    res = art_int64_search(&t, lmax-50, LESS_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(50, results.size());
    results.clear();

    res = art_int64_search(&t, lmax-50, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(49, results.size());
    results.clear();

    res = art_int64_search(&t, lmax-50, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(50, results.size());
    results.clear();
}

TEST(ArtTest, test_int32_array) {
    art_tree t;
    art_tree_init(&t);

    art_document doc = get_document(1);
    const int CHAR_LEN = 8;
    unsigned char chars[CHAR_LEN];

    std::vector<const art_leaf *> results;

    std::vector<std::vector<uint32_t>> values = {{2014, 2015, 2016},
                                                 {2015, 2016},
                                                 {2016},
                                                 {1981, 1985},
                                                 {1999, 2000, 2001, 2002}};

    for (uint32_t i = 0; i < values.size(); i++) {
        for (size_t j = 0; j < values[i].size(); j++) {
            encode_int32(values[i][j], chars);
            doc.id = i;
            art_insert(&t, (unsigned char *) chars, CHAR_LEN, &doc, 1);
        }
    }

    int res = art_int32_search(&t, 2002, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(3, results.size());
}

TEST(ArtTest, test_encode_float_positive) {
    art_tree t;
    art_tree_init(&t);

    float floats[6] = {0.0, 0.1044, 1.004, 1.99, 10.5678, 100.33};
    const int CHAR_LEN = 8;

    for(size_t i = 0; i < 6; i++) {
        unsigned char chars0[CHAR_LEN];
        encode_float(floats[i], chars0);
        art_document doc = get_document(i);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars0, CHAR_LEN, &doc, 1));
    }

    std::vector<const art_leaf*> results;

    int res = art_float_search(&t, 0.0, EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_float_search(&t, 0.0, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(5, results.size());
    results.clear();

    res = art_float_search(&t, 10.5678, LESS_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(4, results.size());
    results.clear();

    res = art_float_search(&t, 10.5678, LESS_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(5, results.size());
    results.clear();

    res = art_float_search(&t, 10.5678, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_float_search(&t, 10.4, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(2, results.size());
    results.clear();

    res = art_float_search(&t, 10.5678, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(2, results.size());
    results.clear();

    res = art_float_search(&t, 10, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(2, results.size());
    results.clear();
}

TEST(ArtTest, test_encode_float_positive_negative) {
    art_tree t;
    art_tree_init(&t);

    float floats[6] = {-24.1033, -2.561, 0.0, 1.99, 10.5678, 100.33};
    const int CHAR_LEN = 8;

    for(size_t i = 0; i < 6; i++) {
        unsigned char chars0[CHAR_LEN];
        encode_float(floats[i], chars0);
        art_document doc = get_document(i);
        ASSERT_TRUE(NULL == art_insert(&t, (unsigned char*)chars0, CHAR_LEN, &doc, 1));
    }

    std::vector<const art_leaf*> results;

    int res = art_float_search(&t, -24.1033, EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(1, results.size());
    results.clear();

    res = art_float_search(&t, 0.0, LESS_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(2, results.size());
    results.clear();

    res = art_float_search(&t, 0.0, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(3, results.size());
    results.clear();

    res = art_float_search(&t,  -2.561, LESS_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(2, results.size());
    results.clear();

    res = art_float_search(&t, -2.561, GREATER_THAN, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(4, results.size());
    results.clear();

    res = art_float_search(&t, -24.1033, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(6, results.size());
    results.clear();

    res = art_float_search(&t, -24, GREATER_THAN_EQUALS, results);
    ASSERT_TRUE(res == 0);
    ASSERT_EQ(5, results.size());
    results.clear();
}