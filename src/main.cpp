#include <iostream>
#include <check.h>
#include "art.h"
#include <fstream>

using namespace std;

static int test_prefix_cb(void *data, const unsigned char *k, uint32_t k_len, void *val) {
    cout << ">>>>Key: ";
    printf("%.*s", k_len, k);
    cout << ", Value: " << (uintptr_t) val << endl;
    return 0;
}

typedef struct {
    uint16_t max_score;
    uint8_t type;
    uint8_t num_children;
    uint32_t partial_len;
    unsigned char partial[MAX_PREFIX_LEN];
} X;


int main() {
    cout << "Running demo...\n\n";
    //cout << "SIZE OF: " << sizeof(X) << endl;

    art_tree t;
    art_tree_init(&t);

    std::ifstream infile("/usr/share/dict/words");

    std::string line;
    uintptr_t num = 1;

    while (std::getline(infile, line)) {
        //cout << "Line: " << line << ", number = " << line << endl;
        art_insert(&t, (const unsigned char *) line.c_str(), line.length(), line.length(), (void*)num);
        num++;
    }

    const unsigned char *prefix = (const unsigned char *) "amaz";
    art_iter_fuzzy_prefix(&t, prefix, strlen((const char *) prefix), 0, test_prefix_cb, NULL);
//    art_iter_prefix(&t, prefix, strlen((const char *) prefix), test_prefix_cb, NULL);

//    art_iter(&t, test_prefix_cb, NULL);

    art_tree_destroy(&t);
    return 0;
}