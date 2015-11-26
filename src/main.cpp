#include <iostream>
#include <check.h>
#include "art.h"
#include <fstream>

using namespace std;

static int test_prefix_cb(void *data, const unsigned char *k, uint32_t k_len, void *val) {
    cout << ">>>>Key: " << k << ", Value: " << (uintptr_t) val << endl;
    return 0;
}


int main() {
    cout << "Running demo...\n\n";

    art_tree t;
    art_tree_init(&t);

    std::ifstream infile("/Users/kishorenc/others/wreally/search/test/words.txt");

    std::string line;
    uintptr_t num = 1;

    while (std::getline(infile, line)) {
        //cout << "Line: " << line << ", number = " << line << endl;
        art_insert(&t, (const unsigned char *) line.c_str(), line.length(), (void*)num);
        num++;
    }

    const unsigned char *prefix = (const unsigned char *) "hawshan";
    art_iter_fuzzy_prefix(&t, prefix, strlen((const char *) prefix), 2, test_prefix_cb, NULL);
//    art_iter_prefix(&t, prefix, strlen((const char *) prefix), test_prefix_cb, NULL);

//    art_iter(&t, test_prefix_cb, NULL);

    art_tree_destroy(&t);
    return 0;
}