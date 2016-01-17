#include <iostream>
#include <check.h>
#include "art.h"
#include <fstream>
#include <art.h>
#include "heap_array.h"
#include <chrono>

#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */

using namespace std;

static int test_prefix_cb(void *data, const unsigned char *k, uint32_t k_len, void *val) {
    cout << ">>>>Key: ";
    printf("%.*s", k_len, k);
    cout << ", Value: " << (uintptr_t) val << endl;
    return 0;
}

void benchmark_heap_array() {
    srand (time(NULL));

    vector<uint32_t> records;

    for(uint32_t i=0; i<10000000; i++) {
        records.push_back((const unsigned int &) rand());
    }

    vector<uint32_t> hits;

    for(uint32_t i=0; i<records.size(); i++) {
        if(i%10 == 0) {
            hits.push_back(i);
        }
    }

    auto begin = std::chrono::high_resolution_clock::now();

    HeapArray heapArray;

    for(uint32_t i=0; i<hits.size(); i++) {
        heapArray.add(records[hits[i]]);
    }

    std::sort(std::begin(heapArray.data), std::end(heapArray.data));

    long long int timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    for(uint32_t i=0; i<heapArray.maxSize; i++) {
        cout << "Res: " << heapArray.data[i] << endl;
    }

    cout << "Time taken: " << timeMillis << endl;
}

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

    const unsigned char *prefix = (const unsigned char *) "responsibe";

    auto begin = std::chrono::high_resolution_clock::now();

    art_iter_fuzzy_prefix(&t, prefix, strlen((const char *) prefix), 1, test_prefix_cb, NULL);

    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();

//    art_iter_prefix(&t, prefix, strlen((const char *) prefix), test_prefix_cb, NULL);

//    art_iter(&t, test_prefix_cb, NULL);

    cout << "Time taken: " << timeMillis << "us" << endl;

    art_tree_destroy(&t);
    return 0;
}