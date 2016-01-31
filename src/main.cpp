#include <stdlib.h>
#include <iostream>
#include <check.h>
#include <fstream>
#include <chrono>
#include <vector>
#include <time.h>
#include <art.h>
#include "art.h"
#include "topster.h"
#include "forarray.h"
#include "util.h"

using namespace std;

static int test_prefix_cb(void *data, const unsigned char *k, uint32_t k_len, void *val) {
    cout << "#>>>>Key: ";
    printf("%.*s", k_len, k);
    cout << ", ID: " << ((art_values*)val)->ids.at(0) << endl;
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

    Topster<4000> heapArray;

    for(uint32_t i=0; i<hits.size(); i++) {
        heapArray.add(records[hits[i]]);
    }

    std::sort(std::begin(heapArray.data), std::end(heapArray.data));

    long long int timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    for(uint32_t i=0; i<heapArray.size; i++) {
        cout << "Res: " << heapArray.data[i] << endl;
    }

    cout << "Time taken: " << timeMillis << endl;
}

void index_document(art_tree& t, uint32_t doc_id, vector<string> & tokens, uint16_t score) {
    for(auto & token: tokens){
        std::transform(token.begin(), token.end(), token.begin(), ::tolower);
        art_document document;
        document.id = doc_id;
        document.score = score;
        art_insert(&t, (const unsigned char *) token.c_str(), token.length(), &document);
    }
}

int main() {
    art_tree t;
    art_tree_init(&t);

    std::ifstream infile("/Users/kishorenc/others/wreally/search/test/documents.txt");

    std::string line;
    uint32_t num = 1;

    while (std::getline(infile, line)) {
        vector<string> parts;
        tokenize(line, parts, "\t", true);
        vector<string> tokens;
        tokenize(parts[0], tokens, " ", true);
        index_document(t, num, tokens, stoi(parts[1]));
        num++;
    }

    const unsigned char *prefix = (const unsigned char *) "propellants";
    size_t prefix_len = strlen((const char *) prefix);

    auto begin = std::chrono::high_resolution_clock::now();
    art_iter_fuzzy_prefix(&t, prefix, prefix_len, 2, test_prefix_cb, NULL);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();

//    art_iter_prefix(&t, prefix, strlen((const char *) prefix), test_prefix_cb, NULL);
//    art_iter(&t, test_prefix_cb, NULL);

    cout << "Time taken: " << timeMillis << "us" << endl;

    art_tree_destroy(&t);
    return 0;
}