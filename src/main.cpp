#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <art.h>
#include <unordered_map>
#include "string_utils.h"
#include "collection.h"

using namespace std;

int main() {
    Collection *collection = new Collection("/tmp/typesense-data");

    std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.jsonl");
    //std::ifstream infile("/Users/kishore/Downloads/hnstories.jsonl");

    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection->add(json_line);
    }

    infile.close();
    cout << "FINISHED INDEXING!" << endl << flush;

    auto begin = std::chrono::high_resolution_clock::now();
    collection->search("platn growing", 1, 100);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    cout << "Time taken: " << timeMillis << "us" << endl;
    delete collection;
    return 0;
}