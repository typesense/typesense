#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <art.h>
#include <unordered_map>
#include "string_utils.h"
#include "collection.h"
#include "json.hpp"

using namespace std;

int main() {
    Collection *collection = new Collection();

    std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.jsonl");
    //std::ifstream infile("/Users/kishore/Downloads/hnstories.jsonl");

    std::string jsonline;

    while (std::getline(infile, jsonline)) {
        nlohmann::json document = nlohmann::json::parse(jsonline);
        collection->add(document);
    }

    infile.close();
    cout << "FINISHED INDEXING!" << endl << flush;

    auto begin = std::chrono::high_resolution_clock::now();
    collection->search("plant", 100);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    cout << "Time taken: " << timeMillis << "us" << endl;
    delete collection;
    return 0;
}