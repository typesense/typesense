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
    Collection *index = new Collection();

    //std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.txt");
    std::ifstream infile("/Users/kishore/Downloads/hnstories.tsv");

    std::string line;

    while (std::getline(infile, line)) {
        vector<string> parts;
        StringUtils::tokenize(line, parts, "\t", true);
        line = StringUtils::replace_all(line, "\"", "");

        vector<string> tokens;
        StringUtils::tokenize(parts[0], tokens, " ", true);

        if(parts.size() != 2) continue;
        index->add(tokens, stoi(parts[1]));
    }

    cout << "FINISHED INDEXING!" << endl << flush;

    auto begin = std::chrono::high_resolution_clock::now();
    index->search("thei rserch", 100);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    cout << "Time taken: " << timeMillis << "us" << endl;
    delete index;
    return 0;
}