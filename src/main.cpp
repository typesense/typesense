#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <art.h>
#include <unordered_map>
#include "string_utils.h"
#include "crow_all.h"
#include "search_index.h"

using namespace std;

int main() {
    SearchIndex *index = new SearchIndex();

    //std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.txt");
    std::ifstream infile("/Users/kishore/Downloads/hnstories.tsv");

    std::string line;
    uint32_t doc_id = 1;

    while (std::getline(infile, line)) {
        vector<string> parts;
        StringUtils::tokenize(line, parts, "\t", true);
        line = StringUtils::replace_all(line, "\"", "");

        vector<string> tokens;
        StringUtils::tokenize(parts[0], tokens, " ", true);

        if(parts.size() != 2) continue;
        index->add(doc_id, tokens, stoi(parts[1]));
        doc_id++;
    }

    cout << "FINISHED INDEXING!" << endl << flush;

    auto begin = std::chrono::high_resolution_clock::now();
    index->search("thei rserch", 100);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    cout << "Time taken: " << timeMillis << "us" << endl;
    delete index;
    return 0;
}