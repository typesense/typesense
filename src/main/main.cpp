#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <chrono>
#include <art.h>
#include <unordered_map>
#include <queue>
#include "string_utils.h"
#include "collection.h"

using namespace std;

int main() {
    std::array<int, 10> s = {5, 7, 4, 2, 8, 6, 1, 9, 0, 3};
    std::sort(s.begin(), s.end(), [](int a, int b) {
        return a > b;
    });
    for (auto a : s) {
        std::cout << a << " ";
    }

    std::cout << "\n\n\n";

    auto cmp = [](int a, int b) { return a > b; };
    std::priority_queue<int, std::vector<int>, decltype(cmp)> q(cmp);

    for(int n : {1,8,5,6,3,4,0,9,7,2})
        q.push(n);

    while(!q.empty()) {
        std::cout << q.top() << " ";
        q.pop();
    }
    std::cout << '\n';

    return 0;

    std::vector<field> fields = {field("title", field_type::STRING)};
    std::vector<std::string> rank_fields = {"points"};
    Collection *collection = new Collection("/tmp/typesense-data", "collection", fields, rank_fields);

    std::ifstream infile("/Users/kishore/others/wreally/typesense/test/documents.jsonl");
    //std::ifstream infile("/Users/kishore/Downloads/hnstories.jsonl");

    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection->add(json_line);
    }

    infile.close();
    cout << "FINISHED INDEXING!" << endl << flush;

    collection->remove("foo");

    auto begin = std::chrono::high_resolution_clock::now();
    collection->search("the", 1, 100);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    cout << "Time taken: " << timeMillis << "us" << endl;
    delete collection;
    return 0;
}