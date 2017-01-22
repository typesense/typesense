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
#include "collection_manager.h"

using namespace std;

int main(int argc, char* argv[]) {
    system("rm -rf /tmp/typesense-data && mkdir -p /tmp/typesense-data");

    std::vector<field> fields_to_index = {field("title", field_types::STRING)};
    std::vector<std::string> rank_fields = {"points"};
    Store *store = new Store("/tmp/typesense-data");
    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(store);

    Collection *collection = collectionManager.get_collection("collection");
    if(collection == nullptr) {
        collection = collectionManager.create_collection("collection", fields_to_index, rank_fields);
    }

    std::ifstream infile(argv[1]);

    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection->add(json_line);
    }

    infile.close();
    cout << "FINISHED INDEXING!" << endl << flush;

    std::vector<std::string> search_fields = {"title"};

    std::vector<string> queries = {"the", "and", "to", "of", "in"};
    auto counter = 0;
    uint64_t results_total = 0; // to prevent optimizations!

    auto begin = std::chrono::high_resolution_clock::now();

    while(counter < 3000) {
        auto i = counter % 5;
        auto results = collection->search(queries[i], search_fields, 1, 100);
        results_total += results.size();
        counter++;
    }

    long long int timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    cout << "Time taken: " << timeMillis << "ms" << endl;
    cout << "Total: " << results_total << endl;
    return 0;
}