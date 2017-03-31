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

void find_indices(const uint32_t *result_ids, int low, int high, std::vector<uint32_t> & results) {
    if(high >= low) {
        size_t pivot = (low + high) / 2;
        //std::cout << pivot << std::endl;
        results.at(pivot) = result_ids[pivot];
        find_indices(result_ids, low, pivot-1, results);
        find_indices(result_ids, pivot+1, high, results);
    }
}

int main(int argc, char* argv[]) {
    std::vector<uint32_t> results(3);
    uint32_t *result_ids = new uint32_t[3];
    /*for(auto i = 0; i < 100; i++) {
        result_ids[i] = i;
    }*/
    result_ids[0] = 6;
    result_ids[1] = 19;
    result_ids[2] = 21;

    find_indices(result_ids, 0, 2, results);
    //std::sort(results.begin(), results.end());
    for(auto i : results) {
        std::cout << i << std::endl;
    }

    return 0;


    const std::string state_dir_path = "/tmp/typesense-data";

    std::vector<field> fields_to_index = {field("title", field_types::STRING)};
    std::vector<std::string> rank_fields = {"points"};
    Store *store = new Store("/tmp/typesense-data");

    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(store);

    Collection *collection = collectionManager.get_collection("collection");
    if(collection == nullptr) {
        collection = collectionManager.create_collection("collection", fields_to_index, {}, rank_fields);
        std::ifstream infile(std::string(ROOT_DIR)+"test/documents.jsonl");
        //std::ifstream infile(argv[1]);

        std::string json_line;

        while (std::getline(infile, json_line)) {
            collection->add(json_line);
        }

        infile.close();
        cout << "FINISHED INDEXING!" << endl << flush;
    }

    //collection->remove("foo");

    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::string> search_fields = {"title"};
    collection->search("the", search_fields, "", {}, {"points"}, 1, 100, MAX_SCORE, 0);
    long long int timeMillis =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    cout << "Time taken: " << timeMillis << "us" << endl;
    return 0;
}