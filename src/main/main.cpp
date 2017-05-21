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
#include <sys/resource.h>
#include "collection.h"
#include "collection_manager.h"

using namespace std;

int main(int argc, char* argv[]) {
    const std::string state_dir_path = "/tmp/typesense-data";
    Store *store = new Store("/tmp/typesense-data");

    CollectionManager & collectionManager = CollectionManager::get_instance();
    collectionManager.init(store);

    std::vector<field> fields_to_index = {
            field("lang", field_types::STRING),
            field("description", field_types::STRING),
            field("topics", field_types::STRING_ARRAY),
            field("stars", field_types::INT32),
            field("repo_name", field_types::STRING),
            field("org", field_types::STRING)
    };

    std::vector<field> facet_fields_index = {
//            field("lang", field_types::STRING),
//            field("org", field_types::STRING),
//            field("topics", field_types::STRING_ARRAY)
    };

    std::vector<field> sort_fields = { field("stars", "INT32")};

    Collection *collection = collectionManager.get_collection("github_top1k");

    if(collection == nullptr) {
        collection = collectionManager.create_collection("github_top1k", fields_to_index, facet_fields_index, sort_fields);
    }

    int j = 0;
    while(j < 1) {
        j++;

        std::ifstream infile(argv[1]);
        std::string json_line;

        cout << "BEGINNING Iteration: " << j << endl << flush;
        auto begin = std::chrono::high_resolution_clock::now();

        while (std::getline(infile, json_line)) {
            nlohmann::json document = nlohmann::json::parse(json_line);
            document["id"] = document["org"].get<std::string>() + ":" + document["repo_name"].get<std::string>();
            collection->add(document.dump());
        }

        infile.close();

        long long int timeMillis =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

        std::cout << "Time taken for insertion: " << timeMillis << "ms" << std::endl;
        begin = std::chrono::high_resolution_clock::now();

        std::ifstream infile2(argv[1]);

        int counter = 0;

        while (std::getline(infile2, json_line)) {
            counter++;
            nlohmann::json document = nlohmann::json::parse(json_line);
            document["id"] = document["org"].get<std::string>() + ":" + document["repo_name"].get<std::string>();
            collection->remove(document["id"]);
            /*if (counter % 100 == 0) {
                std::cout << "Removed " << counter << " so far..." << std::endl;
            }*/
        }

        infile2.close();

        timeMillis =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

        struct rusage r_usage;
        getrusage(RUSAGE_SELF,&r_usage);
        std::cout << "Memory usage: " << r_usage.ru_maxrss << std::endl;
        std::cout << "Time taken for deletion: " << timeMillis << "ms" << std::endl;
    }

    delete collection;
    delete store;
    return 0;
}