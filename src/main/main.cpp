#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <chrono>
#include <sys/resource.h>
#include "collection.h"
#include "collection_manager.h"

using namespace std;

int main(int argc, char* argv[]) {
    const std::string state_dir_path = "/tmp/typesense-data";
    system("rm -rf /tmp/typesense-data && mkdir -p /tmp/typesense-data");

    Store *store = new Store(state_dir_path);

    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> exit;
    collectionManager.init(store, 4, "abcd", exit);
    collectionManager.load(100, 10000);

    std::vector<field> fields_to_index = {
            field("lang", field_types::STRING, true),
            field("description", field_types::STRING, false),
            field("topics", field_types::STRING_ARRAY, true),
            field("stars", field_types::INT32, false),
            field("repo_name", field_types::STRING, false),
            field("org", field_types::STRING, true)
    };

    Collection *collection = collectionManager.get_collection("github_top1k").get();
    if(collection == nullptr) {
        collection = collectionManager.create_collection("github_top1k", 4, fields_to_index, "stars").get();
    }

    int j = 0;
    while(j < 1000) {
        j++;

        std::ifstream infile(argv[1]);
        std::string json_line;

        std::cout << "BEGINNING Iteration: " << j << std::endl;
        auto begin = std::chrono::high_resolution_clock::now();
        int doc_id = 0;

        while (std::getline(infile, json_line)) {
            nlohmann::json document = nlohmann::json::parse(json_line);
            //document["id"] = std::to_string(doc_id);
            document["id"] = document["org"].get<std::string>() + ":" + document["repo_name"].get<std::string>();
            collection->add(document.dump());
            doc_id++;
        }

        infile.close();

        long long int timeMillis =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

        std::cout << "Time taken for insertion: " << timeMillis << "ms" << std::endl;
        begin = std::chrono::high_resolution_clock::now();

        std::ifstream infile2(argv[1]);

        doc_id = 0;

        while (std::getline(infile2, json_line)) {
            nlohmann::json document = nlohmann::json::parse(json_line);
            //document["id"] = std::to_string(doc_id);
            document["id"] = document["org"].get<std::string>() + ":" + document["repo_name"].get<std::string>();
            collection->remove(document["id"]);
            doc_id++;
        }

        infile2.close();

        timeMillis =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

        struct rusage r_usage;
        getrusage(RUSAGE_SELF,&r_usage);
        std::cout << "Memory usage: " << r_usage.ru_maxrss << std::endl;
        std::cout << "Time taken for deletion: " << timeMillis << "ms" << std::endl;
    }

    collectionManager.dispose();
    delete store;
    return 0;
}