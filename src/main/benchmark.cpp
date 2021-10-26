#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <chrono>
#include <art.h>
#include <unordered_map>
#include <queue>
#include <ctime>
#include "collection.h"
#include "string_utils.h"
#include "collection_manager.h"

using namespace std;

std::string get_query(StringUtils & string_utils, std::string & text) {
    std::vector<std::string> tokens;
    std::vector<std::string> normalized_tokens;
    StringUtils::split(text, tokens, " ");

    for(uint32_t i=0; i<tokens.size(); i++) {
        auto token = tokens[i];
        //string_utils.unicode_normalize(token);
        normalized_tokens.push_back(token);
    }

    size_t rand_len = 0 + (rand() % static_cast<int>(2 - 0 + 1));
    size_t rand_index = 0 + (rand() % static_cast<int>(tokens.size()-1 - 0 + 1));
    size_t end_index = std::min(rand_index+rand_len, tokens.size()-1);

    std::stringstream ss;
    for(auto i = rand_index; i <= end_index; i++) {
        if(i != rand_index) {
            ss << " ";
        }
        ss << normalized_tokens[i];
    }

    return ss.str();
}

void benchmark_hn_titles(char* file_path) {
    std::vector<field> fields_to_index = { field("title", field_types::STRING, false),
                                           field("points", field_types::INT32, false) };

    Store *store = new Store("/tmp/typesense-data");
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit;
    collectionManager.init(store, 1, "abcd", quit);
    collectionManager.load(100, 100);

    Collection *collection = collectionManager.get_collection("hnstories_direct").get();
    if(collection == nullptr) {
        collection = collectionManager.create_collection("hnstories_direct", 4, fields_to_index, "points").get();
    }

    std::ifstream infile(file_path);

    std::string json_line;
    StringUtils string_utils;
    std::vector<std::string> queries;
    size_t counter = 0;

    auto begin0 = std::chrono::high_resolution_clock::now();

    while (std::getline(infile, json_line)) {
        counter++;
        collection->add(json_line);

        if(counter % 100 == 0) {
            nlohmann::json obj = nlohmann::json::parse(json_line);
            std::string title = obj["title"];
            std::string query = get_query(string_utils, title);
            queries.push_back(query);
        }
    }

    infile.close();
    long long int timeMillis0 = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin0).count();

    std::cout << "FINISHED INDEXING!" << flush << std::endl;
    std::cout << "Time taken: " << timeMillis0 << "ms" << std::endl;

    std::vector<std::string> search_fields = {"title"};
    uint64_t results_total = 0; // to prevent no-op optimization!

    auto begin = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < queries.size(); i++) {
        auto results_op = collection->search(queries[i], search_fields, "", { }, {sort_by("points", "DESC")}, {2}, 10, 1, MAX_SCORE, {true});
        if(results_op.ok() != true) {
            exit(2);
        }
        auto results = results_op.get();
        results_total += results["hits"].size();
    }

    long long int timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Number of queries: " << queries.size() << std::endl;
    std::cout << "Time taken: " << timeMillis << "ms" << std::endl;
    std::cout << "Results total: " << results_total << std::endl;
}

void benchmark_reactjs_pages(char* file_path) {
    std::vector<field> fields_to_index = {
        field("url", field_types::STRING, false),
        field("h1", field_types::STRING, false),
        field("h2", field_types::STRING_ARRAY, false),
        field("h3", field_types::STRING_ARRAY, false),
        field("h4", field_types::STRING_ARRAY, false),
        field("h5", field_types::STRING_ARRAY, false),
        field("h6", field_types::STRING_ARRAY, false),
        field("p", field_types::STRING_ARRAY, false),
        field("dummy_sorting_field", field_types::INT32, false)
    };

    Store *store = new Store("/tmp/typesense-data");
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit;
    collectionManager.init(store, 4, "abcd", quit);
    collectionManager.load(100, 100);

    Collection* collection = collectionManager.get_collection("reactjs_pages").get();
    if(collection == nullptr) {
        collection = collectionManager.create_collection("reactjs_pages", 4, fields_to_index, "dummy_sorting_field").get();
    }

    std::ifstream infile(file_path);

    std::string json_line;
    StringUtils string_utils;
    std::vector<std::string> queries;
    size_t counter = 0;

    while (std::getline(infile, json_line)) {
        counter++;
        collection->add(json_line);

        if(counter % 1 == 0) {
            nlohmann::json obj = nlohmann::json::parse(json_line);
            std::string title = obj["p"][0];
            std::string query = get_query(string_utils, title);
            queries.push_back(query);
        }
    }

    infile.close();
    std::cout << "FINISHED INDEXING!" << flush << std::endl;

    std::vector<std::string> search_fields = {"h1", "h2", "h3", "h4", "h5", "h6", "p"};
    uint64_t results_total = 0; // to prevent no-op optimization!

    auto begin = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < queries.size(); i++) {
        auto results_op = collection->search(queries[i], search_fields, "", { }, {sort_by("dummy_sorting_field", "DESC")}, {2}, 10, 1,
                                             MAX_SCORE, {true}, 10, spp::sparse_hash_set<std::string>(), {"p"});
        if(results_op.ok() != true) {
            exit(2);
        }
        auto results = results_op.get();
        results_total += results["hits"].size();
    }

    long long int timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    std::cout << "Number of queries: " << queries.size() << std::endl;
    std::cout << "Time taken: " << timeMillis << "ms" << std::endl;
    std::cout << "Results total: " << results_total << std::endl;
}

void generate_word_freq() {
    std::ifstream infile("/tmp/unigram_freq.jsonl");
    std::ofstream outfile("/tmp/eng_words.jsonl", std::ios_base::app);

    std::string json_line;
    while (std::getline(infile, json_line)) {
        try {
            nlohmann::json obj = nlohmann::json::parse(json_line);
            obj["count"] = uint64_t((double(obj["count"].get<uint64_t>()) / 23135851162) * 1000000000);
            std::string json_str = obj.dump();
            outfile << json_str << std::endl;
        } catch(...) {
            LOG(ERROR) << "Failed parsing: " << json_line;
        }
    }

    infile.close();
    outfile.close();
}

int main(int argc, char* argv[]) {
    srand(time(NULL));
//    system("rm -rf /tmp/typesense-data && mkdir -p /tmp/typesense-data");

//    benchmark_hn_titles(argv[1]);
//    benchmark_reactjs_pages(argv[1]);

    generate_word_freq();

    return 0;
}