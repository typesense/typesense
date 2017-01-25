#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "collection.h"

TEST(CollectionManagerTest, RestoreRecordsOnRestart) {
    const std::string state_dir_path = "/tmp/typesense_test/coll_manager_test_db";

    std::cout << "Truncating and creating: " << state_dir_path << std::endl;
    system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

    CollectionManager & collectionManager = CollectionManager::get_instance();
    Store* store = new Store(state_dir_path);
    collectionManager.init(store);

    // Summary: create a collection, add documents, destroy it, try reopening the collection to query
    Collection *collection1;
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {field("title", field_types::STRING), field("starring", field_types::STRING)};
    std::vector<std::string> rank_fields = {"points"};

    collection1 = collectionManager.get_collection("collection1");
    if(collection1 == nullptr) {
        collection1 = collectionManager.create_collection("collection1", fields, rank_fields);
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    std::vector<std::string> search_fields = {"starring", "title"};
    nlohmann::json results = collection1->search("thomas", search_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store);

    collection1 = collectionManager2.get_collection("collection1");
    ASSERT_NE(nullptr, collection1);

    results = collection1->search("thomas", search_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());
}