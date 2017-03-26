#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "collection.h"

class CollectionManagerTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection1;
    std::vector<field> search_fields;
    std::vector<field> facet_fields;
    std::vector<std::string> rank_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/coll_manager_test_db";
        std::cout << "Truncating and creating: " << state_dir_path << std::endl;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store);

        search_fields = {field("title", field_types::STRING), field("starring", field_types::STRING)};
        facet_fields = {field("starring", field_types::STRING)};
        rank_fields = {"points"};

        collection1 = collectionManager.create_collection("collection1", search_fields, facet_fields,
                                                          rank_fields, "points");
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.drop_collection("collection1");
        delete store;
    }
};

TEST_F(CollectionManagerTest, RestoreRecordsOnRestart) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    std::vector<std::string> search_fields = {"starring", "title"};
    std::vector<std::string> facets;

    nlohmann::json results = collection1->search("thomas", search_fields, "", facets, rank_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    spp::sparse_hash_map<std::string, field> schema = collection1->get_schema();

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store);

    collection1 = collectionManager2.get_collection("collection1");
    ASSERT_NE(nullptr, collection1);

    std::vector<std::string> facet_fields_expected = {facet_fields[0].name};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(18, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(rank_fields, collection1->get_rank_fields());
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_token_ordering_field());

    results = collection1->search("thomas", search_fields, "", facets, rank_fields, 0, 10, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());
}

TEST_F(CollectionManagerTest, DropCollectionCleanly) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    collectionManager.drop_collection("collection1");

    rocksdb::Iterator* it = store->get_iterator();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_EQ(it->key().ToString(), "$CI");
    }

    assert(it->status().ok());

    ASSERT_EQ(nullptr, collectionManager.get_collection("collection1"));
    ASSERT_EQ(1, collectionManager.get_next_collection_id());

    delete it;
}