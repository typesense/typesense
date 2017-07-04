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
    std::vector<field> sort_fields_index;

    std::vector<sort_field> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/coll_manager_test_db";
        std::cout << "Truncating and creating: " << state_dir_path << std::endl;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, "auth_key");

        search_fields = {field("title", field_types::STRING), field("starring", field_types::STRING)};
        facet_fields = {field("starring", field_types::STRING)};
        sort_fields = { sort_field("points", "DESC") };
        sort_fields_index = { field("points", "INT32") };

        collection1 = collectionManager.create_collection("collection1", search_fields, facet_fields,
                                                          sort_fields_index, "points");
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.drop_collection("collection1");
        delete store;
    }
};

TEST_F(CollectionManagerTest, CollectionCreation) {
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collection1 = collectionManager2.get_collection("collection1");
    ASSERT_NE(nullptr, collection1);

    spp::sparse_hash_map<std::string, field> schema = collection1->get_schema();
    std::vector<std::string> facet_fields_expected = {facet_fields[0].name};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(0, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(1, collection1->get_sort_fields().size());
    ASSERT_EQ(sort_fields[0].name, collection1->get_sort_fields()[0].name);
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_token_ranking_field());

    // check storage as well
    rocksdb::Iterator* it = store->get_iterator();
    size_t num_keys = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        num_keys += 1;
    }

    delete it;

    std::string collection_meta_json;
    nlohmann::json collection_meta;
    std::string next_seq_id;
    std::string next_collection_id;

    store->get(Collection::get_meta_key("collection1"), collection_meta_json);
    store->get(Collection::get_next_seq_id_key("collection1"), next_seq_id);
    store->get(CollectionManager::NEXT_COLLECTION_ID_KEY, next_collection_id);

    ASSERT_EQ(3, num_keys);
    ASSERT_EQ("1", next_seq_id); // we already call `collection1->get_next_seq_id` above, which is side-effecting
    ASSERT_EQ("{\"facet_fields\":[{\"name\":\"starring\",\"type\":\"STRING\"}],\"id\":0,\"name\":\"collection1\","
               "\"search_fields\":[{\"name\":\"title\",\"type\":\"STRING\"},"
               "{\"name\":\"starring\",\"type\":\"STRING\"}],"
               "\"sort_fields\":[{\"name\":\"points\",\"type\":\"INT32\"}],"
               "\"token_ranking_field\":\"points\"}", collection_meta_json);
    ASSERT_EQ("1", next_collection_id);
}

TEST_F(CollectionManagerTest, RestoreRecordsOnRestart) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    std::vector<std::string> search_fields = {"starring", "title"};
    std::vector<std::string> facets;

    nlohmann::json results = collection1->search("thomas", search_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_EQ(4, results["hits"].size());

    spp::sparse_hash_map<std::string, field> schema = collection1->get_schema();

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store, "auth_key");

    collection1 = collectionManager2.get_collection("collection1");
    ASSERT_NE(nullptr, collection1);

    std::vector<std::string> facet_fields_expected = {facet_fields[0].name};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(18, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(1, collection1->get_sort_fields().size());
    ASSERT_EQ(sort_fields[0].name, collection1->get_sort_fields()[0].name);
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_token_ranking_field());

    results = collection1->search("thomas", search_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false);
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
    size_t num_keys = 0;

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_EQ(it->key().ToString(), "$CI");
        num_keys += 1;
    }

    ASSERT_EQ(1, num_keys);
    ASSERT_TRUE(it->status().ok());

    ASSERT_EQ(nullptr, collectionManager.get_collection("collection1"));
    ASSERT_EQ(1, collectionManager.get_next_collection_id());

    delete it;
}