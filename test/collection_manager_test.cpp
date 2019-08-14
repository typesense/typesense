#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "string_utils.h"
#include "collection.h"

class CollectionManagerTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    Collection *collection1;
    std::vector<field> search_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/coll_manager_test_db";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 4, "auth_key", "search_auth_key");

        search_fields = {
                field("title", field_types::STRING, false),
                field("starring", field_types::STRING, false),
                field("cast", field_types::STRING_ARRAY, true),
                field("points", field_types::INT32, false)
        };

        sort_fields = { sort_by("points", "DESC") };
        collection1 = collectionManager.create_collection("collection1", search_fields, "points", 12345).get();
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

    std::unordered_map<std::string, field> schema = collection1->get_schema();
    std::vector<std::string> facet_fields_expected = {"cast"};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(0, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(1, collection1->get_sort_fields().size());
    ASSERT_EQ(sort_fields[0].name, collection1->get_sort_fields()[0].name);
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_default_sorting_field());

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
    // we already call `collection1->get_next_seq_id` above, which is side-effecting
    ASSERT_EQ(1, StringUtils::deserialize_uint32_t(next_seq_id));
    ASSERT_EQ("{\"created_at\":12345,\"default_sorting_field\":\"points\","
              "\"fields\":[{\"facet\":false,\"name\":\"title\",\"type\":\"string\"},"
              "{\"facet\":false,\"name\":\"starring\",\"type\":\"string\"},"
              "{\"facet\":true,\"name\":\"cast\",\"type\":\"string[]\"},"
              "{\"facet\":false,\"name\":\"points\",\"type\":\"int32\"}],\"id\":0,\"name\":\"collection1\"}",
              collection_meta_json);
    ASSERT_EQ("1", next_collection_id);
}

TEST_F(CollectionManagerTest, ShouldInitCollection) {
    nlohmann::json collection_meta1 =
            nlohmann::json::parse("{\"name\": \"foobar\", \"id\": 100, \"fields\": [{\"name\": \"org\", \"type\": "
                                  "\"string\", \"facet\": false}], \"default_sorting_field\": \"foo\"}");

    Collection *collection = collectionManager.init_collection(collection_meta1, 100);
    ASSERT_EQ("foobar", collection->get_name());
    ASSERT_EQ(100, collection->get_collection_id());
    ASSERT_EQ(1, collection->get_fields().size());
    ASSERT_EQ("foo", collection->get_default_sorting_field());
    ASSERT_EQ(0, collection->get_created_at());

    delete collection;

    // with created_at ts

    nlohmann::json collection_meta2 =
            nlohmann::json::parse("{\"name\": \"foobar\", \"id\": 100, \"fields\": [{\"name\": \"org\", \"type\": "
                                  "\"string\", \"facet\": false}], \"created_at\": 12345,"
                                  "\"default_sorting_field\": \"foo\"}");

    collection = collectionManager.init_collection(collection_meta2, 100);
    ASSERT_EQ(12345, collection->get_created_at());

    delete collection;
}

TEST_F(CollectionManagerTest, GetAllCollections) {
    std::vector<Collection*> collection_vec = collectionManager.get_collections();
    ASSERT_EQ(1, collection_vec.size());
    ASSERT_STREQ("collection1", collection_vec[0]->get_name().c_str());

    // try creating one more collection
    collectionManager.create_collection("collection2", search_fields, "points");
    collection_vec = collectionManager.get_collections();
    ASSERT_EQ(2, collection_vec.size());

    // most recently created collection first
    ASSERT_STREQ("collection2", collection_vec[0]->get_name().c_str());
    ASSERT_STREQ("collection1", collection_vec[1]->get_name().c_str());

    collectionManager.drop_collection("collection2");
}

TEST_F(CollectionManagerTest, RestoreRecordsOnRestart) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    // add some overrides
    nlohmann::json override_json_include = {
        {"id", "include-rule"},
        {
         "rule", {
               {"query", "in"},
               {"match", override_t::MATCH_EXACT}
           }
        }
    };
    override_json_include["includes"] = nlohmann::json::array();
    override_json_include["includes"][0] = nlohmann::json::object();
    override_json_include["includes"][0]["id"] = "0";
    override_json_include["includes"][0]["position"] = 1;

    override_json_include["includes"][1] = nlohmann::json::object();
    override_json_include["includes"][1]["id"] = "3";
    override_json_include["includes"][1]["position"] = 2;

    override_t override_include(override_json_include);

    nlohmann::json override_json = {
            {"id", "exclude-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", override_t::MATCH_EXACT}
                   }
            }
    };
    override_json["excludes"] = nlohmann::json::array();
    override_json["excludes"][0] = nlohmann::json::object();
    override_json["excludes"][0]["id"] = "4";

    override_json["excludes"][1] = nlohmann::json::object();
    override_json["excludes"][1]["id"] = "11";

    override_t override_exclude(override_json);

    nlohmann::json override_json_deleted = {
            {"id", "deleted-rule"},
            {
             "rule", {
                           {"query", "of"},
                           {"match", override_t::MATCH_EXACT}
                   }
            }
    };

    override_t override_deleted(override_json_deleted);

    collection1->add_override(override_include);
    collection1->add_override(override_exclude);
    collection1->add_override(override_deleted);

    collection1->remove_override("deleted-rule");

    std::vector<std::string> search_fields = {"starring", "title"};
    std::vector<std::string> facets;

    nlohmann::json results = collection1->search("thomas", search_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    std::unordered_map<std::string, field> schema = collection1->get_schema();

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store, 4, "auth_key", "search_auth_key");

    collection1 = collectionManager2.get_collection("collection1");
    ASSERT_NE(nullptr, collection1);

    std::vector<std::string> facet_fields_expected = {"cast"};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(18, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(1, collection1->get_sort_fields().size());
    ASSERT_EQ(sort_fields[0].name, collection1->get_sort_fields()[0].name);
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_default_sorting_field());

    ASSERT_EQ(2, collection1->get_overrides().size());
    ASSERT_STREQ("exclude-rule", collection1->get_overrides()["exclude-rule"].id.c_str());
    ASSERT_STREQ("include-rule", collection1->get_overrides()["include-rule"].id.c_str());

    results = collection1->search("thomas", search_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());
}

TEST_F(CollectionManagerTest, DropCollectionCleanly) {
    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::string json_line;

    while (std::getline(infile, json_line)) {
        collection1->add(json_line);
    }

    infile.close();

    ASSERT_FALSE(nullptr == collectionManager.get_collection_with_id(0));
    ASSERT_FALSE(nullptr == collectionManager.get_collection("collection1"));

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
    ASSERT_EQ(nullptr, collectionManager.get_collection_with_id(0));
    ASSERT_EQ(1, collectionManager.get_next_collection_id());

    delete it;
}

TEST_F(CollectionManagerTest, Symlinking) {
    CollectionManager & cmanager = CollectionManager::get_instance();
    std::string state_dir_path = "/tmp/typesense_test/cmanager_test_db";
    system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
    Store *store = new Store(state_dir_path);
    cmanager.init(store, 4, "auth_key", "search_auth_key");

    // try resolving on a blank slate
    Option<std::string> collection_option = cmanager.resolve_symlink("collection");

    ASSERT_FALSE(collection_option.ok());
    ASSERT_EQ(404, collection_option.code());

    ASSERT_EQ(0, cmanager.get_symlinks().size());

    // insert a symlink
    bool inserted = cmanager.upsert_symlink("collection", "collection_2018");
    ASSERT_TRUE(inserted);

    collection_option = cmanager.resolve_symlink("collection");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("collection_2018", collection_option.get());

    // let's try inserting another symlink
    cmanager.upsert_symlink("company", "company_2018");
    collection_option = cmanager.resolve_symlink("company");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2018", collection_option.get());

    ASSERT_EQ(2, cmanager.get_symlinks().size());

    // update existing symlink
    inserted = cmanager.upsert_symlink("company", "company_2019");
    ASSERT_TRUE(inserted);
    collection_option = cmanager.resolve_symlink("company");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2019", collection_option.get());

    // remove link
    cmanager.delete_symlink("collection");
    collection_option = cmanager.resolve_symlink("collection");
    ASSERT_FALSE(collection_option.ok());
    ASSERT_EQ(404, collection_option.code());

    // try adding a few more symlinks
    cmanager.upsert_symlink("company_1", "company_2018");
    cmanager.upsert_symlink("company_2", "company_2019");
    cmanager.upsert_symlink("company_3", "company_2020");

    // should be able to restore state on init
    CollectionManager & cmanager2 = CollectionManager::get_instance();
    cmanager2.init(store, 4, "auth_key", "search_auth_key");
    collection_option = cmanager2.resolve_symlink("company");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2019", collection_option.get());

    collection_option = cmanager2.resolve_symlink("company_1");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2018", collection_option.get());

    collection_option = cmanager2.resolve_symlink("company_3");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2020", collection_option.get());
}