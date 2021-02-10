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
        collectionManager.init(store, 1.0, "auth_key");
        collectionManager.load();

        search_fields = {
                field("title", field_types::STRING, false),
                field("starring", field_types::STRING, false),
                field("cast", field_types::STRING_ARRAY, true, true),
                field("points", field_types::INT32, false)
        };

        sort_fields = { sort_by("points", "DESC") };
        collection1 = collectionManager.create_collection("collection1", 4, search_fields, "points", 12345).get();
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.drop_collection("collection1");
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionManagerTest, CollectionCreation) {
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collection1 = collectionManager2.get_collection("collection1").get();
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
              "\"fields\":[{\"facet\":false,\"name\":\"title\",\"optional\":false,\"type\":\"string\"},"
              "{\"facet\":false,\"name\":\"starring\",\"optional\":false,\"type\":\"string\"},"
              "{\"facet\":true,\"name\":\"cast\",\"optional\":true,\"type\":\"string[]\"},"
              "{\"facet\":false,\"name\":\"points\",\"optional\":false,\"type\":\"int32\"}],\"id\":0,\"name\":\"collection1\",\"num_memory_shards\":4}",
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
    collectionManager.create_collection("collection2", 4, search_fields, "points");
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

    override_t override_include;
    override_t::parse(override_json_include, "", override_include);

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

    override_t override_exclude;
    override_t::parse(override_json, "", override_exclude);

    nlohmann::json override_json_deleted = {
        {"id", "deleted-rule"},
        {
         "rule", {
                   {"query", "of"},
                   {"match", override_t::MATCH_EXACT}
           }
        }
    };

    override_json_deleted["excludes"] = nlohmann::json::array();
    override_json_deleted["excludes"][0] = nlohmann::json::object();
    override_json_deleted["excludes"][0]["id"] = "11";

    override_t override_deleted;
    override_t::parse(override_json_deleted, "", override_deleted);

    collection1->add_override(override_include);
    collection1->add_override(override_exclude);
    collection1->add_override(override_deleted);

    collection1->remove_override("deleted-rule");

    // make some synonym operation
    synonym_t synonym1("id1", {"smart", "phone"}, {{"iphone"}});
    synonym_t synonym2("id2", {"mobile", "phone"}, {{"samsung", "phone"}});
    synonym_t synonym3("id3", {}, {{"football"}, {"foot", "ball"}});

    collection1->add_synonym(synonym1);
    collection1->add_synonym(synonym2);
    collection1->add_synonym(synonym3);

    collection1->remove_synonym("id2");

    std::vector<std::string> search_fields = {"starring", "title"};
    std::vector<std::string> facets;

    nlohmann::json results = collection1->search("thomas", search_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(4, results["hits"].size());

    std::unordered_map<std::string, field> schema = collection1->get_schema();

    // create a new collection manager to ensure that it restores the records from the disk backed store
    CollectionManager & collectionManager2 = CollectionManager::get_instance();
    collectionManager2.init(store, 1.0, "auth_key");
    auto load_op = collectionManager2.load();

    if(!load_op.ok()) {
        LOG(ERROR) << load_op.error();
    }

    ASSERT_TRUE(load_op.ok());

    collection1 = collectionManager2.get_collection("collection1").get();
    ASSERT_NE(nullptr, collection1);

    std::vector<std::string> facet_fields_expected = {"cast"};

    ASSERT_EQ(0, collection1->get_collection_id());
    ASSERT_EQ(18, collection1->get_next_seq_id());
    ASSERT_EQ(facet_fields_expected, collection1->get_facet_fields());
    ASSERT_EQ(1, collection1->get_sort_fields().size());
    ASSERT_EQ(sort_fields[0].name, collection1->get_sort_fields()[0].name);
    ASSERT_EQ(schema.size(), collection1->get_schema().size());
    ASSERT_EQ("points", collection1->get_default_sorting_field());

    auto restored_schema = collection1->get_schema();
    ASSERT_EQ(true, restored_schema.at("cast").optional);
    ASSERT_EQ(true, restored_schema.at("cast").facet);
    ASSERT_EQ(false, restored_schema.at("title").facet);
    ASSERT_EQ(false, restored_schema.at("title").optional);

    ASSERT_EQ(2, collection1->get_overrides().size());
    ASSERT_STREQ("exclude-rule", collection1->get_overrides()["exclude-rule"].id.c_str());
    ASSERT_STREQ("include-rule", collection1->get_overrides()["include-rule"].id.c_str());

    const auto& synonyms = collection1->get_synonyms();
    ASSERT_EQ(2, synonyms.size());

    ASSERT_STREQ("id1", synonyms.at("id1").id.c_str());
    ASSERT_EQ(2, synonyms.at("id1").root.size());
    ASSERT_EQ(1, synonyms.at("id1").synonyms.size());

    ASSERT_STREQ("id3", synonyms.at("id3").id.c_str());
    ASSERT_EQ(0, synonyms.at("id3").root.size());
    ASSERT_EQ(2, synonyms.at("id3").synonyms.size());

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

    ASSERT_FALSE(nullptr == collectionManager.get_collection_with_id(0).get());
    ASSERT_FALSE(nullptr == collectionManager.get_collection("collection1").get());

    collectionManager.drop_collection("collection1");

    rocksdb::Iterator* it = store->get_iterator();
    size_t num_keys = 0;

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_EQ(it->key().ToString(), "$CI");
        num_keys += 1;
    }

    ASSERT_EQ(1, num_keys);
    ASSERT_TRUE(it->status().ok());

    ASSERT_EQ(nullptr, collectionManager.get_collection("collection1").get());
    ASSERT_EQ(nullptr, collectionManager.get_collection_with_id(0).get());
    ASSERT_EQ(1, collectionManager.get_next_collection_id());

    delete it;
}

TEST_F(CollectionManagerTest, Symlinking) {
    CollectionManager & cmanager = CollectionManager::get_instance();
    std::string state_dir_path = "/tmp/typesense_test/cmanager_test_db";
    system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());
    Store *store = new Store(state_dir_path);
    cmanager.init(store, 1.0, "auth_key");
    cmanager.load();

    // try resolving on a blank slate
    Option<std::string> collection_option = cmanager.resolve_symlink("collection");

    ASSERT_FALSE(collection_option.ok());
    ASSERT_EQ(404, collection_option.code());

    ASSERT_EQ(0, cmanager.get_symlinks().size());

    // symlink name cannot be the same as an existing collection
    Option<bool> inserted = cmanager.upsert_symlink("collection1", "collection_2018");
    ASSERT_FALSE(inserted.ok());
    ASSERT_STREQ("Name `collection1` conflicts with an existing collection name.", inserted.error().c_str());

    // insert a symlink
    inserted = cmanager.upsert_symlink("collection_link", "collection_2018");
    ASSERT_TRUE(inserted.ok());

    collection_option = cmanager.resolve_symlink("collection_link");
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
    ASSERT_TRUE(inserted.ok());
    collection_option = cmanager.resolve_symlink("company");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("company_2019", collection_option.get());

    // add and update a symlink against an existing collection
    inserted = cmanager.upsert_symlink("collection1_link", "collection1");
    ASSERT_TRUE(inserted.ok());
    collection_option = cmanager.resolve_symlink("collection1_link");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("collection1", collection_option.get());

    inserted = cmanager.upsert_symlink("collection1_link", "collection2");
    ASSERT_TRUE(inserted.ok());
    collection_option = cmanager.resolve_symlink("collection1_link");
    ASSERT_TRUE(collection_option.ok());
    ASSERT_EQ("collection2", collection_option.get());

    // remove link
    Option<bool> deleted = cmanager.delete_symlink("collection");
    ASSERT_TRUE(deleted.ok());
    collection_option = cmanager.resolve_symlink("collection");
    ASSERT_FALSE(collection_option.ok());
    ASSERT_EQ(404, collection_option.code());

    // try adding a few more symlinks
    cmanager.upsert_symlink("company_1", "company_2018");
    cmanager.upsert_symlink("company_2", "company_2019");
    cmanager.upsert_symlink("company_3", "company_2020");

    // should be able to restore state on init
    CollectionManager & cmanager2 = CollectionManager::get_instance();
    cmanager2.init(store, 1.0, "auth_key");
    cmanager2.load();

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

TEST_F(CollectionManagerTest, ParseSortByClause) {
    std::vector<sort_by> sort_fields;
    bool sort_by_parsed = CollectionManager::parse_sort_by_str("points:desc,loc(24.56,10.45):ASC", sort_fields);
    ASSERT_TRUE(sort_by_parsed);

    ASSERT_STREQ("points", sort_fields[0].name.c_str());
    ASSERT_STREQ("DESC", sort_fields[0].order.c_str());

    ASSERT_STREQ("loc(24.56,10.45)", sort_fields[1].name.c_str());
    ASSERT_STREQ("ASC", sort_fields[1].order.c_str());

    sort_fields.clear();

    sort_by_parsed = CollectionManager::parse_sort_by_str(" points:desc , loc(24.56,10.45):ASC", sort_fields);
    ASSERT_TRUE(sort_by_parsed);

    ASSERT_STREQ("points", sort_fields[0].name.c_str());
    ASSERT_STREQ("DESC", sort_fields[0].order.c_str());

    ASSERT_STREQ("loc(24.56,10.45)", sort_fields[1].name.c_str());
    ASSERT_STREQ("ASC", sort_fields[1].order.c_str());

    sort_fields.clear();

    sort_by_parsed = CollectionManager::parse_sort_by_str(" loc(24.56,10.45):ASC, points:desc ", sort_fields);
    ASSERT_TRUE(sort_by_parsed);

    ASSERT_STREQ("loc(24.56,10.45)", sort_fields[0].name.c_str());
    ASSERT_STREQ("ASC", sort_fields[0].order.c_str());

    ASSERT_STREQ("points", sort_fields[1].name.c_str());
    ASSERT_STREQ("DESC", sort_fields[1].order.c_str());

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str("", sort_fields);
    ASSERT_TRUE(sort_by_parsed);
    ASSERT_EQ(0, sort_fields.size());

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str(",", sort_fields);
    ASSERT_FALSE(sort_by_parsed);

    sort_fields.clear();
    sort_by_parsed = CollectionManager::parse_sort_by_str(",,", sort_fields);
    ASSERT_FALSE(sort_by_parsed);

}