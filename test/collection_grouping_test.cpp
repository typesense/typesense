#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionGroupingTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_grouping";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 4, "auth_key");
        collectionManager.load();
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionGroupingTest, GroupingOnOptionalIntegerArray) {
    Collection *coll_group;

    std::vector<field> fields = {
        field("title", field_types::STRING, false),
        field("description", field_types::STRING, true, true),
        field("max", field_types::INT32, false),
        field("scores", field_types::INT64_ARRAY, true, true),
        field("average", field_types::FLOAT, false, true),
        field("is_valid", field_types::BOOL, false, true),
    };

    coll_group = collectionManager.get_collection("coll_group");
    if(coll_group == nullptr) {
        coll_group = collectionManager.create_collection("coll_group", fields, "max").get();
    }

    std::ifstream infile(std::string(ROOT_DIR)+"test/optional_fields.jsonl");

    std::string json_line;

    while (std::getline(infile, json_line)) {
        auto add_op = coll_group->add(json_line);
        if(!add_op.ok()) {
            std::cout << add_op.error() << std::endl;
        }
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    // first must be able to fetch all records (i.e. all must have been index)

    auto res = coll_group->search("*", {"title"}, "", {}, {}, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(6, res["found"].get<size_t>());
}