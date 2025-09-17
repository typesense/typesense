#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include "collection.h"
#include "override_index_manager.h"

class CollectionOverrideSetsTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    OverrideIndexManager & ovManager = OverrideIndexManager::get_instance();
    std::atomic<bool> quit = false;
    Collection *coll;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_override_sets";
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);

        std::vector<field> fields = {
            field("title", field_types::STRING, false),
            field("points", field_types::INT32, false)
        };

        coll = collectionManager.get_collection("coll_osets").get();
        if(coll == nullptr) {
            coll = collectionManager.create_collection("coll_osets", 2, fields, "points").get();
        }

        ovManager.init_store(store);
        auto upsert_set = nlohmann::json::array({
            nlohmann::json{{"id", "ov-1"}, {"rule", {{"query", "titanic"}, {"match", override_t::MATCH_EXACT}}}, {"includes", {{{"id", "1"}, {"position", 1}}}}}
        });
        ovManager.upsert_override_set("ovs1", upsert_set);

        coll->set_override_sets({"ovs1"});

        // add docs
        coll->add(R"({"id":"1","title":"Titanic","points":10})");
        coll->add(R"({"id":"2","title":"Avatar","points":20})");
    }

    virtual void SetUp() { setupCollection(); }
    virtual void TearDown() {
        collectionManager.drop_collection("coll_osets");
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(CollectionOverrideSetsTest, OverrideSetsApplied) {
    auto res = coll->search("titanic", {"title"}, "", {}, {}, {0}, 10);
    ASSERT_TRUE(res.ok());
    auto json = res.get();
    ASSERT_GE(json["hits"].size(), 1);
    ASSERT_EQ("1", json["hits"][0]["document"]["id"].get<std::string>());
}


