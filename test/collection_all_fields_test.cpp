#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <collection_manager.h>
#include "collection.h"

class CollectionAllFieldsTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_all_fields";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key");
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

TEST_F(CollectionAllFieldsTest, IndexDocsWithoutSchema) {
    Collection *coll1;

    std::ifstream infile(std::string(ROOT_DIR)+"test/multi_field_documents.jsonl");
    std::vector<field> fields = {};

    std::vector<sort_by> sort_fields = { sort_by("points", "DESC") };

    coll1 = collectionManager.get_collection("coll1").get();
    if(coll1 == nullptr) {
        coll1 = collectionManager.create_collection("coll1", 1, fields, "", 0, true).get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        nlohmann::json document = nlohmann::json::parse(json_line);
        Option<nlohmann::json> add_op = coll1->add(document.dump());
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    query_fields = {"starring"};
    std::vector<std::string> facets;

    // same should succeed when verbatim filter is made
    auto results = coll1->search("will", query_fields, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

    ASSERT_EQ(2, results["hits"].size());
    ASSERT_EQ(2, results["found"].get<size_t>());

    ASSERT_STREQ("1", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("0", results["hits"][1]["document"]["id"].get<std::string>().c_str());

    results = coll1->search("chris", {"cast"}, "", facets, sort_fields, 0, 10, 1, FREQUENCY, false).get();

    ASSERT_EQ(3, results["hits"].size());
    ASSERT_EQ(3, results["found"].get<size_t>());

    ASSERT_STREQ("6", results["hits"][0]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("1", results["hits"][1]["document"]["id"].get<std::string>().c_str());
    ASSERT_STREQ("7", results["hits"][2]["document"]["id"].get<std::string>().c_str());

    // reject field with a different type than already inferred type
    // default for `index_all_fields` is `DIRTY_FIELD_COERCE_IGNORE`

    // unable to coerce
    auto doc_json = R"({"cast":"William Barnes","points":63,"starring":"Will Ferrell",
                        "starring_facet":"Will Ferrell","title":"Anchorman 2: The Legend Continues"})";

    Option<nlohmann::json> add_op = coll1->add(doc_json);
    ASSERT_FALSE(add_op.ok());
    ASSERT_STREQ("Field `cast` must be an array.", add_op.error().c_str());

    // coerce integer to string
    doc_json = R"({"cast": ["William Barnes"],"points": 63, "starring":"Will Ferrell",
                        "starring_facet":"Will Ferrell","title": 300})";

    add_op = coll1->add(doc_json);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("300", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("300", results["hits"][0]["document"]["title"].get<std::string>().c_str());

    // with dirty values set to `DIRTY_FIELD_COERCE_IGNORE`
    // `cast` field should not be indexed into store
    doc_json = R"({"cast":"William Barnes","points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title":"With bad cast field."})";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::COERCE_OR_IGNORE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("With bad cast field", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_STREQ("With bad cast field.", results["hits"][0]["document"]["title"].get<std::string>().c_str());
    ASSERT_EQ(0, results["hits"][0]["document"].count("cast"));

    // with dirty values set to `DIRTY_FIELD_IGNORE`
    // no coercion should happen, `title` field will just be ignored, but record indexed
    doc_json = R"({"cast": ["Jeremy Livingston"],"points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title": 1200 })";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::IGNORE);
    ASSERT_TRUE(add_op.ok());

    results = coll1->search("1200", {"title"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(0, results["hits"].size());

    results = coll1->search("Jeremy Livingston", {"cast"}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false).get();
    ASSERT_EQ(1, results["hits"].size());
    ASSERT_EQ(0, results["hits"][0]["document"].count("title"));

    // with dirty values set to `DIRTY_FIELD_REJECT`
    doc_json = R"({"cast": ["Jeremy Livingston"],"points":63,"starring":"Will Ferrell",
                    "starring_facet":"Will Ferrell","title": 1200 })";

    add_op = coll1->add(doc_json, CREATE, "", DIRTY_VALUES::REJECT);
    ASSERT_FALSE(add_op.ok());
    ASSERT_STREQ("Field `title` must be a string.", add_op.error().c_str());

    // try querying using an non-existing sort field
    sort_fields = { sort_by("not-found", "DESC") };
    auto res_op = coll1->search("*", {}, "", {}, sort_fields, 0, 10, 1, FREQUENCY, false);
    ASSERT_FALSE(res_op.ok());
    ASSERT_EQ("Could not find a field named `not-found` in the schema for sorting.", res_op.error());

    collectionManager.drop_collection("coll1");
}
