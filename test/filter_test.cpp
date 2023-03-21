#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include <filter.h>
#include "collection.h"

class FilterTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_join";
        LOG(INFO) << "Truncating and creating: " << state_dir_path;
        system(("rm -rf "+state_dir_path+" && mkdir -p "+state_dir_path).c_str());

        store = new Store(state_dir_path);
        collectionManager.init(store, 1.0, "auth_key", quit);
        collectionManager.load(8, 1000);
    }

    virtual void SetUp() {
        setupCollection();
    }

    virtual void TearDown() {
        collectionManager.dispose();
        delete store;
    }
};

TEST_F(FilterTest, FilterTreeIterator) {
    nlohmann::json schema =
            R"({
                "name": "Collection",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int32"},
                    {"name": "years", "type": "int32[]"},
                    {"name": "rating", "type": "float"},
                    {"name": "tags", "type": "string[]"}
                ]
            })"_json;

    Collection* coll = collectionManager.create_collection(schema).get();

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::string json_line;
    while (std::getline(infile, json_line)) {
        auto add_op = coll->add(json_line);
        ASSERT_TRUE(add_op.ok());
    }
    infile.close();

    const std::string doc_id_prefix = std::to_string(coll->get_collection_id()) + "_" + Collection::DOC_ID_PREFIX + "_";
    filter_node_t* filter_tree_root = nullptr;
    Option<bool> filter_op = filter::parse_filter_query("name: foo", coll->get_schema(), store, doc_id_prefix,
                                                        filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    Option<bool> iter_op(true);
    auto iter_no_match_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);

    ASSERT_FALSE(iter_no_match_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: [foo bar, baz]", coll->get_schema(), store, doc_id_prefix,
                                                        filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_no_match_multi_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);

    ASSERT_FALSE(iter_no_match_multi_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: Jeremy", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_contains_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);
    for (uint32_t i = 0; i < 5; i++) {
        ASSERT_TRUE(iter_contains_test.valid());
        ASSERT_EQ(i, iter_contains_test.doc);
        iter_contains_test.next();
    }
    ASSERT_FALSE(iter_contains_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: [Jeremy, Howard, Richard]", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_contains_multi_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);
    for (uint32_t i = 0; i < 5; i++) {
        ASSERT_TRUE(iter_contains_multi_test.valid());
        ASSERT_EQ(i, iter_contains_multi_test.doc);
        iter_contains_multi_test.next();
    }
    ASSERT_FALSE(iter_contains_multi_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name:= Jeremy Howard", coll->get_schema(), store, doc_id_prefix,
                                                        filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_exact_match_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);
    for (uint32_t i = 0; i < 5; i++) {
        ASSERT_TRUE(iter_exact_match_test.valid());
        ASSERT_EQ(i, iter_exact_match_test.doc);
        iter_exact_match_test.next();
    }
    ASSERT_FALSE(iter_exact_match_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags:= [gold, silver]", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_exact_match_multi_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);

    std::vector<uint32_t> expected = {0, 2, 3, 4};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_exact_match_multi_test.valid());
        ASSERT_EQ(i, iter_exact_match_multi_test.doc);
        iter_exact_match_multi_test.next();
    }
    ASSERT_FALSE(iter_exact_match_multi_test.valid());
    ASSERT_TRUE(iter_op.ok());

//    delete filter_tree_root;
//    filter_tree_root = nullptr;
//    filter_op = filter::parse_filter_query("tags:!= gold", coll->get_schema(), store, doc_id_prefix,
//                                           filter_tree_root);
//    ASSERT_TRUE(filter_op.ok());
//
//    auto iter_not_equals_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);
//
//    std::vector<uint32_t> expected = {1, 3};
//    for (auto const& i : expected) {
//        ASSERT_TRUE(iter_not_equals_test.valid());
//        ASSERT_EQ(i, iter_not_equals_test.doc);
//        iter_not_equals_test.next();
//    }
//
//    ASSERT_FALSE(iter_not_equals_test.valid());
//    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: gold", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_skip_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);

    ASSERT_TRUE(iter_skip_test.valid());
    iter_skip_test.skip_to(3);
    ASSERT_TRUE(iter_skip_test.valid());
    ASSERT_EQ(4, iter_skip_test.doc);
    iter_skip_test.next();

    ASSERT_FALSE(iter_skip_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: jeremy && tags: fine platinum", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_and_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);

    ASSERT_TRUE(iter_and_test.valid());
    ASSERT_EQ(1, iter_and_test.doc);
    iter_and_test.next();

    ASSERT_FALSE(iter_and_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: James || tags: bronze", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto doc =
            R"({
                "name": "James Rowdy",
                "age": 36,
                "years": [2005, 2022],
                "rating": 6.03,
                "tags": ["copper"]
            })"_json;
    auto add_op = coll->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    auto iter_or_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);

    expected = {2, 4, 5};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_or_test.valid());
        ASSERT_EQ(i, iter_or_test.doc);
        iter_or_test.next();
    }

    ASSERT_FALSE(iter_or_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: James || (tags: gold && tags: silver)", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_skip_complex_filter_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root, iter_op);

    ASSERT_TRUE(iter_skip_complex_filter_test.valid());
    iter_skip_complex_filter_test.skip_to(4);

    expected = {4, 5};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_skip_complex_filter_test.valid());
        ASSERT_EQ(i, iter_skip_complex_filter_test.doc);
        iter_skip_complex_filter_test.next();
    }

    ASSERT_FALSE(iter_skip_complex_filter_test.valid());
    ASSERT_TRUE(iter_op.ok());

    delete filter_tree_root;
}