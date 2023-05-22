#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <fstream>
#include <collection_manager.h>
#include <filter.h>
#include <posting.h>
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

    auto iter_null_filter_tree_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);

    ASSERT_TRUE(iter_null_filter_tree_test.init_status().ok());
    ASSERT_FALSE(iter_null_filter_tree_test.is_valid);

    Option<bool> filter_op = filter::parse_filter_query("name: foo", coll->get_schema(), store, doc_id_prefix,
                                                        filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_no_match_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);

    ASSERT_TRUE(iter_no_match_test.init_status().ok());
    ASSERT_FALSE(iter_no_match_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: [foo bar, baz]", coll->get_schema(), store, doc_id_prefix,
                                                        filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_no_match_multi_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);

    ASSERT_TRUE(iter_no_match_multi_test.init_status().ok());
    ASSERT_FALSE(iter_no_match_multi_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: Jeremy", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_contains_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_contains_test.init_status().ok());

    for (uint32_t i = 0; i < 5; i++) {
        ASSERT_TRUE(iter_contains_test.is_valid);
        ASSERT_EQ(i, iter_contains_test.seq_id);
        iter_contains_test.next();
    }
    ASSERT_FALSE(iter_contains_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: [Jeremy, Howard, Richard]", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_contains_multi_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_contains_multi_test.init_status().ok());

    for (uint32_t i = 0; i < 5; i++) {
        ASSERT_TRUE(iter_contains_multi_test.is_valid);
        ASSERT_EQ(i, iter_contains_multi_test.seq_id);
        iter_contains_multi_test.next();
    }
    ASSERT_FALSE(iter_contains_multi_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name:= Jeremy Howard", coll->get_schema(), store, doc_id_prefix,
                                                        filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_exact_match_1_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_exact_match_1_test.init_status().ok());

    for (uint32_t i = 0; i < 5; i++) {
        ASSERT_TRUE(iter_exact_match_1_test.is_valid);
        ASSERT_EQ(i, iter_exact_match_1_test.seq_id);
        iter_exact_match_1_test.next();
    }
    ASSERT_FALSE(iter_exact_match_1_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags:= PLATINUM", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_exact_match_2_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_exact_match_2_test.init_status().ok());
    ASSERT_FALSE(iter_exact_match_2_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags:= [gold, silver]", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_exact_match_multi_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_exact_match_multi_test.init_status().ok());

    std::vector<int> expected = {0, 2, 3, 4};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_exact_match_multi_test.is_valid);
        ASSERT_EQ(i, iter_exact_match_multi_test.seq_id);
        iter_exact_match_multi_test.next();
    }
    ASSERT_FALSE(iter_exact_match_multi_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags:!= gold", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_not_equals_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_not_equals_test.init_status().ok());

    expected = {1, 3};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_not_equals_test.is_valid);
        ASSERT_EQ(i, iter_not_equals_test.seq_id);
        iter_not_equals_test.next();
    }

    ASSERT_FALSE(iter_not_equals_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: gold", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_skip_test1 = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_skip_test1.init_status().ok());

    ASSERT_TRUE(iter_skip_test1.is_valid);
    iter_skip_test1.skip_to(3);
    ASSERT_TRUE(iter_skip_test1.is_valid);
    ASSERT_EQ(4, iter_skip_test1.seq_id);
    iter_skip_test1.next();

    ASSERT_FALSE(iter_skip_test1.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: != silver", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_skip_test2 = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_skip_test2.init_status().ok());

    ASSERT_TRUE(iter_skip_test2.is_valid);
    iter_skip_test2.skip_to(3);
    ASSERT_FALSE(iter_skip_test2.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: jeremy && tags: fine platinum", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_and_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_and_test.init_status().ok());

    ASSERT_TRUE(iter_and_test.is_valid);
    ASSERT_EQ(1, iter_and_test.seq_id);
    iter_and_test.next();

    ASSERT_FALSE(iter_and_test.is_valid);

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

    auto iter_or_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_or_test.init_status().ok());

    expected = {2, 4, 5};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_or_test.is_valid);
        ASSERT_EQ(i, iter_or_test.seq_id);
        iter_or_test.next();
    }

    ASSERT_FALSE(iter_or_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: James || (tags: gold && tags: silver)", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_skip_complex_filter_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_skip_complex_filter_test.init_status().ok());

    ASSERT_TRUE(iter_skip_complex_filter_test.is_valid);
    iter_skip_complex_filter_test.skip_to(4);

    expected = {4, 5};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_skip_complex_filter_test.is_valid);
        ASSERT_EQ(i, iter_skip_complex_filter_test.seq_id);
        iter_skip_complex_filter_test.next();
    }

    ASSERT_FALSE(iter_skip_complex_filter_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: James || (tags: gold && tags: [silver, bronze])", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_validate_ids_test1 = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_validate_ids_test1.init_status().ok());

    std::vector<int> validate_ids = {0, 1, 2, 3, 4, 5, 6}, seq_ids = {0, 2, 2, 4, 4, 5, 5};
    expected = {1, 0, 1, 0, 1, 1, -1};
    for (uint32_t i = 0; i < validate_ids.size(); i++) {
        ASSERT_EQ(expected[i], iter_validate_ids_test1.valid(validate_ids[i]));
        ASSERT_EQ(seq_ids[i], iter_validate_ids_test1.seq_id);
    }

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: platinum || name: James", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_validate_ids_test2 = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_validate_ids_test2.init_status().ok());

    validate_ids = {0, 1, 2, 3, 4, 5, 6}, seq_ids = {1, 1, 5, 5, 5, 5, 5};
    expected = {0, 1, 0, 0, 0, 1, -1};
    for (uint32_t i = 0; i < validate_ids.size(); i++) {
        ASSERT_EQ(expected[i], iter_validate_ids_test2.valid(validate_ids[i]));
        ASSERT_EQ(seq_ids[i], iter_validate_ids_test2.seq_id);
    }

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: gold && rating: < 6", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_validate_ids_test3 = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_validate_ids_test3.init_status().ok());

    validate_ids = {0, 1, 2, 3, 4, 5, 6}, seq_ids = {0, 3, 3, 4, 4, 4, 4};
    expected = {1, 0, 0, 0, 1, -1, -1};
    for (uint32_t i = 0; i < validate_ids.size(); i++) {
        ASSERT_EQ(expected[i], iter_validate_ids_test3.valid(validate_ids[i]));
        ASSERT_EQ(seq_ids[i], iter_validate_ids_test3.seq_id);
    }

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("name: James || tags: != gold", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_validate_ids_not_equals_filter_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(),
                                                                             filter_tree_root);
    ASSERT_TRUE(iter_validate_ids_not_equals_filter_test.init_status().ok());

    validate_ids = {0, 1, 2, 3, 4, 5, 6};
    seq_ids = {1, 1, 3, 3, 5, 5, 5};
    expected = {0, 1, 0, 1, 0, 1, -1};
    for (uint32_t i = 0; i < validate_ids.size(); i++) {
        ASSERT_EQ(expected[i], iter_validate_ids_not_equals_filter_test.valid(validate_ids[i]));
        ASSERT_EQ(seq_ids[i], iter_validate_ids_not_equals_filter_test.seq_id);
    }

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: gold", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_compact_plist_contains_atleast_one_test1 = filter_result_iterator_t(coll->get_name(), coll->_get_index(),
                                                                                  filter_tree_root);
    ASSERT_TRUE(iter_compact_plist_contains_atleast_one_test1.init_status().ok());

    std::vector<uint32_t> ids = {1, 3, 5};
    std::vector<uint32_t> offset_index = {0, 3, 6};
    std::vector<uint32_t> offsets = {0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* c_list1 = compact_posting_list_t::create(3, &ids[0], &offset_index[0], 9, &offsets[0]);
    ASSERT_FALSE(iter_compact_plist_contains_atleast_one_test1.contains_atleast_one(SET_COMPACT_POSTING(c_list1)));
    free(c_list1);

    auto iter_compact_plist_contains_atleast_one_test2 = filter_result_iterator_t(coll->get_name(), coll->_get_index(),
                                                                                  filter_tree_root);
    ASSERT_TRUE(iter_compact_plist_contains_atleast_one_test2.init_status().ok());

    ids = {1, 3, 4};
    offset_index = {0, 3, 6};
    offsets = {0, 3, 4, 0, 3, 4, 0, 3, 4};

    compact_posting_list_t* c_list2 = compact_posting_list_t::create(3, &ids[0], &offset_index[0], 9, &offsets[0]);
    ASSERT_TRUE(iter_compact_plist_contains_atleast_one_test2.contains_atleast_one(SET_COMPACT_POSTING(c_list2)));
    free(c_list2);

    auto iter_plist_contains_atleast_one_test1 = filter_result_iterator_t(coll->get_name(), coll->_get_index(),
                                                                                  filter_tree_root);
    ASSERT_TRUE(iter_plist_contains_atleast_one_test1.init_status().ok());

    posting_list_t p_list1(2);
    ids = {1, 3, 5};
    for (const auto &i: ids) {
        p_list1.upsert(i, {1, 2, 3});
    }

    ASSERT_FALSE(iter_plist_contains_atleast_one_test1.contains_atleast_one(&p_list1));

    auto iter_plist_contains_atleast_one_test2 = filter_result_iterator_t(coll->get_name(), coll->_get_index(),
                                                                          filter_tree_root);
    ASSERT_TRUE(iter_plist_contains_atleast_one_test2.init_status().ok());

    posting_list_t p_list2(2);
    ids = {1, 3, 4};
    for (const auto &i: ids) {
        p_list1.upsert(i, {1, 2, 3});
    }

    ASSERT_TRUE(iter_plist_contains_atleast_one_test2.contains_atleast_one(&p_list1));

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags:= [gold, silver]", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_reset_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_reset_test.init_status().ok());

    expected = {0, 2, 3, 4};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_reset_test.is_valid);
        ASSERT_EQ(i, iter_reset_test.seq_id);
        iter_reset_test.next();
    }
    ASSERT_FALSE(iter_reset_test.is_valid);

    iter_reset_test.reset();

    for (auto const& i : expected) {
        ASSERT_TRUE(iter_reset_test.is_valid);
        ASSERT_EQ(i, iter_reset_test.seq_id);
        iter_reset_test.next();
    }
    ASSERT_FALSE(iter_reset_test.is_valid);

    auto iter_move_assignment_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);

    iter_reset_test.reset();
    iter_move_assignment_test = std::move(iter_reset_test);

    expected = {0, 2, 3, 4};
    for (auto const& i : expected) {
        ASSERT_TRUE(iter_move_assignment_test.is_valid);
        ASSERT_EQ(i, iter_move_assignment_test.seq_id);
        iter_move_assignment_test.next();
    }
    ASSERT_FALSE(iter_move_assignment_test.is_valid);

    delete filter_tree_root;
    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: gold", coll->get_schema(), store, doc_id_prefix,
                                                filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_to_array_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_to_array_test.init_status().ok());

    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length;

    filter_ids_length = iter_to_array_test.to_filter_id_array(filter_ids);
    ASSERT_EQ(3, filter_ids_length);

    expected = {0, 2, 4};
    for (uint32_t i = 0; i < filter_ids_length; i++) {
        ASSERT_EQ(expected[i], filter_ids[i]);
    }
    ASSERT_FALSE(iter_to_array_test.is_valid);

    delete[] filter_ids;

    auto iter_and_scalar_test = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_and_scalar_test.init_status().ok());

    uint32_t a_ids[6] = {0, 1, 3, 4, 5, 6};
    uint32_t* and_result = nullptr;
    uint32_t and_result_length;
    and_result_length = iter_and_scalar_test.and_scalar(a_ids, 6, and_result);
    ASSERT_EQ(2, and_result_length);

    expected = {0, 4};
    for (uint32_t i = 0; i < and_result_length; i++) {
        ASSERT_EQ(expected[i], and_result[i]);
    }
    ASSERT_FALSE(iter_and_scalar_test.is_valid);

    delete[] and_result;
    delete filter_tree_root;

    doc = R"({
            "name": "James Rowdy",
            "age": 36,
            "years": [2005, 2022],
            "rating": 6.03,
            "tags": ["FINE PLATINUM"]
        })"_json;
    add_op = coll->add(doc.dump());
    ASSERT_TRUE(add_op.ok());

    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: != FINE PLATINUM", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_skip_test3 = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_skip_test3.init_status().ok());

    ASSERT_TRUE(iter_skip_test3.is_valid);
    iter_skip_test3.skip_to(4);
    ASSERT_EQ(4, iter_skip_test3.seq_id);

    ASSERT_TRUE(iter_skip_test3.is_valid);

    delete filter_tree_root;

    filter_tree_root = nullptr;
    filter_op = filter::parse_filter_query("tags: != gold", coll->get_schema(), store, doc_id_prefix,
                                           filter_tree_root);
    ASSERT_TRUE(filter_op.ok());

    auto iter_skip_test4 = filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    ASSERT_TRUE(iter_skip_test4.init_status().ok());

    ASSERT_TRUE(iter_skip_test4.is_valid);
    iter_skip_test4.skip_to(6);
    ASSERT_EQ(6, iter_skip_test4.seq_id);
    ASSERT_TRUE(iter_skip_test4.is_valid);

    auto iter_add_phrase_ids_test = new filter_result_iterator_t(coll->get_name(), coll->_get_index(), filter_tree_root);
    std::unique_ptr<filter_result_iterator_t> filter_iter_guard(iter_add_phrase_ids_test);
    ASSERT_TRUE(iter_add_phrase_ids_test->init_status().ok());

    auto phrase_ids = new uint32_t[4];
    for (uint32_t i = 0; i < 4; i++) {
        phrase_ids[i] = i * 2;
    }
    filter_result_iterator_t::add_phrase_ids(iter_add_phrase_ids_test, phrase_ids, 4);
    filter_iter_guard.release();
    filter_iter_guard.reset(iter_add_phrase_ids_test);

    ASSERT_TRUE(iter_add_phrase_ids_test->is_valid);
    ASSERT_EQ(6, iter_add_phrase_ids_test->seq_id);

    delete filter_tree_root;
}