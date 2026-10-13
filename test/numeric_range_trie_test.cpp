#include <gtest/gtest.h>
#include <algorithm>
#include <collection_manager.h>
#include <limits>
#include "collection.h"
#include "numeric_range_trie.h"

class NumericRangeTrieTest : public ::testing::Test {
protected:
    Store *store;
    CollectionManager & collectionManager = CollectionManager::get_instance();
    std::atomic<bool> quit = false;

    std::vector<std::string> query_fields;
    std::vector<sort_by> sort_fields;

    void setupCollection() {
        std::string state_dir_path = "/tmp/typesense_test/collection_filtering";
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

void reset(uint32_t*& ids, uint32_t& ids_length) {
    delete [] ids;
    ids = nullptr;
    ids_length = 0;
}

std::vector<uint32_t> collect_expanded_posting_ids(const std::vector<void*>& raw_posting_lists) {
    std::vector<id_list_t*> id_lists;
    std::vector<id_list_t*> expanded_id_lists;
    ids_t::to_expanded_id_lists(raw_posting_lists, id_lists, expanded_id_lists);

    std::vector<uint32_t> ids;
    for (const auto& id_list : id_lists) {
        auto iterator = id_list->new_iterator();
        while (iterator.valid()) {
            ids.push_back(iterator.id());
            iterator.next();
        }
    }

    for (const auto& expanded_id_list : expanded_id_lists) {
        delete expanded_id_list;
    }

    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    return ids;
}

enum class numeric_trie_comparison_t {
    RANGE,
    LESS_THAN,
    GREATER_THAN,
    EQUAL_TO,
};

struct numeric_trie_query_t {
    numeric_trie_comparison_t comparison;
    int64_t low_or_value;
    bool low_inclusive = true;
    int64_t high = 0;
    bool high_inclusive = true;
};

std::vector<uint32_t> eager_numeric_trie_ids(NumericTrie& trie, const numeric_trie_query_t& query) {
    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;
    switch (query.comparison) {
        case numeric_trie_comparison_t::RANGE:
            trie.search_range(query.low_or_value, query.low_inclusive, query.high, query.high_inclusive,
                              ids, ids_length);
            break;
        case numeric_trie_comparison_t::LESS_THAN:
            trie.search_less_than(query.low_or_value, query.low_inclusive, ids, ids_length);
            break;
        case numeric_trie_comparison_t::GREATER_THAN:
            trie.search_greater_than(query.low_or_value, query.low_inclusive, ids, ids_length);
            break;
        case numeric_trie_comparison_t::EQUAL_TO:
            trie.search_equal_to(query.low_or_value, ids, ids_length);
            break;
    }

    std::vector<uint32_t> result;
    if (ids_length != 0) {
        result.assign(ids, ids + ids_length);
    }
    delete [] ids;
    return result;
}

std::vector<uint32_t> raw_numeric_trie_ids(NumericTrie& trie, const numeric_trie_query_t& query) {
    std::vector<void*> raw_posting_lists;
    switch (query.comparison) {
        case numeric_trie_comparison_t::RANGE:
            trie.search_range(query.low_or_value, query.low_inclusive, query.high, query.high_inclusive,
                              raw_posting_lists);
            break;
        case numeric_trie_comparison_t::LESS_THAN:
            trie.search_less_than(query.low_or_value, query.low_inclusive, raw_posting_lists);
            break;
        case numeric_trie_comparison_t::GREATER_THAN:
            trie.search_greater_than(query.low_or_value, query.low_inclusive, raw_posting_lists);
            break;
        case numeric_trie_comparison_t::EQUAL_TO:
            trie.search_equal_to(query.low_or_value, raw_posting_lists);
            break;
    }

    return collect_expanded_posting_ids(raw_posting_lists);
}

std::vector<uint32_t> iterator_numeric_trie_ids(NumericTrie& trie, const numeric_trie_query_t& query) {
    NumericTrie::iterator_t iterator = [&]() {
        switch (query.comparison) {
            case numeric_trie_comparison_t::RANGE:
                return trie.search_range(query.low_or_value, query.low_inclusive, query.high, query.high_inclusive);
            case numeric_trie_comparison_t::LESS_THAN:
                return trie.search_less_than(query.low_or_value, query.low_inclusive);
            case numeric_trie_comparison_t::GREATER_THAN:
                return trie.search_greater_than(query.low_or_value, query.low_inclusive);
            case numeric_trie_comparison_t::EQUAL_TO:
                return trie.search_equal_to(query.low_or_value);
        }
        throw std::logic_error("Unknown numeric trie comparison");
    }();

    std::vector<uint32_t> ids;
    while (iterator.is_valid) {
        ids.push_back(iterator.seq_id);
        iterator.next();
    }
    return ids;
}

TEST_F(NumericRangeTrieTest, ExposesMatchingRawPostings) {
    NumericTrie trie;
    for (const auto& pair : std::vector<std::pair<int64_t, uint32_t>>{
            {-10, 1}, {-5, 2}, {0, 3}, {5, 4}, {10, 5},
    }) {
        trie.insert(pair.first, pair.second);
    }

    std::vector<void*> raw_posting_lists;
    trie.search_range(-5, false, 5, true, raw_posting_lists);
    ASSERT_EQ((std::vector<uint32_t>{3, 4}), collect_expanded_posting_ids(raw_posting_lists));
    ASSERT_FALSE(raw_posting_lists.empty());
    ASSERT_TRUE(IS_COMPACT_IDS(raw_posting_lists.front()));

    raw_posting_lists.clear();
    trie.search_less_than(0, false, raw_posting_lists);
    ASSERT_EQ((std::vector<uint32_t>{1, 2}), collect_expanded_posting_ids(raw_posting_lists));

    raw_posting_lists.clear();
    trie.search_greater_than(5, false, raw_posting_lists);
    ASSERT_EQ((std::vector<uint32_t>{5}), collect_expanded_posting_ids(raw_posting_lists));

    for (uint32_t id = 100; id < 165; id++) {
        trie.insert(42, id);
    }

    raw_posting_lists.clear();
    trie.search_equal_to(42, raw_posting_lists);
    ASSERT_EQ(1, raw_posting_lists.size());
    ASSERT_FALSE(IS_COMPACT_IDS(raw_posting_lists.front()));

    std::vector<uint32_t> expected_ids;
    for (uint32_t id = 100; id < 165; id++) {
        expected_ids.push_back(id);
    }
    ASSERT_EQ(expected_ids, collect_expanded_posting_ids(raw_posting_lists));

    const auto raw_posting_count = raw_posting_lists.size();
    trie.search_equal_to(42, raw_posting_lists);
    ASSERT_EQ(raw_posting_count * 2, raw_posting_lists.size());
}

TEST_F(NumericRangeTrieTest, RawPostingQueriesMatchEagerQueriesAtBoundaries) {
    for (const auto bits : std::vector<char>{1, 4, 8}) {
        NumericTrie trie(bits * 8);
        const auto limit = bits == 1 ? int64_t{0xFF} :
                           bits == 4 ? int64_t{0xFFFFFFFF} : std::numeric_limits<int64_t>::max();
        for (const auto& pair : std::vector<std::pair<int64_t, uint32_t>>{
                {-limit, 1}, {-1, 2}, {0, 3}, {1, 4}, {limit, 5},
        }) {
            trie.insert(pair.first, pair.second);
        }

        const std::vector<numeric_trie_query_t> queries = {
                {numeric_trie_comparison_t::RANGE, 1, true, limit, true},
                {numeric_trie_comparison_t::RANGE, -1, false, 1, true},
                {numeric_trie_comparison_t::RANGE, 1, true, -1, true},
                {numeric_trie_comparison_t::RANGE, std::numeric_limits<int64_t>::min(), true,
                 std::numeric_limits<int64_t>::max(), true},
                {numeric_trie_comparison_t::RANGE, std::numeric_limits<int64_t>::min(), false, -limit, false},
                {numeric_trie_comparison_t::RANGE, limit, false, std::numeric_limits<int64_t>::max(), true},
                {numeric_trie_comparison_t::LESS_THAN, std::numeric_limits<int64_t>::min(), true},
                {numeric_trie_comparison_t::LESS_THAN, -limit, false},
                {numeric_trie_comparison_t::LESS_THAN, -1, true},
                {numeric_trie_comparison_t::LESS_THAN, 0, false},
                {numeric_trie_comparison_t::LESS_THAN, limit, false},
                {numeric_trie_comparison_t::LESS_THAN, std::numeric_limits<int64_t>::max(), true},
                {numeric_trie_comparison_t::GREATER_THAN, std::numeric_limits<int64_t>::min(), true},
                {numeric_trie_comparison_t::GREATER_THAN, -limit, false},
                {numeric_trie_comparison_t::GREATER_THAN, -1, false},
                {numeric_trie_comparison_t::GREATER_THAN, 0, true},
                {numeric_trie_comparison_t::GREATER_THAN, limit, false},
                {numeric_trie_comparison_t::GREATER_THAN, std::numeric_limits<int64_t>::max(), true},
                {numeric_trie_comparison_t::EQUAL_TO, -limit},
                {numeric_trie_comparison_t::EQUAL_TO, 0},
                {numeric_trie_comparison_t::EQUAL_TO, limit},
                {numeric_trie_comparison_t::EQUAL_TO, std::numeric_limits<int64_t>::min()},
                {numeric_trie_comparison_t::EQUAL_TO, std::numeric_limits<int64_t>::max()},
        };

        for (const auto& query : queries) {
            ASSERT_EQ(eager_numeric_trie_ids(trie, query), raw_numeric_trie_ids(trie, query));
        }

        if (bits < 8) {
            const auto out_of_width = limit + 1;
            const numeric_trie_query_t range_query{numeric_trie_comparison_t::RANGE,
                                                    out_of_width, true, out_of_width, true};
            const numeric_trie_query_t less_than_query{numeric_trie_comparison_t::LESS_THAN, out_of_width, true};
            const numeric_trie_query_t greater_than_query{numeric_trie_comparison_t::GREATER_THAN, out_of_width, true};
            const numeric_trie_query_t equal_to_query{numeric_trie_comparison_t::EQUAL_TO, out_of_width};
            ASSERT_EQ(eager_numeric_trie_ids(trie, range_query), raw_numeric_trie_ids(trie, range_query));
            ASSERT_EQ(eager_numeric_trie_ids(trie, less_than_query), raw_numeric_trie_ids(trie, less_than_query));
            ASSERT_EQ(eager_numeric_trie_ids(trie, greater_than_query), raw_numeric_trie_ids(trie, greater_than_query));
            ASSERT_EQ(eager_numeric_trie_ids(trie, equal_to_query), raw_numeric_trie_ids(trie, equal_to_query));
        }
    }
}

TEST_F(NumericRangeTrieTest, SupportsInt64Extrema) {
    NumericTrie trie(64);
    const auto minimum = std::numeric_limits<int64_t>::min();
    const auto maximum = std::numeric_limits<int64_t>::max();
    trie.insert(minimum, 11);
    trie.insert(-1, 12);
    trie.insert(0, 13);
    trie.insert(1, 14);
    trie.insert(maximum, 15);

    const std::vector<std::pair<numeric_trie_query_t, std::vector<uint32_t>>> queries = {
            {{numeric_trie_comparison_t::EQUAL_TO, minimum}, {11}},
            {{numeric_trie_comparison_t::RANGE, minimum, true, minimum, true}, {11}},
            {{numeric_trie_comparison_t::RANGE, minimum, true, maximum, true}, {11, 12, 13, 14, 15}},
            {{numeric_trie_comparison_t::LESS_THAN, minimum, true}, {11}},
            {{numeric_trie_comparison_t::LESS_THAN, minimum, false}, {}},
            {{numeric_trie_comparison_t::GREATER_THAN, maximum, true}, {15}},
            {{numeric_trie_comparison_t::GREATER_THAN, maximum, false}, {}},
    };

    for (const auto& query : queries) {
        ASSERT_EQ(query.second, eager_numeric_trie_ids(trie, query.first));
        ASSERT_EQ(query.second, iterator_numeric_trie_ids(trie, query.first));
        ASSERT_EQ(query.second, raw_numeric_trie_ids(trie, query.first));
    }

    trie.remove(minimum, 11);
    const numeric_trie_query_t equality_query{numeric_trie_comparison_t::EQUAL_TO, minimum};
    const numeric_trie_query_t range_query{numeric_trie_comparison_t::RANGE, minimum, true, minimum, true};
    ASSERT_TRUE(eager_numeric_trie_ids(trie, equality_query).empty());
    ASSERT_TRUE(iterator_numeric_trie_ids(trie, equality_query).empty());
    ASSERT_TRUE(raw_numeric_trie_ids(trie, equality_query).empty());
    ASSERT_TRUE(eager_numeric_trie_ids(trie, range_query).empty());
    ASSERT_TRUE(iterator_numeric_trie_ids(trie, range_query).empty());
    ASSERT_TRUE(raw_numeric_trie_ids(trie, range_query).empty());
}

TEST_F(NumericRangeTrieTest, SearchRange) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-0x03010101, 1},
            {-0x01010101, 5},
            {-32768, 43},
            {-24576, 35},
            {-16384, 32},
            {-8192, 8},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91},
            {0x01010101, 53},
            {0x03010101, 12},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_range(32768, true, -32768, true, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_range(-32768, true, 32768, true, ids, ids_length);

    std::vector<uint32_t> expected = {8, 32, 35, 43, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-32768, true, 32768, false, ids, ids_length);

    expected = {8, 32, 35, 43, 49, 56, 58};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-32768, true, 0x01000000, true, ids, ids_length);

    expected = {8, 32, 35, 43, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-32768, true, 0x0101010101, true, ids, ids_length);

    expected = {8, 12, 32, 35, 43, 49, 53, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-32768, true, 0, true, ids, ids_length);

    expected = {8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-32768, true, 0, false, ids, ids_length);

    expected = {8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-32768, false, 32768, true, ids, ids_length);

    expected = {8, 32, 35, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-0x01000000, true, 32768, true, ids, ids_length);

    expected = {8, 32, 35, 43, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-0x0101010101, true, 32768, true, ids, ids_length);

    expected = {1, 5, 8, 32, 35, 43, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-0x01000000, true, 0x01000000, true, ids, ids_length);

    expected = {8, 32, 35, 43, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-1, true, 32768, true, ids, ids_length);

    expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-1, false, 32768, true, ids, ids_length);

    expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-1, true, 0, true, ids, ids_length);
    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_range(-1, false, 0, false, ids, ids_length);
    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_range(8192, true, 32768, true, ids, ids_length);

    expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(8192, true, 0x01000000, true, ids, ids_length);

    expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(16384, true, 16384, true, ids, ids_length);

    ASSERT_EQ(1, ids_length);
    ASSERT_EQ(56, ids[0]);

    reset(ids, ids_length);
    trie->search_range(16384, true, 16384, false, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_range(16384, false, 16384, true, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_range(16383, true, 16383, true, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_range(8193, true, 16383, true, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_range(-32768, true, -8192, true, ids, ids_length);

    expected = {8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-0x0101010101, true, -8192, true, ids, ids_length);

    expected = {1, 5, 8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(32768, true, 0x0101010101, true, ids, ids_length);

    expected = {12, 53, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
}

TEST_F(NumericRangeTrieTest, SearchGreaterThan) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-32768, 43},
            {-24576, 35},
            {-16384, 32},
            {-8192, 8},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_greater_than(0, true, ids, ids_length);

    std::vector<uint32_t> expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(-1, false, ids, ids_length);

    expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(-1, true, ids, ids_length);

    expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(-24576, true, ids, ids_length);

    expected = {8, 32, 35, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(-32768, false, ids, ids_length);

    expected = {8, 32, 35, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(8192, true, ids, ids_length);

    expected = {49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(8192, false, ids, ids_length);

    expected = {56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(1000000, false, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_greater_than(-0x01000000, false, ids, ids_length);

    expected = {8, 32, 35, 43, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);

    pairs = {
            {0x01010101, 53},
            {0x03010101, 12},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    trie->search_greater_than(0x01010101, true, ids, ids_length);

    expected = {12, 53};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);

    trie->search_greater_than(0x0101010101, true, ids, ids_length);

    ASSERT_EQ(0, ids_length);
}

TEST_F(NumericRangeTrieTest, SearchLessThan) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-32768, 8},
            {-24576, 32},
            {-16384, 35},
            {-8192, 43},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_less_than(0, true, ids, ids_length);

    std::vector<uint32_t> expected = {8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(0, false, ids, ids_length);

    expected = {8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(-1, true, ids, ids_length);

    expected = {8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(-16384, true, ids, ids_length);

    expected = {8, 32, 35};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(-16384, false, ids, ids_length);

    expected = {8, 32};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(8192, true, ids, ids_length);

    expected = {8, 32, 35, 43, 49};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(8192, false, ids, ids_length);

    expected = {8, 32, 35, 43};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(-0x01000000, false, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_less_than(0x01000000, true, ids, ids_length);

    expected = {8, 32, 35, 43, 49, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);

    pairs = {
            {0x01010101, 53},
            {0x03010101, 12},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    trie->search_less_than(0x01010101010, true, ids, ids_length);

    expected = {8, 12, 32, 35, 43, 49, 53, 56, 58, 91};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);

    pairs = {
            {-0x03010101, 1},
            {-0x01010101, 5},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    trie->search_less_than(-0x01010101, true, ids, ids_length);

    expected = {1, 5};
    ASSERT_EQ(expected.size(), ids_length);
    for (uint32_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);

    trie->search_less_than(-0x0101010101, true, ids, ids_length);

    ASSERT_EQ(0, ids_length);
}

TEST_F(NumericRangeTrieTest, SearchEqualTo) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int64_t, uint32_t>> pairs = {
            {-8192, 8},
            {-16384, 32},
            {-24576, 35},
            {-32769, 41},
            {-32768, 43},
            {-32767, 45},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91},
            {0x01010101, 68},
            {0x0100000000, 68}
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_equal_to(0, ids, ids_length);

    ASSERT_EQ(0, ids_length);

    reset(ids, ids_length);
    trie->search_equal_to(-32768, ids, ids_length);

    ASSERT_EQ(1, ids_length);
    ASSERT_EQ(43, ids[0]);

    reset(ids, ids_length);
    trie->search_equal_to(24576, ids, ids_length);

    ASSERT_EQ(1, ids_length);
    ASSERT_EQ(58, ids[0]);

    reset(ids, ids_length);
    trie->search_equal_to(0x01010101, ids, ids_length);

    ASSERT_EQ(1, ids_length);
    ASSERT_EQ(68, ids[0]);

    reset(ids, ids_length);
    trie->search_equal_to(0x0101010101, ids, ids_length);
    ASSERT_EQ(0, ids_length);
}

TEST_F(NumericRangeTrieTest, IterateSearchEqualTo) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-8192, 8},
            {-16384, 32},
            {-24576, 35},
            {-32769, 41},
            {-32768, 43},
            {-32767, 45},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {24576, 60},
            {32768, 91}
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    auto iterator = trie->search_equal_to(0);
    ASSERT_EQ(false, iterator.is_valid);

    iterator = trie->search_equal_to(0x202020);
    ASSERT_EQ(false, iterator.is_valid);

    iterator = trie->search_equal_to(-32768);
    ASSERT_EQ(true, iterator.is_valid);
    ASSERT_EQ(43, iterator.seq_id);

    iterator.next();
    ASSERT_EQ(false, iterator.is_valid);

    iterator = trie->search_equal_to(24576);
    ASSERT_EQ(true, iterator.is_valid);
    ASSERT_EQ(58, iterator.seq_id);

    iterator.next();
    ASSERT_EQ(true, iterator.is_valid);
    ASSERT_EQ(60, iterator.seq_id);

    iterator.next();
    ASSERT_EQ(false, iterator.is_valid);


    iterator.reset();
    ASSERT_EQ(true, iterator.is_valid);
    ASSERT_EQ(58, iterator.seq_id);

    iterator.skip_to(4);
    ASSERT_EQ(true, iterator.is_valid);
    ASSERT_EQ(58, iterator.seq_id);

    iterator.skip_to(59);
    ASSERT_EQ(true, iterator.is_valid);
    ASSERT_EQ(60, iterator.seq_id);

    iterator.skip_to(66);
    ASSERT_EQ(false, iterator.is_valid);
}

TEST_F(NumericRangeTrieTest, MultivalueData) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-0x202020, 32},
            {-32768, 5},
            {-32768, 8},
            {-24576, 32},
            {-16384, 35},
            {-8192, 43},
            {0, 43},
            {0, 49},
            {1, 8},
            {256, 91},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91},
            {0x202020, 35},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_less_than(0, false, ids, ids_length);

    std::vector<uint32_t> expected = {5, 8, 32, 35, 43};

    ASSERT_EQ(5, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(-16380, false, ids, ids_length);

    ASSERT_EQ(4, ids_length);

    expected = {5, 8, 32, 35};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_less_than(16384, false, ids, ids_length);

    ASSERT_EQ(7, ids_length);

    expected = {5, 8, 32, 35, 43, 49, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(0, true, ids, ids_length);

    ASSERT_EQ(7, ids_length);

    expected = {8, 35, 43, 49, 56, 58, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(256, true, ids, ids_length);

    ASSERT_EQ(5, ids_length);

    expected = {35, 49, 56, 58, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_greater_than(-32768, true, ids, ids_length);

    ASSERT_EQ(9, ids_length);

    expected = {5, 8, 32, 35, 43, 49, 56, 58, 91};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_range(-32768, true, 0, true, ids, ids_length);

    ASSERT_EQ(6, ids_length);

    expected = {5, 8, 32, 35, 43, 49};
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
}

TEST_F(NumericRangeTrieTest, Remove) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);
    std::vector<std::pair<int32_t, uint32_t>> pairs = {
            {-0x202020, 32},
            {-32768, 5},
            {-32768, 8},
            {-24576, 32},
            {-16384, 35},
            {-8192, 43},
            {0, 2},
            {0, 49},
            {1, 8},
            {256, 91},
            {8192, 49},
            {16384, 56},
            {24576, 58},
            {32768, 91},
            {0x202020, 35},
            {0x01010101, 68},
    };

    for (auto const& pair: pairs) {
        trie->insert(pair.first, pair.second);
    }

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_less_than(0, false, ids, ids_length);

    std::vector<uint32_t> expected = {5, 8, 32, 35, 43};

    ASSERT_EQ(5, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->remove(-24576, 32);
    trie->remove(-0x202020, 32);

    reset(ids, ids_length);
    trie->search_less_than(0, false, ids, ids_length);

    expected = {5, 8, 35, 43};
    ASSERT_EQ(4, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    reset(ids, ids_length);
    trie->search_equal_to(0, ids, ids_length);

    expected = {2, 49};
    ASSERT_EQ(2, ids_length);
    for (uint32_t i = 0; i < ids_length; i++) {
        ASSERT_EQ(expected[i], ids[i]);
    }

    trie->remove(0, 2);

    reset(ids, ids_length);
    trie->search_equal_to(0, ids, ids_length);

    ASSERT_EQ(1, ids_length);
    ASSERT_EQ(49, ids[0]);

    reset(ids, ids_length);

    trie->remove(0x0101010101, 68);

    trie->search_equal_to(16843009, ids, ids_length);

    ASSERT_EQ(1, ids_length);
    ASSERT_EQ(68, ids[0]);

    reset(ids, ids_length);
}

TEST_F(NumericRangeTrieTest, EmptyTrieOperations) {
    auto trie = new NumericTrie();
    std::unique_ptr<NumericTrie> trie_guard(trie);

    uint32_t* ids = nullptr;
    uint32_t ids_length = 0;

    trie->search_range(-32768, true, 32768, true, ids, ids_length);
    std::unique_ptr<uint32_t[]> ids_guard(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(-32768, true, -1, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_range(1, true, 32768, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_greater_than(0, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_greater_than(15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_greater_than(-15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_less_than(0, false, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_less_than(-15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_less_than(15, true, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->search_equal_to(15, ids, ids_length);
    ids_guard.reset(ids);

    ASSERT_EQ(0, ids_length);

    trie->remove(15, 0);
    trie->remove(-15, 0);
}

TEST_F(NumericRangeTrieTest, Integration) {
    Collection *coll_array_fields;

    std::ifstream infile(std::string(ROOT_DIR)+"test/numeric_array_documents.jsonl");
    std::vector<field> fields = {
            field("name", field_types::STRING, false),
            field("rating", field_types::FLOAT, false),
            field("age", field_types::INT32, false, false, true, "", -1, -1, false, 0, 0, cosine, "", nlohmann::json(),
                  true), // Setting range index true.
            field("years", field_types::INT32_ARRAY, false),
            field("timestamps", field_types::INT64_ARRAY, false,  false, true, "", -1, -1, false, 0, 0, cosine, "",
                  nlohmann::json(), true),
            field("tags", field_types::STRING_ARRAY, true)
    };

    std::vector<sort_by> sort_fields = { sort_by("age", "DESC") };

    coll_array_fields = collectionManager.get_collection("coll_array_fields").get();
    if(coll_array_fields == nullptr) {
        // ensure that default_sorting_field is a non-array numerical field
        auto coll_op = collectionManager.create_collection("coll_array_fields", 4, fields, "years");
        ASSERT_EQ(false, coll_op.ok());
        ASSERT_STREQ("Default sorting field `years` is not a sortable type.", coll_op.error().c_str());

        // let's try again properly
        coll_op = collectionManager.create_collection("coll_array_fields", 4, fields, "age");
        coll_array_fields = coll_op.get();
    }

    std::string json_line;

    while (std::getline(infile, json_line)) {
        auto add_op = coll_array_fields->add(json_line);
        ASSERT_TRUE(add_op.ok());
    }

    infile.close();

    query_fields = {"name"};
    std::vector<std::string> facets;
    // Searching on an int32 field
    nlohmann::json results = coll_array_fields->search("Jeremy", query_fields, "age:>24", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    std::vector<std::string> ids = {"3", "1", "4"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    // searching on an int64 array field - also ensure that padded space causes no issues
    results = coll_array_fields->search("Jeremy", query_fields, "timestamps : > 475205222", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(4, results["hits"].size());

    ids = {"1", "4", "0", "2"};

    for(size_t i = 0; i < results["hits"].size(); i++) {
        nlohmann::json result = results["hits"].at(i);
        std::string result_id = result["document"]["id"];
        std::string id = ids.at(i);
        ASSERT_STREQ(id.c_str(), result_id.c_str());
    }

    results = coll_array_fields->search("Jeremy", query_fields, "rating: [7.812 .. 9.999, 1.05 .. 1.09]", facets, sort_fields, {0}, 10, 1, FREQUENCY, {false}).get();
    ASSERT_EQ(3, results["hits"].size());

    auto coll_json = coll_array_fields->get_summary_json();
    ASSERT_TRUE(coll_json["fields"][2]["range_index"]);
    ASSERT_TRUE(coll_json["fields"][4]["range_index"]);
}
