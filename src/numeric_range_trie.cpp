#include <timsort.hpp>
#include <set>
#include "numeric_range_trie.h"
#include "array_utils.h"

inline uint64_t indexable_limit(const char& max_level);

uint64_t magnitude(const int64_t value) {
    return value < 0 ? static_cast<uint64_t>(-(value + 1)) + 1 : static_cast<uint64_t>(value);
}

int64_t minimum_indexed_value(const uint64_t limit) {
    return limit == (uint64_t{1} << 63) ? std::numeric_limits<int64_t>::min() : -static_cast<int64_t>(limit);
}

int64_t maximum_indexed_value(const uint64_t limit) {
    return static_cast<int64_t>(std::min(limit, static_cast<uint64_t>(std::numeric_limits<int64_t>::max())));
}

void NumericTrie::insert(const int64_t& value, const uint32_t& seq_id) {
    const auto limit = indexable_limit(max_level);
    if (magnitude(value) > limit) {
        return;
    }

    if (value < 0) {
        if (negative_trie == nullptr) {
            negative_trie = new NumericTrie::Node();
        }

        negative_trie->insert(magnitude(value), seq_id, max_level);
    } else {
        if (positive_trie == nullptr) {
            positive_trie = new NumericTrie::Node();
        }

        positive_trie->insert(value, seq_id, max_level);
    }
}

void NumericTrie::remove(const int64_t& value, const uint32_t& seq_id) {
    const auto limit = indexable_limit(max_level);
    if (magnitude(value) > limit) {
        return;
    }

    if ((value < 0 && negative_trie == nullptr) || (value >= 0 && positive_trie == nullptr)) {
        return;
    }

    if (value < 0) {
        negative_trie->remove(magnitude(value), seq_id, max_level);
    } else {
        positive_trie->remove(value, seq_id, max_level);
    }
}

void NumericTrie::insert_geopoint(const uint64_t& cell_id, const uint32_t& seq_id) {
    if (positive_trie == nullptr) {
        positive_trie = new NumericTrie::Node();
    }

    positive_trie->insert_geopoint(cell_id, seq_id, max_level);
}

void NumericTrie::search_geopoints(const std::vector<uint64_t>& cell_ids, std::vector<uint32_t>& geo_result_ids) {
    if (positive_trie == nullptr) {
        return;
    }

    positive_trie->search_geopoints(cell_ids, max_level, geo_result_ids);
}

void NumericTrie::delete_geopoint(const uint64_t& cell_id, uint32_t id) {
    if (positive_trie == nullptr) {
        return;
    }

    positive_trie->delete_geopoint(cell_id, id, max_level);
}

void NumericTrie::append_matching_postings(const std::vector<Node*>& matches,
                                           std::vector<void*>& raw_posting_lists) {
    for (const auto& match : matches) {
        raw_posting_lists.push_back(match->get_seq_ids());
    }
}

void NumericTrie::merge_matching_ids(const std::vector<Node*>& matches,
                                     uint32_t*& ids, uint32_t& ids_length) {
    std::vector<uint32_t> merged_ids;
    for (const auto& match : matches) {
        auto posting = match->get_seq_ids();
        ids_t::uncompress(posting, merged_ids);
    }

    if (merged_ids.empty()) {
        return;
    }

    gfx::timsort(merged_ids.begin(), merged_ids.end());
    merged_ids.erase(std::unique(merged_ids.begin(), merged_ids.end()), merged_ids.end());

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(merged_ids.data(), merged_ids.size(), ids, ids_length, &out);
    delete [] ids;
    ids = out;
}

void NumericTrie::collect_range_matches(int64_t low, bool low_inclusive, int64_t high, bool high_inclusive,
                                        std::vector<Node*>& matches) {
    const auto limit = indexable_limit(max_level);
    const auto minimum = minimum_indexed_value(limit);
    const auto maximum = maximum_indexed_value(limit);
    if (low > high || high < minimum || low > maximum) {
        return;
    }

    if (low < minimum) {
        low = minimum;
        low_inclusive = true;
    }
    if (high > maximum) {
        high = maximum;
        high_inclusive = true;
    }
    if (low > high || (low == high && (!low_inclusive || !high_inclusive))) {
        return;
    }

    if (low < 0 && high >= 0) {
        if (negative_trie != nullptr && !(low == -1 && !low_inclusive)) {
            const auto absolute_low = magnitude(low);
            negative_trie->search_less_than(low_inclusive ? absolute_low : absolute_low - 1, max_level, matches);
        }

        if (positive_trie != nullptr && !(high == 0 && !high_inclusive)) {
            positive_trie->search_less_than(high_inclusive ? high : high - 1, max_level, matches);
        }
    } else if (low >= 0) {
        if (positive_trie != nullptr) {
            positive_trie->search_range(low_inclusive ? low : low + 1, high_inclusive ? high : high - 1,
                                        max_level, matches);
        }
    } else if (negative_trie != nullptr) {
        const auto absolute_high = magnitude(high);
        const auto absolute_low = magnitude(low);
        negative_trie->search_range(high_inclusive ? absolute_high : absolute_high + 1,
                                    low_inclusive ? absolute_low : absolute_low - 1, max_level, matches);
    }
}

void NumericTrie::collect_greater_than_matches(int64_t value, bool inclusive, std::vector<Node*>& matches) {
    const auto limit = indexable_limit(max_level);
    const auto minimum = minimum_indexed_value(limit);
    const auto maximum = maximum_indexed_value(limit);
    if (value > maximum || (value == maximum && !inclusive)) {
        return;
    }
    if (value < minimum) {
        if (negative_trie != nullptr) {
            matches.push_back(negative_trie);
        }
        if (positive_trie != nullptr) {
            matches.push_back(positive_trie);
        }
        return;
    }

    if ((value == 0 && inclusive) || (value == -1 && !inclusive)) {
        if (positive_trie != nullptr) {
            matches.push_back(positive_trie);
        }
    } else if (value >= 0) {
        if (positive_trie != nullptr) {
            positive_trie->search_greater_than(inclusive ? value : value + 1, max_level, matches);
        }
    } else {
        if (negative_trie != nullptr) {
            const auto absolute_value = magnitude(value);
            negative_trie->search_less_than(inclusive ? absolute_value : absolute_value - 1, max_level, matches);
        }
        if (positive_trie != nullptr) {
            matches.push_back(positive_trie);
        }
    }
}

void NumericTrie::collect_less_than_matches(int64_t value, bool inclusive, std::vector<Node*>& matches) {
    const auto limit = indexable_limit(max_level);
    const auto minimum = minimum_indexed_value(limit);
    const auto maximum = maximum_indexed_value(limit);
    if (value < minimum || (value == minimum && !inclusive)) {
        return;
    }
    if (value > maximum || (value == maximum && inclusive)) {
        if (negative_trie != nullptr) {
            matches.push_back(negative_trie);
        }
        if (positive_trie != nullptr) {
            matches.push_back(positive_trie);
        }
        return;
    }

    if ((value == 0 && !inclusive) || (value == -1 && inclusive)) {
        if (negative_trie != nullptr) {
            matches.push_back(negative_trie);
        }
    } else if (value < 0) {
        if (negative_trie != nullptr) {
            const auto absolute_value = magnitude(value);
            negative_trie->search_greater_than(inclusive ? absolute_value : absolute_value + 1, max_level, matches);
        }
    } else {
        if (positive_trie != nullptr) {
            positive_trie->search_less_than(inclusive ? value : value - 1, max_level, matches);
        }
        if (negative_trie != nullptr) {
            matches.push_back(negative_trie);
        }
    }
}

void NumericTrie::collect_equal_to_matches(int64_t value, std::vector<Node*>& matches) {
    const auto limit = indexable_limit(max_level);
    if (magnitude(value) > limit) {
        return;
    }

    if (value < 0 && negative_trie != nullptr) {
        negative_trie->search_equal_to(magnitude(value), max_level, matches);
    } else if (value >= 0 && positive_trie != nullptr) {
        positive_trie->search_equal_to(value, max_level, matches);
    }
}

void NumericTrie::search_range(const int64_t& low, const bool& low_inclusive,
                               const int64_t& high, const bool& high_inclusive,
                               uint32_t*& ids, uint32_t& ids_length) {
    std::vector<Node*> matches;
    collect_range_matches(low, low_inclusive, high, high_inclusive, matches);
    merge_matching_ids(matches, ids, ids_length);
}

NumericTrie::iterator_t NumericTrie::search_range(const int64_t& low, const bool& low_inclusive,
                                                  const int64_t& high, const bool& high_inclusive) {
    std::vector<Node*> matches;
    collect_range_matches(low, low_inclusive, high, high_inclusive, matches);
    return NumericTrie::iterator_t(matches);
}

void NumericTrie::search_range(const int64_t& low, const bool& low_inclusive,
                               const int64_t& high, const bool& high_inclusive,
                               std::vector<void*>& raw_posting_lists) {
    std::vector<Node*> matches;
    collect_range_matches(low, low_inclusive, high, high_inclusive, matches);
    append_matching_postings(matches, raw_posting_lists);
}

void NumericTrie::search_greater_than(const int64_t& value, const bool& inclusive, uint32_t*& ids, uint32_t& ids_length) {
    std::vector<Node*> matches;
    collect_greater_than_matches(value, inclusive, matches);
    merge_matching_ids(matches, ids, ids_length);
}

NumericTrie::iterator_t NumericTrie::search_greater_than(const int64_t& value, const bool& inclusive) {
    std::vector<Node*> matches;
    collect_greater_than_matches(value, inclusive, matches);
    return NumericTrie::iterator_t(matches);
}

void NumericTrie::search_greater_than(const int64_t& value, const bool& inclusive,
                                      std::vector<void*>& raw_posting_lists) {
    std::vector<Node*> matches;
    collect_greater_than_matches(value, inclusive, matches);
    append_matching_postings(matches, raw_posting_lists);
}

void NumericTrie::search_less_than(const int64_t& value, const bool& inclusive, uint32_t*& ids, uint32_t& ids_length) {
    std::vector<Node*> matches;
    collect_less_than_matches(value, inclusive, matches);
    merge_matching_ids(matches, ids, ids_length);
}

NumericTrie::iterator_t NumericTrie::search_less_than(const int64_t& value, const bool& inclusive) {
    std::vector<Node*> matches;
    collect_less_than_matches(value, inclusive, matches);
    return NumericTrie::iterator_t(matches);
}

void NumericTrie::search_less_than(const int64_t& value, const bool& inclusive,
                                   std::vector<void*>& raw_posting_lists) {
    std::vector<Node*> matches;
    collect_less_than_matches(value, inclusive, matches);
    append_matching_postings(matches, raw_posting_lists);
}

void NumericTrie::search_equal_to(const int64_t& value, uint32_t*& ids, uint32_t& ids_length) {
    std::vector<Node*> matches;
    collect_equal_to_matches(value, matches);
    merge_matching_ids(matches, ids, ids_length);
}

NumericTrie::iterator_t NumericTrie::search_equal_to(const int64_t& value) {
    std::vector<Node*> matches;
    collect_equal_to_matches(value, matches);
    return NumericTrie::iterator_t(matches);
}

void NumericTrie::search_equal_to(const int64_t& value, std::vector<void*>& raw_posting_lists) {
    std::vector<Node*> matches;
    collect_equal_to_matches(value, matches);
    append_matching_postings(matches, raw_posting_lists);
}

void NumericTrie::seq_ids_outside_top_k(const size_t& k, std::vector<uint32_t>& result) {
    size_t ids_skipped = 0;
    if (negative_trie != nullptr && positive_trie != nullptr) {
        positive_trie->seq_ids_outside_top_k(k, max_level, ids_skipped, result);

        if (ids_skipped < k) { // Haven't skipped k ids yet, would need to skip ids in negative trie also.
            negative_trie->seq_ids_outside_top_k(k, max_level, ids_skipped, result, true);
            return;
        }

        negative_trie->get_all_ids(result);
    } else if (negative_trie != nullptr) {
        negative_trie->seq_ids_outside_top_k(k, max_level, ids_skipped, result, true);
    } else {
        positive_trie->seq_ids_outside_top_k(k, max_level, ids_skipped, result);
    }
}

size_t NumericTrie::size() {
    size_t size = 0;
    if (negative_trie != nullptr) {
        size += negative_trie->get_ids_length();
    }
    if (positive_trie != nullptr) {
        size += positive_trie->get_ids_length();
    }

    return size;
}


inline uint64_t indexable_limit(const char& max_level) {
    switch (max_level) {
        case 1:
            return 0xFF;
        case 2:
            return 0xFFFF;
        case 3:
            return 0xFFFFFF;
        case 4:
            return 0xFFFFFFFF;
        case 5:
            return 0xFFFFFFFFFF;
        case 6:
            return 0xFFFFFFFFFFFF;
        case 7:
            return 0xFFFFFFFFFFFFFF;
        case 8:
            return uint64_t{1} << 63;
        default:
            return 0;
    }
}

void NumericTrie::Node::insert(const uint64_t& value, const uint32_t& seq_id, const char& max_level) {
    if (value > indexable_limit(max_level)) {
        return;
    }

    char level = 0;
    return insert_helper(value, seq_id, level, max_level);
}

void NumericTrie::Node::insert_geopoint(const uint64_t& cell_id, const uint32_t& seq_id, const char& max_level) {
    char level = 0;
    return insert_geopoint_helper(cell_id, seq_id, level, max_level);
}

inline short get_index(const uint64_t& value, const char& level, const char& max_level) {
    // Values are index considering higher order of the bytes first.
    // 0x01020408 (16909320) would be indexed in the trie as follows:
    // Level   Index
    //   1       1
    //   2       2
    //   3       4
    //   4       8
    return (value >> (8 * (max_level - level))) & 0xFF;
}

inline short get_geopoint_index(const uint64_t& cell_id, const char& level) {
    // Doing 8-level since cell_id is a 64 bit number.
    return (cell_id >> (8 * (8 - level))) & 0xFF;
}

void NumericTrie::Node::remove(const uint64_t& value, const uint32_t& id, const char& max_level) {
    if (value > indexable_limit(max_level)) {
        return;
    }

    char level = 1;
    Node* root = this;
    auto index = get_index(value, level, max_level);

    while (level < max_level) {
        ids_t::erase(root->seq_ids, id);

        if (root->children == nullptr || root->children[index] == nullptr) {
            return;
        }

        root = root->children[index];
        index = get_index(value, ++level, max_level);
    }

    ids_t::erase(root->seq_ids, id);
    if (root->children != nullptr && root->children[index] != nullptr) {
        auto& child = root->children[index];

        ids_t::erase(child->seq_ids, id);
        if (child->get_ids_length() == 0) {
            delete child;
            child = nullptr;
        }
    }
}

void NumericTrie::Node::insert_helper(const uint64_t& value, const uint32_t& seq_id, char& level, const char& max_level) {
    if (level > max_level) {
        return;
    }

    // Root node contains all the sequence ids present in the tree.
    ids_t::upsert(seq_ids, seq_id);

    if (++level <= max_level) {
        if (children == nullptr) {
            children = new NumericTrie::Node* [EXPANSE]{nullptr};
        }

        auto index = get_index(value, level, max_level);
        if (children[index] == nullptr) {
            children[index] = new NumericTrie::Node();
        }

        return children[index]->insert_helper(value, seq_id, level, max_level);
    }
}

void NumericTrie::Node::insert_geopoint_helper(const uint64_t& cell_id, const uint32_t& seq_id, char& level,
                                               const char& max_level) {
    if (level > max_level) {
        return;
    }

    // Root node contains all the sequence ids present in the tree.
    ids_t::upsert(seq_ids, seq_id);

    if (++level <= max_level) {
        if (children == nullptr) {
            children = new NumericTrie::Node* [EXPANSE]{nullptr};
        }

        auto index = get_geopoint_index(cell_id, level);
        if (children[index] == nullptr) {
            children[index] = new NumericTrie::Node();
        }

        return children[index]->insert_geopoint_helper(cell_id, seq_id, level, max_level);
    }
}

char get_max_search_level(const uint64_t& cell_id, const char& max_level) {
    // For cell id 0x47E66C3000000000, we only have to prefix match the top four bytes since rest of the bytes are 0.
    // So the max search level would be 4 in this case.

    auto mask = (uint64_t) 0xFF << (8 * (8 - max_level)); // We're only indexing top 8-max_level bytes.
    char i = max_level;
    while (((cell_id & mask) == 0) && --i > 0) {
        mask <<= 8;
    }

    return i;
}

void NumericTrie::Node::search_geopoints_helper(const uint64_t& cell_id, const char& max_index_level,
                                                std::set<Node*>& matches, size_t& ids_length) {
    char level = 1;
    Node* root = this;
    auto index = get_geopoint_index(cell_id, level);
    auto max_search_level = get_max_search_level(cell_id, max_index_level);

    while (level < max_search_level) {
        if (root->children == nullptr || root->children[index] == nullptr) {
            return;
        }

        root = root->children[index];
        index = get_geopoint_index(cell_id, ++level);
    }

    ids_length += root->get_ids_length();
    matches.insert(root);
}

void NumericTrie::Node::search_geopoints(const std::vector<uint64_t>& cell_ids, const char& max_level,
                                         std::vector<uint32_t>& geo_result_ids) {
    std::set<Node*> matches;
    size_t ids_length = 0;
    for (const auto &cell_id: cell_ids) {
        search_geopoints_helper(cell_id, max_level, matches, ids_length);
    }

    geo_result_ids.reserve(geo_result_ids.size() + ids_length);
    for (auto const& match: matches) {
        ids_t::uncompress(match->seq_ids, geo_result_ids);
    }

    gfx::timsort(geo_result_ids.begin(), geo_result_ids.end());
    geo_result_ids.erase(unique(geo_result_ids.begin(), geo_result_ids.end()), geo_result_ids.end());
}

void NumericTrie::Node::delete_geopoint(const uint64_t& cell_id, uint32_t id, const char& max_level) {
    char level = 1;
    Node* root = this;
    auto index = get_geopoint_index(cell_id, level);

    while (level < max_level) {
        ids_t::erase(root->seq_ids, id);

        if (root->children == nullptr || root->children[index] == nullptr) {
            return;
        }

        root = root->children[index];
        index = get_geopoint_index(cell_id, ++level);
    }

    ids_t::erase(root->seq_ids, id);
    if (root->children != nullptr && root->children[index] != nullptr) {
        auto& child = root->children[index];

        ids_t::erase(child->seq_ids, id);
        if (child->get_ids_length() == 0) {
            delete child;
            child = nullptr;
        }
    }
}

void NumericTrie::Node::get_all_ids(uint32_t*& ids, uint32_t& ids_length) {
    ids = ids_t::uncompress(seq_ids);
    ids_length = ids_t::num_ids(seq_ids);
}

void NumericTrie::Node::search_less_than(const uint64_t& value, const char& max_level,
                                         uint32_t*& ids, uint32_t& ids_length) {
    if (value >= indexable_limit(max_level)) {
        get_all_ids(ids, ids_length);
        return;
    }

    char level = 0;
    std::vector<NumericTrie::Node*> matches;
    size_t consolidated_ids_length = 0;
    search_less_than_helper(value, level, max_level, matches, consolidated_ids_length);

    std::vector<uint32_t> consolidated_ids;
    consolidated_ids.reserve(consolidated_ids_length);
    for (auto const& match: matches) {
        ids_t::uncompress(match->seq_ids, consolidated_ids);
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
    consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                       ids, ids_length, &out);

    delete [] ids;
    ids = out;
}

void NumericTrie::Node::search_less_than(const uint64_t& value, const char& max_level, std::vector<Node*>& matches) {
    char level = 0;
    size_t dummy;
    search_less_than_helper(value, level, max_level, matches, dummy);
}

void NumericTrie::Node::search_less_than_helper(const uint64_t& value, char& level, const char& max_level,
                                                std::vector<Node*>& matches, size_t& ids_length) {
    if (level == max_level) {
        matches.push_back(this);
        ids_length += get_ids_length();
        return;
    } else if (level > max_level || children == nullptr) {
        return;
    }

    auto index = get_index(value, ++level, max_level);
    if (children[index] != nullptr) {
        children[index]->search_less_than_helper(value, level, max_level, matches, ids_length);
    }

    while (--index >= 0) {
        const auto& child = children[index];
        if (child != nullptr) {
            matches.push_back(child);
            ids_length += child->get_ids_length();
        }
    }

    --level;
}

void NumericTrie::Node::search_range(const uint64_t& low, const uint64_t& high, const char& max_level,
                                     uint32_t*& ids, uint32_t& ids_length) {
    if (low > high) {
        return;
    }
    std::vector<NumericTrie::Node*> matches;
    size_t consolidated_ids_length = 0;
    search_range_helper(low, high >= indexable_limit(max_level) ? indexable_limit(max_level) : high,
                        max_level, matches, consolidated_ids_length);

    std::vector<uint32_t> consolidated_ids;
    consolidated_ids.reserve(consolidated_ids_length);
    for (auto const& match: matches) {
        ids_t::uncompress(match->seq_ids, consolidated_ids);
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
    consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                       ids, ids_length, &out);

    delete [] ids;
    ids = out;
}

void NumericTrie::Node::search_range(const uint64_t& low, const uint64_t& high, const char& max_level,
                                     std::vector<Node*>& matches) {
    if (low > high) {
        return;
    }

    size_t dummy;
    search_range_helper(low, high, max_level, matches, dummy);
}

void NumericTrie::Node::search_range_helper(const uint64_t& low,const uint64_t& high, const char& max_level,
                                            std::vector<Node*>& matches, size_t& ids_length) {
    // Segregating the nodes into matching low, in-between, and matching high.

    NumericTrie::Node* root = this;
    char level = 1;
    auto low_index = get_index(low, level, max_level), high_index = get_index(high, level, max_level);

    // Keep updating the root while the range is contained within a single child node.
    while (root->children != nullptr && low_index == high_index && level < max_level) {
        if (root->children[low_index] == nullptr) {
            return;
        }

        root = root->children[low_index];
        level++;
        low_index = get_index(low, level, max_level);
        high_index = get_index(high, level, max_level);
    }

    if (root->children == nullptr) {
        return;
    } else if (low_index == high_index) { // low and high are equal
        const auto& child = root->children[low_index];
        if (child != nullptr) {
            matches.push_back(child);
            ids_length += child->get_ids_length();
        }
        return;
    }

    if (root->children[low_index] != nullptr) {
        // Collect all the sub-nodes that are greater than low.
        root->children[low_index]->search_greater_than_helper(low, level, max_level, matches, ids_length);
    }

    auto index = low_index + 1;
    // All the nodes in-between low and high are a match by default.
    for (; index < std::min(high_index, EXPANSE); index++) {
        const auto& child = root->children[index];
        if (child != nullptr) {
            matches.push_back(child);
            ids_length += child->get_ids_length();
        }
    }

    if (index < EXPANSE && index == high_index && root->children[index] != nullptr) {
        // Collect all the sub-nodes that are lesser than high.
        root->children[index]->search_less_than_helper(high, level, max_level, matches, ids_length);
    }
}

void NumericTrie::Node::search_greater_than(const uint64_t& value, const char& max_level,
                                            uint32_t*& ids, uint32_t& ids_length) {
    if (value >= indexable_limit(max_level)) {
        return;
    }

    char level = 0;
    std::vector<NumericTrie::Node*> matches;
    size_t consolidated_ids_length = 0;
    search_greater_than_helper(value, level, max_level, matches, consolidated_ids_length);

    std::vector<uint32_t> consolidated_ids;
    consolidated_ids.reserve(consolidated_ids_length);
    for (auto const& match: matches) {
        ids_t::uncompress(match->seq_ids, consolidated_ids);
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
    consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                       ids, ids_length, &out);

    delete [] ids;
    ids = out;
}

void NumericTrie::Node::search_greater_than(const uint64_t& value, const char& max_level, std::vector<Node*>& matches) {
    char level = 0;
    size_t dummy;
    search_greater_than_helper(value, level, max_level, matches, dummy);
}

void NumericTrie::Node::search_greater_than_helper(const uint64_t& value, char& level, const char& max_level,
                                                   std::vector<Node*>& matches, size_t& ids_length) {
    if (level == max_level) {
        matches.push_back(this);
        ids_length += get_ids_length();
        return;
    } else if (level > max_level || children == nullptr) {
        return;
    }

    auto index = get_index(value, ++level, max_level);
    if (children[index] != nullptr) {
        children[index]->search_greater_than_helper(value, level, max_level, matches, ids_length);
    }

    while (++index < EXPANSE) {
        const auto& child = children[index];
        if (child != nullptr) {
            matches.push_back(child);
            ids_length += child->get_ids_length();
        }
    }

    --level;
}

void NumericTrie::Node::search_equal_to(const uint64_t& value, const char& max_level,
                                        uint32_t*& ids, uint32_t& ids_length) {
    if (value > indexable_limit(max_level)) {
        return;
    }

    char level = 1;
    Node* root = this;
    auto index = get_index(value, level, max_level);

    while (level <= max_level) {
        if (root->children == nullptr || root->children[index] == nullptr) {
            return;
        }

        root = root->children[index];
        index = get_index(value, ++level, max_level);
    }

    root->get_all_ids(ids, ids_length);
}

void NumericTrie::Node::search_equal_to(const uint64_t& value, const char& max_level, std::vector<Node*>& matches) {
    char level = 1;
    Node* root = this;
    auto index = get_index(value, level, max_level);

    while (level <= max_level) {
        if (root->children == nullptr || root->children[index] == nullptr) {
            return;
        }

        root = root->children[index];
        index = get_index(value, ++level, max_level);
    }

    matches.push_back(root);
}

uint32_t NumericTrie::Node::get_ids_length() {
    return ids_t::num_ids(seq_ids);
}

void NumericTrie::Node::seq_ids_outside_top_k(const size_t& k,  const char& max_level, size_t& ids_skipped,
                                              std::vector<uint32_t>& result, const bool& is_negative) {
    char level = 0;
    seq_ids_outside_top_k_helper(k, ids_skipped, level, max_level, is_negative, result);
}

void NumericTrie::Node::seq_ids_outside_top_k_helper(const size_t& k, size_t& ids_skipped,
                                                     char& level, const char& max_level,
                                                     const bool& is_negative, std::vector<uint32_t>& result) {
    if (level == max_level) {
        std::vector<uint32_t> ids;
        get_all_ids(ids);

        for(size_t i = 0; i < ids.size(); i++) {
            if(ids_skipped + i >= k) {
                result.push_back(ids[i]);
            }
        }

        ids_skipped += ids.size();
        return;
    } else if (level > max_level) {
        return;
    }

    if (children == nullptr) {
        return;
    }

    short index = is_negative ? 0 : EXPANSE - 1; // Since we need to grab ids in descending order of their values.
    do {
        if (children[index] == nullptr) {
            continue;
        }
        if (ids_skipped + children[index]->get_ids_length() > k) {
            break;
        }

        ids_skipped += children[index]->get_ids_length();
    } while(is_negative ? (++index < EXPANSE) : (--index >= 0));

    if (is_negative ? (index >= EXPANSE) : (index < 0)) {
        return;
    }

    children[index]->seq_ids_outside_top_k_helper(k, ids_skipped, ++level, max_level, is_negative, result);
    --level;

    while (is_negative ? (++index < EXPANSE) : (--index >= 0)) {
        if (children[index] == nullptr) {
            continue;
        }

        children[index]->get_all_ids(result);
    }
}

void NumericTrie::Node::get_all_ids(std::vector<uint32_t>& result) {
    ids_t::uncompress(seq_ids, result);
}

void NumericTrie::iterator_t::reset() {
    for (auto& match: matches) {
        match->index = 0;
    }

    is_valid = true;
    set_seq_id();
}

void NumericTrie::iterator_t::skip_to(uint32_t id) {
    for (auto& match: matches) {
        ArrayUtils::skip_index_to_id(match->index, match->ids, match->ids_length, id);
    }

    set_seq_id();
}

void NumericTrie::iterator_t::next() {
    // Advance all the matches at seq_id.
    for (auto& match: matches) {
        if (match->index < match->ids_length && match->ids[match->index] == seq_id) {
            match->index++;
        }
    }

    set_seq_id();
}

NumericTrie::iterator_t::iterator_t(std::vector<Node*>& node_matches) {
    for (auto const& node_match: node_matches) {
        uint32_t* ids = nullptr;
        uint32_t ids_length;
        node_match->get_all_ids(ids, ids_length);
        if (ids_length > 0) {
            matches.emplace_back(new match_state(ids, ids_length));
        }
    }

    set_seq_id();
}

void NumericTrie::iterator_t::set_seq_id() {
    // Find the lowest id of all the matches and update the seq_id.
    bool one_is_valid = false;
    uint32_t lowest_id = UINT32_MAX;

    for (auto& match: matches) {
        if (match->index < match->ids_length) {
            one_is_valid = true;

            if (match->ids[match->index] < lowest_id) {
                lowest_id = match->ids[match->index];
            }
        }
    }

    if (one_is_valid) {
        seq_id = lowest_id;
    }

    is_valid = one_is_valid;
}

NumericTrie::iterator_t& NumericTrie::iterator_t::operator=(NumericTrie::iterator_t&& obj) noexcept {
    if (&obj == this)
        return *this;

    for (auto& match: matches) {
        delete match;
    }
    matches.clear();

    matches = std::move(obj.matches);
    seq_id = obj.seq_id;
    is_valid = obj.is_valid;

    return *this;
}
