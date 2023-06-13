#include <timsort.hpp>
#include "numeric_range_trie_test.h"
#include "array_utils.h"

void NumericTrie::insert(const int64_t& value, const uint32_t& seq_id) {
    if (value < 0) {
        if (negative_trie == nullptr) {
            negative_trie = new NumericTrie::Node();
        }

        negative_trie->insert(std::abs(value), seq_id, max_level);
    } else {
        if (positive_trie == nullptr) {
            positive_trie = new NumericTrie::Node();
        }

        positive_trie->insert(value, seq_id, max_level);
    }
}

void NumericTrie::insert_geopoint(const uint64_t& cell_id, const uint32_t& seq_id) {
    if (positive_trie == nullptr) {
        positive_trie = new NumericTrie::Node();
    }

    positive_trie->insert_geopoint(cell_id, seq_id, max_level);
}

void NumericTrie::search_geopoint(const uint64_t& cell_id, uint32_t*& ids, uint32_t& ids_length) {
    if (positive_trie == nullptr) {
        return;
    }

    positive_trie->search_geopoint(cell_id, max_level, ids, ids_length);
}

void NumericTrie::delete_geopoint(const uint64_t& cell_id, uint32_t id) {
    if (positive_trie == nullptr) {
        return;
    }

    positive_trie->delete_geopoint(cell_id, id, max_level);
}

void NumericTrie::search_range(const int64_t& low, const bool& low_inclusive,
                               const int64_t& high, const bool& high_inclusive,
                               uint32_t*& ids, uint32_t& ids_length) {
    if (low > high) {
        return;
    }

    if (low < 0 && high >= 0) {
        // Have to combine the results of >low from negative_trie and <high from positive_trie

        if (negative_trie != nullptr && !(low == -1 && !low_inclusive)) { // No need to search for (-1, ...
            uint32_t* negative_ids = nullptr;
            uint32_t negative_ids_length = 0;
            auto abs_low = std::abs(low);

            // Since we store absolute values, search_lesser would yield result for >low from negative_trie.
            negative_trie->search_less_than(low_inclusive ? abs_low : abs_low - 1, max_level,
                                            negative_ids, negative_ids_length);

            uint32_t* out = nullptr;
            ids_length = ArrayUtils::or_scalar(negative_ids, negative_ids_length, ids, ids_length, &out);

            delete [] negative_ids;
            delete [] ids;
            ids = out;
        }

        if (positive_trie != nullptr && !(high == 0 && !high_inclusive)) { // No need to search for ..., 0)
            uint32_t* positive_ids = nullptr;
            uint32_t positive_ids_length = 0;
            positive_trie->search_less_than(high_inclusive ? high : high - 1, max_level,
                                            positive_ids, positive_ids_length);

            uint32_t* out = nullptr;
            ids_length = ArrayUtils::or_scalar(positive_ids, positive_ids_length, ids, ids_length, &out);

            delete [] positive_ids;
            delete [] ids;
            ids = out;
        }
    } else if (low >= 0) {
        // Search only in positive_trie
        if (positive_trie == nullptr) {
            return;
        }

        uint32_t* positive_ids = nullptr;
        uint32_t positive_ids_length = 0;
        positive_trie->search_range(low_inclusive ? low : low + 1, high_inclusive ? high : high - 1, max_level,
                                    positive_ids, positive_ids_length);

        uint32_t* out = nullptr;
        ids_length = ArrayUtils::or_scalar(positive_ids, positive_ids_length, ids, ids_length, &out);

        delete [] positive_ids;
        delete [] ids;
        ids = out;
    } else {
        // Search only in negative_trie
        if (negative_trie == nullptr) {
            return;
        }

        uint32_t* negative_ids = nullptr;
        uint32_t negative_ids_length = 0;
        // Since we store absolute values, switching low and high would produce the correct result.
        auto abs_high = std::abs(high), abs_low = std::abs(low);
        negative_trie->search_range(high_inclusive ? abs_high : abs_high + 1, low_inclusive ? abs_low : abs_low - 1,
                                    max_level,
                                    negative_ids, negative_ids_length);

        uint32_t* out = nullptr;
        ids_length = ArrayUtils::or_scalar(negative_ids, negative_ids_length, ids, ids_length, &out);

        delete [] negative_ids;
        delete [] ids;
        ids = out;
    }
}

NumericTrie::iterator_t NumericTrie::search_range(const int64_t& low, const bool& low_inclusive,
                                                  const int64_t& high, const bool& high_inclusive) {
    std::vector<Node*> matches;
    if (low > high) {
        return NumericTrie::iterator_t(matches);
    }

    if (low < 0 && high >= 0) {
        // Have to combine the results of >low from negative_trie and <high from positive_trie

        if (negative_trie != nullptr && !(low == -1 && !low_inclusive)) { // No need to search for (-1, ...
            auto abs_low = std::abs(low);
            // Since we store absolute values, search_lesser would yield result for >low from negative_trie.
            negative_trie->search_less_than(low_inclusive ? abs_low : abs_low - 1, max_level, matches);
        }

        if (positive_trie != nullptr && !(high == 0 && !high_inclusive)) { // No need to search for ..., 0)
            positive_trie->search_less_than(high_inclusive ? high : high - 1, max_level, matches);
        }
    } else if (low >= 0) {
        // Search only in positive_trie
        if (positive_trie == nullptr) {
            return NumericTrie::iterator_t(matches);
        }

        positive_trie->search_range(low_inclusive ? low : low + 1, high_inclusive ? high : high - 1, max_level, matches);
    } else {
        // Search only in negative_trie
        if (negative_trie == nullptr) {
            return NumericTrie::iterator_t(matches);
        }

        auto abs_high = std::abs(high), abs_low = std::abs(low);
        // Since we store absolute values, switching low and high would produce the correct result.
        negative_trie->search_range(high_inclusive ? abs_high : abs_high + 1, low_inclusive ? abs_low : abs_low - 1,
                                    max_level, matches);
    }

    return NumericTrie::iterator_t(matches);
}

void NumericTrie::search_greater_than(const int64_t& value, const bool& inclusive, uint32_t*& ids, uint32_t& ids_length) {
    if ((value == 0 && inclusive) || (value == -1 && !inclusive)) { // [0, ∞), (-1, ∞)
        if (positive_trie != nullptr) {
            uint32_t* positive_ids = nullptr;
            uint32_t positive_ids_length = 0;
            positive_trie->get_all_ids(positive_ids, positive_ids_length);

            uint32_t* out = nullptr;
            ids_length = ArrayUtils::or_scalar(positive_ids, positive_ids_length, ids, ids_length, &out);

            delete [] positive_ids;
            delete [] ids;
            ids = out;
        }
        return;
    }

    if (value >= 0) {
        if (positive_trie == nullptr) {
            return;
        }

        uint32_t* positive_ids = nullptr;
        uint32_t positive_ids_length = 0;
        positive_trie->search_greater_than(inclusive ? value : value + 1, max_level, positive_ids, positive_ids_length);

        uint32_t* out = nullptr;
        ids_length = ArrayUtils::or_scalar(positive_ids, positive_ids_length, ids, ids_length, &out);

        delete [] positive_ids;
        delete [] ids;
        ids = out;
    } else {
        // Have to combine the results of >value from negative_trie and all the ids in positive_trie

        if (negative_trie != nullptr) {
            uint32_t* negative_ids = nullptr;
            uint32_t negative_ids_length = 0;
            auto abs_low = std::abs(value);

            // Since we store absolute values, search_lesser would yield result for >value from negative_trie.
            negative_trie->search_less_than(inclusive ? abs_low : abs_low - 1, max_level,
                                            negative_ids, negative_ids_length);

            uint32_t* out = nullptr;
            ids_length = ArrayUtils::or_scalar(negative_ids, negative_ids_length, ids, ids_length, &out);

            delete [] negative_ids;
            delete [] ids;
            ids = out;
        }

        if (positive_trie == nullptr) {
            return;
        }

        uint32_t* positive_ids = nullptr;
        uint32_t positive_ids_length = 0;
        positive_trie->get_all_ids(positive_ids, positive_ids_length);

        uint32_t* out = nullptr;
        ids_length = ArrayUtils::or_scalar(positive_ids, positive_ids_length, ids, ids_length, &out);

        delete [] positive_ids;
        delete [] ids;
        ids = out;
    }
}

NumericTrie::iterator_t NumericTrie::search_greater_than(const int64_t& value, const bool& inclusive) {
    std::vector<Node*> matches;

    if ((value == 0 && inclusive) || (value == -1 && !inclusive)) { // [0, ∞), (-1, ∞)
        if (positive_trie != nullptr) {
            matches.push_back(positive_trie);
        }
        return NumericTrie::iterator_t(matches);
    }

    if (value >= 0) {
        if (positive_trie != nullptr) {
            positive_trie->search_greater_than(inclusive ? value : value + 1, max_level, matches);
        }
    } else {
        // Have to combine the results of >value from negative_trie and all the ids in positive_trie
        if (negative_trie != nullptr) {
            auto abs_low = std::abs(value);
            // Since we store absolute values, search_lesser would yield result for >value from negative_trie.
            negative_trie->search_less_than(inclusive ? abs_low : abs_low - 1, max_level, matches);
        }
        if (positive_trie != nullptr) {
            matches.push_back(positive_trie);
        }
    }

    return NumericTrie::iterator_t(matches);
}

void NumericTrie::search_less_than(const int64_t& value, const bool& inclusive, uint32_t*& ids, uint32_t& ids_length) {
    if ((value == 0 && !inclusive) || (value == -1 && inclusive)) { // (-∞, 0), (-∞, -1]
        if (negative_trie != nullptr) {
            uint32_t* negative_ids = nullptr;
            uint32_t negative_ids_length = 0;
            negative_trie->get_all_ids(negative_ids, negative_ids_length);

            uint32_t* out = nullptr;
            ids_length = ArrayUtils::or_scalar(negative_ids, negative_ids_length, ids, ids_length, &out);

            delete [] negative_ids;
            delete [] ids;
            ids = out;
        }
        return;
    }

    if (value < 0) {
        if (negative_trie == nullptr) {
            return;
        }

        uint32_t* negative_ids = nullptr;
        uint32_t negative_ids_length = 0;
        auto abs_low = std::abs(value);

        // Since we store absolute values, search_greater would yield result for <value from negative_trie.
        negative_trie->search_greater_than(inclusive ? abs_low : abs_low + 1, max_level,
                                           negative_ids, negative_ids_length);

        uint32_t* out = nullptr;
        ids_length = ArrayUtils::or_scalar(negative_ids, negative_ids_length, ids, ids_length, &out);

        delete [] negative_ids;
        delete [] ids;
        ids = out;
    } else {
        // Have to combine the results of <value from positive_trie and all the ids in negative_trie

        if (positive_trie != nullptr) {
            uint32_t* positive_ids = nullptr;
            uint32_t positive_ids_length = 0;
            positive_trie->search_less_than(inclusive ? value : value - 1, max_level,
                                            positive_ids, positive_ids_length);

            uint32_t* out = nullptr;
            ids_length = ArrayUtils::or_scalar(positive_ids, positive_ids_length, ids, ids_length, &out);

            delete [] positive_ids;
            delete [] ids;
            ids = out;
        }

        if (negative_trie == nullptr) {
            return;
        }

        uint32_t* negative_ids = nullptr;
        uint32_t negative_ids_length = 0;
        negative_trie->get_all_ids(negative_ids, negative_ids_length);

        uint32_t* out = nullptr;
        ids_length = ArrayUtils::or_scalar(negative_ids, negative_ids_length, ids, ids_length, &out);

        delete [] negative_ids;
        delete [] ids;
        ids = out;
    }
}

NumericTrie::iterator_t NumericTrie::search_less_than(const int64_t& value, const bool& inclusive) {
    std::vector<Node*> matches;

    if ((value == 0 && !inclusive) || (value == -1 && inclusive)) { // (-∞, 0), (-∞, -1]
        if (negative_trie != nullptr) {
            matches.push_back(negative_trie);
        }
        return NumericTrie::iterator_t(matches);
    }

    if (value < 0) {
        if (negative_trie != nullptr) {
            auto abs_low = std::abs(value);
            // Since we store absolute values, search_greater would yield result for <value from negative_trie.
            negative_trie->search_greater_than(inclusive ? abs_low : abs_low + 1, max_level, matches);
        }
    } else {
        // Have to combine the results of <value from positive_trie and all the ids in negative_trie
        if (positive_trie != nullptr) {
            positive_trie->search_less_than(inclusive ? value : value - 1, max_level, matches);
        }
        if (negative_trie != nullptr) {
            matches.push_back(negative_trie);
        }
    }

    return NumericTrie::iterator_t(matches);
}

void NumericTrie::search_equal_to(const int64_t& value, uint32_t*& ids, uint32_t& ids_length) {
    if ((value < 0 && negative_trie == nullptr) || (value >= 0 && positive_trie == nullptr)) {
        return;
    }

    uint32_t* equal_ids = nullptr;
    uint32_t equal_ids_length = 0;

    if (value < 0) {
        negative_trie->search_equal_to(std::abs(value), max_level, equal_ids, equal_ids_length);
    } else {
        positive_trie->search_equal_to(value, max_level, equal_ids, equal_ids_length);
    }

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(equal_ids, equal_ids_length, ids, ids_length, &out);

    delete [] equal_ids;
    delete [] ids;
    ids = out;
}

NumericTrie::iterator_t NumericTrie::search_equal_to(const int64_t& value) {
    std::vector<Node*> matches;
    if (value < 0 && negative_trie != nullptr) {
        negative_trie->search_equal_to(std::abs(value), max_level, matches);
    } else if (value >= 0 && positive_trie != nullptr) {
        positive_trie->search_equal_to(value, max_level, matches);
    }

    return NumericTrie::iterator_t(matches);
}

void NumericTrie::Node::insert(const int64_t& cell_id, const uint32_t& seq_id, const char& max_level) {
    char level = 0;
    return insert_helper(cell_id, seq_id, level, max_level);
}

void NumericTrie::Node::insert_geopoint(const uint64_t& cell_id, const uint32_t& seq_id, const char& max_level) {
    char level = 0;
    return insert_geopoint_helper(cell_id, seq_id, level, max_level);
}

inline int get_index(const int64_t& value, const char& level, const char& max_level) {
    // Values are index considering higher order of the bytes first.
    // 0x01020408 (16909320) would be indexed in the trie as follows:
    // Level   Index
    //   1       1
    //   2       2
    //   3       4
    //   4       8
    return (value >> (8 * (max_level - level))) & 0xFF;
}

inline int get_geopoint_index(const uint64_t& cell_id, const char& level, const char& max_level) {
    return (cell_id >> (8 * (max_level - level))) & 0xFF;
}

void NumericTrie::Node::insert_helper(const int64_t& value, const uint32_t& seq_id, char& level, const char& max_level) {
    if (level > max_level) {
        return;
    }

    // Root node contains all the sequence ids present in the tree.
    if (!seq_ids.contains(seq_id)) {
        seq_ids.append(seq_id);
    }

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
    if (!seq_ids.contains(seq_id)) {
        seq_ids.append(seq_id);
    }

    if (++level <= max_level) {
        if (children == nullptr) {
            children = new NumericTrie::Node* [EXPANSE]{nullptr};
        }

        auto index = get_geopoint_index(cell_id, level, max_level);
        if (children[index] == nullptr) {
            children[index] = new NumericTrie::Node();
        }

        return children[index]->insert_geopoint_helper(cell_id, seq_id, level, max_level);
    }
}

char get_max_search_level(const uint64_t& cell_id, const char& max_level) {
    // For cell id 0x47E66C3000000000, we only have to prefix match the top four bytes since rest of the bytes are 0.
    // So the max search level would be 4 in this case.

    uint64_t mask = 0xff;
    char i = max_level;
    while (((cell_id & mask) == 0) && --i > 0) {
        mask <<= 8;
    }

    return i;
}

void NumericTrie::Node::search_geopoint(const uint64_t& cell_id, const char& max_index_level,
                                        uint32_t*& ids, uint32_t& ids_length) {
    char level = 1;
    Node* root = this;
    auto index = get_geopoint_index(cell_id, level, max_index_level);
    auto max_search_level = get_max_search_level(cell_id, max_index_level);

    while (level < max_search_level) {
        if (root->children == nullptr || root->children[index] == nullptr) {
            return;
        }

        root = root->children[index];
        index = get_geopoint_index(cell_id, ++level, max_index_level);
    }

    root->get_all_ids(ids, ids_length);
}

void NumericTrie::Node::delete_geopoint(const uint64_t& cell_id, uint32_t id, const char& max_level) {
    char level = 1;
    Node* root = this;
    auto index = get_geopoint_index(cell_id, level, max_level);

    while (level < max_level) {
        root->seq_ids.remove_value(id);

        if (root->children == nullptr || root->children[index] == nullptr) {
            return;
        }

        root = root->children[index];
        index = get_geopoint_index(cell_id, ++level, max_level);
    }

    if (root->children != nullptr || root->children[index] != nullptr) {
        delete root->children[index];
        root->children[index] = nullptr;
    }
}

void NumericTrie::Node::get_all_ids(uint32_t*& ids, uint32_t& ids_length) {
    ids = seq_ids.uncompress();
    ids_length = seq_ids.getLength();
}

void NumericTrie::Node::search_less_than(const int64_t& value, const char& max_level,
                                         uint32_t*& ids, uint32_t& ids_length) {
    char level = 0;
    std::vector<NumericTrie::Node*> matches;
    search_less_than_helper(value, level, max_level, matches);

    std::vector<uint32_t> consolidated_ids;
    for (auto const& match: matches) {
        auto const& m_seq_ids = match->seq_ids.uncompress();
        for (uint32_t i = 0; i < match->seq_ids.getLength(); i++) {
            consolidated_ids.push_back(m_seq_ids[i]);
        }

        delete [] m_seq_ids;
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
    consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                       ids, ids_length, &out);

    delete [] ids;
    ids = out;
}

void NumericTrie::Node::search_less_than(const int64_t& value, const char& max_level, std::vector<Node*>& matches) {
    char level = 0;
    search_less_than_helper(value, level, max_level, matches);
}

void NumericTrie::Node::search_less_than_helper(const int64_t& value, char& level, const char& max_level,
                                                std::vector<Node*>& matches) {
    if (level == max_level) {
        matches.push_back(this);
        return;
    } else if (level > max_level || children == nullptr) {
        return;
    }

    auto index = get_index(value, ++level, max_level);
    if (children[index] != nullptr) {
        children[index]->search_less_than_helper(value, level, max_level, matches);
    }

    while (--index >= 0) {
        if (children[index] != nullptr) {
            matches.push_back(children[index]);
        }
    }

    --level;
}

void NumericTrie::Node::search_range(const int64_t& low, const int64_t& high, const char& max_level,
                                     uint32_t*& ids, uint32_t& ids_length) {
    if (low > high) {
        return;
    }
    std::vector<NumericTrie::Node*> matches;
    search_range_helper(low, high, max_level, matches);

    std::vector<uint32_t> consolidated_ids;
    for (auto const& match: matches) {
        auto const& m_seq_ids = match->seq_ids.uncompress();
        for (uint32_t i = 0; i < match->seq_ids.getLength(); i++) {
            consolidated_ids.push_back(m_seq_ids[i]);
        }

        delete [] m_seq_ids;
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
    consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                       ids, ids_length, &out);

    delete [] ids;
    ids = out;
}

void NumericTrie::Node::search_range(const int64_t& low, const int64_t& high, const char& max_level,
                                     std::vector<Node*>& matches) {
    if (low > high) {
        return;
    }

    search_range_helper(low, high, max_level, matches);
}

void NumericTrie::Node::search_range_helper(const int64_t& low,const int64_t& high, const char& max_level,
                                            std::vector<Node*>& matches) {
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
        if (root->children[low_index] != nullptr) {
            matches.push_back(root->children[low_index]);
        }
        return;
    }

    if (root->children[low_index] != nullptr) {
        // Collect all the sub-nodes that are greater than low.
        root->children[low_index]->search_greater_than_helper(low, level, max_level, matches);
    }

    auto index = low_index + 1;
    // All the nodes in-between low and high are a match by default.
    while (index < std::min(high_index, (int)EXPANSE)) {
        if (root->children[index] != nullptr) {
            matches.push_back(root->children[index]);
        }

        index++;
    }

    if (index < EXPANSE && index == high_index && root->children[index] != nullptr) {
        // Collect all the sub-nodes that are lesser than high.
        root->children[index]->search_less_than_helper(high, level, max_level, matches);
    }
}

void NumericTrie::Node::search_greater_than(const int64_t& value, const char& max_level,
                                            uint32_t*& ids, uint32_t& ids_length) {
    char level = 0;
    std::vector<NumericTrie::Node*> matches;
    search_greater_than_helper(value, level, max_level, matches);

    std::vector<uint32_t> consolidated_ids;
    for (auto const& match: matches) {
        auto const& m_seq_ids = match->seq_ids.uncompress();
        for (uint32_t i = 0; i < match->seq_ids.getLength(); i++) {
            consolidated_ids.push_back(m_seq_ids[i]);
        }

        delete [] m_seq_ids;
    }

    gfx::timsort(consolidated_ids.begin(), consolidated_ids.end());
    consolidated_ids.erase(unique(consolidated_ids.begin(), consolidated_ids.end()), consolidated_ids.end());

    uint32_t* out = nullptr;
    ids_length = ArrayUtils::or_scalar(&consolidated_ids[0], consolidated_ids.size(),
                                       ids, ids_length, &out);

    delete [] ids;
    ids = out;
}

void NumericTrie::Node::search_greater_than(const int64_t& value, const char& max_level, std::vector<Node*>& matches) {
    char level = 0;
    search_greater_than_helper(value, level, max_level, matches);
}

void NumericTrie::Node::search_greater_than_helper(const int64_t& value, char& level, const char& max_level,
                                                   std::vector<Node*>& matches) {
    if (level == max_level) {
        matches.push_back(this);
        return;
    } else if (level > max_level || children == nullptr) {
        return;
    }

    auto index = get_index(value, ++level, max_level);
    if (children[index] != nullptr) {
        children[index]->search_greater_than_helper(value, level, max_level, matches);
    }

    while (++index < EXPANSE) {
        if (children[index] != nullptr) {
            matches.push_back(children[index]);
        }
    }

    --level;
}

void NumericTrie::Node::search_equal_to(const int64_t& value, const char& max_level,
                                        uint32_t*& ids, uint32_t& ids_length) {
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

void NumericTrie::Node::search_equal_to(const int64_t& value, const char& max_level, std::vector<Node*>& matches) {
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

