#include "numeric_range_trie_test.h"
#include "array_utils.h"

void NumericTrie::insert(const int32_t& value, const uint32_t& seq_id) {
    if (value < 0) {
        if (negative_trie == nullptr) {
            negative_trie = new NumericTrieNode();
        }

        negative_trie->insert(std::abs(value), seq_id);
    } else {
        if (positive_trie == nullptr) {
            positive_trie = new NumericTrieNode();
        }

        positive_trie->insert(value, seq_id);
    }
}

void NumericTrie::search_range(const int32_t& low, const bool& low_inclusive,
                               const int32_t& high, const bool& high_inclusive,
                               uint32_t*& ids, uint32_t& ids_length) {
    if (low < 0 && high >= 0) {
        // Have to combine the results of >low from negative_trie and <high from positive_trie

        uint32_t* negative_ids = nullptr;
        uint32_t negative_ids_length = 0;
        if (!(low == -1 && !low_inclusive)) { // No need to search for (-1, ...
            auto abs_low = std::abs(low);
            // Since we store absolute values, search_lesser would yield result for >low from negative_trie.
            negative_trie->search_lesser(low_inclusive ? abs_low : abs_low - 1, negative_ids, negative_ids_length);
        }

        uint32_t* positive_ids = nullptr;
        uint32_t positive_ids_length = 0;
        if (!(high == 0 && !high_inclusive)) { // No need to search for ..., 0)
            positive_trie->search_lesser(high_inclusive ? high : high - 1, positive_ids, positive_ids_length);
        }

        ids_length = ArrayUtils::or_scalar(negative_ids, negative_ids_length, positive_ids, positive_ids_length, &ids);

        delete [] negative_ids;
        delete [] positive_ids;
        return;
    }
}

void NumericTrieNode::insert(const int32_t& value, const uint32_t& seq_id) {
    char level = 0;
    return insert(value, seq_id, level);
}

inline int get_index(const int32_t& value, char& level) {
    return (value >> (8 * (MAX_LEVEL - level))) & 0xFF;
}

void NumericTrieNode::insert(const int32_t& value, const uint32_t& seq_id, char& level) {
    if (level > MAX_LEVEL) {
        return;
    }

    if (!seq_ids.contains(seq_id)) {
        seq_ids.append(seq_id);
    }

    if (++level <= MAX_LEVEL) {
        if (children == nullptr) {
            children = new NumericTrieNode* [EXPANSE]{nullptr};
        }

        auto index = get_index(value, level);
        if (children[index] == nullptr) {
            children[index] = new NumericTrieNode();
        }

        return children[index]->insert(value, seq_id, level);
    }
}

void NumericTrieNode::search_lesser(const int32_t& value, uint32_t*& ids, uint32_t& ids_length) {
    char level = 0;
    std::vector<NumericTrieNode*> matches;
    search_lesser_helper(value, level, matches);

    for (auto const& match: matches) {
        uint32_t* out = nullptr;
        auto const& m_seq_ids = match->seq_ids.uncompress();
        ids_length = ArrayUtils::or_scalar(m_seq_ids, match->seq_ids.getLength(), ids, ids_length, &out);

        delete [] m_seq_ids;
        delete [] ids;
        ids = out;
    }
}

void NumericTrieNode::search_lesser_helper(const int32_t& value, char& level, std::vector<NumericTrieNode*>& matches) {
    if (level == MAX_LEVEL) {
        matches.push_back(this);
        return;
    } else if (level > MAX_LEVEL || children == nullptr) {
        return;
    }

    auto index = get_index(value, ++level);
    if (children[index] != nullptr) {
        children[index]->search_lesser_helper(value, level, matches);
    }

    while (--index >= 0) {
        if (children[index] != nullptr) {
            matches.push_back(children[index]);
        }
    }

    --level;
}
