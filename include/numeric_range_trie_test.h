#pragma once

#include <map>
#include "sorted_array.h"

constexpr short EXPANSE = 256;

class NumericTrie {
    char max_level = 4;

    class Node {
        Node** children = nullptr;
        sorted_array seq_ids;

        void insert_helper(const int64_t& value, const uint32_t& seq_id, char& level, const char& max_level);

        void search_range_helper(const int64_t& low,const int64_t& high, const char& max_level,
                                 std::vector<Node*>& matches);

        void search_less_than_helper(const int64_t& value, char& level, const char& max_level,
                                     std::vector<Node*>& matches);

        void search_greater_than_helper(const int64_t& value, char& level, const char& max_level,
                                        std::vector<Node*>& matches);

    public:

        ~Node() {
            if (children != nullptr) {
                for (auto i = 0; i < EXPANSE; i++) {
                    delete children[i];
                }
            }

            delete [] children;
        }

        void insert(const int64_t& value, const uint32_t& seq_id, const char& max_level);

        void get_all_ids(uint32_t*& ids, uint32_t& ids_length);

        void search_range(const int64_t& low, const int64_t& high, const char& max_level,
                          uint32_t*& ids, uint32_t& ids_length);

        void search_less_than(const int64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_greater_than(const int64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_equal_to(const int64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);
    };

    Node* negative_trie = nullptr;
    Node* positive_trie = nullptr;

public:

    explicit NumericTrie(char num_bits = 32) {
        max_level = num_bits / 8;
    }

    ~NumericTrie() {
        delete negative_trie;
        delete positive_trie;
    }

    void insert(const int64_t& value, const uint32_t& seq_id);

    void search_range(const int64_t& low, const bool& low_inclusive,
                      const int64_t& high, const bool& high_inclusive,
                      uint32_t*& ids, uint32_t& ids_length);

    void search_less_than(const int64_t& value, const bool& inclusive,
                          uint32_t*& ids, uint32_t& ids_length);

    void search_greater_than(const int64_t& value, const bool& inclusive,
                             uint32_t*& ids, uint32_t& ids_length);

    void search_equal_to(const int64_t& value, uint32_t*& ids, uint32_t& ids_length);
};
