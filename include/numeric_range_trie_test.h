#pragma once

#include <map>
#include "sorted_array.h"

constexpr char MAX_LEVEL = 4;
constexpr short EXPANSE = 256;

class NumericTrie {
    class Node {
        Node** children = nullptr;
        sorted_array seq_ids;

        void insert_helper(const int32_t& value, const uint32_t& seq_id, char& level);

        void search_range_helper(const int32_t& low,const int32_t& high, std::vector<Node*>& matches);

        void search_less_than_helper(const int32_t& value, char& level, std::vector<Node*>& matches);

        void search_greater_than_helper(const int32_t& value, char& level, std::vector<Node*>& matches);

    public:

        ~Node() {
            if (children != nullptr) {
                for (auto i = 0; i < EXPANSE; i++) {
                    delete children[i];
                }
            }

            delete [] children;
        }

        void insert(const int32_t& value, const uint32_t& seq_id);

        void get_all_ids(uint32_t*& ids, uint32_t& ids_length);

        void search_range(const int32_t& low, const int32_t& high,
                          uint32_t*& ids, uint32_t& ids_length);

        void search_less_than(const int32_t& value, uint32_t*& ids, uint32_t& ids_length);

        void search_greater_than(const int32_t& value, uint32_t*& ids, uint32_t& ids_length);

        void search_equal_to(const int32_t& value, uint32_t*& ids, uint32_t& ids_length);
    };

    Node* negative_trie = nullptr;
    Node* positive_trie = nullptr;

public:

    ~NumericTrie() {
        delete negative_trie;
        delete positive_trie;
    }

    void insert(const int32_t& value, const uint32_t& seq_id);

    void search_range(const int32_t& low, const bool& low_inclusive,
                      const int32_t& high, const bool& high_inclusive,
                      uint32_t*& ids, uint32_t& ids_length);

    void search_less_than(const int32_t& value, const bool& inclusive,
                          uint32_t*& ids, uint32_t& ids_length);

    void search_greater_than(const int32_t& value, const bool& inclusive,
                             uint32_t*& ids, uint32_t& ids_length);

    void search_equal_to(const int32_t& value, uint32_t*& ids, uint32_t& ids_length);
};
