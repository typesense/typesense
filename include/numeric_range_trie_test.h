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

        void insert_geopoint_helper(const uint64_t& cell_id, const uint32_t& seq_id, char& level, const char& max_level);

        void search_geopoints_helper(const uint64_t& cell_id, const char& max_index_level, std::set<Node*>& matches);

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

        void insert(const int64_t& cell_id, const uint32_t& seq_id, const char& max_level);

        void remove(const int64_t& cell_id, const uint32_t& seq_id, const char& max_level);

        void insert_geopoint(const uint64_t& cell_id, const uint32_t& seq_id, const char& max_level);

        void search_geopoints(const std::vector<uint64_t>& cell_ids, const char& max_level,
                              std::vector<uint32_t>& geo_result_ids);

        void delete_geopoint(const uint64_t& cell_id, uint32_t id, const char& max_level);

        void get_all_ids(uint32_t*& ids, uint32_t& ids_length);

        void search_range(const int64_t& low, const int64_t& high, const char& max_level,
                          uint32_t*& ids, uint32_t& ids_length);

        void search_range(const int64_t& low, const int64_t& high, const char& max_level, std::vector<Node*>& matches);

        void search_less_than(const int64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_less_than(const int64_t& value, const char& max_level, std::vector<Node*>& matches);

        void search_greater_than(const int64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_greater_than(const int64_t& value, const char& max_level, std::vector<Node*>& matches);

        void search_equal_to(const int64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_equal_to(const int64_t& value, const char& max_level, std::vector<Node*>& matches);
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

    class iterator_t {
        struct match_state {
            uint32_t* ids = nullptr;
            uint32_t ids_length = 0;
            uint32_t index = 0;

            explicit match_state(uint32_t*& ids, uint32_t& ids_length) : ids(ids), ids_length(ids_length) {}

            ~match_state() {
                delete [] ids;
            }
        };

        std::vector<match_state*> matches;

        void set_seq_id();

    public:

        explicit iterator_t(std::vector<Node*>& matches);

        ~iterator_t() {
            for (auto& match: matches) {
                delete match;
            }
        }

        iterator_t& operator=(iterator_t&& obj) noexcept;

        uint32_t seq_id = 0;
        bool is_valid = true;

        void next();
        void skip_to(uint32_t id);
        void reset();
    };

    void insert(const int64_t& value, const uint32_t& seq_id);

    void remove(const int64_t& value, const uint32_t& seq_id);

    void insert_geopoint(const uint64_t& cell_id, const uint32_t& seq_id);

    void search_geopoints(const std::vector<uint64_t>& cell_ids, std::vector<uint32_t>& geo_result_ids);

    void delete_geopoint(const uint64_t& cell_id, uint32_t id);

    void search_range(const int64_t& low, const bool& low_inclusive,
                      const int64_t& high, const bool& high_inclusive,
                      uint32_t*& ids, uint32_t& ids_length);

    iterator_t search_range(const int64_t& low, const bool& low_inclusive,
                            const int64_t& high, const bool& high_inclusive);

    void search_less_than(const int64_t& value, const bool& inclusive,
                          uint32_t*& ids, uint32_t& ids_length);

    iterator_t search_less_than(const int64_t& value, const bool& inclusive);

    void search_greater_than(const int64_t& value, const bool& inclusive,
                             uint32_t*& ids, uint32_t& ids_length);

    iterator_t search_greater_than(const int64_t& value, const bool& inclusive);

    void search_equal_to(const int64_t& value, uint32_t*& ids, uint32_t& ids_length);

    iterator_t search_equal_to(const int64_t& value);
};
