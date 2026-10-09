#pragma once

#include <ids_t.h>
#include <set>

constexpr short EXPANSE = 256;

class NumericTrie {
    char max_level = 4;

    class Node {
        Node** children = nullptr;
        void* seq_ids = SET_COMPACT_IDS(compact_id_list_t::create(0, {}));

        void insert_helper(const uint64_t& value, const uint32_t& seq_id, char& level, const char& max_level);

        void insert_geopoint_helper(const uint64_t& cell_id, const uint32_t& seq_id, char& level, const char& max_level);

        void search_geopoints_helper(const uint64_t& cell_id, const char& max_index_level, std::set<Node*>& matches,
                                     size_t& ids_length);

        void search_range_helper(const uint64_t& low,const uint64_t& high, const char& max_level,
                                 std::vector<Node*>& matches, size_t& ids_length);

        void search_less_than_helper(const uint64_t& value, char& level, const char& max_level,
                                     std::vector<Node*>& matches, size_t& ids_length);

        void search_greater_than_helper(const uint64_t& value, char& level, const char& max_level,
                                        std::vector<Node*>& matches, size_t& ids_length);

        void seq_ids_outside_top_k_helper(const size_t& k, size_t& ids_skipped, char& level, const char& max_level,
                                          const bool& is_negative, std::vector<uint32_t>& result);

    public:

        ~Node() {
            ids_t::destroy_list(seq_ids);

            if (children != nullptr) {
                for (auto i = 0; i < EXPANSE; i++) {
                    delete children[i];
                }
            }

            delete [] children;
        }

        void insert(const uint64_t& cell_id, const uint32_t& seq_id, const char& max_level);

        void remove(const uint64_t& cell_id, const uint32_t& seq_id, const char& max_level);

        void insert_geopoint(const uint64_t& cell_id, const uint32_t& seq_id, const char& max_level);

        void search_geopoints(const std::vector<uint64_t>& cell_ids, const char& max_level,
                              std::vector<uint32_t>& geo_result_ids);

        void delete_geopoint(const uint64_t& cell_id, uint32_t id, const char& max_level);

        void get_all_ids(uint32_t*& ids, uint32_t& ids_length);

        void get_all_ids(std::vector<uint32_t>& result);

        inline uint32_t get_ids_length();

        // The returned posting is owned by this node.
        void* get_seq_ids() {
            return seq_ids;
        }

        void search_range(const uint64_t& low, const uint64_t& high, const char& max_level,
                          uint32_t*& ids, uint32_t& ids_length);

        void search_range(const uint64_t& low, const uint64_t& high, const char& max_level, std::vector<Node*>& matches);

        void search_less_than(const uint64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_less_than(const uint64_t& value, const char& max_level, std::vector<Node*>& matches);

        void search_greater_than(const uint64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_greater_than(const uint64_t& value, const char& max_level, std::vector<Node*>& matches);

        void search_equal_to(const uint64_t& value, const char& max_level, uint32_t*& ids, uint32_t& ids_length);

        void search_equal_to(const uint64_t& value, const char& max_level, std::vector<Node*>& matches);

        void seq_ids_outside_top_k(const size_t& k,  const char& max_level, size_t& ids_skipped,
                                   std::vector<uint32_t>& result, const bool& is_negative = false);
    };

    Node* negative_trie = nullptr;
    Node* positive_trie = nullptr;

    static void append_matching_postings(const std::vector<Node*>& matches,
                                         std::vector<void*>& raw_posting_lists);
    static void merge_matching_ids(const std::vector<Node*>& matches,
                                   uint32_t*& ids, uint32_t& ids_length);

    void collect_range_matches(int64_t low, bool low_inclusive, int64_t high, bool high_inclusive,
                               std::vector<Node*>& matches);
    void collect_less_than_matches(int64_t value, bool inclusive, std::vector<Node*>& matches);
    void collect_greater_than_matches(int64_t value, bool inclusive, std::vector<Node*>& matches);
    void collect_equal_to_matches(int64_t value, std::vector<Node*>& matches);

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

    // These overloads append node-owned raw postings for matching values without
    // expanding IDs. The postings are valid only while this trie remains alive and
    // unmodified under the caller's existing index lock; callers must not destroy or
    // mutate them.
    void search_range(const int64_t& low, const bool& low_inclusive,
                      const int64_t& high, const bool& high_inclusive,
                      std::vector<void*>& raw_posting_lists);

    iterator_t search_range(const int64_t& low, const bool& low_inclusive,
                            const int64_t& high, const bool& high_inclusive);

    void search_less_than(const int64_t& value, const bool& inclusive,
                          uint32_t*& ids, uint32_t& ids_length);

    void search_less_than(const int64_t& value, const bool& inclusive,
                          std::vector<void*>& raw_posting_lists);

    iterator_t search_less_than(const int64_t& value, const bool& inclusive);

    void search_greater_than(const int64_t& value, const bool& inclusive,
                             uint32_t*& ids, uint32_t& ids_length);

    void search_greater_than(const int64_t& value, const bool& inclusive,
                             std::vector<void*>& raw_posting_lists);

    iterator_t search_greater_than(const int64_t& value, const bool& inclusive);

    void search_equal_to(const int64_t& value, uint32_t*& ids, uint32_t& ids_length);

    void search_equal_to(const int64_t& value, std::vector<void*>& raw_posting_lists);

    iterator_t search_equal_to(const int64_t& value);

    void seq_ids_outside_top_k(const size_t& k, std::vector<uint32_t>& result);

    size_t size();
};
