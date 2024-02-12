#pragma once

#include <map>
#include "sparsepp.h"
#include "sorted_array.h"
#include "array_utils.h"
#include "ids_t.h"
#include "filter.h"

class num_tree_t {
private:
    std::map<int64_t, void*> int64map;

    [[nodiscard]] bool range_inclusive_contains(const int64_t& start, const int64_t& end, const uint32_t& id) const;

    [[nodiscard]] bool contains(const int64_t& value, const uint32_t& id) const {
        if (int64map.count(value) == 0) {
            return false;
        }

        auto ids = int64map.at(value);
        return ids_t::contains(ids, id);
    }

public:

    ~num_tree_t();

    void insert(int64_t value, uint32_t id, bool is_facet=false);

    void range_inclusive_search(int64_t start, int64_t end, uint32_t** ids, size_t& ids_len);

    void approx_range_inclusive_search_count(int64_t start, int64_t end, uint32_t& ids_len);

    void range_inclusive_contains(const int64_t& start, const int64_t& end,
                                  const uint32_t& context_ids_length,
                                  uint32_t* const& context_ids,
                                  size_t& result_ids_len,
                                  uint32_t*& result_ids) const;

    size_t get(int64_t value, std::vector<uint32_t>& geo_result_ids);

    void search(NUM_COMPARATOR comparator, int64_t value, uint32_t** ids, size_t& ids_len);

    uint32_t approx_search_count(NUM_COMPARATOR comparator, int64_t value);

    void remove(uint64_t value, uint32_t id);

    size_t size();

    void seq_ids_outside_top_k(size_t k, std::vector<uint32_t>& seq_ids);

    void contains(const NUM_COMPARATOR& comparator, const int64_t& value,
                  const uint32_t& context_ids_length,
                  uint32_t* const& context_ids,
                  size_t& result_ids_len,
                  uint32_t*& result_ids) const;

    std::pair<int64_t, int64_t> get_min_max(const uint32_t* result_ids, size_t result_ids_len);

    class iterator_t {
        /// If true, `id_list_array` is initialized otherwise `id_list_iterator` is.
        bool is_compact_id_list = true;

        uint32_t index = 0;
        uint32_t id_list_array_len = 0;
        uint32_t* id_list_array = nullptr;

        id_list_t* id_list = nullptr;
        id_list_t::iterator_t id_list_iterator = id_list_t::iterator_t(nullptr, nullptr, nullptr, false);

    public:

        uint32_t seq_id = 0;
        bool is_valid = true;

        /// Holds the upper-bound of the number of seq ids this iterator would match.
        /// Useful in a scenario where we need to differentiate between iterator not matching any document v/s iterator
        /// reaching it's end. (is_valid would be false in both these cases)
        uint32_t approx_filter_ids_length = 0;

        explicit iterator_t(num_tree_t* num_tree, NUM_COMPARATOR comparator, int64_t value);

        ~iterator_t();

        iterator_t& operator=(iterator_t&& obj) noexcept;

        /// Returns a tri-state:
        ///     0: id is not valid
        ///     1: id is valid
        ///    -1: end of iterator
        [[nodiscard]] int is_id_valid(uint32_t id);

        /// Advances the iterator to get the next seq_id. The iterator may become invalid during this operation.
        void next();

        /// Advances the iterator until the seq_id reaches or just overshoots id. The iterator may become invalid during
        /// this operation.
        void skip_to(uint32_t id);

        /// Returns to the initial state of the iterator.
        void reset();
    };
};