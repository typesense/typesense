#pragma once

#include <map>
#include "sparsepp.h"
#include "sorted_array.h"
#include "array_utils.h"
#include "art.h"
#include "ids_t.h"

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

    struct count_list {
        count_list(const int64_t& sv, uint32_t facet_count) {
            facet_value = sv;
            count = facet_count;
        }

        int64_t facet_value;
        uint32_t count;
    };

    std::vector<count_list> counter_list;

public:

    ~num_tree_t();

    void insert(int64_t value, uint32_t id, bool is_facet=false);

    void range_inclusive_search(int64_t start, int64_t end, uint32_t** ids, size_t& ids_len);

    void range_inclusive_search_iterators(int64_t start,
                                          int64_t end,
                                          std::vector<id_list_t::iterator_t>& id_list_iterators,
                                          std::vector<id_list_t*>& expanded_id_lists);

    void approx_range_inclusive_search_count(int64_t start, int64_t end, uint32_t& ids_len);

    void range_inclusive_contains(const int64_t& start, const int64_t& end,
                                  const uint32_t& context_ids_length,
                                  uint32_t* const& context_ids,
                                  size_t& result_ids_len,
                                  uint32_t*& result_ids) const;

    size_t get(int64_t value, std::vector<uint32_t>& geo_result_ids);

    void search(NUM_COMPARATOR comparator, int64_t value, uint32_t** ids, size_t& ids_len);

    void search_iterators(NUM_COMPARATOR comparator,
                          int64_t value,
                          std::vector<id_list_t::iterator_t>& id_list_iterators,
                          std::vector<id_list_t*>& expanded_id_lists);

    void approx_search_count(NUM_COMPARATOR comparator, int64_t value, uint32_t& ids_len);

    void remove(uint64_t value, uint32_t id);

    size_t size();

    void contains(const NUM_COMPARATOR& comparator, const int64_t& value,
                  const uint32_t& context_ids_length,
                  uint32_t* const& context_ids,
                  size_t& result_ids_len,
                  uint32_t*& result_ids) const;
    
    size_t intersect(const uint32_t* result_ids, int result_id_len,
        int max_facet_count, std::map<int64_t, uint32_t>& found, 
        bool is_wildcard_no_filter_query);
    
    size_t counter_list_size() const;
};