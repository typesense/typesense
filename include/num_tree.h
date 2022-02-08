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

public:

    ~num_tree_t();

    void insert(int64_t value, uint32_t id);

    void range_inclusive_search(int64_t start, int64_t end, uint32_t** ids, size_t& ids_len);

    size_t get(int64_t value, std::vector<uint32_t>& geo_result_ids);

    void search(NUM_COMPARATOR comparator, int64_t value, uint32_t** ids, size_t& ids_len);

    void remove(uint64_t value, uint32_t id);

    size_t size();
};