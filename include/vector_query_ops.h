#pragma once

#include <string>
#include <vector>
#include "option.h"

class Collection;

struct vector_query_t {
    std::string field_name;
    size_t k = 0;
    size_t flat_search_cutoff = 0;
    std::vector<float> values;

    uint32_t seq_id = 0;
    bool query_doc_given = false;

    void _reset() {
        // used for testing only
        field_name.clear();
        k = 0;
        values.clear();
        seq_id = 0;
        query_doc_given = false;
    }
};

class VectorQueryOps {
public:
    static Option<bool> parse_vector_query_str(std::string vector_query_str, vector_query_t& vector_query,
                                               const Collection* coll);
};