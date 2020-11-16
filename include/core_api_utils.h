#pragma once

#include <cstdlib>
#include <vector>
#include "collection.h"

struct deletion_state_t {
    Collection* collection;
    std::vector<std::pair<size_t, uint32_t*>> index_ids;
    std::vector<size_t> offsets;
    size_t num_removed;
};

Option<bool> stateful_remove_docs(deletion_state_t* deletion_state, size_t batch_size, bool& done);