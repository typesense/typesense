#pragma once

#include <cstdlib>
#include <vector>
#include "collection.h"

struct deletion_state_t {
    Collection* collection;
    std::vector<std::pair<size_t, uint32_t*>> index_ids;
    std::vector<size_t> offsets;
    size_t num_removed;

    ~deletion_state_t() {
        for(auto& kv: index_ids) {
            delete [] kv.second;
        }
    }
};

struct export_state_t {
    Collection* collection;
    std::vector<std::pair<size_t, uint32_t*>> index_ids;
    std::vector<size_t> offsets;
    std::set<std::string> include_fields;
    std::set<std::string> exclude_fields;
    std::string* res_body;

    bool filtered_export = false;

    rocksdb::Iterator* it = nullptr;

    ~export_state_t() {
        for(auto& kv: index_ids) {
            delete [] kv.second;
        }

        delete it;
    }
};

Option<bool> stateful_remove_docs(deletion_state_t* deletion_state, size_t batch_size, bool& done);
Option<bool> stateful_export_docs(export_state_t* export_state, size_t batch_size, bool& done);