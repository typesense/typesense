#pragma once

#include <cstdlib>
#include <vector>
#include "collection.h"
#include "http_data.h"

struct deletion_state_t: public req_state_t {
    Collection* collection;
    std::vector<std::pair<size_t, uint32_t*>> index_ids;  // ids_len -> ids
    std::vector<size_t> offsets;
    size_t num_removed;

    ~deletion_state_t() override {
        for(auto& kv: index_ids) {
            delete [] kv.second;
        }
    }
};

struct export_state_t: public req_state_t {
    Collection* collection;
    std::vector<std::pair<size_t, uint32_t*>> index_ids;
    std::vector<size_t> offsets;
    tsl::htrie_set<char> include_fields;
    tsl::htrie_set<char> exclude_fields;
    size_t export_batch_size = 100;
    std::string* res_body;

    bool filtered_export = false;

    rocksdb::Iterator* it = nullptr;
    std::string iter_upper_bound_key;
    rocksdb::Slice* iter_upper_bound = nullptr;

    ~export_state_t() override {
        for(auto& kv: index_ids) {
            delete [] kv.second;
        }

        delete iter_upper_bound;
        delete it;
    }
};

Option<bool> stateful_remove_docs(deletion_state_t* deletion_state, size_t batch_size, bool& done);
Option<bool> stateful_export_docs(export_state_t* export_state, size_t batch_size, bool& done);