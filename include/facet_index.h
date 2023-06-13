#pragma once

#include "ids_t.h"
#include "tsl/htrie_map.h"
#include <unordered_set>
#include <posting_list.h>
#include <num_tree.h>
#include <list>

struct facet_value_id_t {
    std::string facet_value;
    uint32_t facet_id = UINT32_MAX;

    bool operator==(const facet_value_id_t& other) const {
        return facet_value == other.facet_value;
    }

    // Hash function for the struct
    struct Hash {
        std::size_t operator()(const facet_value_id_t& value) const {
            return std::hash<std::string>{}(value.facet_value);
        }
    };

    facet_value_id_t() = default;

    facet_value_id_t(const std::string& fvalue, const uint32_t fid): facet_value(fvalue), facet_id(fid) {

    }

    facet_value_id_t(const std::string& fvalue): facet_value(fvalue), facet_id(UINT32_MAX) {

    }
};

class facet_index_t {
private:
    struct facet_count_t {
        facet_count_t() = delete;

        ~facet_count_t () = default;

        facet_count_t(const std::string& sv, uint32_t facet_count, uint32_t this_facet_id) {
            facet_value = sv;
            count = facet_count;
            facet_id = this_facet_id;
        }

        facet_count_t& operator=(facet_count_t& obj) {
            facet_value = obj.facet_value;
            count = obj.count;
            facet_id = obj.facet_id;
            return *this;
        }

        std::string facet_value;
        uint32_t count;
        uint32_t facet_id;
    };

    struct facet_id_seq_ids_t {
        void* seq_ids;
        uint32_t facet_id;
        std::list<facet_count_t>::iterator facet_count_it;

        facet_id_seq_ids_t() {
            seq_ids = nullptr;
            facet_id = UINT32_MAX;
        }

        ~facet_id_seq_ids_t() {};
    };
    
    struct facet_doc_ids_list_t {
        tsl::htrie_map<char, facet_id_seq_ids_t> fvalue_seq_ids;
        std::list<facet_count_t> counts;
        posting_list_t* seq_id_hashes;

        bool has_value_index = true;
        bool has_hash_index = false;

        facet_doc_ids_list_t() {
            fvalue_seq_ids.clear();
            counts.clear();
            seq_id_hashes = nullptr;
        }

        ~facet_doc_ids_list_t() {
            for(auto it = fvalue_seq_ids.begin(); it != fvalue_seq_ids.end(); ++it) {
                if(it.value().seq_ids) {
                    ids_t::destroy_list(it.value().seq_ids);
                }
            }
    
            fvalue_seq_ids.clear();
            counts.clear();

            delete seq_id_hashes;
        }
    };

    // field -> facet_index
    std::unordered_map<std::string, facet_doc_ids_list_t> facet_field_map;

    // auto incrementing ID that is assigned to each unique facet value string
    uint32_t next_facet_id = 0;

public:

    facet_index_t() = default;

    ~facet_index_t();

    void insert(const std::string& field_name, bool is_string,
                std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash>& fvalue_to_seq_ids,
                std::unordered_map<uint32_t, std::vector<facet_value_id_t>>& seq_id_to_fvalues);

    void erase(const std::string& field_name);

    void remove(const std::string& field_name, const uint32_t seq_id);

    bool contains(const std::string& field_name);

    size_t get_facet_count(const std::string& field_name);

    size_t intersect(const std::string& val, const uint32_t* result_ids, size_t result_id_len,
                     size_t max_facet_count, std::map<std::string, uint32_t>& found,
                     bool is_wildcard_no_filter_query);
    
    size_t get_facet_indexes(const std::string& field, 
        std::map<uint32_t, std::vector<uint32_t>>& seqid_countIndexes);
    
    void initialize(const std::string& field);

    void handle_index_change(const std::string& field_name, size_t total_num_docs, size_t facet_index_threshold,
                             size_t facet_count, spp::sparse_hash_map<std::string, num_tree_t*>& numerical_index);

    bool has_hash_index(const std::string& field_name);

    posting_list_t* get_facet_hash_index(const std::string& field_name);
};