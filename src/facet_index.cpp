#include "facet_index.h"
#include <tokenizer.h>
#include "string_utils.h"
#include "array_utils.h"

void facet_index_t::initialize(const std::string& field) {
    const auto facet_field_map_it = facet_field_map.find(field);
    if(facet_field_map_it == facet_field_map.end()) {
        // NOTE: try_emplace is needed to construct the value object in-place without calling the destructor
        facet_field_map.try_emplace(field);
    }
}

void facet_index_t::insert(const std::string& field_name, bool is_string,
                           std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash>& fvalue_to_seq_ids,
                           std::unordered_map<uint32_t, std::vector<facet_value_id_t>>& seq_id_to_fvalues) {
    
    const auto facet_field_map_it = facet_field_map.find(field_name);
    if(facet_field_map_it == facet_field_map.end()) {
        return; // field is not initialized or dropped
    }

    auto& facet_index = facet_field_map_it->second;
    tsl::htrie_map<char, facet_id_seq_ids_t>& fvalue_index = facet_index.fvalue_seq_ids;
    auto fhash_index = facet_index.seq_id_hashes;

    for(const auto& seq_id_fvalues: seq_id_to_fvalues) {
        auto seq_id = seq_id_fvalues.first;
        std::vector<uint32_t> real_facet_ids;
        real_facet_ids.reserve(seq_id_fvalues.second.size());

        for(const auto& fvalue: seq_id_fvalues.second) {
            uint32_t facet_id = fvalue.facet_id;
            const auto& fvalue_index_it = fvalue_index.find(fvalue.facet_value);

            if(fvalue.facet_id == UINT32_MAX) {
                // float, int32 & bool will provide facet_id as their own numerical values
                facet_id = (fvalue_index_it == fvalue_index.end()) ? ++next_facet_id : fvalue_index_it->facet_id;
            }

            real_facet_ids.push_back(facet_id);

            auto seq_ids_it = fvalue_to_seq_ids.find(fvalue);
            if(seq_ids_it == fvalue_to_seq_ids.end()) {
                continue;
            }

            auto& seq_ids = seq_ids_it->second;
            std::list<facet_count_t>& count_list = facet_index.counts;

            if(fvalue_index_it == fvalue_index.end()) {
                facet_id_seq_ids_t fis;
                fis.facet_id = facet_id;

                if(facet_index.has_value_index) {
                    count_list.emplace_back(fvalue.facet_value, seq_ids.size(), facet_id);
                    fis.facet_count_it = std::prev(count_list.end());
                    fis.seq_ids = SET_COMPACT_IDS(compact_id_list_t::create(seq_ids.size(), seq_ids));
                }

                fvalue_index.emplace(fvalue.facet_value, fis);
            } else if(facet_index.has_value_index) {
                for(const auto id : seq_ids) {
                    ids_t::upsert(fvalue_index_it->seq_ids, id);
                }

                auto facet_count_it = fvalue_index_it->facet_count_it;

                if(facet_count_it->facet_id == facet_id) {
                    facet_count_it->count = ids_t::num_ids(fvalue_index_it->seq_ids);
                    auto curr = facet_count_it;
                    while (curr != count_list.begin() && std::prev(curr)->count < curr->count) {
                        count_list.splice(curr, count_list, std::prev(curr));  // swaps list nodes
                        curr--;
                    }
                } else {
                    LOG(ERROR) << "Wrong reference stored for facet " << fvalue.facet_value << " with facet_id " << facet_id;
                }
            }

            fvalue_to_seq_ids.erase(fvalue);
        }

        if(facet_index.has_hash_index && fhash_index != nullptr) {
            fhash_index->upsert(seq_id, real_facet_ids);
        }
    }
}

bool facet_index_t::contains(const std::string& field_name) {
    const auto& facet_field_it = facet_field_map.find(field_name);
    if(facet_field_it == facet_field_map.end()) {
        return false;
    }

    return true;
}

void facet_index_t::erase(const std::string& field_name) {
    facet_field_map.erase(field_name);
}

void facet_index_t::remove(const std::string& field_name, const uint32_t seq_id) {
    const auto facet_field_it = facet_field_map.find(field_name);
    if(facet_field_it != facet_field_map.end()) {
        auto& facet_index_map = facet_field_it->second.fvalue_seq_ids;
        std::vector<std::string> dead_fvalues;

        for(auto facet_ids_seq_ids = facet_index_map.begin(); facet_ids_seq_ids != facet_index_map.end(); facet_ids_seq_ids++) {
            void*& ids = facet_ids_seq_ids.value().seq_ids;
            if(ids && ids_t::contains(ids, seq_id)) {
                ids_t::erase(ids, seq_id);
                auto& count_list = facet_field_it->second.counts;
                auto key = facet_ids_seq_ids.key();
                auto& facet_id_seq_ids = facet_ids_seq_ids.value();
                facet_ids_seq_ids.value().facet_count_it->count--;

                if(ids_t::num_ids(ids) == 0) {
                    ids_t::destroy_list(ids);
                    std::string dead_fvalue;
                    facet_ids_seq_ids.key(dead_fvalue);
                    dead_fvalues.push_back(dead_fvalue);
                    count_list.erase(facet_ids_seq_ids.value().facet_count_it);
                }
            }
        }

        for(auto& dead_fvalue: dead_fvalues) {
            facet_index_map.erase(dead_fvalue);
        }

        auto& seq_id_hashes = facet_field_it->second.seq_id_hashes;
        seq_id_hashes->erase(seq_id);
    }
}

size_t facet_index_t::get_facet_count(const std::string& field_name) {
    const auto it = facet_field_map.find(field_name);

    if(it == facet_field_map.end()) {
        return 0;
    }

    return has_hash_index(field_name) ? it->second.seq_id_hashes->num_ids() :  it->second.counts.size();
}

//returns the count of matching seq_ids from result array
size_t facet_index_t::intersect(const std::string& field, const uint32_t* result_ids, size_t result_ids_len,
                                size_t max_facet_count, std::map<std::string, uint32_t>& found,
                                bool is_wildcard_no_filter_query) {
    //LOG (INFO) << "intersecting field " << field;

    const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return 0;
    }

    const auto& facet_index_map = facet_field_it->second.fvalue_seq_ids;
    const auto& counter_list = facet_field_it->second.counts;

     //LOG(INFO) << "fvalue_seq_ids size " << facet_index_map.size() << " , counts size " << counter_list.size();

    // We look 2 * max_facet_count when keyword search / filtering is involved to ensure that we
    // try and pick the actual top facets by count.
    size_t max_facets = is_wildcard_no_filter_query ? std::min((size_t)max_facet_count, counter_list.size()) :
                        std::min((size_t)2 * max_facet_count, counter_list.size());

    for(const auto& facet_count : counter_list) {
         //LOG(INFO) << "checking ids in facet_value " << facet_count.facet_value << " having total count "
         //           << facet_count.count << ", is_wildcard_no_filter_query: " << is_wildcard_no_filter_query;
        uint32_t count = 0;

        if(is_wildcard_no_filter_query) {
            count = facet_count.count;
        } else {
            auto ids = facet_index_map.at(facet_count.facet_value).seq_ids;
            if(!ids) {
                continue;
            }
            count = ids_t::intersect_count(ids, result_ids, result_ids_len);
        }

        if(count) {
            found[facet_count.facet_value] = count;
            if(found.size() == max_facets) {
                break;
            }
        }
    }
    
    return found.size();
}

facet_index_t::~facet_index_t() {
    facet_field_map.clear();    
}

// used for migrating string and int64 facets
size_t facet_index_t::get_facet_indexes(const std::string& field_name,
    std::map<uint32_t, std::vector<uint32_t>>& seqid_countIndexes) {

  const auto& facet_field_it = facet_field_map.find(field_name);
    if(facet_field_it == facet_field_map.end()) {
        return 0;
    }

    auto& facet_index_map = facet_field_it->second.fvalue_seq_ids;

    std::vector<uint32_t> id_list;

    for(auto facet_index_map_it = facet_index_map.begin(); facet_index_map_it != facet_index_map.end(); ++facet_index_map_it) {
        auto ids = facet_index_map_it->seq_ids;
        ids_t::uncompress(ids, id_list);

        // emplacing seq_id => next_facet_id
        for(const auto& id : id_list) {
            seqid_countIndexes[id].emplace_back(facet_index_map_it->facet_id);
        }

        id_list.clear();
    }

    return seqid_countIndexes.size();
}

void facet_index_t::handle_index_change(const std::string& field_name, size_t total_num_docs,
                                        size_t facet_index_threshold, size_t facet_count) {

    // Low cardinality fields will have only value based facet index. Once a field becomes a high cardinality
    // field (exceeding FACET_INDEX_THRESHOLD), we will create a hash based index and populate it.
    // If a field is an id-like field (cardinality_ratio < 5) we will then remove value based index.

    auto& facet_index = facet_field_map.at(field_name);
    posting_list_t*& fhash_index = facet_index.seq_id_hashes;

    if(fhash_index == nullptr && (facet_count > facet_index_threshold) && total_num_docs < 1000000) {
        fhash_index = new posting_list_t(256);
        std::map<uint32_t, std::vector<uint32_t>> seq_id_index_map;

        if(get_facet_indexes(field_name, seq_id_index_map)) {
            for(const auto& kv : seq_id_index_map) {
                fhash_index->upsert(kv.first, kv.second);
            }
        }

        seq_id_index_map.clear();

        facet_index.has_hash_index = true;

        auto cardinality_ratio = total_num_docs / facet_count;

        if(cardinality_ratio != 0 && cardinality_ratio < 5) {
            // drop the value index for this field
            auto& fvalue_seq_ids = facet_index.fvalue_seq_ids;
            for(auto it = fvalue_seq_ids.begin(); it != fvalue_seq_ids.end(); ++it) {
                ids_t::destroy_list(it.value().seq_ids);
            }
            fvalue_seq_ids.clear();
            facet_index.counts.clear();
            facet_index.has_value_index = false;
        }
    }
}

bool facet_index_t::has_hash_index(const std::string &field_name) {
    auto facet_index_it = facet_field_map.find(field_name);
    return facet_index_it != facet_field_map.end() && facet_index_it->second.has_hash_index;
}

bool facet_index_t::has_value_index(const std::string &field_name) {
    auto facet_index_it = facet_field_map.find(field_name);
    return facet_index_it != facet_field_map.end() && facet_index_it->second.has_value_index;
}

posting_list_t* facet_index_t::get_facet_hash_index(const std::string &field_name) {
    auto facet_index_it = facet_field_map.find(field_name);
    if(facet_index_it != facet_field_map.end()) {
        return facet_index_it->second.seq_id_hashes;
    }
    return nullptr;
}
