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

void facet_index_t::insert(const std::string& field_name,
                           std::unordered_map<facet_value_id_t, std::vector<uint32_t>, facet_value_id_t::Hash>& fvalue_to_seq_ids,
                           std::unordered_map<uint32_t, std::vector<facet_value_id_t>>& seq_id_to_fvalues,
                           bool is_string_field) {
    
    const auto facet_field_map_it = facet_field_map.find(field_name);
    if(facet_field_map_it == facet_field_map.end()) {
        return; // field is not initialized or dropped
    }

    auto& facet_index = facet_field_map_it->second;
    auto& fvalue_index = facet_index.fvalue_seq_ids;
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
                facet_id = (fvalue_index_it == fvalue_index.end()) ? ++next_facet_id : fvalue_index_it->second.facet_id;

                if(!is_string_field) {
                    int64_t val = std::stoll(fvalue.facet_value);
                    facet_index.fhash_to_int64_map[facet_id] = val;
                }
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
                    fis.seq_ids = ids_t::create(seq_ids);
                    auto new_count = ids_t::num_ids(fis.seq_ids);
                    auto& count_map = facet_index.count_map;
                    auto count_map_it = count_map.lower_bound(new_count);

                    count_list.emplace_front(fvalue.facet_value, new_count, facet_id);
                    fis.facet_count_it = count_list.begin();
                    auto curr = fis.facet_count_it;

                    if(count_map_it == facet_index.count_map.end()) {
                        // no element >= new_count, so it remains at head of list
                        facet_index.count_map.emplace(curr->count, curr);
                    } else if(count_map_it->second->count == new_count) {
                        // `curr` must be placed _before_ anchor node
                        // a) [7], 10, <7>, 3
                        auto& existing_node = count_map_it->second;
                        count_list.splice(existing_node, count_list, curr);
                    } else {
                        // b) [9], 12, 10, <10>, 7, 3
                        // `curr` placed _after_ anchor node
                        auto& gt_node = count_map_it->second;
                        count_list.splice(std::next(gt_node), count_list, curr);
                        count_map.emplace(curr->count, curr);
                    }
                }

                fvalue_index.emplace(fvalue.facet_value, fis);
            } else if(facet_index.has_value_index) {
                for(const auto id : seq_ids) {
                    ids_t::upsert(fvalue_index_it->second.seq_ids, id);
                }

                auto facet_count_it = fvalue_index_it->second.facet_count_it;

                if(facet_count_it->facet_id == facet_id) {
                    auto& count_map = facet_index.count_map;
                    auto old_count = facet_count_it->count;
                    facet_count_it->count = ids_t::num_ids(fvalue_index_it->second.seq_ids);
                    auto new_count = facet_count_it->count;

                    // Relocate `curr` to a new sorted location within count list:
                    auto curr = facet_count_it;
                    update_count_nodes(count_list, count_map, old_count, new_count, curr);
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

void facet_index_t::update_count_nodes(std::list<facet_count_t>& count_list,
                                       std::map<uint32_t, std::list<facet_count_t>::iterator>& count_map,
                                       uint32_t old_count, uint32_t new_count,
                                       std::list<facet_count_t>::iterator& curr) {

    auto count_map_it = count_map.lower_bound(new_count);

    if(count_map_it == count_map.end()) {
        // all elements are < new_count

        // 5, 4, [4 -> 7]
        // 5, 4, [4 -> 7], 3
        // 5, [4 -> 7]
        // [4 -> 5]

        // delete count map entry if `curr` is the anchor for `old_count`
        if(std::next(curr) == count_list.end() || std::next(curr)->count < old_count) {
            count_map.erase(old_count);

            // find a replacement for orig_count
            if(curr != count_list.begin() && std::prev(curr)->count == old_count) {
                count_map.emplace(old_count, std::prev(curr));
            }
        }

        // `curr` placed at head of the count list
        count_list.splice(count_list.begin(), count_list, curr);
        count_map.emplace(new_count, curr);
    } else if(count_map_it->first == new_count) {
        // entry for new_count already exists in count map
        // a) 10, 7, [5 -> 7], 3
        //    10, 7, 5, [5 -> 7]
        //    10, 7, 5, [5 -> 8]
        //    10, 7, [5 -> 7]
        //    10, 7, [5 -> 7], 5

        //    [9 -> 8], 8
        //    10, [9 -> 8], 8

        auto existing_node = count_map_it->second;

        // delete count map entry if `curr` is the anchor for `old_count`
        if(std::next(curr) == count_list.end() || std::next(curr)->count < old_count) {
            count_map.erase(old_count);

            // find a replacement for orig_count
            if(curr != count_list.begin() && std::prev(curr)->count == old_count) {
                count_map.emplace(old_count, std::prev(curr));
            }
        }

        // `curr` placed before anchor node
        count_list.splice(existing_node, count_list, curr);
    } else {
        // b) 10, 7, [7 -> 9], 3
        //    10, 7, [7 -> 9]
        //    10, [7 -> 9]
        //    10, [7 -> 9], 7
        //    10, 7, [5 -> 9], 3

        //    [10 -> 9]
        //    [5 -> 4], 2
        //    [5 -> 1], 2
        //    5, 5, [5 -> 4], 5
        //    5, 5, 5, [5 -> 4]

        auto gt_node = count_map_it->second;

        // delete old entry if `orig_count` iterator is same as `curr`
        if(std::next(curr) == count_list.end() || std::next(curr)->count < old_count) {
            count_map.erase(old_count);

            // find a replacement for orig_count
            if(curr != count_list.begin() && std::prev(curr)->count == old_count) {
                count_map.emplace(old_count, std::prev(curr));
            }
        }

        // `curr` placed after anchor node
        count_list.splice(std::next(gt_node), count_list, curr);
        count_map.emplace(new_count, curr);
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

void facet_index_t::get_stringified_value(const nlohmann::json& value, const field& afield,
                                          std::vector<std::string>& values) {
    if(afield.is_int32()) {
        int32_t raw_val = value.get<int32_t>();
        values.push_back(std::to_string(raw_val));
    }
    else if(afield.is_int64()) {
        int64_t raw_val = value.get<int64_t>();
        values.push_back(std::to_string(raw_val));
    }
    else if(afield.is_string()) {
        const std::string& raw_val = value.get<std::string>().substr(0, 100);
        values.push_back(raw_val);
    }
    else if(afield.is_float()) {
        float raw_val = value.get<float>();
        values.push_back(StringUtils::float_to_str(raw_val));
    }
    else if(afield.is_bool()) {
        bool raw_val = value.get<bool>();
        auto fhash = (uint32_t)raw_val;
        auto str_val = (raw_val == 1) ? "true" : "false";
        values.emplace_back(str_val);
    }
}

void facet_index_t::get_stringified_values(const nlohmann::json& document, const field& afield,
                                           std::vector<std::string>& values) {
    bool is_array = afield.is_array();

    if(!is_array) {
        return get_stringified_value(document[afield.name], afield, values);
    } else {
        const auto& field_values = document[afield.name];
        for(size_t i = 0; i < field_values.size(); i++) {
            get_stringified_value(field_values[i], afield, values);
        }
    }
}

void facet_index_t::remove(const nlohmann::json& doc, const field& afield, const uint32_t seq_id) {
    const auto facet_field_it = facet_field_map.find(afield.name);
    if(facet_field_it == facet_field_map.end()) {
        return ;
    }

    auto& facet_index_map = facet_field_it->second.fvalue_seq_ids;
    std::vector<std::string> dead_fvalues;
    std::vector<std::string> values;
    get_stringified_values(doc, afield, values);

    for(const auto& value: values) {
        auto fvalue_it = facet_index_map.find(value);
        if(fvalue_it == facet_index_map.end()) {
            continue;
        }

        void*& ids = fvalue_it->second.seq_ids;
        if(ids && ids_t::contains(ids, seq_id)) {
            ids_t::erase(ids, seq_id);
            auto& count_list = facet_field_it->second.counts;
            auto curr = fvalue_it->second.facet_count_it;
            auto old_count = curr->count;
            curr->count = ids_t::num_ids(ids);
            auto new_count = curr->count;

            // move the node lower in the count list
            auto& count_map = facet_field_it->second.count_map;
            update_count_nodes(count_list, count_map, old_count, new_count, curr);

            if(ids_t::num_ids(ids) == 0) {
                ids_t::destroy_list(ids);
                dead_fvalues.push_back(fvalue_it->first);

                //remove from int64 lookup map first
                auto& fhash_int64_map = facet_field_it->second.fhash_to_int64_map;
                uint32_t fhash = fvalue_it->second.facet_id;
                fhash_int64_map.erase(fhash);

                count_map.erase(new_count);
                count_list.erase(curr);
            }
        }
    }

    for(auto& dead_fvalue: dead_fvalues) {
        facet_index_map.erase(dead_fvalue);
    }

    auto& seq_id_hashes = facet_field_it->second.seq_id_hashes;
    seq_id_hashes->erase(seq_id);
}

size_t facet_index_t::get_facet_count(const std::string& field_name) {
    const auto it = facet_field_map.find(field_name);

    if(it == facet_field_map.end()) {
        return 0;
    }

    return has_hash_index(field_name) ? it->second.seq_id_hashes->num_ids() :  it->second.counts.size();
}

//returns the count of matching seq_ids from result array
size_t facet_index_t::intersect(facet& a_facet, const field& facet_field,
                                bool has_facet_query,
                                bool estimate_facets,
                                size_t facet_sample_interval,
                                const std::vector<std::vector<std::string>>& fvalue_searched_tokens,
                                const std::vector<char>& symbols_to_index, const std::vector<char>& token_separators,
                                const uint32_t* result_ids, size_t result_ids_len,
                                size_t max_facet_count, std::map<std::string, docid_count_t>& found,
                                bool is_wildcard_no_filter_query, const std::string& sort_order) {
    //LOG (INFO) << "intersecting field " << field;

    const auto& facet_field_it = facet_field_map.find(a_facet.field_name);
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

    auto intersect_fn = [&] (std::list<facet_count_t>::const_iterator facet_count_it) {
        uint32_t count = 0;
        uint32_t doc_id = 0;
        if(has_facet_query) {
            bool found_search_token = false;
            auto facet_str = facet_count_it->facet_value;
            std::vector<std::string> facet_tokens;
            if(facet_field.is_string()) {
                Tokenizer(facet_str, true, false, facet_field.locale,
                          symbols_to_index, token_separators).tokenize(facet_tokens);
            } else {
                facet_tokens.push_back(facet_str);
            }

            for(const auto& searched_tokens : fvalue_searched_tokens) {
                bool found_all_search_tokens = true;
                for (const auto &searched_token: searched_tokens) {
                    bool facet_tokens_found = false;
                    for(const auto& token : facet_tokens) {
                        if (token.compare(0, searched_token.size(), searched_token) == 0) {
                            facet_tokens_found = true;
                            break;
                        }
                    }
                    if(!facet_tokens_found) {
                        found_all_search_tokens = false;
                    }
                }

                if (found_all_search_tokens) {
                    a_facet.fvalue_tokens[facet_count_it->facet_value] = searched_tokens;
                    found_search_token = true;
                    break;
                }
            }

            if(!found_search_token) {
                return;
            }
        }

        auto ids = facet_index_map.at(facet_count_it->facet_value).seq_ids;
        if (!ids) {
            return;
        }

        if (is_wildcard_no_filter_query) {
            count = facet_count_it->count;
        } else {
            auto val_count = ids_t::num_ids(ids);
            bool estimate_facet_count = (estimate_facets && val_count > 300);
            count = ids_t::intersect_count(ids, result_ids, result_ids_len, estimate_facet_count, facet_sample_interval);
        }

        if (count) {
            doc_id = ids_t::first_id(ids);
            found[facet_count_it->facet_value] = {doc_id, count};
        }
    };

    if(sort_order.empty()) {
        for (auto facet_count_it = counter_list.begin(); facet_count_it != counter_list.end();
             ++facet_count_it) {
            //LOG(INFO) << "checking ids in facet_value " << facet_count.facet_value << " having total count "
            //           << facet_count.count << ", is_wildcard_no_filter_query: " << is_wildcard_no_filter_query;

            intersect_fn(facet_count_it);
            if (found.size() == max_facets) {
                break;
            }
        }
    } else {
        if(sort_order == "asc") {
            for(auto facet_index_map_it = facet_index_map.begin();
                facet_index_map_it != facet_index_map.end(); ++facet_index_map_it) {

                intersect_fn(facet_index_map_it->second.facet_count_it);
                if (found.size() == max_facets) {
                    break;
                }
            }
        } else if(sort_order == "desc") {
            for(auto facet_index_map_it = facet_index_map.rbegin();
                facet_index_map_it != facet_index_map.rend(); ++facet_index_map_it) {

                intersect_fn(facet_index_map_it->second.facet_count_it);
                if (found.size() == max_facets) {
                    break;
                }
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
        //auto ids = facet_index_map_it->seq_ids;
        auto ids = facet_index_map_it->second.seq_ids;
        if(!ids) {
            continue;
        }

        ids_t::uncompress(ids, id_list);

        // emplacing seq_id => next_facet_id
        for(const auto& id : id_list) {
            //seqid_countIndexes[id].emplace_back(facet_index_map_it->facet_id);
            seqid_countIndexes[id].emplace_back(facet_index_map_it->second.facet_id);
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
                ids_t::destroy_list(it->second.seq_ids);
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

const spp::sparse_hash_map<uint32_t , int64_t >& facet_index_t::get_fhash_int64_map(const std::string& field_name) {
    const auto facet_field_map_it = facet_field_map.find(field_name);
    if(facet_field_map_it == facet_field_map.end()) {
        return spp::sparse_hash_map<uint32_t , int64_t >{}; // field is not initialized or dropped
    }

    const auto& facet_index = facet_field_map_it->second;
    return facet_index.fhash_to_int64_map;
}

bool facet_index_t::facet_value_exists(const std::string& field_name, const std::string& fvalue) {
    const auto facet_field_map_it = facet_field_map.find(field_name);
    if(facet_field_map_it == facet_field_map.end()) {
        return false;
    }

    const auto& facet_index = facet_field_map_it->second;
    return facet_index.fvalue_seq_ids.find(fvalue) != facet_index.fvalue_seq_ids.end();
}

size_t facet_index_t::facet_val_num_ids(const string &field_name, const string &fvalue) {
    const auto facet_field_map_it = facet_field_map.find(field_name);
    if(facet_field_map_it == facet_field_map.end()) {
        return 0;
    }

    if(facet_field_map_it->second.fvalue_seq_ids.count(fvalue) == 0) {
        return 0;
    }

    auto seq_ids = facet_field_map_it->second.fvalue_seq_ids[fvalue].seq_ids;
    return seq_ids ?  ids_t::num_ids(seq_ids) : 0;
}

size_t facet_index_t::facet_node_count(const string &field_name, const string &fvalue) {
    const auto facet_field_map_it = facet_field_map.find(field_name);
    if(facet_field_map_it == facet_field_map.end()) {
        return 0;
    }

    if(facet_field_map_it->second.fvalue_seq_ids.count(fvalue) == 0) {
        return 0;
    }

    return facet_field_map_it->second.fvalue_seq_ids[fvalue].facet_count_it->count;
}

void facet_index_t::check_for_high_cardinality(const string& field_name, size_t total_num_docs) {
    // high cardinality or sparse facet fields must be dropped from value facet index
    const auto facet_field_map_it = facet_field_map.find(field_name);
    if(facet_field_map_it == facet_field_map.end()) {
        return ;
    }

    if(!facet_field_map_it->second.has_value_index) {
        return ;
    }

    size_t value_facet_threshold = 0.8 * total_num_docs;

    auto num_facet_values = facet_field_map_it->second.fvalue_seq_ids.size();
    bool is_sparse_field = false;

    if(facet_field_map.size() > 100 && total_num_docs > 10*1000) {
        size_t num_docs_with_facet = facet_field_map_it->second.seq_id_hashes->num_ids();
        if(num_docs_with_facet > 0 && num_docs_with_facet < 100) {
            is_sparse_field = true;
        }
    }

    if(num_facet_values > value_facet_threshold || is_sparse_field) {
        // if there are too many unique values, we will drop the value index
        auto& fvalue_seq_ids = facet_field_map_it->second.fvalue_seq_ids;
        for(auto it = fvalue_seq_ids.begin(); it != fvalue_seq_ids.end(); ++it) {
            ids_t::destroy_list(it->second.seq_ids);
            it->second.seq_ids = nullptr;
        }

        facet_field_map_it->second.counts.clear();
        facet_field_map_it->second.count_map.clear();
        facet_field_map_it->second.has_value_index = false;
        //LOG(INFO) << "Dropped value index for field " << field_name;
    }
}

