#include "facet_index.h"
#include <tokenizer.h>
#include "string_utils.h"
#include "array_utils.h"

uint32_t facet_index_t::insert(const std::string& field, const std::string& value, 
    uint32_t id, bool is_string) {
    auto& facet_index_map = facet_field_map[field].facet_index_map;
    uint32_t index;
    const auto sv = value.substr(0, 100);
    const auto it = facet_index_map.find(sv);

    if(it == facet_index_map.end()) {
        index = ++count_index;
        
        facet_index_struct fis {};
        fis.index = index;
        if(is_string) {
            fis.id_list_ptr = SET_COMPACT_IDS(compact_id_list_t::create(1, {id}));
        }
        facet_index_map.emplace(sv, fis);
    } else {
        if(is_string) {
            auto ids = it->id_list_ptr;
            if (!ids_t::contains(ids, id)) {
                ids_t::upsert(ids, id);
                facet_index_map[sv].id_list_ptr = ids;
            }
        }
        index = facet_index_map[sv].index;
    }

    if(is_string) {
        const auto facet_count = ids_t::num_ids(facet_index_map.at(sv).id_list_ptr);
        //LOG(INFO) << "Facet count in facet " << sv << " : " << facet_count;
        auto& counter_list = facet_field_map[field].counter_list;

        if(counter_list.empty()) {
            counter_list.emplace_back(sv, facet_count);
        } else {
            auto counter_it = counter_list.begin();
   
            count_list node(sv, facet_count); 

            for(counter_it = counter_list.begin(); counter_it != counter_list.end(); ++counter_it) {
                if(counter_it->facet_value == sv) {
                    counter_it->count = facet_count;

                    auto prev_node = std::prev(counter_it);
                    if(prev_node->count < counter_it->count) {
                        std::swap(prev_node, counter_it);
                    }
                    break;
                }
            }
            if(counter_it == counter_list.end()) {
                // LOG (INFO) << "inserting at last facet " << node.facet_value 
                //     << " with count " << node.count;
                counter_list.emplace_back(node);
            }
        }
    }
    
    return index;
}

bool facet_index_t::contains(const std::string& field) {

    const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return false;
    }

    return true;
}

void facet_index_t::erase(const std::string& field) {
    const auto it = facet_field_map.find(field);
    if(it != facet_field_map.end()) {
        facet_field_map.erase(field);
    }
}

size_t facet_index_t::get_facet_count(const std::string& field) {
    const auto it = facet_field_map.find(field);

    if(it == facet_field_map.end()) {
        return 0;
    }

    return it->second.counter_list.size();
}

//returns the count of matching seq_ids from result array
size_t facet_index_t::intersect(const std::string& field, const uint32_t* result_ids,
        int result_ids_len, int max_facet_count, 
        std::map<std::string, uint32_t>& found, bool is_wildcard_no_filter_query) {
    //LOG (INFO) << "intersecting field " << field;

    const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return 0;
    }

    const auto facet_index_map = facet_field_it->second.facet_index_map;
    const auto counter_list = facet_field_it->second.counter_list;

    // LOG (INFO) << "facet_index_map size " << facet_index_map.size() 
    //     << " , counter_list size " << counter_list.size();
    
    std::vector<uint32_t> id_list;
    for(const auto& counter_list_it : counter_list) {
        // LOG (INFO) << "checking ids in facet_value " << counter_list_it.facet_value 
        //   << " having total count " << counter_list_it.count;
        uint32_t count = 0;

        if(is_wildcard_no_filter_query) {
            count = counter_list_it.count;
        } else {
            auto ids = facet_index_map.at(counter_list_it.facet_value).id_list_ptr;
            ids_t::uncompress(ids, id_list);
            const auto ids_len = id_list.size();
            for(int i = 0; i < result_ids_len; ++i) {
                uint32_t* out = nullptr;
                count = ArrayUtils::and_scalar(id_list.data(), id_list.size(),
                    result_ids, result_ids_len, &out);
            }
            id_list.clear();
        }

        if(count) {
            found[counter_list_it.facet_value] = count;
            if(found.size() == max_facet_count) {
                break;
            }
        }
    }
    
    return found.size();
}

facet_index_t::~facet_index_t() {
    facet_field_map.clear();    
}

