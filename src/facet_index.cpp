#include "facet_index.h"
#include <tokenizer.h>
#include "string_utils.h"
#include "array_utils.h"

void facet_index_t::initialize(const std::string& field) {
    const auto facet_field_map_it = facet_field_map.find(field);
    if(facet_field_map_it == facet_field_map.end()) {
        facet_field_map.emplace(field, facet_index_counter{});
    }
}

void facet_index_t::insert(const std::string& field, const std::string& value, 
    const std::vector<uint32_t>& ids, uint32_t index) {
    
    const auto facet_field_map_it = facet_field_map.find(field);
    if(facet_field_map_it == facet_field_map.end()) {
        return; //field is not initialized or dropped
    }

    auto& facet_index_map = facet_field_map_it->second.facet_index_map;
    const auto sv = value.substr(0, 100);
    const auto it = facet_index_map.find(sv);

    if(it == facet_index_map.end()) {
        facet_index_struct fis{};
        fis.index = index;
        fis.id_list_ptr = SET_COMPACT_IDS(compact_id_list_t::create(ids.size(), ids));
        facet_index_map.emplace(sv, fis);
    } else {
        auto ids_ptr = it->id_list_ptr;
        for(const auto& id : ids) {
            if (!ids_t::contains(ids_ptr, id)) {
                ids_t::upsert(ids_ptr, id);
                facet_index_map[sv].id_list_ptr = ids_ptr;
            }
        }
    }

    const auto facet_count = ids_t::num_ids(facet_index_map.at(sv).id_list_ptr);
    //LOG(INFO) << "Facet count in facet " << sv << " : " << facet_count;
    auto& counter_list = facet_field_map_it->second.counter_list;

    if(counter_list.empty()) {
        count_list* node = new count_list(sv, facet_count, index);
        counter_list.emplace_back(node);
    } else {
        auto ind = 0;
        
        for(; ind < counter_list.size(); ++ind) {
            if(counter_list[ind]->index == index) {
                counter_list[ind]->count = facet_count;
                if(ind > 1) {
                    auto curr = ind;
                    while (curr && (counter_list[curr-1]->count < counter_list[curr]->count)) {
                        std::swap(counter_list[curr-1], counter_list[curr]);
                        --curr;
                    }
                }
                break;
            }
        }
        if(ind == counter_list.size()) {
            // LOG (INFO) << "inserting at last facet " << node.facet_value 
            //     << " with count " << node.count;
            count_list* node = new count_list(sv, facet_count, index);
            counter_list.emplace_back(node);
        }
    }
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

void facet_index_t::remove(const std::string& field, const uint32_t seq_id) {
    const auto facet_field_it = facet_field_map.find(field);
    if(facet_field_it != facet_field_map.end()) {
        auto facet_index_map = facet_field_it->second.facet_index_map;
        auto facet_index_map_it = facet_index_map.begin();
        
        for(; facet_index_map_it != facet_index_map.end(); ++facet_index_map_it) {
                void* ids = facet_index_map_it.value().id_list_ptr;
                if(ids && ids_t::contains(ids, seq_id)) {
                    // ids_t::erase(ids, seq_id);                    
                    // facet_index_map_it.value().id_list_ptr = ids;          
                    break;
                }
        }
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
    const auto& counter_list = facet_field_it->second.counter_list;

    // LOG (INFO) << "facet_index_map size " << facet_index_map.size() 
    //     << " , counter_list size " << counter_list.size();
    
    size_t max_facets = std::min((size_t)2 * max_facet_count, counter_list.size());
    std::vector<uint32_t> id_list;
    for(const auto& counter_list_it : counter_list) {
        // LOG (INFO) << "checking ids in facet_value " << counter_list_it.facet_value 
        //   << " having total count " << counter_list_it.count;
        uint32_t count = 0;

        if(is_wildcard_no_filter_query) {
            count = counter_list_it->count;
        } else {
            auto ids = facet_index_map.at(counter_list_it->facet_value).id_list_ptr;
            ids_t::uncompress(ids, id_list);
            const auto ids_len = id_list.size();
            for(int i = 0; i < result_ids_len; ++i) {
                uint32_t* out = nullptr;
                count = ArrayUtils::and_scalar(id_list.data(), id_list.size(),
                    result_ids, result_ids_len, &out);
                delete[] out;
            }
            id_list.clear();
        }

        if(count) {
            found[counter_list_it->facet_value] = count;
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

 //used for migrating string and int64 facets
size_t facet_index_t::get_facet_indexes(const std::string& field, 
    std::map<uint32_t, std::vector<uint32_t>>& seqid_countIndexes) {

  const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return 0;
    }

    auto& facet_index_map = facet_field_it->second.facet_index_map;

    std::vector<uint32_t> id_list;

    for(auto facet_index_map_it = facet_index_map.begin(); 
        facet_index_map_it != facet_index_map.end(); ++facet_index_map_it) {

        auto ids = facet_index_map_it->id_list_ptr;
        ids_t::uncompress(ids, id_list);

        //emplacing seq_id=>count_index
        for(const auto& id : id_list) {
            seqid_countIndexes[id].emplace_back(facet_index_map_it->index);
        }

        id_list.clear();
    }

    return seqid_countIndexes.size();
}
