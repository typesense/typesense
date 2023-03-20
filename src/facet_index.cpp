#include "facet_index.h"

void facet_index_t::insert(const std::string& field, const std::string& value, uint32_t id) {

    auto& facet_index_map = facet_field_map[field].facet_index_map;

    const auto sv = value.substr(0, 100);
    if(facet_index_map.count(sv) == 0) {
        facet_index_map.emplace(sv, SET_COMPACT_IDS(compact_id_list_t::create(1, {id})));
    }else {
        auto ids = facet_index_map[sv];
        if (!ids_t::contains(ids, id)) {
            ids_t::upsert(ids, id);
            facet_index_map[sv] = ids;
        }
    }

    const auto facet_count = ids_t::num_ids(facet_index_map.at(sv));
    //LOG(INFO) << "Facet count in facet " << sv << " : " << facet_count;
    auto& counter_list = facet_field_map[field].counter_list;

    if(counter_list.empty()) {
        counter_list.emplace_back(count_list(sv, facet_count));
    } else {
        auto it = counter_list.begin();
        //remove node from list
       for(it = counter_list.begin(); it != counter_list.end(); ++it) {
            if(it->facet_value == sv) {
                //found facet in first node
                counter_list.erase(it);
                break;
            }
        }

        //find position in list and add node with updated count
        count_list node(sv, facet_count); 

        for(it = counter_list.begin(); it != counter_list.end(); ++it) {
            // LOG (INFO) << "inserting in middle or front facet " << node.facet_value 
            //     << " with count " << node.count;
            if(it->count >= facet_count) {
                counter_list.emplace(it, node);
                break;
            }
        }
        if(it == counter_list.end()) {
            // LOG (INFO) << "inserting at last facet " << node.facet_value 
            //     << " with count " << node.count;
            counter_list.emplace_back(node);
        }
    }
}

size_t facet_index_t::get(const std::string& field, 
                    std::map<std::string,std::vector<uint32_t>>& result_ids) {

    const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return 0;
    }
    auto& facet_index_map = facet_field_it->second.facet_index_map;

    for(auto it = facet_index_map.begin(); it != facet_index_map.end(); ++it) {
        auto ids = ids_t::uncompress(it.value());
        for(auto i = 0; i < ids_t::num_ids(ids); ++i) {
           result_ids[it.key()].emplace_back(ids[i]);
        }
    }

    return result_ids.size();
}

bool facet_index_t::contains(const std::string& field) {

    const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return false;
    }

    // auto& facet_index_map = facet_field_it->second.facet_index_map;
    // LOG(INFO) << "Size of facet_field " << field << " " << facet_index_map.size();
    
    // for(auto it = facet_index_map.begin(); it != facet_index_map.end(); ++it) {
    //     LOG (INFO) << "facet_value " << it.key() << " with ids as follow";

    //     auto ids = ids_t::uncompress(it.value());
    //     for(auto i = 0; i < ids_t::num_ids(ids); ++i) {
    //         LOG(INFO) << ids[i];
    //     }
    // }
    return true;
}

void facet_index_t::erase(const std::string& field) {

    const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return;
    }

    auto& facet_index_map = facet_field_it->second.facet_index_map;

    for(auto it = facet_index_map.begin(); it != facet_index_map.end(); ++it) {
        ids_t::destroy_list(it.value());
    }
    
    facet_index_map.clear();

    facet_field_it->second.counter_list.clear();

    facet_field_map.erase(field);
}

size_t facet_index_t::size() {
    return facet_field_map.size();
}

//returns the count of matching seq_ids from result array
int facet_index_t::intersect(const std::string& field, const uint32_t* result_ids, 
        int result_ids_len, int max_facet_count, 
        std::map<std::string, uint32_t>& found) {
    //LOG (INFO) << "intersecting field " << field;

    const auto& facet_field_it = facet_field_map.find(field);
    if(facet_field_it == facet_field_map.end()) {
        return 0;
    }

    const auto facet_index_map = facet_field_it->second.facet_index_map;
    const auto counter_list = facet_field_it->second.counter_list;

    // LOG (INFO) << "facet_index_map size " << facet_index_map.size() 
    //     << " , counter_list size " << counter_list.size();
    
    auto counter_list_it = counter_list.begin();
    int facet_count = 0;

    const auto max_facets = std::min((int)counter_list.size(), max_facet_count);
    while(facet_count < max_facets) {
        //LOG (INFO) << "checking ids in facet_value " << counter_list_it->facet_value 
        // << " having total count " << counter_list_it->count;

        auto ids = facet_index_map.at(counter_list_it->facet_value);
        auto id_list = ids_t::uncompress(ids);
        int count = 0;
        
        for(int i = 0; i < result_ids_len; ++i) {
            if(std::binary_search(id_list, id_list + ids_t::num_ids(id_list), result_ids[i])) {
                ++count;
            }
        }
        if(count) {
            //LOG (INFO) << "fount count " << count << " for facet " << counter_list_it->facet_value;
            found[counter_list_it->facet_value] += count;
        }

        ++facet_count;
        ++counter_list_it;
    }

    return found.size();
}

facet_index_t::~facet_index_t() {
    for(auto it = facet_field_map.begin(); it != facet_field_map.end(); ++it) {
        erase(it->first);
    }
    facet_field_map.clear();    
}

