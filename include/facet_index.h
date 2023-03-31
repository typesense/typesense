#pragma once

#include "ids_t.h"
#include "tsl/htrie_map.h"
#include <list>

class facet_index_t {
private:
    struct count_list {
        count_list(const std::string& sv, uint32_t facet_count) {
            facet_value = sv;
            count = facet_count;
        }

        std::string facet_value;
        uint32_t count;
    };

    struct facet_index_counter {
        tsl::htrie_map<char, void*> facet_index_map;
        std::list<count_list> counter_list;

        ~facet_index_counter() {
            for(auto it = facet_index_map.begin(); it != facet_index_map.end(); ++it) {
                ids_t::destroy_list(it.value());
            }
    
            facet_index_map.clear();

            counter_list.clear();
        }
    };

    std::map<std::string, facet_index_counter> facet_field_map;
public:

    facet_index_t() = default;

    ~facet_index_t();

    void insert(const std::string& field, const std::string& value, uint32_t id);

    void erase(const std::string& field);

    bool contains(const std::string& field);

    size_t size();

    int intersect(const std::string& val, const uint32_t* result_ids, int result_id_len, 
        int max_facet_count, std::map<std::string, uint32_t>& found);
    
    int get_facet(const std::string& field, const std::vector<std::string>& searched_tokens,
        std::vector<std::string>& facets);
};