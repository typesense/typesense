#pragma once

#include <string>
#include <vector>
#include <art.h>
#include <unordered_map>

class SearchIndex {
private:
    art_tree t;
    std::unordered_map<uint32_t, uint16_t> doc_scores;
public:
    SearchIndex();
    ~SearchIndex();
    void add(uint32_t doc_id, std::vector<std::string> tokens, uint16_t score);
    void search(std::string query, size_t max_results);
};

