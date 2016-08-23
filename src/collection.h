#pragma once

#include <string>
#include <vector>
#include <art.h>
#include <unordered_map>
#include <collection_state.h>
#include <topster.h>

class Collection {
private:
    CollectionState state;
    art_tree t;
    std::unordered_map<uint32_t, uint16_t> doc_scores;
public:
    Collection();
    ~Collection();
    void add(std::vector<std::string> tokens, uint16_t score);
    void search(std::string query, size_t max_results);

    static inline std::vector<art_leaf *> _next_suggestion(const std::vector<std::vector<art_leaf *>> &token_leaves,
                                             long long int n);

    void score_results(Topster<100> &topster, const std::vector<art_leaf *> &query_suggestion,
                       const uint32_t *result_ids,
                       size_t result_size) const;
};

