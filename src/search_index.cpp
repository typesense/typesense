#include "search_index.h"

#include <iostream>
#include <numeric>
#include <topster.h>
#include <intersection.h>
#include <match_score.h>
#include <string_utils.h>

SearchIndex::SearchIndex() {
    art_tree_init(&t);
}

SearchIndex::~SearchIndex() {
    art_tree_destroy(&t);
}

void SearchIndex::add(uint32_t doc_id, std::vector<std::string> tokens, uint16_t score) {
    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;

    for(uint32_t i=0; i<tokens.size(); i++) {
        auto token = tokens[i];
        std::transform(token.begin(), token.end(), token.begin(), ::tolower);
        token_to_offsets[token].push_back(i);
    }

    for(auto & kv: token_to_offsets) {
        art_document document;
        document.id = doc_id;
        document.score = score;
        document.offsets_len = (uint32_t) kv.second.size();
        document.offsets = new uint32_t[kv.second.size()];

        uint32_t num_hits = document.offsets_len;
        art_leaf* leaf = (art_leaf *) art_search(&t, (const unsigned char *) kv.first.c_str(), (int) kv.first.length());
        if(leaf != NULL) {
            num_hits += leaf->token_count;
        }

        for(auto i=0; i<kv.second.size(); i++) {
            document.offsets[i] = kv.second[i];
        }

        art_insert(&t, (const unsigned char *) kv.first.c_str(), (int) kv.first.length(), &document, num_hits);
        delete document.offsets;
    }

    doc_scores[doc_id] = score;
}


/*
   1. Split the query into tokens
   2. For each token, look up ids using exact lookup
       a. If a token has no result, try again with edit distance of 1, and then 2
   3. Do a limited cartesian product of the word suggestions for each token to form possible corrected search phrases
      (adapted from: http://stackoverflow.com/a/31169617/131050)
   4. Intersect the lists to find docs that match each phrase
   5. Sort the docs based on some ranking criteria
*/
void SearchIndex::search(std::string query, size_t max_results) {
    std::vector<std::string> tokens;
    StringUtils::tokenize(query, tokens, " ", true);

    std::vector<std::vector<art_leaf*>> token_leaves;
    for(std::string token: tokens) {
        std::vector<art_leaf*> leaves;
        int max_cost = 2;
        art_iter_fuzzy_prefix(&t, (const unsigned char *) token.c_str(), (int) token.length(), max_cost, 10, leaves);
        if(!leaves.empty()) {
            for(auto i=0; i<leaves.size(); i++) {
                //printf("%s - ", token.c_str());
                //printf("%.*s", leaves[i]->key_len, leaves[i]->key);
                //printf(" - max_cost: %d, - score: %d\n", max_cost, leaves[i]->token_count);
            }
            token_leaves.push_back(leaves);
        }
    }

    Topster<100> topster;
    size_t total_results = 0;
    const size_t combination_limit = 10;
    auto product = []( long long a, std::vector<art_leaf*>& b ) { return a*b.size(); };
    long long int N = std::accumulate(token_leaves.begin(), token_leaves.end(), 1LL, product );

    for(long long n=0; n<N && n<combination_limit; ++n) {
        // every element in `query_suggestion` represents a token and its associated hits
        std::vector<art_leaf *> query_suggestion = _next_suggestion(token_leaves, n);

        // initialize results with the starting element (for further intersection)
        uint32_t* result_ids = query_suggestion[0]->values->ids.uncompress();
        size_t result_size = query_suggestion[0]->values->ids.getLength();

        if(result_size == 0) continue;

        // intersect the document ids for each token to find docs that contain all the tokens (stored in `result_ids`)
        for(auto i=1; i < query_suggestion.size(); i++) {
            uint32_t* out = new uint32_t[result_size];
            uint32_t* curr = query_suggestion[i]->values->ids.uncompress();
            result_size = Intersection::scalar(result_ids, result_size, curr, query_suggestion[i]->values->ids.getLength(), out);
            delete result_ids;
            delete curr;
            result_ids = out;
        }

        // go through each matching document id and calculate match score
        score_results(topster, query_suggestion, result_ids, result_size);

        total_results += result_size;
        delete result_ids;

        if(total_results >= max_results) break;
    }

    topster.sort();

    for(uint32_t i=0; i<topster.size; i++) {
        uint32_t id = topster.getKeyAt(i);
        std::cout << "ID: " << id << std::endl;
    }
}

void SearchIndex::score_results(Topster<100> &topster, const std::vector<art_leaf *> &query_suggestion,
                                const uint32_t *result_ids, size_t result_size) const {
    for(auto i=0; i<result_size; i++) {
        uint32_t doc_id = result_ids[i];
        std::__1::vector<std::__1::vector<uint16_t>> token_positions;

        // for each token in the query, find the positions that it appears in this document
        for (art_leaf *token_leaf : query_suggestion) {
            std::__1::vector<uint16_t> positions;
            uint32_t doc_index = token_leaf->values->ids.indexOf(doc_id);
            uint32_t offset_index = token_leaf->values->offset_index.at(doc_index);
            uint32_t num_offsets = token_leaf->values->offsets.at(offset_index);
            for (auto offset_count = 1; offset_count <= num_offsets; offset_count++) {
                positions.push_back((uint16_t) token_leaf->values->offsets.at(offset_index + offset_count));
            }
            token_positions.push_back(positions);
        }

        MatchScore mscore = MatchScore::match_score(doc_id, token_positions);
        const uint32_t cumulativeScore = ((uint32_t)(mscore.words_present * 16 + (20 - mscore.distance)) * 64000) + doc_scores.at(doc_id);

        /*std::cout << "result_ids[i]: " << result_ids[i] << " - mscore.distance: "
                  << (int)mscore.distance << " - mscore.words_present: " << (int)mscore.words_present
                  << " - doc_scores[doc_id]: " << (int)doc_scores[doc_id] << "  - cumulativeScore: "
                  << cumulativeScore << std::endl;*/

        topster.add(doc_id, cumulativeScore);
    }
}

inline std::vector<art_leaf *> SearchIndex::_next_suggestion(
        const std::vector<std::vector<art_leaf *>> &token_leaves,
        long long int n) {
    std::vector<art_leaf*> query_suggestion(token_leaves.size());

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for(long long i=token_leaves.size()-1 ; 0<=i ; --i ) {
        q = ldiv(q.quot, token_leaves[i].size());
        query_suggestion[i] = token_leaves[i][q.rem];
    }

    // sort ascending based on matched documents for each token for faster intersection
    sort(query_suggestion.begin(), query_suggestion.end(), [](const art_leaf* left, const art_leaf* right) {
        return left->values->ids.getLength() < right->values->ids.getLength();
    });

    return query_suggestion;
}
