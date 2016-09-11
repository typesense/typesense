#include "collection.h"

#include <iostream>
#include <numeric>
#include <topster.h>
#include <intersection.h>
#include <match_score.h>
#include <string_utils.h>
#include "sole.hpp"
#include "json.hpp"

Collection::Collection(std::string state_dir_path): seq_id(0) {
    store = new Store(state_dir_path);
    art_tree_init(&t);
}

Collection::~Collection() {
    delete store;
    art_tree_destroy(&t);
}

uint32_t Collection::next_seq_id() {
    return ++seq_id;
}

void Collection::add(std::string json_str) {
    nlohmann::json document = nlohmann::json::parse(json_str);

    if(document.count("id") == 0) {
        sole::uuid u1 = sole::uuid1();
        document["id"] = u1.base62();
    }

    uint32_t seq_id = next_seq_id();

    store->insert(std::to_string(seq_id), document.dump());
    store->insert(document["id"], std::to_string(seq_id));

    std::vector<std::string> tokens;
    StringUtils::tokenize(document["title"], tokens, " ", true);

    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;
    for(uint32_t i=0; i<tokens.size(); i++) {
        auto token = tokens[i];
        std::transform(token.begin(), token.end(), token.begin(), ::tolower);
        token_to_offsets[token].push_back(i);
    }

    for(auto & kv: token_to_offsets) {
        art_document art_doc;
        art_doc.id = seq_id;
        art_doc.score = document["points"];
        art_doc.offsets_len = (uint32_t) kv.second.size();
        art_doc.offsets = new uint32_t[kv.second.size()];

        uint32_t num_hits = 0;

        const unsigned char *key = (const unsigned char *) kv.first.c_str();
        int key_len = (int) kv.first.length() + 1;  // for the terminating \0 char

        art_leaf* leaf = (art_leaf *) art_search(&t, key, key_len);
        if(leaf != NULL) {
            num_hits = leaf->values->ids.getLength();
        }

        num_hits += 1;

        for(auto i=0; i<kv.second.size(); i++) {
            art_doc.offsets[i] = kv.second[i];
        }

        art_insert(&t, key, key_len, &art_doc, num_hits);
        delete art_doc.offsets;
    }

    doc_scores[seq_id] = document["points"];
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
std::vector<nlohmann::json> Collection::search(std::string query, const int num_typos, const size_t max_results) {
    std::vector<std::string> tokens;
    StringUtils::tokenize(query, tokens, " ", true);

    const int max_cost = (num_typos < 0 || num_typos > 2) ? 2 : num_typos;

    std::cout << "Searching with max_cost=" << max_cost << std::endl;

    std::vector<std::vector<art_leaf*>> token_leaves;
    for(std::string token: tokens) {
        std::vector<art_leaf*> leaves;
        art_fuzzy_results(&t, (const unsigned char *) token.c_str(), (int) token.length() + 1, max_cost, 10, leaves);
        if(!leaves.empty()) {
            for(auto i=0; i<leaves.size(); i++) {
                //printf("%s - ", token.c_str());
                //printf("%.*s", leaves[i]->key_len, leaves[i]->key);
                //printf(" - max_cost: %d, - score: %d\n", max_cost, leaves[i]->values->ids.getLength());
            }
            token_leaves.push_back(leaves);
        }
    }

    if(token_leaves.size() == 0) {
        return std::vector<nlohmann::json>();
    }

    //std::cout << "token_leaves.size = " << token_leaves.size() << std::endl;

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

    std::vector<nlohmann::json> results;

    for(uint32_t i=0; i<topster.size; i++) {
        uint32_t id = topster.getKeyAt(i);
        std::cout << "ID: " << id << std::endl;

        const std::string value = store->get(std::to_string(id));
        nlohmann::json document = nlohmann::json::parse(value);
        results.push_back(document);
    }

    return results;
}

void Collection::score_results(Topster<100> &topster, const std::vector<art_leaf *> &query_suggestion,
                                const uint32_t *result_ids, size_t result_size) const {
    for(auto i=0; i<result_size; i++) {
        uint32_t doc_id = result_ids[i];
        std::vector<std::vector<uint16_t>> token_positions;

        // for each token in the query, find the positions that it appears in this document
        for (art_leaf *token_leaf : query_suggestion) {
            std::vector<uint16_t> positions;
            uint32_t doc_index = token_leaf->values->ids.indexOf(doc_id);
            uint32_t start_offset = token_leaf->values->offset_index.at(doc_index);
            uint32_t end_offset = (doc_index == token_leaf->values->ids.getLength() - 1) ?
                                  (token_leaf->values->offsets.getLength() - 1) :
                                  token_leaf->values->offset_index.at(doc_index+1);

            while(start_offset <= end_offset) {
                positions.push_back((uint16_t) token_leaf->values->offsets.at(start_offset));
                start_offset++;
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

inline std::vector<art_leaf *> Collection::_next_suggestion(
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
