#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <cstdlib>
#include <numeric>
#include <time.h>
#include <art.h>
#include <unordered_map>
#include "topster.h"
#include "intersection.h"
#include "matchscore.h"
#include "util.h"

using namespace std;

static int test_prefix_cb(void *data, const unsigned char *k, uint32_t k_len, void *val) {
    cout << "#>>>>Key: ";
    printf("%.*s", k_len, k);
    cout << "LENGTH OF IDS: " << ((art_values*)val)->ids.getLength() << endl;

    for(uint32_t i=0; i<((art_values*)val)->ids.getLength(); i++) {
        cout << ", ID: " << ((art_values*)val)->ids.at(i) << endl;
    }
    return 0;
}

void benchmark_heap_array() {
    srand (time(NULL));

    vector<uint32_t> records;

    for(uint32_t i=0; i<10000000; i++) {
        records.push_back((const unsigned int &) rand());
    }

    vector<uint32_t> hits;

    for(uint32_t i=0; i<records.size(); i++) {
        if(i%10 == 0) {
            hits.push_back(i);
        }
    }

    auto begin = std::chrono::high_resolution_clock::now();

    Topster<4000> heapArray;

    for(uint32_t i=0; i<hits.size(); i++) {
        heapArray.add(i, records[hits[i]]);
    }

    std::sort(std::begin(heapArray.data), std::end(heapArray.data));

    long long int timeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    for(uint32_t i=0; i<heapArray.size; i++) {
        cout << "Res: " << heapArray.data[i] << endl;
    }

    cout << "Time taken: " << timeMillis << endl;
}

void index_document(art_tree& t, uint32_t doc_id, vector<string> tokens, uint16_t score) {
    unordered_map<string, vector<uint32_t>> token_to_offsets;

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

      uint32_t num_docs = 0;

      vector<art_leaf*> results;
      art_iter_fuzzy_prefix(&t, (const unsigned char *) kv.first.c_str(), (int) kv.first.length(), 0, 1, results);
      if(results.size() == 1) {
        num_docs = results[0]->values->ids.getLength();
      }

      document.score = (uint16_t) (((uint16_t) num_docs) + 1);

      //cout << "Inserting " << kv.first << " with score: " << document.score << endl;

      for(auto i=0; i<kv.second.size(); i++) {
        document.offsets[i] = kv.second[i];
      }
      art_insert(&t, (const unsigned char *) kv.first.c_str(), (int) kv.first.length(), &document);

      delete document.offsets;
    }
}

/*
   1. Split q into tokens
   2. For each token, look up ids using exact lookup
       a. If a token has no result, try again with edit distance of 1, and then 2
   3. Do a limited cartesian product of the word suggestions for each token to form possible corrected search phrases
      (adapted from: http://stackoverflow.com/a/31169617/131050)
   4. Intersect the lists to find docs that match each phrase
   5. Sort the docs based on some ranking criteria
 */
void find_documents(art_tree & t, unordered_map<uint32_t, uint16_t>& docscores, string query, size_t max_results) {
  vector<string> tokens;
  tokenize(query, tokens, " ", true);

  vector<vector<art_leaf*>> token_leaves;
  for(auto token: tokens) {
      for(int max_cost=0; max_cost<2; max_cost++) {
        vector<art_leaf*> leaves;
        art_iter_fuzzy_prefix(&t, (const unsigned char *) token.c_str(), (int) token.length(), max_cost, 3, leaves);
        if(!leaves.empty()) {
          token_leaves.push_back(leaves);
          break;
        }
      }
  }

  Topster<100> topster;
  size_t total_results = 0;
  const size_t combination_limit = 10;
  auto product = []( long long a, vector<art_leaf*>& b ) { return a*b.size(); };
  long long int N = accumulate(token_leaves.begin(), token_leaves.end(), 1LL, product );

  for(long long n=0; n<N && n<combination_limit; ++n) {
    // every element in vector `query_suggestion` represents a token and its associated hits
    vector<art_leaf*> query_suggestion(token_leaves.size());

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for( long long i=token_leaves.size()-1 ; 0<=i ; --i ) {
        q = div(q.quot, token_leaves[i].size());
        query_suggestion[i] = token_leaves[i][q.rem];
    }

    // sort ascending based on matched documents for each token to perform effective intersection
    sort(query_suggestion.begin(), query_suggestion.end(), [](const art_leaf* left, const art_leaf* right) {
      return left->values->ids.getLength() < right->values->ids.getLength();
    });

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
    for(auto i=0; i<result_size; i++) {
        uint32_t doc_id = result_ids[i];
        std::vector<std::vector<uint16_t>> token_positions;

        // for each token in the query, find the positions that it appears in this document
        for (art_leaf *token_leaf : query_suggestion) {
            vector<uint16_t> positions;
            uint32_t doc_index = token_leaf->values->ids.indexOf(doc_id);
            uint32_t offset_index = token_leaf->values->offset_index.at(doc_index);
            uint32_t num_offsets = token_leaf->values->offsets.at(offset_index);
            for (auto offset_count = 1; offset_count <= num_offsets; offset_count++) {
              positions.push_back((uint16_t) token_leaf->values->offsets.at(offset_index + offset_count));
            }
            token_positions.push_back(positions);
        }

        MatchScore mscore = match_score(doc_id, token_positions);
        const uint32_t cumulativeScore = ((uint32_t)(mscore.words_present * 16 + (20 - mscore.distance)) * 64000) + docscores[doc_id];

        //cout << "result_ids[i]: " << result_ids[i] << " - score.words_present: " << mscore.words_present << endl;
        topster.add(doc_id, cumulativeScore);
    }

    total_results += result_size;
    delete result_ids;

    if(total_results >= max_results) break;
  }

  topster.sort();

  cout << "RESULTS: " << endl << endl;

  for(uint32_t i=0; i<topster.size; i++) {
    uint32_t id = topster.getKeyAt(i);
    cout << "ID: " << id << endl;
  }

  //cin.get();
}

std::string ReplaceAll(std::string str, const std::string& from, const std::string& to) {
  size_t start_pos = 0;
  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
  }
  return str;
}

int main() {
    art_tree t;
    art_tree_init(&t);

    unordered_map<uint32_t, uint16_t> docscores;

    std::ifstream infile("/Users/kishorenc/others/wreally/search/test/documents.txt");
    //std::ifstream infile("/data/hnstories.tsv");

    std::string line;
    uint32_t doc_id = 1;

    while (std::getline(infile, line)) {
        vector<string> parts;
        tokenize(line, parts, "\t", true);
        line = ReplaceAll(line, "\"", "");

        vector<string> tokens;
        tokenize(parts[0], tokens, " ", true);

        if(parts.size() != 2) continue;

        docscores[doc_id] = (uint16_t) stoi(parts[1]);
        index_document(t, doc_id, tokens, stoi(parts[1]));
        doc_id++;
    }

    cout << "FINISHED INDEXING!" << endl << flush;

    /*const unsigned char *prefix = (const unsigned char *) "the";
    size_t prefix_len = strlen((const char *) prefix);
    std::vector<art_leaf*> results;

    auto begin = std::chrono::high_resolution_clock::now();
    art_iter_fuzzy_prefix(&t, prefix, prefix_len, 0, 2, results);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();

    art_iter_prefix(&t, prefix, strlen((const char *) prefix), test_prefix_cb, NULL);
    art_iter(&t, test_prefix_cb, NULL);

    cout << "Time taken: " << timeMillis << "us" << endl;

    for(auto leaf: results) {
        std::cout << ">>>>/Key: " << leaf->key << " - score: " << leaf->score << std::endl;
        for(uint32_t i=0; i<leaf->values->ids.getLength(); i++) {
            std::cout << ", ID: " << leaf->values->ids.at(i) << std::endl;
        }
        std::cout << ", Value: " << leaf->values->ids.at(0) << std::endl;
    }*/

//    find_documents(t, docscores, "lanch", 10);

    string token = "lanch";
    vector<art_leaf*> leaves;

    auto begin = std::chrono::high_resolution_clock::now();
    art_iter_fuzzy_prefix(&t, (const unsigned char *) token.c_str(), (int) token.length(), 1, 4, leaves);
    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    for(auto leaf: leaves) {
      cout << "Word: " << leaf->key << " - score: " << leaf->max_score << endl;
    }

    cout << "Time taken: " << timeMillis << "us" << endl;


    art_tree_destroy(&t);
    return 0;
}