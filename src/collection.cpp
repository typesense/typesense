#include "collection.h"

#include <numeric>
#include <chrono>
#include <intersection.h>
#include <match_score.h>
#include <string_utils.h>

Collection::Collection(const std::string name, const uint32_t collection_id, const uint32_t next_seq_id, Store *store,
                       const std::vector<field> &search_fields, const std::vector<std::string> rank_fields):
    name(name), collection_id(collection_id), next_seq_id(next_seq_id), store(store), rank_fields(rank_fields) {

    for(const field& field: search_fields) {
        art_tree *t = new art_tree;
        art_tree_init(t);
        index_map.emplace(field.name, t);
        schema.emplace(field.name, field);
    }
}

Collection::~Collection() {
    for(std::pair<std::string, field> name_field: schema) {
        art_tree *t = index_map.at(name_field.first);
        art_tree_destroy(t);
        t = nullptr;
    }
}

uint32_t Collection::get_next_seq_id() {
    store->increment(get_next_seq_id_key(name), 1);
    return next_seq_id++;
}

std::string Collection::add(std::string json_str) {
    nlohmann::json document = nlohmann::json::parse(json_str);

    uint32_t seq_id = get_next_seq_id();
    std::string seq_id_str = std::to_string(seq_id);

    if(document.count("id") == 0) {
        document["id"] = seq_id_str;
    }

    store->insert(get_seq_id_key(seq_id), document.dump());
    store->insert(get_doc_id_key(document["id"]), seq_id_str);

    index_in_memory(document, seq_id);
    return document["id"];
}

void Collection::index_in_memory(const nlohmann::json &document, uint32_t seq_id) {
    for(const std::pair<std::string, field> & field_pair: schema) {
        const std::string & field_name = field_pair.first;
        art_tree *t = index_map.at(field_name);
        uint32_t points = document["points"];

        if(field_pair.second.type == field_types::STRING) {
            const std::string & text = document[field_name];
            index_string_field(text, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT32) {
            uint32_t value = document[field_name];
            index_int32_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64) {
            uint64_t value = document[field_name];
            index_int64_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            std::vector<std::string> strings = document[field_name];
            index_string_array_field(strings, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT32_ARRAY) {
            std::vector<std::string> strings = document[field_name];
        }
    }
    if(rank_fields.size() > 0 && document.count(rank_fields[0])) {
        primary_rank_scores[seq_id] = document[rank_fields[0]].get<int64_t>();
    }

    if(rank_fields.size() > 1 && document.count(rank_fields[1])) {
        secondary_rank_scores[seq_id] = document[rank_fields[1]].get<int64_t>();
    }
}

void Collection::index_int32_field(const int32_t value, uint32_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 8;
    unsigned char key[KEY_LEN];

    encode_int32(value, key);

    uint32_t num_hits = 0;
    art_leaf* leaf = (art_leaf *) art_search(t, key, KEY_LEN);
    if(leaf != NULL) {
        num_hits = leaf->values->ids.getLength();
    }

    num_hits += 1;

    art_document art_doc;
    art_doc.id = seq_id;
    art_doc.score = score;
    art_doc.offsets_len = 0;
    art_doc.offsets = nullptr;

    art_insert(t, key, KEY_LEN, &art_doc, num_hits);
}

void Collection::index_int64_field(const int64_t value, uint32_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 8;
    unsigned char key[KEY_LEN];

    encode_int64(value, key);

    uint32_t num_hits = 0;
    art_leaf* leaf = (art_leaf *) art_search(t, key, KEY_LEN);
    if(leaf != NULL) {
        num_hits = leaf->values->ids.getLength();
    }

    num_hits += 1;

    art_document art_doc;
    art_doc.id = seq_id;
    art_doc.score = score;
    art_doc.offsets_len = 0;
    art_doc.offsets = nullptr;

    art_insert(t, key, KEY_LEN, &art_doc, num_hits);
}

void Collection::index_string_field(const std::string & text, const uint32_t score, art_tree *t, uint32_t seq_id) const {
    std::vector<std::string> tokens;
    StringUtils::tokenize(text, tokens, " ", true);

    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;
    for(uint32_t i=0; i<tokens.size(); i++) {
        auto token = tokens[i];
        transform(token.begin(), token.end(), token.begin(), tolower);
        token_to_offsets[token].push_back(i);
    }

    for(auto & kv: token_to_offsets) {
        art_document art_doc;
        art_doc.id = seq_id;
        art_doc.score = score;
        art_doc.offsets_len = (uint32_t) kv.second.size();
        art_doc.offsets = new uint32_t[kv.second.size()];

        uint32_t num_hits = 0;

        const unsigned char *key = (const unsigned char *) kv.first.c_str();
        int key_len = (int) kv.first.length() + 1;  // for the terminating \0 char

        art_leaf* leaf = (art_leaf *) art_search(t, key, key_len);
        if(leaf != NULL) {
            num_hits = leaf->values->ids.getLength();
        }

        num_hits += 1;

        for(auto i=0; i<kv.second.size(); i++) {
            art_doc.offsets[i] = kv.second[i];
        }

        art_insert(t, key, key_len, &art_doc, num_hits);
        delete art_doc.offsets;
    }
}

void Collection::index_string_array_field(const std::vector<std::string> & strings, const uint32_t score, art_tree *t,
                                          uint32_t seq_id) const {
    for(const std::string & str: strings) {
        index_string_field(str, score, t, seq_id);
    }
}

void Collection::index_int32_array_field(const std::vector<int32_t> & values, const uint32_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int32_t value: values) {
        index_int32_field(value, score, t, seq_id);
    }
}

void Collection::index_int64_array_field(const std::vector<int64_t> & values, const uint32_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int64_t value: values) {
        index_int64_field(value, score, t, seq_id);
    }
}

void Collection::search_candidates(int & token_rank, std::vector<std::vector<art_leaf*>> & token_leaves,
                                   Topster<100> & topster, size_t & total_results, size_t & num_found, const size_t & max_results) {
    const size_t combination_limit = 10;
    auto product = []( long long a, std::vector<art_leaf*>& b ) { return a*b.size(); };
    long long int N = std::accumulate(token_leaves.begin(), token_leaves.end(), 1LL, product);

    for(long long n=0; n<N && n<combination_limit; ++n) {
        // every element in `query_suggestion` contains a token and its associated hits
        std::vector<art_leaf *> query_suggestion = next_suggestion(token_leaves, n);
        token_rank++;

        // initialize results with the starting element (for further intersection)
        uint32_t* result_ids = query_suggestion[0]->values->ids.uncompress();
        size_t result_size = query_suggestion[0]->values->ids.getLength();

        if(result_size == 0) continue;

        // intersect the document ids for each token to find docs that contain all the tokens (stored in `result_ids`)
        for(auto i=1; i < query_suggestion.size(); i++) {
            uint32_t* out = new uint32_t[std::min(result_size, (size_t) query_suggestion[i]->values->ids.getLength())];
            result_size = query_suggestion[i]->values->ids.intersect(result_ids, result_size, out);
            delete[] result_ids;
            result_ids = out;
        }

        // go through each matching document id and calculate match score
        score_results(topster, token_rank, query_suggestion, result_ids, result_size);
        delete[] result_ids;

        num_found += result_size;
        total_results += topster.size;

        if(total_results >= max_results) {
            break;
        }
    }
}

nlohmann::json Collection::search(std::string query, const std::vector<std::string> fields, const std::vector<filter> filters,
                                  const int num_typos, const size_t num_results,
                                  const token_ordering token_order, const bool prefix) {
    size_t num_found = 0;

    // process the filters first
    /*for(const filter & a_filter: filters) {
        if(index_map.count(a_filter.field_name) != 0) {
            art_tree* t = index_map.at(a_filter.field_name);
            nlohmann::json json_value = nlohmann::json::parse(a_filter.value_json);
            if(json_value.is_number()) {
                // do integer art search
            } else if(json_value.is_string()) {

            } else if(json_value.is_array()) {

            }
        }
    }*/

    // Order of `fields` are used to rank results
    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<int, Topster<100>::KV>> field_order_kvs;

    for(int i = 0; i < fields.size(); i++) {
        Topster<100> topster;
        const std::string & field = fields[i];

        search(query, field, num_typos, num_results, topster, num_found, token_order, prefix);
        topster.sort();

        for(auto t = 0; t < topster.size && t < num_results; t++) {
            field_order_kvs.push_back(std::make_pair(fields.size() - i, topster.getKV(t)));
        }
    }

    std::sort(field_order_kvs.begin(), field_order_kvs.end(),
      [](const std::pair<int, Topster<100>::KV> & a, const std::pair<int, Topster<100>::KV> & b) {
        if(a.second.match_score != b.second.match_score) return a.second.match_score > b.second.match_score;
        if(a.second.primary_attr != b.second.primary_attr) return a.second.primary_attr > b.second.primary_attr;
        if(a.second.secondary_attr != b.second.secondary_attr) return a.second.secondary_attr > b.second.secondary_attr;
        if(a.first != b.first) return a.first > b.first;
        return a.second.key > b.second.key;
    });

    nlohmann::json result = nlohmann::json::object();
    result["hits"] = nlohmann::json::array();

    for(auto field_order_kv: field_order_kvs) {
        std::string value;
        const std::string &seq_id_key = get_seq_id_key((uint32_t) field_order_kv.second.key);
        store->get(seq_id_key, value);
        nlohmann::json document = nlohmann::json::parse(value);
        result["hits"].push_back(document);
    }

    result["found"] = num_found;

    long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //!std::cout << "Time taken for result calc: " << timeMillis << "us" << std::endl;
    //!store->print_memory_usage();
    return result;
}

/*
   1. Split the query into tokens
   2. Outer loop will generate bounded cartesian product with costs for each token
   3. Inner loop will iterate on each token with associated cost
   4. Cartesian product of the results of the token searches will be used to form search phrases
      (cartesian product adapted from: http://stackoverflow.com/a/31169617/131050)
   4. Intersect the lists to find docs that match each phrase
   5. Sort the docs based on some ranking criteria
*/
void Collection::search(std::string & query, const std::string & field, const int num_typos, const size_t num_results,
                        Topster<100> & topster, size_t & num_found, const token_ordering token_order, const bool prefix) {
    std::vector<std::string> tokens;
    StringUtils::tokenize(query, tokens, " ", true);

    const int max_cost = (num_typos < 0 || num_typos > 2) ? 2 : num_typos;
    const size_t max_results = std::min(num_results, (size_t) Collection::MAX_RESULTS);

    size_t total_results = topster.size;

    // To prevent us from doing ART search repeatedly as we iterate through possible corrections
    spp::sparse_hash_map<std::string, std::vector<art_leaf*>> token_cost_cache;

    // Used to drop the least occurring token(s) for partial searches
    spp::sparse_hash_map<std::string, uint32_t> token_to_count;

    std::vector<std::vector<int>> token_to_costs;
    std::vector<int> all_costs;

    for(int cost = 0; cost <= max_cost; cost++) {
        all_costs.push_back(cost);
    }

    for(size_t token_index = 0; token_index < tokens.size(); token_index++) {
        token_to_costs.push_back(all_costs);
        std::transform(tokens[token_index].begin(), tokens[token_index].end(), tokens[token_index].begin(), ::tolower);
    }

    std::vector<std::vector<art_leaf*>> token_leaves;

    const size_t combination_limit = 10;
    auto product = []( long long a, std::vector<int>& b ) { return a*b.size(); };
    int token_rank = 0;
    long long n = 0;
    long long int N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);

    while(n < N && n < combination_limit) {
        // Outerloop generates combinations of [cost to max_cost] for each token
        // For e.g. for a 3-token query: [0, 0, 0], [0, 0, 1], [0, 1, 1] etc.
        std::vector<uint32_t> costs(token_to_costs.size());
        ldiv_t q { n, 0 };
        for(long long i = (token_to_costs.size() - 1); 0 <= i ; --i ) {
            q = ldiv(q.quot, token_to_costs[i].size());
            costs[i] = token_to_costs[i][q.rem];
        }

        token_leaves.clear();
        int token_index = 0;

        while(token_index < tokens.size()) {
            // For each token, look up the generated cost for this iteration and search using that cost
            std::string token = tokens[token_index];
            const std::string token_cost_hash = token + std::to_string(costs[token_index]);

            std::vector<art_leaf*> leaves;
            /*std::cout << "\nSearching for: " << token << " - cost: " << costs[token_index] << ", token_rank: "
                      << token_rank << std::endl;*/

            if(token_cost_cache.count(token_cost_hash) != 0) {
                leaves = token_cost_cache[token_cost_hash];
            } else {
                int token_len = prefix ? (int) token.length() : (int) token.length() + 1;
                art_fuzzy_search(index_map.at(field), (const unsigned char *) token.c_str(), token_len,
                                 costs[token_index], costs[token_index], 3, token_order, prefix, leaves);
                if(!leaves.empty()) {
                    token_cost_cache.emplace(token_cost_hash, leaves);
                }
            }

            if(!leaves.empty()) {
                //!log_leaves(costs[token_index], token, leaves);
                token_leaves.push_back(leaves);
                token_to_count[token] = leaves.at(0)->values->ids.getLength();
            } else {
                // No result at `cost = costs[token_index]` => remove cost for token and re-do combinations
                auto it = std::find(token_to_costs[token_index].begin(), token_to_costs[token_index].end(), costs[token_index]);
                if(it != token_to_costs[token_index].end()) {
                    token_to_costs[token_index].erase(it);

                    // no more costs left for this token, clean up
                    if(token_to_costs[token_index].empty()) {
                        token_to_costs.erase(token_to_costs.begin()+token_index);
                        tokens.erase(tokens.begin()+token_index);
                        token_index--;
                    }
                }

                n = -1;
                N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);

                break;
            }

            token_index++;
        }

        if(token_leaves.size() != 0 && token_leaves.size() == tokens.size()) {
            // If a) all tokens were found, or b) Some were skipped because they don't exist within max_cost,
            // go ahead and search for candidates with what we have so far
            search_candidates(token_rank, token_leaves, topster, total_results, num_found, max_results);

            if (total_results >= max_results) {
                // If we don't find enough results, we continue outerloop (looking at tokens with greater cost)
                break;
            }
        }

        n++;
    }

    // When there are not enough overall results and atleast one token has results
    if(topster.size < max_results && token_to_count.size() > 1) {
        // Drop certain token with least hits and try searching again
        std::string truncated_query;

        std::vector<std::pair<std::string, uint32_t>> token_count_pairs;
        for (auto itr = token_to_count.begin(); itr != token_to_count.end(); ++itr) {
            token_count_pairs.push_back(*itr);
        }

        std::sort(token_count_pairs.begin(), token_count_pairs.end(), [=]
            (const std::pair<std::string, uint32_t>& a, const std::pair<std::string, uint32_t>& b) {
                return a.second > b.second;
            }
        );

        for(uint32_t i = 0; i < token_count_pairs.size()-1; i++) {
            if(token_to_count.count(token_count_pairs[i].first) != 0) {
                truncated_query += " " + token_count_pairs.at(i).first;
            }
        }

        return search(truncated_query, field, num_typos, num_results, topster, num_found, token_order, prefix);
    }
}

void Collection::log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const {
    printf("Token: %s, cost: %d, candidates: \n", token.c_str(), cost);
    for(auto i=0; i < leaves.size(); i++) {
        printf("%.*s, ", leaves[i]->key_len, leaves[i]->key);
        printf("frequency: %d, max_score: %d\n", leaves[i]->values->ids.getLength(), leaves[i]->max_score);
        /*for(auto j=0; j<leaves[i]->values->ids.getLength(); j++) {
            printf("id: %d\n", leaves[i]->values->ids.at(j));
        }*/
    }
}

void Collection::score_results(Topster<100> &topster, const int & token_rank,
                               const std::vector<art_leaf *> &query_suggestion, const uint32_t *result_ids,
                               const size_t result_size) const {

    const int max_token_rank = 250;

    for(auto i=0; i<result_size; i++) {
        uint32_t seq_id = result_ids[i];
        std::vector<std::vector<uint16_t>> token_positions;

        MatchScore mscore;

        if(query_suggestion.size() == 1) {
            mscore = MatchScore{1, 1};
        } else {
            // for each token in the query, find the positions that it appears in this document
            for (art_leaf *token_leaf : query_suggestion) {
                std::vector<uint16_t> positions;
                uint32_t doc_index = token_leaf->values->ids.indexOf(seq_id);
                uint32_t start_offset = token_leaf->values->offset_index.at(doc_index);
                uint32_t end_offset = (doc_index == token_leaf->values->ids.getLength() - 1) ?
                                      token_leaf->values->offsets.getLength() :
                                      token_leaf->values->offset_index.at(doc_index+1);

                while(start_offset < end_offset) {
                    positions.push_back((uint16_t) token_leaf->values->offsets.at(start_offset));
                    start_offset++;
                }

                token_positions.push_back(positions);
            }

            mscore = MatchScore::match_score(seq_id, token_positions);
        }

        int token_rank_score = max_token_rank - token_rank;

        // Construct a single match_score from individual components (for multi-field sort)
        const uint64_t match_score = (token_rank_score << 16) +
                                     ((uint64_t)(mscore.words_present) << 8) +
                                     (MAX_SEARCH_TOKENS - mscore.distance);

        int64_t primary_rank_score = primary_rank_scores.count(seq_id) > 0 ? primary_rank_scores.at(seq_id) : 0;
        int64_t secondary_rank_score = secondary_rank_scores.count(seq_id) > 0 ? secondary_rank_scores.at(seq_id) : 0;
        topster.add(seq_id, match_score, primary_rank_score, secondary_rank_score);
        /*std::cout << "token_rank_score: " << token_rank_score << ", match_score: "
                  << match_score << ", primary_rank_score: " << primary_rank_score << ", seq_id: " << seq_id << std::endl;*/
    }
}

inline std::vector<art_leaf *> Collection::next_suggestion(const std::vector<std::vector<art_leaf *>> &token_leaves,
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

void Collection::remove_and_shift_offset_index(sorted_array &offset_index, const uint32_t *indices_sorted,
                                               const uint32_t indices_length) {
    uint32_t *curr_array = offset_index.uncompress();
    uint32_t *new_array = new uint32_t[offset_index.getLength()];

    new_array[0] = 0;
    uint32_t new_index = 0;
    uint32_t curr_index = 0;
    uint32_t indices_counter = 0;
    uint32_t shift_value = 0;

    while(curr_index < offset_index.getLength()) {
        if(indices_counter < indices_length && curr_index >= indices_sorted[indices_counter]) {
            // skip copying
            if(curr_index == indices_sorted[indices_counter]) {
                curr_index++;
                const uint32_t diff = curr_index == offset_index.getLength() ?
                                0 : (offset_index.at(curr_index) - offset_index.at(curr_index-1));

                shift_value += diff;
            }
            indices_counter++;
        } else {
            new_array[new_index++] = curr_array[curr_index++] - shift_value;
        }
    }

    offset_index.load(new_array, new_index);

    delete[] curr_array;
    delete[] new_array;
}

void Collection::remove(std::string id) {
    std::string seq_id_str;
    store->get(get_doc_id_key(id), seq_id_str);

    uint32_t seq_id = (uint32_t) std::stol(seq_id_str);

    std::string parsed_document;
    store->get(get_seq_id_key(seq_id), parsed_document);

    nlohmann::json document = nlohmann::json::parse(parsed_document);

    std::vector<std::string> tokens;
    StringUtils::tokenize(document["title"], tokens, " ", true);

    for(auto token: tokens) {
        std::transform(token.begin(), token.end(), token.begin(), ::tolower);

        const unsigned char *key = (const unsigned char *) token.c_str();
        int key_len = (int) (token.length() + 1);

        art_leaf* leaf = (art_leaf *) art_search(index_map.at("title"), key, key_len);
        if(leaf != NULL) {
            uint32_t seq_id_values[1] = {seq_id};

            uint32_t doc_index = leaf->values->ids.indexOf(seq_id);

            /*
            auto len = leaf->values->offset_index.getLength();
            for(auto i=0; i<len; i++) {
                std::cout << "i: " << i << ", val: " << leaf->values->offset_index.at(i) << std::endl;
            }
            std::cout << "----" << std::endl;
            */
            uint32_t start_offset = leaf->values->offset_index.at(doc_index);
            uint32_t end_offset = (doc_index == leaf->values->ids.getLength() - 1) ?
                                  leaf->values->offsets.getLength() :
                                  leaf->values->offset_index.at(doc_index+1);

            uint32_t doc_indices[1] = {doc_index};
            remove_and_shift_offset_index(leaf->values->offset_index, doc_indices, 1);

            leaf->values->offsets.remove_index(start_offset, end_offset);
            leaf->values->ids.remove_values(seq_id_values, 1);

            /*len = leaf->values->offset_index.getLength();
            for(auto i=0; i<len; i++) {
                std::cout << "i: " << i << ", val: " << leaf->values->offset_index.at(i) << std::endl;
            }
            std::cout << "----" << std::endl;*/

            if(leaf->values->ids.getLength() == 0) {
                art_delete(index_map.at("title"), key, key_len);
            }
        }
    }

    store->remove(get_doc_id_key(id));
    store->remove(get_seq_id_key(seq_id));
}

std::string Collection::get_next_seq_id_key(std::string collection_name) {
    return std::string(COLLECTION_NEXT_SEQ_PREFIX) + "_" + collection_name;
}

std::string Collection::get_seq_id_key(uint32_t seq_id) {
    // We can't simply do std::to_string() because we want to preserve the byte order
    unsigned char bytes[4];
    bytes[0] = (unsigned char) ((seq_id >> 24) & 0xFF);
    bytes[1] = (unsigned char) ((seq_id >> 16) & 0xFF);
    bytes[2] = (unsigned char) ((seq_id >> 8) & 0xFF);
    bytes[3] = (unsigned char) ((seq_id & 0xFF));

    return get_seq_id_collection_prefix() + "_" + std::string(bytes, bytes+4);
}

std::string Collection::get_doc_id_key(std::string doc_id) {
    return std::to_string(collection_id) + "_" + DOC_ID_PREFIX + doc_id;
}

uint32_t Collection::get_collection_id() {
    return collection_id;
}

uint32_t Collection::doc_id_to_seq_id(std::string doc_id) {
    std::string seq_id_str;
    store->get(get_doc_id_key(doc_id), seq_id_str);
    uint32_t seq_id = (uint32_t) std::stoi(seq_id_str);
    return seq_id;
}

std::vector<std::string> Collection::get_rank_fields() {
    return rank_fields;
}

spp::sparse_hash_map<std::string, field> Collection::get_schema() {
    return schema;
};

std::string Collection::get_meta_key(std::string collection_name) {
    return COLLECTION_META_PREFIX + collection_name;
}

std::string Collection::get_seq_id_collection_prefix() {
    return std::to_string(collection_id) + "_" + std::string(SEQ_ID_PREFIX);
}