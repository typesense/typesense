#include "index.h"

#include <numeric>
#include <chrono>
#include <unordered_map>
#include <array_utils.h>
#include <match_score.h>
#include <string_utils.h>
#include <art.h>
#include "logger.h"

Index::Index(const std::string name, std::unordered_map<std::string, field> search_schema,
             std::unordered_map<std::string, field> facet_schema, std::unordered_map<std::string, field> sort_schema):
        name(name), search_schema(search_schema), facet_schema(facet_schema), sort_schema(sort_schema) {

    for(const auto pair: search_schema) {
        art_tree *t = new art_tree;
        art_tree_init(t);
        search_index.emplace(pair.first, t);
    }

    for(const auto pair: facet_schema) {
        facet_value fvalue;
        facet_index.emplace(pair.first, fvalue);
    }

    for(const auto pair: sort_schema) {
        spp::sparse_hash_map<uint32_t, number_t> * doc_to_score = new spp::sparse_hash_map<uint32_t, number_t>();
        sort_index.emplace(pair.first, doc_to_score);
    }

    num_documents = 0;

    ready = false;
    processed = false;
    terminate = false;
}

Index::~Index() {
    for(auto & name_tree: search_index) {
        art_tree_destroy(name_tree.second);
        delete name_tree.second;
        name_tree.second = nullptr;
    }

    search_index.clear();

    for(auto & name_map: sort_index) {
        delete name_map.second;
        name_map.second = nullptr;
    }

    sort_index.clear();
}

Option<uint32_t> Index::index_in_memory(const nlohmann::json &document, uint32_t seq_id, int32_t points) {
    // assumes that validation has already been done
    for(const std::pair<std::string, field> & field_pair: search_schema) {
        const std::string & field_name = field_pair.first;
        art_tree *t = search_index.at(field_name);

        if(field_pair.second.type == field_types::STRING) {
            const std::string & text = document[field_name];
            index_string_field(text, points, t, seq_id, field_pair.second.is_facet());
        } else if(field_pair.second.type == field_types::INT32) {
            uint32_t value = document[field_name];
            index_int32_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64) {
            uint64_t value = document[field_name];
            index_int64_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::FLOAT) {
            float value = document[field_name];
            index_float_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::BOOL) {
            bool value = document[field_name];
            index_bool_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            std::vector<std::string> strings = document[field_name];
            index_string_array_field(strings, points, t, seq_id, field_pair.second.is_facet());
        } else if(field_pair.second.type == field_types::INT32_ARRAY) {
            std::vector<int32_t> values = document[field_name];
            index_int32_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64_ARRAY) {
            std::vector<int64_t> values = document[field_name];
            index_int64_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::FLOAT_ARRAY) {
            std::vector<float> values = document[field_name];
            index_float_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::BOOL_ARRAY) {
            std::vector<bool> values = document[field_name];
            index_bool_array_field(values, points, t, seq_id);
        }

        // add numerical values automatically into sort index
        if(field_pair.second.type == field_types::INT32 || field_pair.second.type == field_types::INT64 ||
                field_pair.second.type == field_types::FLOAT || field_pair.second.type == field_types::BOOL) {
            spp::sparse_hash_map<uint32_t, number_t> *doc_to_score = sort_index.at(field_pair.first);

            if(document[field_pair.first].is_number_integer()) {
                doc_to_score->emplace(seq_id, document[field_pair.first].get<int64_t>());
            } else if(document[field_pair.first].is_number_float()) {
                doc_to_score->emplace(seq_id, document[field_pair.first].get<float>());
            } else if(document[field_pair.first].is_boolean()) {
                doc_to_score->emplace(seq_id, (int64_t) document[field_pair.first].get<bool>());
            }
        }
    }

    for(const std::pair<std::string, field> & field_pair: facet_schema) {
        const std::string & field_name = field_pair.first;
        facet_value & fvalue = facet_index.at(field_name);
        if(field_pair.second.type == field_types::STRING) {
            const std::string & value = document[field_name];
            fvalue.index_values(seq_id, { value });
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            const std::vector<std::string> & values = document[field_name];
            fvalue.index_values(seq_id, values);
        }
    }

    num_documents += 1;
    return Option<>(200);
}

void Index::index_int32_field(const int32_t value, uint32_t score, art_tree *t, uint32_t seq_id) const {
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

void Index::index_int64_field(const int64_t value, uint32_t score, art_tree *t, uint32_t seq_id) const {
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

void Index::index_bool_field(const bool value, const uint32_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 1;
    unsigned char key[KEY_LEN];
    key[0] = value ? '1' : '0';
    //key[1] = '\0';

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

void Index::index_float_field(const float value, uint32_t score, art_tree *t, uint32_t seq_id) const {
    const int KEY_LEN = 8;
    unsigned char key[KEY_LEN];

    encode_float(value, key);

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


void Index::index_string_field(const std::string & text, const uint32_t score, art_tree *t,
                                    uint32_t seq_id, const bool verbatim) const {
    std::vector<std::string> tokens;
    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;

    if(verbatim) {
        tokens.push_back(text);
        token_to_offsets[text].push_back(0);
    } else {
        StringUtils::split(text, tokens, " ");

        for(uint32_t i=0; i<tokens.size(); i++) {
            auto & token = tokens[i];
            string_utils.unicode_normalize(token);
            token_to_offsets[token].push_back(i);
        }
    }

    insert_doc(score, t, seq_id, token_to_offsets);
}

void Index::insert_doc(const uint32_t score, art_tree *t, uint32_t seq_id,
                       const std::unordered_map<std::string, std::vector<uint32_t>> &token_to_offsets) const {
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

        for(size_t i=0; i<kv.second.size(); i++) {
            art_doc.offsets[i] = kv.second[i];
        }

        art_insert(t, key, key_len, &art_doc, num_hits);
        delete [] art_doc.offsets;
        art_doc.offsets = nullptr;
    }
}

void Index::index_string_array_field(const std::vector<std::string> & strings, const uint32_t score, art_tree *t,
                                          uint32_t seq_id, const bool verbatim) const {
    std::unordered_map<std::string, std::unordered_map<size_t, std::vector<uint32_t>>> token_array_positions;

    for(size_t array_index = 0; array_index < strings.size(); array_index++) {
        const std::string & str = strings[array_index];

        std::vector<std::string> tokens;
        StringUtils::split(str, tokens, " ");

        for(uint32_t i=0; i<tokens.size(); i++) {
            auto & token = tokens[i];
            string_utils.unicode_normalize(token);
            token_array_positions[token][array_index].push_back(i);
        }
    }

    std::unordered_map<std::string, std::vector<uint32_t>> token_to_offsets;

    for(const auto & kv: token_array_positions) {
        for(size_t array_index = 0; array_index < strings.size(); array_index++) {
            token_to_offsets[kv.first].insert(token_to_offsets[kv.first].end(),
                                              token_array_positions[kv.first][array_index].begin(),
                                              token_array_positions[kv.first][array_index].end());
            token_to_offsets[kv.first].push_back(ARRAY_SEPARATOR);
        }
    }

    insert_doc(score, t, seq_id, token_to_offsets);
}

void Index::index_int32_array_field(const std::vector<int32_t> & values, const uint32_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int32_t value: values) {
        index_int32_field(value, score, t, seq_id);
    }
}

void Index::index_int64_array_field(const std::vector<int64_t> & values, const uint32_t score, art_tree *t,
                                         uint32_t seq_id) const {
    for(const int64_t value: values) {
        index_int64_field(value, score, t, seq_id);
    }
}

void Index::index_bool_array_field(const std::vector<bool> & values, const uint32_t score, art_tree *t,
                                   uint32_t seq_id) const {
    for(const bool value: values) {
        index_bool_field(value, score, t, seq_id);
    }
}

void Index::index_float_array_field(const std::vector<float> & values, const uint32_t score, art_tree *t,
                             uint32_t seq_id) const {
    for(const float value: values) {
        index_float_field(value, score, t, seq_id);
    }
}

void Index::do_facets(std::vector<facet> & facets, uint32_t* result_ids, size_t results_size) {
    for(auto & a_facet: facets) {
        // assumed that facet fields have already been validated upstream
        const field & facet_field = facet_schema.at(a_facet.field_name);
        const facet_value & fvalue = facet_index.at(facet_field.name);

        for(size_t i = 0; i < results_size; i++) {
            uint32_t doc_seq_id = result_ids[i];
            if(fvalue.doc_values.count(doc_seq_id) != 0) {
                // for every result document, get the values associated and increment counter
                const std::vector<uint32_t> & value_indices = fvalue.doc_values.at(doc_seq_id);
                for(size_t j = 0; j < value_indices.size(); j++) {
                    const std::string & facet_value = fvalue.index_value.at(value_indices.at(j));
                    a_facet.result_map[facet_value] += 1;
                }
            }
        }
    }
}

void Index::search_candidates(const uint8_t & field_id, uint32_t* filter_ids, size_t filter_ids_length, std::vector<facet> & facets,
                              const std::vector<sort_by> & sort_fields,
                              std::vector<token_candidates> & token_candidates_vec, const token_ordering token_order,
                              std::vector<std::vector<art_leaf*>> & searched_queries, Topster<512> & topster,
                              uint32_t** all_result_ids, size_t & all_result_ids_len,
                              const size_t & max_results, const bool prefix) {
    const long long combination_limit = 10;

    auto product = []( long long a, token_candidates & b ) { return a*b.candidates.size(); };
    long long int N = std::accumulate(token_candidates_vec.begin(), token_candidates_vec.end(), 1LL, product);

    for(long long n=0; n<N && n<combination_limit; ++n) {
        // every element in `query_suggestion` contains a token and its associated hits
        std::vector<art_leaf *> query_suggestion = next_suggestion(token_candidates_vec, n);

        /*for(size_t i=0; i < query_suggestion.size(); i++) {
            LOG(INFO) << "i: " << i << " - " << query_suggestion[i]->key;
        }*/

        // initialize results with the starting element (for further intersection)
        size_t result_size = query_suggestion[0]->values->ids.getLength();
        if(result_size == 0) {
            continue;
        }

        uint32_t total_cost = 0;
        uint32_t* result_ids = query_suggestion[0]->values->ids.uncompress();

        for(auto tc: token_candidates_vec) {
            total_cost += tc.cost;
        }

        // intersect the document ids for each token to find docs that contain all the tokens (stored in `result_ids`)
        for(size_t i=1; i < query_suggestion.size(); i++) {
            uint32_t* out = nullptr;
            uint32_t* ids = query_suggestion[i]->values->ids.uncompress();
            result_size = ArrayUtils::and_scalar(ids, query_suggestion[i]->values->ids.getLength(), result_ids, result_size, &out);
            delete[] ids;
            delete[] result_ids;
            result_ids = out;
        }

        if(result_size == 0) {
            delete[] result_ids;
            continue;
        }

        if(filter_ids != nullptr) {
            // intersect once again with filter ids
            uint32_t* filtered_result_ids = nullptr;
            size_t filtered_results_size = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                                  result_size, &filtered_result_ids);

            uint32_t* new_all_result_ids;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, filtered_result_ids,
                                  filtered_results_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            do_facets(facets, filtered_result_ids, filtered_results_size);

            // go through each matching document id and calculate match score
            score_results(sort_fields, (uint16_t) searched_queries.size(), field_id, total_cost, topster, query_suggestion,
                          filtered_result_ids, filtered_results_size);

            delete[] filtered_result_ids;
            delete[] result_ids;
        } else {
            do_facets(facets, result_ids, result_size);

            uint32_t* new_all_result_ids;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, result_ids,
                                  result_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            score_results(sort_fields, (uint16_t) searched_queries.size(), field_id, total_cost, topster, query_suggestion,
                          result_ids, result_size);
            delete[] result_ids;
        }

        searched_queries.push_back(query_suggestion);

        if(all_result_ids_len >= max_results) {
            break;
        }
    }
}

size_t Index::union_of_ids(std::vector<std::pair<uint32_t*, size_t>> & result_array_pairs,
                                uint32_t **results_out) {
    uint32_t *results = nullptr;
    size_t results_length = 0;

    uint32_t *prev_results = nullptr;
    size_t prev_results_length = 0;

    for(const std::pair<uint32_t*, size_t> & result_array_pair: result_array_pairs) {
        results_length = ArrayUtils::or_scalar(prev_results, prev_results_length, result_array_pair.first,
                                               result_array_pair.second, &results);
        delete [] prev_results;
        prev_results = results;
        prev_results_length = results_length;
    }

    *results_out = results;
    return results_length;
}

Option<uint32_t> Index::do_filtering(uint32_t** filter_ids_out, const std::vector<filter> & filters) {
    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length = 0;

    for(size_t i = 0; i < filters.size(); i++) {
        const filter & a_filter = filters[i];

        if(search_index.count(a_filter.field_name) != 0) {
            art_tree* t = search_index.at(a_filter.field_name);
            field f = search_schema.at(a_filter.field_name);
            std::vector<std::pair<uint32_t*, size_t>> filter_result_array_pairs;

            if(f.is_integer()) {
                std::vector<const art_leaf*> leaves;

                for(const std::string & filter_value: a_filter.values) {
                    if(f.type == field_types::INT32 || f.type == field_types::INT32_ARRAY) {
                        int32_t value = (int32_t) std::stoi(filter_value);
                        art_int32_search(t, value, a_filter.compare_operator, leaves);
                    } else {
                        int64_t value = (int64_t) std::stoi(filter_value);
                        art_int64_search(t, value, a_filter.compare_operator, leaves);
                    }

                    for(const art_leaf* leaf: leaves) {
                        filter_result_array_pairs.push_back(std::make_pair(leaf->values->ids.uncompress(),
                                                                leaf->values->ids.getLength()));
                    }
                }
            } else if(f.is_float()) {
                std::vector<const art_leaf*> leaves;

                for(const std::string & filter_value: a_filter.values) {
                    float value = (float) std::atof(filter_value.c_str());
                    art_float_search(t, value, a_filter.compare_operator, leaves);
                    for(const art_leaf* leaf: leaves) {
                        filter_result_array_pairs.push_back(std::make_pair(leaf->values->ids.uncompress(),
                                                                leaf->values->ids.getLength()));
                    }
                }
            } else if(f.is_bool()) {
                std::vector<const art_leaf*> leaves;

                for(const std::string & filter_value: a_filter.values) {
                    art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) filter_value.c_str(),
                                                             filter_value.length());
                    if(leaf) {
                        filter_result_array_pairs.push_back(std::make_pair(leaf->values->ids.uncompress(),
                                                                           leaf->values->ids.getLength()));
                    }
                }
            } else if(f.is_string()) {
                for(const std::string & filter_value: a_filter.values) {
                    // we have to tokenize the string, standardize it and then do an exact match
                    std::vector<std::string> str_tokens;
                    StringUtils::split(filter_value, str_tokens, " ");

                    uint32_t* filtered_ids = nullptr;
                    size_t filtered_size = 0;

                    for(size_t i = 0; i < str_tokens.size(); i++) {
                        std::string & str_token = str_tokens[i];
                        string_utils.unicode_normalize(str_token);
                        art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) str_token.c_str(),
                                                                 str_token.length()+1);
                        if(leaf == nullptr) {
                            continue;
                        }

                        if(i == 0) {
                            filtered_ids = leaf->values->ids.uncompress();
                            filtered_size = leaf->values->ids.getLength();
                        } else {
                            // do AND for an exact match
                            uint32_t* out = nullptr;
                            uint32_t* leaf_ids = leaf->values->ids.uncompress();
                            filtered_size = ArrayUtils::and_scalar(filtered_ids, filtered_size, leaf_ids,
                                                                   leaf->values->ids.getLength(), &out);
                            delete[] leaf_ids;
                            delete[] filtered_ids;
                            filtered_ids = out;
                        }
                    }

                    filter_result_array_pairs.push_back(std::make_pair(filtered_ids, filtered_size));
                }
            }

            uint32_t* result_ids = nullptr;
            size_t result_ids_length = union_of_ids(filter_result_array_pairs, &result_ids);

            if(i == 0) {
                filter_ids = result_ids;
                filter_ids_length = result_ids_length;
            } else {
                uint32_t* filtered_results = nullptr;
                filter_ids_length = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                             result_ids_length, &filtered_results);
                delete [] result_ids;
                delete [] filter_ids;
                filter_ids = filtered_results;
            }

            for(std::pair<uint32_t*, size_t> & filter_result_array_pair: filter_result_array_pairs) {
                delete[] filter_result_array_pair.first;
            }
        }
    }

    *filter_ids_out = filter_ids;
    return Option<>(filter_ids_length);
}

void Index::run_search() {
    while(true) {
        // wait until main thread sends data
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [this]{return ready;});

        if(terminate) {
            break;
        }

        // after the wait, we own the lock.
        search(search_params.outcome, search_params.query, search_params.search_fields,
               search_params.filters, search_params.facets,
               search_params.sort_fields_std, search_params.num_typos, search_params.per_page, search_params.page,
               search_params.token_order, search_params.prefix, search_params.drop_tokens_threshold,
               search_params.field_order_kvs, search_params.all_result_ids_len, search_params.searched_queries);

        // hand control back to main thread
        processed = true;
        ready = false;

        // manual unlocking is done before notifying, to avoid waking up the waiting thread only to block again
        lk.unlock();
        cv.notify_one();
    }
}

void Index::search(Option<uint32_t> & outcome, std::string query, const std::vector<std::string> search_fields,
                             const std::vector<filter> & filters, std::vector<facet> & facets,
                             std::vector<sort_by> sort_fields_std, const int num_typos,
                             const size_t per_page, const size_t page, const token_ordering token_order,
                             const bool prefix, const size_t drop_tokens_threshold,
                             std::vector<Topster<512>::KV> & field_order_kvs,
                             size_t & all_result_ids_len, std::vector<std::vector<art_leaf*>> & searched_queries) {

    const size_t num_results = (page * per_page);

    // process the filters first

    uint32_t* filter_ids = nullptr;
    Option<uint32_t> op_filter_ids_length = do_filtering(&filter_ids, filters);
    if(!op_filter_ids_length.ok()) {
        outcome = Option<uint32_t>(op_filter_ids_length);
        return ;
    }

    const uint32_t filter_ids_length = op_filter_ids_length.get();

    // Order of `fields` are used to sort results
    //auto begin = std::chrono::high_resolution_clock::now();
    uint32_t* all_result_ids = nullptr;

    Topster<512> topster;

    if(query == "*") {
        uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - 0);
        score_results(sort_fields_std, (uint16_t) searched_queries.size(), field_id, 0, topster, {},
                      filter_ids, filter_ids_length);
        all_result_ids_len = filter_ids_length;
    } else {
        const size_t num_search_fields = std::min(search_fields.size(), (size_t) FIELD_LIMIT_NUM);
        for(size_t i = 0; i < num_search_fields; i++) {
            const std::string & field = search_fields[i];
            // proceed to query search only when no filters are provided or when filtering produces results
            if(filters.size() == 0 || filter_ids_length > 0) {
                uint8_t field_id = (uint8_t)(FIELD_LIMIT_NUM - i);
                search_field(field_id, query, field, filter_ids, filter_ids_length, facets, sort_fields_std,
                             num_typos, num_results, searched_queries, topster, &all_result_ids, all_result_ids_len,
                             token_order, prefix, drop_tokens_threshold);
            }
        }
    }

    // must be sorted before iterated upon to remove "empty" array entries
    topster.sort();

    // order of fields specified matter: matching docs from earlier fields are more important
    for(uint32_t t = 0; t < topster.size && t < num_results; t++) {
        Topster<512>::KV* kv = topster.getKV(t);
        field_order_kvs.push_back(*kv);
    }

    delete [] filter_ids;
    delete [] all_result_ids;

    //long long int timeMillis = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //!LOG(INFO) << "Time taken for result calc: " << timeMillis << "us";

    outcome = Option<uint32_t>(1);
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
void Index::search_field(const uint8_t & field_id, std::string & query, const std::string & field,
                         uint32_t *filter_ids, size_t filter_ids_length,
                         std::vector<facet> & facets, const std::vector<sort_by> & sort_fields, const int num_typos,
                         const size_t num_results, std::vector<std::vector<art_leaf*>> & searched_queries,
                         Topster<512> &topster, uint32_t** all_result_ids, size_t & all_result_ids_len,
                         const token_ordering token_order, const bool prefix, const size_t drop_tokens_threshold) {
    std::vector<std::string> tokens;
    StringUtils::split(query, tokens, " ");

    const size_t max_cost = (num_typos < 0 || num_typos > 2) ? 2 : num_typos;

    // To prevent us from doing ART search repeatedly as we iterate through possible corrections
    spp::sparse_hash_map<std::string, std::vector<art_leaf*>> token_cost_cache;

    // Used to drop the least occurring token(s) for partial searches
    std::unordered_map<std::string, uint32_t> token_to_count;

    std::vector<std::vector<int>> token_to_costs;

    for(size_t token_index = 0; token_index < tokens.size(); token_index++) {
        std::vector<int> all_costs;
        const size_t token_len = tokens[token_index].length();

        // This ensures that we don't end up doing a cost of 1 for a single char etc.
        int bounded_cost = max_cost;
        if(token_len > 0 && max_cost >= token_len && (token_len == 1 || token_len == 2)) {
            bounded_cost = token_len - 1;
        }

        for(int cost = 0; cost <= bounded_cost; cost++) {
            all_costs.push_back(cost);
        }

        token_to_costs.push_back(all_costs);
        string_utils.unicode_normalize(tokens[token_index]);
    }

    // stores candidates for each token, i.e. i-th index would have all possible tokens with a cost of "c"
    std::vector<token_candidates> token_candidates_vec;

    const long long combination_limit = 10;
    auto product = []( long long a, std::vector<int>& b ) { return a*b.size(); };
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

        token_candidates_vec.clear();
        size_t token_index = 0;

        while(token_index < tokens.size()) {
            // For each token, look up the generated cost for this iteration and search using that cost
            std::string token = tokens[token_index];
            const std::string token_cost_hash = token + std::to_string(costs[token_index]);

            std::vector<art_leaf*> leaves;
            //LOG(INFO) << "\nSearching for: " << token << " - cost: " << costs[token_index];

            if(token_cost_cache.count(token_cost_hash) != 0) {
                leaves = token_cost_cache[token_cost_hash];
            } else {
                // prefix should apply only for last token
                const bool prefix_search = prefix && ((token_index == tokens.size()-1) ? true : false);
                const size_t token_len = prefix_search ? (int) token.length() : (int) token.length() + 1;

                // If this is a prefix search, look for more candidates and do a union of those document IDs
                const int max_candidates = prefix_search ? 10 : 3;
                art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                                 costs[token_index], costs[token_index], max_candidates, token_order, prefix_search, leaves);

                if(!leaves.empty()) {
                    token_cost_cache.emplace(token_cost_hash, leaves);
                }
            }

            if(!leaves.empty()) {
                //log_leaves(costs[token_index], token, leaves);
                token_candidates_vec.push_back(token_candidates{token, costs[token_index], leaves});
                token_to_count[token] = std::max(token_to_count[token], leaves.at(0)->values->ids.getLength());
            } else {
                // No result at `cost = costs[token_index]`. Remove costs until `cost` for token and re-do combinations
                auto it = std::find(token_to_costs[token_index].begin(), token_to_costs[token_index].end(), costs[token_index]);
                if(it != token_to_costs[token_index].end()) {
                    token_to_costs[token_index].erase(it);

                    // when no more costs are left for this token and `drop_tokens_threshold` is breached
                    if(token_to_costs[token_index].empty() && all_result_ids_len >= drop_tokens_threshold) {
                        n = combination_limit; // to break outer loop
                        break;
                    }

                    // otherwise, we try to drop the token and search with remaining tokens
                    if(token_to_costs[token_index].empty()) {
                        token_to_costs.erase(token_to_costs.begin()+token_index);
                        tokens.erase(tokens.begin()+token_index);
                        costs.erase(costs.begin()+token_index);
                        token_index--;
                    }
                }

                // To continue outerloop on new cost combination
                n = -1;
                N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);
                break;
            }

            token_index++;
        }

        if(token_candidates_vec.size() != 0 && token_candidates_vec.size() == tokens.size()) {
            // If all tokens were found, go ahead and search for candidates with what we have so far
            search_candidates(field_id, filter_ids, filter_ids_length, facets, sort_fields, token_candidates_vec,
                              token_order, searched_queries, topster, all_result_ids, all_result_ids_len,
                              Index::SEARCH_LIMIT_NUM, prefix);

            if (all_result_ids_len >= Index::SEARCH_LIMIT_NUM) {
                // If we don't find enough results, we continue outerloop (looking at tokens with greater cost)
                break;
            }
        }

        n++;
    }

    // When there are not enough overall results and atleast one token has results
    if(all_result_ids_len < drop_tokens_threshold && token_to_count.size() > 1) {
        // Drop token with least hits and try searching again
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
            // iterate till last but one
            truncated_query += " " + token_count_pairs.at(i).first;
        }

        return search_field(field_id, truncated_query, field, filter_ids, filter_ids_length, facets, sort_fields, num_typos,
                            num_results, searched_queries, topster, all_result_ids, all_result_ids_len,
                            token_order, prefix);
    }
}

void Index::log_leaves(const int cost, const std::string &token, const std::vector<art_leaf *> &leaves) const {
    LOG(INFO) << "Token: " << token << ", cost: " << cost;

    for(size_t i=0; i < leaves.size(); i++) {
        printf("%.*s, ", leaves[i]->key_len, leaves[i]->key);
        LOG(INFO) << "frequency: " << leaves[i]->values->ids.getLength() << ", max_score: " << leaves[i]->max_score;
        /*for(auto j=0; j<leaves[i]->values->ids.getLength(); j++) {
            LOG(INFO) << "id: " << leaves[i]->values->ids.at(j);
        }*/
    }
}

void Index::score_results(const std::vector<sort_by> & sort_fields, const uint16_t & query_index, const uint8_t & field_id,
                          const uint32_t total_cost, Topster<512> & topster,
                          const std::vector<art_leaf *> &query_suggestion,
                          const uint32_t *result_ids, const size_t result_size) const {

    spp::sparse_hash_map<const art_leaf*, uint32_t*> leaf_to_indices;

    for (art_leaf *token_leaf : query_suggestion) {
        uint32_t *indices = new uint32_t[result_size];
        token_leaf->values->ids.indexOf(result_ids, result_size, indices);
        leaf_to_indices.emplace(token_leaf, indices);
    }

    spp::sparse_hash_map<uint32_t, number_t> * primary_rank_scores = nullptr;
    spp::sparse_hash_map<uint32_t, number_t> * secondary_rank_scores = nullptr;

    // Used for asc/desc ordering. NOTE: Topster keeps biggest keys (i.e. it's desc in nature)
    number_t primary_rank_factor;
    number_t secondary_rank_factor;

    if(sort_fields.size() > 0) {
        // assumed that rank field exists in the index - checked earlier in the chain
        primary_rank_scores = sort_index.at(sort_fields[0].name);

        // initialize primary_rank_factor
        field sort_field = sort_schema.at(sort_fields[0].name);
        if(sort_field.is_single_integer()) {
            primary_rank_factor = ((int64_t) 1);
        } else {
            primary_rank_factor = ((float) 1);
        }

        if(sort_fields[0].order == sort_field_const::asc) {
            primary_rank_factor = -primary_rank_factor;
        }
    }

    if(sort_fields.size() > 1) {
        secondary_rank_scores = sort_index.at(sort_fields[1].name);

        // initialize secondary_rank_factor
        field sort_field = sort_schema.at(sort_fields[1].name);
        if(sort_field.is_single_integer()) {
            secondary_rank_factor = ((int64_t) 1);
        } else {
            secondary_rank_factor = ((float) 1);
        }

        if(sort_fields[1].order == sort_field_const::asc) {
            secondary_rank_factor = -secondary_rank_factor;
        }
    }

    //auto begin = std::chrono::high_resolution_clock::now();

    char empty_offset_diffs[16];
    std::fill_n(empty_offset_diffs, 16, 0);
    Match single_token_match = Match(1, 0, 0, empty_offset_diffs);
    const uint64_t single_token_match_score = single_token_match.get_match_score(total_cost, field_id);

    for(size_t i=0; i<result_size; i++) {
        const uint32_t seq_id = result_ids[i];

        uint64_t match_score = 0;

        if(query_suggestion.size() <= 1) {
            match_score = single_token_match_score;
        } else {
            std::vector<std::vector<std::vector<uint16_t>>> array_token_positions;
            populate_token_positions(query_suggestion, leaf_to_indices, i, array_token_positions);

            for(const std::vector<std::vector<uint16_t>> & token_positions: array_token_positions) {
                if(token_positions.size() == 0) {
                    continue;
                }
                const Match & match = Match::match(seq_id, token_positions);
                uint64_t this_match_score = match.get_match_score(total_cost, field_id);

                if(this_match_score > match_score) {
                    match_score = this_match_score;
                }

                /*std::ostringstream os;
                os << name << ", total_cost: " << (255 - total_cost)
                   << ", words_present: " << match.words_present
                   << ", match_score: " << match_score
                   << ", match.distance: " << match.distance
                   << ", seq_id: " << seq_id << std::endl;
                std::cout << os.str();*/
            }
        }

        const int64_t default_score = 0;
        number_t primary_rank_score = default_score;
        number_t secondary_rank_score = default_score;

        if(primary_rank_scores) {
            auto it = primary_rank_scores->find(seq_id);
            primary_rank_score = (it == primary_rank_scores->end()) ? default_score : it->second;
        }

        if(secondary_rank_scores) {
            auto it = secondary_rank_scores->find(seq_id);
            secondary_rank_score = (it == secondary_rank_scores->end()) ? default_score : it->second;
        }

        const number_t & primary_rank_value = primary_rank_score * primary_rank_factor;
        const number_t & secondary_rank_value = secondary_rank_score * secondary_rank_factor;
        topster.add(seq_id, field_id, query_index, match_score, primary_rank_value, secondary_rank_value);
    }

    //long long int timeNanos = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin).count();
    //LOG(INFO) << "Time taken for results iteration: " << timeNanos << "ms";

    for (auto it = leaf_to_indices.begin(); it != leaf_to_indices.end(); it++) {
        delete [] it->second;
        it->second = nullptr;
    }
}

void Index::populate_token_positions(const std::vector<art_leaf *> &query_suggestion,
                                     spp::sparse_hash_map<const art_leaf *, uint32_t *> &leaf_to_indices,
                                     size_t result_index,
                                     std::vector<std::vector<std::vector<uint16_t>>> &array_token_positions) {

    // array_token_positions:
    // for every element in a potential array, for every token in query suggestion, get the positions

    // first let's ascertain the size of the array
    size_t array_size = 0;

    for (const art_leaf *token_leaf : query_suggestion) {
        uint32_t doc_index = leaf_to_indices.at(token_leaf)[result_index];
        if(doc_index == token_leaf->values->ids.getLength()) {
            continue;
        }

        uint32_t start_offset = token_leaf->values->offset_index.at(doc_index);
        uint32_t end_offset = (doc_index == token_leaf->values->ids.getLength() - 1) ?
                              token_leaf->values->offsets.getLength() :
                              token_leaf->values->offset_index.at(doc_index+1);

        while(start_offset < end_offset) {
            uint16_t pos = (uint16_t) token_leaf->values->offsets.at(start_offset);
            if(pos == ARRAY_SEPARATOR) {
                array_size++;
            }
            start_offset++;
        }

        if(array_size == 0) {
            // for plain string fields that don't use an ARRAY_SEPARATOR
            array_size = 1;
        }

        break;
    }

    // initialize array_token_positions
    array_token_positions = std::vector<std::vector<std::vector<uint16_t>>>(array_size);

    // for each token in the query, find the positions that it appears in the array
    for (const art_leaf *token_leaf : query_suggestion) {
        uint32_t doc_index = leaf_to_indices.at(token_leaf)[result_index];
        if(doc_index == token_leaf->values->ids.getLength()) {
            continue;
        }

        uint32_t start_offset = token_leaf->values->offset_index.at(doc_index);
        uint32_t end_offset = (doc_index == token_leaf->values->ids.getLength() - 1) ?
                              token_leaf->values->offsets.getLength() :
                              token_leaf->values->offset_index.at(doc_index+1);

        size_t array_index = 0;
        std::vector<uint16_t> positions;

        while(start_offset < end_offset) {
            uint16_t pos = (uint16_t) token_leaf->values->offsets.at(start_offset);
            start_offset++;

            if(pos == ARRAY_SEPARATOR) {
                if(positions.size() != 0) {
                    array_token_positions[array_index].push_back(positions);
                    positions.clear();
                }
                array_index++;
                continue;
            }

            positions.push_back(pos);
        }

        if(positions.size() != 0) {
            // for plain string fields that don't use an ARRAY_SEPARATOR
            array_token_positions[array_index].push_back(positions);
        }
    }
}

inline std::vector<art_leaf *> Index::next_suggestion(const std::vector<token_candidates> &token_candidates_vec,
                                                      long long int n) {
    std::vector<art_leaf*> query_suggestion(token_candidates_vec.size());

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for(long long i = 0 ; i < (long long) token_candidates_vec.size(); i++) {
        q = ldiv(q.quot, token_candidates_vec[i].candidates.size());
        query_suggestion[i] = token_candidates_vec[i].candidates[q.rem];
    }

    // Sort ascending based on matched documents for each token for faster intersection.
    // However, this causes the token order to deviate from original query's order.
    sort(query_suggestion.begin(), query_suggestion.end(), [](const art_leaf* left, const art_leaf* right) {
        return left->values->ids.getLength() < right->values->ids.getLength();
    });

    return query_suggestion;
}

void Index::remove_and_shift_offset_index(sorted_array &offset_index, const uint32_t *indices_sorted,
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

Option<uint32_t> Index::remove(const uint32_t seq_id, nlohmann::json & document) {
    for(auto & name_field: search_schema) {
        // Go through all the field names and find the keys+values so that they can be removed from in-memory index
        std::vector<std::string> tokens;
        if(name_field.second.type == field_types::STRING) {
            StringUtils::split(document[name_field.first], tokens, " ");
        } else if(name_field.second.type == field_types::STRING_ARRAY) {
            tokens = document[name_field.first].get<std::vector<std::string>>();
        } else if(name_field.second.type == field_types::INT32) {
            const int KEY_LEN = 8;
            unsigned char key[KEY_LEN];
            int32_t value = document[name_field.first].get<int32_t>();
            encode_int32(value, key);
            tokens.push_back(std::string((char*)key, KEY_LEN));
        } else if(name_field.second.type == field_types::INT32_ARRAY) {
            std::vector<int32_t> values = document[name_field.first].get<std::vector<int32_t>>();
            for(const int32_t value: values) {
                const int KEY_LEN = 8;
                unsigned char key[KEY_LEN];
                encode_int32(value, key);
                tokens.push_back(std::string((char*)key, KEY_LEN));
            }
        } else if(name_field.second.type == field_types::INT64) {
            const int KEY_LEN = 8;
            unsigned char key[KEY_LEN];
            int64_t value = document[name_field.first].get<int64_t>();
            encode_int64(value, key);
            tokens.push_back(std::string((char*)key, KEY_LEN));
        } else if(name_field.second.type == field_types::INT64_ARRAY) {
            std::vector<int64_t> values = document[name_field.first].get<std::vector<int64_t>>();
            for(const int64_t value: values) {
                const int KEY_LEN = 8;
                unsigned char key[KEY_LEN];
                encode_int64(value, key);
                tokens.push_back(std::string((char*)key, KEY_LEN));
            }
        } else if(name_field.second.type == field_types::FLOAT) {
            const int KEY_LEN = 8;
            unsigned char key[KEY_LEN];
            int64_t value = document[name_field.first].get<int64_t>();
            encode_float(value, key);
            tokens.push_back(std::string((char*)key, KEY_LEN));
        } else if(name_field.second.type == field_types::FLOAT_ARRAY) {
            std::vector<float> values = document[name_field.first].get<std::vector<float>>();
            for(const float value: values) {
                const int KEY_LEN = 8;
                unsigned char key[KEY_LEN];
                encode_float(value, key);
                tokens.push_back(std::string((char*)key, KEY_LEN));
            }
        } else if(name_field.second.type == field_types::BOOL) {
            const int KEY_LEN = 1;
            unsigned char key[KEY_LEN];
            bool value = document[name_field.first].get<bool>();
            key[0] = value ? '1' : '0';
            tokens.push_back(std::string((char*)key, KEY_LEN));
        } else if(name_field.second.type == field_types::BOOL_ARRAY) {
            std::vector<bool> values = document[name_field.first].get<std::vector<bool>>();
            for(const bool value: values) {
                const int KEY_LEN = 1;
                unsigned char key[KEY_LEN];
                key[0] = value ? '1' : '0';
                tokens.push_back(std::string((char*)key, KEY_LEN));
            }
        }

        for(auto & token: tokens) {
            const unsigned char *key;
            int key_len;

            if(name_field.second.type == field_types::STRING_ARRAY || name_field.second.type == field_types::STRING) {
                string_utils.unicode_normalize(token);
                key = (const unsigned char *) token.c_str();
                key_len = (int) (token.length() + 1);
            } else {
                key = (const unsigned char *) token.c_str();
                key_len = (int) (token.length());
            }

            art_leaf* leaf = (art_leaf *) art_search(search_index.at(name_field.first), key, key_len);
            if(leaf != NULL) {
                uint32_t seq_id_values[1] = {seq_id};
                uint32_t doc_index = leaf->values->ids.indexOf(seq_id);

                if(doc_index == leaf->values->ids.getLength()) {
                    // not found - happens when 2 tokens repeat in a field, e.g "is it or is is not?"
                    continue;
                }

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
                    LOG(INFO) << "i: " << i << ", val: " << leaf->values->offset_index.at(i);
                }
                LOG(INFO) << "----";*/

                if(leaf->values->ids.getLength() == 0) {
                    art_values* values = (art_values*) art_delete(search_index.at(name_field.first), key, key_len);
                    delete values;
                    values = nullptr;
                }
            }
        }
    }

    // remove facets if any
    for(auto & field_facet_value: facet_index) {
        field_facet_value.second.doc_values.erase(seq_id);
    }

    // remove sort index if any
    for(auto & field_doc_value_map: sort_index) {
        field_doc_value_map.second->erase(seq_id);
    }

    return Option<uint32_t>(seq_id);
}

art_leaf* Index::get_token_leaf(const std::string & field_name, const unsigned char* token, uint32_t token_len) {
    const art_tree *t = search_index.at(field_name);
    return (art_leaf*) art_search(t, token, (int) token_len);
}