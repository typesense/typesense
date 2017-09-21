#include "collection.h"

#include <numeric>
#include <chrono>
#include <array_utils.h>
#include <match_score.h>
#include <string_utils.h>
#include <art.h>

Collection::Collection(const std::string name, const uint32_t collection_id, const uint32_t next_seq_id, Store *store,
                       const std::vector<field> &search_fields, const std::vector<field> & facet_fields,
                       const std::vector<field> & sort_fields, const std::string token_ranking_field):
                       name(name), collection_id(collection_id), next_seq_id(next_seq_id), store(store),
                       token_ranking_field(token_ranking_field) {

    for(const field& field: search_fields) {
        art_tree *t = new art_tree;
        art_tree_init(t);
        search_index.emplace(field.name, t);
        search_schema.emplace(field.name, field);
    }

    for(const field& field: facet_fields) {
        facet_value fvalue;
        facet_index.emplace(field.name, fvalue);
        facet_schema.emplace(field.name, field);
    }

    for(const field & sort_field: sort_fields) {
        spp::sparse_hash_map<uint32_t, number_t> * doc_to_score = new spp::sparse_hash_map<uint32_t, number_t>();
        sort_index.emplace(sort_field.name, doc_to_score);
        sort_schema.emplace(sort_field.name, sort_field);
    }

    num_documents = 0;
}

Collection::~Collection() {
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

uint32_t Collection::get_next_seq_id() {
    store->increment(get_next_seq_id_key(name), 1);
    return next_seq_id++;
}

void Collection::set_next_seq_id(uint32_t seq_id) {
    next_seq_id = seq_id;
}

void Collection::increment_next_seq_id_field() {
    next_seq_id++;
}

Option<std::string> Collection::add(const std::string & json_str) {
    nlohmann::json document;
    try {
        document = nlohmann::json::parse(json_str);
    } catch(...) {
        return Option<std::string>(400, "Bad JSON.");
    }

    uint32_t seq_id = get_next_seq_id();
    std::string seq_id_str = std::to_string(seq_id);

    if(document.count("id") == 0) {
        document["id"] = seq_id_str;
    } else if(!document["id"].is_string()) {
        return Option<std::string>(400, "Document's `id` field should be a string.");
    }

    std::string doc_id = document["id"];

    const Option<uint32_t> & index_memory_op = index_in_memory(document, seq_id);

    if(!index_memory_op.ok()) {
        return Option<std::string>(index_memory_op.code(), index_memory_op.error());
    }

    store->insert(get_doc_id_key(document["id"]), seq_id_str);
    store->insert(get_seq_id_key(seq_id), document.dump());

    return Option<std::string>(doc_id);
}

Option<uint32_t> Collection::index_in_memory(const nlohmann::json &document, uint32_t seq_id) {
    if(!token_ranking_field.empty() && document.count(token_ranking_field) == 0) {
        return Option<>(400, "Field `" + token_ranking_field  + "` has been declared as a token ranking field, "
                        "but is not found in the document.");
    }

    if(!token_ranking_field.empty() && !document[token_ranking_field].is_number_integer()) {
        return Option<>(400, "Token ranking field `" + token_ranking_field  + "` must be an int32.");
    }

    if(!token_ranking_field.empty() && document[token_ranking_field].get<int64_t>() > INT32_MAX) {
        return Option<>(400, "Token ranking field `" + token_ranking_field  + "` exceeds maximum value of int32.");
    }

    if(!token_ranking_field.empty() && document[token_ranking_field].get<int64_t>() < 0) {
        return Option<>(400, "Token ranking field `" + token_ranking_field  + "` must not be negative.");
    }

    uint32_t points = 0;
    if(!token_ranking_field.empty()) {
        points = document[token_ranking_field];
    }

    for(const std::pair<std::string, field> & field_pair: search_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared as a search field in the schema, "
                            "but is not found in the document.");
        }

        art_tree *t = search_index.at(field_name);

        if(field_pair.second.type == field_types::STRING) {
            if(!document[field_name].is_string()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a string.");
            }
            const std::string & text = document[field_name];
            index_string_field(text, points, t, seq_id, false);
        } else if(field_pair.second.type == field_types::INT32) {
            if(!document[field_name].is_number_integer()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an int32.");
            }

            if(document[field_name].get<int64_t>() > INT32_MAX) {
                return Option<>(400, "Search field `" + field_name  + "` exceeds maximum value of int32.");
            }

            uint32_t value = document[field_name];
            index_int32_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64) {
            if(!document[field_name].is_number_integer()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an int64.");
            }

            uint64_t value = document[field_name];
            index_int64_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::FLOAT) {
            if(!document[field_name].is_number_float()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a float.");
            }

            float value = document[field_name];
            index_float_field(value, points, t, seq_id);
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a string array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_string()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a string array.");
            }

            std::vector<std::string> strings = document[field_name];
            index_string_array_field(strings, points, t, seq_id, false);
        } else if(field_pair.second.type == field_types::INT32_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an int32 array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number_integer()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an int32 array.");
            }

            std::vector<int32_t> values = document[field_name];
            index_int32_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::INT64_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an int64 array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number_integer()) {
                return Option<>(400, "Search field `" + field_name  + "` must be an int64 array.");
            }

            std::vector<int64_t> values = document[field_name];
            index_int64_array_field(values, points, t, seq_id);
        } else if(field_pair.second.type == field_types::FLOAT_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a float array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_number_float()) {
                return Option<>(400, "Search field `" + field_name  + "` must be a float array.");
            }

            std::vector<float> values = document[field_name];
            index_float_array_field(values, points, t, seq_id);
        }
    }

    for(const std::pair<std::string, field> & field_pair: facet_schema) {
        const std::string & field_name = field_pair.first;

        if(document.count(field_name) == 0) {
            return Option<>(400, "Field `" + field_name  + "` has been declared as a facet field in the schema, "
                            "but is not found in the document.");
        }

        facet_value & fvalue = facet_index.at(field_name);
        if(field_pair.second.type == field_types::STRING) {
            if(!document[field_name].is_string()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a string.");
            }
            const std::string & value = document[field_name];
            fvalue.index_values(seq_id, { value });
        } else if(field_pair.second.type == field_types::STRING_ARRAY) {
            if(!document[field_name].is_array()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a string array.");
            }

            if(document[field_name].size() > 0 && !document[field_name][0].is_string()) {
                return Option<>(400, "Facet field `" + field_name  + "` must be a string array.");
            }

            const std::vector<std::string> & values = document[field_name];
            fvalue.index_values(seq_id, values);
        }
    }

    for(const std::pair<std::string, field> & field_pair: sort_schema) {
        const field & sort_field = field_pair.second;

        if(document.count(sort_field.name) == 0) {
            return Option<>(400, "Field `" + sort_field.name  + "` has been declared as a sort field in the schema, "
                    "but is not found in the document.");
        }

        if(!document[sort_field.name].is_number()) {
            return Option<>(400, "Sort field `" + sort_field.name  + "` must be a number.");
        }

        spp::sparse_hash_map<uint32_t, number_t> *doc_to_score = sort_index.at(sort_field.name);

        if(document[sort_field.name].is_number_integer()) {
            doc_to_score->emplace(seq_id, document[sort_field.name].get<int64_t>());
        } else {
            doc_to_score->emplace(seq_id, document[sort_field.name].get<float>());
        }
    }

    num_documents += 1;
    return Option<>(200);
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

void Collection::index_float_field(const float value, uint32_t score, art_tree *t, uint32_t seq_id) const {
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


void Collection::index_string_field(const std::string & text, const uint32_t score, art_tree *t,
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
            transform(token.begin(), token.end(), token.begin(), tolower);
            token_to_offsets[token].push_back(i);
        }
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
        delete [] art_doc.offsets;
        art_doc.offsets = nullptr;
    }
}

void Collection::index_string_array_field(const std::vector<std::string> & strings, const uint32_t score, art_tree *t,
                                          uint32_t seq_id, const bool verbatim) const {
    for(const std::string & str: strings) {
        index_string_field(str, score, t, seq_id, verbatim);
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

void Collection::index_float_array_field(const std::vector<float> & values, const uint32_t score, art_tree *t,
                             uint32_t seq_id) const {
    for(const float value: values) {
        index_float_field(value, score, t, seq_id);
    }
}

void Collection::do_facets(std::vector<facet> & facets, uint32_t* result_ids, size_t results_size) {
    for(auto & a_facet: facets) {
        // assumed that facet fields have already been validated upstream
        const field & facet_field = facet_schema.at(a_facet.field_name);
        const facet_value & fvalue = facet_index.at(facet_field.name);

        for(auto i = 0; i < results_size; i++) {
            uint32_t doc_seq_id = result_ids[i];
            if(fvalue.doc_values.count(doc_seq_id) != 0) {
                // for every result document, get the values associated and increment counter
                const std::vector<uint32_t> & value_indices = fvalue.doc_values.at(doc_seq_id);
                for(auto j = 0; j < value_indices.size(); j++) {
                    const std::string & facet_value = fvalue.index_value.at(value_indices.at(j));
                    a_facet.result_map[facet_value] += 1;
                }
            }
        }
    }
}

void Collection::search_candidates(uint32_t* filter_ids, size_t filter_ids_length, std::vector<facet> & facets,
                                   const std::vector<sort_by> & sort_fields, int & candidate_rank,
                                   std::vector<std::vector<art_leaf*>> & token_to_candidates,
                                   std::vector<std::vector<art_leaf*>> & searched_queries, Topster<100> & topster,
                                   size_t & total_results, uint32_t** all_result_ids, size_t & all_result_ids_len,
                                   const size_t & max_results, const bool prefix) {
    const size_t combination_limit = 10;
    auto product = []( long long a, std::vector<art_leaf*>& b ) { return a*b.size(); };
    long long int N = std::accumulate(token_to_candidates.begin(), token_to_candidates.end(), 1LL, product);

    for(long long n=0; n<N && n<combination_limit; ++n) {
        // every element in `query_suggestion` contains a token and its associated hits
        std::vector<art_leaf *> query_suggestion = next_suggestion(token_to_candidates, n);

        /*for(auto i=0; i < query_suggestion.size(); i++) {
            std::cout << "i: " << i << " - " << query_suggestion[i]->key << std::endl;
        }*/

        // initialize results with the starting element (for further intersection)
        uint32_t* result_ids = query_suggestion[0]->values->ids.uncompress();
        size_t result_size = query_suggestion[0]->values->ids.getLength();

        if(result_size == 0) {
            continue;
        }

        candidate_rank += 1;

        int actual_candidate_rank = candidate_rank;
        if(prefix) {
            actual_candidate_rank = 0;
        }

        // intersect the document ids for each token to find docs that contain all the tokens (stored in `result_ids`)
        for(auto i=1; i < query_suggestion.size(); i++) {
            uint32_t* out = nullptr;
            result_size = query_suggestion[i]->values->ids.intersect(result_ids, result_size, &out);
            delete[] result_ids;
            result_ids = out;
        }

        if(filter_ids != nullptr) {
            // intersect once again with filter ids
            uint32_t* filtered_result_ids = new uint32_t[std::min(filter_ids_length, result_size)];
            size_t filtered_results_size = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                                  result_size, filtered_result_ids);

            uint32_t* new_all_result_ids;
            all_result_ids_len = ArrayUtils::or_scalar(*all_result_ids, all_result_ids_len, filtered_result_ids,
                                  filtered_results_size, &new_all_result_ids);
            delete [] *all_result_ids;
            *all_result_ids = new_all_result_ids;

            do_facets(facets, filtered_result_ids, filtered_results_size);

            // go through each matching document id and calculate match score
            score_results(sort_fields, searched_queries.size(), actual_candidate_rank, topster, query_suggestion,
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

            score_results(sort_fields, searched_queries.size(), actual_candidate_rank, topster, query_suggestion,
                          result_ids, result_size);
            delete[] result_ids;
        }

        total_results += topster.size;
        searched_queries.push_back(query_suggestion);

        if(!prefix && total_results >= max_results) {
            break;
        }

        if(prefix && candidate_rank >= max_results) {
            break;
        }
    }
}

size_t Collection::union_of_leaf_ids(std::vector<const art_leaf *> &leaves, uint32_t **results_out) {
    uint32_t *results = nullptr;
    size_t results_length = 0;

    uint32_t *prev_results = nullptr;
    size_t prev_results_length = 0;

    for(const art_leaf* leaf: leaves) {
        results_length = leaf->values->ids.do_union(prev_results, prev_results_length, &results);

        delete [] prev_results;
        prev_results = results;
        prev_results_length = results_length;
    }

    *results_out = results;
    return results_length;
}

Option<uint32_t> Collection::do_filtering(uint32_t** filter_ids_out, const std::string & simple_filter_str) {
    // parse the filter string
    std::vector<std::string> filter_blocks;
    StringUtils::split(simple_filter_str, filter_blocks, "&&");

    std::vector<filter> filters;

    for(const std::string & filter_block: filter_blocks) {
        // split into [field_name, value]
        std::vector<std::string> expression_parts;
        StringUtils::split(filter_block, expression_parts, ":");
        if(expression_parts.size() != 2) {
            return Option<>(400, "Could not parse the filter query.");
        }

        const std::string & field_name = expression_parts[0];
        if(search_schema.count(field_name) == 0) {
            return Option<>(400, "Could not find a filter field named `" + field_name + "` in the schema.");
        }

        field _field = search_schema.at(field_name);
        const std::string & raw_value = expression_parts[1];
        filter f;

        if(_field.is_integer() || _field.is_float()) {
            // could be a single value or a list
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");

                for(const std::string & filter_value: filter_values) {
                    if(_field.is_integer() && !StringUtils::is_integer(filter_value)) {
                        return Option<>(400, "Error with field `" + _field.name + "`: Not an integer.");
                    }

                    if(_field.is_float() && !StringUtils::is_float(filter_value)) {
                        return Option<>(400, "Error with field `" + _field.name + "`: Not a float.");
                    }
                }

                f = {field_name, filter_values, EQUALS};
            } else {
                Option<NUM_COMPARATOR> op_comparator = filter::extract_num_comparator(raw_value);
                if(!op_comparator.ok()) {
                    return Option<>(400, "Error with field `" + _field.name + "`: " + op_comparator.error());
                }

                // extract numerical value
                std::string filter_value;
                if(op_comparator.get() == LESS_THAN || op_comparator.get() == GREATER_THAN) {
                    filter_value = raw_value.substr(1);
                } else if(op_comparator.get() == LESS_THAN_EQUALS || op_comparator.get() == GREATER_THAN_EQUALS) {
                    filter_value = raw_value.substr(2);
                } else {
                    // EQUALS
                    filter_value = raw_value;
                }

                filter_value = StringUtils::trim(filter_value);

                if(_field.is_integer() && !StringUtils::is_integer(filter_value)) {
                    return Option<>(400, "Error with field `" + _field.name + "`: Not an integer.");
                }

                if(_field.is_float() && !StringUtils::is_float(filter_value)) {
                    return Option<>(400, "Error with field `" + _field.name + "`: Not a float.");
                }

                f = {field_name, {filter_value}, op_comparator.get()};
            }
        } else if(_field.is_string()) {
            if(raw_value[0] == '[' && raw_value[raw_value.size() - 1] == ']') {
                std::vector<std::string> filter_values;
                StringUtils::split(raw_value.substr(1, raw_value.size() - 2), filter_values, ",");
                f = {field_name, filter_values, EQUALS};
            } else {
                f = {field_name, {raw_value}, EQUALS};
            }
        } else {
            return Option<>(400, "Error with field `" + _field.name + "`: Unidentified field type.");
        }

        filters.push_back(f);
    }

    uint32_t* filter_ids = nullptr;
    uint32_t filter_ids_length = 0;

    // process the filters first
    for(const filter & a_filter: filters) {
        if(search_index.count(a_filter.field_name) != 0) {
            art_tree* t = search_index.at(a_filter.field_name);
            field f = search_schema.at(a_filter.field_name);
            std::vector<const art_leaf*> leaves;

            if(f.is_integer()) {
                for(const std::string & filter_value: a_filter.values) {
                    if(f.type == field_types::INT32 || f.type == field_types::INT32_ARRAY) {
                        int32_t value = (int32_t) std::stoi(filter_value);
                        art_int32_search(t, value, a_filter.compare_operator, leaves);
                    } else {
                        int64_t value = (int64_t) std::stoi(filter_value);
                        art_int64_search(t, value, a_filter.compare_operator, leaves);
                    }
                }
            } else if(f.is_float()) {
                for(const std::string & filter_value: a_filter.values) {
                    float value = (float) std::atof(filter_value.c_str());
                    art_float_search(t, value, a_filter.compare_operator, leaves);
                }
            } else if(f.is_string()) {
                for(const std::string & filter_value: a_filter.values) {
                    art_leaf* leaf = (art_leaf *) art_search(t, (const unsigned char*) filter_value.c_str(), filter_value.length()+1);
                    if(leaf != nullptr) {
                        leaves.push_back(leaf);
                    }
                }
            }

            uint32_t* result_ids = nullptr;
            size_t result_ids_length = union_of_leaf_ids(leaves, &result_ids);

            if(filter_ids == nullptr) {
                filter_ids = result_ids;
                filter_ids_length = result_ids_length;
            } else {
                uint32_t* filtered_results = new uint32_t[std::min((size_t)filter_ids_length, result_ids_length)];
                filter_ids_length = ArrayUtils::and_scalar(filter_ids, filter_ids_length, result_ids,
                                                             result_ids_length, filtered_results);
                delete [] filter_ids;
                delete [] result_ids;
                filter_ids = filtered_results;
            }
        }
    }

    *filter_ids_out = filter_ids;
    return Option<>(filter_ids_length);
}

Option<nlohmann::json> Collection::search(std::string query, const std::vector<std::string> search_fields,
                                  const std::string & simple_filter_query, const std::vector<std::string> & facet_fields,
                                  const std::vector<sort_by> & sort_fields, const int num_typos,
                                  const size_t per_page, const size_t page,
                                  const token_ordering token_order, const bool prefix) {
    nlohmann::json result = nlohmann::json::object();
    std::vector<facet> facets;

    // validate search fields
    for(const std::string & field_name: search_fields) {
        if(search_schema.count(field_name) == 0) {
            std::string error = "Could not find a search field named `" + field_name + "` in the schema.";
            return Option<nlohmann::json>(400, error);
        }

        field search_field = search_schema.at(field_name);
        if(search_field.type != field_types::STRING && search_field.type != field_types::STRING_ARRAY) {
            std::string error = "Search field `" + field_name + "` should be a string or a string array.";
            return Option<nlohmann::json>(400, error);
        }
    }

    // validate facet fields
    for(const std::string & field_name: facet_fields) {
        if(facet_schema.count(field_name) == 0) {
            std::string error = "Could not find a facet field named `" + field_name + "` in the schema.";
            return Option<nlohmann::json>(400, error);
        }
        facets.push_back(facet(field_name));
    }

    // validate sort fields and standardize

    std::vector<sort_by> sort_fields_std;

    for(const sort_by & _sort_field: sort_fields) {
        if(sort_index.count(_sort_field.name) == 0) {
            std::string error = "Could not find a sort field named `" + _sort_field.name + "` in the schema.";
            return Option<nlohmann::json>(400, error);
        }

        std::string sort_order = _sort_field.order;
        StringUtils::toupper(sort_order);

        if(sort_order != sort_field_const::asc && sort_order != sort_field_const::desc) {
            std::string error = "Order for sort field` " + _sort_field.name + "` should be either ASC or DESC.";
            return Option<nlohmann::json>(400, error);
        }

        sort_fields_std.push_back({_sort_field.name, sort_order});
    }

    // process the filters
    uint32_t* filter_ids = nullptr;
    Option<uint32_t> op_filter_ids_length = do_filtering(&filter_ids, simple_filter_query);
    if(!op_filter_ids_length.ok()) {
        return Option<nlohmann::json>(op_filter_ids_length.code(), op_filter_ids_length.error());
    }

    const uint32_t filter_ids_length = op_filter_ids_length.get();

    // check for valid pagination
    if((page * per_page) > MAX_RESULTS) {
        std::string message = "Only the first " + std::to_string(MAX_RESULTS) + " results are available.";
        return Option<nlohmann::json>(422, message);
    }

    const size_t num_results = (page * per_page);

    // Order of `fields` are used to sort results
    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<int, Topster<100>::KV>> field_order_kvs;
    uint32_t* all_result_ids = nullptr;
    size_t all_result_ids_len = 0;

    // all search queries that were used for generating the results
    std::vector<std::vector<art_leaf*>> searched_queries;
    int searched_queries_index = 0;

    for(int i = 0; i < search_fields.size(); i++) {
        Topster<100> topster;
        const std::string & field = search_fields[i];
        // proceed to query search only when no filters are provided or when filtering produces results
        if(simple_filter_query.size() == 0 || filter_ids_length > 0) {
            search_field(query, field, filter_ids, filter_ids_length, facets, sort_fields_std, num_typos, num_results,
                         searched_queries, searched_queries_index, topster, &all_result_ids, all_result_ids_len, token_order, prefix);
            topster.sort();
        }

        // order of fields specified matter: matching docs from earlier fields are more important
        for(auto t = 0; t < topster.size && t < num_results; t++) {
            field_order_kvs.push_back(std::make_pair(search_fields.size() - i, topster.getKV(t)));
        }
    }

    delete [] filter_ids;
    delete [] all_result_ids;

    // All fields are sorted descending
    std::sort(field_order_kvs.begin(), field_order_kvs.end(),
      [](const std::pair<int, Topster<100>::KV> & a, const std::pair<int, Topster<100>::KV> & b) {
          return std::tie(a.second.match_score, a.second.primary_attr, a.second.secondary_attr, a.first, a.second.key) >
                 std::tie(b.second.match_score, b.second.primary_attr, b.second.secondary_attr, b.first, b.second.key);
    });

    result["hits"] = nlohmann::json::array();
    result["found"] = all_result_ids_len;

    const int start_result_index = (page - 1) * per_page;
    const int kvsize = field_order_kvs.size();

    if(start_result_index > (kvsize - 1)) {
        return Option<nlohmann::json>(result);
    }

    const int end_result_index = std::min(int(page * per_page), kvsize) - 1;

    for(size_t field_order_kv_index = start_result_index; field_order_kv_index <= end_result_index; field_order_kv_index++) {
        const auto & field_order_kv = field_order_kvs[field_order_kv_index];
        const std::string& seq_id_key = get_seq_id_key((uint32_t) field_order_kv.second.key);

        std::string value;
        store->get(seq_id_key, value);

        nlohmann::json document;
        try {
            document = nlohmann::json::parse(value);
        } catch(...) {
            return Option<nlohmann::json>(500, "Error while parsing stored document.");
        }

        // highlight query words in the result
        const std::string & field_name = search_fields[search_fields.size() - field_order_kv.first];
        field search_field = search_schema.at(field_name);

        // only string fields are supported for now
        if(search_field.type == field_types::STRING) {
            std::vector<std::string> tokens;
            StringUtils::split(document[field_name], tokens, " ");

            std::vector<std::vector<uint16_t>> token_positions;

            for (const art_leaf *token_leaf : searched_queries[field_order_kv.second.query_index]) {
                std::vector<uint16_t> positions;
                int doc_index = token_leaf->values->ids.indexOf(field_order_kv.second.key);
                if(doc_index == token_leaf->values->ids.getLength()) {
                    continue;
                }

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

            MatchScore mscore = MatchScore::match_score(field_order_kv.second.key, token_positions);

            // unpack `mscore.offset_diffs` into `token_indices`
            std::vector<size_t> token_indices;
            char num_tokens_found = mscore.offset_diffs[0];
            for(size_t i = 1; i <= num_tokens_found; i++) {
                size_t token_index = (size_t)(mscore.start_offset + mscore.offset_diffs[i]);
                token_indices.push_back(token_index);
            }

            auto minmax = std::minmax_element(token_indices.begin(), token_indices.end());

            // For longer strings, pick surrounding tokens within N tokens of min_index and max_index for the snippet
            const size_t start_index = (tokens.size() <= SNIPPET_STR_ABOVE_LEN) ? 0 :
                                       std::max(0, (int)(*(minmax.first)-5));

            const size_t end_index = (tokens.size() <= SNIPPET_STR_ABOVE_LEN) ? tokens.size() :
                                     std::min((int)tokens.size(), (int)(*(minmax.second)+5));

            for(const size_t token_index: token_indices) {
                tokens[token_index] = "<mark>" + tokens[token_index] + "</mark>";
            }

            std::stringstream snippet_stream;
            for(size_t snippet_index = start_index; snippet_index < end_index; snippet_index++) {
                if(snippet_index != start_index) {
                    snippet_stream << " ";
                }

                snippet_stream << tokens[snippet_index];
            }

            document["_snippets"] = nlohmann::json::object();
            document["_snippets"][field_name] = snippet_stream.str();
        }

        result["hits"].push_back(document);
    }

    result["facet_counts"] = nlohmann::json::array();

    // populate facets
    for(const facet & a_facet: facets) {
        nlohmann::json facet_result = nlohmann::json::object();
        facet_result["field_name"] = a_facet.field_name;
        facet_result["counts"] = nlohmann::json::array();

        // keep only top 10 facets
        std::vector<std::pair<std::string, size_t>> value_to_count;
        for (auto itr = a_facet.result_map.begin(); itr != a_facet.result_map.end(); ++itr) {
            value_to_count.push_back(*itr);
        }

        std::sort(value_to_count.begin(), value_to_count.end(),
                  [=](std::pair<std::string, size_t>& a, std::pair<std::string, size_t>& b) {
                      return a.second > b.second;
                  });

        for(auto i = 0; i < std::min((size_t)10, value_to_count.size()); i++) {
            auto & kv = value_to_count[i];
            nlohmann::json facet_value_count = nlohmann::json::object();
            facet_value_count["value"] = kv.first;
            facet_value_count["count"] = kv.second;
            facet_result["counts"].push_back(facet_value_count);
        }

        result["facet_counts"].push_back(facet_result);
    }

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
void Collection::search_field(std::string & query, const std::string & field, uint32_t *filter_ids, size_t filter_ids_length,
                              std::vector<facet> & facets, const std::vector<sort_by> & sort_fields, const int num_typos,
                              const size_t num_results, std::vector<std::vector<art_leaf*>> & searched_queries,
                              int & searched_queries_index, Topster<100> &topster, uint32_t** all_result_ids, size_t & all_result_ids_len,
                              const token_ordering token_order, const bool prefix) {
    std::vector<std::string> tokens;
    StringUtils::split(query, tokens, " ");

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

    // stores candidates for each token, i.e. i-th index would have all possible tokens with a cost of "c"
    std::vector<std::vector<art_leaf*>> token_to_candidates;

    const size_t combination_limit = 10;
    auto product = []( long long a, std::vector<int>& b ) { return a*b.size(); };
    int candidate_rank = 0;
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

        token_to_candidates.clear();
        int token_index = 0;

        while(token_index < tokens.size()) {
            // For each token, look up the generated cost for this iteration and search using that cost
            std::string token = tokens[token_index];
            const std::string token_cost_hash = token + std::to_string(costs[token_index]);

            std::vector<art_leaf*> leaves;
            /*std::cout << "\nSearching for: " << token << " - cost: " << costs[token_index] << ", candidate_rank: "
                      << candidate_rank << std::endl;*/

            if(token_cost_cache.count(token_cost_hash) != 0) {
                leaves = token_cost_cache[token_cost_hash];
            } else {
                // prefix should apply only for last token
                const bool prefix_search = prefix && ((token_index == tokens.size()-1) ? true : false);
                const int token_len = prefix_search ? (int) token.length() : (int) token.length() + 1;

                art_fuzzy_search(search_index.at(field), (const unsigned char *) token.c_str(), token_len,
                                 costs[token_index], costs[token_index], 3, token_order, prefix_search, leaves);

                if(!leaves.empty()) {
                    token_cost_cache.emplace(token_cost_hash, leaves);
                }
            }

            if(!leaves.empty()) {
                //!log_leaves(costs[token_index], token, leaves);
                token_to_candidates.push_back(leaves);
                token_to_count[token] = std::max(token_to_count[token], leaves.at(0)->values->ids.getLength());
            } else {
                // No result at `cost = costs[token_index]`. Remove costs until `cost` for token and re-do combinations
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

                // To continue outerloop on new cost combination
                n = -1;
                N = std::accumulate(token_to_costs.begin(), token_to_costs.end(), 1LL, product);
                break;
            }

            token_index++;
        }

        if(token_to_candidates.size() != 0 && token_to_candidates.size() == tokens.size()) {
            // If all tokens were found, go ahead and search for candidates with what we have so far
            search_candidates(filter_ids, filter_ids_length, facets, sort_fields, candidate_rank, token_to_candidates,
                              searched_queries, topster, total_results, all_result_ids, all_result_ids_len,
                              max_results, prefix);

            if (!prefix && total_results >= max_results) {
                // If we don't find enough results, we continue outerloop (looking at tokens with greater cost)
                break;
            }

            if(prefix && candidate_rank > 10) {
                break;
            }
        }

        n++;
    }

    // When there are not enough overall results and atleast one token has results
    if(topster.size < max_results && token_to_count.size() > 1) {
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

        return search_field(truncated_query, field, filter_ids, filter_ids_length, facets, sort_fields, num_typos,
                            num_results, searched_queries, candidate_rank, topster, all_result_ids, all_result_ids_len,
                            token_order, prefix);
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

void Collection::score_results(const std::vector<sort_by> & sort_fields, const int & query_index, const int & candidate_rank,
                               Topster<100> & topster, const std::vector<art_leaf *> &query_suggestion,
                               const uint32_t *result_ids, const size_t result_size) const {

    const int max_candidate_rank = 250;
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
        if(sort_field.is_integer()) {
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
        if(sort_field.is_integer()) {
            secondary_rank_factor = ((int64_t) 1);
        } else {
            secondary_rank_factor = ((float) 1);
        }

        if(sort_fields[1].order == sort_field_const::asc) {
            secondary_rank_factor = -secondary_rank_factor;
        }
    }

    for(auto i=0; i<result_size; i++) {
        uint32_t seq_id = result_ids[i];
        MatchScore mscore;

        if(query_suggestion.size() == 1) {
            // short circuit to speed up single token searches (use dummy offsets for now)
            char offset_diffs[16];
            std::fill_n(offset_diffs, 16, 0);
            mscore = MatchScore(1, 0, 0, offset_diffs);
        } else {
            std::vector<std::vector<uint16_t>> token_positions;
            populate_token_positions(query_suggestion, leaf_to_indices, i, token_positions);
            mscore = MatchScore::match_score(seq_id, token_positions);
        }

        int candidate_rank_score = max_candidate_rank - candidate_rank;

        // Construct a single match_score from individual components (for multi-field sort)
        const uint64_t match_score = ((uint64_t)(mscore.words_present) << 16) +
                                     (candidate_rank_score << 8) +
                                     (MAX_SEARCH_TOKENS - mscore.distance);

        const int64_t default_score = 0;
        const number_t & primary_rank_score = (primary_rank_scores && primary_rank_scores->count(seq_id) > 0) ?
                                     primary_rank_scores->at(seq_id) : default_score;
        const number_t & secondary_rank_score = (secondary_rank_scores && secondary_rank_scores->count(seq_id) > 0) ?
                                     secondary_rank_scores->at(seq_id) : default_score;

        const number_t & primary_rank_value = primary_rank_score * primary_rank_factor;
        const number_t & secondary_rank_value = secondary_rank_score * secondary_rank_factor;
        topster.add(seq_id, query_index, match_score, primary_rank_value, secondary_rank_value);

        /*std::cout << "candidate_rank_score: " << candidate_rank_score << ", words_present: " << mscore.words_present
                  << ", match_score: " << match_score << ", primary_rank_score: " << primary_rank_score
                  << ", seq_id: " << seq_id << std::endl;*/
    }

    for (auto it = leaf_to_indices.begin(); it != leaf_to_indices.end(); it++) {
        delete [] it->second;
        it->second = nullptr;
    }
}

void Collection::populate_token_positions(const std::vector<art_leaf *> &query_suggestion,
                                          spp::sparse_hash_map<const art_leaf *, uint32_t *> &leaf_to_indices,
                                          size_t result_index, std::vector<std::vector<uint16_t>> &token_positions) const {
    // for each token in the query, find the positions that it appears in this document
    for (const art_leaf *token_leaf : query_suggestion) {
            std::vector<uint16_t> positions;
            int doc_index = leaf_to_indices.at(token_leaf)[result_index];
            if(doc_index == token_leaf->values->ids.getLength()) {
                continue;
            }

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
}

inline std::vector<art_leaf *> Collection::next_suggestion(const std::vector<std::vector<art_leaf *>> &token_leaves,
                                                           long long int n) {
    std::vector<art_leaf*> query_suggestion(token_leaves.size());

    // generate the next combination from `token_leaves` and store it in `query_suggestion`
    ldiv_t q { n, 0 };
    for(long long i = 0 ; i < token_leaves.size(); i++) {
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

Option<nlohmann::json> Collection::get(const std::string & id) {
    std::string seq_id_str;
    StoreStatus status = store->get(get_doc_id_key(id), seq_id_str);

    if(status == StoreStatus::NOT_FOUND) {
        return Option<nlohmann::json>(404, "Could not find a document with id: " + id);
    }

    uint32_t seq_id = (uint32_t) std::stol(seq_id_str);

    std::string parsed_document;
    store->get(get_seq_id_key(seq_id), parsed_document);

    nlohmann::json document;
    try {
        document = nlohmann::json::parse(parsed_document);
    } catch(...) {
        return Option<nlohmann::json>(500, "Error while parsing stored document.");
    }

    return Option<nlohmann::json>(document);
}

Option<std::string> Collection::remove(const std::string & id, const bool remove_from_store) {
    std::string seq_id_str;
    StoreStatus status = store->get(get_doc_id_key(id), seq_id_str);

    if(status == StoreStatus::NOT_FOUND) {
        return Option<std::string>(404, "Could not find a document with id: " + id);
    }

    uint32_t seq_id = (uint32_t) std::stol(seq_id_str);

    std::string parsed_document;
    store->get(get_seq_id_key(seq_id), parsed_document);

    nlohmann::json document;
    try {
        document = nlohmann::json::parse(parsed_document);
    } catch(...) {
        return Option<std::string>(500, "Error while parsing stored document.");
    }

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
        }

        for(auto & token: tokens) {
            const unsigned char *key;
            int key_len;

            if(name_field.second.type == field_types::STRING_ARRAY || name_field.second.type == field_types::STRING) {
                std::transform(token.begin(), token.end(), token.begin(), ::tolower);
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
                    std::cout << "i: " << i << ", val: " << leaf->values->offset_index.at(i) << std::endl;
                }
                std::cout << "----" << std::endl;*/

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

    if(remove_from_store) {
        store->remove(get_doc_id_key(id));
        store->remove(get_seq_id_key(seq_id));
    }

    num_documents -= 1;

    return Option<std::string>(id);
}

std::string Collection::get_next_seq_id_key(const std::string & collection_name) {
    return std::string(COLLECTION_NEXT_SEQ_PREFIX) + "_" + collection_name;
}

std::string Collection::get_seq_id_key(uint32_t seq_id) {
    // We can't simply do std::to_string() because we want to preserve the byte order.
    // & 0xFF masks all but the lowest eight bits.
    unsigned char bytes[4];
    bytes[0] = (unsigned char) ((seq_id >> 24) & 0xFF);
    bytes[1] = (unsigned char) ((seq_id >> 16) & 0xFF);
    bytes[2] = (unsigned char) ((seq_id >> 8) & 0xFF);
    bytes[3] = (unsigned char) ((seq_id & 0xFF));

    return get_seq_id_collection_prefix() + "_" + std::string(bytes, bytes+4);
}

uint32_t Collection::deserialize_seq_id_key(std::string serialized_seq_id) {
    uint32_t seq_id = ((serialized_seq_id[0] & 0xFF) << 24) | ((serialized_seq_id[1] & 0xFF) << 16) |
                      ((serialized_seq_id[2] & 0xFF) << 8)  | (serialized_seq_id[3] & 0xFF);
    return seq_id;
}

std::string Collection::get_doc_id_key(const std::string & doc_id) {
    return std::to_string(collection_id) + "_" + DOC_ID_PREFIX + "_" + doc_id;
}

std::string Collection::get_name() {
    return name;
}

size_t Collection::get_num_documents() {
    return num_documents;
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

std::vector<std::string> Collection::get_facet_fields() {
    std::vector<std::string> facet_fields_copy;
    for(auto it = facet_schema.begin(); it != facet_schema.end(); ++it) {
        facet_fields_copy.push_back(it->first);
    }

    return facet_fields_copy;
}

std::vector<field> Collection::get_sort_fields() {
    std::vector<field> sort_fields_copy;
    for(auto it = sort_schema.begin(); it != sort_schema.end(); ++it) {
        sort_fields_copy.push_back(it->second);
    }

    return sort_fields_copy;
}

spp::sparse_hash_map<std::string, field> Collection::get_schema() {
    return search_schema;
};

std::string Collection::get_meta_key(const std::string & collection_name) {
    return std::string(COLLECTION_META_PREFIX) + "_" + collection_name;
}

std::string Collection::get_seq_id_collection_prefix() {
    return std::to_string(collection_id) + "_" + std::string(SEQ_ID_PREFIX);
}

std::string Collection::get_token_ranking_field() {
    return token_ranking_field;
}